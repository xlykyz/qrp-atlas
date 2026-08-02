"""Orchestrate PIT historical backfill over existing pipelines.

Stages are decoupled:
  fetch  -> raw parquet
  clean  -> cleaned parquet (reuse existing clean_*)
  load   -> duckdb append-only (reuse existing load_*)
"""

from __future__ import annotations

import json
import logging
import traceback
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Any, Sequence

import pandas as pd

from qrp_atlas.config.settings import AppSettings, redact_secrets
from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError
from qrp_atlas.pipeline.fundamentals.clean import clean_financial
from qrp_atlas.pipeline.fundamentals.fetch import fetch_financial_by_period
from qrp_atlas.pipeline.fundamentals.load_duckdb import load_financial
from qrp_atlas.pipeline.fundamentals.run import ALL_TABLES
from qrp_atlas.pipeline.index_component.clean import clean_index_component
from qrp_atlas.pipeline.index_component.fetch import fetch_index_weight
from qrp_atlas.pipeline.index_component.load_duckdb import load_index_component
from qrp_atlas.pipeline.industry_membership.clean import clean_industry_membership
from qrp_atlas.pipeline.industry_membership.fetch import fetch_industry_membership
from qrp_atlas.pipeline.industry_membership.load_duckdb import load_industry_membership
from qrp_atlas.pipeline.pit_backfill.batches import (
    DEFAULT_INDEX_CODES,
    Batch,
    discover_sw2021_l1_codes,
    financial_batches,
    index_batches,
    industry_batches,
    precheck_batches,
    summarize_plan,
)
from qrp_atlas.pipeline.pit_backfill.manifest import (
    ALL_STAGES,
    STAGE_CLEAN,
    STAGE_FETCH,
    STAGE_LOAD,
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    TERMINAL_OK,
    BatchRecord,
    ManifestStore,
    utc_now_iso,
)
from qrp_atlas.pipeline.pit_backfill.rate_limit import (
    DEFAULT_MIN_INTERVAL,
    RateLimiter,
    is_rate_limit_error,
)
from qrp_atlas.pipeline.pit_backfill.raw_io import (
    CorruptParquetError,
    cleaned_file_path,
    load_parquet,
    load_parquet_or_quarantine,
    quarantine_corrupt,
    raw_file_path,
    save_parquet,
    validate_parquet,
)
from qrp_atlas.pipeline.pit_backfill.safety import (
    ensure_load_backup,
    pipeline_db_lock,
    preflight,
)
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver

RUN_TAG = "20260714"
LOGGER_NAME = "qrp_atlas.pipeline.pit_backfill"


def default_paths(run_tag: str = RUN_TAG) -> dict[str, Path]:
    settings = AppSettings.load()
    paths = settings.paths
    state_dir = paths.state_dir / f"pit_backfill_{run_tag}"
    return {
        "raw_dir": paths.raw_dir / "pit_backfill" / run_tag,
        "cleaned_dir": paths.canonical_dir / "pit_backfill" / run_tag,
        "state_dir": state_dir,
        "log_path": paths.log_dir / f"pit_backfill_{run_tag}.log",
        "manifest_path": state_dir / "manifest.jsonl",
        "plan_path": state_dir / "plan.json",
        "db_path": paths.duckdb_path,
    }


def setup_logging(log_path: str | Path) -> logging.Logger:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def _sanitize_error(exc: BaseException) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return redact_secrets(text)[:2000]


def parse_stages(raw: str | Sequence[str] | None) -> tuple[str, ...]:
    if raw is None:
        return ALL_STAGES
    if isinstance(raw, str):
        if raw.strip().lower() in {"", "all"}:
            return ALL_STAGES
        parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    else:
        parts = [str(p).strip().lower() for p in raw if str(p).strip()]
    if not parts or parts == ["all"]:
        return ALL_STAGES
    bad = [p for p in parts if p not in ALL_STAGES]
    if bad:
        raise ValueError(f"unsupported stages: {bad}; choose from {ALL_STAGES}")
    # keep dependency order
    return tuple(s for s in ALL_STAGES if s in parts)


class RateLimitedPro:
    """Proxy that paces every callable attribute access (including retries)."""

    def __init__(
        self,
        pro: Any,
        limiter: RateLimiter,
        execution_control: ExecutionControl | None = None,
    ):
        self._pro = pro
        self._limiter = limiter
        self._execution_control = execution_control

    def __getattr__(self, name: str):
        attr = getattr(self._pro, name)
        if not callable(attr):
            return attr

        def wrapped(*args, **kwargs):
            try:
                return self._limiter.call(
                    attr,
                    *args,
                    execution_control=self._execution_control,
                    **kwargs,
                )
            except ExecutionControlError:
                raise

        return wrapped


@dataclass
class BackfillConfig:
    run_tag: str = RUN_TAG
    mode: str = "full"  # full | precheck | plan-only
    datasets: Sequence[str] = ("fundamentals", "industry", "index")
    stages: Sequence[str] = ALL_STAGES
    resume: bool = False
    db_path: str | Path | None = None
    raw_dir: str | Path | None = None
    cleaned_dir: str | Path | None = None
    state_dir: str | Path | None = None
    log_path: str | Path | None = None
    min_interval: float = DEFAULT_MIN_INTERVAL
    create_backup: bool = True
    skip_preflight: bool = False
    max_batches: int | None = None
    client: Any = None
    dry_run: bool = False
    run_audit: bool = False
    l1_codes: Sequence[str] | None = None
    index_codes: Sequence[str] = DEFAULT_INDEX_CODES
    # If true, do not call network even when raw missing (fail instead)
    offline_only: bool = False
    # Formal callers supply these explicit scopes; None preserves legacy CLI defaults.
    financial_tables: Sequence[str] | None = None
    financial_periods: Sequence[str] | None = None
    financial_start: Any = None
    financial_end: Any = None
    index_start: Any = None
    index_end: Any = None
    settings: AppSettings | None = None
    execution_control: ExecutionControl | None = None
    lock_path: str | Path | None = None
    strict_scope: bool = False


class PitBackfillRunner:
    def __init__(self, config: BackfillConfig):
        self.config = config
        self.settings = config.settings or AppSettings.load()
        paths = default_paths(config.run_tag)
        self.db_path = Path(config.db_path or paths["db_path"])
        self.raw_dir = Path(config.raw_dir or paths["raw_dir"])
        self.cleaned_dir = Path(config.cleaned_dir or paths["cleaned_dir"])
        self.state_dir = Path(config.state_dir or paths["state_dir"])
        self.log_path = Path(config.log_path or paths["log_path"])
        self.manifest_path = self.state_dir / "manifest.jsonl"
        self.plan_path = self.state_dir / "plan.json"
        self.logger = setup_logging(self.log_path)
        self.limiter = RateLimiter(min_interval=config.min_interval)
        self.manifest = ManifestStore(self.manifest_path)
        self.resolver: NextTradeDateResolver | None = None
        self._base_client = config.client
        self._client: Any | None = None
        self.stages = parse_stages(config.stages)

    @property
    def request_count(self) -> int:
        return self.limiter.call_count

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        if self._base_client is not None:
            self._client = RateLimitedPro(
                self._base_client,
                self.limiter,
                self.config.execution_control,
            )
            return self._client
        from qrp_atlas.config import get_tushare_pro

        self._client = RateLimitedPro(
            get_tushare_pro(
                settings=self.settings,
                execution_control=self.config.execution_control,
            ),
            self.limiter,
            self.config.execution_control,
        )
        return self._client

    def build_plan(self) -> list[Batch]:
        cfg = self.config
        if cfg.mode == "precheck":
            l1 = list(cfg.l1_codes)[0] if cfg.l1_codes else None
            return precheck_batches(l1_code=l1)

        batches: list[Batch] = []
        if "fundamentals" in cfg.datasets:
            tables = tuple(cfg.financial_tables or ())
            if cfg.financial_periods is not None:
                batches.extend(
                    financial_batches(
                        tables=tables or ALL_TABLES,
                        periods=cfg.financial_periods,
                    )
                )
            elif cfg.financial_start is not None or cfg.financial_end is not None:
                if cfg.financial_start is None or cfg.financial_end is None:
                    raise ValueError("financial_start and financial_end must be supplied together")
                batches.extend(
                    financial_batches(
                        tables=tables or ALL_TABLES,
                        start=cfg.financial_start,
                        end=cfg.financial_end,
                    )
                )
            else:
                batches.extend(financial_batches(tables=tables or ALL_TABLES))
        if "industry" in cfg.datasets:
            codes_path = self.state_dir / "sw2021_l1_codes.json"
            if cfg.l1_codes is not None:
                l1_codes = list(cfg.l1_codes)
            elif codes_path.exists() and (cfg.resume or cfg.offline_only):
                l1_codes = json.loads(codes_path.read_text(encoding="utf-8"))
                self.logger.info("Loaded %s L1 codes from cache", len(l1_codes))
            else:
                self.logger.info("Discovering SW2021 L1 industry codes via index_classify")
                if cfg.dry_run:
                    l1_codes = []
                else:
                    l1_codes = discover_sw2021_l1_codes(client=self._get_client())
                self.logger.info("Found %s L1 codes", len(l1_codes))
                self.state_dir.mkdir(parents=True, exist_ok=True)
                if l1_codes:
                    codes_path.write_text(
                        json.dumps(l1_codes, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                elif codes_path.exists():
                    l1_codes = json.loads(codes_path.read_text(encoding="utf-8"))
            batches.extend(industry_batches(l1_codes))
        if "index" in cfg.datasets:
            if cfg.index_start is not None or cfg.index_end is not None:
                if cfg.index_start is None or cfg.index_end is None:
                    raise ValueError("index_start and index_end must be supplied together")
                batches.extend(
                    index_batches(
                        index_codes=cfg.index_codes,
                        start=cfg.index_start,
                        end=cfg.index_end,
                    )
                )
            else:
                batches.extend(index_batches(index_codes=cfg.index_codes))
        if cfg.max_batches is not None:
            batches = batches[: int(cfg.max_batches)]
        return batches

    def save_plan(self, batches: list[Batch]) -> dict[str, Any]:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        summary = summarize_plan(batches)
        payload = {
            "run_tag": self.config.run_tag,
            "mode": self.config.mode,
            "stages": list(self.stages),
            "created_at": utc_now_iso(),
            "summary": summary,
            "batches": [b.to_dict() for b in batches],
        }
        self.plan_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary

    def _ensure_resolver(self) -> NextTradeDateResolver:
        if self.resolver is None:
            self.resolver = NextTradeDateResolver(db_path=str(self.db_path))
        return self.resolver

    def _fetch_from_api(self, batch: Batch) -> pd.DataFrame:
        self._check_control()
        client = self._get_client()
        if batch.dataset == "fundamentals":
            df = fetch_financial_by_period(
                batch.key,
                batch.period,
                client=client,
                execution_control=self.config.execution_control,
                settings=self.settings,
            )
            return df if df is not None else pd.DataFrame()
        if batch.dataset == "industry":
            df = fetch_industry_membership(
                l1_code=batch.key,
                is_new=None,
                client=client,
                execution_control=self.config.execution_control,
                settings=self.settings,
            )
            return df if df is not None else pd.DataFrame()
        if batch.dataset == "index":
            df = fetch_index_weight(
                batch.key,
                start_date=batch.start_date,
                end_date=batch.end_date,
                client=client,
                execution_control=self.config.execution_control,
                settings=self.settings,
            )
            return df if df is not None else pd.DataFrame()
        raise ValueError(f"unknown dataset: {batch.dataset}")

    def _validate_strict_scope(self, batch: Batch, raw: pd.DataFrame | None) -> None:
        """Fail closed on raw artifacts that do not identify their batch scope."""
        if not self.config.strict_scope or raw is None or raw.empty:
            return
        if not isinstance(raw, pd.DataFrame):
            raise ValueError(f"{batch.batch_id} returned a non-DataFrame payload")
        if batch.dataset == "fundamentals":
            required = {"ts_code", "end_date"}
            if "ann_date" not in raw.columns and "f_ann_date" not in raw.columns:
                required.add("ann_date|f_ann_date")
            missing = sorted(required - set(raw.columns))
            if missing:
                raise ValueError(f"{batch.batch_id} raw schema missing {missing}")
            periods = raw["end_date"].astype(str).str.replace("-", "", regex=False)
            if periods.ne(str(batch.period).replace("-", "")).any():
                raise ValueError(f"{batch.batch_id} raw end_date is outside the requested period")
            if raw["ts_code"].isna().any() or raw["end_date"].isna().any():
                raise ValueError(f"{batch.batch_id} raw identity fields contain nulls")
            announcement = raw["ann_date"] if "ann_date" in raw.columns else raw["f_ann_date"]
            if announcement.isna().any():
                raise ValueError(f"{batch.batch_id} raw announcement field contains nulls")
        elif batch.dataset == "industry":
            required = {"ts_code", "in_date", "l1_code"}
            missing = sorted(required - set(raw.columns))
            if missing:
                raise ValueError(f"{batch.batch_id} raw schema missing {missing}")
            if raw["l1_code"].astype(str).str.strip().ne(str(batch.key)).any():
                raise ValueError(f"{batch.batch_id} raw l1_code is outside the requested scope")
        elif batch.dataset == "index":
            required = {"index_code", "con_code", "trade_date"}
            missing = sorted(required - set(raw.columns))
            if missing:
                raise ValueError(f"{batch.batch_id} raw schema missing {missing}")
            dates = raw["trade_date"].astype(str).str.replace("-", "", regex=False)
            start = str(batch.start_date).replace("-", "")
            end = str(batch.end_date).replace("-", "")
            if dates.lt(start).any() or dates.gt(end).any():
                raise ValueError(f"{batch.batch_id} raw trade_date is outside the requested range")
            if raw["index_code"].astype(str).str.strip().ne(str(batch.key)).any():
                raise ValueError(f"{batch.batch_id} raw index_code is outside the requested scope")

    def _clean_df(self, batch: Batch, raw: pd.DataFrame) -> pd.DataFrame:
        self._check_control()
        resolver = self._ensure_resolver()
        if raw is None or raw.empty:
            return pd.DataFrame()
        if batch.dataset == "fundamentals":
            return clean_financial(
                raw,
                batch.key,
                trade_date_resolver=resolver,
                execution_control=self.config.execution_control,
            )
        if batch.dataset == "industry":
            return clean_industry_membership(
                raw,
                trade_date_resolver=resolver,
                execution_control=self.config.execution_control,
            )
        if batch.dataset == "index":
            return clean_index_component(
                raw,
                trade_date_resolver=resolver,
                execution_control=self.config.execution_control,
            )
        raise ValueError(f"unknown dataset: {batch.dataset}")

    def _load_df(self, batch: Batch, cleaned: pd.DataFrame) -> int:
        self._check_control()
        if cleaned is None or cleaned.empty:
            return 0
        if batch.dataset == "fundamentals":
            return int(
                load_financial(
                    cleaned,
                    batch.key,
                    db_path=self.db_path,
                    init=True,
                    execution_control=self.config.execution_control,
                )
            )
        if batch.dataset == "industry":
            return int(
                load_industry_membership(
                    cleaned,
                    db_path=self.db_path,
                    init=True,
                    execution_control=self.config.execution_control,
                )
            )
        if batch.dataset == "index":
            return int(
                load_index_component(
                    cleaned,
                    db_path=self.db_path,
                    init=True,
                    execution_control=self.config.execution_control,
                )
            )
        raise ValueError(f"unknown dataset: {batch.dataset}")

    def _check_control(self) -> None:
        if self.config.execution_control is not None:
            self.config.execution_control.check()

    def _needs_stage(self, rec: BatchRecord, stage: str) -> bool:
        if stage not in self.stages:
            return False
        if not self.config.resume:
            # non-resume still skips fetch if offline raw exists to avoid re-requesting
            if stage == STAGE_FETCH:
                raw_path = Path(rec.raw_path) if rec.raw_path else raw_file_path(self.raw_dir, rec.batch_id)
                if raw_path.exists():
                    return False
            return True
        return rec.stage_status(stage) not in TERMINAL_OK

    def _run_fetch(self, batch: Batch, rec: BatchRecord) -> BatchRecord:
        self._check_control()
        raw_path = raw_file_path(self.raw_dir, batch.batch_id)
        rec.raw_path = str(raw_path)
        rec.set_stage(STAGE_FETCH, STATUS_RUNNING, error=None)
        self.manifest.save(rec)
        try:
            if raw_path.exists():
                self.logger.info("batch=%s FETCH offline raw=%s", batch.batch_id, raw_path)
                try:
                    raw = self._load_parquet_resilient(rec, STAGE_FETCH, raw_path)
                except CorruptParquetError:
                    # reset to pending and try network unless offline_only
                    if self.config.offline_only:
                        raise
                    self.logger.warning("batch=%s re-fetch after corrupt raw", batch.batch_id)
                    raw = self._fetch_from_api(batch)
                    self._validate_strict_scope(batch, raw)
                    save_parquet(raw, raw_path)
                    offline = False
                    fetched = 0 if raw is None else len(raw)
                    rec.fetched_rows = fetched
                    rec.set_stage(STAGE_FETCH, STATUS_EMPTY if fetched == 0 else STATUS_SUCCESS, finished=True)
                    self.manifest.save(rec)
                    return rec
                offline = True
            else:
                if self.config.offline_only:
                    raise FileNotFoundError(f"offline_only set but raw missing: {raw_path}")
                self.logger.info("batch=%s FETCH network", batch.batch_id)
                raw = self._fetch_from_api(batch)
                self._validate_strict_scope(batch, raw)
                save_parquet(raw, raw_path)
                offline = False
            fetched = 0 if raw is None else len(raw)
            self._validate_strict_scope(batch, raw)
            rec.fetched_rows = fetched
            if fetched == 0:
                if not raw_path.exists():
                    save_parquet(pd.DataFrame(), raw_path)
                rec.set_stage(STAGE_FETCH, STATUS_EMPTY, finished=True)
                # downstream becomes empty as well when nothing fetched
                if STAGE_CLEAN in self.stages:
                    rec.cleaned_rows = 0
                    rec.set_stage(STAGE_CLEAN, STATUS_EMPTY, finished=True)
                if STAGE_LOAD in self.stages:
                    rec.inserted_rows = 0
                    rec.set_stage(STAGE_LOAD, STATUS_EMPTY, finished=True)
            else:
                rec.set_stage(STAGE_FETCH, STATUS_SUCCESS, finished=True)
            self.manifest.save(rec)
            self.logger.info(
                "batch=%s FETCH %s rows=%s offline=%s",
                batch.batch_id,
                rec.fetch_status,
                fetched,
                offline,
            )
            return rec
        except ExecutionControlError:
            raise
        except Exception as exc:
            err = _sanitize_error(exc)
            if is_rate_limit_error(exc):
                self.logger.warning("batch=%s FETCH rate-limited: %s", batch.batch_id, err)
            self.logger.error("batch=%s FETCH FAILED %s", batch.batch_id, err)
            self.logger.debug("traceback:\n%s", traceback.format_exc())
            rec.set_stage(STAGE_FETCH, STATUS_FAILED, error=err, finished=True)
            self.manifest.save(rec)
            return rec

    def _run_clean(self, batch: Batch, rec: BatchRecord) -> BatchRecord:
        self._check_control()
        raw_path = Path(rec.raw_path) if rec.raw_path else raw_file_path(self.raw_dir, batch.batch_id)
        cleaned_path = cleaned_file_path(self.cleaned_dir, batch.batch_id)
        rec.cleaned_path = str(cleaned_path)
        rec.set_stage(STAGE_CLEAN, STATUS_RUNNING, error=None)
        self.manifest.save(rec)
        try:
            if rec.fetch_status == STATUS_EMPTY:
                save_parquet(pd.DataFrame(), cleaned_path)
                rec.cleaned_rows = 0
                rec.set_stage(STAGE_CLEAN, STATUS_EMPTY, finished=True)
                self.manifest.save(rec)
                self.logger.info("batch=%s CLEAN empty", batch.batch_id)
                return rec

            if not raw_path.exists():
                raise FileNotFoundError(f"raw missing for clean: {raw_path}")

            if cleaned_path.exists() and rec.clean_status in TERMINAL_OK:
                try:
                    cleaned = self._load_parquet_resilient(rec, STAGE_CLEAN, cleaned_path)
                except CorruptParquetError:
                    cleaned = None
                if cleaned is None:
                    raw = self._load_parquet_resilient(rec, STAGE_FETCH, raw_path)
                    cleaned = self._clean_df(batch, raw)
                    save_parquet(cleaned, cleaned_path)
            else:
                raw = self._load_parquet_resilient(rec, STAGE_FETCH, raw_path)
                cleaned = self._clean_df(batch, raw)
                save_parquet(cleaned, cleaned_path)
            rec.cleaned_rows = 0 if cleaned is None else len(cleaned)
            rec.set_stage(
                STAGE_CLEAN,
                STATUS_EMPTY if rec.cleaned_rows == 0 else STATUS_SUCCESS,
                finished=True,
            )
            self.manifest.save(rec)
            self.logger.info(
                "batch=%s CLEAN %s rows=%s path=%s",
                batch.batch_id,
                rec.clean_status,
                rec.cleaned_rows,
                cleaned_path,
            )
            return rec
        except ExecutionControlError:
            raise
        except Exception as exc:
            err = _sanitize_error(exc)
            self.logger.error("batch=%s CLEAN FAILED %s", batch.batch_id, err)
            self.logger.debug("traceback:\n%s", traceback.format_exc())
            rec.set_stage(STAGE_CLEAN, STATUS_FAILED, error=err, finished=True)
            self.manifest.save(rec)
            return rec

    def _run_load(self, batch: Batch, rec: BatchRecord) -> BatchRecord:
        self._check_control()
        cleaned_path = Path(rec.cleaned_path) if rec.cleaned_path else cleaned_file_path(self.cleaned_dir, batch.batch_id)
        rec.cleaned_path = str(cleaned_path)
        rec.set_stage(STAGE_LOAD, STATUS_RUNNING, error=None)
        self.manifest.save(rec)
        try:
            if rec.clean_status == STATUS_EMPTY or rec.cleaned_rows == 0:
                if cleaned_path.exists():
                    # confirm empty
                    pass
                rec.inserted_rows = 0
                rec.set_stage(STAGE_LOAD, STATUS_EMPTY, finished=True)
                self.manifest.save(rec)
                self.logger.info("batch=%s LOAD empty", batch.batch_id)
                return rec
            if not cleaned_path.exists():
                raise FileNotFoundError(f"cleaned missing for load: {cleaned_path}")
            cleaned = self._load_parquet_resilient(rec, STAGE_CLEAN, cleaned_path)
            write_started = monotonic()
            inserted = self._load_df(batch, cleaned)
            rec.meta["database_write_seconds"] = max(0.0, monotonic() - write_started)
            rec.inserted_rows = int(inserted)
            # load success even when inserted=0 (idempotent)
            rec.set_stage(STAGE_LOAD, STATUS_SUCCESS, finished=True)
            self.manifest.save(rec)
            self.logger.info(
                "batch=%s LOAD %s inserted=%s cleaned_rows=%s",
                batch.batch_id,
                rec.load_status,
                rec.inserted_rows,
                rec.cleaned_rows,
            )
            return rec
        except ExecutionControlError:
            raise
        except Exception as exc:
            err = _sanitize_error(exc)
            self.logger.error("batch=%s LOAD FAILED %s", batch.batch_id, err)
            self.logger.debug("traceback:\n%s", traceback.format_exc())
            rec.set_stage(STAGE_LOAD, STATUS_FAILED, error=err, finished=True)
            self.manifest.save(rec)
            return rec


    def _quarantine_and_reset_stage(self, rec: BatchRecord, stage: str, path: Path, reason: str) -> BatchRecord:
        q = None
        try:
            if path.exists():
                q = quarantine_corrupt(path)
        except Exception as exc:
            self.logger.warning("quarantine failed path=%s err=%s", path, exc)
        self.logger.error(
            "batch=%s %s corrupt path=%s reason=%s quarantined=%s",
            rec.batch_id,
            stage,
            path,
            reason,
            q,
        )
        if stage == STAGE_FETCH:
            rec.fetched_rows = 0
            rec.raw_path = str(path)
            rec.set_stage(STAGE_FETCH, STATUS_PENDING, error=f"corrupt raw: {reason}")
            # downstream must redo
            if rec.clean_status in TERMINAL_OK or rec.clean_status == STATUS_FAILED:
                rec.set_stage(STAGE_CLEAN, STATUS_PENDING, error="reset after corrupt raw")
            if rec.load_status in TERMINAL_OK or rec.load_status == STATUS_FAILED:
                rec.set_stage(STAGE_LOAD, STATUS_PENDING, error="reset after corrupt raw")
        elif stage == STAGE_CLEAN:
            rec.cleaned_rows = 0
            rec.cleaned_path = str(path)
            rec.set_stage(STAGE_CLEAN, STATUS_PENDING, error=f"corrupt cleaned: {reason}")
            if rec.load_status in TERMINAL_OK or rec.load_status == STATUS_FAILED:
                rec.set_stage(STAGE_LOAD, STATUS_PENDING, error="reset after corrupt cleaned")
        self.manifest.save(rec)
        return rec

    def _load_parquet_resilient(self, rec: BatchRecord, stage: str, path: Path) -> pd.DataFrame:
        try:
            return load_parquet_or_quarantine(path)
        except CorruptParquetError as exc:
            self._quarantine_and_reset_stage(rec, stage, path, exc.reason)
            raise

    def validate_raw_gate(self, batches: list[Batch]) -> dict:
        """Full raw integrity scan. Returns issues; mutates manifest for corrupt/missing."""
        issues: list[dict[str, Any]] = []
        # Reload latest multi-process state
        self.manifest.reload()
        for batch in batches:
            self._check_control()
            rec = self.manifest.get(batch.batch_id)
            if rec is None:
                issues.append({"batch_id": batch.batch_id, "issue": "fetch_not_terminal", "status": "missing"})
                continue
            if rec.fetch_status == STATUS_FAILED:
                issues.append({"batch_id": batch.batch_id, "issue": "fetch_failed"})
                continue
            if rec.fetch_status not in TERMINAL_OK:
                issues.append(
                    {
                        "batch_id": batch.batch_id,
                        "issue": "fetch_not_terminal",
                        "status": rec.fetch_status,
                    }
                )
                continue
            raw_path = Path(rec.raw_path) if rec.raw_path else raw_file_path(self.raw_dir, batch.batch_id)
            rec.raw_path = str(raw_path)
            if not raw_path.exists():
                issues.append({"batch_id": batch.batch_id, "issue": "raw_missing", "path": str(raw_path)})
                rec.set_stage(STAGE_FETCH, STATUS_PENDING, error="raw missing at gate")
                rec.set_stage(STAGE_CLEAN, STATUS_PENDING, error="reset after raw missing")
                rec.set_stage(STAGE_LOAD, STATUS_PENDING, error="reset after raw missing")
                self.manifest.save(rec)
                continue
            try:
                n = validate_parquet(raw_path)
                prev = int(rec.fetched_rows or 0)
                if prev and abs(prev - n) > max(1, int(0.01 * max(prev, n))):
                    issues.append(
                        {
                            "batch_id": batch.batch_id,
                            "issue": "row_count_mismatch",
                            "manifest_rows": prev,
                            "file_rows": n,
                        }
                    )
                rec.fetched_rows = n
                expected = STATUS_EMPTY if n == 0 else STATUS_SUCCESS
                if rec.fetch_status != expected:
                    rec.set_stage(STAGE_FETCH, expected, finished=True)
                    self.manifest.save(rec)
            except CorruptParquetError as exc:
                self._quarantine_and_reset_stage(rec, STAGE_FETCH, raw_path, exc.reason)
                issues.append({"batch_id": batch.batch_id, "issue": "raw_corrupt", "reason": exc.reason})
        blocking = {
            "raw_missing",
            "raw_corrupt",
            "fetch_failed",
            "fetch_not_terminal",
            "row_count_mismatch",
        }
        blocking_issues = [i for i in issues if i["issue"] in blocking]
        # After mutations, require every batch fetch terminal ok
        self.manifest.reload()
        all_terminal_ok = all(
            (self.manifest.get(b.batch_id) is not None)
            and (self.manifest.get(b.batch_id).fetch_status in TERMINAL_OK)
            for b in batches
        )
        summary = {
            "ok": not blocking_issues and all_terminal_ok,
            "issues": issues,
            "issue_count": len(issues),
            "blocking_issue_count": len(blocking_issues),
        }
        return summary

    def process_batch(self, batch: Batch) -> dict[str, Any]:
        self._check_control()
        rec = self.manifest.get(batch.batch_id)
        if rec is None:
            rec = BatchRecord.from_batch(batch)
            self.manifest.upsert(rec)
            rec = self.manifest.get(batch.batch_id)
        assert rec is not None

        rec.attempts = int(rec.attempts or 0) + 1
        if not rec.started_at:
            rec.started_at = utc_now_iso()
        rec.finished_at = None
        self.manifest.save(rec)

        # Ensure path fields populated for artifact checks
        if not rec.raw_path:
            rec.raw_path = str(raw_file_path(self.raw_dir, batch.batch_id))
        if not rec.cleaned_path:
            rec.cleaned_path = str(cleaned_file_path(self.cleaned_dir, batch.batch_id))

        # Terminal stages must not keep unreadable artifacts forever.
        if STAGE_FETCH in self.stages and rec.fetch_status in TERMINAL_OK:
            raw_check = Path(rec.raw_path)
            if not raw_check.exists():
                rec.set_stage(STAGE_FETCH, STATUS_PENDING, error="raw missing on resume")
                rec.set_stage(STAGE_CLEAN, STATUS_PENDING, error="reset after raw missing")
                rec.set_stage(STAGE_LOAD, STATUS_PENDING, error="reset after raw missing")
                self.manifest.save(rec)
            else:
                try:
                    validate_parquet(raw_check)
                except CorruptParquetError as exc:
                    self._quarantine_and_reset_stage(rec, STAGE_FETCH, raw_check, exc.reason)
                    rec = self.manifest.get(batch.batch_id) or rec
        if STAGE_CLEAN in self.stages and rec.clean_status in TERMINAL_OK:
            cleaned_check = Path(rec.cleaned_path)
            if cleaned_check.exists():
                try:
                    validate_parquet(cleaned_check)
                except CorruptParquetError as exc:
                    self._quarantine_and_reset_stage(rec, STAGE_CLEAN, cleaned_check, exc.reason)
                    rec = self.manifest.get(batch.batch_id) or rec

        # Artifact-driven promotion before stages (resume friendliness)
        raw_p = Path(rec.raw_path)
        if raw_p.exists() and rec.fetch_status not in TERMINAL_OK:
            try:
                df = load_parquet_or_quarantine(raw_p)
                n = len(df)
                rec.fetched_rows = n
                rec.set_stage(STAGE_FETCH, STATUS_EMPTY if n == 0 else STATUS_SUCCESS, finished=True)
                self.manifest.save(rec)
            except CorruptParquetError as exc:
                self._quarantine_and_reset_stage(rec, STAGE_FETCH, raw_p, exc.reason)
            except Exception:
                pass
        cleaned_p = Path(rec.cleaned_path)
        if cleaned_p.exists() and rec.clean_status not in TERMINAL_OK:
            try:
                df = load_parquet_or_quarantine(cleaned_p)
                n = len(df)
                rec.cleaned_rows = n
                rec.set_stage(STAGE_CLEAN, STATUS_EMPTY if n == 0 else STATUS_SUCCESS, finished=True)
                self.manifest.save(rec)
            except CorruptParquetError as exc:
                self._quarantine_and_reset_stage(rec, STAGE_CLEAN, cleaned_p, exc.reason)
            except Exception:
                pass

        # FETCH
        self._check_control()
        if self._needs_stage(rec, STAGE_FETCH):
            rec = self._run_fetch(batch, rec)
            if rec.fetch_status == STATUS_FAILED:
                rec.finished_at = utc_now_iso()
                self.manifest.save(rec)
                return self._result_from_rec(rec)
        elif STAGE_FETCH in self.stages:
            self.logger.info("batch=%s skip FETCH status=%s", batch.batch_id, rec.fetch_status)

        # If fetch empty, short-circuit remaining requested stages
        if rec.fetch_status == STATUS_EMPTY:
            if STAGE_CLEAN in self.stages and rec.clean_status not in TERMINAL_OK:
                rec.cleaned_rows = 0
                rec.set_stage(STAGE_CLEAN, STATUS_EMPTY, finished=True)
            if STAGE_LOAD in self.stages and rec.load_status not in TERMINAL_OK:
                rec.inserted_rows = 0
                rec.set_stage(STAGE_LOAD, STATUS_EMPTY, finished=True)
            rec.finished_at = utc_now_iso()
            self.manifest.save(rec)
            return self._result_from_rec(rec)

        # CLEAN
        self._check_control()
        if self._needs_stage(rec, STAGE_CLEAN):
            # require fetch ok; if not ready skip (fetch may still be in another process)
            if rec.fetch_status not in TERMINAL_OK:
                raw_p = Path(rec.raw_path) if rec.raw_path else raw_file_path(self.raw_dir, batch.batch_id)
                if raw_p.exists():
                    try:
                        n = len(load_parquet(raw_p))
                        rec.fetched_rows = n
                        rec.set_stage(STAGE_FETCH, STATUS_EMPTY if n == 0 else STATUS_SUCCESS, finished=True)
                        self.manifest.save(rec)
                    except Exception:
                        self.logger.info(
                            "batch=%s skip CLEAN; fetch not ready (%s)",
                            batch.batch_id,
                            rec.fetch_status,
                        )
                        rec.finished_at = utc_now_iso()
                        self.manifest.save(rec)
                        return self._result_from_rec(rec)
                else:
                    self.logger.info(
                        "batch=%s skip CLEAN; fetch not ready (%s)",
                        batch.batch_id,
                        rec.fetch_status,
                    )
                    rec.finished_at = utc_now_iso()
                    self.manifest.save(rec)
                    return self._result_from_rec(rec)
            rec = self._run_clean(batch, rec)
            if rec.clean_status == STATUS_FAILED:
                rec.finished_at = utc_now_iso()
                self.manifest.save(rec)
                return self._result_from_rec(rec)
        elif STAGE_CLEAN in self.stages:
            self.logger.info("batch=%s skip CLEAN status=%s", batch.batch_id, rec.clean_status)

        # LOAD
        self._check_control()
        if self._needs_stage(rec, STAGE_LOAD):
            if rec.clean_status not in TERMINAL_OK:
                cleaned_p = Path(rec.cleaned_path) if rec.cleaned_path else cleaned_file_path(self.cleaned_dir, batch.batch_id)
                if cleaned_p.exists():
                    try:
                        n = len(load_parquet(cleaned_p))
                        rec.cleaned_rows = n
                        rec.set_stage(STAGE_CLEAN, STATUS_EMPTY if n == 0 else STATUS_SUCCESS, finished=True)
                        self.manifest.save(rec)
                    except Exception:
                        self.logger.info(
                            "batch=%s skip LOAD; clean not ready (%s)",
                            batch.batch_id,
                            rec.clean_status,
                        )
                        rec.finished_at = utc_now_iso()
                        self.manifest.save(rec)
                        return self._result_from_rec(rec)
                else:
                    self.logger.info(
                        "batch=%s skip LOAD; clean not ready (%s)",
                        batch.batch_id,
                        rec.clean_status,
                    )
                    rec.finished_at = utc_now_iso()
                    self.manifest.save(rec)
                    return self._result_from_rec(rec)
            rec = self._run_load(batch, rec)
        elif STAGE_LOAD in self.stages:
            self.logger.info("batch=%s skip LOAD status=%s", batch.batch_id, rec.load_status)

        rec.finished_at = utc_now_iso()
        self.manifest.save(rec)
        self.logger.info(
            "batch=%s DONE agg=%s fetch=%s clean=%s load=%s f=%s c=%s i=%s",
            batch.batch_id,
            rec.status,
            rec.fetch_status,
            rec.clean_status,
            rec.load_status,
            rec.fetched_rows,
            rec.cleaned_rows,
            rec.inserted_rows,
        )
        return self._result_from_rec(rec)

    def _result_from_rec(self, rec: BatchRecord) -> dict[str, Any]:
        return {
            "batch_id": rec.batch_id,
            "status": rec.status,
            "fetch_status": rec.fetch_status,
            "clean_status": rec.clean_status,
            "load_status": rec.load_status,
            "fetched": rec.fetched_rows,
            "cleaned": rec.cleaned_rows,
            "inserted": rec.inserted_rows,
            "database_write_seconds": float(rec.meta.get("database_write_seconds", 0.0) or 0.0),
            "error": rec.error,
            "raw_path": rec.raw_path,
            "cleaned_path": rec.cleaned_path,
        }

    def run(self) -> dict[str, Any]:
        self._check_control()
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.cleaned_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(
            "start pit_backfill mode=%s stages=%s resume=%s offline_only=%s db=%s interval=%.2fs",
            self.config.mode,
            ",".join(self.stages),
            self.config.resume,
            self.config.offline_only,
            self.db_path,
            self.config.min_interval,
        )

        preflight_info = None
        needs_db = STAGE_LOAD in self.stages or STAGE_CLEAN in self.stages
        if not self.config.skip_preflight and not self.config.dry_run and needs_db:
            preflight_info = preflight(
                self.db_path,
                state_dir=self.state_dir,
                create_backup=False,
                backup_tag=self.config.run_tag,
            )
            self.logger.info(
                "preflight ok free_gb=%s",
                preflight_info["free_gb"],
            )

        batches = self.build_plan()
        summary = self.save_plan(batches)
        self.manifest.ensure_batches(batches)
        if self.config.resume:
            reset_n = self.manifest.reset_running_to_pending(
                stages=self.stages,
                stale_seconds=3600.0,
            )
            if reset_n:
                self.logger.info(
                    "reset %s running records for stages=%s",
                    reset_n,
                    ",".join(self.stages),
                )

        self.logger.info("plan %s", summary)

        raw_gate = None
        backup_info = None
        # Clean/load workers (no fetch stage) must pass full raw integrity + DB backup
        # before any load. Same-process fetch+load skips the full pre-gate because
        # each fetch already validates parquet on write; post-run audit still applies.
        if (
            STAGE_LOAD in self.stages
            and STAGE_FETCH not in self.stages
            and self.config.mode not in {"plan-only"}
            and not self.config.dry_run
        ):
            raw_gate = self.validate_raw_gate(batches)
            self.logger.info("raw gate ok=%s issues=%s", raw_gate["ok"], raw_gate["issue_count"])
            if not raw_gate["ok"]:
                if not self.config.offline_only:
                    self.logger.warning("raw gate failed; re-fetching broken batches before load")
                    issue_ids = {i["batch_id"] for i in raw_gate["issues"]}
                    old_stages = self.stages
                    self.stages = (STAGE_FETCH,)
                    try:
                        for batch in batches:
                            if batch.batch_id not in issue_ids:
                                continue
                            self.process_batch(batch)
                    finally:
                        self.stages = old_stages
                    raw_gate = self.validate_raw_gate(batches)
                    self.logger.info(
                        "raw gate after refetch ok=%s issues=%s",
                        raw_gate["ok"],
                        raw_gate["issue_count"],
                    )
                if not raw_gate["ok"]:
                    raise RuntimeError(
                        f"raw integrity gate failed with {raw_gate['issue_count']} issues; refuse load"
                    )

            # Backup gate before any load (marker reuses an existing verified backup)
            if self.config.create_backup:
                backup_info = ensure_load_backup(
                    self.db_path,
                    state_dir=self.state_dir,
                    tag=self.config.run_tag,
                )
                self.logger.info(
                    "load backup ready path=%s size=%s reused=%s",
                    backup_info["backup_path"],
                    backup_info.get("backup_size_bytes"),
                    backup_info["reused"],
                )
            else:
                # Still require a verified marker/backup from an earlier pass
                from qrp_atlas.pipeline.pit_backfill.safety import load_backup_marker, assert_db_readable

                marker = load_backup_marker(self.state_dir)
                if not marker or not marker.get("backup_path"):
                    raise RuntimeError("refusing load without backup (create_backup=False and no marker)")
                bp = Path(marker["backup_path"])
                if not bp.exists():
                    raise RuntimeError(f"refusing load; backup marker path missing: {bp}")
                assert_db_readable(bp)
                backup_info = {
                    "backup_path": str(bp),
                    "marker_path": str(self.state_dir / "backup_marker.json"),
                    "backup_size_bytes": bp.stat().st_size,
                    "reused": True,
                }
                self.logger.info("load backup reused via marker path=%s", backup_info["backup_path"])

        if self.config.mode == "plan-only" or self.config.dry_run:
            return {
                "mode": self.config.mode,
                "summary": summary,
                "stages": list(self.stages),
                "plan_path": str(self.plan_path),
                "manifest_path": str(self.manifest_path),
                "preflight": preflight_info,
                "request_count": self.request_count,
                "ok": True,
            }

        results: list[dict[str, Any]] = []
        # DB lock only required when loading; clean may open calendar read-only.
        if STAGE_LOAD in self.stages:
            lock_cm = pipeline_db_lock(self.config.lock_path)
        else:
            from contextlib import nullcontext

            lock_cm = nullcontext()

        with lock_cm:
            for batch in batches:
                self._check_control()
                # Revalidate terminal artifacts before skip decision so corrupt/missing
                # raw or cleaned files are reopened even when stage status is success.
                rec = self.manifest.get(batch.batch_id)
                if rec is not None and self.config.resume:
                    if STAGE_FETCH in self.stages and rec.fetch_status in TERMINAL_OK:
                        raw_check = Path(rec.raw_path) if rec.raw_path else raw_file_path(self.raw_dir, batch.batch_id)
                        if not raw_check.exists():
                            rec.set_stage(STAGE_FETCH, STATUS_PENDING, error="raw missing on resume")
                            rec.set_stage(STAGE_CLEAN, STATUS_PENDING, error="reset after raw missing")
                            rec.set_stage(STAGE_LOAD, STATUS_PENDING, error="reset after raw missing")
                            self.manifest.save(rec)
                        else:
                            try:
                                validate_parquet(raw_check)
                            except CorruptParquetError as exc:
                                self._quarantine_and_reset_stage(rec, STAGE_FETCH, raw_check, exc.reason)
                    rec = self.manifest.get(batch.batch_id)
                    if rec is not None and STAGE_CLEAN in self.stages and rec.clean_status in TERMINAL_OK:
                        cleaned_check = Path(rec.cleaned_path) if rec.cleaned_path else cleaned_file_path(self.cleaned_dir, batch.batch_id)
                        if cleaned_check.exists():
                            try:
                                validate_parquet(cleaned_check)
                            except CorruptParquetError as exc:
                                self._quarantine_and_reset_stage(rec, STAGE_CLEAN, cleaned_check, exc.reason)

                if not self.manifest.should_process(
                    batch.batch_id, resume=self.config.resume, stages=self.stages
                ):
                    rec = self.manifest.get(batch.batch_id)
                    self.logger.info(
                        "skip batch=%s status=%s fetch=%s clean=%s load=%s",
                        batch.batch_id,
                        rec.status if rec else "?",
                        rec.fetch_status if rec else "?",
                        rec.clean_status if rec else "?",
                        rec.load_status if rec else "?",
                    )
                    continue
                results.append(self.process_batch(batch))
                self._check_control()

        counts = self.manifest.counts()
        stage_counts = self.manifest.stage_counts()
        totals = {
            "fetched_rows": sum(int(r.get("fetched", 0) or 0) for r in results),
            "cleaned_rows": sum(int(r.get("cleaned", 0) or 0) for r in results),
            "inserted_rows": sum(int(r.get("inserted", 0) or 0) for r in results),
            "processed": len(results),
            "database_write_seconds": sum(
                float(r.get("database_write_seconds", 0.0) or 0.0) for r in results
            ),
        }
        out: dict[str, Any] = {
            "mode": self.config.mode,
            "stages": list(self.stages),
            "summary": summary,
            "counts": counts,
            "stage_counts": stage_counts,
            "totals": totals,
            "request_count": self.request_count,
            "results": results,
            "paths": {
                "raw_dir": str(self.raw_dir),
                "cleaned_dir": str(self.cleaned_dir),
                "state_dir": str(self.state_dir),
                "log_path": str(self.log_path),
                "manifest_path": str(self.manifest_path),
                "plan_path": str(self.plan_path),
                "db_path": str(self.db_path),
            },
            "preflight": preflight_info,
            "raw_gate": raw_gate,
            "backup": backup_info,
        }

        if self.config.run_audit:
            from qrp_atlas.pipeline.pit_backfill.audit import run_full_audit

            out["audit"] = run_full_audit(self.db_path)
            audit_path = self.state_dir / "audit.json"
            audit_path.write_text(
                json.dumps(out["audit"], ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            self.logger.info("audit written %s", audit_path)

        failed = counts.get(STATUS_FAILED, 0)
        stage_failed = 0
        for st in stage_counts.values():
            stage_failed += int(st.get(STATUS_FAILED, 0))
        self.logger.info(
            "finished counts=%s stage_counts=%s requests=%s",
            counts,
            stage_counts,
            self.request_count,
        )
        out["ok"] = failed == 0 and stage_failed == 0
        out["exit_nonzero_reason"] = None if out["ok"] else "failed_batches_or_stages"
        return out


def run_backfill(**kwargs) -> dict[str, Any]:
    cfg = BackfillConfig(**kwargs)
    return PitBackfillRunner(cfg).run()
