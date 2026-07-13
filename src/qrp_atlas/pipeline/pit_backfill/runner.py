"""Orchestrate PIT historical backfill over existing pipelines.

Stages are decoupled:
  fetch  -> raw parquet
  clean  -> cleaned parquet (reuse existing clean_*)
  load   -> duckdb append-only (reuse existing load_*)
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from qrp_atlas.config import DB_PATH, PROJECT_ROOT, ensure_dirs
from qrp_atlas.pipeline.fundamentals.clean import clean_financial
from qrp_atlas.pipeline.fundamentals.fetch import fetch_financial_by_period
from qrp_atlas.pipeline.fundamentals.load_duckdb import load_financial
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
    cleaned_file_path,
    load_parquet,
    raw_file_path,
    save_parquet,
)
from qrp_atlas.pipeline.pit_backfill.safety import pipeline_db_lock, preflight
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver

RUN_TAG = "20260714"
LOGGER_NAME = "qrp_atlas.pipeline.pit_backfill"


def default_paths(run_tag: str = RUN_TAG) -> dict[str, Path]:
    data = PROJECT_ROOT / "data"
    return {
        "raw_dir": data / "raw" / "pit_backfill" / run_tag,
        "cleaned_dir": data / "canonical" / "pit_backfill" / run_tag,
        "state_dir": data / "state" / f"pit_backfill_{run_tag}",
        "log_path": data / "logs" / f"pit_backfill_{run_tag}.log",
        "manifest_path": data / "state" / f"pit_backfill_{run_tag}" / "manifest.jsonl",
        "plan_path": data / "state" / f"pit_backfill_{run_tag}" / "plan.json",
        "db_path": Path(DB_PATH),
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
    token = os.getenv("TUSHARE_TOKEN") or ""
    if token and token in text:
        text = text.replace(token, "***")
    lowered = text.lower()
    if any(k in lowered for k in ("token=", "authorization", "bearer ")):
        text = text[:200] + " ...[redacted]"
    return text[:2000]


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

    def __init__(self, pro: Any, limiter: RateLimiter):
        self._pro = pro
        self._limiter = limiter

    def __getattr__(self, name: str):
        attr = getattr(self._pro, name)
        if not callable(attr):
            return attr

        def wrapped(*args, **kwargs):
            import time

            try:
                return self._limiter.call(attr, *args, **kwargs)
            except Exception as exc:
                if is_rate_limit_error(exc):
                    time.sleep(30.0)
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


class PitBackfillRunner:
    def __init__(self, config: BackfillConfig):
        self.config = config
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
            self._client = RateLimitedPro(self._base_client, self.limiter)
            return self._client
        from qrp_atlas.config import get_tushare_pro

        self._client = RateLimitedPro(get_tushare_pro(), self.limiter)
        return self._client

    def build_plan(self) -> list[Batch]:
        cfg = self.config
        if cfg.mode == "precheck":
            l1 = list(cfg.l1_codes)[0] if cfg.l1_codes else None
            return precheck_batches(l1_code=l1)

        batches: list[Batch] = []
        if "fundamentals" in cfg.datasets:
            batches.extend(financial_batches())
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
        client = self._get_client()
        if batch.dataset == "fundamentals":
            df = fetch_financial_by_period(batch.key, batch.period, client=client)
            return df if df is not None else pd.DataFrame()
        if batch.dataset == "industry":
            df = fetch_industry_membership(l1_code=batch.key, is_new=None, client=client)
            return df if df is not None else pd.DataFrame()
        if batch.dataset == "index":
            df = fetch_index_weight(
                batch.key,
                start_date=batch.start_date,
                end_date=batch.end_date,
                client=client,
            )
            return df if df is not None else pd.DataFrame()
        raise ValueError(f"unknown dataset: {batch.dataset}")

    def _clean_df(self, batch: Batch, raw: pd.DataFrame) -> pd.DataFrame:
        resolver = self._ensure_resolver()
        if raw is None or raw.empty:
            return pd.DataFrame()
        if batch.dataset == "fundamentals":
            return clean_financial(raw, batch.key, trade_date_resolver=resolver)
        if batch.dataset == "industry":
            return clean_industry_membership(raw, trade_date_resolver=resolver)
        if batch.dataset == "index":
            return clean_index_component(raw, trade_date_resolver=resolver)
        raise ValueError(f"unknown dataset: {batch.dataset}")

    def _load_df(self, batch: Batch, cleaned: pd.DataFrame) -> int:
        if cleaned is None or cleaned.empty:
            return 0
        if batch.dataset == "fundamentals":
            return int(load_financial(cleaned, batch.key, db_path=self.db_path, init=True))
        if batch.dataset == "industry":
            return int(load_industry_membership(cleaned, db_path=self.db_path, init=True))
        if batch.dataset == "index":
            return int(load_index_component(cleaned, db_path=self.db_path, init=True))
        raise ValueError(f"unknown dataset: {batch.dataset}")

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
        raw_path = raw_file_path(self.raw_dir, batch.batch_id)
        rec.raw_path = str(raw_path)
        rec.set_stage(STAGE_FETCH, STATUS_RUNNING, error=None)
        self.manifest.save(rec)
        try:
            if raw_path.exists():
                self.logger.info("batch=%s FETCH offline raw=%s", batch.batch_id, raw_path)
                raw = load_parquet(raw_path)
                offline = True
            else:
                if self.config.offline_only:
                    raise FileNotFoundError(f"offline_only set but raw missing: {raw_path}")
                self.logger.info("batch=%s FETCH network", batch.batch_id)
                raw = self._fetch_from_api(batch)
                save_parquet(raw, raw_path)
                offline = False
            fetched = 0 if raw is None else len(raw)
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
                cleaned = load_parquet(cleaned_path)
            else:
                raw = load_parquet(raw_path)
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
        except Exception as exc:
            err = _sanitize_error(exc)
            self.logger.error("batch=%s CLEAN FAILED %s", batch.batch_id, err)
            self.logger.debug("traceback:\n%s", traceback.format_exc())
            rec.set_stage(STAGE_CLEAN, STATUS_FAILED, error=err, finished=True)
            self.manifest.save(rec)
            return rec

    def _run_load(self, batch: Batch, rec: BatchRecord) -> BatchRecord:
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
            cleaned = load_parquet(cleaned_path)
            inserted = self._load_df(batch, cleaned)
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
        except Exception as exc:
            err = _sanitize_error(exc)
            self.logger.error("batch=%s LOAD FAILED %s", batch.batch_id, err)
            self.logger.debug("traceback:\n%s", traceback.format_exc())
            rec.set_stage(STAGE_LOAD, STATUS_FAILED, error=err, finished=True)
            self.manifest.save(rec)
            return rec

    def process_batch(self, batch: Batch) -> dict[str, Any]:
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

        # Artifact-driven promotion before stages (resume friendliness)
        raw_p = Path(rec.raw_path)
        if raw_p.exists() and rec.fetch_status not in TERMINAL_OK:
            try:
                n = len(load_parquet(raw_p))
                rec.fetched_rows = n
                rec.set_stage(STAGE_FETCH, STATUS_EMPTY if n == 0 else STATUS_SUCCESS, finished=True)
                self.manifest.save(rec)
            except Exception:
                pass
        cleaned_p = Path(rec.cleaned_path)
        if cleaned_p.exists() and rec.clean_status not in TERMINAL_OK:
            try:
                n = len(load_parquet(cleaned_p))
                rec.cleaned_rows = n
                rec.set_stage(STAGE_CLEAN, STATUS_EMPTY if n == 0 else STATUS_SUCCESS, finished=True)
                self.manifest.save(rec)
            except Exception:
                pass

        # FETCH
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
            "error": rec.error,
            "raw_path": rec.raw_path,
            "cleaned_path": rec.cleaned_path,
        }

    def run(self) -> dict[str, Any]:
        ensure_dirs()
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
                create_backup=self.config.create_backup and not self.config.resume,
                backup_tag=self.config.run_tag,
            )
            self.logger.info(
                "preflight ok free_gb=%s backup=%s",
                preflight_info["free_gb"],
                preflight_info["backup_path"],
            )

        batches = self.build_plan()
        summary = self.save_plan(batches)
        self.manifest.ensure_batches(batches)
        if self.config.resume:
            reset_n = self.manifest.reset_running_to_pending()
            if reset_n:
                self.logger.info("reset %s running batches/stages to pending", reset_n)

        self.logger.info("plan %s", summary)

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
            lock_cm = pipeline_db_lock()
        else:
            from contextlib import nullcontext

            lock_cm = nullcontext()

        with lock_cm:
            for batch in batches:
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

        counts = self.manifest.counts()
        stage_counts = self.manifest.stage_counts()
        totals = {
            "fetched_rows": sum(int(r.get("fetched", 0) or 0) for r in results),
            "cleaned_rows": sum(int(r.get("cleaned", 0) or 0) for r in results),
            "inserted_rows": sum(int(r.get("inserted", 0) or 0) for r in results),
            "processed": len(results),
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
        self.logger.info(
            "finished counts=%s stage_counts=%s requests=%s",
            counts,
            stage_counts,
            self.request_count,
        )
        out["ok"] = failed == 0
        return out


def run_backfill(**kwargs) -> dict[str, Any]:
    cfg = BackfillConfig(**kwargs)
    return PitBackfillRunner(cfg).run()
