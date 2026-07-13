"""Orchestrate PIT historical backfill over existing pipelines."""

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
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    BatchRecord,
    ManifestStore,
    utc_now_iso,
)
from qrp_atlas.pipeline.pit_backfill.rate_limit import (
    DEFAULT_MIN_INTERVAL,
    RateLimiter,
    is_rate_limit_error,
)
from qrp_atlas.pipeline.pit_backfill.raw_io import load_raw_parquet, raw_file_path, save_raw_parquet
from qrp_atlas.pipeline.pit_backfill.safety import pipeline_db_lock, preflight
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver

RUN_TAG = "20260714"
LOGGER_NAME = "qrp_atlas.pipeline.pit_backfill"


def default_paths(run_tag: str = RUN_TAG) -> dict[str, Path]:
    data = PROJECT_ROOT / "data"
    return {
        "raw_dir": data / "raw" / "pit_backfill" / run_tag,
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
            # Pace every attempt; on rate-limit errors sleep longer then raise
            # so upstream fetch retry loops also stay single-threaded / paced.
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
    resume: bool = False
    db_path: str | Path | None = None
    raw_dir: str | Path | None = None
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


class PitBackfillRunner:
    def __init__(self, config: BackfillConfig):
        self.config = config
        paths = default_paths(config.run_tag)
        self.db_path = Path(config.db_path or paths["db_path"])
        self.raw_dir = Path(config.raw_dir or paths["raw_dir"])
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
        self._request_count_before = 0

    @property
    def request_count(self) -> int:
        return self.limiter.call_count

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        if self._base_client is not None:
            # If caller already provided a client (e.g. FakePro), still wrap for pacing in tests optionally.
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
            elif codes_path.exists() and cfg.resume:
                l1_codes = json.loads(codes_path.read_text(encoding="utf-8"))
                self.logger.info("Loaded %s L1 codes from cache", len(l1_codes))
            else:
                self.logger.info("Discovering SW2021 L1 industry codes via index_classify")
                if cfg.dry_run:
                    l1_codes = []
                else:
                    # discover under rate-limited client
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

    def _fetch_raw(self, batch: Batch) -> pd.DataFrame:
        client = self._get_client()
        if batch.dataset == "fundamentals":
            df = fetch_financial_by_period(batch.key, batch.period, client=client)
            return df if df is not None else pd.DataFrame()
        if batch.dataset == "industry":
            # is_new omitted on purpose: need current + historical exits
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

    def _clean_and_load(self, batch: Batch, raw: pd.DataFrame) -> tuple[int, int]:
        resolver = self._ensure_resolver()
        if raw is None or raw.empty:
            return 0, 0
        if batch.dataset == "fundamentals":
            cleaned = clean_financial(raw, batch.key, trade_date_resolver=resolver)
            inserted = load_financial(cleaned, batch.key, db_path=self.db_path, init=True)
            return len(cleaned), int(inserted)
        if batch.dataset == "industry":
            cleaned = clean_industry_membership(raw, trade_date_resolver=resolver)
            inserted = load_industry_membership(cleaned, db_path=self.db_path, init=True)
            return len(cleaned), int(inserted)
        if batch.dataset == "index":
            cleaned = clean_index_component(raw, trade_date_resolver=resolver)
            inserted = load_index_component(cleaned, db_path=self.db_path, init=True)
            return len(cleaned), int(inserted)
        raise ValueError(f"unknown dataset: {batch.dataset}")

    def process_batch(self, batch: Batch) -> dict[str, Any]:
        if self.manifest.get(batch.batch_id) is None:
            self.manifest.upsert(BatchRecord.from_batch(batch))

        prev = self.manifest.get(batch.batch_id)
        attempts = (prev.attempts if prev else 0) + 1
        self.manifest.update(
            batch.batch_id,
            status=STATUS_RUNNING,
            started_at=utc_now_iso(),
            finished_at=None,
            error=None,
            attempts=attempts,
        )

        raw_path = raw_file_path(self.raw_dir, batch.batch_id)
        used_offline = False
        try:
            if raw_path.exists():
                self.logger.info("batch=%s offline raw=%s", batch.batch_id, raw_path)
                raw = load_raw_parquet(raw_path)
                used_offline = True
            else:
                self.logger.info("batch=%s fetching", batch.batch_id)
                raw = self._fetch_raw(batch)
                save_raw_parquet(raw, raw_path)

            fetched = 0 if raw is None else len(raw)
            if fetched == 0:
                # keep empty parquet for resume offline path
                if not raw_path.exists():
                    save_raw_parquet(pd.DataFrame(), raw_path)
                self.manifest.update(
                    batch.batch_id,
                    status=STATUS_EMPTY,
                    fetched_rows=0,
                    cleaned_rows=0,
                    inserted_rows=0,
                    raw_path=str(raw_path),
                    finished_at=utc_now_iso(),
                    error=None,
                )
                self.logger.info("batch=%s EMPTY", batch.batch_id)
                return {
                    "batch_id": batch.batch_id,
                    "status": STATUS_EMPTY,
                    "fetched": 0,
                    "cleaned": 0,
                    "inserted": 0,
                    "offline": used_offline,
                }

            cleaned_n, inserted_n = self._clean_and_load(batch, raw)
            self.manifest.update(
                batch.batch_id,
                status=STATUS_SUCCESS,
                fetched_rows=fetched,
                cleaned_rows=cleaned_n,
                inserted_rows=inserted_n,
                raw_path=str(raw_path),
                finished_at=utc_now_iso(),
                error=None,
            )
            self.logger.info(
                "batch=%s SUCCESS fetched=%s cleaned=%s inserted=%s offline=%s",
                batch.batch_id,
                fetched,
                cleaned_n,
                inserted_n,
                used_offline,
            )
            return {
                "batch_id": batch.batch_id,
                "status": STATUS_SUCCESS,
                "fetched": fetched,
                "cleaned": cleaned_n,
                "inserted": inserted_n,
                "offline": used_offline,
            }
        except Exception as exc:
            err = _sanitize_error(exc)
            if is_rate_limit_error(exc):
                self.logger.warning("batch=%s rate-limited: %s", batch.batch_id, err)
            self.logger.error("batch=%s FAILED attempts=%s err=%s", batch.batch_id, attempts, err)
            self.logger.debug("traceback:\n%s", traceback.format_exc())
            self.manifest.update(
                batch.batch_id,
                status=STATUS_FAILED,
                raw_path=str(raw_path) if raw_path.exists() else None,
                finished_at=utc_now_iso(),
                error=err,
            )
            return {
                "batch_id": batch.batch_id,
                "status": STATUS_FAILED,
                "error": err,
                "offline": used_offline,
            }

    def run(self) -> dict[str, Any]:
        ensure_dirs()
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(
            "start pit_backfill mode=%s resume=%s db=%s interval=%.2fs",
            self.config.mode,
            self.config.resume,
            self.db_path,
            self.config.min_interval,
        )

        preflight_info = None
        if not self.config.skip_preflight and not self.config.dry_run:
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
                self.logger.info("reset %s running batches to pending", reset_n)

        self.logger.info("plan %s", summary)

        if self.config.mode == "plan-only" or self.config.dry_run:
            return {
                "mode": self.config.mode,
                "summary": summary,
                "plan_path": str(self.plan_path),
                "manifest_path": str(self.manifest_path),
                "preflight": preflight_info,
                "request_count": self.request_count,
                "ok": True,
            }

        results: list[dict[str, Any]] = []
        with pipeline_db_lock():
            for batch in batches:
                if not self.manifest.should_process(batch.batch_id, resume=self.config.resume):
                    rec = self.manifest.get(batch.batch_id)
                    self.logger.info(
                        "skip batch=%s status=%s",
                        batch.batch_id,
                        rec.status if rec else "?",
                    )
                    continue
                results.append(self.process_batch(batch))

        counts = self.manifest.counts()
        totals = {
            "fetched_rows": sum(int(r.get("fetched", 0) or 0) for r in results),
            "cleaned_rows": sum(int(r.get("cleaned", 0) or 0) for r in results),
            "inserted_rows": sum(int(r.get("inserted", 0) or 0) for r in results),
            "processed": len(results),
        }
        out: dict[str, Any] = {
            "mode": self.config.mode,
            "summary": summary,
            "counts": counts,
            "totals": totals,
            "request_count": self.request_count,
            "results": results,
            "paths": {
                "raw_dir": str(self.raw_dir),
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
        self.logger.info("finished counts=%s requests=%s", counts, self.request_count)
        out["ok"] = failed == 0
        return out


def run_backfill(**kwargs) -> dict[str, Any]:
    cfg = BackfillConfig(**kwargs)
    return PitBackfillRunner(cfg).run()
