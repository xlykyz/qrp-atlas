"""CLI / programmatic entry for earnings forecast event pipeline.

Examples:
  # period bulk (forecast_vip) small verification — auto raw/manifest
  python -m qrp_atlas.pipeline.earnings_forecast.run \\
    --mode period --periods 20231231 --tickers 000001.SZ

  # single ticker debug (forecast)
  python -m qrp_atlas.pipeline.earnings_forecast.run \\
    --mode ticker --tickers 000001.SZ --start-date 20200101 --end-date 20261231

  # announcement-date incremental candidate
  python -m qrp_atlas.pipeline.earnings_forecast.run \\
    --mode ann_date --ann-dates 20240131

  # offline resume from raw parquet
  python -m qrp_atlas.pipeline.earnings_forecast.run \\
    --from-raw data/raw/earnings_forecast/.../batch.parquet
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from qrp_atlas.config import CANONICAL_DIR, DATA_DIR, RAW_DIR, ensure_dirs
from qrp_atlas.pipeline.earnings_forecast.clean import (
    EarningsForecastDataQualityError,
    clean_earnings_forecast,
)
from qrp_atlas.pipeline.earnings_forecast.fetch import (
    ENDPOINT_FORECAST,
    ENDPOINT_FORECAST_VIP,
    ForecastApiError,
    ForecastPermissionError,
    fetch_earnings_forecast,
)
from qrp_atlas.pipeline.earnings_forecast.load_duckdb import load_earnings_forecast
from qrp_atlas.pipeline.pit_backfill.manifest import (
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    BatchRecord,
    ManifestStore,
    utc_now_iso,
)
from qrp_atlas.pipeline.pit_backfill.raw_io import (
    cleaned_file_path,
    load_parquet,
    raw_file_path,
    save_parquet,
)
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver

DATASET = "earnings_forecast"
DEFAULT_RUN_TAG = "earnings_forecast"


def _parse_list(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [x.strip() for x in raw.split(",") if x.strip()]


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def default_artifact_dirs(run_tag: str = DEFAULT_RUN_TAG) -> dict[str, Path]:
    ensure_dirs()
    return {
        "raw_dir": RAW_DIR / DATASET / run_tag,
        "cleaned_dir": CANONICAL_DIR / DATASET / run_tag,
        "state_dir": DATA_DIR / "state" / DATASET / run_tag,
    }


def build_batch_id(
    *,
    mode: str,
    periods: Sequence[str] | None = None,
    tickers: Sequence[str] | None = None,
    ann_dates: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    stamp: str | None = None,
) -> str:
    """Deterministic-ish batch id with timestamp for unique real-fetch runs."""
    stamp = stamp or _utc_stamp()
    parts = [DATASET, mode, stamp]
    if periods:
        parts.append("p" + "-".join(periods[:4]) + (f"+{len(periods)-4}" if len(periods) > 4 else ""))
    if tickers:
        parts.append("t" + "-".join(tickers[:3]) + (f"+{len(tickers)-3}" if len(tickers) > 3 else ""))
    if ann_dates:
        parts.append("a" + "-".join(ann_dates[:3]) + (f"+{len(ann_dates)-3}" if len(ann_dates) > 3 else ""))
    if start_date:
        parts.append(f"s{start_date}")
    if end_date:
        parts.append(f"e{end_date}")
    return ":".join(parts)


def _endpoint_for_mode(mode: str) -> str:
    return ENDPOINT_FORECAST_VIP if mode == "period" else ENDPOINT_FORECAST


def _request_params(
    *,
    mode: str,
    periods: Sequence[str] | None,
    tickers: Sequence[str] | None,
    ann_dates: Sequence[str] | None,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "periods": list(periods) if periods else None,
        "tickers": list(tickers) if tickers else None,
        "ann_dates": list(ann_dates) if ann_dates else None,
        "start_date": start_date,
        "end_date": end_date,
    }


def _write_manifest_record(store: ManifestStore | None, rec: BatchRecord | None) -> None:
    if store is not None and rec is not None:
        store.save(rec)


def _result_from_record(rec: BatchRecord, *, ok: bool) -> dict[str, Any]:
    meta = dict(rec.meta or {})
    return {
        "ok": ok,
        "batch_id": rec.batch_id,
        "mode": meta.get("mode"),
        "endpoint": meta.get("endpoint"),
        "request": meta.get("request"),
        "status": rec.status,
        "fetch_status": rec.fetch_status,
        "clean_status": rec.clean_status,
        "load_status": rec.load_status,
        "fetched": int(rec.fetched_rows or 0),
        "cleaned": int(rec.cleaned_rows or 0),
        "invalid_rows": int(meta.get("invalid_rows") or 0),
        "inserted": int(rec.inserted_rows or 0),
        "raw_path": rec.raw_path,
        "cleaned_path": rec.cleaned_path,
        "manifest_path": meta.get("manifest_path"),
        "error": rec.error,
        "error_type": meta.get("error_type"),
    }


def run_earnings_forecast(
    *,
    mode: str = "period",
    periods: Sequence[str] | None = None,
    tickers: Sequence[str] | None = None,
    ann_dates: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
    db_path: str | None = None,
    resolver: NextTradeDateResolver | None = None,
    init_db: bool = True,
    raw_path: str | Path | None = None,
    cleaned_path: str | Path | None = None,
    load: bool = True,
    run_tag: str = DEFAULT_RUN_TAG,
    batch_id: str | None = None,
    state_dir: str | Path | None = None,
) -> dict:
    """Fetch → force raw archive → clean → optional load, with manifest audit."""
    endpoint = _endpoint_for_mode(mode)
    request = _request_params(
        mode=mode,
        periods=periods,
        tickers=tickers,
        ann_dates=ann_dates,
        start_date=start_date,
        end_date=end_date,
    )
    print(
        f"[EARNINGS_FORECAST] mode={mode} endpoint={endpoint} "
        f"periods={periods} tickers={tickers} ann_dates={ann_dates}"
    )

    dirs = default_artifact_dirs(run_tag)
    raw_dir = dirs["raw_dir"]
    cleaned_dir = dirs["cleaned_dir"]
    state_path = Path(state_dir) if state_dir is not None else dirs["state_dir"]
    state_path.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    cleaned_dir.mkdir(parents=True, exist_ok=True)

    batch_id = batch_id or build_batch_id(
        mode=mode,
        periods=periods,
        tickers=tickers,
        ann_dates=ann_dates,
        start_date=start_date,
        end_date=end_date,
    )
    if raw_path is None:
        raw_path = raw_file_path(raw_dir, batch_id)
    if cleaned_path is None:
        cleaned_path = cleaned_file_path(cleaned_dir, batch_id)
    raw_path = Path(raw_path)
    cleaned_path = Path(cleaned_path)
    manifest_path = state_path / "manifest.jsonl"
    store = ManifestStore(manifest_path)

    rec = BatchRecord(
        batch_id=batch_id,
        dataset=DATASET,
        key=mode,
        period=",".join(periods) if periods else None,
        start_date=start_date,
        end_date=end_date,
        raw_path=str(raw_path),
        cleaned_path=str(cleaned_path),
        started_at=utc_now_iso(),
        meta={
            "mode": mode,
            "endpoint": endpoint,
            "request": request,
            "manifest_path": str(manifest_path),
            "invalid_rows": 0,
        },
    )
    rec.set_stage("fetch", STATUS_RUNNING, error=None)
    store.save(rec)

    if resolver is None and db_path is not None:
        resolver = NextTradeDateResolver(db_path=db_path)

    # ---- FETCH ----
    try:
        raw = fetch_earnings_forecast(
            mode=mode,
            periods=periods,
            tickers=tickers,
            ann_dates=ann_dates,
            start_date=start_date,
            end_date=end_date,
            client=client,
        )
    except ForecastPermissionError as exc:
        rec.meta["error_type"] = "permission"
        rec.set_stage("fetch", STATUS_FAILED, error=str(exc), finished=True)
        rec.finished_at = utc_now_iso()
        store.save(rec)
        result = _result_from_record(rec, ok=False)
        print(json.dumps(result, ensure_ascii=False))
        return result
    except (ForecastApiError, Exception) as exc:
        rec.meta["error_type"] = "api" if isinstance(exc, ForecastApiError) else "fetch"
        rec.set_stage("fetch", STATUS_FAILED, error=str(exc), finished=True)
        rec.finished_at = utc_now_iso()
        store.save(rec)
        result = _result_from_record(rec, ok=False)
        print(json.dumps(result, ensure_ascii=False))
        return result

    fetched = 0 if raw is None else int(len(raw))
    rec.fetched_rows = fetched
    try:
        # Always persist raw evidence for real fetches before clean/load.
        save_parquet(raw if raw is not None else pd.DataFrame(), raw_path)
        rec.raw_path = str(raw_path)
    except Exception as exc:
        rec.meta["error_type"] = "raw_io"
        rec.set_stage("fetch", STATUS_FAILED, error=f"raw save failed: {exc}", finished=True)
        rec.finished_at = utc_now_iso()
        store.save(rec)
        return _result_from_record(rec, ok=False)

    if fetched == 0:
        rec.set_stage("fetch", STATUS_EMPTY, error=None, finished=True)
        rec.set_stage("clean", STATUS_EMPTY, error=None, finished=True)
        if load:
            try:
                load_earnings_forecast(pd.DataFrame(), db_path=db_path, init=init_db)
                rec.set_stage("load", STATUS_EMPTY, error=None, finished=True)
            except Exception as exc:
                rec.meta["error_type"] = "load"
                rec.set_stage("load", STATUS_FAILED, error=str(exc), finished=True)
                rec.finished_at = utc_now_iso()
                store.save(rec)
                return _result_from_record(rec, ok=False)
        else:
            rec.set_stage("load", STATUS_EMPTY, error=None, finished=True)
        rec.finished_at = utc_now_iso()
        store.save(rec)
        result = _result_from_record(rec, ok=True)
        print(f"[EARNINGS_FORECAST] fetched=0 batch_id={batch_id}")
        return result

    rec.set_stage("fetch", STATUS_SUCCESS, error=None, finished=True)
    store.save(rec)
    print(f"[EARNINGS_FORECAST] fetched={fetched} raw={raw_path}")

    # ---- CLEAN ----
    rec.set_stage("clean", STATUS_RUNNING, error=None)
    store.save(rec)
    try:
        cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver)
        invalid_rows = int(getattr(cleaned, "attrs", {}).get("invalid_rows", 0) or 0)
        rec.meta["invalid_rows"] = invalid_rows
        rec.cleaned_rows = int(len(cleaned))
        save_parquet(cleaned, cleaned_path)
        rec.cleaned_path = str(cleaned_path)
        rec.set_stage(
            "clean",
            STATUS_SUCCESS if rec.cleaned_rows > 0 else STATUS_EMPTY,
            error=None,
            finished=True,
        )
        store.save(rec)
        print(f"[EARNINGS_FORECAST] cleaned={rec.cleaned_rows} cleaned_path={cleaned_path}")
    except EarningsForecastDataQualityError as exc:
        rec.meta["error_type"] = "data_quality"
        # best-effort: if clean partially counted invalids via exception message
        rec.meta["invalid_rows"] = rec.meta.get("invalid_rows") or fetched
        rec.set_stage("clean", STATUS_FAILED, error=str(exc), finished=True)
        rec.set_stage("load", STATUS_PENDING, error=None)
        rec.finished_at = utc_now_iso()
        store.save(rec)
        result = _result_from_record(rec, ok=False)
        print(json.dumps(result, ensure_ascii=False))
        return result
    except Exception as exc:
        rec.meta["error_type"] = "clean"
        rec.set_stage("clean", STATUS_FAILED, error=str(exc), finished=True)
        rec.finished_at = utc_now_iso()
        store.save(rec)
        return _result_from_record(rec, ok=False)

    # ---- LOAD ----
    inserted = 0
    if load:
        rec.set_stage("load", STATUS_RUNNING, error=None)
        store.save(rec)
        try:
            inserted = int(load_earnings_forecast(cleaned, db_path=db_path, init=init_db))
            rec.inserted_rows = inserted
            rec.set_stage("load", STATUS_SUCCESS, error=None, finished=True)
            print(f"[EARNINGS_FORECAST] inserted={inserted}")
        except Exception as exc:
            rec.meta["error_type"] = "load"
            rec.set_stage("load", STATUS_FAILED, error=str(exc), finished=True)
            rec.finished_at = utc_now_iso()
            store.save(rec)
            return _result_from_record(rec, ok=False)
    else:
        rec.set_stage("load", STATUS_EMPTY, error=None, finished=True)

    rec.finished_at = utc_now_iso()
    store.save(rec)
    return _result_from_record(rec, ok=True)


def run_earnings_forecast_by_period(**kwargs) -> dict:
    kwargs = dict(kwargs)
    kwargs["mode"] = "period"
    return run_earnings_forecast(**kwargs)


def run_earnings_forecast_by_ticker(**kwargs) -> dict:
    kwargs = dict(kwargs)
    kwargs["mode"] = "ticker"
    return run_earnings_forecast(**kwargs)


def run_earnings_forecast_by_ann_date(**kwargs) -> dict:
    kwargs = dict(kwargs)
    kwargs["mode"] = "ann_date"
    return run_earnings_forecast(**kwargs)


def run_from_raw_parquet(
    raw_path: str | Path,
    *,
    db_path: str | None = None,
    resolver: NextTradeDateResolver | None = None,
    cleaned_path: str | Path | None = None,
    init_db: bool = True,
    load: bool = True,
    run_tag: str = DEFAULT_RUN_TAG,
    batch_id: str | None = None,
    state_dir: str | Path | None = None,
) -> dict:
    """Offline resume: raw parquet → clean → load with manifest."""
    raw_path = Path(raw_path)
    dirs = default_artifact_dirs(run_tag)
    state_path = Path(state_dir) if state_dir is not None else dirs["state_dir"]
    state_path.mkdir(parents=True, exist_ok=True)
    batch_id = batch_id or f"{DATASET}:from_raw:{raw_path.stem}:{_utc_stamp()}"
    if cleaned_path is None:
        cleaned_path = cleaned_file_path(dirs["cleaned_dir"], batch_id)
    cleaned_path = Path(cleaned_path)
    manifest_path = state_path / "manifest.jsonl"
    store = ManifestStore(manifest_path)

    rec = BatchRecord(
        batch_id=batch_id,
        dataset=DATASET,
        key="from_raw",
        raw_path=str(raw_path),
        cleaned_path=str(cleaned_path),
        started_at=utc_now_iso(),
        meta={
            "mode": "from_raw",
            "endpoint": None,
            "request": {"raw_path": str(raw_path)},
            "manifest_path": str(manifest_path),
            "invalid_rows": 0,
        },
    )
    # raw already exists
    rec.set_stage("fetch", STATUS_SUCCESS if raw_path.exists() else STATUS_FAILED, error=None, finished=True)
    if not raw_path.exists():
        rec.meta["error_type"] = "raw_missing"
        rec.set_stage("fetch", STATUS_FAILED, error=f"raw missing: {raw_path}", finished=True)
        rec.finished_at = utc_now_iso()
        store.save(rec)
        return _result_from_record(rec, ok=False)
    store.save(rec)

    if resolver is None and db_path is not None:
        resolver = NextTradeDateResolver(db_path=db_path)

    try:
        raw = load_parquet(raw_path, quarantine=True)
    except Exception as exc:
        rec.meta["error_type"] = "raw_io"
        rec.set_stage("fetch", STATUS_FAILED, error=str(exc), finished=True)
        rec.finished_at = utc_now_iso()
        store.save(rec)
        return _result_from_record(rec, ok=False)

    rec.fetched_rows = 0 if raw is None else int(len(raw))
    if raw is None or raw.empty:
        rec.set_stage("clean", STATUS_EMPTY, error=None, finished=True)
        rec.set_stage("load", STATUS_EMPTY, error=None, finished=True)
        rec.finished_at = utc_now_iso()
        store.save(rec)
        return _result_from_record(rec, ok=True)

    rec.set_stage("clean", STATUS_RUNNING, error=None)
    store.save(rec)
    try:
        cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver)
        rec.cleaned_rows = int(len(cleaned))
        rec.meta["invalid_rows"] = int(getattr(cleaned, "attrs", {}).get("invalid_rows", 0) or 0)
        save_parquet(cleaned, cleaned_path)
        rec.cleaned_path = str(cleaned_path)
        rec.set_stage("clean", STATUS_SUCCESS, error=None, finished=True)
        store.save(rec)
    except Exception as exc:
        rec.meta["error_type"] = (
            "data_quality" if isinstance(exc, EarningsForecastDataQualityError) else "clean"
        )
        rec.set_stage("clean", STATUS_FAILED, error=str(exc), finished=True)
        rec.finished_at = utc_now_iso()
        store.save(rec)
        return _result_from_record(rec, ok=False)

    if load:
        rec.set_stage("load", STATUS_RUNNING, error=None)
        store.save(rec)
        try:
            inserted = int(load_earnings_forecast(cleaned, db_path=db_path, init=init_db))
            rec.inserted_rows = inserted
            rec.set_stage("load", STATUS_SUCCESS, error=None, finished=True)
        except Exception as exc:
            rec.meta["error_type"] = "load"
            rec.set_stage("load", STATUS_FAILED, error=str(exc), finished=True)
            rec.finished_at = utc_now_iso()
            store.save(rec)
            return _result_from_record(rec, ok=False)
    else:
        rec.set_stage("load", STATUS_EMPTY, error=None, finished=True)

    rec.finished_at = utc_now_iso()
    store.save(rec)
    return _result_from_record(rec, ok=True)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="QRP earnings forecast event pipeline")
    parser.add_argument("--mode", choices=["period", "ticker", "ann_date"], default="period")
    parser.add_argument("--periods", default=None, help="comma YYYYMMDD report periods")
    parser.add_argument("--tickers", default=None, help="comma ts_code filter / list")
    parser.add_argument("--ann-dates", default=None, help="comma YYYYMMDD announcement dates")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--raw-path", default=None, help="optional override; auto-generated by default")
    parser.add_argument("--cleaned-path", default=None, help="optional override; auto-generated by default")
    parser.add_argument("--run-tag", default=DEFAULT_RUN_TAG)
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--no-load", action="store_true")
    parser.add_argument("--from-raw", default=None, help="offline clean/load from raw parquet")
    args = parser.parse_args(argv)

    if args.from_raw:
        result = run_from_raw_parquet(
            args.from_raw,
            db_path=args.db_path,
            cleaned_path=args.cleaned_path,
            load=not args.no_load,
            run_tag=args.run_tag,
            batch_id=args.batch_id,
            state_dir=args.state_dir,
        )
        print(json.dumps(result, ensure_ascii=False, default=str))
        if not result.get("ok", False):
            raise SystemExit(2)
        return

    periods = _parse_list(args.periods)
    tickers = _parse_list(args.tickers)
    ann_dates = _parse_list(args.ann_dates)
    if args.mode == "period" and not periods:
        raise SystemExit("--periods is required in period mode")
    if args.mode == "ticker" and not tickers:
        raise SystemExit("--tickers is required in ticker mode")
    if args.mode == "ann_date" and not ann_dates:
        raise SystemExit("--ann-dates is required in ann_date mode")

    result = run_earnings_forecast(
        mode=args.mode,
        periods=periods,
        tickers=tickers,
        ann_dates=ann_dates,
        start_date=args.start_date,
        end_date=args.end_date,
        db_path=args.db_path,
        raw_path=args.raw_path,
        cleaned_path=args.cleaned_path,
        load=not args.no_load,
        run_tag=args.run_tag,
        batch_id=args.batch_id,
        state_dir=args.state_dir,
    )
    print(json.dumps(result, ensure_ascii=False, default=str))
    if not result.get("ok", False):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
