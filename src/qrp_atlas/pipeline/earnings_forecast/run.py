"""CLI / programmatic entry for earnings forecast event pipeline.

Examples:
  # period bulk (forecast_vip) small verification
  python -m qrp_atlas.pipeline.earnings_forecast.run \\
    --mode period --periods 20231231 --tickers 000001.SZ

  # single ticker debug (forecast)
  python -m qrp_atlas.pipeline.earnings_forecast.run \\
    --mode ticker --tickers 000001.SZ --start-date 20200101 --end-date 20261231

  # announcement-date incremental candidate
  python -m qrp_atlas.pipeline.earnings_forecast.run \\
    --mode ann_date --ann-dates 20240131
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

import pandas as pd

from qrp_atlas.pipeline.earnings_forecast.clean import clean_earnings_forecast
from qrp_atlas.pipeline.earnings_forecast.fetch import (
    ENDPOINT_FORECAST,
    ENDPOINT_FORECAST_VIP,
    ForecastApiError,
    ForecastPermissionError,
    fetch_earnings_forecast,
)
from qrp_atlas.pipeline.earnings_forecast.load_duckdb import load_earnings_forecast
from qrp_atlas.pipeline.pit_backfill.raw_io import load_parquet, save_parquet
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver


def _parse_list(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [x.strip() for x in raw.split(",") if x.strip()]


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
) -> dict:
    """Fetch → clean → optional parquet archive → append-only load."""
    endpoint = ENDPOINT_FORECAST_VIP if mode == "period" else ENDPOINT_FORECAST
    print(
        f"[EARNINGS_FORECAST] mode={mode} endpoint={endpoint} "
        f"periods={periods} tickers={tickers} ann_dates={ann_dates}"
    )
    if resolver is None and db_path is not None:
        resolver = NextTradeDateResolver(db_path=db_path)

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
        result = {
            "ok": False,
            "mode": mode,
            "endpoint": endpoint,
            "error_type": "permission",
            "error": str(exc),
            "fetched": 0,
            "cleaned": 0,
            "inserted": 0,
        }
        print(json.dumps(result, ensure_ascii=False))
        return result
    except ForecastApiError as exc:
        result = {
            "ok": False,
            "mode": mode,
            "endpoint": endpoint,
            "error_type": "api",
            "error": str(exc),
            "fetched": 0,
            "cleaned": 0,
            "inserted": 0,
        }
        print(json.dumps(result, ensure_ascii=False))
        return result

    fetched = 0 if raw is None else int(len(raw))
    print(f"[EARNINGS_FORECAST] fetched={fetched}")
    if raw_path is not None:
        save_parquet(raw if raw is not None else pd.DataFrame(), raw_path)
        print(f"[EARNINGS_FORECAST] raw_saved={raw_path}")

    if raw is None or raw.empty:
        if init_db and load:
            load_earnings_forecast(pd.DataFrame(), db_path=db_path, init=True)
        return {
            "ok": True,
            "mode": mode,
            "endpoint": endpoint,
            "fetched": 0,
            "cleaned": 0,
            "inserted": 0,
            "raw_path": str(raw_path) if raw_path else None,
            "cleaned_path": str(cleaned_path) if cleaned_path else None,
        }

    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver)
    print(f"[EARNINGS_FORECAST] cleaned={len(cleaned)}")
    if cleaned_path is not None:
        save_parquet(cleaned, cleaned_path)
        print(f"[EARNINGS_FORECAST] cleaned_saved={cleaned_path}")

    inserted = 0
    if load:
        inserted = load_earnings_forecast(cleaned, db_path=db_path, init=init_db)
        print(f"[EARNINGS_FORECAST] inserted={inserted}")

    return {
        "ok": True,
        "mode": mode,
        "endpoint": endpoint,
        "fetched": fetched,
        "cleaned": int(len(cleaned)),
        "inserted": int(inserted),
        "raw_path": str(raw_path) if raw_path else None,
        "cleaned_path": str(cleaned_path) if cleaned_path else None,
    }


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
) -> dict:
    """Offline resume: raw parquet → clean → load."""
    raw = load_parquet(raw_path, quarantine=True)
    if resolver is None and db_path is not None:
        resolver = NextTradeDateResolver(db_path=db_path)
    if raw is None or raw.empty:
        return {"ok": True, "fetched": 0, "cleaned": 0, "inserted": 0, "raw_path": str(raw_path)}
    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver)
    if cleaned_path is not None:
        save_parquet(cleaned, cleaned_path)
    inserted = 0
    if load:
        inserted = load_earnings_forecast(cleaned, db_path=db_path, init=init_db)
    return {
        "ok": True,
        "fetched": int(len(raw)),
        "cleaned": int(len(cleaned)),
        "inserted": int(inserted),
        "raw_path": str(raw_path),
        "cleaned_path": str(cleaned_path) if cleaned_path else None,
    }


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="QRP earnings forecast event pipeline")
    parser.add_argument("--mode", choices=["period", "ticker", "ann_date"], default="period")
    parser.add_argument("--periods", default=None, help="comma YYYYMMDD report periods")
    parser.add_argument("--tickers", default=None, help="comma ts_code filter / list")
    parser.add_argument("--ann-dates", default=None, help="comma YYYYMMDD announcement dates")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--raw-path", default=None)
    parser.add_argument("--cleaned-path", default=None)
    parser.add_argument("--no-load", action="store_true")
    parser.add_argument("--from-raw", default=None, help="offline clean/load from raw parquet")
    args = parser.parse_args(argv)

    if args.from_raw:
        result = run_from_raw_parquet(
            args.from_raw,
            db_path=args.db_path,
            cleaned_path=args.cleaned_path,
            load=not args.no_load,
        )
        print(json.dumps(result, ensure_ascii=False, default=str))
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
    )
    print(json.dumps(result, ensure_ascii=False, default=str))
    if not result.get("ok", False):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
