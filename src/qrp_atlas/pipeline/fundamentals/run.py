"""CLI / programmatic entry for financial pipelines.

Examples:
  # small verification pull (recommended)
  python -m qrp_atlas.pipeline.fundamentals.run \\
    --tables all --periods 20231231,20240630 \\
    --tickers 000001.SZ,600519.SH,300750.SZ

  # period backfill entry (do not auto-run full market)
  python -m qrp_atlas.pipeline.fundamentals.run --tables income_statement --periods 20231231

  # ticker backfill
  python -m qrp_atlas.pipeline.fundamentals.run \\
    --mode ticker --tables all --tickers 000001.SZ --periods 20231231,20240630
"""

from __future__ import annotations

import argparse
from typing import Sequence

import pandas as pd

from qrp_atlas.pipeline.fundamentals.clean import clean_financial
from qrp_atlas.pipeline.fundamentals.fetch import fetch_financial
from qrp_atlas.pipeline.fundamentals.load_duckdb import load_financial
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver

ALL_TABLES = (
    "income_statement",
    "balance_sheet",
    "cashflow_statement",
    "financial_indicator",
)


def _parse_list(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [x.strip() for x in raw.split(",") if x.strip()]


def run_one(
    table: str,
    *,
    periods: Sequence[str] | None = None,
    tickers: Sequence[str] | None = None,
    mode: str = "period",
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
    db_path: str | None = None,
    resolver: NextTradeDateResolver | None = None,
    init_db: bool = True,
) -> dict:
    print(f"[FUNDAMENTALS] table={table} mode={mode} periods={periods} tickers={tickers}")
    raw = fetch_financial(
        table,
        periods=periods,
        tickers=tickers,
        mode=mode,
        start_date=start_date,
        end_date=end_date,
        client=client,
    )
    print(f"[FUNDAMENTALS] fetched={0 if raw is None else len(raw)}")
    if raw is None or raw.empty:
        return {"table": table, "fetched": 0, "cleaned": 0, "inserted": 0}

    cleaned = clean_financial(raw, table, trade_date_resolver=resolver)
    print(f"[FUNDAMENTALS] cleaned={len(cleaned)}")
    inserted = load_financial(cleaned, table, db_path=db_path, init=init_db)
    print(f"[FUNDAMENTALS] inserted={inserted}")
    return {"table": table, "fetched": len(raw), "cleaned": len(cleaned), "inserted": inserted}


def run_income_statement(**kwargs) -> dict:
    return run_one("income_statement", **kwargs)


def run_balance_sheet(**kwargs) -> dict:
    return run_one("balance_sheet", **kwargs)


def run_cashflow_statement(**kwargs) -> dict:
    return run_one("cashflow_statement", **kwargs)


def run_financial_indicator(**kwargs) -> dict:
    return run_one("financial_indicator", **kwargs)


def run_fundamentals(
    tables: Sequence[str] = ALL_TABLES,
    **kwargs,
) -> list[dict]:
    results = []
    for table in tables:
        results.append(run_one(table, **kwargs))
    return results


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="QRP financial data pipeline (Tushare VIP)")
    parser.add_argument(
        "--tables",
        default="all",
        help="comma list or 'all' (income_statement,balance_sheet,cashflow_statement,financial_indicator)",
    )
    parser.add_argument("--periods", default=None, help="comma YYYYMMDD report periods")
    parser.add_argument("--tickers", default=None, help="optional comma ts_code filter")
    parser.add_argument("--mode", choices=["period", "ticker"], default="period")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--db-path", default=None)
    args = parser.parse_args(argv)

    tables = ALL_TABLES if args.tables.strip().lower() == "all" else tuple(_parse_list(args.tables) or [])
    periods = _parse_list(args.periods)
    tickers = _parse_list(args.tickers)
    if args.mode == "period" and not periods:
        raise SystemExit("--periods is required in period mode")
    if args.mode == "ticker" and not tickers:
        raise SystemExit("--tickers is required in ticker mode")

    run_fundamentals(
        tables=tables,
        periods=periods,
        tickers=tickers,
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        db_path=args.db_path,
    )


if __name__ == "__main__":
    main()
