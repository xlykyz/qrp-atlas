"""Industry membership history entrypoints.

Examples:
  python -m qrp_atlas.pipeline.industry_membership.run \\
    --tickers 000001.SZ,600519.SH,300750.SZ,000002.SZ

  # later backfill by industry code (not default daily job)
  python -m qrp_atlas.pipeline.industry_membership.run --l3-code 851251.SI
"""

from __future__ import annotations

import argparse
from typing import Sequence

from qrp_atlas.pipeline.industry_membership.clean import clean_industry_membership
from qrp_atlas.pipeline.industry_membership.fetch import fetch_industry_membership
from qrp_atlas.pipeline.industry_membership.load_duckdb import load_industry_membership
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver


def _parse_list(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [x.strip() for x in raw.split(",") if x.strip()]


def run_industry_membership(
    *,
    tickers: Sequence[str] | None = None,
    l1_code: str | None = None,
    l2_code: str | None = None,
    l3_code: str | None = None,
    is_new: str | None = None,
    client=None,
    db_path: str | None = None,
    resolver: NextTradeDateResolver | None = None,
    init_db: bool = True,
) -> dict:
    print(f"[INDUSTRY] tickers={tickers} l1={l1_code} l2={l2_code} l3={l3_code}")
    if resolver is None and db_path is not None:
        resolver = NextTradeDateResolver(db_path=db_path)
    raw = fetch_industry_membership(
        tickers=tickers,
        l1_code=l1_code,
        l2_code=l2_code,
        l3_code=l3_code,
        is_new=is_new,
        client=client,
    )
    print(f"[INDUSTRY] fetched={0 if raw is None else len(raw)}")
    if raw is None or raw.empty:
        return {"fetched": 0, "cleaned": 0, "inserted": 0}
    cleaned = clean_industry_membership(raw, trade_date_resolver=resolver)
    print(f"[INDUSTRY] cleaned={len(cleaned)}")
    inserted = load_industry_membership(cleaned, db_path=db_path, init=init_db)
    print(f"[INDUSTRY] inserted={inserted}")
    return {"fetched": len(raw), "cleaned": len(cleaned), "inserted": inserted}


def main(argv: Sequence[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="QRP industry membership history pipeline")
    p.add_argument("--tickers", default=None)
    p.add_argument("--l1-code", default=None)
    p.add_argument("--l2-code", default=None)
    p.add_argument("--l3-code", default=None)
    p.add_argument("--is-new", default=None, help="optional Tushare is_new filter Y/N")
    p.add_argument("--db-path", default=None)
    args = p.parse_args(argv)
    tickers = _parse_list(args.tickers)
    if not tickers and not any([args.l1_code, args.l2_code, args.l3_code]):
        raise SystemExit("provide --tickers or an industry code")
    run_industry_membership(
        tickers=tickers,
        l1_code=args.l1_code,
        l2_code=args.l2_code,
        l3_code=args.l3_code,
        is_new=args.is_new,
        db_path=args.db_path,
    )


if __name__ == "__main__":
    main()
