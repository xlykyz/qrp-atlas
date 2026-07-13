"""Index component history entrypoints.

Examples:
  python -m qrp_atlas.pipeline.index_component.run \\
    --index-code 000300.SH --start 20240101 --end 20240331

  # later multi-index backfill (manual only)
  python -m qrp_atlas.pipeline.index_component.run \\
    --index-code 000300.SH,000905.SH --start 20240101 --end 20241231
"""

from __future__ import annotations

import argparse
from typing import Sequence

from qrp_atlas.pipeline.index_component.clean import clean_index_component
from qrp_atlas.pipeline.index_component.fetch import fetch_index_weights
from qrp_atlas.pipeline.index_component.load_duckdb import load_index_component
from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver


def _parse_list(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [x.strip() for x in raw.split(",") if x.strip()]


def run_index_component(
    *,
    index_codes: Sequence[str],
    start_date: str,
    end_date: str,
    client=None,
    db_path: str | None = None,
    resolver: NextTradeDateResolver | None = None,
    init_db: bool = True,
) -> dict:
    print(f"[INDEX] codes={index_codes} range={start_date}->{end_date}")
    raw = fetch_index_weights(index_codes, start_date=start_date, end_date=end_date, client=client)
    print(f"[INDEX] fetched={0 if raw is None else len(raw)}")
    if raw is None or raw.empty:
        return {"fetched": 0, "cleaned": 0, "inserted": 0}
    cleaned = clean_index_component(raw, trade_date_resolver=resolver)
    print(f"[INDEX] cleaned={len(cleaned)}")
    inserted = load_index_component(cleaned, db_path=db_path, init=init_db)
    print(f"[INDEX] inserted={inserted}")
    return {"fetched": len(raw), "cleaned": len(cleaned), "inserted": inserted}


def main(argv: Sequence[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="QRP index component history pipeline")
    p.add_argument("--index-code", required=True, help="comma index codes, e.g. 000300.SH")
    p.add_argument("--start", required=True, help="YYYYMMDD")
    p.add_argument("--end", required=True, help="YYYYMMDD")
    p.add_argument("--db-path", default=None)
    args = p.parse_args(argv)
    codes = _parse_list(args.index_code) or []
    run_index_component(index_codes=codes, start_date=args.start, end_date=args.end, db_path=args.db_path)


if __name__ == "__main__":
    main()
