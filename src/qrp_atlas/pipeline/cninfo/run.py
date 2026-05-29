"""
run.py - cninfo 调研公告数据管道入口

用法:
    python -m qrp_atlas.pipeline.cninfo.run --date 2026-05-28
    python -m qrp_atlas.pipeline.cninfo.run --date 2026-05-28 --date 2026-05-29 --incremental
"""
import argparse
import sys
import duckdb

from qrp_atlas.pipeline.cninfo.fetch import fetch_from_eastmoney
from qrp_atlas.pipeline.cninfo.clean import clean_eastmoney
from qrp_atlas.pipeline.cninfo.load import upsert_research_visits
from qrp_atlas.config.paths import DB_PATH
from qrp_atlas.contracts.schema import init_database


def run(date_str: str, source: str = "eastmoney", incremental: bool = False) -> int:
    """执行单日调研公告数据管道"""
    
    if source != "eastmoney":
        print(f"Unsupported source: {source}", file=sys.stderr)
        return 1
    
    mode = "incremental" if incremental else "full"
    print(f"[cninfo] Starting pipeline for {date_str} ({mode})")
    
    # Fetch
    print(f"[cninfo] Fetching data...")
    raw_records = fetch_from_eastmoney(date_str)
    print(f"[cninfo] Fetched {len(raw_records)} raw records")
    
    # Clean
    print(f"[cninfo] Cleaning data...")
    cleaned = clean_eastmoney(raw_records)
    print(f"[cninfo] Cleaned to {len(cleaned)} unique surveys")
    
    # Load
    print(f"[cninfo] Loading to database ({mode})...")
    con = duckdb.connect(str(DB_PATH))
    try:
        init_database(con)
        count = upsert_research_visits(con, cleaned, incremental=incremental)
        action = "Skipped (IGNORE)" if incremental else "Loaded"
        print(f"[cninfo] {action} {count} records")
        
        # Verify
        row_count = con.execute(
            "SELECT COUNT(*) FROM cninfo_research_visits WHERE notice_date = ?",
            [date_str]
        ).fetchone()[0]
        print(f"[cninfo] Verified: {row_count} rows in DB for {date_str}")
    finally:
        con.close()
    
    print(f"[cninfo] Pipeline completed successfully")
    return 0


def main():
    parser = argparse.ArgumentParser(description="cninfo 调研公告数据管道")
    parser.add_argument("--date", required=True, action="append", help="日期，格式 YYYY-MM-DD（可多次指定）")
    parser.add_argument("--source", default="eastmoney", choices=["eastmoney"], help="数据源（默认 eastmoney）")
    parser.add_argument("--incremental", action="store_true", help="增量模式：INSERT OR IGNORE 跳过已有记录")
    args = parser.parse_args()
    
    for date_str in args.date:
        exit_code = run(date_str, args.source, incremental=args.incremental)
        if exit_code != 0:
            sys.exit(exit_code)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
