"""
run.py - cninfo 调研公告数据管道入口

用法:
    python -m qrp_atlas.pipeline.cninfo.run --date 2026-05-28
"""
import argparse
import sys
import duckdb

from qrp_atlas.pipeline.cninfo.fetch import fetch_from_eastmoney
from qrp_atlas.pipeline.cninfo.clean import clean_eastmoney
from qrp_atlas.pipeline.cninfo.load import upsert_research_visits
from qrp_atlas.config.paths import DB_PATH
from qrp_atlas.contracts.schema import init_database


def run(date_str: str, source: str = "eastmoney") -> int:
    """执行单日调研公告数据管道"""
    
    if source != "eastmoney":
        print(f"Unsupported source: {source}", file=sys.stderr)
        return 1
    
    print(f"[cninfo] Starting pipeline for {date_str} (source={source})")
    
    # Fetch
    print(f"[cninfo] Fetching data...")
    raw_records = fetch_from_eastmoney(date_str)
    print(f"[cninfo] Fetched {len(raw_records)} raw records")
    
    # Clean
    print(f"[cninfo] Cleaning data...")
    cleaned = clean_eastmoney(raw_records)
    print(f"[cninfo] Cleaned to {len(cleaned)} unique surveys")
    
    # Load
    print(f"[cninfo] Loading to database...")
    con = duckdb.connect(str(DB_PATH))
    try:
        init_database(con)
        count = upsert_research_visits(con, cleaned)
        print(f"[cninfo] Loaded {count} records")
        
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
    parser.add_argument("--date", required=True, help="日期，格式 YYYY-MM-DD")
    parser.add_argument("--source", default="eastmoney", choices=["eastmoney"], help="数据源（默认 eastmoney）")
    args = parser.parse_args()
    
    exit_code = run(args.date, args.source)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
