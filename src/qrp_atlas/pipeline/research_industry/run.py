"""
run.py - 研报数据管道入口

用法:
    python -m qrp_atlas.pipeline.research_report.run --begin 2026-05-01 --end 2026-05-31
    python -m qrp_atlas.pipeline.research_report.run --begin 2026-05-01 --end 2026-05-31 --incremental
"""
import argparse
import sys
import duckdb

from qrp_atlas.pipeline.research_report.fetch import fetch_report_list
from qrp_atlas.pipeline.research_report.fetch_detail import fetch_report_detail
from qrp_atlas.pipeline.research_report.clean import clean_report, save_raw_csv, save_canonical_csv
from qrp_atlas.pipeline.research_report.load import load_report
from qrp_atlas.config.paths import DB_PATH
from qrp_atlas.contracts.schema import init_database


def run(begin: str, end: str, incremental: bool = False) -> int:
    """执行研报数据管道"""
    
    mode = "incremental" if incremental else "full"
    print(f"[research_report] Pipeline started: {begin} → {end} (mode: {mode})")
    
    # Step 1: Fetch list
    print(f"[fetch] Fetching report list...")
    records = fetch_report_list(begin, end)
    print(f"[fetch] Fetched {len(records)} records")
    
    if not records:
        print("[research_report] No records fetched, skipping remaining steps")
        print("[research_report] Pipeline completed successfully")
        return 0
    
    # Step 2: Fetch detail
    print(f"[fetch_detail] Processing {len(records)} records...")
    detailed = fetch_report_detail(records)
    print(f"[fetch_detail] Processed {len(detailed)} records")
    
    # Step 3: Save raw CSV
    date_tag = f"{begin}_{end}"
    raw_csv_path = save_raw_csv(detailed, date_tag)
    print(f"[clean] Raw CSV saved: {raw_csv_path}")
    
    # Step 4: Clean
    print(f"[clean] Cleaning data...")
    cleaned = clean_report(detailed)
    print(f"[clean] Cleaned {len(cleaned)} records")
    
    # Step 5: Save canonical CSV
    canonical_csv_path = save_canonical_csv(cleaned, date_tag)
    print(f"[clean] Canonical CSV saved: {canonical_csv_path}")
    
    # Step 6: Load to database
    print(f"[load] Loading to database...")
    con = duckdb.connect(str(DB_PATH))
    try:
        init_database(con)
        count = load_report(con, cleaned, incremental=incremental)
        action = "Skipped" if incremental else "Loaded"
        print(f"[load] {action} {count} records into research_report_stock")
        
        # Step 7: Verify
        row_count = con.execute(
            "SELECT COUNT(*) FROM research_report_stock WHERE publish_date >= ? AND publish_date <= ?",
            [begin, end]
        ).fetchone()[0]
        print(f"[research_report] Verified: {row_count} rows in DB")
    finally:
        con.close()
    
    print("[research_report] Pipeline completed successfully")
    return 0


def main():
    parser = argparse.ArgumentParser(description="研报数据管道")
    parser.add_argument("--begin", required=True, help="开始日期，格式 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="结束日期，格式 YYYY-MM-DD")
    parser.add_argument("--incremental", action="store_true", help="增量模式：INSERT OR IGNORE 跳过已有记录")
    args = parser.parse_args()
    
    exit_code = run(args.begin, args.end, incremental=args.incremental)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
