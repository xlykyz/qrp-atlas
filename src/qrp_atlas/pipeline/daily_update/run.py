from datetime import date

import duckdb

from qrp_atlas.config import (
    DAILY_SNAPSHOT_RAW_DIR,
    DAILY_MARKET_SNAPSHOT_CANONICAL_DIR,
    DB_PATH,
    ensure_dirs,
)

from .fetch import fetch_current_snapshot, save_raw_snapshot, DataSource, get_latest_trade_date
from .clean import clean_daily_snapshot
from .enrich import enrich_daily_snapshot
from .load_duckdb import load_daily_market_snapshot


FETCH_TO_CLEAN_SOURCE = {
    "tushare": "tushare_daily",
    "sina": "sina_realtime",
    "em": "eastmoney_realtime",
}


def run() -> None:
    """
    执行日更数据管道
    
    流程：
    1. fetch -> 保存 raw csv
    2. clean -> 保存 canonical csv
    3. enrich -> 增补缺失数据
    4. load_duckdb
    """
    ensure_dirs()
    
    trade_date = get_latest_trade_date()
    trade_date_str = trade_date.isoformat()
    
    print(f"[DAILY_UPDATE] trade_date={trade_date_str}")
    
    df_raw, fetch_source = fetch_current_snapshot(trade_date)
    
    if fetch_source != "tushare":
        df_raw["trade_date"] = trade_date_str
    
    raw_path = save_raw_snapshot(df_raw, fetch_source, trade_date)
    print(f"[DAILY_UPDATE] raw_saved={raw_path}")
    
    clean_source = FETCH_TO_CLEAN_SOURCE.get(fetch_source, "akshare_realtime")
    df_clean = clean_daily_snapshot(df_raw, source=clean_source)
    
    canonical_dir = DAILY_MARKET_SNAPSHOT_CANONICAL_DIR
    canonical_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = canonical_dir / f"{trade_date_str}.csv"
    df_clean.to_csv(canonical_path, index=False, encoding="utf-8")
    print(f"[DAILY_UPDATE] canonical_saved={canonical_path}")
    
    con = duckdb.connect(str(DB_PATH))
    try:
        df_enriched = enrich_daily_snapshot(df_clean, trade_date, con)
    finally:
        con.close()
    print(f"[DAILY_UPDATE] enriched_rows={len(df_enriched)}")
    
    rows_loaded = load_daily_market_snapshot(df_enriched, trade_date_str)
    print(f"[DAILY_UPDATE] rows_loaded={rows_loaded}")


def main() -> None:
    run()


if __name__ == "__main__":
    main()
