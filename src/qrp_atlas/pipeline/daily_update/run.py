"""run.py - 日更数据管道

使用方式:
    python src/qrp_atlas/pipeline/daily_update/run.py           # 跑最新交易日
    python src/qrp_atlas/pipeline/daily_update/run.py --date 2026-02-24  # 跑指定日期
"""

import argparse
from datetime import date

import duckdb

from qrp_atlas.config import (
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
}


def run_for_date(trade_date: date) -> None:
    """对指定交易日执行完整 pipeline（fetch → clean → enrich → load）

    Args:
        trade_date: 交易日期（历史或当天均可）
    """
    ensure_dirs()

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


def run() -> None:
    """默认行为：跑最新交易日"""
    run_for_date(get_latest_trade_date())


def main() -> None:
    parser = argparse.ArgumentParser(description="QRP Atlas 日更数据管道")
    parser.add_argument(
        "--date", type=str, default=None,
        help="指定交易日 (YYYY-MM-DD)，不传则跑最新交易日"
    )
    args = parser.parse_args()

    if args.date:
        try:
            trade_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"❌ 日期格式错误: {args.date}，请使用 YYYY-MM-DD")
            return
        run_for_date(trade_date)
    else:
        run()


if __name__ == "__main__":
    main()
