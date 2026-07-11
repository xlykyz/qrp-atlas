"""run.py - daily_basic 数据管道

使用方式:
    python src/qrp_atlas/pipeline/daily_basic/run.py                          # 跑最新交易日
    python src/qrp_atlas/pipeline/daily_basic/run.py --date 2026-02-24       # 跑指定日期
"""

import argparse
from datetime import date, timedelta

import duckdb

from qrp_atlas.config import DB_PATH, ensure_dirs

from .fetch import fetch_daily_basic
from .clean import clean_daily_basic
from .load_duckdb import load_daily_basic


def run_for_date(trade_date: date, *, init_db: bool = False) -> None:
    """对指定交易日执行完整 pipeline（fetch → clean → load）

    Args:
        trade_date: 交易日期
    """
    ensure_dirs()

    trade_date_str = trade_date.isoformat()
    print(f"[DAILY_BASIC] trade_date={trade_date_str}")

    # 1. fetch
    df_raw = fetch_daily_basic(trade_date)
    print(f"[DAILY_BASIC] fetched={len(df_raw)} rows")

    # 2. clean
    df_clean = clean_daily_basic(df_raw)
    print(f"[DAILY_BASIC] cleaned={len(df_clean)} rows")

    # 3. load
    rows_loaded = load_daily_basic(df_clean, trade_date_str, init=init_db)
    print(f"[DAILY_BASIC] rows_loaded={rows_loaded}")


def guess_latest_trade_date() -> date:
    """从数据库 daily_market_snapshot 获取最新交易日

    若数据库无数据，回退到昨天。
    """
    try:
        con = duckdb.connect(str(DB_PATH))
        try:
            result = con.execute(
                "SELECT MAX(trade_date) FROM daily_market_snapshot"
            ).fetchone()
            if result and result[0]:
                return result[0]
        finally:
            con.close()
    except Exception:
        pass
    return date.today() - timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser(description="QRP Atlas daily_basic 数据管道")
    parser.add_argument(
        "--date", type=str, default=None,
        help="指定交易日 (YYYY-MM-DD)，不传则跑最新交易日",
    )
    args = parser.parse_args()

    if args.date:
        try:
            trade_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"❌ 日期格式错误: {args.date}，请使用 YYYY-MM-DD")
            return
        run_for_date(trade_date, init_db=True)
    else:
        trade_date = guess_latest_trade_date()
        print(f"[DAILY_BASIC] 自动检测最新交易日: {trade_date}")
        run_for_date(trade_date, init_db=True)


if __name__ == "__main__":
    main()