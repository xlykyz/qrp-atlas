"""run.py - suspend_d 数据管道

使用方式:
    python -m qrp_atlas.pipeline.suspend_d.run --year 2025              # 跑指定年份
    python -m qrp_atlas.pipeline.suspend_d.run --date 2025-01-15        # 跑单日期
    python -m qrp_atlas.pipeline.suspend_d.run --start 2025-01-01 --end 2025-06-30  # 日期区间
    python -m qrp_atlas.pipeline.suspend_d.run                          # 跑当前年份
"""

import argparse
from datetime import date

import pandas as pd

from .fetch import fetch_suspend_d, fetch_suspend_d_year
from .clean import clean_suspend_d
from .load_duckdb import load_suspend_d


def run_for_year(year: int) -> None:
    """对指定年份执行完整 pipeline

    Args:
        year: 年份，如 2025
    """
    print(f"[SUSPEND_D] year={year}")
    try:
        df_raw = fetch_suspend_d_year(year)
    except ValueError as e:
        print(f"  ⚠️  {e}")
        return
    print(f"[SUSPEND_D] fetched={len(df_raw)} rows")
    _load(df_raw)


def run_for_date(trade_date: date) -> None:
    """对指定交易日执行完整 pipeline

    Args:
        trade_date: 交易日期
    """
    print(f"[SUSPEND_D] date={trade_date}")
    try:
        df_raw = fetch_suspend_d(trade_date, trade_date)
    except ValueError as e:
        print(f"  ⚠️  {e}")
        return
    print(f"[SUSPEND_D] fetched={len(df_raw)} rows")
    _load(df_raw)


def run_for_range(start_date: date, end_date: date) -> None:
    """对指定日期区间执行完整 pipeline

    Args:
        start_date: 起始日期
        end_date: 结束日期
    """
    print(f"[SUSPEND_D] range={start_date} -> {end_date}")
    try:
        df_raw = fetch_suspend_d(start_date, end_date)
    except ValueError as e:
        print(f"  ⚠️  {e}")
        return
    print(f"[SUSPEND_D] fetched={len(df_raw)} rows")
    _load(df_raw)


def _load(df_raw: pd.DataFrame) -> None:
    """公共的 clean + load 步骤"""
    df_clean = clean_suspend_d(df_raw)
    print(f"[SUSPEND_D] cleaned={len(df_clean)} rows")
    if len(df_clean) > 0:
        print(f"[SUSPEND_D] 日期范围: {df_clean['trade_date'].min()} -> {df_clean['trade_date'].max()}")
    rows_loaded = load_suspend_d(df_clean, init=True)
    print(f"[SUSPEND_D] rows_loaded={rows_loaded}")


def main() -> None:
    parser = argparse.ArgumentParser(description="QRP Atlas suspend_d 数据管道")
    parser.add_argument("--year", type=int, default=None, help="指定年份 (如 2025)")
    parser.add_argument("--date", type=str, default=None, help="指定交易日 (YYYY-MM-DD)")
    parser.add_argument("--start", type=str, default=None, dest="start_date", help="起始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, dest="end_date", help="结束日期 (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.year:
        run_for_year(args.year)
    elif args.date:
        try:
            d = date.fromisoformat(args.date)
        except ValueError:
            print(f"❌ 日期格式错误: {args.date}，请使用 YYYY-MM-DD")
            return
        run_for_date(d)
    elif args.start_date or args.end_date:
        start = date.fromisoformat(args.start_date) if args.start_date else date(2000, 1, 1)
        end = date.fromisoformat(args.end_date) if args.end_date else date.today()
        if start > end:
            print(f"❌ start_date ({start}) 不能晚于 end_date ({end})")
            return
        run_for_range(start, end)
    else:
        run_for_year(date.today().year)


if __name__ == "__main__":
    main()