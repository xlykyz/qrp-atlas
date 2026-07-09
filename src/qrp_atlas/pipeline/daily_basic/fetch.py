"""fetch.py - 从 tushare daily_basic 接口获取每日基本面指标

接口: pro.daily_basic(trade_date='YYYYMMDD')
更新: 交易日 15:00~17:00
"""

from datetime import date, datetime

import pandas as pd

from qrp_atlas.config import get_tushare_pro


def fetch_daily_basic(trade_date: date) -> pd.DataFrame:
    """获取指定交易日全市场 daily_basic 数据

    Args:
        trade_date: 交易日期

    Returns:
        DataFrame(ts_code, trade_date, close, turnover_rate, ...)
    """
    date_str = trade_date.strftime("%Y%m%d")
    pro = get_tushare_pro()

    df = pro.daily_basic(trade_date=date_str)

    if df is None or df.empty:
        raise ValueError(f"daily_basic returned empty data for {date_str}")

    # 增加 trade_date 列（tushare 返回含 trade_date）
    return df


def get_latest_trade_date() -> date:
    """获取最近一个交易日（从 trading_calendar 或今日）

    默认返回今天，由调用方根据交易日历决定。
    """
    return date.today()
