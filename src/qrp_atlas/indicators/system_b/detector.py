"""系统 B 基础状态检测。

基于个股 ma5 趋势结果，判断每个 ticker 最近交易日是否满足系统 B 的
基础趋势条件。只做状态检测，不做交易决策。

规则：
    - system_b_trend_valid:    最近连续 2 个交易日收盘价不低于 ma5
    - system_b_exit_triggered: 最近连续 2 个交易日收盘价跌破 ma5
"""

from __future__ import annotations

import pandas as pd

from qrp_atlas.contracts import CLOSE, TICKER, TRADE_DATE
from qrp_atlas.indicators.stock.trend import MA5, calculate_stock_trend

SYSTEM_B_TREND_VALID = "system_b_trend_valid"
SYSTEM_B_EXIT_TRIGGERED = "system_b_exit_triggered"

_CONSECUTIVE_DAYS = 2


def detect_system_b_basic_state(df: pd.DataFrame) -> pd.DataFrame:
    """检测系统 B 基础状态。

    Args:
        df: 多日多股行情，需含 ticker、trade_date、close。内部会先调用
            calculate_stock_trend 计算 ma5。

    Returns:
        每个 ticker 一行，包含：
            - ticker
            - trade_date: 该 ticker 最后一个交易日
            - close: 最后收盘价
            - ma5: 最后交易日 ma5（不足 5 日为 NaN）
            - system_b_trend_valid: 最近 2 日是否均 close >= ma5
            - system_b_exit_triggered: 最近 2 日是否均 close < ma5
    """
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                TICKER,
                TRADE_DATE,
                CLOSE,
                MA5,
                SYSTEM_B_TREND_VALID,
                SYSTEM_B_EXIT_TRIGGERED,
            ]
        )

    trend_df = calculate_stock_trend(df)

    latest = trend_df.groupby(TICKER, sort=False).tail(_CONSECUTIVE_DAYS)
    ge_ma5 = (latest[CLOSE] >= latest[MA5]).fillna(False)
    lt_ma5 = (latest[CLOSE] < latest[MA5]).fillna(False)

    grouped = latest.groupby(TICKER, sort=False)
    valid = grouped.apply(lambda g: bool(ge_ma5.loc[g.index].all()))
    exit_triggered = grouped.apply(lambda g: bool(lt_ma5.loc[g.index].all()))

    summary = trend_df.groupby(TICKER, sort=False).tail(1).reset_index(drop=True)
    summary = summary[[TICKER, TRADE_DATE, CLOSE, MA5]]
    summary[SYSTEM_B_TREND_VALID] = summary[TICKER].map(valid).fillna(False).astype(bool)
    summary[SYSTEM_B_EXIT_TRIGGERED] = summary[TICKER].map(exit_triggered).fillna(False).astype(bool)

    return summary
