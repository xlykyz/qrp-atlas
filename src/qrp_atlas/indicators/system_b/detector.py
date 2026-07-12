"""系统 B 基础状态检测。

基于已经计算好的个股 ma5 趋势结果，判断每个 ticker 最近交易日是否满足
系统 B 的基础趋势条件。只做状态检测，不做交易决策。

规则：
    - system_b_trend_valid:    最近连续 2 个交易日收盘价不低于 ma5
    - system_b_exit_triggered: 最近连续 2 个交易日收盘价跌破 ma5
"""

from __future__ import annotations

import pandas as pd

from qrp_atlas.contracts import CLOSE, TICKER, TRADE_DATE
from qrp_atlas.indicators.stock.trend import (
    CLOSE_ABOVE_MA5_DAYS,
    CLOSE_BELOW_MA5_DAYS,
    MA5,
    calculate_stock_trend,
)

SYSTEM_B_TREND_VALID = "system_b_trend_valid"
SYSTEM_B_EXIT_TRIGGERED = "system_b_exit_triggered"

_CONSECUTIVE_DAYS = 2
_REQUIRED_TREND_COLUMNS = (
    TICKER,
    TRADE_DATE,
    CLOSE,
    MA5,
    CLOSE_ABOVE_MA5_DAYS,
    CLOSE_BELOW_MA5_DAYS,
)
_OUTPUT_COLUMNS = (
    TICKER,
    TRADE_DATE,
    CLOSE,
    MA5,
    SYSTEM_B_TREND_VALID,
    SYSTEM_B_EXIT_TRIGGERED,
)


def detect_system_b_basic_state(trend_df: pd.DataFrame) -> pd.DataFrame:
    """检测系统 B 基础状态。

    Args:
        trend_df: 已计算个股趋势的多日多股行情，需含 ticker、trade_date、
            close、ma5、close_above_ma5_days、close_below_ma5_days。

    Returns:
        每个 ticker 一行，包含：
            - ticker
            - trade_date: 该 ticker 最后一个交易日
            - close: 最后收盘价
            - ma5: 最后交易日 ma5（不足 5 日为 NaN）
            - system_b_trend_valid: 最近 2 日是否均 close >= ma5
            - system_b_exit_triggered: 最近 2 日是否均 close < ma5
    """
    if trend_df is None or trend_df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    missing = [column for column in _REQUIRED_TREND_COLUMNS if column not in trend_df.columns]
    if missing:
        raise ValueError(f"detect_system_b_basic_state 缺少必要趋势列: {missing}")

    ordered = trend_df.sort_values([TICKER, TRADE_DATE]).reset_index(drop=True)
    latest = ordered.groupby(TICKER, sort=False).tail(_CONSECUTIVE_DAYS).copy()
    latest["_close_ge_ma5"] = (latest[CLOSE] >= latest[MA5]).fillna(False)
    latest["_close_lt_ma5"] = (latest[CLOSE] < latest[MA5]).fillna(False)

    latest_states = latest.groupby(TICKER, sort=False).agg(
        _observation_count=(TRADE_DATE, "size"),
        _all_close_ge_ma5=("_close_ge_ma5", "all"),
        _all_close_lt_ma5=("_close_lt_ma5", "all"),
    )
    valid = (
        (latest_states["_observation_count"] == _CONSECUTIVE_DAYS)
        & latest_states["_all_close_ge_ma5"]
    )
    exit_triggered = (
        (latest_states["_observation_count"] == _CONSECUTIVE_DAYS)
        & latest_states["_all_close_lt_ma5"]
    )

    summary = ordered.groupby(TICKER, sort=False).tail(1).reset_index(drop=True)
    summary = summary[[TICKER, TRADE_DATE, CLOSE, MA5]]
    summary[SYSTEM_B_TREND_VALID] = summary[TICKER].map(valid).fillna(False).astype(bool)
    summary[SYSTEM_B_EXIT_TRIGGERED] = (
        summary[TICKER].map(exit_triggered).fillna(False).astype(bool)
    )

    return summary


def detect_system_b_basic_state_from_prices(price_df: pd.DataFrame) -> pd.DataFrame:
    """从原始行情计算趋势后，检测系统 B 基础状态。"""
    trend_df = calculate_stock_trend(price_df)
    return detect_system_b_basic_state(trend_df)
