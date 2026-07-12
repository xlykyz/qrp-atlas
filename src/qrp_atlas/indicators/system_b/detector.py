"""System B basic state indicators derived from already-calculated stock trends."""

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


def calculate_system_b_basic_states(trend_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate System B basic states for every ticker/date row.

    This is an indicator-layer operation.  It intentionally exposes no trading
    actions and can therefore be reused by strategies and non-strategy clients.
    """

    if trend_df is None or trend_df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)
    missing = [column for column in _REQUIRED_TREND_COLUMNS if column not in trend_df.columns]
    if missing:
        raise ValueError(f"calculate_system_b_basic_states 缺少必要趋势列: {missing}")

    result = trend_df.sort_values([TICKER, TRADE_DATE], kind="mergesort").reset_index(drop=True).copy()
    close_ge_ma5 = (result[CLOSE] >= result[MA5]).fillna(False).astype(bool)
    close_lt_ma5 = (result[CLOSE] < result[MA5]).fillna(False).astype(bool)
    result[SYSTEM_B_TREND_VALID] = (
        close_ge_ma5.groupby(result[TICKER], sort=False)
        .transform(lambda series: series.rolling(_CONSECUTIVE_DAYS, min_periods=_CONSECUTIVE_DAYS).sum().eq(_CONSECUTIVE_DAYS))
        .fillna(False).astype(bool)
    )
    result[SYSTEM_B_EXIT_TRIGGERED] = (
        close_lt_ma5.groupby(result[TICKER], sort=False)
        .transform(lambda series: series.rolling(_CONSECUTIVE_DAYS, min_periods=_CONSECUTIVE_DAYS).sum().eq(_CONSECUTIVE_DAYS))
        .fillna(False).astype(bool)
    )
    return result.loc[:, _OUTPUT_COLUMNS]


def calculate_system_b_basic_states_from_prices(price_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily System B states directly from raw price data."""

    return calculate_system_b_basic_states(calculate_stock_trend(price_df))


def detect_system_b_basic_state(trend_df: pd.DataFrame) -> pd.DataFrame:
    """Return the most recent System B state for each ticker (legacy public API)."""

    states = calculate_system_b_basic_states(trend_df)
    if states.empty:
        return states
    return states.groupby(TICKER, sort=False).tail(1).reset_index(drop=True)


def detect_system_b_basic_state_from_prices(price_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate trends from raw prices then return the latest state per ticker."""

    return detect_system_b_basic_state(calculate_stock_trend(price_df))
