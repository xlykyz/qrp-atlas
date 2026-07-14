"""Classic volatility, range, channel, and drawdown indicators."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import CLOSE, HIGH, LOW, TICKER

from ._classic_utils import ema_series, finite_numeric, positive_numeric, wilder_series


def _true_range_series(group: pd.DataFrame) -> pd.Series:
    high = positive_numeric(group[HIGH])
    low = positive_numeric(group[LOW])
    close = positive_numeric(group[CLOSE])
    previous_close = close.shift(1)
    intraday = high - low
    valid = high.ge(low)
    ranges = pd.concat(
        [intraday, (high - previous_close).abs(), (low - previous_close).abs()], axis=1
    )
    result = ranges.max(axis=1, skipna=True).where(valid)
    return result.where(high.notna() & low.notna())


def true_range_values(df: pd.DataFrame) -> pd.Series:
    pieces = [_true_range_series(group) for _, group in df.groupby(TICKER, sort=False)]
    if not pieces:
        return pd.Series(index=df.index, dtype=float)
    return pd.concat(pieces).sort_index(kind="mergesort")


def true_range_calculator(
    df: pd.DataFrame, _: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Maximum of intraday range and gaps from the previous close."""

    return {"value": true_range_values(df)}


def atr_values(df: pd.DataFrame, window: int) -> pd.Series:
    tr = true_range_values(df)
    return tr.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: wilder_series(series, window)
    )


def atr_calculator(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """Wilder ATR, seeded with the arithmetic mean of the first window TR values."""

    return {"value": atr_values(df, int(parameters["window"]))}


def bollinger_bands_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """SMA bands using population standard deviation (ddof=0)."""

    window = int(parameters["window"])
    multiplier = float(parameters["multiplier"])
    close = positive_numeric(df[CLOSE])
    grouped = close.groupby(df[TICKER], sort=False)
    middle = grouped.transform(lambda s: s.rolling(window, min_periods=window).mean())
    std = grouped.transform(lambda s: s.rolling(window, min_periods=window).std(ddof=0))
    upper = middle + multiplier * std
    lower = middle - multiplier * std
    bandwidth = (upper - lower).div(middle.where(middle > 0.0))
    width = upper - lower
    percent_b = (close - lower).div(width.where(width > 0.0))
    return {
        "middle": middle,
        "upper": upper,
        "lower": lower,
        "bandwidth": bandwidth,
        "percent_b": percent_b,
    }


def keltner_channel_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """EMA center with Wilder ATR envelopes, all including the current bar."""

    ema_window = int(parameters["ema_window"])
    atr_window = int(parameters["atr_window"])
    multiplier = float(parameters["multiplier"])
    close = positive_numeric(df[CLOSE])
    middle = close.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: ema_series(series, ema_window)
    )
    atr = atr_values(df, atr_window)
    return {
        "middle": middle,
        "upper": middle + multiplier * atr,
        "lower": middle - multiplier * atr,
        "atr": atr,
    }


def _simple_returns(df: pd.DataFrame) -> pd.Series:
    close = positive_numeric(df[CLOSE])
    values = close.groupby(df[TICKER], sort=False).transform(
        lambda series: series.div(series.shift(1)).sub(1.0)
    )
    return values.where(np.isfinite(values))


def return_volatility_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Population standard deviation of simple returns, annualized by sqrt(scale)."""

    window = int(parameters["window"])
    annualization = float(parameters["annualization"])
    returns = _simple_returns(df)
    values = returns.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).std(ddof=0)
    ) * math.sqrt(annualization)
    return {"value": values}


def downside_volatility_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Root mean square of returns below the configurable target, annualized."""

    window = int(parameters["window"])
    annualization = float(parameters["annualization"])
    target = float(parameters["target"])
    returns = _simple_returns(df)
    downside_square = (returns - target).clip(upper=0.0).pow(2)
    values = downside_square.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).mean()
    ).pow(0.5) * math.sqrt(annualization)
    return {"value": values}


def rolling_current_drawdown_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Current close divided by the maximum close in the inclusive rolling window, minus one."""

    window = int(parameters["window"])
    close = positive_numeric(df[CLOSE])
    peak = close.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).max()
    )
    return {"value": close.div(peak.where(peak > 0.0)).sub(1.0)}


def _window_max_drawdown(raw: np.ndarray) -> float:
    if not np.isfinite(raw).all() or np.any(raw <= 0.0):
        return math.nan
    peak = np.maximum.accumulate(raw)
    return float(np.min(raw / peak - 1.0))


def rolling_max_drawdown_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Worst peak-to-trough drawdown observed inside each inclusive rolling window."""

    window = int(parameters["window"])
    close = positive_numeric(df[CLOSE])
    values = close.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).apply(
            _window_max_drawdown, raw=True
        )
    )
    return {"value": values}


def _window_ulcer_index(raw: np.ndarray) -> float:
    if not np.isfinite(raw).all() or np.any(raw <= 0.0):
        return math.nan
    peak = np.maximum.accumulate(raw)
    drawdown_pct = 100.0 * (raw / peak - 1.0)
    return float(np.sqrt(np.mean(np.square(drawdown_pct))))


def ulcer_index_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Root-mean-square percentage drawdown within each inclusive rolling window."""

    window = int(parameters["window"])
    close = positive_numeric(df[CLOSE])
    values = close.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).apply(
            _window_ulcer_index, raw=True
        )
    )
    return {"value": values}


def atr_breakout_bands_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Prior-close bands using prior Wilder ATR; the current bar is excluded."""

    window = int(parameters["window"])
    multiplier = float(parameters["multiplier"])
    close = positive_numeric(df[CLOSE])
    atr = atr_values(df, window)
    previous_close = close.groupby(df[TICKER], sort=False).shift(1)
    previous_atr = atr.groupby(df[TICKER], sort=False).shift(1)
    return {
        "upper": previous_close + multiplier * previous_atr,
        "lower": previous_close - multiplier * previous_atr,
        "atr": previous_atr,
    }


__all__ = [
    "atr_breakout_bands_calculator",
    "atr_calculator",
    "atr_values",
    "bollinger_bands_calculator",
    "downside_volatility_calculator",
    "keltner_channel_calculator",
    "return_volatility_calculator",
    "rolling_current_drawdown_calculator",
    "rolling_max_drawdown_calculator",
    "true_range_calculator",
    "true_range_values",
    "ulcer_index_calculator",
]
