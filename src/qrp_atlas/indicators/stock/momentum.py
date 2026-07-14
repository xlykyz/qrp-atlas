"""Classic momentum, oscillator, and directional-movement indicators."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import CLOSE, HIGH, LOW, TICKER

from ._classic_utils import finite_numeric, positive_numeric, wilder_series
from .volatility import atr_values


def rsi_calculator(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """Wilder RSI seeded from the first window consecutive close changes."""

    window = int(parameters["window"])
    close = positive_numeric(df[CLOSE])
    delta = close.groupby(df[TICKER], sort=False).diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    average_gain = gain.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: wilder_series(series, window)
    )
    average_loss = loss.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: wilder_series(series, window)
    )
    denominator = average_gain + average_loss
    rsi = 100.0 * average_gain.div(denominator.where(denominator > 0.0))
    return {"value": rsi}


def _rolling_range(df: pd.DataFrame, window: int) -> tuple[pd.Series, pd.Series]:
    high = positive_numeric(df[HIGH])
    low = positive_numeric(df[LOW])
    valid = high.ge(low)
    high = high.where(valid)
    low = low.where(valid)
    rolling_high = high.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).max()
    )
    rolling_low = low.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).min()
    )
    return rolling_high, rolling_low


def stochastic_oscillator_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Fast %K and simple-moving-average %D on inclusive high/low ranges."""

    window = int(parameters["window"])
    d_window = int(parameters["d_window"])
    close = positive_numeric(df[CLOSE])
    high, low = _rolling_range(df, window)
    width = high - low
    percent_k = 100.0 * (close - low).div(width.where(width > 0.0))
    percent_d = percent_k.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(d_window, min_periods=d_window).mean()
    )
    return {"percent_k": percent_k, "percent_d": percent_d}


def williams_r_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Williams %R on the inclusive rolling high/low range."""

    window = int(parameters["window"])
    close = positive_numeric(df[CLOSE])
    high, low = _rolling_range(df, window)
    width = high - low
    values = -100.0 * (high - close).div(width.where(width > 0.0))
    return {"value": values}


def cci_calculator(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """Commodity Channel Index using typical price and mean absolute deviation."""

    window = int(parameters["window"])
    constant = float(parameters["constant"])
    high = positive_numeric(df[HIGH])
    low = positive_numeric(df[LOW])
    close = positive_numeric(df[CLOSE])
    typical = ((high + low + close) / 3.0).where(high.ge(low))
    mean = typical.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).mean()
    )
    mean_deviation = typical.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).apply(
            lambda raw: float(np.mean(np.abs(raw - raw.mean()))), raw=True
        )
    )
    values = (typical - mean).div((constant * mean_deviation).where(mean_deviation > 0.0))
    return {"value": values}


def adx_calculator(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """Wilder ADX with +DI/-DI; zero directional denominator remains NaN."""

    window = int(parameters["window"])
    high = positive_numeric(df[HIGH])
    low = positive_numeric(df[LOW])
    valid_bar = high.ge(low)
    high = high.where(valid_bar)
    low = low.where(valid_bar)
    high_change = high.groupby(df[TICKER], sort=False).diff()
    low_change = -low.groupby(df[TICKER], sort=False).diff()
    plus_dm = high_change.where((high_change > low_change) & (high_change > 0.0), 0.0)
    minus_dm = low_change.where((low_change > high_change) & (low_change > 0.0), 0.0)
    invalid_move = high.isna() | low.isna()
    plus_dm = plus_dm.where(~invalid_move)
    minus_dm = minus_dm.where(~invalid_move)
    # The first valid bar has no direction by definition and contributes zero.
    first_in_group = df.groupby(TICKER, sort=False).cumcount().eq(0) & ~invalid_move
    plus_dm = plus_dm.mask(first_in_group, 0.0)
    minus_dm = minus_dm.mask(first_in_group, 0.0)

    smoothed_plus = plus_dm.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: wilder_series(series, window)
    )
    smoothed_minus = minus_dm.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: wilder_series(series, window)
    )
    atr = atr_values(df, window)
    plus_di = 100.0 * smoothed_plus.div(atr.where(atr > 0.0))
    minus_di = 100.0 * smoothed_minus.div(atr.where(atr > 0.0))
    directional_sum = plus_di + minus_di
    dx = 100.0 * (plus_di - minus_di).abs().div(directional_sum.where(directional_sum > 0.0))
    adx = dx.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: wilder_series(series, window)
    )
    return {"adx": adx, "plus_di": plus_di, "minus_di": minus_di}


__all__ = [
    "adx_calculator",
    "cci_calculator",
    "rsi_calculator",
    "stochastic_oscillator_calculator",
    "williams_r_calculator",
]
