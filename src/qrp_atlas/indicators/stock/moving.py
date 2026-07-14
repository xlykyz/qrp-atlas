"""Classic trend and moving-average indicator calculations.

All calculations consume an already ordered single/multi-asset price frame.
They are objective after-close facts: a value on trading day T may only be used
for a T+1-or-later decision unless the caller has a separate intraday contract.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import CLOSE, TICKER

from ._classic_utils import ema_series, finite_numeric


def ema_calculator(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """EMA seeded by the SMA of the first ``window`` consecutive values."""

    window = int(parameters["window"])
    close = finite_numeric(df[CLOSE])
    values = close.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: ema_series(series, window)
    )
    return {"value": values}


def wma_calculator(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """Linearly weighted moving average with weights 1..window."""

    window = int(parameters["window"])
    weights = np.arange(1.0, window + 1.0)
    denominator = float(weights.sum())
    close = finite_numeric(df[CLOSE])
    values = close.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).apply(
            lambda raw: float(np.dot(raw, weights) / denominator), raw=True
        )
    )
    return {"value": values}


def macd_calculator(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """MACD line, signal and histogram using SMA-seeded EMAs."""

    fast = int(parameters["fast_window"])
    slow = int(parameters["slow_window"])
    signal_window = int(parameters["signal_window"])
    close = finite_numeric(df[CLOSE])

    fast_ema = close.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: ema_series(series, fast)
    )
    slow_ema = close.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: ema_series(series, slow)
    )
    line = fast_ema - slow_ema
    signal = line.groupby(df[TICKER], sort=False, group_keys=False).transform(
        lambda series: ema_series(series, signal_window)
    )
    return {"line": line, "signal": signal, "histogram": line - signal}


def _regression_window(raw: np.ndarray) -> tuple[float, float, float]:
    if not np.isfinite(raw).all():
        return math.nan, math.nan, math.nan
    x = np.arange(len(raw), dtype=float)
    x_centered = x - x.mean()
    y_centered = raw - raw.mean()
    denominator = float(np.dot(x_centered, x_centered))
    if denominator <= 0.0:
        return math.nan, math.nan, math.nan
    slope = float(np.dot(x_centered, y_centered) / denominator)
    scale = float(np.mean(np.abs(raw)))
    normalized = slope / scale if scale > 0.0 else math.nan
    fitted_centered = slope * x_centered
    total = float(np.dot(y_centered, y_centered))
    if total <= 0.0:
        r_squared = math.nan
    else:
        residual = y_centered - fitted_centered
        r_squared = 1.0 - float(np.dot(residual, residual)) / total
        r_squared = min(1.0, max(0.0, r_squared))
    return slope, normalized, r_squared


def linear_regression_trend_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Rolling OLS slope, slope/mean(abs(price)), and coefficient of determination."""

    window = int(parameters["window"])
    close = finite_numeric(df[CLOSE])
    slope = pd.Series(math.nan, index=df.index, dtype=float)
    normalized = slope.copy()
    r_squared = slope.copy()
    for _, indexes in df.groupby(TICKER, sort=False).groups.items():
        series = close.loc[indexes]
        rolling = series.rolling(window, min_periods=window)
        slope.loc[indexes] = rolling.apply(lambda raw: _regression_window(raw)[0], raw=True)
        normalized.loc[indexes] = rolling.apply(
            lambda raw: _regression_window(raw)[1], raw=True
        )
        r_squared.loc[indexes] = rolling.apply(
            lambda raw: _regression_window(raw)[2], raw=True
        )
    return {"slope": slope, "normalized_slope": normalized, "r_squared": r_squared}


def kaufman_efficiency_ratio_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Absolute net price change divided by path length over ``window`` bars."""

    window = int(parameters["window"])
    close = finite_numeric(df[CLOSE])

    def calculate(series: pd.Series) -> pd.Series:
        net = (series - series.shift(window)).abs()
        path = series.diff().abs().rolling(window, min_periods=window).sum()
        return net.div(path.where(path > 0.0))

    values = close.groupby(df[TICKER], sort=False, group_keys=False).transform(calculate)
    return {"value": values}


__all__ = [
    "ema_calculator",
    "kaufman_efficiency_ratio_calculator",
    "linear_regression_trend_calculator",
    "macd_calculator",
    "wma_calculator",
]
