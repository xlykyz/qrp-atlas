"""Internal numeric helpers shared by classic stock indicators."""

from __future__ import annotations

import math
from collections.abc import Callable

import numpy as np
import pandas as pd

from qrp_atlas.contracts import TICKER


def finite_numeric(series: pd.Series) -> pd.Series:
    """Coerce to float and replace all non-finite observations with NaN."""

    values = pd.to_numeric(series, errors="coerce").astype(float)
    return values.where(np.isfinite(values))


def positive_numeric(series: pd.Series) -> pd.Series:
    values = finite_numeric(series)
    return values.where(values > 0.0)


def nonnegative_numeric(series: pd.Series) -> pd.Series:
    values = finite_numeric(series)
    return values.where(values >= 0.0)


def seeded_recursive_average(series: pd.Series, period: int, alpha: float) -> pd.Series:
    """SMA-seeded recursive average, restarting after a missing/non-finite gap.

    The first value is emitted after ``period`` consecutive finite observations
    and equals their arithmetic mean. Later values use
    ``alpha * current + (1 - alpha) * previous``.
    """

    values = finite_numeric(series).to_numpy(dtype=float)
    output = np.full(len(values), np.nan, dtype=float)
    seed: list[float] = []
    state = math.nan
    for index, value in enumerate(values):
        if not math.isfinite(value):
            seed.clear()
            state = math.nan
            continue
        if not math.isfinite(state):
            seed.append(value)
            if len(seed) < period:
                continue
            state = float(np.mean(seed[-period:]))
        else:
            state = alpha * value + (1.0 - alpha) * state
        output[index] = state
    return pd.Series(output, index=series.index, dtype=float)


def ema_series(series: pd.Series, period: int) -> pd.Series:
    return seeded_recursive_average(series, period, 2.0 / (period + 1.0))


def wilder_series(series: pd.Series, period: int) -> pd.Series:
    return seeded_recursive_average(series, period, 1.0 / period)


def grouped_series(
    df: pd.DataFrame,
    source: str,
    function: Callable[[pd.Series], pd.Series],
) -> pd.Series:
    """Apply a same-index Series transform independently to each ticker."""

    values = finite_numeric(df[source])
    return values.groupby(df[TICKER], sort=False, group_keys=False).transform(function)


def grouped_frame_apply(
    df: pd.DataFrame,
    function: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """Apply a same-index frame transform independently to each ticker."""

    pieces = [function(group.copy()) for _, group in df.groupby(TICKER, sort=False)]
    if not pieces:
        return pd.DataFrame(index=df.index)
    return pd.concat(pieces).sort_index(kind="mergesort")
