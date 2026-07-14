"""Classic volume, money-flow, liquidity, and price-volume indicators."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import AMOUNT, CLOSE, HIGH, LOW, TICKER, VOLUME

from ._classic_utils import nonnegative_numeric, positive_numeric


def obv_calculator(df: pd.DataFrame, _: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """On-Balance Volume, initialized at zero for each asset's first valid bar."""

    close = positive_numeric(df[CLOSE])
    volume = nonnegative_numeric(df[VOLUME])
    output = pd.Series(math.nan, index=df.index, dtype=float)
    for _, indexes in df.groupby(TICKER, sort=False).groups.items():
        running = 0.0
        previous_close = math.nan
        for index in indexes:
            current_close = close.loc[index]
            current_volume = volume.loc[index]
            if pd.isna(current_close) or pd.isna(current_volume):
                output.loc[index] = math.nan
                continue
            if math.isfinite(previous_close):
                if current_close > previous_close:
                    running += float(current_volume)
                elif current_close < previous_close:
                    running -= float(current_volume)
            output.loc[index] = running
            previous_close = float(current_close)
    return {"value": output}


def rolling_vwap_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Rolling close-price VWAP; zero total volume yields NaN."""

    window = int(parameters["window"])
    close = positive_numeric(df[CLOSE])
    volume = nonnegative_numeric(df[VOLUME])
    price_volume = close * volume
    numerator = price_volume.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).sum()
    )
    denominator = volume.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).sum()
    )
    return {"value": numerator.div(denominator.where(denominator > 0.0))}


def volume_sma_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Simple moving average of non-negative volume."""

    window = int(parameters["window"])
    volume = nonnegative_numeric(df[VOLUME])
    values = volume.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).mean()
    )
    return {"value": values}


def relative_volume_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Current volume divided by the mean of the prior ``window`` bars."""

    window = int(parameters["window"])
    volume = nonnegative_numeric(df[VOLUME])
    prior_mean = volume.groupby(df[TICKER], sort=False).transform(
        lambda series: series.shift(1).rolling(window, min_periods=window).mean()
    )
    return {"value": volume.div(prior_mean.where(prior_mean > 0.0))}


def mfi_calculator(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """Money Flow Index from typical price and non-negative volume."""

    window = int(parameters["window"])
    high = positive_numeric(df[HIGH])
    low = positive_numeric(df[LOW])
    close = positive_numeric(df[CLOSE])
    volume = nonnegative_numeric(df[VOLUME])
    typical = ((high + low + close) / 3.0).where(high.ge(low))
    raw_flow = typical * volume
    change = typical.groupby(df[TICKER], sort=False).diff()
    positive_flow = raw_flow.where(change > 0.0, 0.0)
    negative_flow = raw_flow.where(change < 0.0, 0.0)
    invalid = raw_flow.isna() | change.isna()
    positive_flow = positive_flow.where(~invalid)
    negative_flow = negative_flow.where(~invalid)
    first = df.groupby(TICKER, sort=False).cumcount().eq(0) & raw_flow.notna()
    positive_flow = positive_flow.mask(first, 0.0)
    negative_flow = negative_flow.mask(first, 0.0)
    positive_sum = positive_flow.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).sum()
    )
    negative_sum = negative_flow.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).sum()
    )
    total = positive_sum + negative_sum
    values = 100.0 * positive_sum.div(total.where(total > 0.0))
    return {"value": values}


def cmf_calculator(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """Chaikin Money Flow using the close-location multiplier."""

    window = int(parameters["window"])
    high = positive_numeric(df[HIGH])
    low = positive_numeric(df[LOW])
    close = positive_numeric(df[CLOSE])
    volume = nonnegative_numeric(df[VOLUME])
    width = high - low
    multiplier = ((close - low) - (high - close)).div(width.where(width > 0.0))
    money_flow_volume = multiplier * volume
    numerator = money_flow_volume.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).sum()
    )
    denominator = volume.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).sum()
    )
    return {"value": numerator.div(denominator.where(denominator > 0.0))}


def amihud_illiquidity_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Rolling mean of absolute simple return divided by positive traded amount."""

    window = int(parameters["window"])
    scale = float(parameters["scale"])
    close = positive_numeric(df[CLOSE])
    amount = positive_numeric(df[AMOUNT])
    returns = close.groupby(df[TICKER], sort=False).transform(
        lambda series: series.div(series.shift(1)).sub(1.0)
    )
    daily = returns.abs().div(amount) * scale
    daily = daily.where(np.isfinite(daily))
    values = daily.groupby(df[TICKER], sort=False).transform(
        lambda series: series.rolling(window, min_periods=window).mean()
    )
    return {"value": values}


def price_volume_correlation_calculator(
    df: pd.DataFrame, parameters: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Rolling Pearson correlation of close returns and volume percentage changes."""

    window = int(parameters["window"])
    close = positive_numeric(df[CLOSE])
    volume = nonnegative_numeric(df[VOLUME])
    price_return = close.groupby(df[TICKER], sort=False).transform(
        lambda series: series.div(series.shift(1)).sub(1.0)
    ).where(lambda series: np.isfinite(series))
    volume_change = volume.groupby(df[TICKER], sort=False).transform(
        lambda series: series.div(series.shift(1)).sub(1.0)
    ).where(lambda series: np.isfinite(series))
    output = pd.Series(math.nan, index=df.index, dtype=float)
    for _, indexes in df.groupby(TICKER, sort=False).groups.items():
        output.loc[indexes] = price_return.loc[indexes].rolling(
            window, min_periods=window
        ).corr(volume_change.loc[indexes])
    return {"value": output}


__all__ = [
    "amihud_illiquidity_calculator",
    "cmf_calculator",
    "mfi_calculator",
    "obv_calculator",
    "price_volume_correlation_calculator",
    "relative_volume_calculator",
    "rolling_vwap_calculator",
    "volume_sma_calculator",
]
