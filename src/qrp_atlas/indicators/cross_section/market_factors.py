"""Classic cross-sectional market factors computed from prepared OHLCV panels.

The functions in this module produce raw, objective factor values only. They do
not rank assets, select Top-N names, create weights, or make trading decisions.
Every calculation is isolated by asset and uses bars dated no later than the
target date. Values that include the T-day close are intended for T+1 or later
use.
"""

from __future__ import annotations

import math
from collections.abc import Callable

import pandas as pd

from qrp_atlas.contracts import (
    AMOUNT,
    ASSET_ID,
    CLOSE,
    HIGH,
    LOW,
    TRADE_DATE,
    TURNOVER,
)
from qrp_atlas.indicators.cross_section.conventions import (
    empty_cross_section_frame,
    ensure_cross_section_frame,
    sort_cross_section_frame,
)
from qrp_atlas.indicators.cross_section.factors import (
    FactorRequestError,
    _as_finite_series,
    _attach_nan_column,
    _normalize_panel_keys,
    compute_momentum_factor,
)


def _validate_positive_integer(value: int, *, label: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise FactorRequestError(f"{label} must be a positive integer; got {value!r}")


def _validate_non_negative_integer(value: int, *, label: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise FactorRequestError(
            f"{label} must be a non-negative integer; got {value!r}"
        )


def _positive_finite(series: pd.Series) -> pd.Series:
    values = _as_finite_series(series)
    return values.where(values > 0)


def _non_negative_finite(series: pd.Series) -> pd.Series:
    values = _as_finite_series(series)
    return values.where(values >= 0)


def _compute_grouped_factor(
    prices: pd.DataFrame,
    *,
    universe: pd.DataFrame,
    required_columns: tuple[str, ...],
    output_column: str,
    calculator: Callable[[pd.DataFrame], pd.Series],
) -> pd.DataFrame:
    """Normalize one market panel, calculate by asset, and align to a universe."""
    uni = ensure_cross_section_frame(universe, enforce_primary_key=True)
    if uni.empty:
        return empty_cross_section_frame([output_column])

    panel = _normalize_panel_keys(
        prices,
        label="prices",
        required_value_columns=required_columns,
    )
    if panel.empty:
        return _attach_nan_column(uni, output_column)

    max_date = uni[TRADE_DATE].max()
    needed_assets = set(uni[ASSET_ID].tolist())
    panel = panel.loc[
        (panel[TRADE_DATE] <= max_date) & panel[ASSET_ID].isin(needed_assets)
    ].copy()
    if panel.empty:
        return _attach_nan_column(uni, output_column)

    panel = panel.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort")
    pieces: list[pd.DataFrame] = []
    for _, group in panel.groupby(ASSET_ID, sort=False):
        values = calculator(group)
        piece = group[[TRADE_DATE, ASSET_ID]].copy()
        piece[output_column] = _as_finite_series(values).to_numpy()
        pieces.append(piece)

    computed = pd.concat(pieces, ignore_index=True)
    computed = ensure_cross_section_frame(
        computed,
        feature_columns=[output_column],
        enforce_primary_key=True,
    )
    out = uni[[TRADE_DATE, ASSET_ID]].merge(
        computed[[TRADE_DATE, ASSET_ID, output_column]],
        on=[TRADE_DATE, ASSET_ID],
        how="left",
    )
    out[output_column] = _as_finite_series(out[output_column])
    return sort_cross_section_frame(out)


def compute_intermediate_momentum_factor(
    prices: pd.DataFrame,
    *,
    universe: pd.DataFrame,
    lookback: int = 252,
    skip_recent: int = 21,
    output_column: str = "intermediate_momentum",
    price_column: str = CLOSE,
) -> pd.DataFrame:
    """Return from T-lookback to T-skip_recent, supporting classic 12-1.

    Formula: ``close[T-skip_recent] / close[T-lookback] - 1``. Both endpoints
    must be finite and strictly positive. Intermediate missing observations keep
    their bar positions and do not change the endpoint definition.
    """
    _validate_positive_integer(lookback, label="intermediate_momentum lookback")
    _validate_non_negative_integer(
        skip_recent, label="intermediate_momentum skip_recent"
    )
    if lookback <= skip_recent:
        raise FactorRequestError(
            "intermediate_momentum lookback must be greater than skip_recent"
        )

    def _calculate(group: pd.DataFrame) -> pd.Series:
        close = _positive_finite(group[price_column])
        start = close.shift(lookback)
        end = close.shift(skip_recent)
        return end.div(start).sub(1.0).replace([math.inf, -math.inf], math.nan)

    return _compute_grouped_factor(
        prices,
        universe=universe,
        required_columns=(price_column,),
        output_column=output_column,
        calculator=_calculate,
    )


def compute_short_term_reversal_factor(
    prices: pd.DataFrame,
    *,
    universe: pd.DataFrame,
    lookback: int = 5,
    output_column: str = "short_term_reversal",
    price_column: str = CLOSE,
) -> pd.DataFrame:
    """Raw trailing short-horizon return used by reversal research.

    Formula: ``close[T] / close[T-lookback] - 1``. The raw return is preserved;
    lower values indicate larger recent losses and therefore stronger classic
    reversal candidates. No sign flip, rank, or selection is applied here.
    """
    _validate_positive_integer(lookback, label="short_term_reversal lookback")
    return compute_momentum_factor(
        prices,
        universe=universe,
        lookback=lookback,
        output_column=output_column,
        price_column=price_column,
    )


def compute_distance_to_high_factor(
    prices: pd.DataFrame,
    *,
    universe: pd.DataFrame,
    lookback: int = 252,
    output_column: str = "distance_to_high",
    price_column: str = CLOSE,
    high_column: str = HIGH,
) -> pd.DataFrame:
    """Distance from T close to the inclusive rolling high.

    Formula: ``close[T] / max(high[T-lookback+1:T]) - 1``. Values are at most
    zero for internally consistent OHLC data; values closer to zero indicate
    stronger proximity to the stage high.
    """
    _validate_positive_integer(lookback, label="distance_to_high lookback")

    def _calculate(group: pd.DataFrame) -> pd.Series:
        close = _positive_finite(group[price_column])
        high = _positive_finite(group[high_column])
        rolling_high = high.rolling(lookback, min_periods=lookback).max()
        return close.div(rolling_high).sub(1.0).replace(
            [math.inf, -math.inf], math.nan
        )

    return _compute_grouped_factor(
        prices,
        universe=universe,
        required_columns=(price_column, high_column),
        output_column=output_column,
        calculator=_calculate,
    )


def compute_high_low_range_volatility_factor(
    prices: pd.DataFrame,
    *,
    universe: pd.DataFrame,
    lookback: int = 20,
    output_column: str = "high_low_range_volatility",
    high_column: str = HIGH,
    low_column: str = LOW,
    price_column: str = CLOSE,
) -> pd.DataFrame:
    """Rolling mean of the close-normalized daily high-low range.

    Formula: ``mean((high-low)/close, lookback)`` with population-free simple
    averaging. Every bar in the window must have finite positive OHLC values and
    ``high >= low``.
    """
    _validate_positive_integer(
        lookback, label="high_low_range_volatility lookback"
    )

    def _calculate(group: pd.DataFrame) -> pd.Series:
        high = _positive_finite(group[high_column])
        low = _positive_finite(group[low_column])
        close = _positive_finite(group[price_column])
        valid = high.notna() & low.notna() & close.notna() & high.ge(low)
        daily_range = high.sub(low).div(close).where(valid)
        return daily_range.rolling(lookback, min_periods=lookback).mean()

    return _compute_grouped_factor(
        prices,
        universe=universe,
        required_columns=(high_column, low_column, price_column),
        output_column=output_column,
        calculator=_calculate,
    )


def compute_average_turnover_factor(
    prices: pd.DataFrame,
    *,
    universe: pd.DataFrame,
    lookback: int = 20,
    output_column: str = "average_turnover",
    turnover_column: str = TURNOVER,
) -> pd.DataFrame:
    """Rolling arithmetic mean of non-negative turnover observations."""
    _validate_positive_integer(lookback, label="average_turnover lookback")

    def _calculate(group: pd.DataFrame) -> pd.Series:
        turnover = _non_negative_finite(group[turnover_column])
        return turnover.rolling(lookback, min_periods=lookback).mean()

    return _compute_grouped_factor(
        prices,
        universe=universe,
        required_columns=(turnover_column,),
        output_column=output_column,
        calculator=_calculate,
    )


def compute_turnover_change_factor(
    prices: pd.DataFrame,
    *,
    universe: pd.DataFrame,
    recent_window: int = 20,
    prior_window: int = 20,
    output_column: str = "turnover_change",
    turnover_column: str = TURNOVER,
) -> pd.DataFrame:
    """Change in mean turnover versus the immediately preceding window.

    Formula: ``mean(turnover[T-recent+1:T]) /
    mean(turnover[T-recent-prior+1:T-recent]) - 1``. A zero prior mean yields
    NaN rather than infinity.
    """
    _validate_positive_integer(
        recent_window, label="turnover_change recent_window"
    )
    _validate_positive_integer(prior_window, label="turnover_change prior_window")

    def _calculate(group: pd.DataFrame) -> pd.Series:
        turnover = _non_negative_finite(group[turnover_column])
        recent = turnover.rolling(recent_window, min_periods=recent_window).mean()
        prior = (
            turnover.shift(recent_window)
            .rolling(prior_window, min_periods=prior_window)
            .mean()
        )
        prior = prior.where(prior > 0)
        return recent.div(prior).sub(1.0).replace(
            [math.inf, -math.inf], math.nan
        )

    return _compute_grouped_factor(
        prices,
        universe=universe,
        required_columns=(turnover_column,),
        output_column=output_column,
        calculator=_calculate,
    )


def compute_average_traded_amount_factor(
    prices: pd.DataFrame,
    *,
    universe: pd.DataFrame,
    lookback: int = 20,
    output_column: str = "average_traded_amount",
    amount_column: str = AMOUNT,
) -> pd.DataFrame:
    """Rolling arithmetic mean of non-negative traded amount."""
    _validate_positive_integer(
        lookback, label="average_traded_amount lookback"
    )

    def _calculate(group: pd.DataFrame) -> pd.Series:
        amount = _non_negative_finite(group[amount_column])
        return amount.rolling(lookback, min_periods=lookback).mean()

    return _compute_grouped_factor(
        prices,
        universe=universe,
        required_columns=(amount_column,),
        output_column=output_column,
        calculator=_calculate,
    )


__all__ = [
    "compute_average_traded_amount_factor",
    "compute_average_turnover_factor",
    "compute_distance_to_high_factor",
    "compute_high_low_range_volatility_factor",
    "compute_intermediate_momentum_factor",
    "compute_short_term_reversal_factor",
    "compute_turnover_change_factor",
]
