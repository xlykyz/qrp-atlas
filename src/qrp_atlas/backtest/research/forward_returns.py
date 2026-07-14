"""Forward-return labels for post-hoc cross-sectional research."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    ensure_cross_section_frame,
    normalize_trade_date,
    normalize_trade_dates,
    sort_cross_section_frame,
)

DEFAULT_FORWARD_HORIZONS: tuple[int, ...] = (1, 5, 20)


class ForwardReturnError(ValueError):
    """Raised when forward returns cannot be computed."""


def forward_return_column(horizon: int) -> str:
    """Return the canonical forward-return column name for ``horizon`` days."""
    if not isinstance(horizon, int) or isinstance(horizon, bool) or horizon <= 0:
        raise ForwardReturnError("horizon must be a positive integer")
    return f"forward_return_{horizon}d"


def compute_forward_returns(
    price_df: pd.DataFrame,
    *,
    trading_days: Sequence[Any],
    horizons: Sequence[int] = DEFAULT_FORWARD_HORIZONS,
    price_field: str = "close",
    as_of_dates: Sequence[Any] | None = None,
    assets: Sequence[Any] | None = None,
) -> pd.DataFrame:
    """Compute calendar-aligned forward returns for research evaluation only.

    Final semantics:

    ```text
    forward_return_h[T] = price[T+h] / price[T] - 1
    ```

    ``T+h`` is resolved on the caller-provided full-market trading calendar,
    never by shifting a single-asset record index. Missing or non-positive
    endpoint prices become NaN without forward-filling alternative dates.
    """
    if price_df is None or not isinstance(price_df, pd.DataFrame):
        raise ForwardReturnError("price_df must be a pandas DataFrame")
    if price_field not in price_df.columns:
        raise ForwardReturnError(f"price_df missing price field: {price_field!r}")

    horizon_list = _normalize_horizons(horizons)
    try:
        calendar = sorted(normalize_trade_dates(trading_days))
    except CrossSectionFrameError as exc:
        raise ForwardReturnError(str(exc)) from exc
    if not calendar:
        raise ForwardReturnError("trading_days must be non-empty")
    calendar_index = {day: idx for idx, day in enumerate(calendar)}

    try:
        prices = ensure_cross_section_frame(
            price_df,
            feature_columns=(price_field,),
            copy=True,
            enforce_primary_key=True,
        )
    except CrossSectionFrameError as exc:
        raise ForwardReturnError(str(exc)) from exc

    if as_of_dates is None:
        signal_dates = sorted({normalize_trade_date(value) for value in prices[TRADE_DATE]})
    else:
        try:
            signal_dates = sorted(normalize_trade_dates(as_of_dates))
        except CrossSectionFrameError as exc:
            raise ForwardReturnError(str(exc)) from exc

    if assets is None:
        asset_ids = sorted({str(value) for value in prices[ASSET_ID].tolist()})
    else:
        asset_ids = sorted({str(value).strip() for value in assets if str(value).strip()})

    columns = [TRADE_DATE, ASSET_ID, *[forward_return_column(h) for h in horizon_list]]
    if prices.empty or not signal_dates or not asset_ids:
        return _empty_forward_frame(horizon_list)

    price_map: dict[tuple[pd.Timestamp, str], float] = {}
    numeric = pd.to_numeric(prices[price_field], errors="coerce")
    for trade_date, asset_id, raw in zip(
        prices[TRADE_DATE].tolist(),
        prices[ASSET_ID].tolist(),
        numeric.tolist(),
        strict=True,
    ):
        value = _finite_positive(raw)
        if value is None:
            continue
        price_map[(normalize_trade_date(trade_date), str(asset_id))] = value

    rows: list[dict[str, Any]] = []
    for signal in signal_dates:
        if signal not in calendar_index:
            # Signal dates outside the provided market calendar cannot resolve
            # T+h; emit NaNs for requested assets without inventing offsets.
            base_idx = None
        else:
            base_idx = calendar_index[signal]
        for asset_id in asset_ids:
            row: dict[str, Any] = {TRADE_DATE: signal, ASSET_ID: asset_id}
            start_price = price_map.get((signal, asset_id))
            for horizon in horizon_list:
                col = forward_return_column(horizon)
                if start_price is None or base_idx is None:
                    row[col] = math.nan
                    continue
                end_idx = base_idx + horizon
                if end_idx >= len(calendar):
                    row[col] = math.nan
                    continue
                end_date = calendar[end_idx]
                end_price = price_map.get((end_date, asset_id))
                if end_price is None:
                    row[col] = math.nan
                    continue
                row[col] = end_price / start_price - 1.0
            rows.append(row)

    out = pd.DataFrame(rows, columns=columns)
    return sort_cross_section_frame(out)


def _normalize_horizons(horizons: Sequence[int]) -> list[int]:
    if horizons is None:
        raise ForwardReturnError("horizons must be non-empty")
    if isinstance(horizons, (str, bytes)) or not isinstance(horizons, Sequence):
        raise ForwardReturnError("horizons must be a sequence of positive integers")
    ordered: list[int] = []
    seen: set[int] = set()
    for value in horizons:
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ForwardReturnError("horizon must be a positive integer")
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    if not ordered:
        raise ForwardReturnError("horizons must be non-empty")
    return ordered


def _finite_positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def _empty_forward_frame(horizons: Sequence[int]) -> pd.DataFrame:
    columns = [TRADE_DATE, ASSET_ID, *[forward_return_column(h) for h in horizons]]
    frame = pd.DataFrame(columns=columns)
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    frame[ASSET_ID] = pd.Series(dtype=object)
    for horizon in horizons:
        frame[forward_return_column(horizon)] = pd.Series(dtype=float)
    return frame
