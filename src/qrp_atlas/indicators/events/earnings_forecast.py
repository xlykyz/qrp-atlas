"""Earnings-forecast event indicators (objective facts only).

These helpers consume EventFrame / as_of query outputs from 05-A. They do not
query Tushare, open DuckDB, or recompute available_trade_date.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

# Indicator codes (stable registry identities)
PROFIT_CHANGE_MIDPOINT = "profit_change_midpoint"
PROFIT_CHANGE_RANGE = "profit_change_range"
NET_PROFIT_MIDPOINT = "net_profit_midpoint"
NET_PROFIT_RANGE = "net_profit_range"
DIRECTION_SCORE = "direction_score"
EVENT_AGE = "event_age"
EVENT_WINDOW = "event_window"

# Real Tushare forecast_type values observed on forecast_vip(period=20231231)
# plus conservative extensions documented for research stability.
DIRECTION_SCORE_BY_FORECAST_TYPE: dict[str, int] = {
    # positive
    "预增": 1,
    "略增": 1,
    "扭亏": 1,
    "续盈": 1,
    "首盈": 1,
    "减亏": 1,
    # negative
    "预减": -1,
    "略减": -1,
    "续亏": -1,
    "首亏": -1,
    # neutral / uncertain
    "不确定": 0,
    "持平": 0,
    "预警": 0,
}

UNKNOWN_FORECAST_TYPE_DIRECTION = 0


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _to_float(value: Any) -> float:
    if _is_missing(value):
        return math.nan
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    if not math.isfinite(number):
        return math.nan
    return number


def _midpoint(low: Any, high: Any) -> float:
    left = _to_float(low)
    right = _to_float(high)
    if math.isnan(left) or math.isnan(right):
        return math.nan
    return (left + right) / 2.0


def _range(low: Any, high: Any) -> float:
    left = _to_float(low)
    right = _to_float(high)
    if math.isnan(left) or math.isnan(right):
        return math.nan
    return right - left


def compute_profit_change_midpoint(min_value: Any, max_value: Any) -> float:
    """(profit_change_min + profit_change_max) / 2; NaN if either endpoint missing."""
    return _midpoint(min_value, max_value)


def compute_profit_change_range(min_value: Any, max_value: Any) -> float:
    """profit_change_max - profit_change_min; NaN if either endpoint missing."""
    return _range(min_value, max_value)


def compute_net_profit_midpoint(min_value: Any, max_value: Any) -> float:
    """(net_profit_min + net_profit_max) / 2; unit 万元; NaN if either missing."""
    return _midpoint(min_value, max_value)


def compute_net_profit_range(min_value: Any, max_value: Any) -> float:
    """net_profit_max - net_profit_min; unit 万元; NaN if either missing."""
    return _range(min_value, max_value)


def map_forecast_type_direction(forecast_type: Any) -> tuple[int, bool]:
    """Map forecast_type text to direction_score.

    Returns:
        (score, is_unknown)
        Unknown types get ``UNKNOWN_FORECAST_TYPE_DIRECTION`` (0) and
        ``is_unknown=True``. Never silently scores unknown as positive.
    """
    if _is_missing(forecast_type):
        return UNKNOWN_FORECAST_TYPE_DIRECTION, True
    text = str(forecast_type).strip()
    if not text:
        return UNKNOWN_FORECAST_TYPE_DIRECTION, True
    if text in DIRECTION_SCORE_BY_FORECAST_TYPE:
        return int(DIRECTION_SCORE_BY_FORECAST_TYPE[text]), False
    return UNKNOWN_FORECAST_TYPE_DIRECTION, True


def compute_direction_score(forecast_type: Any) -> int:
    score, _ = map_forecast_type_direction(forecast_type)
    return score


def _normalize_open_dates(open_dates: Sequence[Any]) -> list[pd.Timestamp]:
    if open_dates is None:
        raise ValueError("open_dates is required")
    out: list[pd.Timestamp] = []
    for value in open_dates:
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            continue
        if ts.tzinfo is not None:
            ts = ts.tz_localize(None)
        out.append(ts.normalize())
    uniq = sorted(set(out))
    if not uniq:
        raise ValueError("open_dates must contain at least one valid trading day")
    return uniq


def compute_event_age(
    trade_date: Any,
    available_trade_date: Any,
    *,
    open_dates: Sequence[Any],
) -> float:
    """Open-day distance from available_trade_date to trade_date.

    available_trade_date itself is age 0. Dates before availability or outside
    the open calendar yield NaN.
    """
    calendar = _normalize_open_dates(open_dates)
    as_of = pd.Timestamp(trade_date)
    avail = pd.Timestamp(available_trade_date)
    if pd.isna(as_of) or pd.isna(avail):
        return math.nan
    as_of = as_of.tz_localize(None).normalize() if as_of.tzinfo else as_of.normalize()
    avail = avail.tz_localize(None).normalize() if avail.tzinfo else avail.normalize()
    index = {day: i for i, day in enumerate(calendar)}
    if as_of not in index or avail not in index:
        return math.nan
    age = index[as_of] - index[avail]
    if age < 0:
        return math.nan
    return float(age)


def compute_event_window(
    trade_date: Any,
    available_trade_date: Any,
    *,
    open_dates: Sequence[Any],
    window_days: int,
) -> bool | float:
    """True when 0 <= event_age < window_days; NaN when age is undefined."""
    if not isinstance(window_days, int) or isinstance(window_days, bool) or window_days <= 0:
        raise ValueError("window_days must be a positive integer")
    age = compute_event_age(trade_date, available_trade_date, open_dates=open_dates)
    if math.isnan(age):
        return math.nan
    return bool(0.0 <= age < float(window_days))


def attach_earnings_forecast_indicators(
    events: pd.DataFrame,
    *,
    trade_date: Any | None = None,
    open_dates: Sequence[Any] | None = None,
    window_days: int = 5,
    copy: bool = True,
) -> tuple[pd.DataFrame, list[str]]:
    """Attach all 05-B event indicators to an earnings-forecast EventFrame.

    Args:
        events: EventFrame-like frame with 05-A fields.
        trade_date: Optional evaluation trade date for event_age/window.
        open_dates: Required when trade_date is provided.
        window_days: Window length for event_window.
        copy: When True, do not mutate the input frame.

    Returns:
        (frame, diagnostics)
    """
    if events is None or not isinstance(events, pd.DataFrame):
        raise TypeError("events must be a pandas DataFrame")
    out = events.copy() if copy else events
    diagnostics: list[str] = []

    required = ("profit_change_min", "profit_change_max", "net_profit_min", "net_profit_max")
    for col in required:
        if col not in out.columns:
            out[col] = math.nan

    if "forecast_type" not in out.columns:
        out["forecast_type"] = None

    out[PROFIT_CHANGE_MIDPOINT] = [
        compute_profit_change_midpoint(a, b)
        for a, b in zip(out["profit_change_min"], out["profit_change_max"], strict=True)
    ]
    out[PROFIT_CHANGE_RANGE] = [
        compute_profit_change_range(a, b)
        for a, b in zip(out["profit_change_min"], out["profit_change_max"], strict=True)
    ]
    out[NET_PROFIT_MIDPOINT] = [
        compute_net_profit_midpoint(a, b)
        for a, b in zip(out["net_profit_min"], out["net_profit_max"], strict=True)
    ]
    out[NET_PROFIT_RANGE] = [
        compute_net_profit_range(a, b)
        for a, b in zip(out["net_profit_min"], out["net_profit_max"], strict=True)
    ]

    scores: list[int] = []
    unknown_types: set[str] = set()
    for value in out["forecast_type"].tolist():
        score, unknown = map_forecast_type_direction(value)
        scores.append(score)
        if unknown:
            label = "<missing>" if _is_missing(value) or str(value).strip() == "" else str(value).strip()
            unknown_types.add(label)
    out[DIRECTION_SCORE] = scores
    if unknown_types:
        diagnostics.append(
            "unknown_forecast_type_default_direction_0:"
            + ",".join(sorted(unknown_types))
        )

    if trade_date is not None:
        if open_dates is None:
            raise ValueError("open_dates is required when trade_date is provided")
        if "available_trade_date" not in out.columns:
            raise ValueError("events missing available_trade_date")
        ages = [
            compute_event_age(trade_date, avail, open_dates=open_dates)
            for avail in out["available_trade_date"].tolist()
        ]
        out[EVENT_AGE] = ages
        out[EVENT_WINDOW] = [
            (math.nan if math.isnan(age) else bool(0.0 <= age < float(window_days)))
            for age in ages
        ]
    else:
        out[EVENT_AGE] = math.nan
        out[EVENT_WINDOW] = math.nan

    return out, diagnostics
