"""Deterministic trading-day rebalance schedules for cross-sectional strategies."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Any, Literal

import pandas as pd

from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    normalize_trade_date,
    normalize_trade_dates,
)

REBALANCE_FREQUENCIES = ("daily", "weekly", "monthly", "explicit")
Frequency = Literal["daily", "weekly", "monthly", "explicit"]


class RebalanceScheduleError(ValueError):
    """Raised when a rebalance schedule cannot be built deterministically."""


def next_trading_day(
    trading_days: Sequence[Any],
    signal_date: Any,
) -> pd.Timestamp | None:
    """Return the first trading day strictly after ``signal_date``."""
    days = _sorted_trading_days(trading_days)
    signal = normalize_trade_date(signal_date)
    for day in days:
        if day > signal:
            return day
    return None


def build_rebalance_schedule(
    trading_days: Sequence[Any],
    *,
    frequency: Frequency = "daily",
    explicit_dates: Sequence[Any] | None = None,
    start_date: Any | None = None,
    end_date: Any | None = None,
) -> pd.DataFrame:
    """Build a deterministic rebalance schedule from a real trading calendar.

    Signal dates are always normalized, de-duplicated and sorted ascending
    before execution mapping, including ``frequency="explicit"``.

    Final schedule invariants:

    - ``signal_date`` unique and strictly ascending
    - ``trade_date`` unique and strictly ascending
    - every ``trade_date`` is strictly greater than its ``signal_date``
    """
    if frequency not in REBALANCE_FREQUENCIES:
        raise RebalanceScheduleError(
            f"unsupported rebalance frequency: {frequency!r}; "
            f"expected one of {list(REBALANCE_FREQUENCIES)}"
        )

    try:
        full_calendar = _sorted_trading_days(trading_days)
    except CrossSectionFrameError as exc:
        raise RebalanceScheduleError(str(exc)) from exc
    except RebalanceScheduleError:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise RebalanceScheduleError(str(exc)) from exc

    calendar = list(full_calendar)
    if start_date is not None:
        start = normalize_trade_date(start_date)
        calendar = [day for day in calendar if day >= start]
    if end_date is not None:
        end = normalize_trade_date(end_date)
        calendar = [day for day in calendar if day <= end]

    if not calendar:
        return _empty_schedule()

    if frequency == "daily":
        signal_dates = list(calendar)
    elif frequency == "weekly":
        signal_dates = _weekly_end_signals(calendar)
    elif frequency == "monthly":
        signal_dates = _monthly_end_signals(calendar)
    else:
        signal_dates = _explicit_signals(calendar, explicit_dates)

    # Canonical order: every frequency ends as unique ascending signal dates.
    signal_dates = sorted(set(signal_dates))

    rows: list[dict[str, pd.Timestamp]] = []
    for signal in signal_dates:
        execution = next_trading_day(full_calendar, signal)
        if execution is None:
            continue
        rows.append({"signal_date": signal, "trade_date": execution})

    if not rows:
        return _empty_schedule()
    out = pd.DataFrame(rows)
    out["signal_date"] = pd.to_datetime(out["signal_date"])
    out["trade_date"] = pd.to_datetime(out["trade_date"])
    out = out.sort_values(["signal_date", "trade_date"], kind="mergesort").reset_index(
        drop=True
    )
    _validate_schedule(out)
    return out


def _validate_schedule(schedule: pd.DataFrame) -> None:
    if schedule.empty:
        return
    signals = list(schedule["signal_date"])
    trades = list(schedule["trade_date"])
    if len(signals) != len(set(signals)):
        raise RebalanceScheduleError("signal_date values must be unique")
    if len(trades) != len(set(trades)):
        raise RebalanceScheduleError("trade_date values must be unique")
    if signals != sorted(signals):
        raise RebalanceScheduleError("signal_date values must be strictly ascending")
    if trades != sorted(trades):
        raise RebalanceScheduleError("trade_date values must be strictly ascending")
    for signal, trade in zip(signals, trades, strict=True):
        if not trade > signal:
            raise RebalanceScheduleError(
                "trade_date must be strictly after signal_date: "
                f"{pd.Timestamp(signal).strftime('%Y-%m-%d')} -> "
                f"{pd.Timestamp(trade).strftime('%Y-%m-%d')}"
            )
    # Strict ascending implies uniqueness for total order; still ensure no equals.
    for left, right in zip(signals, signals[1:], strict=False):
        if not right > left:
            raise RebalanceScheduleError("signal_date values must be strictly ascending")
    for left, right in zip(trades, trades[1:], strict=False):
        if not right > left:
            raise RebalanceScheduleError("trade_date values must be strictly ascending")


def _empty_schedule() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "signal_date": pd.Series(dtype="datetime64[ns]"),
            "trade_date": pd.Series(dtype="datetime64[ns]"),
        }
    )


def _sorted_trading_days(trading_days: Sequence[Any]) -> list[pd.Timestamp]:
    if trading_days is None:
        return []
    if isinstance(trading_days, pd.DataFrame):
        if "trade_date" not in trading_days.columns:
            raise RebalanceScheduleError(
                "trading_days DataFrame must contain a 'trade_date' column"
            )
        values = trading_days["trade_date"].tolist()
    elif isinstance(trading_days, pd.Series):
        values = trading_days.tolist()
    elif isinstance(trading_days, (str, bytes, date, datetime, pd.Timestamp)):
        values = [trading_days]
    elif isinstance(trading_days, Sequence):
        values = list(trading_days)
    else:
        values = list(trading_days)

    try:
        days = normalize_trade_dates(values)
    except CrossSectionFrameError as exc:
        raise RebalanceScheduleError(str(exc)) from exc
    return sorted(days)


def _weekly_end_signals(calendar: Sequence[pd.Timestamp]) -> list[pd.Timestamp]:
    if not calendar:
        return []
    buckets: dict[tuple[int, int], pd.Timestamp] = {}
    for day in calendar:
        iso = day.isocalendar()
        key = (int(iso.year), int(iso.week))
        previous = buckets.get(key)
        if previous is None or day > previous:
            buckets[key] = day
    return [buckets[key] for key in sorted(buckets)]


def _monthly_end_signals(calendar: Sequence[pd.Timestamp]) -> list[pd.Timestamp]:
    if not calendar:
        return []
    buckets: dict[tuple[int, int], pd.Timestamp] = {}
    for day in calendar:
        key = (int(day.year), int(day.month))
        previous = buckets.get(key)
        if previous is None or day > previous:
            buckets[key] = day
    return [buckets[key] for key in sorted(buckets)]


def _explicit_signals(
    calendar: Sequence[pd.Timestamp],
    explicit_dates: Sequence[Any] | None,
) -> list[pd.Timestamp]:
    if explicit_dates is None:
        raise RebalanceScheduleError(
            "explicit_dates is required when frequency='explicit'"
        )
    try:
        requested = normalize_trade_dates(explicit_dates)
    except CrossSectionFrameError as exc:
        raise RebalanceScheduleError(str(exc)) from exc
    calendar_set = set(calendar)
    missing = [day for day in requested if day not in calendar_set]
    if missing:
        sample = [day.strftime("%Y-%m-%d") for day in missing[:5]]
        raise RebalanceScheduleError(
            "explicit rebalance dates must exist in the trading calendar; "
            f"missing: {sample}"
        )
    # Sort ascending after normalize/dedupe so input order never affects output.
    return sorted(set(requested))
