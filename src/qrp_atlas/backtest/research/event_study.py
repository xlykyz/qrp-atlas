"""Earnings-forecast event study: labels, grouping, and research entrypoint.

Boundary:
- consumes 05-A EventFrame / as_of outputs already prepared by the caller
- does not query Tushare or recompute available_trade_date
- future returns are evaluation-only and never feed selection
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from qrp_atlas.indicators.events.earnings_forecast import (
    DIRECTION_SCORE,
    PROFIT_CHANGE_MIDPOINT,
    attach_earnings_forecast_indicators,
)

DEFAULT_EVENT_HORIZONS: tuple[int, ...] = (1, 5, 10, 20)


class EventStudyError(ValueError):
    """Raised when an event study cannot be computed."""


def event_forward_return_column(horizon: int) -> str:
    if not isinstance(horizon, int) or isinstance(horizon, bool) or horizon <= 0:
        raise EventStudyError("horizon must be a positive integer")
    return f"forward_return_{horizon}d"


def _normalize_date(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts.normalize()


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def _normalize_horizons(horizons: Sequence[int]) -> list[int]:
    out: list[int] = []
    for h in horizons:
        if not isinstance(h, int) or isinstance(h, bool) or h <= 0:
            raise EventStudyError(f"invalid horizon: {h!r}")
        out.append(h)
    if not out:
        raise EventStudyError("horizons must be non-empty")
    return out


def _build_price_maps(
    price_df: pd.DataFrame,
    *,
    open_field: str,
    close_field: str,
    asset_col: str,
    date_col: str,
) -> tuple[dict[tuple[pd.Timestamp, str], float], dict[tuple[pd.Timestamp, str], float]]:
    if price_df is None or not isinstance(price_df, pd.DataFrame):
        raise EventStudyError("price_df must be a pandas DataFrame")
    for col in (date_col, asset_col, open_field, close_field):
        if col not in price_df.columns:
            raise EventStudyError(f"price_df missing required column: {col}")

    open_map: dict[tuple[pd.Timestamp, str], float] = {}
    close_map: dict[tuple[pd.Timestamp, str], float] = {}
    for _, row in price_df.iterrows():
        day = _normalize_date(row[date_col])
        asset = str(row[asset_col]).strip()
        if day is None or not asset:
            continue
        o = _finite(row[open_field])
        c = _finite(row[close_field])
        if o is not None:
            open_map[(day, asset)] = o
        if c is not None:
            close_map[(day, asset)] = c
    return open_map, close_map


def _normalize_calendar(trading_days: Sequence[Any]) -> list[pd.Timestamp]:
    days = []
    for value in trading_days:
        day = _normalize_date(value)
        if day is not None:
            days.append(day)
    calendar = sorted(set(days))
    if not calendar:
        raise EventStudyError("trading_days must contain valid dates")
    return calendar


def compute_event_forward_returns(
    events: pd.DataFrame,
    price_df: pd.DataFrame,
    *,
    trading_days: Sequence[Any],
    horizons: Sequence[int] = DEFAULT_EVENT_HORIZONS,
    open_field: str = "open",
    close_field: str = "close",
    asset_col: str = "ticker",
    price_asset_col: str | None = None,
    price_date_col: str = "trade_date",
) -> tuple[pd.DataFrame, list[str]]:
    """Compute event forward returns with 05-B entry/exit semantics.

    ```text
    entry_date  = available_trade_date
    entry_price = open[entry_date]
    horizon=N:
      available_trade_date is day 1
      exit_price = close[day N]
    forward_return_N = exit_price / entry_price - 1
    ```
    """
    if events is None or not isinstance(events, pd.DataFrame):
        raise EventStudyError("events must be a pandas DataFrame")
    horizon_list = _normalize_horizons(horizons)
    calendar = _normalize_calendar(trading_days)
    index = {day: i for i, day in enumerate(calendar)}
    price_asset_col = price_asset_col or (
        "asset_id" if "asset_id" in price_df.columns and asset_col not in price_df.columns else asset_col
    )
    if price_asset_col not in price_df.columns and "asset_id" in price_df.columns:
        price_asset_col = "asset_id"
    open_map, close_map = _build_price_maps(
        price_df,
        open_field=open_field,
        close_field=close_field,
        asset_col=price_asset_col,
        date_col=price_date_col,
    )

    work = events.copy()
    diagnostics: list[str] = []
    missing_entry = 0
    missing_exit = 0
    incomplete_window = 0

    for horizon in horizon_list:
        work[event_forward_return_column(horizon)] = math.nan

    if work.empty:
        return work.reset_index(drop=True), diagnostics

    required = ("ticker", "available_trade_date")
    for col in required:
        if col not in work.columns:
            raise EventStudyError(f"events missing required column: {col}")

    for i, row in work.iterrows():
        asset = str(row["ticker"]).strip()
        entry = _normalize_date(row["available_trade_date"])
        if not asset or entry is None:
            missing_entry += 1
            continue
        if entry not in index:
            incomplete_window += 1
            continue
        entry_price = open_map.get((entry, asset))
        if entry_price is None:
            missing_entry += 1
            continue
        base_idx = index[entry]
        for horizon in horizon_list:
            # day 1 is entry itself => exit index = base_idx + (horizon - 1)
            exit_idx = base_idx + (horizon - 1)
            col = event_forward_return_column(horizon)
            if exit_idx >= len(calendar):
                incomplete_window += 1
                work.at[i, col] = math.nan
                continue
            exit_day = calendar[exit_idx]
            exit_price = close_map.get((exit_day, asset))
            if exit_price is None:
                missing_exit += 1
                work.at[i, col] = math.nan
                continue
            work.at[i, col] = exit_price / entry_price - 1.0

    if missing_entry:
        diagnostics.append(f"missing_entry_open={missing_entry}")
    if missing_exit:
        diagnostics.append(f"missing_exit_close={missing_exit}")
    if incomplete_window:
        diagnostics.append(f"incomplete_future_window={incomplete_window}")
    return work.reset_index(drop=True), diagnostics


def _bucket_profit_change_midpoint(value: Any, n_buckets: int = 5) -> str | float:
    if n_buckets <= 0:
        raise EventStudyError("n_buckets must be positive")
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    if not math.isfinite(number):
        return math.nan
    # Fixed research bins in percent points for stable cross-sample comparison.
    edges = [-math.inf, -50.0, -10.0, 0.0, 10.0, 50.0, math.inf]
    labels = ["<=-50", "(-50,-10]", "(-10,0]", "(0,10]", "(10,50]", ">50"]
    for idx in range(1, len(edges)):
        left, right = edges[idx - 1], edges[idx]
        if number > left and number <= right:
            return labels[idx - 1]
        if idx == 1 and number == left:
            return labels[0]
    return math.nan


def summarize_event_groups(
    frame: pd.DataFrame,
    *,
    group_cols: Sequence[str],
    return_col: str,
) -> pd.DataFrame:
    """Aggregate event-study group statistics for one return column."""
    if return_col not in frame.columns:
        raise EventStudyError(f"missing return column: {return_col}")
    for col in group_cols:
        if col not in frame.columns:
            raise EventStudyError(f"missing group column: {col}")

    rows: list[dict[str, Any]] = []
    grouped = frame.groupby(list(group_cols), dropna=False, sort=True)
    for keys, part in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        values = pd.to_numeric(part[return_col], errors="coerce")
        valid = values[values.map(lambda x: math.isfinite(float(x)) if pd.notna(x) else False)]
        sample_count = int(len(part))
        valid_count = int(len(valid))
        row: dict[str, Any] = {
            **{col: key for col, key in zip(group_cols, keys, strict=True)},
            "return_col": return_col,
            "sample_count": sample_count,
            "valid_return_count": valid_count,
            "mean_return": float(valid.mean()) if valid_count else math.nan,
            "median_return": float(valid.median()) if valid_count else math.nan,
            "win_rate": float((valid > 0).mean()) if valid_count else math.nan,
            "std_return": float(valid.std(ddof=0)) if valid_count else math.nan,
        }
        rows.append(row)
    columns = [
        *group_cols,
        "return_col",
        "sample_count",
        "valid_return_count",
        "mean_return",
        "median_return",
        "win_rate",
        "std_return",
    ]
    return pd.DataFrame(rows, columns=columns)


@dataclass(frozen=True)
class EarningsForecastEventStudyResult:
    events: pd.DataFrame
    labeled: pd.DataFrame
    group_stats: pd.DataFrame
    diagnostics: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "events_rows": int(len(self.events)),
            "labeled_rows": int(len(self.labeled)),
            "group_stats_rows": int(len(self.group_stats)),
            "diagnostics": list(self.diagnostics),
            "metadata": dict(self.metadata),
        }


def run_earnings_forecast_event_study(
    events: pd.DataFrame,
    price_df: pd.DataFrame,
    *,
    trading_days: Sequence[Any],
    horizons: Sequence[int] = DEFAULT_EVENT_HORIZONS,
    forecast_types: Sequence[str] | None = None,
    direction_scores: Sequence[int] | None = None,
    tickers: Sequence[str] | None = None,
    start_date: Any | None = None,
    end_date: Any | None = None,
    open_field: str = "open",
    close_field: str = "close",
    midpoint_buckets: int = 5,
) -> EarningsForecastEventStudyResult:
    """Run the 05-B earnings-forecast event research loop."""
    if events is None or not isinstance(events, pd.DataFrame):
        raise EventStudyError("events must be a pandas DataFrame")
    # never mutate caller input
    base = events.copy()
    diagnostics: list[str] = []

    enriched, ind_diag = attach_earnings_forecast_indicators(base, copy=False)
    diagnostics.extend(ind_diag)

    work = enriched
    if forecast_types is not None:
        allowed = {str(x) for x in forecast_types}
        work = work[work["forecast_type"].astype(str).isin(allowed)].copy()
    if direction_scores is not None:
        allowed_dir = {int(x) for x in direction_scores}
        work = work[work[DIRECTION_SCORE].isin(allowed_dir)].copy()
    if tickers is not None:
        allowed_t = {str(x) for x in tickers}
        work = work[work["ticker"].astype(str).isin(allowed_t)].copy()
    if start_date is not None:
        start = _normalize_date(start_date)
        work = work[pd.to_datetime(work["available_trade_date"]).dt.normalize() >= start].copy()
    if end_date is not None:
        end = _normalize_date(end_date)
        work = work[pd.to_datetime(work["available_trade_date"]).dt.normalize() <= end].copy()

    labeled, ret_diag = compute_event_forward_returns(
        work,
        price_df,
        trading_days=trading_days,
        horizons=horizons,
        open_field=open_field,
        close_field=close_field,
    )
    diagnostics.extend(ret_diag)

    labeled["profit_change_midpoint_bucket"] = [
        _bucket_profit_change_midpoint(v, n_buckets=midpoint_buckets)
        for v in labeled.get(PROFIT_CHANGE_MIDPOINT, pd.Series(dtype=float)).tolist()
    ]

    horizon_list = _normalize_horizons(horizons)
    group_frames = []
    for group_col in ("forecast_type", DIRECTION_SCORE, "profit_change_midpoint_bucket"):
        for horizon in horizon_list:
            ret_col = event_forward_return_column(horizon)
            stats = summarize_event_groups(labeled, group_cols=(group_col,), return_col=ret_col)
            stats.insert(0, "group_dim", group_col)
            stats.insert(1, "horizon", horizon)
            group_frames.append(stats)
    group_stats = (
        pd.concat(group_frames, ignore_index=True)
        if group_frames
        else pd.DataFrame(
            columns=[
                "group_dim",
                "horizon",
                "forecast_type",
                "return_col",
                "sample_count",
                "valid_return_count",
                "mean_return",
                "median_return",
                "win_rate",
                "std_return",
            ]
        )
    )

    return EarningsForecastEventStudyResult(
        events=enriched.reset_index(drop=True),
        labeled=labeled.reset_index(drop=True),
        group_stats=group_stats,
        diagnostics=diagnostics,
        metadata={
            "horizons": list(horizon_list),
            "entry_price_field": open_field,
            "exit_price_field": close_field,
            "entry_rule": "available_trade_date open",
            "horizon_rule": "available_trade_date is day 1; exit on day N close",
        },
    )
