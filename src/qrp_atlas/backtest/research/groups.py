"""Deterministic factor grouping and group-return analytics."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    ensure_cross_section_frame,
    normalize_feature_columns,
    normalize_trade_date,
    sort_cross_section_frame,
)

from .forward_returns import forward_return_column

GROUP_ASSIGN_COLUMNS = (TRADE_DATE, ASSET_ID, "factor", "factor_value", "group")
GROUP_RETURN_COLUMNS = (
    TRADE_DATE,
    "factor",
    "horizon",
    "group",
    "group_return",
    "member_count",
    "valid_return_count",
    "coverage",
)
GROUP_SPREAD_COLUMNS = (
    TRADE_DATE,
    "factor",
    "horizon",
    "high_group",
    "low_group",
    "spread_return",
)


class GroupAnalysisError(ValueError):
    """Raised when factor grouping cannot proceed."""


@dataclass(frozen=True)
class GroupReturnResult:
    """Group assignments, per-group returns and high-minus-low spreads."""

    assignments: pd.DataFrame
    group_returns: pd.DataFrame
    spreads: pd.DataFrame


def assign_factor_groups(
    factor_frame: pd.DataFrame,
    *,
    factor_columns: str | Sequence[str],
    n_groups: int = 5,
) -> pd.DataFrame:
    """Assign deterministic quantity groups for each date and factor.

    Group 1 is lowest score, group N is highest. Ties break by ``asset_id``
    lexicographic order. Group sizes differ by at most one.
    """
    if not isinstance(n_groups, int) or isinstance(n_groups, bool) or n_groups <= 0:
        raise GroupAnalysisError("n_groups must be a positive integer")
    factors = normalize_feature_columns(factor_columns)
    if not factors:
        raise GroupAnalysisError("factor_columns must be non-empty")
    try:
        frame = ensure_cross_section_frame(
            factor_frame,
            feature_columns=factors,
            copy=True,
            enforce_primary_key=True,
        )
    except CrossSectionFrameError as exc:
        raise GroupAnalysisError(str(exc)) from exc
    if frame.empty:
        return _empty_assignments()

    rows: list[dict[str, Any]] = []
    for trade_date, day in frame.groupby(TRADE_DATE, sort=True):
        for factor in factors:
            values = pd.to_numeric(day[factor], errors="coerce")
            valid_mask = values.map(_is_finite)
            valid = day.loc[valid_mask].copy()
            valid["_factor_value"] = values.loc[valid_mask].astype(float)
            if valid.empty:
                continue
            ordered = valid.sort_values(
                ["_factor_value", ASSET_ID],
                ascending=[True, True],
                kind="mergesort",
            )
            groups = _balanced_groups(len(ordered), n_groups)
            for asset_id, factor_value, group in zip(
                ordered[ASSET_ID].tolist(),
                ordered["_factor_value"].tolist(),
                groups,
                strict=True,
            ):
                rows.append(
                    {
                        TRADE_DATE: normalize_trade_date(trade_date),
                        ASSET_ID: str(asset_id),
                        "factor": factor,
                        "factor_value": float(factor_value),
                        "group": int(group),
                    }
                )
    if not rows:
        return _empty_assignments()
    out = pd.DataFrame(rows, columns=list(GROUP_ASSIGN_COLUMNS))
    return out.sort_values(
        [TRADE_DATE, "factor", "group", ASSET_ID],
        kind="mergesort",
    ).reset_index(drop=True)


def compute_group_returns(
    assignments: pd.DataFrame,
    forward_returns: pd.DataFrame,
    *,
    horizons: Sequence[int] = (1, 5, 20),
) -> GroupReturnResult:
    """Compute equal-weight group returns and high-minus-low spreads."""
    if assignments is None or not isinstance(assignments, pd.DataFrame):
        raise GroupAnalysisError("assignments must be a pandas DataFrame")
    if forward_returns is None or not isinstance(forward_returns, pd.DataFrame):
        raise GroupAnalysisError("forward_returns must be a pandas DataFrame")
    horizon_list = _normalize_horizons(horizons)
    if assignments.empty:
        return GroupReturnResult(
            assignments=_empty_assignments(),
            group_returns=_empty_group_returns(),
            spreads=_empty_spreads(),
        )

    required = list(GROUP_ASSIGN_COLUMNS)
    missing = [column for column in required if column not in assignments.columns]
    if missing:
        raise GroupAnalysisError(f"assignments missing columns: {missing}")

    return_cols = [forward_return_column(h) for h in horizon_list]
    try:
        returns_df = ensure_cross_section_frame(
            forward_returns,
            feature_columns=return_cols,
            copy=True,
            enforce_primary_key=True,
        )
    except CrossSectionFrameError as exc:
        raise GroupAnalysisError(str(exc)) from exc

    base = assignments.copy()
    base[TRADE_DATE] = base[TRADE_DATE].map(normalize_trade_date)
    base[ASSET_ID] = base[ASSET_ID].astype(str)
    merged = base.merge(
        returns_df[[TRADE_DATE, ASSET_ID, *return_cols]],
        on=[TRADE_DATE, ASSET_ID],
        how="left",
        sort=False,
    )

    group_rows: list[dict[str, Any]] = []
    spread_rows: list[dict[str, Any]] = []
    for (trade_date, factor), day in merged.groupby([TRADE_DATE, "factor"], sort=True):
        groups_present = sorted({int(value) for value in day["group"].tolist()})
        for horizon in horizon_list:
            ret_col = forward_return_column(horizon)
            day_stats: dict[int, dict[str, Any]] = {}
            for group in groups_present:
                members = day[day["group"] == group]
                member_count = int(len(members))
                returns = pd.to_numeric(members[ret_col], errors="coerce")
                valid = returns[returns.map(_is_finite)].astype(float)
                valid_count = int(len(valid))
                group_return = float(valid.mean()) if valid_count else math.nan
                coverage = (
                    float(valid_count / member_count) if member_count else math.nan
                )
                day_stats[group] = {
                    "group_return": group_return,
                    "member_count": member_count,
                    "valid_return_count": valid_count,
                    "coverage": coverage,
                }
                group_rows.append(
                    {
                        TRADE_DATE: normalize_trade_date(trade_date),
                        "factor": factor,
                        "horizon": int(horizon),
                        "group": int(group),
                        "group_return": group_return,
                        "member_count": member_count,
                        "valid_return_count": valid_count,
                        "coverage": coverage,
                    }
                )
            if groups_present:
                low_group = min(groups_present)
                high_group = max(groups_present)
                # Fewer than two distinct groups cannot form a comparable spread.
                if high_group == low_group:
                    spread = math.nan
                else:
                    high_ret = day_stats[high_group]["group_return"]
                    low_ret = day_stats[low_group]["group_return"]
                    if _is_finite(high_ret) and _is_finite(low_ret):
                        spread = float(high_ret) - float(low_ret)
                    else:
                        spread = math.nan
                spread_rows.append(
                    {
                        TRADE_DATE: normalize_trade_date(trade_date),
                        "factor": factor,
                        "horizon": int(horizon),
                        "high_group": int(high_group),
                        "low_group": int(low_group),
                        "spread_return": spread,
                    }
                )

    group_returns = (
        pd.DataFrame(group_rows, columns=list(GROUP_RETURN_COLUMNS))
        if group_rows
        else _empty_group_returns()
    )
    spreads = (
        pd.DataFrame(spread_rows, columns=list(GROUP_SPREAD_COLUMNS))
        if spread_rows
        else _empty_spreads()
    )
    if not group_returns.empty:
        group_returns = group_returns.sort_values(
            [TRADE_DATE, "factor", "horizon", "group"],
            kind="mergesort",
        ).reset_index(drop=True)
    if not spreads.empty:
        spreads = spreads.sort_values(
            [TRADE_DATE, "factor", "horizon"],
            kind="mergesort",
        ).reset_index(drop=True)
    return GroupReturnResult(
        assignments=assignments.copy(),
        group_returns=group_returns,
        spreads=spreads,
    )


def _balanced_groups(count: int, n_groups: int) -> list[int]:
    if count <= 0:
        return []
    actual_groups = min(n_groups, count)
    base = count // actual_groups
    remainder = count % actual_groups
    # Distribute remainder to higher groups so high/low remain extremes and
    # sizes differ by at most one.
    sizes = [base + (1 if idx >= actual_groups - remainder else 0) for idx in range(actual_groups)]
    groups: list[int] = []
    for group_id, size in enumerate(sizes, start=1):
        groups.extend([group_id] * size)
    return groups


def _normalize_horizons(horizons: Sequence[int]) -> list[int]:
    if not horizons:
        raise GroupAnalysisError("horizons must be non-empty")
    ordered: list[int] = []
    seen: set[int] = set()
    for value in horizons:
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise GroupAnalysisError("horizon must be a positive integer")
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _is_finite(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def _empty_assignments() -> pd.DataFrame:
    frame = pd.DataFrame(columns=list(GROUP_ASSIGN_COLUMNS))
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    frame[ASSET_ID] = pd.Series(dtype=object)
    frame["factor"] = pd.Series(dtype=object)
    frame["factor_value"] = pd.Series(dtype=float)
    frame["group"] = pd.Series(dtype=int)
    return frame


def _empty_group_returns() -> pd.DataFrame:
    frame = pd.DataFrame(columns=list(GROUP_RETURN_COLUMNS))
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    frame["factor"] = pd.Series(dtype=object)
    frame["horizon"] = pd.Series(dtype=int)
    frame["group"] = pd.Series(dtype=int)
    for column in ("group_return", "coverage"):
        frame[column] = pd.Series(dtype=float)
    frame["member_count"] = pd.Series(dtype=int)
    frame["valid_return_count"] = pd.Series(dtype=int)
    return frame


def _empty_spreads() -> pd.DataFrame:
    frame = pd.DataFrame(columns=list(GROUP_SPREAD_COLUMNS))
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    frame["factor"] = pd.Series(dtype=object)
    frame["horizon"] = pd.Series(dtype=int)
    frame["high_group"] = pd.Series(dtype=int)
    frame["low_group"] = pd.Series(dtype=int)
    frame["spread_return"] = pd.Series(dtype=float)
    return frame
