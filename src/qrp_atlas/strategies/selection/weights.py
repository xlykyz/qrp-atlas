"""Equal-weight target construction for Top-N selections."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import normalize_trade_date

from .selection import RANK_COLUMN, SCORE_COLUMN, SELECTED_COLUMN

TARGET_WEIGHT_COLUMNS = ("trade_date", "asset_id", "target_weight", "priority")


class WeightConstructionError(ValueError):
    """Raised when target weights cannot be constructed."""


def equal_weight_targets(
    selected_assets: Sequence[str],
    *,
    trade_date: Any,
    scores: Mapping[str, float] | None = None,
    ranks: Mapping[str, int] | None = None,
    top_n: int | None = None,
    max_positions: int | None = None,
    max_weight_per_asset: float = 1.0,
    cash_buffer: float = 0.0,
) -> pd.DataFrame:
    """Build one equal-weight target snapshot for a single execution date.

    Capacity order and ``priority`` use rank as the sole authority when ranks
    are provided (``priority = -rank``). Raw scores are never re-interpreted
    for ordering when ranks exist.
    """
    if not 0.0 <= float(cash_buffer) < 1.0:
        raise WeightConstructionError("cash_buffer must be in [0, 1)")
    if not 0.0 < float(max_weight_per_asset) <= 1.0:
        raise WeightConstructionError("max_weight_per_asset must be in (0, 1]")
    if max_positions is not None and (
        not isinstance(max_positions, int)
        or isinstance(max_positions, bool)
        or max_positions <= 0
    ):
        raise WeightConstructionError("max_positions must be a positive integer")
    if top_n is not None and (
        not isinstance(top_n, int) or isinstance(top_n, bool) or top_n <= 0
    ):
        raise WeightConstructionError("top_n must be a positive integer")

    execution_date = normalize_trade_date(trade_date).strftime("%Y-%m-%d")
    ordered = sorted(
        {str(asset_id) for asset_id in selected_assets if str(asset_id).strip()}
    )
    # Rank is authoritative whenever present; scores are audit-only fallback.
    if ranks is not None:
        ordered = sorted(
            ordered,
            key=lambda asset_id: (
                int(ranks.get(asset_id, 10**12)),
                asset_id,
            ),
        )
    elif scores is not None:
        ordered = sorted(
            ordered,
            key=lambda asset_id: (
                -float(scores.get(asset_id, float("-inf"))),
                asset_id,
            ),
        )

    limit = max_positions if max_positions is not None else top_n
    if limit is not None:
        ordered = ordered[:limit]

    if not ordered:
        return pd.DataFrame(columns=list(TARGET_WEIGHT_COLUMNS))

    target_gross = 1.0 - float(cash_buffer)
    raw_weight = target_gross / len(ordered)
    weight = min(raw_weight, float(max_weight_per_asset))
    if weight < 0:
        raise WeightConstructionError("computed target weight must be non-negative")

    rows = []
    for asset_id in ordered:
        if ranks is not None and asset_id in ranks:
            priority = float(-int(ranks[asset_id]))
        elif scores is not None and asset_id in scores:
            priority = float(scores[asset_id])
        else:
            priority = 0.0
        rows.append(
            {
                "trade_date": execution_date,
                "asset_id": asset_id,
                "target_weight": float(weight),
                "priority": priority,
            }
        )
    out = pd.DataFrame(rows, columns=list(TARGET_WEIGHT_COLUMNS))
    total = float(out["target_weight"].sum())
    if total > target_gross + 1e-12:
        raise WeightConstructionError(
            "target weights must sum to <= 1 - cash_buffer"
        )
    if total > 1.0 + 1e-12:
        raise WeightConstructionError("target weights must sum to <= 1")
    if max_positions is not None and (out["target_weight"] > 0).sum() > max_positions:
        raise WeightConstructionError("positive target count exceeds max_positions")
    return out


def selection_to_target_weights(
    selection: pd.DataFrame,
    *,
    signal_to_trade: Mapping[Any, Any] | pd.DataFrame | None = None,
    trade_date_column: str | None = None,
    max_positions: int | None = None,
    max_weight_per_asset: float = 1.0,
    cash_buffer: float = 0.0,
    previous_assets_by_trade_date: Mapping[Any, Sequence[str]] | None = None,
    include_zero_targets: bool = True,
) -> pd.DataFrame:
    """Convert multi-date Top-N selection rows into full target snapshots.

    Parameters
    ----------
    selection:
        Output of ``select_top_n`` (or compatible frame with ``selected``).
    signal_to_trade:
        Mapping/DataFrame from signal_date -> trade_date. When omitted, the
        selection ``trade_date`` column is treated as the execution date.
    trade_date_column:
        Optional explicit execution-date column already present on selection.
    previous_assets_by_trade_date:
        Optional prior holdings keyed by execution ``trade_date``. Used to seed
        holdings before that date so exits can be expressed as zero targets,
        including empty current selections that must fully liquidate.
    include_zero_targets:
        When True (default), assets held previously but not selected now get
        ``target_weight=0`` on the execution date.
    """
    if selection is None or not isinstance(selection, pd.DataFrame):
        raise WeightConstructionError("selection must be a pandas DataFrame")

    prior_map = _normalize_previous_assets(previous_assets_by_trade_date)

    if selection.empty:
        if not prior_map or not include_zero_targets:
            return pd.DataFrame(columns=list(TARGET_WEIGHT_COLUMNS))
        rows: list[dict[str, Any]] = []
        for execution_date, assets in sorted(prior_map.items()):
            for asset_id in sorted(assets):
                rows.append(
                    {
                        "trade_date": execution_date.strftime("%Y-%m-%d"),
                        "asset_id": asset_id,
                        "target_weight": 0.0,
                        "priority": 0.0,
                    }
                )
        if not rows:
            return pd.DataFrame(columns=list(TARGET_WEIGHT_COLUMNS))
        return (
            pd.DataFrame(rows, columns=list(TARGET_WEIGHT_COLUMNS))
            .sort_values(["trade_date", "asset_id"], kind="mergesort")
            .reset_index(drop=True)
        )

    if SELECTED_COLUMN not in selection.columns:
        raise WeightConstructionError("selection must include a 'selected' column")
    if ASSET_ID not in selection.columns:
        raise WeightConstructionError("selection must include asset_id")

    working = selection.copy()
    if trade_date_column is not None:
        if trade_date_column not in working.columns:
            raise WeightConstructionError(
                f"selection missing trade_date column: {trade_date_column!r}"
            )
        execution_col = trade_date_column
    elif signal_to_trade is not None:
        mapping = _as_signal_trade_map(signal_to_trade)
        if TRADE_DATE not in working.columns:
            raise WeightConstructionError(
                "selection must include trade_date/signal_date"
            )
        working["_execution_date"] = working[TRADE_DATE].map(
            lambda value: mapping.get(normalize_trade_date(value))
        )
        if working["_execution_date"].isna().any():
            missing = (
                working.loc[working["_execution_date"].isna(), TRADE_DATE]
                .map(lambda value: normalize_trade_date(value).strftime("%Y-%m-%d"))
                .drop_duplicates()
                .tolist()
            )
            raise WeightConstructionError(
                f"missing execution trade_date for signal dates: {missing[:5]}"
            )
        execution_col = "_execution_date"
    else:
        if TRADE_DATE not in working.columns:
            raise WeightConstructionError("selection must include trade_date")
        execution_col = TRADE_DATE

    score_col = SCORE_COLUMN if SCORE_COLUMN in working.columns else None
    rank_col = RANK_COLUMN if RANK_COLUMN in working.columns else None

    ordered_dates = sorted(
        {normalize_trade_date(value) for value in working[execution_col].tolist()}
        | set(prior_map)
    )

    rows = []
    previous_selected: set[str] = set()
    for execution_date in ordered_dates:
        if execution_date in prior_map:
            # Explicit prior holdings describe state before this execution date.
            previous_selected = set(prior_map[execution_date]) | previous_selected

        day = working[
            working[execution_col].map(normalize_trade_date) == execution_date
        ]
        selected = day[day[SELECTED_COLUMN].astype(bool)] if not day.empty else day
        selected_ids = (
            selected[ASSET_ID].astype(str).tolist() if not selected.empty else []
        )
        scores = None
        ranks = None
        if score_col is not None and not selected.empty:
            scores = {
                str(asset_id): float(score)
                for asset_id, score in zip(
                    selected[ASSET_ID].tolist(),
                    selected[score_col].tolist(),
                    strict=True,
                )
                if score is not None and pd.notna(score) and math.isfinite(float(score))
            }
        if rank_col is not None and not selected.empty:
            ranks = {
                str(asset_id): int(rank)
                for asset_id, rank in zip(
                    selected[ASSET_ID].tolist(),
                    selected[rank_col].tolist(),
                    strict=True,
                )
                if rank is not None and pd.notna(rank)
            }
        snapshot = equal_weight_targets(
            selected_ids,
            trade_date=execution_date,
            scores=scores,
            ranks=ranks,
            max_positions=max_positions,
            max_weight_per_asset=max_weight_per_asset,
            cash_buffer=cash_buffer,
        )
        current = set(snapshot["asset_id"].tolist()) if not snapshot.empty else set()
        if include_zero_targets:
            for asset_id in sorted(previous_selected - current):
                rows.append(
                    {
                        "trade_date": execution_date.strftime("%Y-%m-%d"),
                        "asset_id": asset_id,
                        "target_weight": 0.0,
                        "priority": 0.0,
                    }
                )
        rows.extend(snapshot.to_dict("records"))
        previous_selected = current

    if not rows:
        return pd.DataFrame(columns=list(TARGET_WEIGHT_COLUMNS))
    out = pd.DataFrame(rows, columns=list(TARGET_WEIGHT_COLUMNS))
    return out.sort_values(["trade_date", "asset_id"], kind="mergesort").reset_index(
        drop=True
    )


def _normalize_previous_assets(
    previous_assets_by_trade_date: Mapping[Any, Sequence[str]] | None,
) -> dict[pd.Timestamp, set[str]]:
    if not previous_assets_by_trade_date:
        return {}
    if not isinstance(previous_assets_by_trade_date, Mapping):
        raise WeightConstructionError(
            "previous_assets_by_trade_date must be a mapping"
        )
    out: dict[pd.Timestamp, set[str]] = {}
    for key, assets in previous_assets_by_trade_date.items():
        day = normalize_trade_date(key)
        cleaned = {
            str(asset_id).strip()
            for asset_id in (assets or [])
            if str(asset_id).strip()
        }
        out[day] = cleaned
    return out


def _as_signal_trade_map(
    signal_to_trade: Mapping[Any, Any] | pd.DataFrame,
) -> dict[pd.Timestamp, pd.Timestamp]:
    if isinstance(signal_to_trade, pd.DataFrame):
        required = {"signal_date", "trade_date"}
        missing = required - set(signal_to_trade.columns)
        if missing:
            raise WeightConstructionError(
                f"signal_to_trade DataFrame missing columns: {sorted(missing)}"
            )
        mapping: dict[pd.Timestamp, pd.Timestamp] = {}
        for row in signal_to_trade.itertuples(index=False):
            mapping[normalize_trade_date(row.signal_date)] = normalize_trade_date(
                row.trade_date
            )
        return mapping
    if not isinstance(signal_to_trade, Mapping):
        raise WeightConstructionError("signal_to_trade must be a mapping or DataFrame")
    return {
        normalize_trade_date(key): normalize_trade_date(value)
        for key, value in signal_to_trade.items()
    }
