"""Deterministic per-date Top-N selection."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE

from .eligibility import (
    ELIGIBILITY_REASON_COLUMN,
    ELIGIBLE_COLUMN,
    EligibilityError,
    apply_eligibility,
)

SCORE_COLUMN = "score"
RANK_COLUMN = "rank"
SELECTED_COLUMN = "selected"

SELECTION_COLUMNS = (
    TRADE_DATE,
    ASSET_ID,
    SCORE_COLUMN,
    RANK_COLUMN,
    SELECTED_COLUMN,
    ELIGIBLE_COLUMN,
    ELIGIBILITY_REASON_COLUMN,
)

# Columns owned by the selection contract. Callers may use score_column="score"
# as the canonical score field, but may not point score_column at other reserved
# helper columns that would be overwritten or ambiguous.
RESERVED_SCORE_COLUMNS = frozenset(
    {
        TRADE_DATE,
        ASSET_ID,
        RANK_COLUMN,
        SELECTED_COLUMN,
        ELIGIBLE_COLUMN,
        ELIGIBILITY_REASON_COLUMN,
        "selection_eligible",
    }
)


class SelectionError(ValueError):
    """Raised when Top-N selection cannot proceed."""


def select_top_n(
    score_frame: pd.DataFrame,
    *,
    n: int,
    score_column: str = SCORE_COLUMN,
    ascending: bool = False,
    eligibility: pd.DataFrame | None = None,
    date_column: str = TRADE_DATE,
    keep_unselected: bool = True,
) -> pd.DataFrame:
    """Select Top-N assets independently for each trade/signal date.

    Rules
    -----
    - larger scores win by default (``ascending=False``);
    - non-finite scores are excluded;
    - eligibility is applied first;
    - fewer than N eligible assets selects all of them;
    - ties break by ``asset_id`` lexicographic order;
    - output does not depend on input row order;
    - empty cross-sections return a stable empty schema.
    """
    if not isinstance(n, int) or isinstance(n, bool) or n <= 0:
        raise SelectionError("n must be a positive integer")
    if score_frame is None or not isinstance(score_frame, pd.DataFrame):
        raise SelectionError("score_frame must be a pandas DataFrame")
    if not isinstance(score_column, str) or not score_column.strip():
        raise SelectionError("score_column must be a non-empty string")
    if score_column in RESERVED_SCORE_COLUMNS:
        raise SelectionError(
            f"score_column {score_column!r} conflicts with reserved selection fields"
        )
    if score_column not in getattr(score_frame, "columns", []):
        raise SelectionError(f"score_frame missing score column: {score_column!r}")

    try:
        annotated = apply_eligibility(
            score_frame,
            score_column=score_column,
            eligibility=eligibility,
            date_column=date_column,
        )
    except EligibilityError as exc:
        raise SelectionError(str(exc)) from exc

    if annotated.empty:
        return _empty_selection(score_column)

    rows: list[dict[str, Any]] = []
    for _signal_date, group in annotated.groupby(TRADE_DATE, sort=True):
        day = group.copy()
        eligible_mask = day["selection_eligible"].astype(bool)
        eligible = day.loc[eligible_mask].copy()
        if eligible.empty:
            ordered = day.sort_values([ASSET_ID], kind="mergesort")
            for _, row in ordered.iterrows():
                rows.append(
                    _selection_row(
                        row,
                        score_column=score_column,
                        rank=None,
                        selected=False,
                    )
                )
            continue

        eligible = eligible.sort_values(
            [score_column, ASSET_ID],
            ascending=[ascending, True],
            kind="mergesort",
        )
        eligible[RANK_COLUMN] = range(1, len(eligible) + 1)
        selected_ids = set(eligible.loc[eligible[RANK_COLUMN] <= n, ASSET_ID].tolist())
        rank_map = {
            asset_id: int(rank)
            for asset_id, rank in zip(
                eligible[ASSET_ID].tolist(),
                eligible[RANK_COLUMN].tolist(),
                strict=True,
            )
        }

        ordered_all = day.sort_values([ASSET_ID], kind="mergesort")
        for _, row in ordered_all.iterrows():
            asset_id = row[ASSET_ID]
            rank = rank_map.get(asset_id)
            selected = asset_id in selected_ids
            if not keep_unselected and not selected:
                continue
            rows.append(
                _selection_row(
                    row,
                    score_column=score_column,
                    rank=rank,
                    selected=selected,
                )
            )

    if not rows:
        return _empty_selection(score_column)

    out = pd.DataFrame(rows)
    out["_selected_order"] = (~out[SELECTED_COLUMN]).astype(int)
    out["_rank_order"] = out[RANK_COLUMN].fillna(10**12)
    out = out.sort_values(
        [TRADE_DATE, "_selected_order", "_rank_order", ASSET_ID],
        kind="mergesort",
    ).drop(columns=["_selected_order", "_rank_order"])
    return out.reset_index(drop=True)


def _selection_row(
    row: pd.Series,
    *,
    score_column: str,
    rank: int | None,
    selected: bool,
) -> dict[str, Any]:
    score_value = row[score_column]
    if pd.isna(score_value) or not math.isfinite(float(score_value)):
        score_out: float | None = None
    else:
        score_out = float(score_value)
    payload = {
        TRADE_DATE: row[TRADE_DATE],
        ASSET_ID: row[ASSET_ID],
        SCORE_COLUMN: score_out,
        RANK_COLUMN: rank,
        SELECTED_COLUMN: bool(selected),
        ELIGIBLE_COLUMN: bool(row.get(ELIGIBLE_COLUMN, False)),
        ELIGIBILITY_REASON_COLUMN: row.get(ELIGIBILITY_REASON_COLUMN),
    }
    if score_column != SCORE_COLUMN:
        payload[score_column] = score_out
    return payload


def _empty_selection(score_column: str) -> pd.DataFrame:
    columns = list(SELECTION_COLUMNS)
    if score_column not in columns:
        columns.append(score_column)
    frame = pd.DataFrame(columns=columns)
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    frame[ASSET_ID] = pd.Series(dtype=object)
    frame[SCORE_COLUMN] = pd.Series(dtype=float)
    frame[RANK_COLUMN] = pd.Series(dtype="float")
    frame[SELECTED_COLUMN] = pd.Series(dtype=bool)
    frame[ELIGIBLE_COLUMN] = pd.Series(dtype=bool)
    frame[ELIGIBILITY_REASON_COLUMN] = pd.Series(dtype=object)
    if score_column != SCORE_COLUMN:
        frame[score_column] = pd.Series(dtype=float)
    return frame
