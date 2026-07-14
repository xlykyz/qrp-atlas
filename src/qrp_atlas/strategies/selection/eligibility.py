"""Eligibility filtering for cross-sectional selection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    ensure_cross_section_frame,
    normalize_trade_date,
    normalize_asset_id,
    sort_cross_section_frame,
)

ELIGIBLE_COLUMN = "eligible"
ELIGIBILITY_REASON_COLUMN = "reason_code"

ELIGIBILITY_COLUMNS = (TRADE_DATE, ASSET_ID, ELIGIBLE_COLUMN, ELIGIBILITY_REASON_COLUMN)

REASON_MISSING_ELIGIBILITY = "MISSING_ELIGIBILITY"
REASON_INELIGIBLE = "INELIGIBLE"
REASON_INVALID_SCORE = "INVALID_SCORE"
REASON_ELIGIBLE = "ELIGIBLE"


class EligibilityError(ValueError):
    """Raised when an eligibility panel fails validation."""


def empty_eligibility_frame() -> pd.DataFrame:
    """Return a stable empty eligibility panel."""
    frame = pd.DataFrame(columns=list(ELIGIBILITY_COLUMNS))
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    frame[ASSET_ID] = pd.Series(dtype=object)
    frame[ELIGIBLE_COLUMN] = pd.Series(dtype=bool)
    frame[ELIGIBILITY_REASON_COLUMN] = pd.Series(dtype=object)
    return frame


def ensure_eligibility_frame(
    eligibility: pd.DataFrame | None,
    *,
    copy: bool = True,
) -> pd.DataFrame:
    """Validate and normalize an eligibility panel.

    Contract columns:
    - trade_date
    - asset_id
    - eligible
    - reason_code (optional; filled when missing)
    """
    if eligibility is None:
        return empty_eligibility_frame()
    if not isinstance(eligibility, pd.DataFrame):
        raise EligibilityError("eligibility panel must be a pandas DataFrame")

    required = (TRADE_DATE, ASSET_ID, ELIGIBLE_COLUMN)
    missing = [column for column in required if column not in eligibility.columns]
    if missing:
        raise EligibilityError(f"eligibility panel missing required columns: {missing}")

    try:
        frame = ensure_cross_section_frame(
            eligibility,
            feature_columns=(ELIGIBLE_COLUMN,),
            copy=copy,
            enforce_primary_key=True,
        )
    except CrossSectionFrameError as exc:
        raise EligibilityError(str(exc)) from exc

    if frame.empty:
        return empty_eligibility_frame()

    eligible = frame[ELIGIBLE_COLUMN]
    if pd.api.types.is_bool_dtype(eligible):
        normalized_eligible = eligible.astype(bool)
    else:
        mapped = eligible.map(_coerce_eligible_flag)
        if mapped.isna().any():
            raise EligibilityError("eligible values must be boolean-compatible")
        normalized_eligible = mapped.astype(bool)

    if ELIGIBILITY_REASON_COLUMN in frame.columns:
        reasons = frame[ELIGIBILITY_REASON_COLUMN].where(
            ~frame[ELIGIBILITY_REASON_COLUMN].isna(),
            None,
        )
        reasons = reasons.map(
            lambda value: None
            if value is None or (isinstance(value, float) and pd.isna(value))
            else str(value)
        )
    else:
        reasons = pd.Series([None] * len(frame), index=frame.index, dtype=object)

    reasons = [
        reason
        if reason is not None
        else (REASON_ELIGIBLE if flag else REASON_INELIGIBLE)
        for flag, reason in zip(normalized_eligible.tolist(), reasons.tolist(), strict=True)
    ]

    out = pd.DataFrame(
        {
            TRADE_DATE: frame[TRADE_DATE],
            ASSET_ID: frame[ASSET_ID],
            ELIGIBLE_COLUMN: normalized_eligible.to_numpy(),
            ELIGIBILITY_REASON_COLUMN: reasons,
        }
    )
    return sort_cross_section_frame(out)


def apply_eligibility(
    score_frame: pd.DataFrame,
    *,
    score_column: str,
    eligibility: pd.DataFrame | None = None,
    date_column: str = TRADE_DATE,
) -> pd.DataFrame:
    """Annotate a score frame with eligibility without dropping research rows.

    Semantics
    ---------
    - Without an eligibility panel, universe assets default to eligible and
      only non-finite scores are marked invalid.
    - With an eligibility panel, missing (date, asset) pairs default to
      ineligible with reason ``MISSING_ELIGIBILITY``.
    - ``eligible=False`` never enters Top N.
    - Non-finite / missing scores never enter Top N.
    """
    if score_frame is None or not isinstance(score_frame, pd.DataFrame):
        raise EligibilityError("score_frame must be a pandas DataFrame")
    if score_column not in score_frame.columns:
        raise EligibilityError(f"score_frame missing score column: {score_column!r}")
    if date_column not in score_frame.columns or ASSET_ID not in score_frame.columns:
        raise EligibilityError(
            f"score_frame must include {date_column!r} and {ASSET_ID!r}"
        )

    try:
        base = ensure_cross_section_frame(
            score_frame.rename(columns={date_column: TRADE_DATE})
            if date_column != TRADE_DATE
            else score_frame,
            feature_columns=(score_column,),
            copy=True,
            enforce_primary_key=True,
        )
    except CrossSectionFrameError as exc:
        raise EligibilityError(str(exc)) from exc

    if base.empty:
        out = base.copy()
        out[ELIGIBLE_COLUMN] = pd.Series(dtype=bool)
        out[ELIGIBILITY_REASON_COLUMN] = pd.Series(dtype=object)
        out["selection_eligible"] = pd.Series(dtype=bool)
        return out

    scores = pd.to_numeric(base[score_column], errors="coerce")
    finite_score = scores.map(lambda value: bool(pd.notna(value) and float("-inf") < float(value) < float("inf")))

    panel = ensure_eligibility_frame(eligibility)
    if panel.empty and eligibility is None:
        eligible_flag = pd.Series(True, index=base.index)
        reason = pd.Series([REASON_ELIGIBLE] * len(base), index=base.index, dtype=object)
    else:
        lookup = {
            (row[TRADE_DATE], row[ASSET_ID]): (bool(row[ELIGIBLE_COLUMN]), row[ELIGIBILITY_REASON_COLUMN])
            for _, row in panel.iterrows()
        }
        eligible_values: list[bool] = []
        reason_values: list[str] = []
        for trade_date, asset_id in zip(base[TRADE_DATE].tolist(), base[ASSET_ID].tolist(), strict=True):
            key = (trade_date, asset_id)
            if key not in lookup:
                eligible_values.append(False)
                reason_values.append(REASON_MISSING_ELIGIBILITY)
            else:
                flag, code = lookup[key]
                eligible_values.append(flag)
                reason_values.append(
                    code
                    if code is not None
                    else (REASON_ELIGIBLE if flag else REASON_INELIGIBLE)
                )
        eligible_flag = pd.Series(eligible_values, index=base.index)
        reason = pd.Series(reason_values, index=base.index, dtype=object)

    # Score invalidity overrides eligibility for selection, but keeps original
    # eligibility audit fields separate.
    selection_eligible = eligible_flag & finite_score
    final_reason = [
        REASON_INVALID_SCORE if (elig and not finite) else code
        for elig, finite, code in zip(
            eligible_flag.tolist(),
            finite_score.tolist(),
            reason.tolist(),
            strict=True,
        )
    ]

    out = base.copy()
    out[score_column] = scores
    out[ELIGIBLE_COLUMN] = eligible_flag.to_numpy()
    out[ELIGIBILITY_REASON_COLUMN] = final_reason
    out["selection_eligible"] = selection_eligible.to_numpy()
    return sort_cross_section_frame(out)


def _coerce_eligible_flag(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
    return None
