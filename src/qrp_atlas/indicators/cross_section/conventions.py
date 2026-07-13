"""Stable conventions for cross-sectional research frames.

All cross-sectional calculations operate on a tabular snapshot with at least:

- trade_date
- asset_id
- one or more feature columns

Rules shared by operators and the pipeline entry point:

- computations are independent per trade_date;
- samples from different dates must never mix;
- callers' DataFrames are never mutated;
- empty inputs return empty frames with stable columns;
- outputs have deterministic sort order (trade_date, asset_id).
"""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE

REQUIRED_CROSS_SECTION_COLUMNS: tuple[str, str] = (TRADE_DATE, ASSET_ID)


class CrossSectionFrameError(ValueError):
    """Raised when a cross-section frame fails validation."""


def normalize_feature_columns(
    feature_columns: str | Sequence[str] | None,
    *,
    available: Sequence[str] | None = None,
) -> list[str]:
    """Normalize a feature-column argument to a de-duplicated list.

    Args:
        feature_columns: single name, sequence of names, or None.
        available: optional whitelist used to validate membership.

    Returns:
        A list of feature column names preserving first-seen order.
    """
    if feature_columns is None:
        cols: list[str] = []
    elif isinstance(feature_columns, str):
        cols = [feature_columns]
    else:
        cols = [str(c) for c in feature_columns]

    seen: set[str] = set()
    ordered: list[str] = []
    for col in cols:
        if not col:
            raise CrossSectionFrameError("feature column names must be non-empty")
        if col in seen:
            continue
        seen.add(col)
        ordered.append(col)

    if available is not None:
        available_set = set(available)
        missing = [c for c in ordered if c not in available_set]
        if missing:
            raise CrossSectionFrameError(f"missing feature columns: {missing}")
    return ordered


def sort_cross_section_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return a stably sorted copy ordered by trade_date then asset_id."""
    if df is None or df.empty:
        if df is None:
            return pd.DataFrame(columns=list(REQUIRED_CROSS_SECTION_COLUMNS))
        return df.copy().reset_index(drop=True)

    sort_cols = [c for c in REQUIRED_CROSS_SECTION_COLUMNS if c in df.columns]
    if not sort_cols:
        return df.copy().reset_index(drop=True)
    return df.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)


def ensure_cross_section_frame(
    df: pd.DataFrame | None,
    *,
    feature_columns: str | Sequence[str] | None = None,
    require_features: bool = False,
    copy: bool = True,
) -> pd.DataFrame:
    """Validate and normalize a cross-section input frame.

    Args:
        df: candidate cross-section DataFrame.
        feature_columns: optional feature columns that must exist when provided.
        require_features: when True, at least one feature column is required.
        copy: when True (default), return a defensive copy.

    Returns:
        A validated DataFrame. Empty/None inputs become empty frames that still
        carry trade_date/asset_id (and requested feature) columns when known.
    """
    features = normalize_feature_columns(feature_columns)
    if require_features and not features:
        raise CrossSectionFrameError("at least one feature column is required")

    if df is None:
        columns = list(REQUIRED_CROSS_SECTION_COLUMNS) + features
        return pd.DataFrame(columns=columns)

    if not isinstance(df, pd.DataFrame):
        raise CrossSectionFrameError("cross-section input must be a pandas DataFrame")

    missing_keys = [c for c in REQUIRED_CROSS_SECTION_COLUMNS if c not in df.columns]
    if missing_keys:
        raise CrossSectionFrameError(
            f"cross-section frame missing required columns: {missing_keys}"
        )

    features = normalize_feature_columns(features, available=df.columns)
    if require_features and not features:
        raise CrossSectionFrameError("at least one feature column is required")

    out = df.copy() if copy else df
    out[ASSET_ID] = out[ASSET_ID].map(lambda x: x if pd.isna(x) else str(x))
    return out


def empty_cross_section_frame(
    feature_columns: Sequence[str] | None = None,
    *,
    extra_columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Build an empty cross-section frame with a stable column layout."""
    cols: list[str] = list(REQUIRED_CROSS_SECTION_COLUMNS)
    for col in list(feature_columns or ()) + list(extra_columns or ()):
        if col not in cols:
            cols.append(str(col))
    return pd.DataFrame(columns=cols)
