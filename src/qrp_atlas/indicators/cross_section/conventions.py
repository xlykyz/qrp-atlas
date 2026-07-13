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
- outputs have deterministic sort order (trade_date, asset_id);
- trade_date is normalized to timezone-naive midnight timestamps;
- timezone-aware inputs keep their local wall calendar day (no UTC shift);
- multi-date inputs are de-duplicated after normalization, preserving first-seen order;
- (trade_date, asset_id) is a unique non-null primary key.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE

REQUIRED_CROSS_SECTION_COLUMNS: tuple[str, str] = (TRADE_DATE, ASSET_ID)


class CrossSectionFrameError(ValueError):
    """Raised when a cross-section frame fails validation."""


def _is_scalar_date_like(value: Any) -> bool:
    """Return True for a single date-like value (not a date sequence)."""
    if value is None or isinstance(value, (str, bytes, date, datetime, pd.Timestamp)):
        return True
    if isinstance(value, pd.Period):
        return True
    if getattr(value, "shape", None) == ():
        return True
    return False


def normalize_trade_date(value: Any) -> pd.Timestamp:
    """Normalize one trade_date to a timezone-naive midnight Timestamp.

    Accepts str / date / datetime / Timestamp (and other pandas-parseable
    scalars). Unparseable or empty values raise ``CrossSectionFrameError``.

    ``trade_date`` is a trading-day label, not a UTC event timestamp. When the
    input is timezone-aware, the timezone is stripped while preserving the local
    wall-clock calendar date, then normalized to midnight. It is never converted
    through UTC first (which would shift Asia/Shanghai midnight to the previous
    day).
    """
    if value is None:
        raise CrossSectionFrameError("trade_date must be non-empty and parseable")
    try:
        if isinstance(value, float) and pd.isna(value):
            raise CrossSectionFrameError("trade_date must be non-empty and parseable")
    except (TypeError, ValueError):
        pass

    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise CrossSectionFrameError("trade_date must be non-empty and parseable")
        value = text

    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise CrossSectionFrameError(
            f"trade_date must be non-empty and parseable: {value!r}"
        ) from exc

    if pd.isna(ts):
        raise CrossSectionFrameError(
            f"trade_date must be non-empty and parseable: {value!r}"
        )

    # Keep the local trading-day label; do not convert via UTC.
    if ts.tz is not None:
        ts = ts.tz_localize(None)
    return ts.normalize()


def normalize_trade_dates(trade_dates: Sequence[Any] | Any) -> list[pd.Timestamp]:
    """Normalize zero-or-more trade dates with scalar-safe iteration.

    Supports:
    - a single string / date / datetime / Timestamp
    - a sequence of date-like values
    - an empty sequence

    Strings are never iterated character-by-character. After normalization,
    duplicate calendar days are removed deterministically while preserving
    first-seen order.
    """
    if trade_dates is None:
        return []
    if _is_scalar_date_like(trade_dates):
        values = [trade_dates]
    elif isinstance(trade_dates, pd.Series):
        values = trade_dates.tolist()
    elif isinstance(trade_dates, Sequence) and not isinstance(trade_dates, (str, bytes)):
        values = list(trade_dates)
    else:
        values = [trade_dates]

    ordered: list[pd.Timestamp] = []
    seen: set[pd.Timestamp] = set()
    for value in values:
        day = normalize_trade_date(value)
        if day in seen:
            continue
        seen.add(day)
        ordered.append(day)
    return ordered


def normalize_trade_date_series(series: pd.Series) -> pd.Series:
    """Normalize a Series of trade dates to timezone-naive midnight timestamps."""
    if series.empty:
        return pd.Series(dtype="datetime64[ns]", index=series.index, name=series.name)
    return pd.Series(
        [normalize_trade_date(value) for value in series.tolist()],
        index=series.index,
        name=series.name,
        dtype="datetime64[ns]",
    )


def normalize_asset_id(value: Any) -> str:
    """Normalize one asset_id to a non-empty string."""
    if value is None:
        raise CrossSectionFrameError("asset_id must be a non-empty string")
    try:
        if isinstance(value, float) and pd.isna(value):
            raise CrossSectionFrameError("asset_id must be a non-empty string")
    except (TypeError, ValueError):
        pass
    if pd.isna(value):
        raise CrossSectionFrameError("asset_id must be a non-empty string")
    text = value.strip() if isinstance(value, str) else str(value).strip()
    if not text:
        raise CrossSectionFrameError("asset_id must be a non-empty string")
    return text


def normalize_asset_id_series(series: pd.Series) -> pd.Series:
    """Normalize a Series of asset ids to non-empty strings."""
    if series.empty:
        return series.astype(object)
    return series.map(normalize_asset_id)


def enforce_cross_section_primary_key(df: pd.DataFrame) -> None:
    """Raise if (trade_date, asset_id) is not a unique non-null primary key."""
    if df is None or df.empty:
        return
    if TRADE_DATE not in df.columns or ASSET_ID not in df.columns:
        raise CrossSectionFrameError(
            "cross-section frame missing required columns: "
            f"{[c for c in REQUIRED_CROSS_SECTION_COLUMNS if c not in getattr(df, 'columns', [])]}"
        )
    if df[TRADE_DATE].isna().any():
        raise CrossSectionFrameError("trade_date must be non-empty and parseable")
    if df[ASSET_ID].isna().any():
        raise CrossSectionFrameError("asset_id must be a non-empty string")
    blank_assets = df[ASSET_ID].map(lambda x: isinstance(x, str) and x == "")
    if bool(blank_assets.any()):
        raise CrossSectionFrameError("asset_id must be a non-empty string")
    duplicated = df.duplicated(subset=[TRADE_DATE, ASSET_ID], keep=False)
    if bool(duplicated.any()):
        sample = (
            df.loc[duplicated, [TRADE_DATE, ASSET_ID]]
            .drop_duplicates()
            .head(5)
            .to_dict(orient="records")
        )
        raise CrossSectionFrameError(
            "duplicate cross-section primary key (trade_date, asset_id): "
            f"{sample}"
        )


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
    enforce_primary_key: bool = True,
) -> pd.DataFrame:
    """Validate and normalize a cross-section input frame.

    Args:
        df: candidate cross-section DataFrame.
        feature_columns: optional feature columns that must exist when provided.
        require_features: when True, at least one feature column is required.
        copy: when True (default), return a defensive copy.
        enforce_primary_key: when True (default), require unique non-null
            ``(trade_date, asset_id)`` keys after normalization.

    Returns:
        A validated DataFrame. Empty/None inputs become empty frames that still
        carry trade_date/asset_id (and requested feature) columns when known.

        ``trade_date`` values are normalized to timezone-naive midnight
        timestamps. ``asset_id`` values are normalized to non-empty strings.
    """
    features = normalize_feature_columns(feature_columns)
    if require_features and not features:
        raise CrossSectionFrameError("at least one feature column is required")

    if df is None:
        columns = list(REQUIRED_CROSS_SECTION_COLUMNS) + features
        return empty_cross_section_frame(features)

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
    if out.empty:
        out[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
        out[ASSET_ID] = out[ASSET_ID].astype(object)
        return out

    out[TRADE_DATE] = normalize_trade_date_series(out[TRADE_DATE])
    out[ASSET_ID] = normalize_asset_id_series(out[ASSET_ID])
    if enforce_primary_key:
        enforce_cross_section_primary_key(out)
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
    out = pd.DataFrame(columns=cols)
    out[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    if ASSET_ID in out.columns:
        out[ASSET_ID] = pd.Series(dtype=object)
    return out
