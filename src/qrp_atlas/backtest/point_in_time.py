"""Point-in-time selection helpers for versioned historical records.

These helpers are intentionally independent from storage and data-source concerns.  They
select the single record that would have been visible for each entity on a given
trading date, without allowing later revisions to leak into earlier queries.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd


def select_latest_available_records(
    records: pd.DataFrame,
    *,
    as_of_date: Any,
    entity_keys: str | Sequence[str],
    available_date_col: str = "available_trade_date",
    published_at_col: str | None = "published_at",
    ingested_at_col: str | None = "ingested_at",
    revision_col: str | None = "revision_id",
) -> pd.DataFrame:
    """Select the latest point-in-time record for every entity group.

    A record is eligible only when its ``available_date_col`` is on or before
    ``as_of_date``.  Within an entity group, eligible versions are ordered by
    availability date, publication timestamp, ingestion timestamp, and finally
    revision identifier.  The revision identifier is only a deterministic
    tiebreaker for records with identical temporal fields; it is not interpreted
    as a publication sequence.

    Optional ordering columns that are not present in ``records`` are skipped.
    Invalid or missing availability dates are never eligible.  When every
    available ordering value ties, the later input row wins, giving deterministic
    and stable behavior for a fixed input order.

    Args:
        records: Versioned records to select from. The input is not modified.
        as_of_date: Historical trading date to evaluate.
        entity_keys: One or more columns that identify an entity group.
        available_date_col: Required column containing each version's effective
            trading date.
        published_at_col: Optional publication timestamp column.
        ingested_at_col: Optional ingestion timestamp column.
        revision_col: Optional stable tiebreaker column.

    Returns:
        A copy containing at most one original row per entity group, sorted by
        ``entity_keys`` with a reset index.

    Raises:
        ValueError: If ``records`` is not a DataFrame, a required column is
            missing, ``entity_keys`` is empty, or ``as_of_date`` is invalid.
    """
    if not isinstance(records, pd.DataFrame):
        raise ValueError("records must be a pandas DataFrame")

    normalized_entity_keys = _normalize_entity_keys(entity_keys)
    _validate_column_name(available_date_col, "available_date_col")
    required_columns = [*normalized_entity_keys, available_date_col]
    missing_columns = [column for column in required_columns if column not in records.columns]
    if missing_columns:
        raise ValueError(f"records missing required columns: {missing_columns}")

    as_of_timestamp = _coerce_as_of_date(as_of_date)

    if records.empty:
        return records.copy().reset_index(drop=True)

    working = records.copy()
    source_order_col = _temporary_column_name(working, "__pit_source_order")
    available_sort_col = _temporary_column_name(working, "__pit_available_date")
    working[source_order_col] = range(len(working))
    working[available_sort_col] = _coerce_trade_date_series(working[available_date_col])

    eligible = working.loc[working[available_sort_col].notna()]
    eligible = eligible.loc[eligible[available_sort_col] <= as_of_timestamp]
    if eligible.empty:
        return records.iloc[0:0].copy().reset_index(drop=True)

    sort_columns = [*normalized_entity_keys, available_sort_col]
    temporary_columns = [source_order_col, available_sort_col]

    for column, label in (
        (published_at_col, "published_at"),
        (ingested_at_col, "ingested_at"),
    ):
        if column is not None and column in records.columns:
            sort_column = _temporary_column_name(working, f"__pit_{label}")
            eligible = eligible.copy()
            eligible[sort_column] = _coerce_datetime_series(eligible[column])
            sort_columns.append(sort_column)
            temporary_columns.append(sort_column)

    if revision_col is not None and revision_col in records.columns:
        revision_sort_col = _temporary_column_name(working, "__pit_revision")
        eligible = eligible.copy()
        eligible[revision_sort_col] = eligible[revision_col].map(_revision_sort_key)
        sort_columns.append(revision_sort_col)
        temporary_columns.append(revision_sort_col)

    ordered = eligible.sort_values(
        [*sort_columns, source_order_col],
        kind="mergesort",
        na_position="first",
    )
    selected = ordered.drop_duplicates(subset=normalized_entity_keys, keep="last")
    selected = selected.sort_values(normalized_entity_keys, kind="mergesort")
    return selected.drop(columns=temporary_columns).reset_index(drop=True)


def _normalize_entity_keys(entity_keys: str | Sequence[str]) -> list[str]:
    """Return validated entity keys while allowing a single string shorthand."""
    if isinstance(entity_keys, str):
        keys = [entity_keys]
    else:
        try:
            keys = list(entity_keys)
        except TypeError as exc:
            raise ValueError("entity_keys must be a non-empty sequence of column names") from exc

    if not keys:
        raise ValueError("entity_keys must not be empty")
    if any(not isinstance(key, str) or not key for key in keys):
        raise ValueError("entity_keys must contain non-empty column names")
    return keys


def _validate_column_name(column: str, parameter_name: str) -> None:
    if not isinstance(column, str) or not column:
        raise ValueError(f"{parameter_name} must be a non-empty column name")


def _coerce_as_of_date(value: Any) -> pd.Timestamp:
    """Parse one date-like input into a timezone-naive normalized timestamp."""
    if value is None or isinstance(value, (list, tuple, set, dict, pd.Series, pd.Index)):
        raise ValueError("as_of_date must be one valid date-like scalar")

    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("as_of_date must be one valid date-like scalar") from exc

    if pd.isna(timestamp):
        raise ValueError("as_of_date must be one valid date-like scalar")
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_localize(None)
    return timestamp.normalize()


def _coerce_trade_date_series(values: pd.Series) -> pd.Series:
    """Coerce date values without allowing invalid values to become eligible."""
    return values.map(_coerce_optional_timestamp).map(
        lambda value: value.normalize() if not pd.isna(value) else pd.NaT
    )


def _coerce_datetime_series(values: pd.Series) -> pd.Series:
    """Coerce timestamp values while retaining time-of-day for ordering."""
    return values.map(_coerce_optional_timestamp)


def _coerce_optional_timestamp(value: Any) -> pd.Timestamp:
    """Return a timezone-naive timestamp or ``NaT`` for invalid scalar values."""
    try:
        if pd.isna(value):
            return pd.NaT
    except (TypeError, ValueError):
        return pd.NaT

    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError):
        return pd.NaT

    if pd.isna(timestamp):
        return pd.NaT
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_localize(None)
    return timestamp


def _revision_sort_key(value: Any) -> str:
    """Build a deterministic tiebreaker without assigning temporal meaning."""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        return ""
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}:{value!s}"


def _temporary_column_name(records: pd.DataFrame, prefix: str) -> str:
    """Choose a helper-column name that cannot overwrite an input field."""
    candidate = prefix
    suffix = 1
    while candidate in records.columns:
        candidate = f"{prefix}_{suffix}"
        suffix += 1
    return candidate
