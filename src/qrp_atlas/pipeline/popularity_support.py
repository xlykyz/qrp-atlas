"""Shared helpers and batch implementation for Task04-B1 M5 popularity data foundation.

Owns provider invocation, raw concatenation, Raw CSV, cleaning, logical snapshot
reconstruction, Top100 integrity validation, Clean CSV, and atomic DuckDB replacement
for dc_hot (Eastmoney popularity rank) and ths_hot (THS hot stock rank).
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    CONCEPT,
    CREATED_AT,
    CURRENT_PRICE,
    DC_HOT,
    HOT,
    INPUT_VERSION,
    LIST_NAME,
    NAME,
    PCT_CHANGE,
    RANK_POSITION,
    RANK_REASON,
    SNAPSHOT_COMPLETED_AT,
    SNAPSHOT_SEQ,
    SNAPSHOT_STARTED_AT,
    SNAPSHOT_SEQS,
    SOURCE,
    SOURCE_RANK_TIME,
    SOURCE_PROVENANCE,
    SOURCE_STATUS,
    THS_HOT,
    TICKER,
    TRADE_DATE,
    POPULARITY_AVAILABLE,
    POPULARITY_SOURCE_AVAILABILITY_TABLE,
    POPULARITY_UNAVAILABLE,
    VALID_SNAPSHOT_COUNT,
    align_to_schema,
    get_table,
    normalize_ticker,
    quick_validate,
)
from qrp_atlas.orchestration.execution_control import ExecutionControlError

from .contracts import (
    CheckResult,
    ContractError,
    PipelineInvocation,
    PipelineRunContext,
    TargetWindow,
)
from .tushare_snapshot_support import (
    CHINA_TZ,
    QUANT_DB_RESOURCE,
    QUANT_DB_WRITER,
    _is_missing,
    _target_bounds,
    parse_scope_date,
    provider_configuration,
    provider_freshness,
    resolve_date_or_range_target,
    target_dates,
    validate_single_date_override,
)

LOGGER = logging.getLogger(__name__)

DC_HOT_RAW_FIELDS = (
    "trade_date",
    "data_type",
    "ts_code",
    "ts_name",
    "rank",
    "pct_change",
    "current_price",
    "rank_time",
)

THS_HOT_RAW_FIELDS = (
    "trade_date",
    "data_type",
    "ts_code",
    "ts_name",
    "rank",
    "pct_change",
    "current_price",
    "concept",
    "rank_reason",
    "hot",
    "rank_time",
)

DC_SOURCE = "EASTMONEY"
DC_LIST_NAME = "POPULARITY"

THS_SOURCE = "THS"
THS_LIST_NAME = "HOT_STOCK"


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}_{time.monotonic_ns()}.tmp")
    try:
        frame.to_csv(temporary, index=False, encoding="utf-8")
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _format_trade_date_for_provider(value: date | str) -> str:
    if isinstance(value, str):
        parsed = parse_scope_date(value, "trade_date")
        if parsed is None:
            raise ContractError("PROVIDER_DATE_INVALID", "trade_date is empty")
        return parsed.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    raise ContractError("PROVIDER_DATE_INVALID", f"unsupported date type: {type(value)}")


# ── 17.1 Provider Methods ──


def fetch_dc_hot(client: object, trade_date: date | str) -> pd.DataFrame:
    """Fetch raw Eastmoney popularity rank (A-share, popularity list) from Tushare.

    Fixed parameters per frozen design:
        market="A股市场"
        hot_type="人气榜"
        is_new="N"
    Returns the unmodified raw DataFrame from the provider.
    """
    method = getattr(client, "dc_hot", None)
    if not callable(method):
        raise ContractError("PROVIDER_API_UNAVAILABLE", "client missing dc_hot")
    date_str = _format_trade_date_for_provider(trade_date)
    try:
        return method(
            trade_date=date_str,
            market="A股市场",
            hot_type="人气榜",
            is_new="N",
        )
    except ExecutionControlError:
        raise
    except Exception as exc:
        raise ContractError("PROVIDER_REQUEST_FAILED", type(exc).__name__) from exc


def fetch_ths_hot(client: object, trade_date: date | str) -> pd.DataFrame:
    """Fetch raw THS hot stock rank from Tushare.

    Fixed parameters per frozen design:
        market="热股"
        is_new="N"
    Returns the unmodified raw DataFrame from the provider.
    """
    method = getattr(client, "ths_hot", None)
    if not callable(method):
        raise ContractError("PROVIDER_API_UNAVAILABLE", "client missing ths_hot")
    date_str = _format_trade_date_for_provider(trade_date)
    try:
        return method(
            trade_date=date_str,
            market="热股",
            is_new="N",
        )
    except ExecutionControlError:
        raise
    except Exception as exc:
        raise ContractError("PROVIDER_REQUEST_FAILED", type(exc).__name__) from exc


# ── 17.2 Batch Methods ──


def _validate_raw_frame(
    raw: object,
    *,
    endpoint: str,
    required_fields: tuple[str, ...],
    requested_date: date,
    target_window: TargetWindow,
) -> pd.DataFrame:
    if raw is None:
        raise ContractError("PROVIDER_RESPONSE_INVALID", f"{endpoint} returned None instead of a DataFrame")
    if not isinstance(raw, pd.DataFrame):
        raise ContractError(
            "PROVIDER_RESPONSE_INVALID",
            f"{endpoint} returned {type(raw).__name__}, expected DataFrame",
        )
    if raw.empty:
        return raw
    missing = sorted(set(required_fields) - set(raw.columns))
    if missing:
        raise ContractError("PROVIDER_SCHEMA_MISSING", ",".join(missing))

    # Verify returned trade_date matches requested_date
    for val in raw["trade_date"].tolist():
        parsed = parse_scope_date(val, "trade_date")
        if parsed != requested_date:
            raise ContractError(
                "PROVIDER_SCOPE_MISMATCH",
                f"row trade_date {val} does not match requested date {requested_date.isoformat()}",
            )
    return raw


def fetch_dc_hot_range(
    context: PipelineRunContext,
    client: object,
) -> tuple[pd.DataFrame, Path, tuple[date, ...], int, int]:
    """Fetch dc_hot across target_window, concatenate into one Raw DataFrame, and write Raw CSV."""
    frames: list[pd.DataFrame] = []
    empty_dates: list[date] = []
    expected_raw_columns: list[str] | None = None
    rows_read = 0
    api_requests = 0

    for requested_date in target_dates(context.target_window):
        context.execution_control.check()
        raw = fetch_dc_hot(client, requested_date)
        context.execution_control.check()
        checked = _validate_raw_frame(
            raw,
            endpoint="dc_hot",
            required_fields=DC_HOT_RAW_FIELDS,
            requested_date=requested_date,
            target_window=context.target_window,
        )
        api_requests += 1
        if checked.empty:
            empty_dates.append(requested_date)
        else:
            current_columns = list(checked.columns)
            if expected_raw_columns is None:
                expected_raw_columns = current_columns
            elif current_columns != expected_raw_columns:
                raise ContractError(
                    "PROVIDER_SCHEMA_DRIFT",
                    f"dc_hot raw columns drifted on {requested_date.isoformat()}: "
                    f"expected {expected_raw_columns}, got {current_columns}",
                )
            rows_read += len(checked)
            frames.append(checked)

    if frames:
        raw_concat = pd.concat(frames, ignore_index=True, sort=False)
    else:
        raw_concat = pd.DataFrame(columns=list(DC_HOT_RAW_FIELDS))

    start_date, end_date = _target_bounds(context)
    raw_path = (
        context.settings.paths.raw_dir
        / "dc_hot"
        / f"dc_hot_raw_{start_date.isoformat()}_{end_date.isoformat()}.csv"
    )
    _atomic_csv(raw_concat, raw_path)
    return raw_concat, raw_path, tuple(empty_dates), rows_read, api_requests


def fetch_ths_hot_range(
    context: PipelineRunContext,
    client: object,
) -> tuple[pd.DataFrame, Path, tuple[date, ...], int, int]:
    """Fetch ths_hot across target_window, concatenate into one Raw DataFrame, and write Raw CSV."""
    frames: list[pd.DataFrame] = []
    empty_dates: list[date] = []
    expected_raw_columns: list[str] | None = None
    rows_read = 0
    api_requests = 0

    for requested_date in target_dates(context.target_window):
        context.execution_control.check()
        raw = fetch_ths_hot(client, requested_date)
        context.execution_control.check()
        checked = _validate_raw_frame(
            raw,
            endpoint="ths_hot",
            required_fields=THS_HOT_RAW_FIELDS,
            requested_date=requested_date,
            target_window=context.target_window,
        )
        api_requests += 1
        if checked.empty:
            empty_dates.append(requested_date)
        else:
            current_columns = list(checked.columns)
            if expected_raw_columns is None:
                expected_raw_columns = current_columns
            elif current_columns != expected_raw_columns:
                raise ContractError(
                    "PROVIDER_SCHEMA_DRIFT",
                    f"ths_hot raw columns drifted on {requested_date.isoformat()}: "
                    f"expected {expected_raw_columns}, got {current_columns}",
                )
            rows_read += len(checked)
            frames.append(checked)

    if frames:
        raw_concat = pd.concat(frames, ignore_index=True, sort=False)
    else:
        raw_concat = pd.DataFrame(columns=list(THS_HOT_RAW_FIELDS))

    start_date, end_date = _target_bounds(context)
    raw_path = (
        context.settings.paths.raw_dir
        / "ths_hot"
        / f"ths_hot_raw_{start_date.isoformat()}_{end_date.isoformat()}.csv"
    )
    _atomic_csv(raw_concat, raw_path)
    return raw_concat, raw_path, tuple(empty_dates), rows_read, api_requests


def _reconstruct_snapshots(
    df: pd.DataFrame,
    *,
    error_code: str,
) -> pd.DataFrame:
    """Reconstruct logical snapshots and validate Top100 constraints per Section 7 & 8."""
    if df.empty:
        return df

    reconstructed_groups: list[pd.DataFrame] = []

    for td, date_group in df.groupby("trade_date", sort=False):
        date_df = date_group.copy()
        total_rows = len(date_df)
        if total_rows % 100 != 0:
            raise ContractError(
                error_code,
                f"trade_date {td} has {total_rows} rows, not a multiple of 100",
            )

        # Retain original index as tie breaker for identical rank_time (Section 22)
        date_df["_orig_idx"] = np.arange(len(date_df))

        # Assign snapshot_seq by occurrence order of rank_position sorted by source_rank_time
        date_df = date_df.sort_values(
            by=["rank_position", "source_rank_time", "_orig_idx"],
            ascending=[True, True, True],
            kind="stable",
        )
        date_df["snapshot_seq"] = date_df.groupby("rank_position", sort=False).cumcount() + 1

        seqs = sorted(date_df["snapshot_seq"].unique())
        expected_seqs = list(range(1, len(seqs) + 1))
        if seqs != expected_seqs:
            raise ContractError(
                error_code,
                f"Snapshot seq discontinuity on {td}: found {seqs}, expected {expected_seqs}",
            )

        snapshot_times: dict[int, tuple[str, str]] = {}

        # Validate Section 8 constraints for each logical snapshot
        for seq in seqs:
            snap = date_df[date_df["snapshot_seq"] == seq]
            if len(snap) != 100:
                raise ContractError(
                    error_code,
                    f"Snapshot {seq} on {td} has {len(snap)} rows, expected 100",
                )
            if snap["ticker"].nunique() != 100:
                raise ContractError(
                    error_code,
                    f"Snapshot {seq} on {td} has {snap['ticker'].nunique()} distinct tickers, expected 100",
                )
            if snap["rank_position"].nunique() != 100:
                raise ContractError(
                    error_code,
                    f"Snapshot {seq} on {td} has {snap['rank_position'].nunique()} distinct ranks, expected 100",
                )
            if int(snap["rank_position"].min()) != 1 or int(snap["rank_position"].max()) != 100:
                raise ContractError(
                    error_code,
                    f"Snapshot {seq} on {td} ranks span {snap['rank_position'].min()}..{snap['rank_position'].max()}, expected 1..100",
                )
            if set(snap["rank_position"]) != set(range(1, 101)):
                raise ContractError(
                    error_code,
                    f"Snapshot {seq} on {td} does not exactly cover ranks 1..100",
                )

            start_t = str(snap["source_rank_time"].min())
            end_t = str(snap["source_rank_time"].max())
            date_df.loc[snap.index, "snapshot_started_at"] = start_t
            date_df.loc[snap.index, "snapshot_completed_at"] = end_t
            snapshot_times[seq] = (start_t, end_t)

        # Monotonicity check across sequential snapshots on the same date (Section 8)
        for i in range(len(seqs) - 1):
            prev_seq = seqs[i]
            next_seq = seqs[i + 1]
            prev_completed = snapshot_times[prev_seq][1]
            next_started = snapshot_times[next_seq][0]
            if not (prev_completed < next_started):
                raise ContractError(
                    error_code,
                    f"Snapshot timing anomaly on {td}: snapshot {prev_seq} completed at {prev_completed} "
                    f">= snapshot {next_seq} started at {next_started}",
                )

        date_df = date_df.drop(columns=["_orig_idx"])
        reconstructed_groups.append(date_df)

    combined = pd.concat(reconstructed_groups, ignore_index=True, sort=False)
    # Canonical ordering by primary key: trade_date, snapshot_seq, rank_position
    return combined.sort_values(
        by=["trade_date", "snapshot_seq", "rank_position"],
        ascending=[True, True, True],
    ).reset_index(drop=True)


def clean_dc_hot_batch(
    raw_df: pd.DataFrame,
    target_window: TargetWindow,
    context: PipelineRunContext | None = None,
) -> tuple[pd.DataFrame, Path | None]:
    """Clean the raw DC hot batch into canonical schema, validate snapshots, and write Clean CSV."""
    table = get_table(DC_HOT.name)
    columns_no_created = [col for col in table.column_names() if col != CREATED_AT]

    start_date = target_window.target_date or target_window.start_date
    end_date = target_window.target_date or target_window.end_date
    assert start_date is not None and end_date is not None

    clean_path: Path | None = None
    if context is not None:
        clean_path = (
            context.settings.paths.canonical_dir
            / "dc_hot"
            / f"dc_hot_clean_{start_date.isoformat()}_{end_date.isoformat()}.csv"
        )

    if raw_df.empty:
        empty_clean = pd.DataFrame(columns=columns_no_created)
        if clean_path is not None:
            _atomic_csv(empty_clean, clean_path)
        return empty_clean, clean_path

    missing = sorted(set(DC_HOT_RAW_FIELDS) - set(raw_df.columns))
    if missing:
        raise ContractError("DC_HOT_API_PARTIAL", f"raw missing fields: {','.join(missing)}")

    cleaned = pd.DataFrame()

    # trade_date
    parsed_dates: list[date] = []
    for val in raw_df["trade_date"].tolist():
        d = parse_scope_date(val, "trade_date")
        if d is None or not (start_date <= d <= end_date):
            raise ContractError("DC_HOT_API_PARTIAL", f"invalid trade_date: {val}")
        parsed_dates.append(d)
    cleaned["trade_date"] = parsed_dates

    # source & list_name
    cleaned["source"] = DC_SOURCE
    cleaned["list_name"] = DC_LIST_NAME

    # ticker
    cleaned["ticker"] = [
        normalize_ticker(str(val)) if not _is_missing(val) else "" for val in raw_df["ts_code"].tolist()
    ]
    if (cleaned["ticker"] == "").any():
        raise ContractError("DC_HOT_API_PARTIAL", "empty or invalid ticker found")

    # name
    cleaned["name"] = [
        None if _is_missing(val) else str(val).strip() for val in raw_df["ts_name"].tolist()
    ]

    # rank_position
    ranks: list[int] = []
    for val in raw_df["rank"].tolist():
        try:
            num = float(val)
            if not num.is_integer() or not (1 <= num <= 100):
                raise ValueError
            ranks.append(int(num))
        except (ValueError, TypeError) as exc:
            raise ContractError("DC_HOT_API_PARTIAL", f"invalid rank value: {val}") from exc
    cleaned["rank_position"] = ranks

    # pct_change
    pcts: list[float | None] = []
    for val in raw_df["pct_change"].tolist():
        if _is_missing(val):
            pcts.append(None)
        else:
            try:
                num = float(val)
                pcts.append(num if math.isfinite(num) else None)
            except (ValueError, TypeError) as exc:
                raise ContractError("DC_HOT_API_PARTIAL", f"invalid pct_change: {val}") from exc
    cleaned["pct_change"] = pcts

    # current_price
    prices: list[float | None] = []
    for val in raw_df["current_price"].tolist():
        if _is_missing(val):
            prices.append(None)
        else:
            try:
                num = float(val)
                prices.append(num if math.isfinite(num) else None)
            except (ValueError, TypeError) as exc:
                raise ContractError("DC_HOT_API_PARTIAL", f"invalid current_price: {val}") from exc
    cleaned["current_price"] = prices

    # source_rank_time
    times: list[str] = []
    for val in raw_df["rank_time"].tolist():
        if _is_missing(val):
            raise ContractError("DC_HOT_API_PARTIAL", "missing rank_time")
        times.append(str(val).strip())
    cleaned["source_rank_time"] = times

    # Snapshot reconstruction & validation
    reconstructed = _reconstruct_snapshots(cleaned, error_code="DC_HOT_API_PARTIAL")

    try:
        aligned = align_to_schema(reconstructed, DC_HOT.name, fill_missing_optional=True, drop_extra=True)
        validated = quick_validate(aligned, DC_HOT.name, allow_extra=False)
    except Exception as exc:
        raise ContractError("DC_HOT_API_PARTIAL", type(exc).__name__) from exc

    if clean_path is not None:
        _atomic_csv(validated[columns_no_created], clean_path)

    return validated, clean_path


def clean_ths_hot_batch(
    raw_df: pd.DataFrame,
    target_window: TargetWindow,
    context: PipelineRunContext | None = None,
) -> tuple[pd.DataFrame, Path | None]:
    """Clean the raw THS hot batch into canonical schema, validate snapshots, and write Clean CSV."""
    table = get_table(THS_HOT.name)
    columns_no_created = [col for col in table.column_names() if col != CREATED_AT]

    start_date = target_window.target_date or target_window.start_date
    end_date = target_window.target_date or target_window.end_date
    assert start_date is not None and end_date is not None

    clean_path: Path | None = None
    if context is not None:
        clean_path = (
            context.settings.paths.canonical_dir
            / "ths_hot"
            / f"ths_hot_clean_{start_date.isoformat()}_{end_date.isoformat()}.csv"
        )

    if raw_df.empty:
        empty_clean = pd.DataFrame(columns=columns_no_created)
        if clean_path is not None:
            _atomic_csv(empty_clean, clean_path)
        return empty_clean, clean_path

    missing = sorted(set(THS_HOT_RAW_FIELDS) - set(raw_df.columns))
    if missing:
        raise ContractError("THS_HOT_API_PARTIAL", f"raw missing fields: {','.join(missing)}")

    cleaned = pd.DataFrame()

    # trade_date
    parsed_dates: list[date] = []
    for val in raw_df["trade_date"].tolist():
        d = parse_scope_date(val, "trade_date")
        if d is None or not (start_date <= d <= end_date):
            raise ContractError("THS_HOT_API_PARTIAL", f"invalid trade_date: {val}")
        parsed_dates.append(d)
    cleaned["trade_date"] = parsed_dates

    # source & list_name
    cleaned["source"] = THS_SOURCE
    cleaned["list_name"] = THS_LIST_NAME

    # ticker
    cleaned["ticker"] = [
        normalize_ticker(str(val)) if not _is_missing(val) else "" for val in raw_df["ts_code"].tolist()
    ]
    if (cleaned["ticker"] == "").any():
        raise ContractError("THS_HOT_API_PARTIAL", "empty or invalid ticker found")

    # name
    cleaned["name"] = [
        None if _is_missing(val) else str(val).strip() for val in raw_df["ts_name"].tolist()
    ]

    # rank_position
    ranks: list[int] = []
    for val in raw_df["rank"].tolist():
        try:
            num = float(val)
            if not num.is_integer() or not (1 <= num <= 100):
                raise ValueError
            ranks.append(int(num))
        except (ValueError, TypeError) as exc:
            raise ContractError("THS_HOT_API_PARTIAL", f"invalid rank value: {val}") from exc
    cleaned["rank_position"] = ranks

    # pct_change
    pcts: list[float | None] = []
    for val in raw_df["pct_change"].tolist():
        if _is_missing(val):
            pcts.append(None)
        else:
            try:
                num = float(val)
                pcts.append(num if math.isfinite(num) else None)
            except (ValueError, TypeError) as exc:
                raise ContractError("THS_HOT_API_PARTIAL", f"invalid pct_change: {val}") from exc
    cleaned["pct_change"] = pcts

    # current_price
    prices: list[float | None] = []
    for val in raw_df["current_price"].tolist():
        if _is_missing(val):
            prices.append(None)
        else:
            try:
                num = float(val)
                prices.append(num if math.isfinite(num) else None)
            except (ValueError, TypeError) as exc:
                raise ContractError("THS_HOT_API_PARTIAL", f"invalid current_price: {val}") from exc
    cleaned["current_price"] = prices

    # hot (DOUBLE)
    hots: list[float | None] = []
    for val in raw_df["hot"].tolist():
        if _is_missing(val):
            hots.append(None)
        else:
            try:
                num = float(val)
                hots.append(num if math.isfinite(num) else None)
            except (ValueError, TypeError) as exc:
                raise ContractError("THS_HOT_API_PARTIAL", f"invalid hot value: {val}") from exc
    cleaned["hot"] = hots

    # concept & rank_reason
    cleaned["concept"] = [
        None if _is_missing(val) else str(val).strip() for val in raw_df["concept"].tolist()
    ]
    cleaned["rank_reason"] = [
        None if _is_missing(val) else str(val).strip() for val in raw_df["rank_reason"].tolist()
    ]

    # source_rank_time
    times: list[str] = []
    for val in raw_df["rank_time"].tolist():
        if _is_missing(val):
            raise ContractError("THS_HOT_API_PARTIAL", "missing rank_time")
        times.append(str(val).strip())
    cleaned["source_rank_time"] = times

    # Snapshot reconstruction & validation
    reconstructed = _reconstruct_snapshots(cleaned, error_code="THS_HOT_API_PARTIAL")

    try:
        aligned = align_to_schema(reconstructed, THS_HOT.name, fill_missing_optional=True, drop_extra=True)
        validated = quick_validate(aligned, THS_HOT.name, allow_extra=False)
    except Exception as exc:
        raise ContractError("THS_HOT_API_PARTIAL", type(exc).__name__) from exc

    if clean_path is not None:
        _atomic_csv(validated[columns_no_created], clean_path)

    return validated, clean_path


# ── 17.3 Persistence Methods ──


def replace_popularity_batch(
    context: PipelineRunContext,
    *,
    table_name: str,
    prepared: pd.DataFrame,
    empty_dates: Iterable[date],
    empty_response_error_code: str,
    write_failed_error_code: str,
) -> tuple[int, float]:
    """Single-connection, single-transaction atomic replacement for one popularity batch."""
    table = get_table(table_name)
    availability_table = get_table(POPULARITY_SOURCE_AVAILABILITY_TABLE)
    requested_dates = target_dates(context.target_window)
    requested_date_set = set(requested_dates)
    empty_date_set = set(empty_dates)
    if not empty_date_set <= requested_date_set:
        raise ContractError("INVALID_EMPTY_DATE_SCOPE", table_name)

    started = time.monotonic()
    connection: duckdb.DuckDBPyConnection | None = None
    transaction_open = False
    registered = False
    availability_registered = False
    try:
        context.execution_control.check()
        connection = duckdb.connect(str(context.settings.paths.duckdb_path))
        connection.execute(table.duckdb_create_sql())
        connection.execute(availability_table.duckdb_create_sql())

        # Section 13: Fail-closed check if empty response would wipe existing historical data
        for empty_date in requested_dates:
            if empty_date not in empty_date_set:
                continue
            context.execution_control.check()
            existing = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table.name} WHERE trade_date = ?",
                    [empty_date],
                ).fetchone()[0]
            )
            if existing:
                raise ContractError(
                    empty_response_error_code,
                    f"{table.name} returned an empty response for {empty_date.isoformat()} "
                    f"but {existing} existing rows would be removed",
                )

        context.execution_control.check()
        connection.execute("BEGIN TRANSACTION")
        transaction_open = True

        # Replace only successfully fetched and verified non-empty dates
        for requested_date in requested_dates:
            if requested_date in empty_date_set:
                continue
            connection.execute(
                f"DELETE FROM {table.name} WHERE trade_date = ?",
                [requested_date],
            )
            connection.execute(
                f"DELETE FROM {availability_table.name} WHERE trade_date = ? AND source = ?",
                [requested_date, table_name],
            )
            context.execution_control.check()

        if not prepared.empty:
            columns = [column for column in table.column_names() if column != CREATED_AT]
            connection.register("_popularity_batch_rows", prepared[columns])
            registered = True
            connection.execute(
                f"INSERT INTO {table.name} ({', '.join(columns)}) "
                f"SELECT {', '.join(columns)} FROM _popularity_batch_rows"
            )
            connection.unregister("_popularity_batch_rows")
            registered = False

        # Availability is a first-class, replayable source/date fact. It is
        # committed with the popularity rows so Task06 never observes a source
        # table and an unrelated availability version.
        availability_rows: list[dict[str, object]] = []
        for requested_date in requested_dates:
            if requested_date in empty_date_set:
                snapshot_seqs: list[int] = []
                status = POPULARITY_UNAVAILABLE
                consumed_rows = 0
            else:
                date_rows = prepared.loc[prepared[TRADE_DATE].eq(requested_date)]
                snapshot_seqs = sorted(
                    int(value) for value in date_rows[SNAPSHOT_SEQ].dropna().unique()
                )
                status = POPULARITY_AVAILABLE
                consumed_rows = len(date_rows)
            availability_rows.append(
                {
                    TRADE_DATE: requested_date,
                    SOURCE: table_name,
                    SOURCE_STATUS: status,
                    VALID_SNAPSHOT_COUNT: len(snapshot_seqs),
                    SNAPSHOT_SEQS: json.dumps(snapshot_seqs, separators=(",", ":")),
                    INPUT_VERSION: f"{context.pipeline_id}:{context.run_id}:{requested_date.isoformat()}",
                    SOURCE_PROVENANCE: json.dumps(
                        {
                            "pipeline_id": context.pipeline_id,
                            "pipeline_run_id": context.run_id,
                            "table": table_name,
                            "consumed_rows": consumed_rows,
                            "target_date": requested_date.isoformat(),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "source_pipeline_run_id": context.run_id,
                }
            )
        availability_frame = pd.DataFrame(availability_rows)
        availability_columns = [
            column for column in availability_table.column_names() if column != CREATED_AT
        ]
        connection.register(
            "_popularity_availability_rows",
            availability_frame[availability_columns],
        )
        availability_registered = True
        connection.execute(
            f"DELETE FROM {availability_table.name} WHERE trade_date IN "
            f"(SELECT trade_date FROM _popularity_availability_rows) AND source = ?",
            [table_name],
        )
        connection.execute(
            f"INSERT INTO {availability_table.name} ({', '.join(availability_columns)}) "
            f"SELECT {', '.join(availability_columns)} FROM _popularity_availability_rows"
        )
        connection.unregister("_popularity_availability_rows")
        availability_registered = False

        context.execution_control.check()
        connection.execute("COMMIT")
        transaction_open = False
        return len(prepared), time.monotonic() - started
    except ExecutionControlError:
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception as rollback_err:
                LOGGER.debug("rollback failed on execution control stop", exc_info=rollback_err)
        raise
    except ContractError:
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception as rollback_err:
                LOGGER.debug("rollback failed on contract error", exc_info=rollback_err)
        raise
    except Exception as exc:
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception as rollback_err:
                LOGGER.debug("rollback failed on unexpected error", exc_info=rollback_err)
        raise ContractError(write_failed_error_code, type(exc).__name__) from exc
    finally:
        if connection is not None:
            if registered:
                try:
                    connection.unregister("_popularity_batch_rows")
                except Exception as unregister_err:
                    LOGGER.debug("unregister failed", exc_info=unregister_err)
            if availability_registered:
                try:
                    connection.unregister("_popularity_availability_rows")
                except Exception as unregister_err:
                    LOGGER.debug("availability unregister failed", exc_info=unregister_err)
            connection.close()


def replace_dc_hot_batch(
    context: PipelineRunContext,
    prepared: pd.DataFrame,
    empty_dates: Iterable[date],
) -> tuple[int, float]:
    return replace_popularity_batch(
        context,
        table_name=DC_HOT.name,
        prepared=prepared,
        empty_dates=empty_dates,
        empty_response_error_code="DC_HOT_API_PARTIAL",
        write_failed_error_code="DC_HOT_WRITE_FAILED",
    )


def replace_ths_hot_batch(
    context: PipelineRunContext,
    prepared: pd.DataFrame,
    empty_dates: Iterable[date],
) -> tuple[int, float]:
    return replace_popularity_batch(
        context,
        table_name=THS_HOT.name,
        prepared=prepared,
        empty_dates=empty_dates,
        empty_response_error_code="THS_HOT_API_PARTIAL",
        write_failed_error_code="THS_HOT_WRITE_FAILED",
    )


# ── Quality & Completion Checkers ──


def popularity_range_completion(table_name: str, error_code: str):
    """Return a completion check verifying output rows in DuckDB."""
    def check(context: PipelineRunContext) -> CheckResult:
        try:
            start_date, end_date = _target_bounds(context)
            connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
            try:
                rows = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE trade_date BETWEEN ? AND ?",
                        [start_date, end_date],
                    ).fetchone()[0]
                )
            finally:
                connection.close()
        except Exception as exc:
            return CheckResult.failure(
                f"{table_name}_completion",
                error_code,
                "output table could not be read after committed replacement",
                exception=type(exc).__name__,
            )
        return CheckResult.success(
            f"{table_name}_completion",
            rows=rows,
            empty_snapshot=rows == 0,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

    check.__name__ = f"{table_name}_range_completion"
    return check


def popularity_unique_key_quality(table_name: str, error_code: str):
    """Return a target-range uniqueness quality check for (trade_date, snapshot_seq, rank_position)."""
    keys = "trade_date, snapshot_seq, rank_position"

    def check(context: PipelineRunContext) -> CheckResult:
        try:
            start_date, end_date = _target_bounds(context)
            connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
            try:
                duplicates = int(
                    connection.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM (
                            SELECT {keys}, COUNT(*) AS row_count
                            FROM {table_name}
                            WHERE trade_date BETWEEN ? AND ?
                            GROUP BY {keys}
                            HAVING COUNT(*) > 1
                        )
                        """,
                        [start_date, end_date],
                    ).fetchone()[0]
                )
            finally:
                connection.close()
        except Exception as exc:
            return CheckResult.failure(
                f"{table_name}_unique_key_quality",
                error_code,
                "output key quality could not be checked",
                exception=type(exc).__name__,
            )
        if duplicates:
            return CheckResult.failure(
                f"{table_name}_unique_key_quality",
                error_code,
                "output contains duplicate primary keys",
                duplicate_keys=duplicates,
            )
        return CheckResult.success(
            f"{table_name}_unique_key_quality",
            duplicate_keys=0,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

    check.__name__ = f"{table_name}_unique_key_quality"
    return check


def popularity_unique_ticker_quality(table_name: str, error_code: str):
    """Return a target-range uniqueness quality check for (trade_date, snapshot_seq, ticker)."""
    keys = "trade_date, snapshot_seq, ticker"

    def check(context: PipelineRunContext) -> CheckResult:
        try:
            start_date, end_date = _target_bounds(context)
            connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
            try:
                duplicates = int(
                    connection.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM (
                            SELECT {keys}, COUNT(*) AS row_count
                            FROM {table_name}
                            WHERE trade_date BETWEEN ? AND ?
                            GROUP BY {keys}
                            HAVING COUNT(*) > 1
                        )
                        """,
                        [start_date, end_date],
                    ).fetchone()[0]
                )
            finally:
                connection.close()
        except Exception as exc:
            return CheckResult.failure(
                f"{table_name}_unique_ticker_quality",
                error_code,
                "output ticker uniqueness could not be checked",
                exception=type(exc).__name__,
            )
        if duplicates:
            return CheckResult.failure(
                f"{table_name}_unique_ticker_quality",
                error_code,
                "output contains duplicate tickers within the same snapshot",
                duplicate_tickers=duplicates,
            )
        return CheckResult.success(
            f"{table_name}_unique_ticker_quality",
            duplicate_tickers=0,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

    check.__name__ = f"{table_name}_unique_ticker_quality"
    return check
