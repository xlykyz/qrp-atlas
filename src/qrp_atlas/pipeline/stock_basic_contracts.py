"""Formal current-state Pipeline for the Tushare ``stock_basic`` endpoint.

The standard output is a complete current snapshot in ``stock_info``. Provider
identities that are explicitly historical but do not have a standard trading
code are retained in ``stock_info_historical_identity`` in the same atomic
replacement. This keeps provider identity, standard trading code, and the
standard-table compatibility key distinct without creating a general MDM layer.
"""

from __future__ import annotations

import re
import time
from datetime import date
from itertools import product
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.config.tushare_client import get_tushare_pro
from qrp_atlas.contracts import (
    CAPTURED_AT,
    DELIST_DATE,
    EXCHANGE,
    IDENTITY_TYPE,
    IS_ACTIVE,
    ISOLATION_REASON,
    LIST_DATE,
    LIST_STATUS,
    PROVIDER,
    SNAPSHOT_DATE,
    STANDARD_TICKER,
    STOCK_INFO,
    STOCK_INFO_HISTORICAL_IDENTITY,
    TICKER,
    TS_CODE,
    TUSHARE_STOCK_BASIC,
    UPDATED_AT,
    align_to_schema,
    quick_validate,
)
from qrp_atlas.orchestration.execution_control import ExecutionControlError
from qrp_atlas.orchestration.models import OverlapPolicy

from .contracts import (
    BusinessExecution,
    CheckResult,
    CompletionContract,
    ContractError,
    ExecutionPolicy,
    FreshnessContract,
    IdempotencyContract,
    InputContract,
    InputKind,
    NonTradingDayPolicy,
    OutputContract,
    OutputResult,
    PerformanceBudget,
    PipelineContract,
    PipelineKind,
    PipelineMetrics,
    PipelineRunContext,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .registry import register_pipeline


CHINA_TZ = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
STOCK_BASIC_LIST_STATUSES: tuple[str, ...] = ("L", "D", "P", "G")
STOCK_BASIC_EXCHANGES: tuple[str, ...] = ("SSE", "SZSE", "BSE")
STOCK_BASIC_QUERY_PARTITIONS: tuple[tuple[str, str], ...] = tuple(
    product(STOCK_BASIC_LIST_STATUSES, STOCK_BASIC_EXCHANGES)
)
STOCK_BASIC_FIELDS = ",".join(TUSHARE_STOCK_BASIC.keys())
STOCK_BASIC_REQUIRED_FIELDS = tuple(TUSHARE_STOCK_BASIC.values())
STOCK_BASIC_CODE_PATTERN = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")
STOCK_BASIC_EXCHANGE_SUFFIX = {"SSE": ".SH", "SZSE": ".SZ", "BSE": ".BJ"}
STOCK_BASIC_PROVIDER = "tushare"
STOCK_BASIC_STANDARD_IDENTITY = "standard_trading_code"
STOCK_BASIC_HISTORICAL_IDENTITY = "provider_historical_id"
STOCK_BASIC_HISTORICAL_REASON = "non_standard_provider_identity"
STOCK_BASIC_IDENTITY_FIELDS = (TS_CODE, EXCHANGE, LIST_STATUS)


def _resolve_stock_basic_target(invocation) -> TargetWindow:
    invocation.execution_control.check()
    target = invocation.scheduled_for.astimezone(CHINA_TZ).date()
    return TargetWindow.for_date(target)


def _validate_stock_basic_date(target_date: date, _invocation) -> bool:
    return isinstance(target_date, date)


STOCK_BASIC_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="stock_basic_snapshot_clock_date_v1",
    description=(
        "Uses the Asia/Shanghai execution date as the current stock reference snapshot date; "
        "trading-calendar status is irrelevant."
    ),
    trading_calendar_id="clock:Asia/Shanghai",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_resolve_stock_basic_target,
    validate_explicit_date=_validate_stock_basic_date,
)


def _tushare_configuration(context: PipelineRunContext) -> CheckResult:
    if not context.settings.external_services.tushare_token:
        return CheckResult.failure(
            "tushare_configuration",
            "TUSHARE_CONFIGURATION_MISSING",
            "TUSHARE_TOKEN must be configured by the approved QRP environment",
        )
    return CheckResult.success("tushare_configuration", configured=True, provider="tushare")


def _snapshot_freshness(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    target = context.target_window.target_date
    return CheckResult.success(
        "stock_basic_snapshot_freshness",
        target_date=target.isoformat() if target else None,
        semantics="provider snapshot is observed during this execution",
    )


def _required_columns(frame: pd.DataFrame) -> None:
    missing = sorted(set(STOCK_BASIC_REQUIRED_FIELDS) - set(frame.columns))
    if missing:
        raise ContractError("STOCK_BASIC_API_PARTIAL", ", ".join(missing))


def _normalise_provider_dates(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for column in (LIST_DATE, DELIST_DATE):
        raw = result[column].astype("string").str.strip()
        empty = raw.isna() | raw.isin(("", "None", "none", "nan", "NaN"))
        parsed = pd.to_datetime(raw.mask(empty, None), errors="coerce")
        invalid = (~empty) & parsed.isna()
        if invalid.any():
            raise ContractError("STOCK_BASIC_API_PARTIAL", f"{column} contains invalid dates")
        result[column] = parsed.dt.date
    return result


def _normalize_provider_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        raise ContractError("STOCK_BASIC_API_EMPTY", "all stock_basic partitions returned no rows")

    frame = pd.concat(frames, ignore_index=True, sort=False).copy()
    _required_columns(frame)

    for column in STOCK_BASIC_REQUIRED_FIELDS:
        values = frame[column].astype("string").str.strip()
        frame[column] = values.mask(values.eq(""), pd.NA)

    for column in STOCK_BASIC_IDENTITY_FIELDS:
        if frame[column].isna().any():
            raise ContractError("STOCK_BASIC_IDENTITY_MISSING", f"{column} contains empty values")

    codes = frame[TS_CODE].astype("string").str.upper()
    frame[TS_CODE] = codes

    frame[EXCHANGE] = frame[EXCHANGE].astype("string").str.upper()
    frame[LIST_STATUS] = frame[LIST_STATUS].astype("string").str.upper()
    expected_suffix = frame[EXCHANGE].map(STOCK_BASIC_EXCHANGE_SUFFIX)
    if expected_suffix.isna().any():
        raise ContractError(
            "STOCK_BASIC_IDENTITY_CONFLICT",
            "exchange cannot be mapped to a supported exchange",
        )

    # The standard regex is applied only to standard identities. A provider
    # historical identity may use a different code shape, but an explicit
    # exchange suffix must still agree when one is present.
    standard_codes = codes.str.fullmatch(STOCK_BASIC_CODE_PATTERN.pattern, na=False)
    has_suffix = codes.str.contains(".", regex=False, na=False)
    suffix_mismatch = has_suffix & codes.str[-3:].ne(expected_suffix)
    if suffix_mismatch.any():
        raise ContractError("STOCK_BASIC_IDENTITY_CONFLICT", "ts_code suffix does not match exchange")

    unsupported_special = (~standard_codes) & frame[LIST_STATUS].ne("D")
    if unsupported_special.any():
        raise ContractError(
            "STOCK_BASIC_IDENTITY_UNSUPPORTED",
            "non-standard provider identity is not marked as delisted historical data",
        )
    frame[IDENTITY_TYPE] = pd.Series(
        STOCK_BASIC_HISTORICAL_IDENTITY,
        index=frame.index,
        dtype="string",
    ).mask(standard_codes, STOCK_BASIC_STANDARD_IDENTITY)

    frame = _normalise_provider_dates(frame)
    duplicate_rows = frame[frame.duplicated(TS_CODE, keep=False)]
    for code, group in duplicate_rows.groupby(TS_CODE, sort=True):
        signatures = group.loc[:, STOCK_BASIC_REQUIRED_FIELDS].astype("string").fillna("").drop_duplicates()
        if len(signatures) > 1:
            raise ContractError("STOCK_BASIC_CONFLICTING_DUPLICATE", str(code))
    return frame.drop_duplicates(subset=[TS_CODE], keep="last").reset_index(drop=True)


def _prepare_frame(frame: pd.DataFrame, captured_at: pd.Timestamp) -> pd.DataFrame:
    prepared = frame.rename(columns=TUSHARE_STOCK_BASIC).copy()
    prepared[TICKER] = prepared[TS_CODE]
    prepared[IS_ACTIVE] = prepared[LIST_STATUS].eq("L")
    prepared[UPDATED_AT] = captured_at
    prepared = align_to_schema(prepared, STOCK_INFO.name, fill_missing_optional=True, drop_extra=True)
    return quick_validate(prepared, STOCK_INFO.name, allow_extra=False)


def _prepare_historical_identity_frame(
    frame: pd.DataFrame,
    target_date: date,
    captured_at: pd.Timestamp,
) -> pd.DataFrame:
    prepared = frame.rename(columns=TUSHARE_STOCK_BASIC).copy()
    prepared[PROVIDER] = STOCK_BASIC_PROVIDER
    prepared[STANDARD_TICKER] = None
    prepared[ISOLATION_REASON] = STOCK_BASIC_HISTORICAL_REASON
    prepared[SNAPSHOT_DATE] = target_date
    prepared[CAPTURED_AT] = captured_at
    prepared = align_to_schema(
        prepared,
        STOCK_INFO_HISTORICAL_IDENTITY.name,
        fill_missing_optional=True,
        drop_extra=True,
    )
    return quick_validate(
        prepared,
        STOCK_INFO_HISTORICAL_IDENTITY.name,
        allow_extra=False,
    )


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return table_name in {row[0] for row in connection.execute("SHOW TABLES").fetchall()}


def _ensure_compatible_schema(connection: duckdb.DuckDBPyConnection) -> None:
    if not _table_exists(connection, STOCK_INFO.name):
        raise ContractError("STOCK_INFO_SCHEMA_MISSING", "stock_info table is not present in the quant database")

    existing_columns = {
        row[0] for row in connection.execute(f"DESCRIBE {STOCK_INFO.name}").fetchall()
    }
    if not set(STOCK_INFO.primary_key).issubset(existing_columns):
        raise ContractError("STOCK_INFO_SCHEMA_INVALID", "stock_info compatibility key is missing")

    for column in STOCK_INFO.columns:
        if column.name in existing_columns:
            continue
        connection.execute(
            f"ALTER TABLE {STOCK_INFO.name} ADD COLUMN {column.name} {column.dtype}"
        )

    if not _table_exists(connection, STOCK_INFO_HISTORICAL_IDENTITY.name):
        connection.execute(STOCK_INFO_HISTORICAL_IDENTITY.duckdb_create_sql())
        return
    historical_columns = {
        row[0]
        for row in connection.execute(
            f"DESCRIBE {STOCK_INFO_HISTORICAL_IDENTITY.name}"
        ).fetchall()
    }
    if not set(STOCK_INFO_HISTORICAL_IDENTITY.column_names()).issubset(historical_columns):
        raise ContractError(
            "STOCK_INFO_HISTORICAL_SCHEMA_INVALID",
            "stock_info_historical_identity columns are incomplete",
        )


def _replace_frames(
    context: PipelineRunContext,
    standard_frame: pd.DataFrame,
    historical_frame: pd.DataFrame,
) -> tuple[int, int, float]:
    started = time.monotonic()
    connection = duckdb.connect(str(context.settings.paths.duckdb_path))
    standard_columns = list(STOCK_INFO.column_names())
    historical_columns = list(STOCK_INFO_HISTORICAL_IDENTITY.column_names())
    try:
        context.execution_control.check()
        connection.execute("BEGIN TRANSACTION")
        _ensure_compatible_schema(connection)
        context.execution_control.check()
        connection.register("_stock_info_rows", standard_frame)
        connection.register("_stock_info_historical_rows", historical_frame)
        try:
            connection.execute(f"DELETE FROM {STOCK_INFO.name}")
            connection.execute(
                f"DELETE FROM {STOCK_INFO_HISTORICAL_IDENTITY.name}"
            )
            connection.execute(
                f"INSERT INTO {STOCK_INFO.name} ({', '.join(standard_columns)}) "
                f"SELECT {', '.join(standard_columns)} FROM _stock_info_rows"
            )
            connection.execute(
                f"INSERT INTO {STOCK_INFO_HISTORICAL_IDENTITY.name} ({', '.join(historical_columns)}) "
                f"SELECT {', '.join(historical_columns)} FROM _stock_info_historical_rows"
            )
        finally:
            connection.unregister("_stock_info_rows")
            connection.unregister("_stock_info_historical_rows")
        context.execution_control.check()
        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()
    return len(standard_frame), len(historical_frame), time.monotonic() - started


def _stock_info_completion(context: PipelineRunContext) -> CheckResult:
    try:
        context.execution_control.check()
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            rows = int(connection.execute(f"SELECT COUNT(*) FROM {STOCK_INFO.name}").fetchone()[0])
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "stock_info_completion",
            "STOCK_INFO_COMPLETION_MISSING",
            "stock_info could not be read after the write",
            exception=type(exc).__name__,
        )
    if rows <= 0:
        return CheckResult.failure(
            "stock_info_completion",
            "STOCK_INFO_COMPLETION_MISSING",
            "stock_info contains no stock definitions",
        )
    return CheckResult.success("stock_info_completion", rows=rows)


def _stock_info_key_quality(context: PipelineRunContext) -> CheckResult:
    try:
        context.execution_control.check()
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            duplicates = connection.execute(
                f"""
                SELECT {TS_CODE}, COUNT(*)
                FROM {STOCK_INFO.name}
                GROUP BY {TS_CODE}
                HAVING COUNT(*) > 1
                ORDER BY {TS_CODE}
                """
            ).fetchall()
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "stock_info_unique_key_quality",
            "STOCK_INFO_DUPLICATE_KEY",
            "stock_info uniqueness could not be checked",
            exception=type(exc).__name__,
        )
    if duplicates:
        return CheckResult.failure(
            "stock_info_unique_key_quality",
            "STOCK_INFO_DUPLICATE_KEY",
            "stock_info contains duplicate ts_code values",
            duplicate_keys=[row[0] for row in duplicates],
        )
    return CheckResult.success("stock_info_unique_key_quality", duplicate_keys=0)


def _stock_info_compatibility_quality(context: PipelineRunContext) -> CheckResult:
    try:
        context.execution_control.check()
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            invalid = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {STOCK_INFO.name}
                    WHERE {TS_CODE} IS NULL
                       OR {TICKER} IS NULL
                       OR {TICKER} <> {TS_CODE}
                       OR {LIST_STATUS} IS NULL
                       OR {IS_ACTIVE} IS NULL
                       OR {IS_ACTIVE} <> ({LIST_STATUS} = 'L')
                    """
                ).fetchone()[0]
            )
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "stock_info_compatibility_quality",
            "STOCK_INFO_COMPATIBILITY_INVALID",
            "stock_info compatibility fields could not be checked",
            exception=type(exc).__name__,
        )
    if invalid:
        return CheckResult.failure(
            "stock_info_compatibility_quality",
            "STOCK_INFO_COMPATIBILITY_INVALID",
            "stock_info contains invalid compatibility fields",
            invalid_rows=invalid,
        )
    return CheckResult.success("stock_info_compatibility_quality", invalid_rows=0)


def _historical_identity_completion(context: PipelineRunContext) -> CheckResult:
    try:
        context.execution_control.check()
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            rows = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {STOCK_INFO_HISTORICAL_IDENTITY.name}"
                ).fetchone()[0]
            )
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "historical_identity_completion",
            "STOCK_INFO_HISTORICAL_COMPLETION_MISSING",
            "stock_info_historical_identity could not be read after the write",
            exception=type(exc).__name__,
        )
    return CheckResult.success("historical_identity_completion", rows=rows)


def _historical_identity_quality(context: PipelineRunContext) -> CheckResult:
    try:
        context.execution_control.check()
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            invalid = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {STOCK_INFO_HISTORICAL_IDENTITY.name}
                    WHERE {PROVIDER} IS NULL
                       OR {TS_CODE} IS NULL
                       OR {EXCHANGE} IS NULL
                       OR {LIST_STATUS} IS NULL
                       OR {IDENTITY_TYPE} <> '{STOCK_BASIC_HISTORICAL_IDENTITY}'
                       OR {STANDARD_TICKER} IS NOT NULL
                       OR {ISOLATION_REASON} IS NULL
                       OR {SNAPSHOT_DATE} IS NULL
                       OR {CAPTURED_AT} IS NULL
                    """
                ).fetchone()[0]
            )
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "historical_identity_quality",
            "STOCK_INFO_HISTORICAL_QUALITY_FAILED",
            "stock_info_historical_identity quality could not be checked",
            exception=type(exc).__name__,
        )
    if invalid:
        return CheckResult.failure(
            "historical_identity_quality",
            "STOCK_INFO_HISTORICAL_QUALITY_INVALID",
            "stock_info_historical_identity contains invalid identity rows",
            invalid_rows=invalid,
        )
    return CheckResult.success("historical_identity_quality", invalid_rows=0)


def execute_stock_basic_update(context: PipelineRunContext) -> BusinessExecution:
    started = time.monotonic()
    frames: list[pd.DataFrame] = []
    rows_read = 0
    try:
        client = get_tushare_pro(settings=context.settings, execution_control=context.execution_control)
        for list_status, exchange in STOCK_BASIC_QUERY_PARTITIONS:
            context.execution_control.check()
            raw = client.stock_basic(
                exchange=exchange,
                list_status=list_status,
                fields=STOCK_BASIC_FIELDS,
            )
            context.execution_control.check()
            if raw is None:
                continue
            if not isinstance(raw, pd.DataFrame):
                raise ContractError(
                    "STOCK_BASIC_API_FAILED",
                    f"{list_status}/{exchange} returned {type(raw).__name__}",
                )
            if raw.empty:
                continue
            _required_columns(raw)
            frames.append(raw.copy())
            rows_read += len(raw)
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("STOCK_BASIC_API_FAILED", type(exc).__name__) from exc

    fetched_at = time.monotonic()
    target_date = context.target_window.target_date
    if target_date is None:
        raise ContractError("STOCK_BASIC_TARGET_DATE_MISSING", "stock_basic requires a snapshot date")
    normalized = _normalize_provider_frames(frames)
    historical_mask = normalized[IDENTITY_TYPE].eq(STOCK_BASIC_HISTORICAL_IDENTITY)
    standard_source = normalized.loc[~historical_mask].copy()
    historical_source = normalized.loc[historical_mask].copy()
    captured_at = pd.Timestamp.now(tz=CHINA_TZ).tz_localize(None)
    prepared = _prepare_frame(standard_source, captured_at)
    prepared_historical = _prepare_historical_identity_frame(
        historical_source,
        target_date,
        captured_at,
    )
    normalized_at = time.monotonic()
    try:
        standard_rows_written, historical_rows_written, database_seconds = _replace_frames(
            context,
            prepared,
            prepared_historical,
        )
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("STOCK_BASIC_WRITE_FAILED", type(exc).__name__) from exc

    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=rows_read,
            rows_written=standard_rows_written + historical_rows_written,
            assets_processed=standard_rows_written + historical_rows_written,
            dates_processed=1,
            database_write_seconds=database_seconds,
            stage_durations_seconds={
                "fetch": fetched_at - started,
                "normalize": normalized_at - fetched_at,
                "database_write": database_seconds,
            },
            api_requests=len(STOCK_BASIC_QUERY_PARTITIONS),
            batches=len(STOCK_BASIC_QUERY_PARTITIONS),
        ),
        outputs=(
            OutputResult(
                output_id=STOCK_INFO.name,
                rows_written=standard_rows_written,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={
                    "list_statuses": list(STOCK_BASIC_LIST_STATUSES),
                    "exchanges": list(STOCK_BASIC_EXCHANGES),
                    "provider_fields": list(STOCK_BASIC_REQUIRED_FIELDS),
                    "standard_rows": standard_rows_written,
                    "historical_identity_rows": historical_rows_written,
                    "snapshot_semantics": "current-state full replacement",
                },
            ),
            OutputResult(
                output_id=STOCK_INFO_HISTORICAL_IDENTITY.name,
                rows_written=historical_rows_written,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={
                    "provider": STOCK_BASIC_PROVIDER,
                    "identity_type": STOCK_BASIC_HISTORICAL_IDENTITY,
                    "isolation_reason": STOCK_BASIC_HISTORICAL_REASON,
                    "standard_rows": standard_rows_written,
                    "historical_identity_rows": historical_rows_written,
                    "snapshot_date": target_date.isoformat(),
                    "provider_identity_samples": [
                        str(value)
                        for value in historical_source[TS_CODE].head(10).tolist()
                    ],
                },
            ),
        ),
    )


STOCK_BASIC_UPDATE = register_pipeline(
    PipelineContract(
        pipeline_id="stock_basic_update",
        name="Tushare stock basic current snapshot",
        description=(
            "Synchronizes the complete current Tushare stock_basic fields into stock_info "
            "and replaces the local current-state snapshot without retaining history; "
            "non-standard provider identities marked D are retained in the historical "
            "identity companion table instead of being forced into the standard code contract."
        ),
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_stock_basic_update,
        target_date_policy=STOCK_BASIC_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            InputContract(
                input_id="tushare_stock_basic",
                kind=InputKind.EXTERNAL_API,
                source=(
                    "tushare.pro.stock_basic(list_status=..., exchange=..., "
                    "fields=ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,"
                    "curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type)"
                ),
                required_fields=STOCK_BASIC_REQUIRED_FIELDS,
                target_date_semantics="complete current provider snapshot observed during the execution",
                missing_error_code="TUSHARE_CONFIGURATION_MISSING",
                structure_check=_tushare_configuration,
                freshness=FreshnessContract(
                    check_id="stock_basic_snapshot_freshness",
                    target_date_semantics="snapshot is fetched during the current execution",
                    maximum_lag_trading_days=0,
                    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                    error_code="STOCK_BASIC_SNAPSHOT_STALE",
                    checker=_snapshot_freshness,
                ),
            ),
        ),
        outputs=(
            OutputContract(
                output_id=STOCK_INFO.name,
                physical_resource=QUANT_DB_RESOURCE,
                location="settings.paths.duckdb_path",
                object_name=STOCK_INFO.name,
                unique_key=STOCK_INFO.primary_key,
                write_mode=WriteMode.FULL_REBUILD,
                target_date_semantics="current stock_info snapshot committed during this execution",
                completion=CompletionContract(
                    marker="stock_info contains the complete validated current stock_basic snapshot",
                    error_code="STOCK_INFO_COMPLETION_MISSING",
                    checker=_stock_info_completion,
                ),
                quality_checks=(_stock_info_key_quality, _stock_info_compatibility_quality),
                allow_empty=False,
            ),
            OutputContract(
                output_id=STOCK_INFO_HISTORICAL_IDENTITY.name,
                physical_resource=QUANT_DB_RESOURCE,
                location="settings.paths.duckdb_path",
                object_name=STOCK_INFO_HISTORICAL_IDENTITY.name,
                unique_key=STOCK_INFO_HISTORICAL_IDENTITY.primary_key,
                write_mode=WriteMode.FULL_REBUILD,
                target_date_semantics="current provider historical identities observed during this execution",
                completion=CompletionContract(
                    marker="stock_info_historical_identity is queryable after the current replacement",
                    error_code="STOCK_INFO_HISTORICAL_COMPLETION_MISSING",
                    checker=_historical_identity_completion,
                ),
                quality_checks=(_historical_identity_quality,),
                allow_empty=True,
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=IdempotencyContract(
            idempotency_key="stock_info.ticker; stock_info_historical_identity.provider+ts_code",
            repeat_run_semantics="repeated provider snapshots replace both current-state tables without retaining history",
            existing_target_handling="the existing standard and historical-identity rows are replaced only after the complete validated provider snapshot is available",
            failure_recovery="a failed fetch, classification, or write leaves the previous committed standard and historical-identity snapshots available; rerun after correcting the provider or schema failure",
            uses_staging=False,
            atomic_replace_boundary="one transaction deletes and replaces stock_info and stock_info_historical_identity",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="validated complete stock_basic snapshot, including standard and historical identity classification, for all configured statuses and exchanges",
            failure_visibility="a failed transaction rolls back both replacements and preserves the previous standard and historical-identity snapshots",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=PerformanceBudget(
            normal_budget_seconds=180.0,
            warning_threshold_seconds=120.0,
            hard_timeout_seconds=600,
            benchmark_scope="end-to-end: twelve partitioned stock_basic requests and one atomic replacement of the standard and historical-identity snapshots",
            baseline_source="internal:stock_basic_update_v1; offline temporary-DuckDB acceptance tests",
        ),
        manual_execution_allowed=True,
    )
)


STOCK_BASIC_CONTRACTS: tuple[PipelineContract, ...] = (STOCK_BASIC_UPDATE,)
