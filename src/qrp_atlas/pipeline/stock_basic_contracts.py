"""Formal current-state Pipeline for the Tushare ``stock_basic`` endpoint.

The output is a complete provider snapshot in ``stock_info``.  It deliberately
does not retain historical snapshots: each successful run replaces the current
table in one DuckDB transaction.  ``ticker``, ``is_active`` and ``updated_at``
remain compatibility fields for existing consumers.
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
    DELIST_DATE,
    EXCHANGE,
    IS_ACTIVE,
    LIST_DATE,
    LIST_STATUS,
    MARKET,
    NAME,
    STOCK_INFO,
    SYMBOL,
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
STOCK_BASIC_CORE_FIELDS = (TS_CODE, SYMBOL, NAME, EXCHANGE, MARKET, LIST_STATUS)


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

    for column in STOCK_BASIC_CORE_FIELDS:
        if frame[column].isna().any():
            raise ContractError("STOCK_BASIC_API_PARTIAL", f"{column} contains empty values")

    codes = frame[TS_CODE].astype("string").str.upper()
    invalid_codes = codes.isna() | ~codes.str.fullmatch(STOCK_BASIC_CODE_PATTERN.pattern, na=False)
    if invalid_codes.any():
        raise ContractError("STOCK_BASIC_API_PARTIAL", "invalid ts_code in stock_basic response")
    frame[TS_CODE] = codes

    expected_suffix = frame[EXCHANGE].map(STOCK_BASIC_EXCHANGE_SUFFIX)
    suffix_mismatch = expected_suffix.notna() & codes.str[-3:].ne(expected_suffix)
    if suffix_mismatch.any():
        raise ContractError("STOCK_BASIC_API_PARTIAL", "ts_code suffix does not match exchange")

    frame = _normalise_provider_dates(frame)
    duplicate_rows = frame[frame.duplicated(TS_CODE, keep=False)]
    for code, group in duplicate_rows.groupby(TS_CODE, sort=True):
        signatures = group.loc[:, STOCK_BASIC_REQUIRED_FIELDS].astype("string").fillna("").drop_duplicates()
        if len(signatures) > 1:
            raise ContractError("STOCK_BASIC_CONFLICTING_DUPLICATE", str(code))
    return frame.drop_duplicates(subset=[TS_CODE], keep="last").reset_index(drop=True)


def _prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.rename(columns=TUSHARE_STOCK_BASIC).copy()
    prepared[TICKER] = prepared[TS_CODE]
    prepared[IS_ACTIVE] = prepared[LIST_STATUS].eq("L")
    prepared[UPDATED_AT] = pd.Timestamp.now(tz=CHINA_TZ).tz_localize(None)
    prepared = align_to_schema(prepared, STOCK_INFO.name, fill_missing_optional=True, drop_extra=True)
    return quick_validate(prepared, STOCK_INFO.name, allow_extra=False)


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


def _replace_frame(context: PipelineRunContext, frame: pd.DataFrame) -> tuple[int, float]:
    started = time.monotonic()
    connection = duckdb.connect(str(context.settings.paths.duckdb_path))
    columns = list(STOCK_INFO.column_names())
    try:
        context.execution_control.check()
        connection.execute("BEGIN TRANSACTION")
        _ensure_compatible_schema(connection)
        context.execution_control.check()
        connection.register("_stock_info_rows", frame)
        try:
            connection.execute(f"DELETE FROM {STOCK_INFO.name}")
            connection.execute(
                f"INSERT INTO {STOCK_INFO.name} ({', '.join(columns)}) "
                f"SELECT {', '.join(columns)} FROM _stock_info_rows"
            )
        finally:
            connection.unregister("_stock_info_rows")
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
    return len(frame), time.monotonic() - started


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
    normalized = _normalize_provider_frames(frames)
    prepared = _prepare_frame(normalized)
    normalized_at = time.monotonic()
    try:
        rows_written, database_seconds = _replace_frame(context, prepared)
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("STOCK_BASIC_WRITE_FAILED", type(exc).__name__) from exc

    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=rows_read,
            rows_written=rows_written,
            assets_processed=rows_written,
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
                rows_written=rows_written,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={
                    "list_statuses": list(STOCK_BASIC_LIST_STATUSES),
                    "exchanges": list(STOCK_BASIC_EXCHANGES),
                    "provider_fields": list(STOCK_BASIC_REQUIRED_FIELDS),
                    "snapshot_semantics": "current-state full replacement",
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
            "and replaces the local current-state snapshot without retaining history."
        ),
        contract_version="1.0.0",
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
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=IdempotencyContract(
            idempotency_key="stock_info.ticker",
            repeat_run_semantics="repeated provider snapshots replace the same current-state table without retaining history",
            existing_target_handling="the existing stock_info rows are replaced only after the complete validated provider snapshot is available",
            failure_recovery="a failed fetch or write leaves the previous committed stock_info snapshot available; rerun after correcting the provider or schema failure",
            uses_staging=False,
            atomic_replace_boundary="one transaction deletes and replaces all current stock_info rows",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="validated complete stock_basic snapshot for all configured statuses and exchanges",
            failure_visibility="a failed transaction rolls back the full replacement and preserves the previous snapshot",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=PerformanceBudget(
            normal_budget_seconds=180.0,
            warning_threshold_seconds=120.0,
            hard_timeout_seconds=600,
            benchmark_scope="end-to-end: twelve partitioned stock_basic requests and one full stock_info replacement",
            baseline_source="internal:stock_basic_update_v1; offline temporary-DuckDB acceptance tests",
        ),
        manual_execution_allowed=True,
    )
)


STOCK_BASIC_CONTRACTS: tuple[PipelineContract, ...] = (STOCK_BASIC_UPDATE,)
