"""Formal Pipeline contracts for the three CNINFO research-visit jobs.

The three historical job identities retain their different target-date
semantics while sharing one provider, validation, and transactional writer.
Deployment schedules remain outside this source module.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    CNINFO_RESEARCH_VISITS,
    CREATED_AT,
    SECU_CODE,
    TRADING_CALENDAR,
    align_to_schema,
    quick_validate,
)
from qrp_atlas.orchestration.execution_control import ExecutionControlError
from qrp_atlas.orchestration.models import OverlapPolicy

from .cninfo.clean import clean_eastmoney
from .cninfo.config import (
    EASTMONEY_CLIENT,
    EASTMONEY_PAGE_SIZE,
    EASTMONEY_REPORT,
    EASTMONEY_SOURCE,
    EASTMONEY_URL,
)
from .cninfo.fetch import EastmoneyFetchReport, fetch_from_eastmoney_report
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
    PipelineInvocation,
    PipelineRunContext,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .registry import register_pipeline


CNINFO_TIMEZONE = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
CNINFO_TABLE = CNINFO_RESEARCH_VISITS.name
CNINFO_RESOURCE_READ = "duckdb://quant_db#trading_calendar"

# These are the fields requested from and consumed from each Eastmoney record.
# URL is requested by the legacy client but is optional in the table contract;
# the cleaner may omit it when the provider does not return an attachment URL.
CNINFO_REQUIRED_PROVIDER_FIELDS: tuple[str, ...] = (
    "SECUCODE",
    "SECURITY_NAME_ABBR",
    "NOTICE_DATE",
    "RECEIVE_START_DATE",
    "RECEIVE_WAY_EXPLAIN",
    "RECEIVE_PLACE",
    "RECEPTIONIST",
    "CONTENT",
)
CNINFO_OPTIONAL_PROVIDER_FIELDS: tuple[str, ...] = ("URL",)
CNINFO_PROVIDER_FIELDS = CNINFO_REQUIRED_PROVIDER_FIELDS + CNINFO_OPTIONAL_PROVIDER_FIELDS


def _scheduled_local_date(invocation: PipelineInvocation) -> date:
    scheduled_for = invocation.scheduled_for
    if scheduled_for.tzinfo is None:
        raise ContractError("CNINFO_SCHEDULE_TIMEZONE_MISSING")
    return scheduled_for.astimezone(CNINFO_TIMEZONE).date()


def _calendar_connection(settings: Any, control: Any) -> duckdb.DuckDBPyConnection:
    control.check()
    return duckdb.connect(str(settings.paths.duckdb_path), read_only=True)


def _open_calendar_dates(
    settings: Any,
    control: Any,
    start_date: date,
    end_date: date,
) -> tuple[date, ...]:
    connection = _calendar_connection(settings, control)
    try:
        control.check()
        rows = connection.execute(
            """
            SELECT trade_date
            FROM trading_calendar
            WHERE is_open IS TRUE
              AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date
            """,
            [start_date, end_date],
        ).fetchall()
        control.check()
        return tuple(
            value.date() if isinstance(value, datetime) else value
            for (value,) in rows
            if isinstance(value, date)
        )
    finally:
        connection.close()


def _previous_open_calendar_date(settings: Any, control: Any, current_date: date) -> date:
    connection = _calendar_connection(settings, control)
    try:
        control.check()
        row = connection.execute(
            """
            SELECT trade_date
            FROM trading_calendar
            WHERE is_open IS TRUE AND trade_date < ?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            [current_date],
        ).fetchone()
        control.check()
    finally:
        connection.close()
    if row is None:
        raise ContractError("CNINFO_CALENDAR_STALE", "previous open trading date is unavailable")
    value = row[0]
    return value.date() if isinstance(value, datetime) else value


def _main_target_resolver(invocation: PipelineInvocation) -> TargetWindow:
    current_date = _scheduled_local_date(invocation)
    previous_date = _previous_open_calendar_date(invocation.settings, invocation.execution_control, current_date)
    # The legacy 08:00 wrapper explicitly fetched previous trading date and
    # today. The executor later keeps only open dates present in the calendar.
    return TargetWindow(start_date=previous_date, end_date=current_date)


def _incremental_target_resolver(invocation: PipelineInvocation) -> TargetWindow:
    return TargetWindow.for_date(_scheduled_local_date(invocation))


def _main_explicit_date_validator(target_date: date, invocation: PipelineInvocation) -> bool:
    try:
        return target_date in _open_calendar_dates(
            invocation.settings,
            invocation.execution_control,
            target_date,
            target_date,
        )
    except ExecutionControlError:
        raise
    except Exception:
        return False


def _incremental_explicit_date_validator(target_date: date, invocation: PipelineInvocation) -> bool:
    invocation.execution_control.check()
    return isinstance(target_date, date)


def _calendar_structure(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    connection = _calendar_connection(context.settings, context.execution_control)
    try:
        columns = {
            row[0]
            for row in connection.execute(f"DESCRIBE {TRADING_CALENDAR.name}").fetchall()
        }
        required = set(TRADING_CALENDAR.column_names())
        missing = sorted(required - columns)
        if missing:
            return CheckResult.failure(
                "cninfo_calendar_structure",
                "CNINFO_CALENDAR_STRUCTURE_MISSING",
                "trading_calendar is missing required columns",
                missing=missing,
            )
        context.execution_control.check()
        return CheckResult.success("cninfo_calendar_structure", table=TRADING_CALENDAR.name)
    except ExecutionControlError:
        raise
    except Exception as exc:
        return CheckResult.failure(
            "cninfo_calendar_structure",
            "CNINFO_CALENDAR_UNAVAILABLE",
            "trading_calendar is unavailable",
            exception=type(exc).__name__,
        )
    finally:
        connection.close()


def _main_calendar_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        dates = _target_dates(context)
    except ExecutionControlError:
        raise
    except ContractError as exc:
        return CheckResult.failure(
            "cninfo_calendar_freshness",
            "CNINFO_CALENDAR_STALE",
            "calendar cannot resolve a CNINFO target date",
            reason=exc.code,
        )
    except Exception as exc:
        return CheckResult.failure(
            "cninfo_calendar_freshness",
            "CNINFO_CALENDAR_STALE",
            "calendar cannot resolve a CNINFO target date",
            exception=type(exc).__name__,
        )
    if not dates:
        return CheckResult.failure(
            "cninfo_calendar_freshness",
            "CNINFO_CALENDAR_STALE",
            "calendar has no open target date",
        )
    return CheckResult.success(
        "cninfo_calendar_freshness",
        target_dates=[item.isoformat() for item in dates],
    )


def _provider_configuration(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    if not EASTMONEY_URL or not EASTMONEY_REPORT or EASTMONEY_PAGE_SIZE <= 0:
        return CheckResult.failure(
            "cninfo_provider_configuration",
            "CNINFO_PROVIDER_CONFIGURATION_MISSING",
            "Eastmoney provider configuration is incomplete",
        )
    return CheckResult.success(
        "cninfo_provider_configuration",
        provider="eastmoney",
        report=EASTMONEY_REPORT,
        page_size=EASTMONEY_PAGE_SIZE,
        source=EASTMONEY_SOURCE,
        client=EASTMONEY_CLIENT,
    )


def _provider_freshness(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    dates = _target_dates(context)
    return CheckResult.success(
        "cninfo_provider_freshness",
        provider="eastmoney",
        target_dates=[item.isoformat() for item in dates],
        response_date_rule="each response NOTICE_DATE must equal its requested date",
    )


def _target_dates(context: PipelineRunContext) -> tuple[date, ...]:
    context.execution_control.check()
    window = context.target_window
    if window.target_date is not None:
        return (window.target_date,)
    if window.start_date is None or window.end_date is None:
        raise ContractError("CNINFO_TARGET_DATE_REQUIRED")
    try:
        dates = _open_calendar_dates(
            context.settings,
            context.execution_control,
            window.start_date,
            window.end_date,
        )
    except ExecutionControlError:
        raise
    except Exception as exc:
        raise ContractError("CNINFO_CALENDAR_UNAVAILABLE", type(exc).__name__) from exc
    if not dates:
        raise ContractError("CNINFO_CALENDAR_STALE", "no open target dates")
    return dates


def _parse_provider_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    for candidate in (text[:10], text):
        try:
            return date.fromisoformat(candidate)
        except ValueError:
            pass
    for fmt in ("%Y/%m/%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def _validate_provider_records(records: Sequence[Mapping[str, Any]], target_date: date) -> None:
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ContractError("CNINFO_PROVIDER_SCHEMA_MISSING", f"record {index} is not an object")
        missing = [field for field in CNINFO_REQUIRED_PROVIDER_FIELDS if field not in record]
        if missing:
            raise ContractError(
                "CNINFO_PROVIDER_SCHEMA_MISSING",
                f"record {index} is missing fields: {','.join(missing)}",
            )
        if not str(record.get("SECUCODE") or "").strip():
            raise ContractError("CNINFO_PROVIDER_SCHEMA_MISSING", f"record {index} has empty SECUCODE")
        notice_date = _parse_provider_date(record.get("NOTICE_DATE"))
        receive_date = _parse_provider_date(record.get("RECEIVE_START_DATE"))
        if notice_date is None or receive_date is None:
            raise ContractError(
                "CNINFO_PROVIDER_SCHEMA_MISSING",
                f"record {index} has an invalid notice or receive date",
            )
        if notice_date != target_date:
            raise ContractError(
                "CNINFO_PROVIDER_WRONG_DATE",
                f"record {index} notice date is {notice_date.isoformat()}, expected {target_date.isoformat()}",
            )


def _fetch_and_clean(
    target_date: date,
    context: PipelineRunContext,
) -> tuple[list[dict[str, Any]], int, EastmoneyFetchReport]:
    context.execution_control.check()
    try:
        report = fetch_from_eastmoney_report(
            target_date.isoformat(),
            execution_control=context.execution_control,
        )
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("CNINFO_PROVIDER_ERROR", type(exc).__name__) from exc
    context.execution_control.check()
    if report.date_str != target_date.isoformat():
        raise ContractError("CNINFO_PROVIDER_WRONG_DATE", "provider report date does not match request")
    if not report.complete:
        code = "CNINFO_PROVIDER_PARTIAL" if report.pages_fetched else "CNINFO_PROVIDER_ERROR"
        detail = f"failed pages: {','.join(str(item) for item in report.failed_pages)}"
        raise ContractError(code, detail)
    raw_records = list(report.records)
    _validate_provider_records(raw_records, target_date)
    context.execution_control.check()
    try:
        cleaned = clean_eastmoney(raw_records)
    except ExecutionControlError:
        raise
    except Exception as exc:
        raise ContractError("CNINFO_CLEAN_FAILED", type(exc).__name__) from exc
    context.execution_control.check()
    if not isinstance(cleaned, list):
        raise ContractError("CNINFO_CLEAN_FAILED", "cleaner must return a list")
    return cleaned, len(raw_records), report


def _prepare_cninfo_rows(records: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=[column.name for column in CNINFO_RESEARCH_VISITS.columns])
    try:
        frame = pd.DataFrame(records)
        frame = align_to_schema(frame, CNINFO_TABLE, fill_missing_optional=True, drop_extra=True)
        frame = quick_validate(frame, CNINFO_TABLE, allow_extra=False)
    except Exception as exc:
        raise ContractError("CNINFO_CLEAN_SCHEMA_INVALID", type(exc).__name__) from exc
    for field in CNINFO_RESEARCH_VISITS.primary_key:
        if field not in frame or frame[field].isna().any():
            raise ContractError("CNINFO_CLEAN_SCHEMA_INVALID", f"primary key field {field} is null")
        if field == SECU_CODE and frame[field].astype(str).str.strip().eq("").any():
            raise ContractError("CNINFO_CLEAN_SCHEMA_INVALID", "secu_code is empty")
    return frame.drop_duplicates(subset=list(CNINFO_RESEARCH_VISITS.primary_key), keep="first").reset_index(drop=True)


def _insert_cninfo_rows(connection: duckdb.DuckDBPyConnection, rows: pd.DataFrame) -> int:
    if rows.empty:
        return 0
    view_name = "cninfo_contract_rows"
    connection.register(view_name, rows)
    try:
        primary_key = CNINFO_RESEARCH_VISITS.primary_key
        join = " AND ".join(f"target.{field} = incoming.{field}" for field in primary_key)
        existing = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM {view_name} AS incoming
                JOIN {CNINFO_TABLE} AS target ON {join}
                """
            ).fetchone()[0]
        )
        insert_columns = [
            column.name for column in CNINFO_RESEARCH_VISITS.columns if column.name != CREATED_AT
        ]
        columns_sql = ", ".join(insert_columns)
        connection.execute(
            f"""
            INSERT OR IGNORE INTO {CNINFO_TABLE} ({columns_sql})
            SELECT {columns_sql}
            FROM {view_name}
            """
        )
        return max(0, len(rows) - existing)
    finally:
        connection.unregister(view_name)


def _append_cninfo_rows(
    context: PipelineRunContext,
    rows: pd.DataFrame,
) -> tuple[int, float]:
    started = monotonic()
    connection: duckdb.DuckDBPyConnection | None = None
    transaction_open = False
    try:
        context.execution_control.check()
        connection = duckdb.connect(str(context.settings.paths.duckdb_path))
        context.execution_control.check()
        connection.execute("BEGIN TRANSACTION")
        transaction_open = True
        context.execution_control.check()
        inserted = _insert_cninfo_rows(connection, rows)
        context.execution_control.check()
        connection.execute("COMMIT")
        transaction_open = False
        context.execution_control.check()
        return inserted, monotonic() - started
    except ExecutionControlError:
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
        raise
    except ContractError:
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
        raise
    except Exception as exc:
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
        raise ContractError("CNINFO_WRITE_FAILED", type(exc).__name__) from exc
    finally:
        if connection is not None:
            connection.close()


def _execute_cninfo(context: PipelineRunContext) -> BusinessExecution:
    started = monotonic()
    target_dates = _target_dates(context)
    cleaned_records: list[dict[str, Any]] = []
    raw_rows = 0
    api_requests = 0
    batches = 0
    retries = 0
    for target_date in target_dates:
        context.execution_control.check()
        cleaned, raw_count, report = _fetch_and_clean(target_date, context)
        raw_rows += raw_count
        api_requests += report.requests
        batches += report.pages_fetched
        retries += report.retries
        cleaned_records.extend(cleaned)
        context.execution_control.check()

    # No cancellation/deadline can enter the database transaction after the
    # final provider response without first passing this check.
    context.execution_control.check()
    rows = _prepare_cninfo_rows(cleaned_records)
    context.execution_control.check()
    inserted, write_seconds = _append_cninfo_rows(context, rows)
    context.execution_control.check()
    assets = int(rows[SECU_CODE].nunique()) if not rows.empty else 0
    elapsed = monotonic() - started
    output = OutputResult(
        output_id=CNINFO_TABLE,
        rows_written=inserted,
        location="settings.paths.duckdb_path",
        completed=True,
        detail={
            "target_dates": [item.isoformat() for item in target_dates],
            "rows_received": raw_rows,
            "rows_cleaned": len(rows),
            "rows_inserted": inserted,
            "empty_snapshot": raw_rows == 0,
            "api_requests": api_requests,
            "pages_fetched": batches,
            "retries": retries,
        },
    )
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=raw_rows,
            rows_written=inserted,
            assets_processed=assets,
            dates_processed=len(target_dates),
            database_write_seconds=write_seconds,
            stage_durations_seconds={"provider_and_clean": max(0.0, elapsed - write_seconds)},
            api_requests=api_requests,
            batches=batches,
            retries=retries,
        ),
        outputs=(output,),
    )


def _cninfo_completion(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    try:
        target_dates = _target_dates(context)
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            context.execution_control.check()
            total = int(connection.execute(f"SELECT COUNT(*) FROM {CNINFO_TABLE}").fetchone()[0])
            context.execution_control.check()
        finally:
            connection.close()
        return CheckResult.success(
            "cninfo_output_completion",
            table=CNINFO_TABLE,
            target_dates=[item.isoformat() for item in target_dates],
            total_rows=total,
            marker="committed transactional append is queryable",
        )
    except ExecutionControlError:
        raise
    except ContractError as exc:
        return CheckResult.failure(
            "cninfo_output_completion",
            "CNINFO_COMPLETION_MISSING",
            "CNINFO target resolution failed during completion check",
            reason=exc.code,
        )
    except Exception as exc:
        return CheckResult.failure(
            "cninfo_output_completion",
            "CNINFO_COMPLETION_MISSING",
            "CNINFO output is not queryable after commit",
            exception=type(exc).__name__,
        )


def _cninfo_unique_key_quality(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    try:
        target_dates = _target_dates(context)
        placeholders = ", ".join("?" for _ in target_dates)
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            duplicate = connection.execute(
                f"""
                SELECT 1
                FROM {CNINFO_TABLE}
                WHERE notice_date IN ({placeholders})
                GROUP BY secu_code, notice_date, receive_date
                HAVING COUNT(*) > 1
                LIMIT 1
                """,
                list(target_dates),
            ).fetchone()
            context.execution_control.check()
        finally:
            connection.close()
        if duplicate is not None:
            return CheckResult.failure(
                "cninfo_unique_key_quality",
                "CNINFO_DUPLICATE_KEY",
                "CNINFO output contains duplicate primary keys",
            )
        return CheckResult.success(
            "cninfo_unique_key_quality",
            unique_key=list(CNINFO_RESEARCH_VISITS.primary_key),
        )
    except ExecutionControlError:
        raise
    except ContractError as exc:
        return CheckResult.failure(
            "cninfo_unique_key_quality",
            "CNINFO_COMPLETION_MISSING",
            "CNINFO target resolution failed during quality check",
            reason=exc.code,
        )
    except Exception as exc:
        return CheckResult.failure(
            "cninfo_unique_key_quality",
            "CNINFO_DUPLICATE_KEY",
            "CNINFO unique-key quality query failed",
            exception=type(exc).__name__,
        )


def _calendar_input() -> InputContract:
    return InputContract(
        input_id="cninfo_trading_calendar",
        kind=InputKind.TABLE,
        source="quant.db.trading_calendar",
        required_fields=tuple(TRADING_CALENDAR.column_names()),
        target_date_semantics="main update resolves previous open date through scheduled local calendar date",
        missing_error_code="CNINFO_CALENDAR_STRUCTURE_MISSING",
        structure_check=_calendar_structure,
        freshness=FreshnessContract(
            check_id="cninfo_calendar_freshness",
            target_date_semantics="resolved target range must contain at least one open calendar date",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.PREVIOUS_TRADING_DAY,
            error_code="CNINFO_CALENDAR_STALE",
            checker=_main_calendar_freshness,
        ),
    )


def _provider_input() -> InputContract:
    return InputContract(
        input_id="external_cninfo",
        kind=InputKind.EXTERNAL_API,
        source=f"{EASTMONEY_URL}?reportName={EASTMONEY_REPORT}",
        required_fields=CNINFO_REQUIRED_PROVIDER_FIELDS,
        target_date_semantics="provider NOTICE_DATE must equal every requested target date",
        missing_error_code="CNINFO_PROVIDER_CONFIGURATION_MISSING",
        structure_check=_provider_configuration,
        freshness=FreshnessContract(
            check_id="cninfo_provider_freshness",
            target_date_semantics="each complete paginated response is date-bound to its request",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
            error_code="CNINFO_PROVIDER_STALE",
            checker=_provider_freshness,
        ),
    )


def _output() -> OutputContract:
    return OutputContract(
        output_id=CNINFO_TABLE,
        physical_resource=QUANT_DB_RESOURCE,
        location="settings.paths.duckdb_path",
        object_name=CNINFO_TABLE,
        unique_key=CNINFO_RESEARCH_VISITS.primary_key,
        write_mode=WriteMode.APPEND,
        target_date_semantics="notice_date equals each resolved CNINFO target date",
        completion=CompletionContract(
            marker="committed transactional append is queryable in cninfo_research_visits",
            error_code="CNINFO_COMPLETION_MISSING",
            checker=_cninfo_completion,
        ),
        quality_checks=(_cninfo_unique_key_quality,),
        allow_empty=True,
    )


def _performance() -> PerformanceBudget:
    return PerformanceBudget(
        normal_budget_seconds=300.0,
        warning_threshold_seconds=180.0,
        hard_timeout_seconds=600,
        benchmark_scope="one scheduled invocation, Eastmoney pagination and one quant.db transaction",
        baseline_source=(
            "docs/QRP产品蓝图v1.1/09_Pipeline现状事实与迁移边界.md CNINFO rows and 600-second wrapper timeout; "
            "src/qrp_atlas/pipeline/cninfo/config.py request page and retry settings"
        ),
    )


CNINFO_MAIN_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="cninfo_main_previous_through_scheduled_date",
    description="Resolve the legacy main window from the previous open date through the scheduled Shanghai date.",
    trading_calendar_id=TRADING_CALENDAR.name,
    non_trading_day_policy=NonTradingDayPolicy.PREVIOUS_TRADING_DAY,
    resolver=_main_target_resolver,
    validate_explicit_date=_main_explicit_date_validator,
)

CNINFO_INCREMENTAL_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="cninfo_incremental_scheduled_local_date",
    description="Use the scheduled Shanghai local calendar date for each historical incremental run.",
    trading_calendar_id="none",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_incremental_target_resolver,
    validate_explicit_date=_incremental_explicit_date_validator,
)


def _idempotency() -> IdempotencyContract:
    return IdempotencyContract(
        idempotency_key="(secu_code, notice_date, receive_date)",
        repeat_run_semantics="same target date may be fetched repeatedly without duplicate rows",
        existing_target_handling="INSERT OR IGNORE preserves the first committed primary-key row",
        failure_recovery="rollback the complete CNINFO target batch, then retry the invocation",
        uses_staging=False,
        atomic_replace_boundary="one database transaction around the prepared append",
    )


def _contract(
    pipeline_id: str,
    name: str,
    description: str,
    target_date_policy: TargetDatePolicy,
    inputs: tuple[InputContract, ...],
) -> PipelineContract:
    return PipelineContract(
        pipeline_id=pipeline_id,
        name=name,
        description=description,
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_cninfo,
        target_date_policy=target_date_policy,
        parameters=(),
        inputs=inputs,
        outputs=(_output(),),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=(CNINFO_RESOURCE_READ,) if target_date_policy is CNINFO_MAIN_TARGET_DATE_POLICY else (),
        idempotency=_idempotency(),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="all resolved target dates are prepared before one cninfo_research_visits append transaction",
            failure_visibility="any provider, validation, or database error returns failure and leaves no new rows",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=_performance(),
        manual_execution_allowed=True,
    )


CNINFO_MAIN_UPDATE = _contract(
    "cninfo_main_update",
    "CNINFO main update",
    "Fetches the previous open trading date through the scheduled Shanghai date.",
    CNINFO_MAIN_TARGET_DATE_POLICY,
    (_calendar_input(), _provider_input()),
)
CNINFO_INCREMENTAL_NOON = _contract(
    "cninfo_incremental_noon",
    "CNINFO noon incremental update",
    "Fetches the scheduled Shanghai date as the noon CNINFO increment.",
    CNINFO_INCREMENTAL_TARGET_DATE_POLICY,
    (_provider_input(),),
)
CNINFO_INCREMENTAL_AFTERNOON = _contract(
    "cninfo_incremental_afternoon",
    "CNINFO afternoon incremental update",
    "Fetches the scheduled Shanghai date as the afternoon CNINFO increment.",
    CNINFO_INCREMENTAL_TARGET_DATE_POLICY,
    (_provider_input(),),
)

CNINFO_CONTRACTS: tuple[PipelineContract, ...] = (
    CNINFO_MAIN_UPDATE,
    CNINFO_INCREMENTAL_NOON,
    CNINFO_INCREMENTAL_AFTERNOON,
)

for _contract_definition in CNINFO_CONTRACTS:
    register_pipeline(_contract_definition)


__all__ = [
    "CNINFO_CONTRACTS",
    "CNINFO_INCREMENTAL_AFTERNOON",
    "CNINFO_INCREMENTAL_NOON",
    "CNINFO_INCREMENTAL_TARGET_DATE_POLICY",
    "CNINFO_MAIN_TARGET_DATE_POLICY",
    "CNINFO_MAIN_UPDATE",
    "CNINFO_OPTIONAL_PROVIDER_FIELDS",
    "CNINFO_PROVIDER_FIELDS",
    "CNINFO_REQUIRED_PROVIDER_FIELDS",
]
