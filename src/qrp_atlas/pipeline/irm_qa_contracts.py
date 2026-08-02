"""Formal Contract for incremental P5W investor-relations Q&A ingestion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from time import monotonic
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    COMPANY_CODE,
    IRM_INTERACTION_QA,
    INTERACTION_PID,
    QUESTION_TIME,
    REPLY_DATE,
    REPLY_TIME,
    TICKER,
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
    PipelineInvocation,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .irm_qa.clean import clean_record
from .irm_qa.config import (
    P5W_HEADERS,
    P5W_MAX_PAGES,
    P5W_PAGE_SIZE,
    P5W_PROVIDER_MAX_RETRIES,
    P5W_REQUEST_TIMEOUT,
    P5W_SOURCE,
    P5W_URL,
)
from .irm_qa.fetch import (
    P5W_REQUIRED_PROVIDER_FIELDS,
    InteractionQAFetchReport,
    fetch_interaction_qa_with_report,
)
from .irm_qa.load import append_interaction_qa, prepare_interaction_qa_frame
from .registry import register_pipeline


IRM_TIMEZONE = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
IRM_TABLE = IRM_INTERACTION_QA.name


def _scheduled_local_date(invocation: PipelineInvocation) -> date:
    scheduled_for = invocation.scheduled_for
    if scheduled_for.tzinfo is None:
        raise ContractError("IRM_SCHEDULE_TIMEZONE_MISSING")
    return scheduled_for.astimezone(IRM_TIMEZONE).date()


def _target_date_resolver(invocation: PipelineInvocation) -> TargetWindow:
    """Use the scheduled local date only as an observation label."""

    return TargetWindow.for_date(_scheduled_local_date(invocation))


def _explicit_date_validator(target_date: date, invocation: PipelineInvocation) -> bool:
    """Reject historical-date overrides because P5W exposes a latest feed only."""

    invocation.execution_control.check()
    return False


def _provider_configuration(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    if (
        not P5W_URL
        or not P5W_SOURCE
        or P5W_PAGE_SIZE <= 0
        or P5W_MAX_PAGES <= 0
        or P5W_PROVIDER_MAX_RETRIES < 0
        or P5W_REQUEST_TIMEOUT <= 0
    ):
        return CheckResult.failure(
            "irm_provider_configuration",
            "IRM_PROVIDER_CONFIGURATION_INVALID",
            "P5W provider configuration is incomplete",
        )
    return CheckResult.success(
        "irm_provider_configuration",
        provider=P5W_SOURCE,
        endpoint=P5W_URL,
        page_size=P5W_PAGE_SIZE,
        max_pages=P5W_MAX_PAGES,
        request_timeout_seconds=P5W_REQUEST_TIMEOUT,
        provider_retries=P5W_PROVIDER_MAX_RETRIES,
        headers=tuple(sorted(P5W_HEADERS)),
    )


def _provider_freshness(context: PipelineRunContext) -> CheckResult:
    """Document the real freshness boundary: latest feed at request time."""

    context.execution_control.check()
    target = context.target_window.target_date
    return CheckResult.success(
        "irm_provider_freshness",
        observation_date=target.isoformat() if target is not None else None,
        provider_scope="latest replies returned at request time",
        persistent_watermark=None,
        date_filter_applied=False,
    )


def _parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _validate_cleaned_record(record: Mapping[str, object], index: int) -> None:
    pid = str(record.get(INTERACTION_PID) or "").strip()
    company_code = str(record.get(COMPANY_CODE) or "").strip()
    reply_time = _parse_timestamp(record.get(REPLY_TIME))
    question_time = record.get(QUESTION_TIME)
    if not pid:
        raise ContractError("IRM_CLEAN_SCHEMA_INVALID", f"record {index} has empty pid")
    if len(company_code) != 6 or not company_code.isdigit():
        raise ContractError("IRM_CLEAN_SCHEMA_INVALID", f"record {index} has invalid company_code")
    if reply_time is None:
        raise ContractError("IRM_CLEAN_SCHEMA_INVALID", f"record {index} has invalid reply_time")
    if question_time is not None and _parse_timestamp(question_time) is None:
        raise ContractError("IRM_CLEAN_SCHEMA_INVALID", f"record {index} has invalid question_time")
    reply_date = str(record.get(REPLY_DATE) or "").strip()
    if reply_date != reply_time.date().isoformat():
        raise ContractError("IRM_CLEAN_SCHEMA_INVALID", f"record {index} reply_date does not match reply_time")
    if not str(record.get(TICKER) or "").strip():
        raise ContractError("IRM_CLEAN_SCHEMA_INVALID", f"record {index} has empty ticker")


def _clean_records(
    records: Sequence[Mapping[str, object]],
    context: PipelineRunContext,
) -> list[dict[str, object]]:
    by_pid: dict[str, dict[str, object]] = {}
    for index, raw in enumerate(records):
        context.execution_control.check()
        try:
            cleaned = clean_record(dict(raw))
        except Exception as exc:  # noqa: BLE001
            raise ContractError("IRM_CLEAN_FAILED", f"record {index}: {type(exc).__name__}") from exc
        if cleaned is None:
            raise ContractError("IRM_CLEAN_SCHEMA_INVALID", f"record {index} could not be normalized")
        _validate_cleaned_record(cleaned, index)
        pid = str(cleaned[INTERACTION_PID])
        previous = by_pid.get(pid)
        if previous is not None and previous != cleaned:
            raise ContractError("IRM_DUPLICATE_PID_CONFLICT", f"pid {pid} has conflicting payloads")
        by_pid[pid] = cleaned
    context.execution_control.check()
    return list(by_pid.values())


def _fetch_and_clean(
    context: PipelineRunContext,
) -> tuple[list[dict[str, object]], int, InteractionQAFetchReport]:
    context.execution_control.check()
    try:
        raw_records, report = fetch_interaction_qa_with_report(
            execution_control=context.execution_control,
        )
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ContractError("IRM_PROVIDER_ERROR", type(exc).__name__) from exc
    context.execution_control.check()
    cleaned = _clean_records(raw_records, context)
    context.execution_control.check()
    return cleaned, report.rows_read, report


def _prepare_rows(
    records: Sequence[Mapping[str, object]],
) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=IRM_INTERACTION_QA.column_names())
    try:
        frame = prepare_interaction_qa_frame(records)
    except Exception as exc:  # noqa: BLE001
        raise ContractError("IRM_CLEAN_SCHEMA_INVALID", type(exc).__name__) from exc
    for field in (INTERACTION_PID, TICKER, COMPANY_CODE, REPLY_TIME, REPLY_DATE):
        if field not in frame or frame[field].isna().any():
            raise ContractError("IRM_CLEAN_SCHEMA_INVALID", f"required field {field} is null")
        if frame[field].astype(str).str.strip().eq("").any():
            raise ContractError("IRM_CLEAN_SCHEMA_INVALID", f"required field {field} is empty")
    return frame.drop_duplicates(subset=list(IRM_INTERACTION_QA.primary_key), keep="first").reset_index(drop=True)


def _append_rows(
    context: PipelineRunContext,
    rows: pd.DataFrame,
) -> tuple[int, float]:
    if rows.empty:
        return 0, 0.0

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
        inserted = append_interaction_qa(
            connection,
            rows,
            execution_control=context.execution_control,
        )
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
    except Exception as exc:  # noqa: BLE001
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
        raise ContractError("IRM_WRITE_FAILED", type(exc).__name__) from exc
    finally:
        if connection is not None:
            connection.close()


def _execute_irm(context: PipelineRunContext) -> BusinessExecution:
    fetch_started = monotonic()
    cleaned_records, raw_rows, report = _fetch_and_clean(context)
    fetched_at = monotonic()
    context.execution_control.check()
    rows = _prepare_rows(cleaned_records)
    prepared_at = monotonic()
    context.execution_control.check()
    inserted, write_seconds = _append_rows(context, rows)
    context.execution_control.check()
    observation_date = context.target_window.target_date
    output = OutputResult(
        output_id=IRM_TABLE,
        rows_written=inserted,
        location="settings.paths.duckdb_path",
        completed=True,
        detail={
            "observation_date": observation_date.isoformat() if observation_date else None,
            "provider": P5W_SOURCE,
            "provider_scope": "latest replies returned at request time",
            "persistent_watermark": None,
            "pages_fetched": report.pages_fetched,
            "api_requests": report.api_requests,
            "retries": report.retries,
            "rows_read": raw_rows,
            "rows_deduplicated": len(rows),
            "rows_written": inserted,
            "stop_reason": report.stop_reason,
            "same_pid_semantics": "first committed row is retained; later payloads are ignored",
        },
    )
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=raw_rows,
            rows_written=inserted,
            assets_processed=int(rows[TICKER].nunique()) if not rows.empty else 0,
            dates_processed=1,
            database_write_seconds=write_seconds,
            stage_durations_seconds={
                "fetch": max(0.0, fetched_at - fetch_started),
                "clean_and_prepare": max(0.0, prepared_at - fetched_at),
                "database_write": write_seconds,
            },
            api_requests=report.api_requests,
            batches=report.pages_fetched,
            retries=report.retries,
        ),
        outputs=(output,),
    )


def _irm_completion(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            total_rows = int(connection.execute(f"SELECT COUNT(*) FROM {IRM_TABLE}").fetchone()[0])
            context.execution_control.check()
        finally:
            connection.close()
    except ExecutionControlError:
        raise
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "irm_output_completion",
            "IRM_COMPLETION_MISSING",
            "IRM output is not queryable after the transaction",
            exception=type(exc).__name__,
        )
    return CheckResult.success(
        "irm_output_completion",
        table=IRM_TABLE,
        total_rows=total_rows,
        marker="committed transactional append is queryable",
    )


def _irm_unique_key_quality(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            duplicate = connection.execute(
                f"""
                SELECT {INTERACTION_PID}
                FROM {IRM_TABLE}
                GROUP BY {INTERACTION_PID}
                HAVING COUNT(*) > 1
                LIMIT 1
                """
            ).fetchone()
            context.execution_control.check()
        finally:
            connection.close()
    except ExecutionControlError:
        raise
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "irm_unique_key_quality",
            "IRM_UNIQUE_KEY_CHECK_FAILED",
            "IRM unique-key quality query failed",
            exception=type(exc).__name__,
        )
    if duplicate is not None:
        return CheckResult.failure(
            "irm_unique_key_quality",
            "IRM_DUPLICATE_KEY",
            "IRM output contains duplicate pid values",
        )
    return CheckResult.success(
        "irm_unique_key_quality",
        unique_key=list(IRM_INTERACTION_QA.primary_key),
    )


def _provider_input() -> InputContract:
    return InputContract(
        input_id="external_p5w",
        kind=InputKind.EXTERNAL_API,
        source=P5W_URL,
        required_fields=P5W_REQUIRED_PROVIDER_FIELDS,
        target_date_semantics="latest provider feed is evaluated at request time; resolved date is observation metadata only",
        missing_error_code="IRM_PROVIDER_CONFIGURATION_INVALID",
        structure_check=_provider_configuration,
        freshness=FreshnessContract(
            check_id="irm_provider_freshness",
            target_date_semantics="no provider date filter or persistent watermark is available",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
            error_code="IRM_PROVIDER_STALE",
            checker=_provider_freshness,
        ),
    )


def _output() -> OutputContract:
    return OutputContract(
        output_id=IRM_TABLE,
        physical_resource=QUANT_DB_RESOURCE,
        location="settings.paths.duckdb_path",
        object_name=IRM_TABLE,
        unique_key=IRM_INTERACTION_QA.primary_key,
        write_mode=WriteMode.APPEND,
        target_date_semantics="append records observed from the latest feed; no reply-date target filtering",
        completion=CompletionContract(
            marker="committed transactional append is queryable in irm_interaction_qa",
            error_code="IRM_COMPLETION_MISSING",
            checker=_irm_completion,
        ),
        quality_checks=(_irm_unique_key_quality,),
        allow_empty=True,
    )


def _performance() -> PerformanceBudget:
    return PerformanceBudget(
        normal_budget_seconds=180.0,
        warning_threshold_seconds=120.0,
        hard_timeout_seconds=300,
        benchmark_scope="mocked two-page latest-feed scan with ten records per full page; provider request timeout is 15 seconds",
        baseline_source=(
            "tests/pipeline/test_irm_contracts.py mocked page benchmark; "
            "docs/全景网互动问答接口调研报告.md sections 3-7; "
            "src/qrp_atlas/pipeline/irm_qa/config.py"
        ),
    )


IRM_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="irm_qa_incremental_observation_date",
    description="Use the scheduled Shanghai natural date as observation metadata; P5W remains a latest-feed scan.",
    trading_calendar_id="none",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_target_date_resolver,
    validate_explicit_date=_explicit_date_validator,
)


IRM_QA_INCREMENTAL = register_pipeline(
    PipelineContract(
        pipeline_id="irm_qa_incremental",
        name="IRM Q&A incremental ingestion",
        description=(
            "Scans the P5W latest-reply feed, validates complete page boundaries, "
            "and transactionally appends new interaction Q&A rows by pid."
        ),
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_irm,
        target_date_policy=IRM_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(_provider_input(),),
        outputs=(_output(),),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=(),
        idempotency=IdempotencyContract(
            idempotency_key="pid",
            repeat_run_semantics="the latest feed may be scanned repeatedly without duplicate logical rows",
            existing_target_handling="INSERT OR IGNORE preserves the first committed row for each pid",
            failure_recovery="rollback the complete prepared append and retry the scan",
            uses_staging=False,
            atomic_replace_boundary="one database transaction around the prepared pid append",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="all cleaned rows from one latest-feed scan",
            failure_visibility=(
                "provider, validation, cancellation, or database errors before commit leave no new rows; "
                "a committed outcome is safe to retry by pid"
            ),
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=_performance(),
        manual_execution_allowed=True,
    )
)


IRM_CONTRACTS: tuple[PipelineContract, ...] = (IRM_QA_INCREMENTAL,)


__all__ = [
    "IRM_CONTRACTS",
    "IRM_QA_INCREMENTAL",
    "IRM_TARGET_DATE_POLICY",
]
