"""Formal Pipeline for Tushare limit-step (consecutive limit-up) data."""

from __future__ import annotations

import time

from qrp_atlas.config.tushare_client import get_tushare_pro
from qrp_atlas.contracts import (
    CONSECUTIVE_BOARDS,
    LIMIT_STEP,
    NAME,
    TICKER,
    TRADE_DATE,
)
from qrp_atlas.orchestration.execution_control import ExecutionControlError
from qrp_atlas.orchestration.models import OverlapPolicy

from .contracts import (
    BusinessExecution,
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
    ParameterContract,
    ParameterType,
    PerformanceBudget,
    PipelineContract,
    PipelineKind,
    PipelineMetrics,
    PipelineRunContext,
    TargetDatePolicy,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .registry import register_pipeline
from .tushare_snapshot_support import (
    QUANT_DB_RESOURCE,
    QUANT_DB_WRITER,
    fetch_by_calendar_date,
    prepare_canonical_frame,
    provider_configuration,
    provider_freshness,
    range_completion,
    range_unique_key_quality,
    replace_target_window,
    resolve_date_or_range_target,
    validate_single_date_override,
)

LIMIT_STEP_REQUIRED_FIELDS = ("ts_code", "name", "trade_date", "nums")
LIMIT_STEP_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="limit_step_date_or_range_v1",
    description=(
        "Uses explicit start_date/end_date parameters for a bounded inclusive range; "
        "otherwise uses the scheduled Asia/Shanghai calendar date."
    ),
    trading_calendar_id="clock:Asia/Shanghai",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=resolve_date_or_range_target,
    validate_explicit_date=validate_single_date_override,
)


def _freshness(context: PipelineRunContext):
    return provider_freshness(context, "tushare_limit_step")


def _translate_provider_error(exc: ContractError) -> ContractError:
    if exc.code in {
        "PROVIDER_RESPONSE_INVALID",
        "PROVIDER_SCHEMA_MISSING",
        "PROVIDER_DATE_INVALID",
        "PROVIDER_SCOPE_MISMATCH",
    }:
        return ContractError("LIMIT_STEP_API_PARTIAL", exc.detail)
    if exc.code in {"PROVIDER_API_UNAVAILABLE", "PROVIDER_REQUEST_FAILED"}:
        return ContractError("LIMIT_STEP_API_FAILED", exc.detail)
    return exc


def execute_limit_step_ingest(context: PipelineRunContext) -> BusinessExecution:
    started = time.monotonic()
    try:
        client = get_tushare_pro(
            settings=context.settings,
            execution_control=context.execution_control,
        )
        raw, rows_read, api_requests, empty_dates = fetch_by_calendar_date(
            context,
            client=client,
            endpoint="limit_step",
            required_fields=LIMIT_STEP_REQUIRED_FIELDS,
        )
    except ExecutionControlError:
        raise
    except ContractError as exc:
        raise _translate_provider_error(exc) from exc
    except Exception as exc:
        raise ContractError("LIMIT_STEP_API_FAILED", type(exc).__name__) from exc
    fetched_at = time.monotonic()

    try:
        prepared = prepare_canonical_frame(
            raw,
            table_name=LIMIT_STEP.name,
            mapping_source="tushare_limit_step",
            required_text_columns=(TICKER, NAME),
            integer_columns=(CONSECUTIVE_BOARDS,),
            error_code="LIMIT_STEP_API_PARTIAL",
        )
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("LIMIT_STEP_API_PARTIAL", type(exc).__name__) from exc
    normalized_at = time.monotonic()

    try:
        rows_written, database_seconds = replace_target_window(
            context,
            table_name=LIMIT_STEP.name,
            prepared=prepared,
            empty_dates=empty_dates,
            empty_response_error_code="LIMIT_STEP_API_PARTIAL",
        )
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("LIMIT_STEP_WRITE_FAILED", type(exc).__name__) from exc
    completed_at = time.monotonic()

    assets = int(prepared[TICKER].nunique()) if not prepared.empty else 0
    dates = int(prepared[TRADE_DATE].nunique()) if not prepared.empty else 0
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=rows_read,
            rows_written=rows_written,
            assets_processed=assets,
            dates_processed=dates,
            database_write_seconds=database_seconds,
            stage_durations_seconds={
                "provider": fetched_at - started,
                "normalize": normalized_at - fetched_at,
                "database_write": completed_at - normalized_at,
            },
            api_requests=api_requests,
            batches=api_requests,
        ),
        outputs=(
            OutputResult(
                output_id=LIMIT_STEP.name,
                rows_written=rows_written,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={
                    "scope": context.target_window.as_dict(),
                    "complete_date_snapshot": True,
                    "request_granularity": "one trade_date request per calendar date",
                },
            ),
        ),
    )


LIMIT_STEP_INGEST = register_pipeline(
    PipelineContract(
        pipeline_id="limit_step_ingest",
        name="Tushare consecutive limit-up ladder",
        description=(
            "Fetches Tushare limit_step records for a bounded date or date range, "
            "normalizes the stock code and consecutive-board count, and replaces "
            "the matching target rows in quant.db."
        ),
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_limit_step_ingest,
        target_date_policy=LIMIT_STEP_TARGET_DATE_POLICY,
        parameters=(
            ParameterContract(
                "start_date",
                ParameterType.STRING,
                "Optional inclusive provider range start, in YYYY-MM-DD or YYYYMMDD form.",
                default="",
            ),
            ParameterContract(
                "end_date",
                ParameterType.STRING,
                "Optional inclusive provider range end, in YYYY-MM-DD or YYYYMMDD form.",
                default="",
            ),
        ),
        inputs=(
            InputContract(
                input_id="tushare_limit_step",
                kind=InputKind.EXTERNAL_API,
                source="tushare.pro.limit_step(trade_date=YYYYMMDD)",
                required_fields=LIMIT_STEP_REQUIRED_FIELDS,
                target_date_semantics=(
                    "provider rows must fall inside the resolved target date or inclusive date range"
                ),
                missing_error_code="TUSHARE_CONFIGURATION_MISSING",
                structure_check=provider_configuration,
                freshness=FreshnessContract(
                    check_id="tushare_limit_step_freshness",
                    target_date_semantics="date-bounded provider response is checked before write",
                    maximum_lag_trading_days=0,
                    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                    error_code="LIMIT_STEP_API_STALE",
                    checker=_freshness,
                ),
            ),
        ),
        outputs=(
            OutputContract(
                output_id=LIMIT_STEP.name,
                physical_resource=QUANT_DB_RESOURCE,
                location="settings.paths.duckdb_path",
                object_name=LIMIT_STEP.name,
                unique_key=LIMIT_STEP.primary_key,
                write_mode=WriteMode.REPLACE_TARGET_RANGE,
                target_date_semantics="all rows for the resolved target date or inclusive range",
                completion=CompletionContract(
                    marker="limit_step target range is queryable after replacement",
                    error_code="LIMIT_STEP_COMPLETION_MISSING",
                    checker=range_completion(LIMIT_STEP.name, "LIMIT_STEP_COMPLETION_MISSING"),
                ),
                quality_checks=(
                    range_unique_key_quality(
                        LIMIT_STEP.name,
                        LIMIT_STEP.primary_key,
                        "LIMIT_STEP_DUPLICATE_KEY",
                    ),
                ),
                allow_empty=True,
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=IdempotencyContract(
            idempotency_key="limit_step.(trade_date, ticker, consecutive_boards)",
            repeat_run_semantics=(
                "repeating the same complete date scope replaces that complete scope and produces no duplicate keys"
            ),
            existing_target_handling="delete and insert only dates with complete non-empty responses in one transaction; empty dates with existing rows fail before deletion",
            failure_recovery="provider and normalization failures happen before the write; a failed transaction rolls back the target range",
            uses_staging=False,
            atomic_replace_boundary=(
                "preflight checks every requested date; one quant.db transaction replaces "
                "non-empty complete dates and leaves valid empty dates untouched"
            ),
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="validated complete limit_step date snapshots for the resolved date scope",
            failure_visibility="failed target replacement is rolled back and prior target rows remain visible",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=PerformanceBudget(
            normal_budget_seconds=600.0,
            warning_threshold_seconds=300.0,
            hard_timeout_seconds=900,
            benchmark_scope="offline acceptance: one request per calendar date, normalization, and one target-range DuckDB replacement",
            baseline_source="tests/pipeline/test_tushare_event_contracts.py limit_step_ingest acceptance path",
        ),
        manual_execution_allowed=True,
    )
)

# Compatibility aliases for callers that use the endpoint/update naming style.
LIMIT_STEP_UPDATE = LIMIT_STEP_INGEST
LIMIT_STEP_CONTRACTS = (LIMIT_STEP_INGEST,)
