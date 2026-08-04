"""Formal Pipeline for Tushare stock severe-abnormal-volatility notices."""

from __future__ import annotations

import time

from qrp_atlas.config.tushare_client import get_tushare_pro
from qrp_atlas.contracts import (
    NAME,
    PERIOD,
    REASON,
    STK_HIGH_SHOCK,
    TICKER,
    TRADE_DATE,
    TRADE_MARKET,
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
    optional_text,
    prepare_canonical_frame,
    provider_configuration,
    provider_freshness,
    range_completion,
    range_unique_key_quality,
    replace_target_window,
    resolve_date_or_range_target,
    validate_single_date_override,
)


STK_HIGH_SHOCK_REQUIRED_FIELDS = (
    "ts_code",
    "trade_date",
    "name",
    "trade_market",
    "reason",
    "period",
)
STK_HIGH_SHOCK_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="stk_high_shock_date_or_range_v1",
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
    return provider_freshness(context, "tushare_stk_high_shock")


def _scope(context: PipelineRunContext) -> str | None:
    context.execution_control.check()
    return optional_text(context.parameter_overrides.get("ts_code"))


def _translate_provider_error(exc: ContractError) -> ContractError:
    if exc.code in {
        "PROVIDER_RESPONSE_INVALID",
        "PROVIDER_SCHEMA_MISSING",
        "PROVIDER_DATE_INVALID",
        "PROVIDER_SCOPE_MISMATCH",
    }:
        return ContractError("STK_HIGH_SHOCK_API_PARTIAL", exc.detail)
    if exc.code in {"PROVIDER_API_UNAVAILABLE", "PROVIDER_REQUEST_FAILED"}:
        return ContractError("STK_HIGH_SHOCK_API_FAILED", exc.detail)
    return exc


def execute_stk_high_shock_ingest(context: PipelineRunContext) -> BusinessExecution:
    started = time.monotonic()
    requested_code = _scope(context)
    try:
        client = get_tushare_pro(
            settings=context.settings,
            execution_control=context.execution_control,
        )
        raw, rows_read, api_requests = fetch_by_calendar_date(
            context,
            client=client,
            endpoint="stk_high_shock",
            required_fields=STK_HIGH_SHOCK_REQUIRED_FIELDS,
            requested_code=requested_code,
        )
    except ExecutionControlError:
        raise
    except ContractError as exc:
        raise _translate_provider_error(exc) from exc
    except Exception as exc:
        raise ContractError("STK_HIGH_SHOCK_API_FAILED", type(exc).__name__) from exc
    fetched_at = time.monotonic()

    try:
        prepared = prepare_canonical_frame(
            raw,
            table_name=STK_HIGH_SHOCK.name,
            mapping_source="tushare_stk_high_shock",
            required_text_columns=(TICKER, NAME, TRADE_MARKET, REASON, PERIOD),
            error_code="STK_HIGH_SHOCK_API_PARTIAL",
        )
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("STK_HIGH_SHOCK_API_PARTIAL", type(exc).__name__) from exc
    normalized_at = time.monotonic()

    try:
        rows_written, database_seconds = replace_target_window(
            context,
            table_name=STK_HIGH_SHOCK.name,
            prepared=prepared,
        )
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("STK_HIGH_SHOCK_WRITE_FAILED", type(exc).__name__) from exc
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
                output_id=STK_HIGH_SHOCK.name,
                rows_written=rows_written,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={
                    "requested_ts_code": requested_code,
                    "scope": context.target_window.as_dict(),
                    "request_granularity": "one trade_date request per calendar date",
                    "primary_key_semantics": "(trade_date, ticker, reason, period)",
                },
            ),
        ),
    )


STK_HIGH_SHOCK_INGEST = register_pipeline(
    PipelineContract(
        pipeline_id="stk_high_shock_ingest",
        name="Tushare stock severe abnormal volatility",
        description=(
            "Fetches Tushare stk_high_shock notices for a bounded date or date range, "
            "preserves each stock/reason/period event, and replaces the matching target rows in quant.db."
        ),
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_stk_high_shock_ingest,
        target_date_policy=STK_HIGH_SHOCK_TARGET_DATE_POLICY,
        parameters=(
            ParameterContract(
                "ts_code",
                ParameterType.STRING,
                "Optional Tushare stock code filter, for example 002015.SZ.",
                default="",
            ),
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
                input_id="tushare_stk_high_shock",
                kind=InputKind.EXTERNAL_API,
                source="tushare.pro.stk_high_shock(ts_code=..., trade_date=YYYYMMDD)",
                required_fields=STK_HIGH_SHOCK_REQUIRED_FIELDS,
                target_date_semantics=(
                    "provider rows must fall inside the resolved target date or inclusive date range"
                ),
                missing_error_code="TUSHARE_CONFIGURATION_MISSING",
                structure_check=provider_configuration,
                freshness=FreshnessContract(
                    check_id="tushare_stk_high_shock_freshness",
                    target_date_semantics="date-bounded provider response is checked before write",
                    maximum_lag_trading_days=0,
                    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                    error_code="STK_HIGH_SHOCK_API_STALE",
                    checker=_freshness,
                ),
            ),
        ),
        outputs=(
            OutputContract(
                output_id=STK_HIGH_SHOCK.name,
                physical_resource=QUANT_DB_RESOURCE,
                location="settings.paths.duckdb_path",
                object_name=STK_HIGH_SHOCK.name,
                unique_key=STK_HIGH_SHOCK.primary_key,
                write_mode=WriteMode.REPLACE_TARGET_RANGE,
                target_date_semantics="all rows for the resolved target date or inclusive range",
                completion=CompletionContract(
                    marker="stk_high_shock target range is queryable after replacement",
                    error_code="STK_HIGH_SHOCK_COMPLETION_MISSING",
                    checker=range_completion(
                        STK_HIGH_SHOCK.name,
                        "STK_HIGH_SHOCK_COMPLETION_MISSING",
                    ),
                ),
                quality_checks=(
                    range_unique_key_quality(
                        STK_HIGH_SHOCK.name,
                        STK_HIGH_SHOCK.primary_key,
                        "STK_HIGH_SHOCK_DUPLICATE_KEY",
                    ),
                ),
                allow_empty=True,
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=IdempotencyContract(
            idempotency_key="stk_high_shock.(trade_date, ticker, reason, period)",
            repeat_run_semantics=(
                "repeating the same date scope replaces only that scope and produces no duplicate events"
            ),
            existing_target_handling="delete and insert the complete validated target range in one transaction",
            failure_recovery="provider and normalization failures happen before the write; a failed transaction rolls back the target range",
            uses_staging=False,
            atomic_replace_boundary="one quant.db transaction replaces every target-date row",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="validated stk_high_shock rows for the complete resolved date scope",
            failure_visibility="failed target replacement is rolled back and prior target rows remain visible",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=PerformanceBudget(
            normal_budget_seconds=600.0,
            warning_threshold_seconds=300.0,
            hard_timeout_seconds=900,
            benchmark_scope="offline acceptance: one request per calendar date, normalization, and one target-range DuckDB replacement",
            baseline_source="tests/pipeline/test_tushare_event_contracts.py stk_high_shock_ingest acceptance path",
        ),
        manual_execution_allowed=True,
    )
)

STK_HIGH_SHOCK_UPDATE = STK_HIGH_SHOCK_INGEST
STK_HIGH_SHOCK_CONTRACTS = (STK_HIGH_SHOCK_INGEST,)
