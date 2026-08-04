"""Formal Pipeline for Tushare Tonghuashun sector-index daily data."""

from __future__ import annotations

import time

from qrp_atlas.config.tushare_client import get_tushare_pro
from qrp_atlas.contracts import (
    AVG_PRICE,
    CHANGE,
    CLOSE,
    FLOAT_MV,
    HIGH,
    INDEX_CODE,
    LOW,
    OPEN,
    PCT_CHANGE,
    PRE_CLOSE,
    THS_DAILY,
    TOTAL_MV,
    TRADE_DATE,
    TURNOVER_RATE,
    VOLUME,
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


THS_DAILY_REQUIRED_FIELDS = (
    "ts_code",
    "trade_date",
    "close",
    "open",
    "high",
    "low",
    "pre_close",
    "avg_price",
    "change",
    "pct_change",
    "vol",
    "turnover_rate",
)
THS_DAILY_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="ths_daily_date_or_range_v1",
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
    return provider_freshness(context, "tushare_ths_daily")


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
        return ContractError("THS_DAILY_API_PARTIAL", exc.detail)
    if exc.code in {"PROVIDER_API_UNAVAILABLE", "PROVIDER_REQUEST_FAILED"}:
        return ContractError("THS_DAILY_API_FAILED", exc.detail)
    return exc


def execute_ths_daily_ingest(context: PipelineRunContext) -> BusinessExecution:
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
            endpoint="ths_daily",
            required_fields=THS_DAILY_REQUIRED_FIELDS,
            requested_code=requested_code,
        )
    except ExecutionControlError:
        raise
    except ContractError as exc:
        raise _translate_provider_error(exc) from exc
    except Exception as exc:
        raise ContractError("THS_DAILY_API_FAILED", type(exc).__name__) from exc
    fetched_at = time.monotonic()

    try:
        prepared = prepare_canonical_frame(
            raw,
            table_name=THS_DAILY.name,
            mapping_source="tushare_ths_daily",
            required_text_columns=(INDEX_CODE,),
            numeric_columns=(
                CLOSE,
                OPEN,
                HIGH,
                LOW,
                PRE_CLOSE,
                AVG_PRICE,
                CHANGE,
                PCT_CHANGE,
                VOLUME,
                TURNOVER_RATE,
                TOTAL_MV,
                FLOAT_MV,
            ),
            error_code="THS_DAILY_API_PARTIAL",
        )
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("THS_DAILY_API_PARTIAL", type(exc).__name__) from exc
    normalized_at = time.monotonic()

    try:
        rows_written, database_seconds = replace_target_window(
            context,
            table_name=THS_DAILY.name,
            prepared=prepared,
        )
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("THS_DAILY_WRITE_FAILED", type(exc).__name__) from exc
    completed_at = time.monotonic()

    assets = int(prepared[INDEX_CODE].nunique()) if not prepared.empty else 0
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
                output_id=THS_DAILY.name,
                rows_written=rows_written,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={
                    "requested_ts_code": requested_code,
                    "scope": context.target_window.as_dict(),
                    "request_granularity": "one trade_date request per calendar date",
                    "optional_provider_fields": ["total_mv", "float_mv"],
                },
            ),
        ),
    )


THS_DAILY_INGEST = register_pipeline(
    PipelineContract(
        pipeline_id="ths_daily_ingest",
        name="Tushare Tonghuashun sector-index daily data",
        description=(
            "Fetches Tushare ths_daily rows for a bounded date or date range, "
            "normalizes index market fields, and replaces the matching target rows in quant.db."
        ),
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_ths_daily_ingest,
        target_date_policy=THS_DAILY_TARGET_DATE_POLICY,
        parameters=(
            ParameterContract(
                "ts_code",
                ParameterType.STRING,
                "Optional Tonghuashun index code filter, for example 865001.TI.",
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
                input_id="tushare_ths_daily",
                kind=InputKind.EXTERNAL_API,
                source="tushare.pro.ths_daily(ts_code=..., trade_date=YYYYMMDD)",
                required_fields=THS_DAILY_REQUIRED_FIELDS,
                target_date_semantics=(
                    "provider rows must fall inside the resolved target date or inclusive date range"
                ),
                missing_error_code="TUSHARE_CONFIGURATION_MISSING",
                structure_check=provider_configuration,
                freshness=FreshnessContract(
                    check_id="tushare_ths_daily_freshness",
                    target_date_semantics="date-bounded provider response is checked before write",
                    maximum_lag_trading_days=0,
                    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                    error_code="THS_DAILY_API_STALE",
                    checker=_freshness,
                ),
            ),
        ),
        outputs=(
            OutputContract(
                output_id=THS_DAILY.name,
                physical_resource=QUANT_DB_RESOURCE,
                location="settings.paths.duckdb_path",
                object_name=THS_DAILY.name,
                unique_key=THS_DAILY.primary_key,
                write_mode=WriteMode.REPLACE_TARGET_RANGE,
                target_date_semantics="all rows for the resolved target date or inclusive range",
                completion=CompletionContract(
                    marker="ths_daily target range is queryable after replacement",
                    error_code="THS_DAILY_COMPLETION_MISSING",
                    checker=range_completion(THS_DAILY.name, "THS_DAILY_COMPLETION_MISSING"),
                ),
                quality_checks=(
                    range_unique_key_quality(
                        THS_DAILY.name,
                        THS_DAILY.primary_key,
                        "THS_DAILY_DUPLICATE_KEY",
                    ),
                ),
                allow_empty=True,
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=IdempotencyContract(
            idempotency_key="ths_daily.(trade_date, index_code)",
            repeat_run_semantics=(
                "repeating the same date scope replaces only that scope and produces no duplicate keys"
            ),
            existing_target_handling="delete and insert the complete validated target range in one transaction",
            failure_recovery="provider and normalization failures happen before the write; a failed transaction rolls back the target range",
            uses_staging=False,
            atomic_replace_boundary="one quant.db transaction replaces every target-date row",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="validated ths_daily rows for the complete resolved date scope",
            failure_visibility="failed target replacement is rolled back and prior target rows remain visible",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=PerformanceBudget(
            normal_budget_seconds=600.0,
            warning_threshold_seconds=300.0,
            hard_timeout_seconds=900,
            benchmark_scope="offline acceptance: one request per calendar date, normalization, and one target-range DuckDB replacement",
            baseline_source="tests/pipeline/test_tushare_event_contracts.py ths_daily_ingest acceptance path",
        ),
        manual_execution_allowed=True,
    )
)

THS_DAILY_UPDATE = THS_DAILY_INGEST
THS_DAILY_CONTRACTS = (THS_DAILY_INGEST,)
