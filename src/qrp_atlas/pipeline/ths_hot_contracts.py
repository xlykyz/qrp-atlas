"""Formal Pipeline for THS hot stock rank (ths_hot) data."""

from __future__ import annotations

import time

from qrp_atlas.config.tushare_client import get_tushare_pro
from qrp_atlas.contracts import (
    THS_HOT,
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
from .popularity_support import (
    THS_HOT_RAW_FIELDS,
    clean_ths_hot_batch,
    fetch_ths_hot_range,
    popularity_range_completion,
    popularity_unique_key_quality,
    popularity_unique_ticker_quality,
    replace_ths_hot_batch,
)
from .registry import register_pipeline
from .tushare_snapshot_support import (
    QUANT_DB_RESOURCE,
    QUANT_DB_WRITER,
    provider_configuration,
    provider_freshness,
    resolve_date_or_range_target,
    validate_single_date_override,
)

THS_HOT_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="ths_hot_date_or_range_v1",
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
    return provider_freshness(context, "tushare_ths_hot")


def _translate_provider_error(exc: ContractError) -> ContractError:
    if exc.code in {
        "PROVIDER_RESPONSE_INVALID",
        "PROVIDER_SCHEMA_MISSING",
        "PROVIDER_DATE_INVALID",
        "PROVIDER_SCOPE_MISMATCH",
    }:
        return ContractError("THS_HOT_API_PARTIAL", exc.detail)
    if exc.code in {"PROVIDER_API_UNAVAILABLE", "PROVIDER_REQUEST_FAILED"}:
        return ContractError("THS_HOT_API_FAILED", exc.detail)
    return exc


def execute_ths_hot_ingest(context: PipelineRunContext) -> BusinessExecution:
    started = time.monotonic()
    try:
        client = get_tushare_pro(
            settings=context.settings,
            execution_control=context.execution_control,
        )
        raw, raw_path, empty_dates, rows_read, api_requests = fetch_ths_hot_range(
            context,
            client=client,
        )
    except ExecutionControlError:
        raise
    except ContractError as exc:
        raise _translate_provider_error(exc) from exc
    except Exception as exc:
        raise ContractError("THS_HOT_API_FAILED", type(exc).__name__) from exc
    fetched_at = time.monotonic()

    try:
        prepared, clean_path = clean_ths_hot_batch(
            raw,
            target_window=context.target_window,
            context=context,
        )
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("THS_HOT_API_PARTIAL", type(exc).__name__) from exc
    normalized_at = time.monotonic()

    try:
        rows_written, database_seconds = replace_ths_hot_batch(
            context,
            prepared=prepared,
            empty_dates=empty_dates,
        )
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("THS_HOT_WRITE_FAILED", type(exc).__name__) from exc
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
                output_id=THS_HOT.name,
                rows_written=rows_written,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={
                    "scope": context.target_window.as_dict(),
                    "raw_path": str(raw_path),
                    "clean_path": str(clean_path) if clean_path else None,
                    "empty_dates": [d.isoformat() for d in empty_dates],
                },
            ),
        ),
    )


THS_HOT_INGEST = register_pipeline(
    PipelineContract(
        pipeline_id="ths_hot_ingest",
        name="THS hot stock rank",
        description=(
            "Fetches Tushare ths_hot records for a bounded date or date range, "
            "saves Raw CSV, cleans into canonical schema with snapshot reconstruction, "
            "saves Clean CSV, and atomically replaces matching target rows in quant.db."
        ),
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_ths_hot_ingest,
        target_date_policy=THS_HOT_TARGET_DATE_POLICY,
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
                input_id="tushare_ths_hot",
                kind=InputKind.EXTERNAL_API,
                source="tushare.pro.ths_hot(trade_date=YYYYMMDD, market=热股, is_new=N)",
                required_fields=THS_HOT_RAW_FIELDS,
                target_date_semantics=(
                    "provider rows must fall inside the resolved target date or inclusive date range"
                ),
                missing_error_code="TUSHARE_CONFIGURATION_MISSING",
                structure_check=provider_configuration,
                freshness=FreshnessContract(
                    check_id="tushare_ths_hot_freshness",
                    target_date_semantics="date-bounded provider response is checked before write",
                    maximum_lag_trading_days=0,
                    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                    error_code="THS_HOT_API_STALE",
                    checker=_freshness,
                ),
            ),
        ),
        outputs=(
            OutputContract(
                output_id=THS_HOT.name,
                physical_resource=QUANT_DB_RESOURCE,
                location="settings.paths.duckdb_path",
                object_name=THS_HOT.name,
                unique_key=THS_HOT.primary_key,
                write_mode=WriteMode.REPLACE_TARGET_RANGE,
                target_date_semantics="all rows for the resolved target date or inclusive range",
                completion=CompletionContract(
                    marker="ths_hot target range is queryable after replacement",
                    error_code="THS_HOT_COMPLETION_MISSING",
                    checker=popularity_range_completion(THS_HOT.name, "THS_HOT_COMPLETION_MISSING"),
                ),
                quality_checks=(
                    popularity_unique_key_quality(
                        THS_HOT.name,
                        "THS_HOT_DUPLICATE_KEY",
                    ),
                    popularity_unique_ticker_quality(
                        THS_HOT.name,
                        "THS_HOT_DUPLICATE_TICKER",
                    ),
                ),
                allow_empty=True,
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=IdempotencyContract(
            idempotency_key="ths_hot.(trade_date, snapshot_seq, rank_position)",
            repeat_run_semantics=(
                "repeating the same complete date scope replaces that complete scope and produces no duplicate keys"
            ),
            existing_target_handling=(
                "delete and insert only dates with complete non-empty responses in one transaction; "
                "empty dates with existing rows fail before deletion"
            ),
            failure_recovery="provider and normalization failures happen before write; a failed transaction rolls back the target range",
            uses_staging=False,
            atomic_replace_boundary=(
                "preflight checks every requested date; one quant.db transaction replaces "
                "non-empty complete dates and leaves valid empty dates untouched"
            ),
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="validated complete ths_hot date snapshots for the resolved date scope",
            failure_visibility="failed target replacement is rolled back and prior target rows remain visible",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=PerformanceBudget(
            normal_budget_seconds=600.0,
            warning_threshold_seconds=300.0,
            hard_timeout_seconds=900,
            benchmark_scope="offline acceptance: one request per calendar date, snapshot reconstruction, and one target-range DuckDB replacement",
            baseline_source="tests/pipeline/test_popularity_contracts.py ths_hot_ingest acceptance path",
        ),
        manual_execution_allowed=True,
    )
)

THS_HOT_UPDATE = THS_HOT_INGEST
THS_HOT_CONTRACTS = (THS_HOT_INGEST,)
