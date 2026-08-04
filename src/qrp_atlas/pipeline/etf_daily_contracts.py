"""Formal Tushare Contract for the ETF daily market snapshot."""

from __future__ import annotations

import time

import pandas as pd

from qrp_atlas.config.tushare_client import get_tushare_pro
from qrp_atlas.contracts import ETF_DAILY, TICKER
from qrp_atlas.orchestration.execution_control import ExecutionControlError

from .contracts import (
    BusinessExecution,
    ContractError,
    OutputResult,
    PerformanceBudget,
    PipelineContract,
    PipelineKind,
    PipelineMetrics,
    PipelineRunContext,
)
from .etf_support import (
    ETF_TARGET_DATE_POLICY,
    FUND_DAILY_MAX_ROWS,
    FUND_DAILY_FIELDS,
    calendar_input,
    execution_policy,
    external_input,
    idempotency,
    normalize_fund_daily,
    non_empty_completion,
    output,
    replace_target_date,
    transaction,
)
from .registry import register_pipeline


def execute_etf_daily_update(context: PipelineRunContext) -> BusinessExecution:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("TARGET_DATE_REQUIRED")
    started = time.monotonic()
    try:
        context.execution_control.check()
        client = get_tushare_pro(settings=context.settings, execution_control=context.execution_control)
        raw = client.fund_daily(trade_date=target.strftime("%Y%m%d"))
        context.execution_control.check()
        if isinstance(raw, pd.DataFrame) and len(raw) >= FUND_DAILY_MAX_ROWS:
            raise ContractError(
                "ETF_DAILY_API_LIMIT_REACHED",
                "fund_daily returned the provider row limit; completeness cannot be proven without pagination",
            )
        normalized = normalize_fund_daily(raw, target)
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("ETF_DAILY_API_FAILED", type(exc).__name__) from exc

    fetched_at = time.monotonic()
    try:
        rows_written, database_seconds = replace_target_date(context, ETF_DAILY, normalized, target)
    except ExecutionControlError:
        raise
    except Exception as exc:
        raise ContractError("ETF_DAILY_WRITE_FAILED", type(exc).__name__) from exc

    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=len(raw),
            rows_written=rows_written,
            assets_processed=int(normalized[TICKER].nunique()),
            dates_processed=1,
            database_write_seconds=database_seconds,
            stage_durations_seconds={
                "fetch_and_normalize": fetched_at - started,
                "database_write": database_seconds,
            },
            api_requests=1,
            batches=1,
        ),
        outputs=(
            OutputResult(
                output_id=ETF_DAILY.name,
                rows_written=rows_written,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={
                    "target_date": target.isoformat(),
                    "provider": "tushare.fund_daily",
                    "provider_row_limit": 5000,
                    "volume_unit": "shares",
                    "amount_unit": "CNY",
                },
            ),
        ),
    )


ETF_DAILY_UPDATE = register_pipeline(
    PipelineContract(
        pipeline_id="etf_daily_update",
        name="ETF daily market data",
        description=(
            "Fetches one target trading date from Tushare fund_daily and atomically replaces the complete "
            "etf_daily snapshot. Provider vol/amount are normalized from hands/thousand CNY to shares/CNY. "
            "The endpoint is limited to 5,000 rows per request; a response at that limit is rejected as "
            "unprovable completeness because fund_daily exposes no pagination parameters."
        ),
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_etf_daily_update,
        target_date_policy=ETF_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            calendar_input(),
            # The provider schema is repeated in the Contract so the API
            # boundary remains explicit and machine-readable.
            external_input(
                "tushare_fund_daily",
                "tushare.pro.fund_daily(trade_date=YYYYMMDD)",
                FUND_DAILY_FIELDS,
            ),
        ),
        outputs=(
            output(
                ETF_DAILY.name,
                ETF_DAILY,
                non_empty_completion(ETF_DAILY.name, "ETF_DAILY_COMPLETION_MISSING"),
            ),
        ),
        dependencies=(),
        resource_locks=("quant_db_writer",),
        idempotency=idempotency(
            ETF_DAILY.name,
            repeat_run_semantics="same target date deletes then replaces the validated ETF snapshot in one transaction",
            recovery="rerun the same target date; a failed fetch or transaction leaves the prior committed snapshot intact",
        ),
        transaction=transaction(ETF_DAILY.name),
        execution=execution_policy(),
        performance=PerformanceBudget(
            normal_budget_seconds=120.0,
            warning_threshold_seconds=60.0,
            hard_timeout_seconds=300,
            benchmark_scope="one fund_daily target-date response up to 5,000 rows and one temporary-DuckDB target-date replacement",
            baseline_source="internal:etf_daily_update_v1; offline temporary-DuckDB acceptance tests",
        ),
    )
)


ETF_DAILY_CONTRACTS: tuple[PipelineContract, ...] = (ETF_DAILY_UPDATE,)
