"""Formal Tushare Contract for the ETF daily adjustment factors."""

from __future__ import annotations

import time

from qrp_atlas.config.tushare_client import get_tushare_pro
from qrp_atlas.contracts import ETF_ADJ_FACTOR, ETF_DAILY, TICKER
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
    FUND_ADJ_FIELDS,
    calendar_input,
    execution_policy,
    expected_etf_codes,
    external_input,
    fetch_fund_adj_pages,
    idempotency,
    normalize_fund_adj,
    non_empty_completion,
    output,
    replace_target_date,
    table_target_input,
    transaction,
)
from .registry import register_pipeline


def execute_etf_adj_factor_update(context: PipelineRunContext) -> BusinessExecution:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("TARGET_DATE_REQUIRED")
    started = time.monotonic()
    try:
        context.execution_control.check()
        expected = expected_etf_codes(context, target)
        client = get_tushare_pro(settings=context.settings, execution_control=context.execution_control)
        raw, api_requests = fetch_fund_adj_pages(client, target, context.execution_control)
        normalized = normalize_fund_adj(raw, target)
        received = set(normalized[TICKER])
        missing = sorted(expected - received)
        if missing:
            raise ContractError(
                "ETF_ADJ_FACTOR_API_PARTIAL",
                f"missing {len(missing)} ETF factors for the target-date ETF snapshot",
            )
        context.execution_control.check()
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("ETF_ADJ_FACTOR_API_FAILED", type(exc).__name__) from exc

    fetched_at = time.monotonic()
    try:
        rows_written, database_seconds = replace_target_date(context, ETF_ADJ_FACTOR, normalized, target)
    except ExecutionControlError:
        raise
    except Exception as exc:
        raise ContractError("ETF_ADJ_FACTOR_WRITE_FAILED", type(exc).__name__) from exc

    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=len(normalized),
            rows_written=rows_written,
            assets_processed=int(normalized[TICKER].nunique()),
            dates_processed=1,
            database_write_seconds=database_seconds,
            stage_durations_seconds={
                "fetch_and_normalize": fetched_at - started,
                "database_write": database_seconds,
            },
            api_requests=api_requests,
            batches=api_requests,
        ),
        outputs=(
            OutputResult(
                output_id=ETF_ADJ_FACTOR.name,
                rows_written=rows_written,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={
                    "target_date": target.isoformat(),
                    "provider": "tushare.fund_adj",
                    "provider_page_size": 2000,
                    "expected_etf_codes": len(expected),
                    "received_factor_codes": len(received),
                },
            ),
        ),
    )


ETF_ADJ_FACTOR_UPDATE = register_pipeline(
    PipelineContract(
        pipeline_id="etf_adj_factor_update",
        name="ETF daily adjustment factors",
        description=(
            "Fetches the complete target-date ETF adjustment-factor response from Tushare fund_adj, follows "
            "the documented 2,000-row offset/limit pagination until a short page, verifies coverage of the "
            "etf_daily target snapshot, and atomically replaces etf_adj_factor for that date."
        ),
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_etf_adj_factor_update,
        target_date_policy=ETF_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            calendar_input(),
            table_target_input(
                input_id="etf_daily_update_output",
                pipeline_id="etf_daily_update",
                table=ETF_DAILY,
                required_fields=("trade_date", "ticker", "close"),
            ),
            external_input(
                "tushare_fund_adj",
                "tushare.pro.fund_adj(trade_date=YYYYMMDD, offset=..., limit=...)",
                FUND_ADJ_FIELDS,
            ),
        ),
        outputs=(
            output(
                ETF_ADJ_FACTOR.name,
                ETF_ADJ_FACTOR,
                non_empty_completion(ETF_ADJ_FACTOR.name, "ETF_ADJ_FACTOR_COMPLETION_MISSING"),
            ),
        ),
        dependencies=("etf_daily_update",),
        resource_locks=("quant_db_writer",),
        idempotency=idempotency(
            ETF_ADJ_FACTOR.name,
            repeat_run_semantics="same target date deletes then replaces all validated ETF factor rows in one transaction",
            recovery="rerun after the complete paginated provider response is available; failed writes preserve the prior target snapshot",
        ),
        transaction=transaction(ETF_ADJ_FACTOR.name),
        execution=execution_policy(),
        performance=PerformanceBudget(
            normal_budget_seconds=120.0,
            warning_threshold_seconds=60.0,
            hard_timeout_seconds=300,
            benchmark_scope="one fund_adj target-date response with 2,000-row pagination and one temporary-DuckDB target-date replacement",
            baseline_source="internal:etf_adj_factor_update_v1; offline temporary-DuckDB acceptance tests",
        ),
    )
)


ETF_ADJ_FACTOR_CONTRACTS: tuple[PipelineContract, ...] = (ETF_ADJ_FACTOR_UPDATE,)
