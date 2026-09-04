"""Formal Pipeline contract for M6 Market Sentiment observation production."""

from __future__ import annotations

import time
from datetime import date
from zoneinfo import ZoneInfo

import duckdb

from qrp_atlas.contracts import (
    DAILY_MARKET_SNAPSHOT,
    MARKET_M6_OBSERVATION_TABLE,
    MARKET_SCOPES,
    STOCK_INFO,
    SUSPEND_D,
    TRADING_CALENDAR,
)
from qrp_atlas.orchestration.models import OverlapPolicy

from .contracts import (
    BusinessExecution,
    CheckResult,
    CompletionContract,
    ContractError,
    DiagnosticLevel,
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
    PipelineDiagnostic,
    PipelineKind,
    PipelineMetrics,
    PipelineRunContext,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .market_m6.service import MarketM6PipelineService
from .registry import register_pipeline

CHINA_TZ = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"

REQUIRED_INPUT_TABLES = (
    DAILY_MARKET_SNAPSHOT.name,
    STOCK_INFO.name,
    SUSPEND_D.name,
    TRADING_CALENDAR.name,
)

MARKET_M6_READS = (
    f"duckdb://quant_db#{DAILY_MARKET_SNAPSHOT.name}",
    f"duckdb://quant_db#{STOCK_INFO.name}",
    f"duckdb://quant_db#{SUSPEND_D.name}",
    f"duckdb://quant_db#{TRADING_CALENDAR.name}",
)


def _target_date(invocation) -> TargetWindow:
    if invocation.scheduled_for.tzinfo is None:
        raise ContractError("SCHEDULE_TIMEZONE_MISSING")
    return TargetWindow.for_date(invocation.scheduled_for.astimezone(CHINA_TZ).date())


def _validate_target_date(target_date: date, _invocation) -> bool:
    return isinstance(target_date, date)


MARKET_M6_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="market_m6_scheduled_shanghai_date_v1",
    description="Uses explicit target date or scheduled Asia/Shanghai trading date.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_target_date,
    validate_explicit_date=_validate_target_date,
)


def _target(context: PipelineRunContext) -> date:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("MARKET_M6_TARGET_DATE_MISSING")
    return target


def _check_source_structure(context: PipelineRunContext) -> CheckResult:
    try:
        con = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            tables = {
                t[0]
                for t in con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                ).fetchall()
            }
            missing = [t for t in REQUIRED_INPUT_TABLES if t not in tables]
            if missing:
                return CheckResult.failure(
                    "market_m6_source_structure",
                    "MARKET_M6_INPUT_TABLES_MISSING",
                    f"Missing tables in quant_db: {missing}",
                )
            return CheckResult.success(
                "market_m6_source_structure",
                required_tables=list(REQUIRED_INPUT_TABLES),
            )
        finally:
            con.close()
    except Exception as exc:
        return CheckResult.failure(
            "market_m6_source_structure",
            "MARKET_M6_SOURCE_STRUCTURE_MISSING",
            "quant.db inputs could not be inspected",
            exception=type(exc).__name__,
        )


def _check_source_freshness(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    try:
        con = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            cal_row = con.execute(
                "SELECT is_open FROM trading_calendar WHERE trade_date = ?",
                [target],
            ).fetchone()
            if not cal_row:
                return CheckResult.failure(
                    "market_m6_source_freshness",
                    "MARKET_M6_INPUTS_STALE",
                    f"trading_calendar missing target date {target}",
                )
            is_open = bool(cal_row[0])
            if not is_open:
                return CheckResult.success(
                    "market_m6_source_freshness",
                    target_date=target.isoformat(),
                    is_open=False,
                )

            dms_cnt = int(
                con.execute(
                    "SELECT COUNT(*) FROM daily_market_snapshot WHERE trade_date = ?",
                    [target],
                ).fetchone()[0]
            )
            if dms_cnt == 0:
                return CheckResult.failure(
                    "market_m6_source_freshness",
                    "MARKET_M6_INPUTS_STALE",
                    f"daily_market_snapshot missing target date {target}",
                )
        finally:
            con.close()
    except Exception as exc:
        return CheckResult.failure(
            "market_m6_source_freshness",
            "MARKET_M6_INPUTS_STALE",
            f"Failed to check source freshness: {exc}",
            exception=type(exc).__name__,
        )
    return CheckResult.success(
        "market_m6_source_freshness",
        target_date=target.isoformat(),
        is_open=True,
    )


def _source_input() -> InputContract:
    return InputContract(
        input_id="market_m6_source_facts",
        kind=InputKind.TABLE,
        source="quant.db tables (daily_market_snapshot, stock_info, suspend_d, trading_calendar)",
        required_fields=tuple(REQUIRED_INPUT_TABLES),
        target_date_semantics="canonical daily facts up to target date",
        missing_error_code="MARKET_M6_INPUTS_UNAVAILABLE",
        structure_check=_check_source_structure,
        freshness=FreshnessContract(
            check_id="market_m6_source_freshness",
            target_date_semantics="all source facts cover target trading date",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
            error_code="MARKET_M6_INPUTS_STALE",
            checker=_check_source_freshness,
        ),
    )


def _check_m6_completion(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    try:
        con = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            rows = con.execute(
                f"""
                SELECT market_scope
                FROM {MARKET_M6_OBSERVATION_TABLE}
                WHERE trade_date = ?
                """,
                [target],
            ).fetchall()
            scopes = {r[0] for r in rows}
            expected_scopes = set(MARKET_SCOPES)
            if scopes != expected_scopes:
                return CheckResult.failure(
                    "market_m6_completion",
                    "MARKET_M6_SCOPES_INCOMPLETE",
                    f"Expected scopes {expected_scopes}, got {scopes}",
                )
            return CheckResult.success(
                "market_m6_completion",
                target_date=target.isoformat(),
                row_count=len(rows),
            )
        finally:
            con.close()
    except Exception as exc:
        return CheckResult.failure(
            "market_m6_completion",
            "MARKET_M6_COMPLETION_CHECK_FAILED",
            str(exc),
            exception=type(exc).__name__,
        )


def _check_m6_uniqueness(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    try:
        con = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            cnt = int(
                con.execute(
                    f"""
                    SELECT count(*) - count(DISTINCT market_scope)
                    FROM {MARKET_M6_OBSERVATION_TABLE}
                    WHERE trade_date = ?
                    """,
                    [target],
                ).fetchone()[0]
            )
            if cnt > 0:
                return CheckResult.failure(
                    "market_m6_uniqueness",
                    "MARKET_M6_DUPLICATE_KEYS",
                    f"Duplicate (trade_date, market_scope) found: {cnt}",
                )
            return CheckResult.success("market_m6_uniqueness", target_date=target.isoformat(), duplicates=0)
        finally:
            con.close()
    except Exception as exc:
        return CheckResult.failure(
            "market_m6_uniqueness",
            "MARKET_M6_UNIQUENESS_FAILED",
            str(exc),
            exception=type(exc).__name__,
        )


def _m6_output() -> OutputContract:
    return OutputContract(
        output_id="market_m6_observation",
        physical_resource=QUANT_DB_RESOURCE,
        location="settings.paths.duckdb_path",
        object_name=MARKET_M6_OBSERVATION_TABLE,
        unique_key=("trade_date", "market_scope"),
        write_mode=WriteMode.REPLACE_TARGET_DATE,
        target_date_semantics="target date M6 market sentiment observations atomically calculated and replaced",
        completion=CompletionContract(
            marker=f"table:{MARKET_M6_OBSERVATION_TABLE}",
            error_code="MARKET_M6_OUTPUT_EMPTY",
            checker=_check_m6_completion,
        ),
        quality_checks=(_check_m6_uniqueness,),
        allow_empty=False,
    )


def execute_market_m6_production(context: PipelineRunContext) -> BusinessExecution:
    start_time = time.monotonic()
    target = _target(context)
    run_id = context.run_id or context.invocation_id

    con = duckdb.connect(str(context.settings.paths.duckdb_path))
    try:
        service = MarketM6PipelineService(con)
        observations_df = service.run_m6_daily(
            trade_date=target,
            production_run_id=run_id,
            execution_control=context.execution_control,
        )
        total_rows_written = len(observations_df)
    finally:
        con.close()

    elapsed = time.monotonic() - start_time
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=0,
            rows_written=total_rows_written,
            assets_processed=total_rows_written,
            dates_processed=1,
            database_write_seconds=elapsed,
            stage_durations_seconds={"production": elapsed},
        ),
        outputs=(
            OutputResult(
                output_id="market_m6_observation",
                rows_written=total_rows_written,
                location=str(context.settings.paths.duckdb_path),
                completed=True,
                detail={"target_date": target.isoformat(), "scopes": list(MARKET_SCOPES)},
            ),
        ),
        diagnostics=(
            PipelineDiagnostic(code="INFO", level=DiagnosticLevel.INFO, message=f"Run ID: {run_id}"),
            PipelineDiagnostic(code="INFO", level=DiagnosticLevel.INFO, message=f"Target Date: {target.isoformat()}"),
        ),
    )


MARKET_M6_PRODUCTION_CONTRACT = register_pipeline(
    PipelineContract(
        pipeline_id="market_m6_production",
        name="Market Sentiment M6 Observation Production",
        description="Atomically derives and replaces daily M6 Market Sentiment observations for all 5 canonical market scopes.",
        contract_version="0.1.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_market_m6_production,
        target_date_policy=MARKET_M6_TARGET_DATE_POLICY,
        parameters=(
            ParameterContract(
                name="target_date",
                parameter_type=ParameterType.DATE,
                description="Explicit single target trading date override.",
                required=False,
            ),
        ),
        inputs=(_source_input(),),
        outputs=(_m6_output(),),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=MARKET_M6_READS,
        idempotency=IdempotencyContract(
            idempotency_key="market_m6_observation.trade_date,market_scope",
            repeat_run_semantics="same target date replaces existing target date observations for all 5 scopes",
            existing_target_handling="atomic single transaction replaces target date rows",
            failure_recovery="rerun the target date after underlying market data or stock info is corrected",
            uses_staging=False,
            atomic_replace_boundary="single DuckDB transaction across market_m6_observation table",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="single DuckDB transaction wrapping daily rebuild of M6 observations",
            failure_visibility="on error rollback leaves prior state unmodified",
        ),
        execution=ExecutionPolicy(
            overlap_policy=OverlapPolicy.FORBID,
            max_retries=0,
        ),
        performance=PerformanceBudget(
            normal_budget_seconds=60.0,
            warning_threshold_seconds=40.0,
            hard_timeout_seconds=120,
            benchmark_scope="all 5 market scopes daily M6 production",
            baseline_source="docs/QRP产品蓝图v1.1/task04/Task04-C M6 市场情绪完整事实能力设计书 v0.1.md",
        ),
        manual_execution_allowed=True,
    )
)
