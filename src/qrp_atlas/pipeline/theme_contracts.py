"""Formal Pipeline contract for Theme Custom Index and M4 Observation production."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb

from qrp_atlas.contracts import (
    DAILY_MARKET_SNAPSHOT,
    STOCK_INFO,
    SUSPEND_D,
    THS_DAILY,
    TRADING_CALENDAR,
)
from qrp_atlas.contracts.m4 import (
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_M4_OBSERVATION_TABLE,
)
from qrp_atlas.contracts.stock_collection import (
    STOCK_COLLECTION_TABLE,
    THEME_MEMBERSHIP_HISTORY_TABLE,
    THEME_TABLE,
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
    PipelineInvocation,
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
from .theme.service import ThemePipelineService

CHINA_TZ = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"

REQUIRED_INPUT_TABLES = (
    DAILY_MARKET_SNAPSHOT.name,
    STOCK_INFO.name,
    SUSPEND_D.name,
    THS_DAILY.name,
    TRADING_CALENDAR.name,
    STOCK_COLLECTION_TABLE,
    THEME_TABLE,
    THEME_MEMBERSHIP_HISTORY_TABLE,
)

THEME_M4_READS = (
    f"duckdb://quant_db#{DAILY_MARKET_SNAPSHOT.name}",
    f"duckdb://quant_db#{STOCK_INFO.name}",
    f"duckdb://quant_db#{SUSPEND_D.name}",
    f"duckdb://quant_db#{THS_DAILY.name}",
    f"duckdb://quant_db#{TRADING_CALENDAR.name}",
    f"duckdb://quant_db#{STOCK_COLLECTION_TABLE}",
    f"duckdb://quant_db#{THEME_TABLE}",
    f"duckdb://quant_db#{THEME_MEMBERSHIP_HISTORY_TABLE}",
)


def _target_date(invocation: PipelineInvocation) -> TargetWindow:
    if invocation.scheduled_for.tzinfo is None:
        raise ContractError("SCHEDULE_TIMEZONE_MISSING")
    return TargetWindow.for_date(invocation.scheduled_for.astimezone(CHINA_TZ).date())


def _validate_target_date(target_date: date, _invocation: PipelineInvocation) -> bool:
    return isinstance(target_date, date)


THEME_M4_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="theme_m4_scheduled_shanghai_date_v1",
    description="Uses explicit target date or scheduled Asia/Shanghai trading date.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_target_date,
    validate_explicit_date=_validate_target_date,
)


def _target(context: PipelineRunContext) -> date:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("THEME_M4_TARGET_DATE_MISSING")
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
                    "theme_m4_source_structure",
                    "THEME_M4_INPUT_TABLES_MISSING",
                    f"Missing tables in quant_db: {missing}",
                )
            return CheckResult.success(
                "theme_m4_source_structure",
                required_tables=list(REQUIRED_INPUT_TABLES),
            )
        finally:
            con.close()
    except Exception as exc:
        return CheckResult.failure(
            "theme_m4_source_structure",
            "THEME_M4_SOURCE_STRUCTURE_MISSING",
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
                    "theme_m4_source_freshness",
                    "THEME_M4_INPUTS_STALE",
                    f"trading_calendar missing target date {target}",
                )
            is_open = bool(cal_row[0])
            if not is_open:
                return CheckResult.success("theme_m4_source_freshness", target_date=target.isoformat(), is_open=False)

            dms_cnt = int(con.execute("SELECT COUNT(*) FROM daily_market_snapshot WHERE trade_date = ?", [target]).fetchone()[0])
            if dms_cnt == 0:
                return CheckResult.failure(
                    "theme_m4_source_freshness",
                    "THEME_M4_INPUTS_STALE",
                    f"daily_market_snapshot missing target date {target}",
                )

            ths_cnt = int(con.execute("SELECT COUNT(*) FROM ths_daily WHERE trade_date = ?", [target]).fetchone()[0])
            if ths_cnt == 0:
                return CheckResult.failure(
                    "theme_m4_source_freshness",
                    "THEME_M4_INPUTS_STALE",
                    f"ths_daily missing target date {target}",
                )
        finally:
            con.close()
    except Exception as exc:
        return CheckResult.failure(
            "theme_m4_source_freshness",
            "THEME_M4_INPUTS_STALE",
            f"Failed to check source freshness: {exc}",
            exception=type(exc).__name__,
        )
    return CheckResult.success("theme_m4_source_freshness", target_date=target.isoformat(), is_open=True)


def _source_input() -> InputContract:
    return InputContract(
        input_id="theme_m4_source_facts",
        kind=InputKind.TABLE,
        source="quant.db tables (daily_market_snapshot, stock_info, suspend_d, ths_daily, trading_calendar, stock_collection, theme, theme_membership_history)",
        required_fields=tuple(REQUIRED_INPUT_TABLES),
        target_date_semantics="canonical daily facts up to target date",
        missing_error_code="THEME_M4_INPUTS_UNAVAILABLE",
        structure_check=_check_source_structure,
        freshness=FreshnessContract(
            check_id="theme_m4_source_freshness",
            target_date_semantics="all source facts cover target trading date",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
            error_code="THEME_M4_INPUTS_STALE",
            checker=_check_source_freshness,
        ),
    )


def _duplicate_quality(
    table_name: str,
    check_id: str,
    error_code: str,
    key: tuple[str, ...],
):
    def checker(context: PipelineRunContext) -> CheckResult:
        try:
            con = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
            try:
                key_sql = ", ".join(key)
                duplicate = int(
                    con.execute(
                        f"SELECT COUNT(*) FROM (SELECT {key_sql}, COUNT(*) AS n FROM {table_name} GROUP BY {key_sql} HAVING COUNT(*) > 1)"
                    ).fetchone()[0]
                )
            finally:
                con.close()
        except Exception as exc:
            return CheckResult.failure(
                check_id,
                error_code,
                "Theme M4 output uniqueness could not be checked",
                exception=type(exc).__name__,
            )
        if duplicate:
            return CheckResult.failure(
                check_id,
                error_code,
                "duplicate Theme M4 output keys exist",
                duplicate_groups=duplicate,
            )
        return CheckResult.success(check_id, duplicate_groups=0)

    checker.__name__ = f"{check_id}_checker"
    return checker


def _check_m4_completion(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    try:
        con = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            row = con.execute(
                f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date = ?",
                [target],
            ).fetchone()
            rows = int(row[0]) if row else 0
        finally:
            con.close()
    except Exception as exc:
        return CheckResult.failure(
            "theme_m4_completion",
            "THEME_M4_COMPLETION_MISSING",
            "M4 observation output table is not queryable",
            exception=type(exc).__name__,
        )
    return CheckResult.success("theme_m4_completion", target_date=target.isoformat(), rows=rows)


def _m4_outputs() -> tuple[OutputContract, ...]:
    return (
        OutputContract(
            output_id="theme_m4_observations",
            physical_resource=QUANT_DB_RESOURCE,
            location="settings.paths.duckdb_path",
            object_name=THEME_M4_OBSERVATION_TABLE,
            unique_key=("theme_id", "trade_date"),
            write_mode=WriteMode.REPLACE_TARGET_DATE,
            target_date_semantics="target date M4 observations atomically calculated and replaced",
            completion=CompletionContract(
                marker=f"table:{THEME_M4_OBSERVATION_TABLE}",
                error_code="THEME_M4_OUTPUT_EMPTY",
                checker=_check_m4_completion,
            ),
            quality_checks=(
                _duplicate_quality(
                    THEME_M4_OBSERVATION_TABLE,
                    "theme_m4_observation_duplicate_quality",
                    "THEME_M4_DUPLICATE_KEYS",
                    ("theme_id", "trade_date"),
                ),
            ),
            allow_empty=True,
        ),
    )


def _execute_theme_m4(context: PipelineRunContext) -> BusinessExecution:
    target = _target(context)
    kd_param = context.parameter_overrides.get("knowledge_date") if context.parameter_overrides else None
    if kd_param:
        if isinstance(kd_param, str):
            knowledge_date = date.fromisoformat(kd_param)
        elif isinstance(kd_param, date):
            knowledge_date = kd_param
        else:
            knowledge_date = target
    else:
        knowledge_date = target

    con = duckdb.connect(str(context.settings.paths.duckdb_path))
    try:
        service = ThemePipelineService(con)
        report = service.run_m4_daily(
            trade_date=target,
            knowledge_date=knowledge_date,
            execution_control=context.execution_control,
        )
        return BusinessExecution.success(
            metrics=PipelineMetrics(
                rows_read=report.total_index_rows,
                rows_written=report.total_observation_rows,
                database_write_seconds=report.execution_seconds,
            ),
            outputs=(
                OutputResult(
                    output_id="theme_m4_observations",
                    rows_written=report.total_observation_rows,
                    location="settings.paths.duckdb_path",
                    completed=True,
                ),
            ),
            diagnostics=(
                PipelineDiagnostic(code="INFO", level=DiagnosticLevel.INFO, message=f"Theme count: {report.theme_count}"),
                PipelineDiagnostic(code="INFO", level=DiagnosticLevel.INFO, message=f"Run ID: {report.production_run_id}"),
                PipelineDiagnostic(code="INFO", level=DiagnosticLevel.INFO, message=f"Input Snapshot ID: {report.input_snapshot_id}"),
                PipelineDiagnostic(code="INFO", level=DiagnosticLevel.INFO, message=f"Knowledge date: {knowledge_date.isoformat()}"),
            ),
        )
    except Exception as exc:
        raise ContractError("THEME_M4_EXECUTION_FAILED", str(exc)) from exc
    finally:
        con.close()


THEME_M4_PRODUCTION_CONTRACT = register_pipeline(
    PipelineContract(
        pipeline_id="theme_m4_production",
        name="Theme Custom Index and M4 Observation Production",
        description="Produces Theme custom indices, trend states, episodes, and M4 observations.",
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_theme_m4,
        target_date_policy=THEME_M4_TARGET_DATE_POLICY,
        parameters=(
            ParameterContract(
                name="target_date",
                parameter_type=ParameterType.DATE,
                description="Explicit target date for daily calculation",
                required=False,
            ),
            ParameterContract(
                name="knowledge_date",
                parameter_type=ParameterType.DATE,
                description="As-of knowledge date for PIT query cutoff",
                required=False,
            ),
        ),
        inputs=(_source_input(),),
        outputs=_m4_outputs(),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=THEME_M4_READS,
        idempotency=IdempotencyContract(
            idempotency_key="theme_m4_observation.theme_id,trade_date",
            repeat_run_semantics="same target date replaces existing target date observation and preserves continuous indices",
            existing_target_handling="atomic single transaction replaces or inserts target date rows",
            failure_recovery="rerun the target date after underlying market data or collection is corrected",
            uses_staging=False,
            atomic_replace_boundary="single DuckDB transaction across all theme custom index and observation tables",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="single DuckDB transaction wrapping daily rebuild of index, state, episode and m4 observations",
            failure_visibility="on error rollback leaves prior state unmodified",
        ),
        execution=ExecutionPolicy(
            overlap_policy=OverlapPolicy.FORBID,
            max_retries=0,
        ),
        performance=PerformanceBudget(
            normal_budget_seconds=180.0,
            warning_threshold_seconds=120.0,
            hard_timeout_seconds=300,
            benchmark_scope="all active canonical themes daily M4 production",
            baseline_source="theme custom index and m4 fact production",
        ),
    )
)
