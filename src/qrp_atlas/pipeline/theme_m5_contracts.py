"""Formal Pipeline contract for Task04-B2 Theme M5 popularity observations."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import duckdb

from qrp_atlas.contracts import (
    DC_HOT,
    THS_HOT,
    THEME_M5_OBSERVATION_TABLE,
    THEME_M5_OBSERVATION_VERSION,
    THEME_ID,
    COLLECTION_ID,
    TRADE_DATE,
    THEME_HOT_LIST_APPEARANCE_COUNT,
    THEME_HOT_SOURCE_COUNT,
    THEME_HOT_STOCK_COUNT,
    THEME_HOT_STOCK_RATIO,
    THEME_MEMBER_COUNT,
)
from qrp_atlas.contracts.stock_collection import (
    STOCK_COLLECTION_TABLE,
    THEME_MEMBERSHIP_HISTORY_TABLE,
    THEME_TABLE,
)
from qrp_atlas.orchestration.execution_control import ExecutionControlError
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
from .theme.m5_service import (
    DC_HOT_LIST,
    DC_HOT_SOURCE,
    THS_HOT_LIST,
    THS_HOT_SOURCE,
    ThemeM5PipelineError,
    ThemeM5PipelineService,
    read_complete_popularity_source,
)
from qrp_atlas.stock_collections.resolver import StockCollectionResolver


CHINA_TZ = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"

M5_THEME_INPUT_TABLES = (
    STOCK_COLLECTION_TABLE,
    THEME_TABLE,
    THEME_MEMBERSHIP_HISTORY_TABLE,
)
M5_READS = (
    f"duckdb://quant_db#{DC_HOT.name}",
    f"duckdb://quant_db#{THS_HOT.name}",
    *(f"duckdb://quant_db#{table}" for table in M5_THEME_INPUT_TABLES),
)


def _target_date(invocation: PipelineInvocation) -> TargetWindow:
    if invocation.scheduled_for.tzinfo is None:
        raise ContractError("SCHEDULE_TIMEZONE_MISSING")
    return TargetWindow.for_date(invocation.scheduled_for.astimezone(CHINA_TZ).date())


def _validate_target_date(target_date: date, _invocation: PipelineInvocation) -> bool:
    return isinstance(target_date, date) and not isinstance(target_date, datetime)


THEME_M5_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="theme_m5_scheduled_shanghai_date_v1",
    description="Uses an explicit target date or the scheduled Asia/Shanghai calendar date.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_target_date,
    validate_explicit_date=_validate_target_date,
)


def _target(context: PipelineRunContext) -> date:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("THEME_M5_TARGET_DATE_MISSING")
    return target


def _inspect_tables(
    context: PipelineRunContext,
    *,
    check_id: str,
    required_tables: tuple[str, ...],
    required_columns: dict[str, tuple[str, ...]],
    error_code: str,
) -> CheckResult:
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        missing_tables = [table for table in required_tables if table not in tables]
        if missing_tables:
            return CheckResult.failure(check_id, error_code, f"missing tables: {missing_tables}")
        missing_columns: dict[str, list[str]] = {}
        for table, columns in required_columns.items():
            actual = {
                row[0]
                for row in connection.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ?",
                    [table],
                ).fetchall()
            }
            missing = sorted(set(columns) - actual)
            if missing:
                missing_columns[table] = missing
        if missing_columns:
            return CheckResult.failure(check_id, error_code, f"missing columns: {missing_columns}")
        return CheckResult.success(check_id, required_tables=list(required_tables))
    except Exception as exc:
        return CheckResult.failure(check_id, error_code, "quant_db inputs could not be inspected", exception=type(exc).__name__)
    finally:
        if connection is not None:
            connection.close()


def _check_dc_hot_structure(context: PipelineRunContext) -> CheckResult:
    required = tuple(column for column in DC_HOT.column_names() if column != "created_at")
    return _inspect_tables(
        context,
        check_id="theme_m5_dc_hot_structure",
        required_tables=(DC_HOT.name,),
        required_columns={DC_HOT.name: required},
        error_code="THEME_M5_DC_HOT_INPUT_UNAVAILABLE",
    )


def _check_ths_hot_structure(context: PipelineRunContext) -> CheckResult:
    required = tuple(column for column in THS_HOT.column_names() if column != "created_at")
    return _inspect_tables(
        context,
        check_id="theme_m5_ths_hot_structure",
        required_tables=(THS_HOT.name,),
        required_columns={THS_HOT.name: required},
        error_code="THEME_M5_THS_HOT_INPUT_UNAVAILABLE",
    )


def _check_theme_structure(context: PipelineRunContext) -> CheckResult:
    required_columns = {
        STOCK_COLLECTION_TABLE: (
            "collection_id",
            "collection_type",
            "collection_scope",
            "status",
            "effective_from",
            "effective_to",
            "available_trade_date",
            "ingested_at",
        ),
        THEME_TABLE: (
            "theme_id",
            "collection_id",
            "status",
            "effective_from",
            "effective_to",
            "available_trade_date",
            "ingested_at",
        ),
        THEME_MEMBERSHIP_HISTORY_TABLE: (
            "membership_id",
            "theme_id",
            "collection_id",
            "asset_id",
            "effective_from",
            "effective_to",
            "available_trade_date",
            "ingested_at",
        ),
    }
    return _inspect_tables(
        context,
        check_id="theme_m5_theme_structure",
        required_tables=M5_THEME_INPUT_TABLES,
        required_columns=required_columns,
        error_code="THEME_M5_THEME_INPUTS_UNAVAILABLE",
    )


def _check_dc_hot_freshness(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        frame = read_complete_popularity_source(
            connection,
            table_name=DC_HOT.name,
            expected_source=DC_HOT_SOURCE,
            expected_list_name=DC_HOT_LIST,
            trade_date=target,
            error_code="THEME_M5_DC_HOT_INPUT_INCOMPLETE",
        )
        return CheckResult.success(
            "theme_m5_dc_hot_freshness",
            target_date=target.isoformat(),
            rows=len(frame),
            snapshots=int(frame["snapshot_seq"].nunique()),
        )
    except ThemeM5PipelineError as exc:
        return CheckResult.failure("theme_m5_dc_hot_freshness", exc.code, exc.detail)
    except Exception as exc:
        return CheckResult.failure(
            "theme_m5_dc_hot_freshness",
            "THEME_M5_DC_HOT_INPUT_INCOMPLETE",
            "dc_hot completeness could not be verified",
            exception=type(exc).__name__,
        )
    finally:
        if connection is not None:
            connection.close()


def _check_ths_hot_freshness(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        frame = read_complete_popularity_source(
            connection,
            table_name=THS_HOT.name,
            expected_source=THS_HOT_SOURCE,
            expected_list_name=THS_HOT_LIST,
            trade_date=target,
            error_code="THEME_M5_THS_HOT_INPUT_INCOMPLETE",
        )
        return CheckResult.success(
            "theme_m5_ths_hot_freshness",
            target_date=target.isoformat(),
            rows=len(frame),
            snapshots=int(frame["snapshot_seq"].nunique()),
        )
    except ThemeM5PipelineError as exc:
        return CheckResult.failure("theme_m5_ths_hot_freshness", exc.code, exc.detail)
    except Exception as exc:
        return CheckResult.failure(
            "theme_m5_ths_hot_freshness",
            "THEME_M5_THS_HOT_INPUT_INCOMPLETE",
            "ths_hot completeness could not be verified",
            exception=type(exc).__name__,
        )
    finally:
        if connection is not None:
            connection.close()


def _check_theme_freshness(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        themes = StockCollectionResolver(connection).resolve_active_themes(
            target,
            allowed_scopes=("CANONICAL",),
            enforce_admission_cutoff=True,
        )
        if themes.empty:
            return CheckResult.failure(
                "theme_m5_theme_freshness",
                "THEME_M5_NO_ACTIVE_THEMES",
                f"no active canonical themes as of {target.isoformat()}",
            )
        return CheckResult.success(
            "theme_m5_theme_freshness",
            target_date=target.isoformat(),
            active_theme_count=len(themes),
        )
    except Exception as exc:
        return CheckResult.failure(
            "theme_m5_theme_freshness",
            "THEME_M5_THEME_INPUTS_STALE",
            "PIT Theme universe could not be resolved",
            exception=type(exc).__name__,
        )
    finally:
        if connection is not None:
            connection.close()


def _duplicate_quality(check_id: str) -> object:
    def checker(context: PipelineRunContext) -> CheckResult:
        target = _target(context)
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
            duplicate_groups = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT theme_id, trade_date, COUNT(*) AS row_count
                        FROM {THEME_M5_OBSERVATION_TABLE}
                        WHERE trade_date = ?
                        GROUP BY theme_id, trade_date
                        HAVING COUNT(*) > 1
                    )
                    """,
                    [target],
                ).fetchone()[0]
            )
        except Exception as exc:
            return CheckResult.failure(check_id, "THEME_M5_DUPLICATE_KEYS", "M5 output uniqueness could not be checked", exception=type(exc).__name__)
        finally:
            if connection is not None:
                connection.close()
        if duplicate_groups:
            return CheckResult.failure(check_id, "THEME_M5_DUPLICATE_KEYS", "duplicate M5 output keys exist", duplicate_groups=duplicate_groups)
        return CheckResult.success(check_id, duplicate_groups=0, target_date=target.isoformat())

    checker.__name__ = f"{check_id}_checker"
    return checker


def _metric_quality(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        invalid = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM {THEME_M5_OBSERVATION_TABLE}
                WHERE trade_date = ?
                  AND (
                      {THEME_MEMBER_COUNT} < 0
                      OR {THEME_HOT_STOCK_COUNT} < 0
                      OR {THEME_HOT_LIST_APPEARANCE_COUNT} < 0
                      OR {THEME_HOT_SOURCE_COUNT} < 0
                      OR {THEME_HOT_STOCK_COUNT} > {THEME_MEMBER_COUNT}
                      OR {THEME_HOT_SOURCE_COUNT} > 2
                      OR ({THEME_MEMBER_COUNT} = 0 AND {THEME_HOT_STOCK_RATIO} IS NOT NULL)
                      OR ({THEME_MEMBER_COUNT} > 0 AND (
                          {THEME_HOT_STOCK_RATIO} IS NULL OR
                          {THEME_HOT_STOCK_RATIO} < 0 OR {THEME_HOT_STOCK_RATIO} > 1
                      ))
                      OR calculation_version <> ?
                  )
                """,
                [target, THEME_M5_OBSERVATION_VERSION],
            ).fetchone()[0]
        )
    except Exception as exc:
        return CheckResult.failure("theme_m5_metric_quality", "THEME_M5_METRIC_QUALITY_FAILED", "M5 metric quality could not be checked", exception=type(exc).__name__)
    finally:
        if connection is not None:
            connection.close()
    if invalid:
        return CheckResult.failure("theme_m5_metric_quality", "THEME_M5_METRIC_QUALITY_FAILED", "invalid M5 metric values exist", invalid_rows=invalid)
    return CheckResult.success("theme_m5_metric_quality", invalid_rows=0, target_date=target.isoformat())


def _completion(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        expected = len(
            StockCollectionResolver(connection).resolve_active_themes(
                target,
                allowed_scopes=("CANONICAL",),
                enforce_admission_cutoff=True,
            )
        )
        actual = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {THEME_M5_OBSERVATION_TABLE} WHERE trade_date = ?",
                [target],
            ).fetchone()[0]
        )
    except Exception as exc:
        return CheckResult.failure("theme_m5_completion", "THEME_M5_COMPLETION_MISSING", "M5 output could not be read after commit", exception=type(exc).__name__)
    finally:
        if connection is not None:
            connection.close()
    if expected == 0 or actual != expected:
        return CheckResult.failure(
            "theme_m5_completion",
            "THEME_M5_COMPLETION_MISSING",
            "M5 output does not contain exactly one row per active Theme",
            expected_theme_count=expected,
            actual_rows=actual,
        )
    return CheckResult.success("theme_m5_completion", expected_theme_count=expected, actual_rows=actual, target_date=target.isoformat())


def _m5_inputs() -> tuple[InputContract, ...]:
    return (
        InputContract(
            input_id="dc_hot_canonical",
            kind=InputKind.UPSTREAM_PIPELINE,
            source="quant_db.dc_hot",
            required_fields=tuple(column for column in DC_HOT.column_names() if column != "created_at"),
            target_date_semantics="all complete D-day dc_hot snapshots produced by dc_hot_ingest",
            missing_error_code="THEME_M5_DC_HOT_INPUT_UNAVAILABLE",
            structure_check=_check_dc_hot_structure,
            freshness=FreshnessContract(
                check_id="theme_m5_dc_hot_freshness",
                target_date_semantics="all complete dc_hot snapshots cover target date",
                maximum_lag_trading_days=0,
                non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                error_code="THEME_M5_DC_HOT_INPUT_INCOMPLETE",
                checker=_check_dc_hot_freshness,
            ),
            upstream_pipeline_id="dc_hot_ingest",
        ),
        InputContract(
            input_id="ths_hot_canonical",
            kind=InputKind.UPSTREAM_PIPELINE,
            source="quant_db.ths_hot",
            required_fields=tuple(column for column in THS_HOT.column_names() if column != "created_at"),
            target_date_semantics="all complete D-day ths_hot snapshots produced by ths_hot_ingest",
            missing_error_code="THEME_M5_THS_HOT_INPUT_UNAVAILABLE",
            structure_check=_check_ths_hot_structure,
            freshness=FreshnessContract(
                check_id="theme_m5_ths_hot_freshness",
                target_date_semantics="all complete ths_hot snapshots cover target date",
                maximum_lag_trading_days=0,
                non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                error_code="THEME_M5_THS_HOT_INPUT_INCOMPLETE",
                checker=_check_ths_hot_freshness,
            ),
            upstream_pipeline_id="ths_hot_ingest",
        ),
        InputContract(
            input_id="theme_pit_membership",
            kind=InputKind.TABLE,
            source="quant_db.stock_collection + theme + theme_membership_history",
            required_fields=tuple(M5_THEME_INPUT_TABLES),
            target_date_semantics="D-day canonical Theme universe and valid PIT memberships",
            missing_error_code="THEME_M5_THEME_INPUTS_UNAVAILABLE",
            structure_check=_check_theme_structure,
            freshness=FreshnessContract(
                check_id="theme_m5_theme_freshness",
                target_date_semantics="active canonical Themes are visible at the D-day admission cutoff",
                maximum_lag_trading_days=0,
                non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                error_code="THEME_M5_THEME_INPUTS_STALE",
                checker=_check_theme_freshness,
            ),
        ),
    )


def _execute_theme_m5(context: PipelineRunContext) -> BusinessExecution:
    target = _target(context)
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path))
        report = ThemeM5PipelineService(connection).run_m5_daily(
            trade_date=target,
            production_run_id=context.run_id,
            execution_control=context.execution_control,
        )
        return BusinessExecution.success(
            metrics=PipelineMetrics(
                rows_read=report.total_member_rows + report.total_popularity_rows,
                rows_written=report.total_observation_rows,
                assets_processed=report.total_member_rows,
                dates_processed=1,
                database_write_seconds=report.execution_seconds,
                batches=1,
            ),
            outputs=(
                OutputResult(
                    output_id="theme_m5_observation",
                    rows_written=report.total_observation_rows,
                    location="settings.paths.duckdb_path",
                    completed=True,
                    detail={
                        "trade_date": target.isoformat(),
                        "theme_count": report.theme_count,
                        "input_snapshot_id": report.input_snapshot_id,
                    },
                ),
            ),
            diagnostics=(
                PipelineDiagnostic(
                    code="INFO",
                    level=DiagnosticLevel.INFO,
                    message=f"Theme count: {report.theme_count}",
                ),
                PipelineDiagnostic(
                    code="INFO",
                    level=DiagnosticLevel.INFO,
                    message=f"Input Snapshot ID: {report.input_snapshot_id}",
                ),
            ),
        )
    except ExecutionControlError:
        raise
    except ThemeM5PipelineError as exc:
        raise ContractError(exc.code, exc.detail) from exc
    except Exception as exc:
        raise ContractError("THEME_M5_EXECUTION_FAILED", str(exc)) from exc
    finally:
        if connection is not None:
            connection.close()


THEME_M5_PRODUCTION_CONTRACT = register_pipeline(
    PipelineContract(
        pipeline_id="theme_m5_production",
        name="Theme M5 Popularity Observation Production",
        description="Maps complete D-day dc_hot and ths_hot snapshots to PIT Theme Membership and produces five M5 facts.",
        contract_version="0.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_theme_m5,
        target_date_policy=THEME_M5_TARGET_DATE_POLICY,
        parameters=(),
        inputs=_m5_inputs(),
        outputs=(
            OutputContract(
                output_id="theme_m5_observation",
                physical_resource=QUANT_DB_RESOURCE,
                location="settings.paths.duckdb_path",
                object_name=THEME_M5_OBSERVATION_TABLE,
                unique_key=(THEME_ID, TRADE_DATE),
                write_mode=WriteMode.REPLACE_TARGET_DATE,
                target_date_semantics="one complete M5 observation per active PIT Theme on target date",
                completion=CompletionContract(
                    marker=f"table:{THEME_M5_OBSERVATION_TABLE}",
                    error_code="THEME_M5_COMPLETION_MISSING",
                    checker=_completion,
                ),
                quality_checks=(_duplicate_quality("theme_m5_duplicate_quality"), _metric_quality),
                allow_empty=False,
            ),
        ),
        dependencies=("dc_hot_ingest", "ths_hot_ingest"),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=M5_READS,
        idempotency=IdempotencyContract(
            idempotency_key="theme_m5_observation.theme_id,trade_date",
            repeat_run_semantics="same target date deterministically recalculates facts and replaces only that date",
            existing_target_handling="delete and insert the target date inside one database transaction",
            failure_recovery="rerun the target date after both B1 sources or PIT membership inputs are corrected",
            uses_staging=False,
            atomic_replace_boundary="single DuckDB transaction containing target-date replacement and complete observation insert",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="single DuckDB transaction around all theme_m5_observation target-date writes",
            failure_visibility="on error rollback leaves prior target-date observations unmodified",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=0),
        performance=PerformanceBudget(
            normal_budget_seconds=180.0,
            warning_threshold_seconds=120.0,
            hard_timeout_seconds=300,
            benchmark_scope="all active canonical Themes, both complete B1 popularity sources, one D-day M5 production",
            baseline_source="offline acceptance benchmark for set-based Theme mapping and one DuckDB target-date transaction",
        ),
        manual_execution_allowed=True,
    )
)


__all__ = ["THEME_M5_PRODUCTION_CONTRACT"]
