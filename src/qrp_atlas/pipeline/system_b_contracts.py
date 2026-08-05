"""Formal daily Contracts for the System B state, episode, and pool chain."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import duckdb

from qrp_atlas.contracts import (
    DAILY_MARKET_SNAPSHOT,
    SYSTEM_B_EPISODE,
    SYSTEM_B_EPISODE_OBSERVATION,
    SYSTEM_B_EPISODE_OBSERVATION_TABLE,
    SYSTEM_B_EPISODE_TABLE,
    SYSTEM_B_POOL_MEMBERSHIP,
    SYSTEM_B_POOL_MEMBERSHIP_TABLE,
    SYSTEM_B_POOL_RUN,
    SYSTEM_B_POOL_RUN_TABLE,
    SYSTEM_B_PRODUCTION_RUN,
    SYSTEM_B_PRODUCTION_RUN_TABLE,
    SYSTEM_B_STATE_OBSERVATION,
    SYSTEM_B_STATE_OBSERVATION_TABLE,
    TRADING_CALENDAR,
)
from qrp_atlas.indicators.system_b.pools import CAPACITY, HEIGHT, RECOGNITION
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
    ParameterContract,
    ParameterType,
    PerformanceBudget,
    PipelineContract,
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
from .system_b.repository import REQUIRED_INPUT_TABLES, SystemBProductionError, validate_source_schema
from .system_b.service import check_readiness, run_daily
from .system_b_episode.service import (
    SystemBEpisodeProductionError,
    rebuild_episodes,
)
from .system_b_pools.service import (
    SystemBPoolProductionError,
    build_stock_pool,
)


CHINA_TZ = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
EPISODE_DB_RESOURCE = "system_b_episode_db"
EPISODE_DB_WRITER = "system_b_episode_writer"
POOLS_DB_RESOURCE = "system_b_pools_db"
POOLS_DB_WRITER = "system_b_pools_writer"
QUANT_DB_LOCATION = "settings.paths.duckdb_path"
EPISODE_DB_LOCATION = "settings.paths.episode_db_path"
POOLS_DB_LOCATION = "settings.paths.pool_db_path"
STATE_READS = tuple(f"duckdb://quant_db#{table}" for table in REQUIRED_INPUT_TABLES)
STATE_OUTPUT_READS = (f"duckdb://quant_db#{SYSTEM_B_STATE_OBSERVATION_TABLE}",)
EPISODE_READS = (f"duckdb://quant_db#{SYSTEM_B_STATE_OBSERVATION_TABLE}",)
POOL_READS = (
    f"duckdb://quant_db#{SYSTEM_B_STATE_OBSERVATION_TABLE}",
    f"duckdb://quant_db#{DAILY_MARKET_SNAPSHOT.name}",
    f"duckdb://{EPISODE_DB_RESOURCE}#{SYSTEM_B_EPISODE_TABLE}",
    f"duckdb://{EPISODE_DB_RESOURCE}#{SYSTEM_B_EPISODE_OBSERVATION_TABLE}",
)


def _target_date(invocation: PipelineInvocation) -> TargetWindow:
    if invocation.scheduled_for.tzinfo is None:
        raise ContractError("SCHEDULE_TIMEZONE_MISSING")
    return TargetWindow.for_date(invocation.scheduled_for.astimezone(CHINA_TZ).date())


def _validate_target_date(target_date: date, _invocation: PipelineInvocation) -> bool:
    return isinstance(target_date, date)


SYSTEM_B_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="system_b_scheduled_shanghai_date_v1",
    description="Uses the scheduled Asia/Shanghai calendar date; closed dates complete as explicit no-ops.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_target_date,
    validate_explicit_date=_validate_target_date,
)


def _target(context: PipelineRunContext) -> date:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("SYSTEM_B_TARGET_DATE_MISSING")
    return target


def _path(settings: Any, attribute: str, error_code: str) -> Path:
    value = getattr(settings.paths, attribute, None)
    if value is None:
        raise ContractError(error_code)
    return Path(value)


def _connection(path: Path, *, read_only: bool = True) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(path), read_only=read_only)


def _table_columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {
        row[0]
        for row in connection.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table_name],
        ).fetchall()
    }


def _source_structure(context: PipelineRunContext) -> CheckResult:
    try:
        connection = _connection(Path(context.settings.paths.duckdb_path))
        try:
            validate_source_schema(connection)
        finally:
            connection.close()
    except SystemBProductionError as exc:
        return CheckResult.failure("system_b_source_structure", exc.code, exc.detail)
    except Exception as exc:
        return CheckResult.failure(
            "system_b_source_structure",
            "SYSTEM_B_SOURCE_STRUCTURE_MISSING",
            "quant.db System B inputs could not be inspected",
            exception=type(exc).__name__,
        )
    return CheckResult.success("system_b_source_structure", required_tables=list(REQUIRED_INPUT_TABLES))


def _calendar_status(context: PipelineRunContext) -> tuple[bool, date]:
    target = _target(context)
    connection = _connection(Path(context.settings.paths.duckdb_path))
    try:
        row = connection.execute(
            f"SELECT is_open FROM {TRADING_CALENDAR.name} WHERE trade_date = ?",
            [target],
        ).fetchone()
    finally:
        connection.close()
    return bool(row and row[0]), target


def _target_calendar_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        is_open, target = _calendar_status(context)
    except Exception as exc:
        return CheckResult.failure(
            "system_b_target_calendar_freshness",
            "SYSTEM_B_CALENDAR_UNAVAILABLE",
            "System B target calendar status could not be read",
            exception=type(exc).__name__,
        )
    return CheckResult.success(
        "system_b_target_calendar_freshness",
        target_date=target.isoformat(),
        is_open=is_open,
        noop=not is_open,
    )


def _target_is_open(context: PipelineRunContext) -> tuple[bool, date]:
    try:
        return _calendar_status(context)
    except Exception as exc:
        raise ContractError("SYSTEM_B_CALENDAR_UNAVAILABLE", type(exc).__name__) from exc


def _state_table_structure(context: PipelineRunContext) -> CheckResult:
    path = Path(context.settings.paths.duckdb_path)
    try:
        connection = _connection(path)
        try:
            columns = _table_columns(connection, SYSTEM_B_STATE_OBSERVATION_TABLE)
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "system_b_state_table_structure",
            "SYSTEM_B_STATE_TABLE_MISSING",
            "System B state output is not queryable",
            exception=type(exc).__name__,
        )
    required = set(SYSTEM_B_STATE_OBSERVATION.column_names())
    missing = sorted(required - columns)
    if missing:
        return CheckResult.failure(
            "system_b_state_table_structure",
            "SYSTEM_B_STATE_TABLE_MISSING",
            "System B state table is missing required columns",
            missing=missing,
        )
    return CheckResult.success("system_b_state_table_structure", table=SYSTEM_B_STATE_OBSERVATION_TABLE)


def _state_target_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        is_open, target = _target_is_open(context)
        if not is_open:
            return CheckResult.success("system_b_state_target_freshness", target_date=target.isoformat(), noop=True)
        connection = _connection(Path(context.settings.paths.duckdb_path))
        try:
            rows = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} WHERE trade_date = ?",
                    [target],
                ).fetchone()[0]
            )
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "system_b_state_target_freshness",
            "SYSTEM_B_STATE_INPUT_STALE",
            "System B state target could not be checked",
            exception=type(exc).__name__,
        )
    if rows <= 0:
        return CheckResult.failure(
            "system_b_state_target_freshness",
            "SYSTEM_B_STATE_INPUT_STALE",
            "System B state has no target-date observations",
            target_date=target.isoformat(),
        )
    return CheckResult.success("system_b_state_target_freshness", target_date=target.isoformat(), rows=rows)


def _configured_episode_structure(context: PipelineRunContext) -> CheckResult:
    try:
        path = _path(context.settings, "episode_db_path", "SYSTEM_B_EPISODE_OUTPUT_NOT_CONFIGURED")
    except ContractError as exc:
        return CheckResult.failure("system_b_episode_database_structure", exc.code, "episode database path is not configured")
    if not path.exists():
        return CheckResult.success("system_b_episode_database_structure", path=str(path), exists=False)
    try:
        connection = _connection(path)
        try:
            tables = {
                row[0]
                for row in connection.execute("SELECT table_name FROM information_schema.tables").fetchall()
            }
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "system_b_episode_database_structure",
            "SYSTEM_B_EPISODE_INPUT_UNREADABLE",
            "episode database could not be read",
            exception=type(exc).__name__,
        )
    missing = sorted({SYSTEM_B_EPISODE_TABLE, SYSTEM_B_EPISODE_OBSERVATION_TABLE} - tables)
    if missing:
        return CheckResult.failure(
            "system_b_episode_database_structure",
            "SYSTEM_B_EPISODE_INPUT_STRUCTURE_MISSING",
            "episode database is missing required tables",
            missing=missing,
        )
    return CheckResult.success("system_b_episode_database_structure", path=str(path), exists=True)


def _episode_target_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        is_open, target = _target_is_open(context)
        path = _path(context.settings, "episode_db_path", "SYSTEM_B_EPISODE_INPUT_NOT_CONFIGURED")
        if not is_open:
            return CheckResult.success("system_b_episode_target_freshness", target_date=target.isoformat(), noop=True)
        if not path.exists():
            return CheckResult.failure("system_b_episode_target_freshness", "SYSTEM_B_EPISODE_INPUT_STALE", "episode database does not exist")
        connection = _connection(path)
        try:
            row = connection.execute(
                f"SELECT COUNT(*), MAX(trade_date) FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE}"
            ).fetchone()
        finally:
            connection.close()
    except ContractError as exc:
        return CheckResult.failure("system_b_episode_target_freshness", exc.code, "episode input is not configured")
    except Exception as exc:
        return CheckResult.failure(
            "system_b_episode_target_freshness",
            "SYSTEM_B_EPISODE_INPUT_STALE",
            "episode target could not be checked",
            exception=type(exc).__name__,
        )
    if not row or int(row[0]) == 0 or row[1] is None or row[1] < target:
        return CheckResult.failure(
            "system_b_episode_target_freshness",
            "SYSTEM_B_EPISODE_INPUT_STALE",
            "episode observations do not cover the target date",
            target_date=target.isoformat(),
            latest_date=str(row[1]) if row else None,
        )
    return CheckResult.success("system_b_episode_target_freshness", target_date=target.isoformat(), latest_date=str(row[1]))


def _pool_source_structure(context: PipelineRunContext) -> CheckResult:
    path = Path(context.settings.paths.duckdb_path)
    try:
        connection = _connection(path)
        try:
            tables = {
                row[0]
                for row in connection.execute("SELECT table_name FROM information_schema.tables").fetchall()
            }
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure("system_b_pool_source_structure", "SYSTEM_B_POOL_INPUT_UNREADABLE", "quant.db could not be read", exception=type(exc).__name__)
    required = {SYSTEM_B_STATE_OBSERVATION_TABLE, DAILY_MARKET_SNAPSHOT.name}
    missing = sorted(required - tables)
    if missing:
        return CheckResult.failure("system_b_pool_source_structure", "SYSTEM_B_POOL_INPUT_STRUCTURE_MISSING", "pool input tables are missing", missing=missing)
    return CheckResult.success("system_b_pool_source_structure", tables=sorted(required))


def _pool_episode_structure(context: PipelineRunContext) -> CheckResult:
    return _configured_episode_structure(context)


def _pool_source_freshness(context: PipelineRunContext) -> CheckResult:
    return _state_target_freshness(context)


def _pool_episode_freshness(context: PipelineRunContext) -> CheckResult:
    return _episode_target_freshness(context)


def _input(
    *,
    input_id: str,
    kind: InputKind,
    source: str,
    fields: tuple[str, ...],
    structure: Callable[[PipelineRunContext], CheckResult],
    freshness: Callable[[PipelineRunContext], CheckResult],
    freshness_id: str,
    freshness_error: str,
    upstream_pipeline_id: str | None = None,
) -> InputContract:
    return InputContract(
        input_id=input_id,
        kind=kind,
        source=source,
        required_fields=fields,
        target_date_semantics="resolved System B calendar date; closed dates are explicit no-ops",
        missing_error_code=freshness_error,
        structure_check=structure,
        freshness=FreshnessContract(
            check_id=freshness_id,
            target_date_semantics="target-date source coverage is checked before calculation",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
            error_code=freshness_error,
            checker=freshness,
        ),
        upstream_pipeline_id=upstream_pipeline_id,
    )


def _state_source_input() -> InputContract:
    return _input(
        input_id="system_b_source_tables",
        kind=InputKind.TABLE,
        source="quant_db.stock_info,trading_calendar,daily_market_snapshot,adj_factor_changes,suspend_d",
        fields=tuple(REQUIRED_INPUT_TABLES),
        structure=_source_structure,
        freshness=_target_calendar_freshness,
        freshness_id="system_b_source_target_freshness",
        freshness_error="SYSTEM_B_SOURCE_STALE",
    )


def _state_table_input() -> InputContract:
    return _input(
        input_id="system_b_state_output",
        kind=InputKind.TABLE,
        source=f"quant_db.{SYSTEM_B_STATE_OBSERVATION_TABLE}",
        fields=tuple(SYSTEM_B_STATE_OBSERVATION.column_names()),
        structure=_state_table_structure,
        freshness=_state_target_freshness,
        freshness_id="system_b_state_target_freshness",
        freshness_error="SYSTEM_B_STATE_INPUT_STALE",
    )


def _episode_input() -> InputContract:
    return _input(
        input_id="system_b_episode_output",
        kind=InputKind.TABLE,
        source=f"{EPISODE_DB_RESOURCE}.{SYSTEM_B_EPISODE_TABLE},{EPISODE_DB_RESOURCE}.{SYSTEM_B_EPISODE_OBSERVATION_TABLE}",
        fields=tuple(SYSTEM_B_EPISODE_OBSERVATION.column_names()),
        structure=_configured_episode_structure,
        freshness=_episode_target_freshness,
        freshness_id="system_b_episode_target_freshness",
        freshness_error="SYSTEM_B_EPISODE_INPUT_STALE",
    )


def _pool_inputs() -> tuple[InputContract, ...]:
    return (
        _input(
            input_id="system_b_pool_quant_input",
            kind=InputKind.TABLE,
            source=f"quant_db.{SYSTEM_B_STATE_OBSERVATION_TABLE},{QUANT_DB_RESOURCE}.{DAILY_MARKET_SNAPSHOT.name}",
            fields=(SYSTEM_B_STATE_OBSERVATION_TABLE, DAILY_MARKET_SNAPSHOT.name),
            structure=_pool_source_structure,
            freshness=_pool_source_freshness,
            freshness_id="system_b_pool_quant_target_freshness",
            freshness_error="SYSTEM_B_POOL_INPUT_STALE",
        ),
        _input(
            input_id="system_b_pool_episode_input",
            kind=InputKind.TABLE,
            source=f"{EPISODE_DB_RESOURCE}.{SYSTEM_B_EPISODE_TABLE},{EPISODE_DB_RESOURCE}.{SYSTEM_B_EPISODE_OBSERVATION_TABLE}",
            fields=tuple(SYSTEM_B_EPISODE_OBSERVATION.column_names()),
            structure=_pool_episode_structure,
            freshness=_pool_episode_freshness,
            freshness_id="system_b_pool_episode_target_freshness",
            freshness_error="SYSTEM_B_EPISODE_INPUT_STALE",
        ),
    )


def _state_completion(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    try:
        connection = _connection(Path(context.settings.paths.duckdb_path))
        try:
            rows = int(connection.execute(
                f"SELECT COUNT(*) FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} WHERE trade_date = ?",
                [target],
            ).fetchone()[0])
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure("system_b_state_completion", "SYSTEM_B_STATE_COMPLETION_MISSING", "state output is not queryable", exception=type(exc).__name__)
    if rows <= 0:
        return CheckResult.failure("system_b_state_completion", "SYSTEM_B_STATE_COMPLETION_MISSING", "state output has no target-date rows")
    return CheckResult.success("system_b_state_completion", target_date=target.isoformat(), rows=rows)


def _state_run_completion(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    try:
        connection = _connection(Path(context.settings.paths.duckdb_path))
        try:
            row = connection.execute(
                f"SELECT COUNT(*) FROM {SYSTEM_B_PRODUCTION_RUN_TABLE} WHERE run_type='DAILY' AND status='SUCCEEDED' AND target_start_date=? AND target_end_date=?",
                [target, target],
            ).fetchone()
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure("system_b_state_run_completion", "SYSTEM_B_STATE_COMPLETION_MISSING", "state production run is not queryable", exception=type(exc).__name__)
    if not row or int(row[0]) <= 0:
        return CheckResult.failure("system_b_state_run_completion", "SYSTEM_B_STATE_COMPLETION_MISSING", "no succeeded daily state run covers the target")
    return CheckResult.success("system_b_state_run_completion", target_date=target.isoformat())


def _duplicate_quality(
    table_name: str,
    check_id: str,
    error_code: str,
    key: tuple[str, ...],
    *,
    path_attribute: str = "duckdb_path",
    path_error_code: str = "SYSTEM_B_DATABASE_NOT_CONFIGURED",
):
    def checker(context: PipelineRunContext) -> CheckResult:
        try:
            path = _path(context.settings, path_attribute, path_error_code)
            connection = _connection(path)
            try:
                key_sql = ", ".join(key)
                duplicate = int(connection.execute(
                    f"SELECT COUNT(*) FROM (SELECT {key_sql}, COUNT(*) AS n FROM {table_name} GROUP BY {key_sql} HAVING COUNT(*) > 1)"
                ).fetchone()[0])
            finally:
                connection.close()
        except Exception as exc:
            return CheckResult.failure(check_id, error_code, "System B output uniqueness could not be checked", exception=type(exc).__name__)
        if duplicate:
            return CheckResult.failure(check_id, error_code, "duplicate System B output keys exist", duplicate_groups=duplicate)
        return CheckResult.success(check_id, duplicate_groups=0)

    checker.__name__ = f"{check_id}_checker"
    return checker


def _episode_completion(table_name: str, check_id: str, error_code: str):
    def checker(context: PipelineRunContext) -> CheckResult:
        target = _target(context)
        path = _path(context.settings, "episode_db_path", "SYSTEM_B_EPISODE_OUTPUT_NOT_CONFIGURED")
        try:
            connection = _connection(path)
            try:
                if table_name == SYSTEM_B_EPISODE_TABLE:
                    row = connection.execute(f"SELECT COUNT(*), MAX(episode_confirmed_date) FROM {table_name}").fetchone()
                else:
                    row = connection.execute(f"SELECT COUNT(*), MAX(trade_date) FROM {table_name}").fetchone()
            finally:
                connection.close()
        except Exception as exc:
            return CheckResult.failure(check_id, error_code, "episode output is not queryable after commit", exception=type(exc).__name__)
        if not row or int(row[0]) <= 0 or row[1] is None:
            return CheckResult.failure(check_id, error_code, "episode output is empty")
        if table_name == SYSTEM_B_EPISODE_OBSERVATION_TABLE and row[1] < target:
            return CheckResult.failure(check_id, error_code, "episode observations do not cover target date")
        return CheckResult.success(check_id, rows=int(row[0]), latest_date=str(row[1]))

    checker.__name__ = f"{check_id}_checker"
    return checker


def _pool_completion(table_name: str, pool_type: str, check_id: str, error_code: str):
    def checker(context: PipelineRunContext) -> CheckResult:
        target = _target(context)
        path = _path(context.settings, "pool_db_path", "SYSTEM_B_POOL_OUTPUT_NOT_CONFIGURED")
        try:
            connection = _connection(path)
            try:
                if table_name == SYSTEM_B_POOL_RUN_TABLE:
                    row = connection.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE trade_date=? AND pool_type=? AND status='COMPLETED'",
                        [target, pool_type],
                    ).fetchone()
                else:
                    row = connection.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE trade_date=? AND pool_type=?",
                        [target, pool_type],
                    ).fetchone()
            finally:
                connection.close()
        except Exception as exc:
            return CheckResult.failure(check_id, error_code, "pool output is not queryable after commit", exception=type(exc).__name__)
        rows = int(row[0]) if row else 0
        if table_name == SYSTEM_B_POOL_RUN_TABLE and rows != 1:
            return CheckResult.failure(check_id, error_code, "pool run is not completed for target date")
        return CheckResult.success(check_id, target_date=target.isoformat(), rows=rows, pool_type=pool_type)

    checker.__name__ = f"{check_id}_checker"
    return checker


def _state_outputs() -> tuple[OutputContract, ...]:
    return (
        OutputContract(
            output_id=SYSTEM_B_STATE_OBSERVATION_TABLE,
            physical_resource=QUANT_DB_RESOURCE,
            location=QUANT_DB_LOCATION,
            object_name=SYSTEM_B_STATE_OBSERVATION_TABLE,
            unique_key=SYSTEM_B_STATE_OBSERVATION.primary_key,
            write_mode=WriteMode.REPLACE_TARGET_DATE,
            target_date_semantics="one target market date in quant.db",
            completion=CompletionContract("target-date state rows are committed", "SYSTEM_B_STATE_COMPLETION_MISSING", _state_completion),
            quality_checks=(_duplicate_quality(SYSTEM_B_STATE_OBSERVATION_TABLE, "system_b_state_unique", "SYSTEM_B_STATE_DUPLICATE_KEY", SYSTEM_B_STATE_OBSERVATION.primary_key),),
            allow_empty=False,
        ),
        OutputContract(
            output_id=SYSTEM_B_PRODUCTION_RUN_TABLE,
            physical_resource=QUANT_DB_RESOURCE,
            location=QUANT_DB_LOCATION,
            object_name=SYSTEM_B_PRODUCTION_RUN_TABLE,
            unique_key=SYSTEM_B_PRODUCTION_RUN.primary_key,
            write_mode=WriteMode.REPLACE_TARGET_DATE,
            target_date_semantics="one succeeded DAILY production-run marker for the target date",
            completion=CompletionContract("daily state production run is succeeded", "SYSTEM_B_STATE_COMPLETION_MISSING", _state_run_completion),
            quality_checks=(_duplicate_quality(SYSTEM_B_PRODUCTION_RUN_TABLE, "system_b_run_unique", "SYSTEM_B_PRODUCTION_RUN_DUPLICATE_KEY", SYSTEM_B_PRODUCTION_RUN.primary_key),),
            allow_empty=False,
        ),
    )


def _episode_outputs() -> tuple[OutputContract, ...]:
    return (
        OutputContract(
            output_id=SYSTEM_B_EPISODE_TABLE,
            physical_resource=EPISODE_DB_RESOURCE,
            location=EPISODE_DB_LOCATION,
            object_name=SYSTEM_B_EPISODE_TABLE,
            unique_key=SYSTEM_B_EPISODE.primary_key,
            write_mode=WriteMode.FULL_REBUILD,
            target_date_semantics="complete rule-version rebuild through the target date",
            completion=CompletionContract("episode table is queryable after rebuild", "SYSTEM_B_EPISODE_COMPLETION_MISSING", _episode_completion(SYSTEM_B_EPISODE_TABLE, "system_b_episode_completion", "SYSTEM_B_EPISODE_COMPLETION_MISSING")),
            quality_checks=(_duplicate_quality(SYSTEM_B_EPISODE_TABLE, "system_b_episode_unique", "SYSTEM_B_EPISODE_DUPLICATE_KEY", SYSTEM_B_EPISODE.primary_key, path_attribute="episode_db_path", path_error_code="SYSTEM_B_EPISODE_OUTPUT_NOT_CONFIGURED"),),
            allow_empty=False,
        ),
        OutputContract(
            output_id=SYSTEM_B_EPISODE_OBSERVATION_TABLE,
            physical_resource=EPISODE_DB_RESOURCE,
            location=EPISODE_DB_LOCATION,
            object_name=SYSTEM_B_EPISODE_OBSERVATION_TABLE,
            unique_key=SYSTEM_B_EPISODE_OBSERVATION.primary_key,
            write_mode=WriteMode.FULL_REBUILD,
            target_date_semantics="complete rule-version observation rebuild through the target date",
            completion=CompletionContract("episode observations cover the target date", "SYSTEM_B_EPISODE_COMPLETION_MISSING", _episode_completion(SYSTEM_B_EPISODE_OBSERVATION_TABLE, "system_b_episode_observation_completion", "SYSTEM_B_EPISODE_COMPLETION_MISSING")),
            quality_checks=(_duplicate_quality(SYSTEM_B_EPISODE_OBSERVATION_TABLE, "system_b_episode_observation_unique", "SYSTEM_B_EPISODE_OBSERVATION_DUPLICATE_KEY", SYSTEM_B_EPISODE_OBSERVATION.primary_key, path_attribute="episode_db_path", path_error_code="SYSTEM_B_EPISODE_OUTPUT_NOT_CONFIGURED"),),
            allow_empty=False,
        ),
    )


def _pool_outputs(pool_type: str) -> tuple[OutputContract, ...]:
    return (
        OutputContract(
            output_id=f"{SYSTEM_B_POOL_MEMBERSHIP_TABLE}_{pool_type.lower()}",
            physical_resource=POOLS_DB_RESOURCE,
            location=POOLS_DB_LOCATION,
            object_name=SYSTEM_B_POOL_MEMBERSHIP_TABLE,
            unique_key=SYSTEM_B_POOL_MEMBERSHIP.primary_key,
            write_mode=WriteMode.REPLACE_TARGET_DATE,
            target_date_semantics="one target date and pool type in system_b_pools.duckdb",
            completion=CompletionContract("target-date pool membership is queryable", "SYSTEM_B_POOL_COMPLETION_MISSING", _pool_completion(SYSTEM_B_POOL_MEMBERSHIP_TABLE, pool_type, f"system_b_{pool_type.lower()}_membership_completion", "SYSTEM_B_POOL_COMPLETION_MISSING")),
            quality_checks=(_duplicate_quality(SYSTEM_B_POOL_MEMBERSHIP_TABLE, f"system_b_{pool_type.lower()}_membership_unique", "SYSTEM_B_POOL_DUPLICATE_KEY", SYSTEM_B_POOL_MEMBERSHIP.primary_key, path_attribute="pool_db_path", path_error_code="SYSTEM_B_POOL_OUTPUT_NOT_CONFIGURED"),),
            allow_empty=True,
        ),
        OutputContract(
            output_id=f"{SYSTEM_B_POOL_RUN_TABLE}_{pool_type.lower()}",
            physical_resource=POOLS_DB_RESOURCE,
            location=POOLS_DB_LOCATION,
            object_name=SYSTEM_B_POOL_RUN_TABLE,
            unique_key=SYSTEM_B_POOL_RUN.primary_key,
            write_mode=WriteMode.REPLACE_TARGET_DATE,
            target_date_semantics="one completed target-date pool run marker",
            completion=CompletionContract("target-date pool run is COMPLETED", "SYSTEM_B_POOL_COMPLETION_MISSING", _pool_completion(SYSTEM_B_POOL_RUN_TABLE, pool_type, f"system_b_{pool_type.lower()}_run_completion", "SYSTEM_B_POOL_COMPLETION_MISSING")),
            quality_checks=(_duplicate_quality(SYSTEM_B_POOL_RUN_TABLE, f"system_b_{pool_type.lower()}_run_unique", "SYSTEM_B_POOL_RUN_DUPLICATE_KEY", SYSTEM_B_POOL_RUN.primary_key, path_attribute="pool_db_path", path_error_code="SYSTEM_B_POOL_OUTPUT_NOT_CONFIGURED"),),
            allow_empty=False,
        ),
    )


def _execution() -> ExecutionPolicy:
    return ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1)


def _metrics(*, rows_read: int, rows_written: int, assets: int, batches: int = 1, write_seconds: float = 0.0) -> PipelineMetrics:
    return PipelineMetrics(
        rows_read=max(0, rows_read),
        rows_written=max(0, rows_written),
        assets_processed=max(0, assets),
        dates_processed=1,
        database_write_seconds=max(0.0, write_seconds),
        batches=max(0, batches),
    )


def _error(exc: Exception, fallback: str) -> ContractError:
    if isinstance(exc, (SystemBProductionError, SystemBEpisodeProductionError, SystemBPoolProductionError)):
        return ContractError(exc.code, exc.detail)
    return ContractError(fallback, type(exc).__name__)


def _execute_readiness(context: PipelineRunContext) -> BusinessExecution:
    is_open, target = _target_is_open(context)
    if not is_open:
        return BusinessExecution.noop("non_trading_day", metrics=_metrics(rows_read=0, rows_written=0, assets=0))
    try:
        report = check_readiness(Path(context.settings.paths.duckdb_path), target)
    except Exception as exc:
        raise _error(exc, "SYSTEM_B_READINESS_FAILED") from exc
    return BusinessExecution.success(
        metrics=_metrics(rows_read=int(report.get("row_count", 0)), rows_written=0, assets=int(report.get("asset_count", 0))),
    )


def _execute_state(context: PipelineRunContext) -> BusinessExecution:
    is_open, target = _target_is_open(context)
    if not is_open:
        return BusinessExecution.noop("non_trading_day", metrics=_metrics(rows_read=0, rows_written=0, assets=0))
    try:
        report = run_daily(
            source_database=Path(context.settings.paths.duckdb_path),
            output_database=Path(context.settings.paths.duckdb_path),
            staging_root=Path(context.settings.paths.tmp_dir),
            trade_date=target,
        )
    except Exception as exc:
        raise _error(exc, "SYSTEM_B_STATE_DAILY_FAILED") from exc
    state_rows = int(report.output_row_count)
    return BusinessExecution.success(
        metrics=_metrics(rows_read=int(report.input_row_count), rows_written=state_rows + 1, assets=int(report.asset_count), batches=int(report.batch_count), write_seconds=float(report.import_seconds)),
        outputs=(
            OutputResult(SYSTEM_B_STATE_OBSERVATION_TABLE, state_rows, QUANT_DB_LOCATION, True, {"target_date": target.isoformat(), "report": {"status": report.status, "production_run_id": report.production_run_id, "unresolved_market_fact_count": report.unresolved_market_fact_count}}),
            OutputResult(SYSTEM_B_PRODUCTION_RUN_TABLE, 1, QUANT_DB_LOCATION, True, {"target_date": target.isoformat(), "production_run_id": report.production_run_id, "status": report.status}),
        ),
    )


def _execute_episode(context: PipelineRunContext) -> BusinessExecution:
    is_open, target = _target_is_open(context)
    if not is_open:
        return BusinessExecution.noop("non_trading_day", metrics=_metrics(rows_read=0, rows_written=0, assets=0))
    output_path = _path(context.settings, "episode_db_path", "SYSTEM_B_EPISODE_OUTPUT_NOT_CONFIGURED")
    try:
        report = rebuild_episodes(Path(context.settings.paths.duckdb_path), output_path, end_date=target)
    except Exception as exc:
        raise _error(exc, "SYSTEM_B_EPISODE_REBUILD_FAILED") from exc
    episode_rows = int(report.get("episode_rows", 0))
    observation_rows = int(report.get("observation_rows", 0))
    return BusinessExecution.success(
        metrics=_metrics(rows_read=int(report.get("state_row_count", 0)), rows_written=episode_rows + observation_rows, assets=int(report.get("state_asset_count", 0))),
        outputs=(
            OutputResult(SYSTEM_B_EPISODE_TABLE, episode_rows, EPISODE_DB_LOCATION, True, {"target_date": target.isoformat(), "output_database": str(output_path), "strategy": "transactional rule-version replacement"}),
            OutputResult(SYSTEM_B_EPISODE_OBSERVATION_TABLE, observation_rows, EPISODE_DB_LOCATION, True, {"target_date": target.isoformat(), "output_database": str(output_path)}),
        ),
    )


def _execute_pool(context: PipelineRunContext, pool_type: str) -> BusinessExecution:
    is_open, target = _target_is_open(context)
    if not is_open:
        return BusinessExecution.noop("non_trading_day", metrics=_metrics(rows_read=0, rows_written=0, assets=0))
    episode_path = _path(context.settings, "episode_db_path", "SYSTEM_B_EPISODE_INPUT_NOT_CONFIGURED")
    output_path = _path(context.settings, "pool_db_path", "SYSTEM_B_POOL_OUTPUT_NOT_CONFIGURED")
    try:
        report = build_stock_pool(
            Path(context.settings.paths.duckdb_path),
            output_path,
            pool_type=pool_type,
            start_date=target,
            end_date=target,
            episode_database=episode_path,
        )
    except Exception as exc:
        raise _error(exc, "SYSTEM_B_POOL_BUILD_FAILED") from exc
    memberships = int(report.get("membership_rows", 0))
    return BusinessExecution.success(
        metrics=_metrics(rows_read=0, rows_written=memberships + 1, assets=int(report.get("asset_count", 0)), write_seconds=float(report.get("timings", {}).get("write_seconds", 0.0))),
        outputs=(
            OutputResult(f"{SYSTEM_B_POOL_MEMBERSHIP_TABLE}_{pool_type.lower()}", memberships, POOLS_DB_LOCATION, True, {"target_date": target.isoformat(), "pool_type": pool_type, "output_database": str(output_path)}),
            OutputResult(f"{SYSTEM_B_POOL_RUN_TABLE}_{pool_type.lower()}", 1, POOLS_DB_LOCATION, True, {"target_date": target.isoformat(), "pool_type": pool_type, "status": report.get("status")}),
        ),
    )


def _performance(scope: str, *, normal: float, warning: float, hard: int) -> PerformanceBudget:
    return PerformanceBudget(
        normal_budget_seconds=normal,
        warning_threshold_seconds=warning,
        hard_timeout_seconds=hard,
        benchmark_scope=scope,
        baseline_source="tests/pipeline/system_b/test_production.py, tests/pipeline/system_b_episode/test_production.py, tests/pipeline/system_b_pools/test_service.py",
    )


def _readonly_idempotency(name: str) -> IdempotencyContract:
    return IdempotencyContract(
        idempotency_key=f"{name}.target_date",
        repeat_run_semantics="repeating a readiness check reads the same target facts without writes",
        existing_target_handling="no target data is changed",
        failure_recovery="rerun after the source freshness or schema issue is corrected",
        uses_staging=False,
        atomic_replace_boundary="read-only check",
    )


SYSTEM_B_STATE_READINESS = register_pipeline(
    PipelineContract(
        pipeline_id="system_b_state_readiness",
        name="System B state readiness",
        description="Checks the canonical market facts required before the System B daily state calculation.",
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_readiness,
        target_date_policy=SYSTEM_B_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(_state_source_input(),),
        outputs=(),
        dependencies=("market_daily_update", "adj_factor_daily", "suspend_d_ingest"),
        resource_locks=(),
        resource_reads=STATE_READS,
        idempotency=_readonly_idempotency("system_b_state_readiness"),
        transaction=TransactionContract(TransactionMode.READ_ONLY, "one target-date source readiness read", "read-only checks never change source tables"),
        execution=_execution(),
        performance=_performance("one target-date System B readiness scan", normal=120.0, warning=60.0, hard=300),
    )
)


SYSTEM_B_STATE_DAILY = register_pipeline(
    PipelineContract(
        pipeline_id="system_b_state_daily",
        name="System B daily state machine",
        description="Calculates and atomically replaces System B state observations for one market date.",
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_state,
        target_date_policy=SYSTEM_B_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(_state_source_input(),),
        outputs=_state_outputs(),
        dependencies=("system_b_state_readiness",),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=STATE_READS,
        idempotency=IdempotencyContract(
            idempotency_key="system_b_state_observation.asset_id,trade_date,rule_version_set_id,parameter_set_id",
            repeat_run_semantics="same target date removes and replaces only the target state snapshot",
            existing_target_handling="prior target state remains until the service import transaction commits",
            failure_recovery="rerun the target date after the source or calculation failure is corrected",
            uses_staging=True,
            atomic_replace_boundary="validated target parquet import and production-run update in one quant.db transaction",
        ),
        transaction=TransactionContract(TransactionMode.DATABASE_TRANSACTION, "one target-date state replacement and production-run marker", "failed staging or import leaves the prior target snapshot visible"),
        execution=_execution(),
        performance=_performance("one target-date System B calculation over the canonical market universe", normal=1800.0, warning=1200.0, hard=2400),
    )
)


SYSTEM_B_EPISODE_REBUILD = register_pipeline(
    PipelineContract(
        pipeline_id="system_b_episode_rebuild",
        name="System B episode rebuild",
        description="Rebuilds the configured System B episode database through the current target date.",
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_episode,
        target_date_policy=SYSTEM_B_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(_state_table_input(),),
        outputs=_episode_outputs(),
        dependencies=("system_b_state_daily",),
        resource_locks=(EPISODE_DB_WRITER,),
        resource_reads=EPISODE_READS,
        idempotency=IdempotencyContract(
            idempotency_key="system_b_episode.rule_version and target end date",
            repeat_run_semantics="same rule-version rebuild deterministically replaces prior episode rows",
            existing_target_handling="only the managed System B episode rule version is deleted and rebuilt",
            failure_recovery="the database transaction rolls back and the prior episode rule-version output remains",
            uses_staging=False,
            atomic_replace_boundary="one episode database transaction around rule-version replacement",
        ),
        transaction=TransactionContract(TransactionMode.DATABASE_TRANSACTION, "managed episode rule-version replacement through target date", "failed rebuild rolls back the episode database transaction"),
        execution=_execution(),
        performance=_performance("one System B episode rebuild over the initialized state history", normal=1800.0, warning=1200.0, hard=2400),
    )
)


def _pool_contract(pool_type: str) -> PipelineContract:
    lower = pool_type.lower()
    return PipelineContract(
        pipeline_id=f"system_b_pool_{lower}",
        name=f"System B {pool_type} pool",
        description=f"Builds and atomically replaces the System B {pool_type} pool for one target date.",
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=lambda context: _execute_pool(context, pool_type),
        target_date_policy=SYSTEM_B_TARGET_DATE_POLICY,
        parameters=(),
        inputs=_pool_inputs(),
        outputs=_pool_outputs(pool_type),
        dependencies=("system_b_episode_rebuild",),
        resource_locks=(POOLS_DB_WRITER,),
        resource_reads=POOL_READS,
        idempotency=IdempotencyContract(
            idempotency_key=f"system_b_pool_membership_daily.trade_date,asset_id,pool_type ({pool_type})",
            repeat_run_semantics="same target date and pool type replaces only that pool snapshot",
            existing_target_handling="prior target pool rows and run marker remain until the staged database replacement commits",
            failure_recovery="staged output is removed and the prior pool database remains after a failed build",
            uses_staging=True,
            atomic_replace_boundary="target pool/date replacement through the service's staged database file",
        ),
        transaction=TransactionContract(TransactionMode.STAGING_ATOMIC_REPLACE, "one target-date pool replacement and completed run marker", "failed calculation or staged commit leaves the prior pool database unchanged"),
        execution=_execution(),
        performance=_performance(f"one target-date System B {pool_type} pool calculation", normal=900.0, warning=600.0, hard=1200),
    )


SYSTEM_B_POOL_HEIGHT = register_pipeline(_pool_contract(HEIGHT))
SYSTEM_B_POOL_CAPACITY = register_pipeline(_pool_contract(CAPACITY))
SYSTEM_B_POOL_RECOGNITION = register_pipeline(_pool_contract(RECOGNITION))


SYSTEM_B_CONTRACTS: tuple[PipelineContract, ...] = (
    SYSTEM_B_STATE_READINESS,
    SYSTEM_B_STATE_DAILY,
    SYSTEM_B_EPISODE_REBUILD,
    SYSTEM_B_POOL_HEIGHT,
    SYSTEM_B_POOL_CAPACITY,
    SYSTEM_B_POOL_RECOGNITION,
)

__all__ = [
    "SYSTEM_B_CONTRACTS",
    "SYSTEM_B_EPISODE_REBUILD",
    "SYSTEM_B_POOL_CAPACITY",
    "SYSTEM_B_POOL_HEIGHT",
    "SYSTEM_B_POOL_RECOGNITION",
    "SYSTEM_B_STATE_DAILY",
    "SYSTEM_B_STATE_READINESS",
    "SYSTEM_B_TARGET_DATE_POLICY",
]
