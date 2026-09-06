"""Formal production contract for Task06-A asset-relative ranking."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from qrp_atlas.contracts import (
    DAILY_MARKET_SNAPSHOT,
    POPULARITY_SOURCE_AVAILABILITY,
    SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT,
    SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE,
    SYSTEM_B_ASSET_RANK_SNAPSHOT,
    SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE,
    SYSTEM_B_EPISODE_OBSERVATION,
    SYSTEM_B_EPISODE_OBSERVATION_TABLE,
    SYSTEM_B_EPISODE_TABLE,
    SYSTEM_B_POOL_MEMBERSHIP,
    SYSTEM_B_POOL_MEMBERSHIP_TABLE,
    SYSTEM_B_POOL_RUN,
    SYSTEM_B_POOL_RUN_TABLE,
    STOCK_INFO,
    TICKER,
    TRADE_DATE,
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
from .system_b_asset_rank.service import (
    SystemBAssetRankProductionError,
    build_canonical_a_share_universe,
    run_asset_rank_daily,
)


CHINA_TZ = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
EPISODE_DB_RESOURCE = "system_b_episode_db"
POOLS_DB_RESOURCE = "system_b_pools_db"

_ASSET_RANK_TABLES = (
    STOCK_INFO.name,
    TRADING_CALENDAR.name,
    DAILY_MARKET_SNAPSHOT.name,
    POPULARITY_SOURCE_AVAILABILITY.name,
)


def _target_date(invocation: PipelineInvocation) -> TargetWindow:
    if invocation.scheduled_for.tzinfo is None:
        raise ContractError("SCHEDULE_TIMEZONE_MISSING")
    return TargetWindow.for_date(invocation.scheduled_for.astimezone(CHINA_TZ).date())


def _validate_target_date(target_date: date, _invocation: PipelineInvocation) -> bool:
    return isinstance(target_date, date) and not isinstance(target_date, datetime)


SYSTEM_B_ASSET_RANK_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="system_b_asset_rank_scheduled_shanghai_date_v1",
    description="Uses the scheduled Asia/Shanghai calendar date; closed dates are explicit no-ops.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_target_date,
    validate_explicit_date=_validate_target_date,
)


def _target(context: PipelineRunContext) -> date:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("ASSET_RANK_TARGET_DATE_MISSING")
    return target


def _path(context: PipelineRunContext, attribute: str, error_code: str) -> Path:
    value = getattr(context.settings.paths, attribute, None)
    if value is None:
        raise ContractError(error_code)
    return Path(value)


def _inspect(
    path: Path,
    *,
    check_id: str,
    tables: tuple[str, ...],
    columns: dict[str, tuple[str, ...]],
    error_code: str,
) -> CheckResult:
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(path), read_only=True)
        actual_tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        missing_tables = sorted(set(tables) - actual_tables)
        if missing_tables:
            return CheckResult.failure(check_id, error_code, "required tables are missing", missing=missing_tables)
        missing_columns: dict[str, list[str]] = {}
        for table, required in columns.items():
            actual = {
                str(row[0])
                for row in connection.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema='main' AND table_name=?",
                    [table],
                ).fetchall()
            }
            missing = sorted(set(required) - actual)
            if missing:
                missing_columns[table] = missing
        if missing_columns:
            return CheckResult.failure(check_id, error_code, "required columns are missing", missing=missing_columns)
        return CheckResult.success(check_id, path=str(path), tables=list(tables))
    except Exception as exc:
        return CheckResult.failure(check_id, error_code, "database could not be inspected", exception=type(exc).__name__)
    finally:
        if connection is not None:
            connection.close()


def _quant_structure(context: PipelineRunContext) -> CheckResult:
    return _inspect(
        Path(context.settings.paths.duckdb_path),
        check_id="system_b_asset_rank_quant_structure",
        tables=_ASSET_RANK_TABLES,
        columns={
            STOCK_INFO.name: (TICKER, "list_date", "delist_date"),
            TRADING_CALENDAR.name: (TRADE_DATE, "is_open"),
            DAILY_MARKET_SNAPSHOT.name: (TRADE_DATE, TICKER, "close"),
            POPULARITY_SOURCE_AVAILABILITY.name: tuple(POPULARITY_SOURCE_AVAILABILITY.column_names()),
        },
        error_code="ASSET_RANK_QUANT_INPUT_STRUCTURE_MISSING",
    )


def _episode_structure(context: PipelineRunContext) -> CheckResult:
    try:
        path = _path(context, "episode_db_path", "ASSET_RANK_EPISODE_DATABASE_NOT_CONFIGURED")
    except ContractError as exc:
        return CheckResult.failure("system_b_asset_rank_episode_structure", exc.code, "episode database is not configured")
    return _inspect(
        path,
        check_id="system_b_asset_rank_episode_structure",
        tables=(SYSTEM_B_EPISODE_TABLE, SYSTEM_B_EPISODE_OBSERVATION_TABLE),
        columns={SYSTEM_B_EPISODE_OBSERVATION_TABLE: tuple(SYSTEM_B_EPISODE_OBSERVATION.column_names())},
        error_code="ASSET_RANK_EPISODE_INPUT_STRUCTURE_MISSING",
    )


def _pool_structure(context: PipelineRunContext) -> CheckResult:
    try:
        path = _path(context, "pool_db_path", "ASSET_RANK_POOL_DATABASE_NOT_CONFIGURED")
    except ContractError as exc:
        return CheckResult.failure("system_b_asset_rank_pool_structure", exc.code, "pool database is not configured")
    return _inspect(
        path,
        check_id="system_b_asset_rank_pool_structure",
        tables=(SYSTEM_B_POOL_RUN_TABLE, SYSTEM_B_POOL_MEMBERSHIP_TABLE),
        columns={
            SYSTEM_B_POOL_RUN_TABLE: tuple(SYSTEM_B_POOL_RUN.column_names()),
            SYSTEM_B_POOL_MEMBERSHIP_TABLE: tuple(SYSTEM_B_POOL_MEMBERSHIP.column_names()),
        },
        error_code="ASSET_RANK_POOL_INPUT_STRUCTURE_MISSING",
    )


def _is_open(context: PipelineRunContext) -> tuple[bool, date]:
    target = _target(context)
    connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
    try:
        row = connection.execute(
            f"SELECT is_open FROM {TRADING_CALENDAR.name} WHERE trade_date=?", [target]
        ).fetchone()
    finally:
        connection.close()
    return bool(row and row[0]), target


def _quant_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        is_open, target = _is_open(context)
        return CheckResult.success("system_b_asset_rank_quant_freshness", target_date=target.isoformat(), is_open=is_open, noop=not is_open)
    except Exception as exc:
        return CheckResult.failure("system_b_asset_rank_quant_freshness", "ASSET_RANK_QUANT_INPUT_STALE", "target calendar could not be read", exception=type(exc).__name__)


def _episode_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        is_open, target = _is_open(context)
        if not is_open:
            return CheckResult.success("system_b_asset_rank_episode_freshness", target_date=target.isoformat(), noop=True)
        path = _path(context, "episode_db_path", "ASSET_RANK_EPISODE_DATABASE_NOT_CONFIGURED")
        connection = duckdb.connect(str(path), read_only=True)
        try:
            latest = connection.execute(
                f"SELECT MAX(trade_date) FROM {SYSTEM_B_EPISODE_OBSERVATION_TABLE}"
            ).fetchone()[0]
        finally:
            connection.close()
        if latest is None or latest < target:
            return CheckResult.failure("system_b_asset_rank_episode_freshness", "ASSET_RANK_EPISODE_INPUT_STALE", "episode observations do not cover target date", latest_date=str(latest))
        return CheckResult.success("system_b_asset_rank_episode_freshness", target_date=target.isoformat(), latest_date=str(latest))
    except ContractError as exc:
        return CheckResult.failure("system_b_asset_rank_episode_freshness", exc.code, "episode input is not configured")
    except Exception as exc:
        return CheckResult.failure("system_b_asset_rank_episode_freshness", "ASSET_RANK_EPISODE_INPUT_STALE", "episode observations could not be read", exception=type(exc).__name__)


def _pool_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        is_open, target = _is_open(context)
        if not is_open:
            return CheckResult.success("system_b_asset_rank_pool_freshness", target_date=target.isoformat(), noop=True)
        path = _path(context, "pool_db_path", "ASSET_RANK_POOL_DATABASE_NOT_CONFIGURED")
        connection = duckdb.connect(str(path), read_only=True)
        try:
            rows = connection.execute(
                f"SELECT pool_type, status FROM {SYSTEM_B_POOL_RUN_TABLE} WHERE trade_date=?",
                [target],
            ).fetchall()
        finally:
            connection.close()
        completed = {str(pool).upper() for pool, status in rows if str(status).upper() == "COMPLETED"}
        expected = {"CAPACITY", "HEIGHT", "RECOGNITION"}
        if completed != expected:
            return CheckResult.failure("system_b_asset_rank_pool_freshness", "ASSET_RANK_POOL_INPUT_STALE", "all three pool completion markers are required", completed=sorted(completed))
        return CheckResult.success("system_b_asset_rank_pool_freshness", target_date=target.isoformat(), completed=sorted(completed))
    except ContractError as exc:
        return CheckResult.failure("system_b_asset_rank_pool_freshness", exc.code, "pool input is not configured")
    except Exception as exc:
        return CheckResult.failure("system_b_asset_rank_pool_freshness", "ASSET_RANK_POOL_INPUT_STALE", "pool completion could not be read", exception=type(exc).__name__)


def _popularity_freshness(context: PipelineRunContext) -> CheckResult:
    try:
        is_open, target = _is_open(context)
        if not is_open:
            return CheckResult.success("system_b_asset_rank_popularity_freshness", target_date=target.isoformat(), noop=True)
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            rows = connection.execute(
                f"SELECT source, source_status, valid_snapshot_count FROM {POPULARITY_SOURCE_AVAILABILITY.name} WHERE trade_date=?",
                [target],
            ).fetchall()
        finally:
            connection.close()
        sources = {str(source).lower(): (str(status).upper(), int(count)) for source, status, count in rows}
        if set(sources) != {"dc_hot", "ths_hot", "eastmoney", "ths"} and not ({"dc_hot", "ths_hot"} <= set(sources)):
            return CheckResult.failure("system_b_asset_rank_popularity_freshness", "ASSET_RANK_POPULARITY_AVAILABILITY_MISSING", "both popularity source availability rows are required")
        canonical = {("dc_hot" if source in {"dc_hot", "eastmoney"} else "ths_hot"): value for source, value in sources.items()}
        if set(canonical) != {"dc_hot", "ths_hot"}:
            return CheckResult.failure("system_b_asset_rank_popularity_freshness", "ASSET_RANK_POPULARITY_AVAILABILITY_MISSING", "both popularity source availability rows are required")
        invalid = {source: value for source, value in canonical.items() if value[0] not in {"AVAILABLE", "UNAVAILABLE"} or value[1] < 0 or (value[0] == "UNAVAILABLE" and value[1] != 0)}
        if invalid:
            return CheckResult.failure("system_b_asset_rank_popularity_freshness", "ASSET_RANK_POPULARITY_AVAILABILITY_INVALID", "invalid popularity availability status", invalid=invalid)
        return CheckResult.success("system_b_asset_rank_popularity_freshness", target_date=target.isoformat(), sources=canonical)
    except Exception as exc:
        return CheckResult.failure("system_b_asset_rank_popularity_freshness", "ASSET_RANK_POPULARITY_AVAILABILITY_MISSING", "popularity availability could not be read", exception=type(exc).__name__)


def _snapshot_completion(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        universe = len(build_canonical_a_share_universe(connection, target))
        rows = int(connection.execute(f"SELECT COUNT(*) FROM {SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE} WHERE trade_date=?", [target]).fetchone()[0])
    except Exception as exc:
        return CheckResult.failure("system_b_asset_rank_snapshot_completion", "ASSET_RANK_SNAPSHOT_COMPLETION_MISSING", "snapshot output could not be read", exception=type(exc).__name__)
    finally:
        if connection is not None:
            connection.close()
    if rows != universe:
        return CheckResult.failure("system_b_asset_rank_snapshot_completion", "ASSET_RANK_SNAPSHOT_COMPLETION_MISSING", "snapshot must contain one row per canonical A-share", expected=universe, actual=rows)
    return CheckResult.success("system_b_asset_rank_snapshot_completion", expected=universe, actual=rows, target_date=target.isoformat())


def _audit_completion(context: PipelineRunContext) -> CheckResult:
    target = _target(context)
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        universe = len(build_canonical_a_share_universe(connection, target))
        rows = int(connection.execute(f"SELECT COUNT(*) FROM {SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE} WHERE trade_date=?", [target]).fetchone()[0])
    except Exception as exc:
        return CheckResult.failure("system_b_asset_rank_audit_completion", "ASSET_RANK_AUDIT_COMPLETION_MISSING", "component audit output could not be read", exception=type(exc).__name__)
    finally:
        if connection is not None:
            connection.close()
    expected = universe * 7
    if rows != expected:
        return CheckResult.failure("system_b_asset_rank_audit_completion", "ASSET_RANK_AUDIT_COMPLETION_MISSING", "component audit must contain seven components per canonical A-share", expected=expected, actual=rows)
    return CheckResult.success("system_b_asset_rank_audit_completion", expected=expected, actual=rows, target_date=target.isoformat())


def _quality(table_name: str, check_id: str, key_columns: tuple[str, ...], error_code: str):
    def checker(context: PipelineRunContext) -> CheckResult:
        target = _target(context)
        group_by = ", ".join(key_columns)
        try:
            connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
            try:
                duplicates = int(connection.execute(f"SELECT COUNT(*) FROM (SELECT {group_by}, COUNT(*) c FROM {table_name} WHERE trade_date=? GROUP BY {group_by} HAVING COUNT(*)>1)", [target]).fetchone()[0])
            finally:
                connection.close()
        except Exception as exc:
            return CheckResult.failure(check_id, error_code, "output uniqueness could not be checked", exception=type(exc).__name__)
        if duplicates:
            return CheckResult.failure(check_id, error_code, "duplicate output keys exist", duplicate_groups=duplicates)
        return CheckResult.success(check_id, target_date=target.isoformat(), duplicate_groups=0)
    checker.__name__ = check_id
    return checker


def _execute(context: PipelineRunContext) -> BusinessExecution:
    target = _target(context)
    try:
        pool_path = _path(context, "pool_db_path", "ASSET_RANK_POOL_DATABASE_NOT_CONFIGURED")
        episode_path = _path(context, "episode_db_path", "ASSET_RANK_EPISODE_DATABASE_NOT_CONFIGURED")
        report = run_asset_rank_daily(
            quant_database=Path(context.settings.paths.duckdb_path),
            pool_database=pool_path,
            episode_database=episode_path,
            trade_date=target,
            production_run_id=context.run_id,
            execution_control=context.execution_control,
        )
    except SystemBAssetRankProductionError as exc:
        raise ContractError(exc.code, exc.detail) from exc
    except Exception as exc:
        raise ContractError("ASSET_RANK_EXECUTION_FAILED", type(exc).__name__) from exc
    if report["status"] == "NOOP":
        return BusinessExecution.noop(report["reason"], metrics=PipelineMetrics(dates_processed=1))
    diagnostics = tuple(
        PipelineDiagnostic(code=code, level=DiagnosticLevel.WARNING, message=f"expected popularity source condition: {code}")
        for code in report.get("diagnostics", [])
    )
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=report["asset_count"],
            rows_written=report["rows_written"],
            assets_processed=report["asset_count"],
            dates_processed=1,
            batches=1,
        ),
        outputs=(
            OutputResult(SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE, report["snapshot_rows"], "settings.paths.duckdb_path", True, {"target_date": target.isoformat()}),
            OutputResult(SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE, report["component_audit_rows"], "settings.paths.duckdb_path", True, {"target_date": target.isoformat()}),
        ),
        diagnostics=diagnostics,
    )


SYSTEM_B_ASSET_RANK_PRODUCTION = register_pipeline(
    PipelineContract(
        pipeline_id="system_b_asset_rank_daily",
        name="System B Task06-A asset relative ranking",
        description="Calculates and atomically publishes M1/M2/M3 asset scores and component audit for one canonical A-share date.",
        contract_version="0.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute,
        target_date_policy=SYSTEM_B_ASSET_RANK_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            InputContract("asset_rank_quant_facts", InputKind.TABLE, "quant_db.stock_info,trading_calendar,daily_market_snapshot,popularity_source_availability", tuple(_ASSET_RANK_TABLES), "target-date canonical facts and explicit popularity availability", "ASSET_RANK_QUANT_INPUT_STRUCTURE_MISSING", _quant_structure, FreshnessContract("system_b_asset_rank_quant_freshness", "target-date calendar is resolved before calculation", 0, NonTradingDayPolicy.ALLOW_CALENDAR_DATE, "ASSET_RANK_QUANT_INPUT_STALE", _quant_freshness)),
            InputContract("asset_rank_episode", InputKind.UPSTREAM_PIPELINE, f"{EPISODE_DB_RESOURCE}.{SYSTEM_B_EPISODE_TABLE},{EPISODE_DB_RESOURCE}.{SYSTEM_B_EPISODE_OBSERVATION_TABLE}", tuple(SYSTEM_B_EPISODE_OBSERVATION.column_names()), "episode observations through target date", "ASSET_RANK_EPISODE_INPUT_STRUCTURE_MISSING", _episode_structure, FreshnessContract("system_b_asset_rank_episode_freshness", "episode observations cover target date", 0, NonTradingDayPolicy.ALLOW_CALENDAR_DATE, "ASSET_RANK_EPISODE_INPUT_STALE", _episode_freshness), "system_b_episode_rebuild"),
            InputContract("asset_rank_pool", InputKind.UPSTREAM_PIPELINE, f"{POOLS_DB_RESOURCE}.{SYSTEM_B_POOL_RUN_TABLE},{POOLS_DB_RESOURCE}.{SYSTEM_B_POOL_MEMBERSHIP_TABLE}", tuple(SYSTEM_B_POOL_MEMBERSHIP.column_names()), "all three completed pool snapshots for target date", "ASSET_RANK_POOL_INPUT_STRUCTURE_MISSING", _pool_structure, FreshnessContract("system_b_asset_rank_pool_freshness", "all three pool completion markers cover target date", 0, NonTradingDayPolicy.ALLOW_CALENDAR_DATE, "ASSET_RANK_POOL_INPUT_STALE", _pool_freshness), "system_b_pool_recognition"),
            InputContract("asset_rank_popularity_availability", InputKind.TABLE, "quant_db.popularity_source_availability with dc_hot/ths_hot source rows", tuple(POPULARITY_SOURCE_AVAILABILITY.column_names()), "explicit AVAILABLE or expected UNAVAILABLE source/date facts", "ASSET_RANK_POPULARITY_AVAILABILITY_MISSING", _quant_structure, FreshnessContract("system_b_asset_rank_popularity_freshness", "availability is checked without treating expected UNAVAILABLE as failure", 0, NonTradingDayPolicy.ALLOW_CALENDAR_DATE, "ASSET_RANK_POPULARITY_AVAILABILITY_MISSING", _popularity_freshness)),
        ),
        outputs=(
            OutputContract(SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE, QUANT_DB_RESOURCE, "settings.paths.duckdb_path", SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE, SYSTEM_B_ASSET_RANK_SNAPSHOT.primary_key, WriteMode.REPLACE_TARGET_DATE, "one row per target-date canonical A-share", CompletionContract("canonical target-date Asset Rank snapshot is queryable", "ASSET_RANK_SNAPSHOT_COMPLETION_MISSING", _snapshot_completion), (_quality(SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE, "asset_rank_snapshot_unique", SYSTEM_B_ASSET_RANK_SNAPSHOT.primary_key, "ASSET_RANK_SNAPSHOT_DUPLICATE_KEY"),), False),
            OutputContract(SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE, QUANT_DB_RESOURCE, "settings.paths.duckdb_path", SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE, SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT.primary_key, WriteMode.REPLACE_TARGET_DATE, "seven audited components per target-date canonical A-share", CompletionContract("component audit is queryable after snapshot commit", "ASSET_RANK_AUDIT_COMPLETION_MISSING", _audit_completion), (_quality(SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE, "asset_rank_audit_unique", SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT.primary_key, "ASSET_RANK_AUDIT_DUPLICATE_KEY"),), False),
        ),
        dependencies=("system_b_episode_rebuild", "system_b_pool_height", "system_b_pool_capacity", "system_b_pool_recognition", "dc_hot_ingest", "ths_hot_ingest"),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=(
            *(f"duckdb://{QUANT_DB_RESOURCE}#{table}" for table in _ASSET_RANK_TABLES),
            f"duckdb://{QUANT_DB_RESOURCE}#{SYSTEM_B_ASSET_RANK_SNAPSHOT_TABLE}",
            f"duckdb://{QUANT_DB_RESOURCE}#{SYSTEM_B_ASSET_RANK_COMPONENT_AUDIT_TABLE}",
            f"duckdb://{EPISODE_DB_RESOURCE}#{SYSTEM_B_EPISODE_TABLE}",
            f"duckdb://{EPISODE_DB_RESOURCE}#{SYSTEM_B_EPISODE_OBSERVATION_TABLE}",
            f"duckdb://{POOLS_DB_RESOURCE}#{SYSTEM_B_POOL_RUN_TABLE}",
            f"duckdb://{POOLS_DB_RESOURCE}#{SYSTEM_B_POOL_MEMBERSHIP_TABLE}",
        ),
        idempotency=IdempotencyContract("system_b_asset_rank_snapshot.trade_date,ticker", "same target date replaces only that date and creates a new run provenance", "prior snapshot and audit remain visible until one transaction commits", "rerun explicitly after late popularity input; no automatic historical mutation", False, "single quant.db transaction around snapshot and component-audit target replacement"),
        transaction=TransactionContract(TransactionMode.DATABASE_TRANSACTION, "one quant.db target-date replacement for snapshot and component audit", "failed write rolls back both output tables"),
        execution=ExecutionPolicy(OverlapPolicy.FORBID, 1),
        performance=PerformanceBudget(1800.0, 1200.0, 2400, "canonical A-share universe, three completed pools, episode observations and two popularity availability facts", "Task06-A offline acceptance benchmark"),
        manual_execution_allowed=True,
    )
)


SYSTEM_B_ASSET_RANK_PRODUCTION_CONTRACT = SYSTEM_B_ASSET_RANK_PRODUCTION
SYSTEM_B_ASSET_RANK_CONTRACTS = (SYSTEM_B_ASSET_RANK_PRODUCTION,)


__all__ = [
    "SYSTEM_B_ASSET_RANK_PRODUCTION",
    "SYSTEM_B_ASSET_RANK_PRODUCTION_CONTRACT",
    "SYSTEM_B_ASSET_RANK_CONTRACTS",
    "SYSTEM_B_ASSET_RANK_TARGET_DATE_POLICY",
]
