"""Formal source contracts for the Task09 System B daily chain."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import duckdb

from qrp_atlas.contracts import (
    SYSTEM_B_DECISION_FACTS_DAILY,
    SYSTEM_B_STRATEGY_CLOSEOUT,
    SYSTEM_B_STRATEGY_RESULT,
    SYSTEM_B_STRATEGY_TARGET,
    TRADING_CALENDAR,
)
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
    PipelineRunContext,
    PipelineKind,
    PipelineMetrics,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .registry import register_pipeline
from .system_b_task09 import (
    closeout_strategy_daily,
    normalize_decision_facts,
    persist_decision_facts,
    resolve_task09_target_date,
    run_task09_daily,
)

def _target_date(invocation) -> TargetWindow:
    return TargetWindow.for_date(resolve_task09_target_date(invocation.scheduled_for, invocation.trade_date_override))


def _validate_target_date(value: date, _invocation) -> bool:
    return isinstance(value, date) and not isinstance(value, datetime)


TASK09_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="task09_scheduled_or_explicit_shanghai_date_v1",
    description="Explicit date is strict date-only; otherwise resolve scheduled instant to Asia/Shanghai date.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_target_date,
    validate_explicit_date=_validate_target_date,
)


def _path(context: PipelineRunContext) -> Path:
    value = getattr(context.settings.paths, "duckdb_path", None)
    if value is None:
        raise ContractError("TASK09_DATABASE_NOT_CONFIGURED")
    return Path(value)


def _table_check(context: PipelineRunContext, check_id: str, tables: tuple[str, ...], error_code: str) -> CheckResult:
    try:
        connection = duckdb.connect(str(_path(context)), read_only=True)
        try:
            actual = {row[0] for row in connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}
        finally:
            connection.close()
        missing = sorted(set(tables) - actual)
        if missing:
            return CheckResult.failure(check_id, error_code, "required tables are missing", missing=missing)
        return CheckResult.success(check_id, tables=list(tables))
    except Exception as exc:
        return CheckResult.failure(check_id, error_code, "database could not be inspected", exception=type(exc).__name__)


def _facts_structure(context: PipelineRunContext) -> CheckResult:
    return _table_check(context, "task09_facts_structure", (SYSTEM_B_DECISION_FACTS_DAILY.name,), "TASK09_FACTS_STRUCTURE_MISSING")


def _facts_freshness(context: PipelineRunContext) -> CheckResult:
    target = context.target_window.target_date
    if target is None:
        return CheckResult.failure("task09_facts_freshness", "TASK09_TARGET_DATE_MISSING", "target date is required")
    try:
        connection = duckdb.connect(str(_path(context)), read_only=True)
        try:
            calendar_row = connection.execute(
                f"SELECT is_open FROM {TRADING_CALENDAR.name} WHERE trade_date=?", [target]
            ).fetchone()
            if calendar_row is None:
                return CheckResult.failure("task09_facts_freshness", "TASK09_CALENDAR_UNAVAILABLE", "calendar does not cover target date")
            count = connection.execute(
                f"SELECT COUNT(*) FROM {SYSTEM_B_DECISION_FACTS_DAILY.name} WHERE trade_date=?", [target]
            ).fetchone()[0]
        finally:
            connection.close()
        if not bool(calendar_row[0]):
            return CheckResult.success("task09_facts_freshness", target_date=target.isoformat(), rows=int(count), non_trading_day=True)
        if int(count) == 0:
            return CheckResult.failure("task09_facts_freshness", "TASK09_FACTS_UNAVAILABLE", "no decision facts cover target date")
        return CheckResult.success("task09_facts_freshness", target_date=target.isoformat(), rows=int(count))
    except Exception as exc:
        return CheckResult.failure("task09_facts_freshness", "TASK09_FACTS_UNAVAILABLE", "facts could not be read", exception=type(exc).__name__)


def _calendar_freshness(context: PipelineRunContext) -> CheckResult:
    target = context.target_window.target_date
    if target is None:
        return CheckResult.failure("task09_calendar_freshness", "TASK09_TARGET_DATE_MISSING", "target date is required")
    try:
        connection = duckdb.connect(str(_path(context)), read_only=True)
        try:
            row = connection.execute(f"SELECT is_open FROM {TRADING_CALENDAR.name} WHERE trade_date=?", [target]).fetchone()
        finally:
            connection.close()
        if row is None:
            return CheckResult.failure("task09_calendar_freshness", "TASK09_CALENDAR_UNAVAILABLE", "calendar does not cover target date")
        return CheckResult.success("task09_calendar_freshness", target_date=target.isoformat(), is_open=bool(row[0]))
    except Exception as exc:
        return CheckResult.failure("task09_calendar_freshness", "TASK09_CALENDAR_UNAVAILABLE", "calendar could not be read", exception=type(exc).__name__)


def _completed(context: PipelineRunContext) -> CheckResult:
    return _table_check(
        context,
        "task09_result_tables",
        (SYSTEM_B_STRATEGY_RESULT.name, SYSTEM_B_STRATEGY_TARGET.name, SYSTEM_B_STRATEGY_CLOSEOUT.name),
        "TASK09_RESULT_STRUCTURE_MISSING",
    )


def _facts_completed(context: PipelineRunContext) -> CheckResult:
    return _table_check(
        context,
        "task09_facts_completed",
        (SYSTEM_B_DECISION_FACTS_DAILY.name,),
        "TASK09_FACTS_COMPLETION_MISSING",
    )


def _task09_quality(context: PipelineRunContext, check_id: str, table: str) -> CheckResult:
    result = _table_check(context, check_id, (table,), "TASK09_OUTPUT_QUALITY_FAILED")
    if not result.passed:
        return result
    try:
        connection = duckdb.connect(str(_path(context)), read_only=True)
        try:
            connection.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        finally:
            connection.close()
        return CheckResult.success(check_id, table=table)
    except Exception as exc:
        return CheckResult.failure(check_id, "TASK09_OUTPUT_QUALITY_FAILED", "output table is not queryable", exception=type(exc).__name__)


def _facts_quality(context: PipelineRunContext) -> CheckResult:
    return _task09_quality(context, "task09_facts_quality", SYSTEM_B_DECISION_FACTS_DAILY.name)


def _result_quality(context: PipelineRunContext) -> CheckResult:
    return _task09_quality(context, "task09_result_quality", SYSTEM_B_STRATEGY_RESULT.name)


def _closeout_quality(context: PipelineRunContext) -> CheckResult:
    return _task09_quality(context, "task09_closeout_quality", SYSTEM_B_STRATEGY_CLOSEOUT.name)


def _facts_executor(context: PipelineRunContext) -> BusinessExecution:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("TASK09_TARGET_DATE_MISSING")
    raw = context.parameter_overrides.get("facts_json")
    if raw is None:
        raise ContractError("TASK09_FACTS_INPUT_MISSING")
    try:
        facts = json.loads(raw)
        provenance = json.loads(context.parameter_overrides.get("provenance_json") or "{}")
    except (TypeError, ValueError) as exc:
        raise ContractError("TASK09_FACTS_INPUT_INVALID") from exc
    rows = normalize_decision_facts(target, facts, provenance=provenance)
    connection = duckdb.connect(str(_path(context)))
    try:
        written = persist_decision_facts(connection, rows)
    finally:
        connection.close()
    return BusinessExecution.success(
        metrics=PipelineMetrics(rows_written=written, dates_processed=1, assets_processed=written),
        outputs=(OutputResult("system_b_decision_facts_daily", written, str(_path(context)), True),),
    )


def _load_facts(path: Path, target: date) -> list[dict]:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        rows = connection.execute(
            f"SELECT * EXCLUDE (created_at,provenance_json,producer_version) FROM {SYSTEM_B_DECISION_FACTS_DAILY.name} WHERE trade_date=? ORDER BY ticker",
            [target],
        ).fetchdf()
    finally:
        connection.close()
    return rows.to_dict("records")


def _calendar_status(path: Path, target: date) -> bool | None:
    """Read the formal calendar when available; never infer a trading day."""

    connection = duckdb.connect(str(path), read_only=True)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        if TRADING_CALENDAR.name not in tables:
            raise ContractError("TASK09_CALENDAR_UNAVAILABLE")
        row = connection.execute(
            f"SELECT is_open FROM {TRADING_CALENDAR.name} WHERE trade_date=?", [target]
        ).fetchone()
        if row is None:
            raise ContractError("TASK09_CALENDAR_UNAVAILABLE")
        return bool(row[0])
    finally:
        connection.close()


def _strategy_executor(context: PipelineRunContext) -> BusinessExecution:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("TASK09_TARGET_DATE_MISSING")
    params = context.parameter_overrides
    facts = json.loads(params["facts_json"]) if params.get("facts_json") else _load_facts(_path(context), target)
    holdings = json.loads(params.get("holdings_json") or "[]")
    authorization_input = json.loads(params.get("authorization_json") or "{}")
    candidates = json.loads(params["candidate_asset_ids_json"]) if params.get("candidate_asset_ids_json") else None
    provenance = json.loads(params.get("provenance_json") or "{}")
    result = run_task09_daily(
        trade_date=target,
        facts=facts,
        holdings=holdings,
        authorization_input=authorization_input,
        invocation_id=context.run_id,
        duckdb_path=_path(context),
        candidate_asset_ids=candidates,
        rule_version_set_id=params.get("rule_version_set_id"),
        parameter_set_id=params.get("parameter_set_id"),
        input_snapshot_id=params.get("input_snapshot_id"),
        comparison_score_provenance=provenance,
        trading_day=_calendar_status(_path(context), target),
    )
    return BusinessExecution.success(
        metrics=PipelineMetrics(rows_written=2, dates_processed=1, assets_processed=len(result["target"].positions)),
        outputs=(OutputResult("system_b_strategy_result", 1, str(_path(context)), True, {"strategy_run_id": result["strategy_run_id"]}),),
    )


def _closeout_executor(context: PipelineRunContext) -> BusinessExecution:
    target = context.target_window.target_date
    params = context.parameter_overrides
    if target is None:
        raise ContractError("TASK09_TARGET_DATE_MISSING")
    required = ("strategy_run_id", "result_digest", "target_identity")
    if any(not params.get(key) for key in required):
        raise ContractError("TASK09_CLOSEOUT_INPUT_MISSING")
    identity = closeout_strategy_daily(
        duckdb_path=_path(context),
        strategy_run_id=str(params["strategy_run_id"]),
        trade_date=target,
        result_digest=str(params["result_digest"]),
        target_identity=str(params["target_identity"]),
    )
    return BusinessExecution.success(
        metrics=PipelineMetrics(rows_written=1, dates_processed=1),
        outputs=(OutputResult("system_b_strategy_closeout", 1, str(_path(context)), True, {"completion_identity": identity}),),
    )


def _parameter(name: str, description: str, *, required: bool = False, default: str | None = None) -> ParameterContract:
    return ParameterContract(name, ParameterType.STRING, description, required=required, default=default)


def _facts_contract() -> PipelineContract:
    return PipelineContract(
        pipeline_id="system_b_decision_facts_daily",
        name="System B Decision Facts Daily",
        description="Formal Task09 decision-facts boundary; unavailable business facts remain explicit.",
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=_facts_executor,
        target_date_policy=TASK09_TARGET_DATE_POLICY,
        parameters=(_parameter("facts_json", "JSON array of prepared System B decision facts", required=True), _parameter("provenance_json", "JSON provenance envelope", default="{}")),
        inputs=(),
        outputs=(OutputContract("system_b_decision_facts_daily", "DUCKDB", "quant_db", SYSTEM_B_DECISION_FACTS_DAILY.name, ("trade_date", "ticker", "producer_version", "input_snapshot_id"), WriteMode.UPSERT, "TARGET_DATE", CompletionContract("facts rows persisted", "TASK09_FACTS_COMPLETION_MISSING", _facts_completed), (_facts_quality,), True),),
        dependencies=(), resource_locks=("quant_db_writer",), idempotency=IdempotencyContract("trade_date+fact_provenance", "same fact identity is an immutable replacement of the same snapshot", "UPSERT same snapshot", "transaction rollback", False, "facts upsert"), transaction=TransactionContract(TransactionMode.DATABASE_TRANSACTION, "facts rows", "rollback"), execution=ExecutionPolicy(OverlapPolicy.FORBID, 1), performance=PerformanceBudget(120, 60, 300, "one target date", "Task09 v1"),
    )


def _strategy_contract() -> PipelineContract:
    facts_input = InputContract("decision_facts", InputKind.TABLE, "quant_db.system_b_decision_facts_daily", tuple(SYSTEM_B_DECISION_FACTS_DAILY.column_names()), "TARGET_DATE", "TASK09_FACTS_STRUCTURE_MISSING", _facts_structure, FreshnessContract("task09_facts_freshness", "TARGET_DATE", 0, NonTradingDayPolicy.ALLOW_CALENDAR_DATE, "TASK09_FACTS_UNAVAILABLE", _facts_freshness))
    return PipelineContract(
        pipeline_id="system_b_strategy_daily", name="System B Strategy Daily", description="Formal System B strategy to complete portfolio target.", contract_version="1.0.0", kind=PipelineKind.ATOMIC, executor=_strategy_executor, target_date_policy=TASK09_TARGET_DATE_POLICY,
        parameters=(
            _parameter("facts_json", "Optional prepared facts JSON"),
            _parameter("holdings_json", "JSON holdings snapshot", default="[]"),
            _parameter("authorization_json", "JSON phase/V authorization input", default="{}"),
            _parameter("candidate_asset_ids_json", "Optional explicit candidate IDs JSON"),
            _parameter("provenance_json", "JSON score provenance", default="{}"),
            _parameter("rule_version_set_id", "Rule set identity", required=True),
            _parameter("parameter_set_id", "Parameter identity", required=True),
            _parameter("input_snapshot_id", "Input snapshot identity", required=True),
        ),
        inputs=(facts_input,), outputs=(OutputContract("system_b_strategy_result", "DUCKDB", "quant_db", SYSTEM_B_STRATEGY_RESULT.name, ("strategy_run_id",), WriteMode.UPSERT, "TARGET_DATE", CompletionContract("result and target persisted", "TASK09_RESULT_COMPLETION_MISSING", _completed), (_result_quality,), False),), dependencies=("system_b_state_daily", "system_b_episode_rebuild", "system_b_pool_height", "system_b_pool_capacity", "system_b_pool_recognition", "system_b_asset_rank_daily", "system_b_decision_facts_daily"), resource_locks=("quant_db_writer",), idempotency=IdempotencyContract("strategy_run_id", "same invocation is idempotent; new invocation is immutable history", "reject digest collision", "transaction rollback", True, "result+target"), transaction=TransactionContract(TransactionMode.DATABASE_TRANSACTION, "result and target", "rollback"), execution=ExecutionPolicy(OverlapPolicy.FORBID, 1), performance=PerformanceBudget(300, 120, 600, "one target date", "Task09 v1"),
    )


def _closeout_contract() -> PipelineContract:
    return PipelineContract(
        pipeline_id="system_b_daily_closeout", name="System B Daily Closeout", description="Explicit completion marker for a persisted strategy result and target.", contract_version="1.0.0", kind=PipelineKind.ATOMIC, executor=_closeout_executor, target_date_policy=TASK09_TARGET_DATE_POLICY,
        parameters=tuple(_parameter(name, name, required=True) for name in ("strategy_run_id", "result_digest", "target_identity")), inputs=(), outputs=(OutputContract("system_b_strategy_closeout", "DUCKDB", "quant_db", SYSTEM_B_STRATEGY_CLOSEOUT.name, ("closeout_identity",), WriteMode.UPSERT, "TARGET_DATE", CompletionContract("closeout persisted", "TASK09_CLOSEOUT_COMPLETION_MISSING", _completed), (_closeout_quality,), False),), dependencies=("system_b_strategy_daily",), resource_locks=("quant_db_writer",), idempotency=IdempotencyContract("strategy_run_id", "repeat closeout is idempotent", "upsert same completion", "retry", False, "closeout row"), transaction=TransactionContract(TransactionMode.DATABASE_TRANSACTION, "closeout row", "rollback"), execution=ExecutionPolicy(OverlapPolicy.FORBID, 1), performance=PerformanceBudget(60, 30, 120, "one result", "Task09 v1"),
    )


SYSTEM_B_DECISION_FACTS_DAILY_CONTRACT = register_pipeline(_facts_contract())
SYSTEM_B_STRATEGY_DAILY_CONTRACT = register_pipeline(_strategy_contract())
SYSTEM_B_DAILY_CLOSEOUT_CONTRACT = register_pipeline(_closeout_contract())

__all__ = ["SYSTEM_B_DECISION_FACTS_DAILY_CONTRACT", "SYSTEM_B_STRATEGY_DAILY_CONTRACT", "SYSTEM_B_DAILY_CLOSEOUT_CONTRACT"]
