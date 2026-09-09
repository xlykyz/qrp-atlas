"""Task09 System B daily adapter and business-result persistence.

This module is intentionally an adapter around the registered authorization and
portfolio strategies.  It owns the formal facts boundary and business result
identity; it does not implement a second scheduler or a replacement decision
policy.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    SYSTEM_B_DECISION_FACTS_DAILY,
    SYSTEM_B_STRATEGY_CLOSEOUT,
    SYSTEM_B_STRATEGY_RESULT,
    SYSTEM_B_STRATEGY_TARGET,
    MARKET_PHASE,
    PHASE,
    TRADE_DATE,
    TICKER,
    V_TRIGGERED,
)
from qrp_atlas.strategies import get_strategy, run_strategy
from qrp_atlas.strategies.builtin.system_b_decision import (
    COMPARISON_SCORE,
    ENTRY_ELIGIBLE,
    SEVERE_ABNORMAL_SUPERVISION_STATUS,
    SYSTEM_B_EXIT_TRIGGERED,
)
from qrp_atlas.strategies.models import (
    StrategyHoldingState,
    StrategyInput,
    StrategyPortfolioTarget,
    StrategyRunResult,
)

CHINA_TZ = ZoneInfo("Asia/Shanghai")
TASK09_FACTS_PRODUCER_VERSION = "task09_decision_facts_v0.1"
TARGET_CANONICAL_SCHEMA = "system_b_target@1"
TARGET_HASH_ALGORITHM = "sha256"


def resolve_task09_target_date(
    scheduled_for: datetime,
    explicit_trade_date: date | None = None,
) -> date:
    """Resolve the single Task09 date contract, rejecting datetime overrides."""

    if explicit_trade_date is not None:
        if isinstance(explicit_trade_date, datetime) or not isinstance(explicit_trade_date, date):
            raise ValueError("TASK09_TARGET_DATE_INVALID")
        return explicit_trade_date
    if scheduled_for.tzinfo is None or scheduled_for.utcoffset() is None:
        raise ValueError("TASK09_SCHEDULE_TIMEZONE_MISSING")
    return scheduled_for.astimezone(CHINA_TZ).date()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _normalized_weight(value: Any) -> str:
    try:
        if not math.isfinite(float(value)):
            raise ValueError
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("TASK09_TARGET_WEIGHT_INVALID") from exc
    decimal = Decimal(str(value)).quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_EVEN)
    return format(decimal, ".12f")


def canonical_target_payload(target: StrategyPortfolioTarget) -> dict[str, Any]:
    """Return the frozen canonical representation, excluding diagnostics/evidence."""

    seen_asset_ids: set[str] = set()
    positions = []
    for position in target.positions:
        asset_id = str(position.asset_id).strip()
        if not asset_id:
            raise ValueError("TASK09_TARGET_ASSET_ID_INVALID")
        if asset_id in seen_asset_ids:
            raise ValueError("TASK09_TARGET_ASSET_ID_DUPLICATE")
        seen_asset_ids.add(asset_id)
        positions.append({
            "asset_id": asset_id,
            "target_weight": _normalized_weight(position.target_weight),
            "reason_code": position.reason_code,
        })
    positions.sort(key=lambda item: item["asset_id"])
    return {
        "schema": TARGET_CANONICAL_SCHEMA,
        "trade_date": target.trade_date,
        "strategy_code": target.strategy_code,
        "strategy_version": target.strategy_version,
        "positions": positions,
    }


def canonical_target_json(target: StrategyPortfolioTarget) -> str:
    return _canonical_json(canonical_target_payload(target))


def target_digest(target: StrategyPortfolioTarget) -> str:
    return hashlib.sha256(canonical_target_json(target).encode("utf-8")).hexdigest()


def strategy_run_identity(
    *,
    trade_date: date,
    strategy_code: str,
    strategy_version: str,
    rule_version_set_id: str | None,
    parameter_set_id: str | None,
    input_snapshot_id: str | None,
    parameters: Mapping[str, Any] | None = None,
    authorization_input: Mapping[str, Any] | None = None,
    authorization_status: bool | None = None,
    candidate_asset_ids: Sequence[str] = (),
    holdings: Mapping[str, StrategyHoldingState] | None = None,
    comparison_score_provenance: Mapping[str, Any] | None = None,
    invocation_id: str | None = None,
) -> str:
    """Return a stable business identity; invocation_id is deliberately ignored.

    ``invocation_id`` remains an accepted keyword for compatibility with
    callers during the Task09 migration.  It is job-runtime metadata, not a
    business input, and must never alter persisted strategy identity.
    """

    envelope = {
        "schema": "system_b_strategy_invocation@1",
        "trade_date": trade_date.isoformat(),
        "strategy_code": strategy_code,
        "strategy_version": strategy_version,
        "rule_version_set_id": rule_version_set_id,
        "parameter_set_id": parameter_set_id,
        "input_snapshot_id": input_snapshot_id,
        "parameters": dict(parameters or {}),
        "authorization_input": dict(authorization_input or {}),
        "authorization_status": authorization_status,
        "candidate_asset_ids": sorted(candidate_asset_ids),
        "holdings": [
            state.to_dict()
            for _, state in sorted((holdings or {}).items())
        ],
        "comparison_score_provenance": dict(comparison_score_provenance or {}),
    }
    return hashlib.sha256(_canonical_json(envelope).encode("utf-8")).hexdigest()


def _status(value: Any, *, true_status: str, false_status: str) -> tuple[Any, str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None, "UNAVAILABLE"
    if not isinstance(value, bool):
        raise ValueError("TASK09_FACTS_BOOLEAN_INVALID")
    return value, true_status if value else false_status


def normalize_decision_facts(
    trade_date: date,
    facts: Sequence[Mapping[str, Any]],
    *,
    provenance: Mapping[str, Any] | None = None,
    producer_version: str = TASK09_FACTS_PRODUCER_VERSION,
) -> list[dict[str, Any]]:
    """Normalize the formal facts boundary without inventing missing producers."""

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    provenance = dict(provenance or {})
    # The frozen fixture uses concise external names.  The existing Task07
    # decision strategy consumes the fully named provenance contract.
    aliases = {
        "score_version": "score_calculation_version",
        "rule_version": "rule_version_set_id",
        "parameter_version": "parameter_set_id",
    }
    for source, target in aliases.items():
        if target not in provenance and source in provenance:
            provenance[target] = provenance[source]
    provenance.setdefault(TRADE_DATE, trade_date.isoformat())
    for raw in facts:
        if not isinstance(raw, Mapping):
            raise ValueError("TASK09_FACTS_INVALID_ROW")
        asset_id = str(raw.get(TICKER, raw.get("asset_id", ""))).strip()
        if not asset_id or asset_id in seen:
            raise ValueError("TASK09_FACTS_ASSET_DOMAIN_INVALID")
        seen.add(asset_id)
        score = raw.get(COMPARISON_SCORE)
        if score is not None and not (isinstance(score, (int, float)) and not isinstance(score, bool) and pd.notna(score)):
            raise ValueError("TASK09_FACTS_COMPARISON_SCORE_INVALID")
        entry_value, entry_status = _status(raw.get(ENTRY_ELIGIBLE), true_status="ELIGIBLE", false_status="INELIGIBLE")
        exit_value, exit_status = _status(raw.get(SYSTEM_B_EXIT_TRIGGERED), true_status="TRIGGERED", false_status="NOT_TRIGGERED")
        supervision = raw.get(SEVERE_ABNORMAL_SUPERVISION_STATUS)
        supervision_status = str(supervision).strip() if supervision not in (None, "") else "UNAVAILABLE"
        candidate = raw.get("candidate_membership")
        candidate_value, candidate_status = _status(candidate, true_status="MEMBER", false_status="NOT_MEMBER")
        normalized.append({
            TRADE_DATE: trade_date,
            TICKER: asset_id,
            COMPARISON_SCORE: float(score) if score is not None else None,
            "comparison_score_status": "AVAILABLE" if score is not None else "UNAVAILABLE",
            ENTRY_ELIGIBLE: entry_value,
            "entry_eligibility_status": entry_status,
            SYSTEM_B_EXIT_TRIGGERED: exit_value,
            "exit_status": exit_status,
            SEVERE_ABNORMAL_SUPERVISION_STATUS: supervision_status,
            "candidate_membership": candidate_value,
            "candidate_membership_status": candidate_status,
            "score_calculation_version": raw.get("score_calculation_version") or provenance.get("score_calculation_version") or "UNAVAILABLE",
            "rule_version_set_id": raw.get("rule_version_set_id") or provenance.get("rule_version_set_id") or "UNAVAILABLE",
            "parameter_set_id": raw.get("parameter_set_id") or provenance.get("parameter_set_id") or "UNAVAILABLE",
            "input_snapshot_id": raw.get("input_snapshot_id", provenance.get("input_snapshot_id")) or "UNAVAILABLE",
            "producer_version": raw.get("producer_version") or producer_version,
            "provenance_json": _canonical_json(provenance),
        })
    return sorted(normalized, key=lambda row: row[TICKER])


def _create_task09_tables(connection: duckdb.DuckDBPyConnection) -> None:
    for schema in (
        SYSTEM_B_DECISION_FACTS_DAILY,
        SYSTEM_B_STRATEGY_RESULT,
        SYSTEM_B_STRATEGY_TARGET,
        SYSTEM_B_STRATEGY_CLOSEOUT,
    ):
        connection.execute(schema.duckdb_create_sql())


def persist_decision_facts(
    connection: duckdb.DuckDBPyConnection,
    rows: Sequence[Mapping[str, Any]],
    *,
    created_at: datetime | None = None,
) -> int:
    _create_task09_tables(connection)
    timestamp = created_at or datetime.now(UTC)
    if rows:
        _decision_facts_provenance(rows)
    transaction_started = False
    try:
        connection.execute("BEGIN TRANSACTION")
        transaction_started = True
        for row in rows:
            connection.execute(
            """INSERT INTO system_b_decision_facts_daily
            (trade_date,ticker,comparison_score,comparison_score_status,entry_eligible,
             entry_eligibility_status,system_b_exit_triggered,exit_status,
             severe_abnormal_supervision_status,candidate_membership,candidate_membership_status,
             score_calculation_version,rule_version_set_id,parameter_set_id,input_snapshot_id,
             producer_version,provenance_json,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (trade_date,ticker,producer_version,input_snapshot_id) DO UPDATE SET
              comparison_score=excluded.comparison_score,
              comparison_score_status=excluded.comparison_score_status,
              entry_eligible=excluded.entry_eligible,
              entry_eligibility_status=excluded.entry_eligibility_status,
              system_b_exit_triggered=excluded.system_b_exit_triggered,
              exit_status=excluded.exit_status,
              severe_abnormal_supervision_status=excluded.severe_abnormal_supervision_status,
              candidate_membership=excluded.candidate_membership,
              candidate_membership_status=excluded.candidate_membership_status,
              provenance_json=excluded.provenance_json""",
                [row.get(key) for key in (
                    TRADE_DATE, TICKER, COMPARISON_SCORE, "comparison_score_status", ENTRY_ELIGIBLE,
                    "entry_eligibility_status", SYSTEM_B_EXIT_TRIGGERED, "exit_status",
                    SEVERE_ABNORMAL_SUPERVISION_STATUS, "candidate_membership", "candidate_membership_status",
                    "score_calculation_version", "rule_version_set_id", "parameter_set_id", "input_snapshot_id",
                    "producer_version", "provenance_json")]
                + [timestamp],
            )
        connection.execute("COMMIT")
    except Exception:
        if transaction_started:
            connection.execute("ROLLBACK")
        raise
    return len(rows)


_FACT_IDENTITY_COLUMNS = (
    "score_calculation_version",
    "rule_version_set_id",
    "parameter_set_id",
    "input_snapshot_id",
    "producer_version",
    "provenance_json",
)


def _normalized_provenance(provenance: Mapping[str, Any] | None, trade_date: date) -> dict[str, Any]:
    value = dict(provenance or {})
    for source, target in (
        ("score_version", "score_calculation_version"),
        ("rule_version", "rule_version_set_id"),
        ("parameter_version", "parameter_set_id"),
    ):
        if target not in value and source in value:
            value[target] = value[source]
    value.setdefault(TRADE_DATE, trade_date.isoformat())
    return value


def _decision_facts_provenance(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Validate and return the one formal snapshot identity for a facts set."""

    if not rows:
        raise ValueError("TASK09_FACTS_UNAVAILABLE")
    identities = {
        tuple(row.get(column) for column in _FACT_IDENTITY_COLUMNS)
        for row in rows
    }
    if len(identities) != 1:
        raise ValueError("TASK09_FACTS_PROVENANCE_MISMATCH")
    identity = dict(zip(_FACT_IDENTITY_COLUMNS, identities.pop(), strict=True))
    if any(not isinstance(identity[column], str) or not identity[column].strip() for column in _FACT_IDENTITY_COLUMNS):
        raise ValueError("TASK09_FACTS_PROVENANCE_INVALID")
    try:
        provenance = json.loads(identity.pop("provenance_json"))
    except (TypeError, ValueError) as exc:
        raise ValueError("TASK09_FACTS_PROVENANCE_INVALID") from exc
    if not isinstance(provenance, dict):
        raise ValueError("TASK09_FACTS_PROVENANCE_INVALID")
    provenance.update(identity)
    return provenance


def load_persisted_decision_facts(path: str | Path, trade_date: date) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Load exactly one dated facts snapshot, failing closed on ambiguity."""

    connection = duckdb.connect(str(path), read_only=True)
    try:
        rows = connection.execute(
            f"SELECT * EXCLUDE (created_at) FROM {SYSTEM_B_DECISION_FACTS_DAILY.name} WHERE trade_date=? ORDER BY ticker",
            [trade_date],
        ).fetchdf().to_dict("records")
    finally:
        connection.close()
    provenance = _decision_facts_provenance(rows)
    if str(provenance.get(TRADE_DATE, trade_date.isoformat())) != trade_date.isoformat():
        raise ValueError("TASK09_FACTS_PROVENANCE_MISMATCH")
    return rows, provenance


def _holdings(value: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None) -> dict[str, StrategyHoldingState]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        items = [{"asset_id": key, **(item if isinstance(item, Mapping) else {"current_weight": item})} for key, item in value.items()]
    else:
        items = list(value)
    return {
        str(item["asset_id"]): StrategyHoldingState(
            asset_id=str(item["asset_id"]),
            current_weight=float(item["current_weight"]),
            entry_count=int(item.get("entry_count", 1)),
            first_entry_date=item.get("first_entry_date"),
            last_entry_date=item.get("last_entry_date"),
        )
        for item in items
    }


def _authorization_status(
    trade_date: date,
    authorization_input: Mapping[str, Any] | None,
) -> tuple[bool | None, dict[str, Any]]:
    if not authorization_input or PHASE not in authorization_input or V_TRIGGERED not in authorization_input:
        return None, {"status": "UNAVAILABLE", "trade_date": trade_date.isoformat()}
    frame = pd.DataFrame([{TRADE_DATE: trade_date.isoformat(), PHASE: authorization_input[PHASE], V_TRIGGERED: authorization_input[V_TRIGGERED]}])
    result = run_strategy("system_b_authorization", StrategyInput(prepared_data=frame))
    if len(result.authorizations) != 1:
        raise ValueError("TASK09_AUTHORIZATION_RESULT_INVALID")
    authorization = result.authorizations[0]
    return authorization.is_authorized, authorization.to_dict()


def _resolve_authorization_input(
    trade_date: date,
    authorization_input: Mapping[str, Any] | None,
    duckdb_path: str | Path,
) -> Mapping[str, Any] | None:
    """Use caller precedence, otherwise read the formal market-phase fact."""

    if authorization_input:
        return authorization_input
    if not Path(duckdb_path).exists():
        return None
    connection = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        if MARKET_PHASE.name not in tables:
            return None
        row = connection.execute(
            f"SELECT {PHASE}, {V_TRIGGERED} FROM {MARKET_PHASE.name} WHERE {TRADE_DATE}=?",
            [trade_date],
        ).fetchone()
        if row is None:
            return None
        return {PHASE: row[0], V_TRIGGERED: row[1]}
    finally:
        connection.close()


def run_task09_daily(
    *,
    trade_date: date,
    facts: Sequence[Mapping[str, Any]],
    holdings: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
    authorization_input: Mapping[str, Any] | None,
    invocation_id: str | None = None,
    duckdb_path: str | Path,
    candidate_asset_ids: Sequence[str] | None = None,
    strategy_code: str = "system_b_portfolio",
    strategy_version: str | None = None,
    rule_version_set_id: str | None = None,
    parameter_set_id: str | None = None,
    input_snapshot_id: str | None = None,
    comparison_score_provenance: Mapping[str, Any] | None = None,
    trading_day: bool | None = None,
) -> dict[str, Any]:
    """Execute one formal Task09 daily result and atomically persist result+target."""

    if isinstance(trade_date, datetime) or not isinstance(trade_date, date):
        raise ValueError("TASK09_TARGET_DATE_INVALID")
    normalized = normalize_decision_facts(trade_date, facts, provenance=comparison_score_provenance)
    supplied_provenance = _normalized_provenance(comparison_score_provenance, trade_date)
    supplied_identity = {
        "rule_version_set_id": rule_version_set_id,
        "parameter_set_id": parameter_set_id,
        "input_snapshot_id": input_snapshot_id,
    }
    if normalized:
        facts_provenance = _decision_facts_provenance(normalized)
        for key, expected in supplied_identity.items():
            actual = facts_provenance[key]
            if expected is not None and expected != actual:
                raise ValueError("TASK09_FACTS_PROVENANCE_MISMATCH")
        for key in ("score_calculation_version", "rule_version_set_id", "parameter_set_id", "input_snapshot_id"):
            expected = supplied_provenance.get(key)
            if expected is not None and expected != facts_provenance[key]:
                raise ValueError("TASK09_FACTS_PROVENANCE_MISMATCH")
        provenance = _normalized_provenance(facts_provenance, trade_date)
    else:
        provenance = supplied_provenance
        for key, expected in supplied_identity.items():
            if expected is not None:
                provenance.setdefault(key, expected)
        provenance.setdefault("producer_version", TASK09_FACTS_PRODUCER_VERSION)
        if trading_day is False:
            provenance.setdefault("score_calculation_version", "NO_OP")
    for key in ("rule_version_set_id", "parameter_set_id", "input_snapshot_id", "score_calculation_version"):
        value = provenance.get(key)
        if not isinstance(value, str) or not value.strip() or value == "UNAVAILABLE":
            raise ValueError("TASK09_STRATEGY_VERSION_IDENTITY_MISSING")
    rule_version_set_id = str(provenance["rule_version_set_id"])
    parameter_set_id = str(provenance["parameter_set_id"])
    input_snapshot_id = str(provenance["input_snapshot_id"])
    fact_asset_ids = {str(row[TICKER]) for row in normalized}
    if candidate_asset_ids is None:
        candidate_asset_ids = [row[TICKER] for row in normalized if row["candidate_membership"] is True]
    else:
        candidate_asset_ids = [str(asset).strip() for asset in candidate_asset_ids if str(asset).strip()]
        if not set(candidate_asset_ids).issubset(fact_asset_ids):
            raise ValueError("TASK09_CANDIDATE_DOMAIN_INVALID")
    strategy = get_strategy(strategy_code, strategy_version)
    actual_version = strategy.definition.version
    holding_map = _holdings(holdings)
    if trading_day is False:
        auth_value = None
        auth_record = {"status": "NO_OP", "trade_date": trade_date.isoformat()}
        target = StrategyPortfolioTarget(
            trade_date=trade_date.isoformat(), strategy_code=strategy.definition.code,
            strategy_version=strategy.definition.version, positions=(), diagnostics=("NON_TRADING_DAY_NO_OP",),
        )
        result = StrategyRunResult(definition=strategy.definition, parameters={}, portfolio_targets=(target,), diagnostics=("NON_TRADING_DAY_NO_OP",))
        result_status = "NO_OP"
        target_kind = "NO_OP"
    else:
        authorization_input = _resolve_authorization_input(trade_date, authorization_input, duckdb_path)
        auth_value, auth_record = _authorization_status(trade_date, authorization_input)
        prepared = pd.DataFrame(normalized)
        strategy_input = StrategyInput(
            prepared_data=prepared[[TICKER, TRADE_DATE, COMPARISON_SCORE, ENTRY_ELIGIBLE, SYSTEM_B_EXIT_TRIGGERED, SEVERE_ABNORMAL_SUPERVISION_STATUS]],
            runtime_context={
                "authorization": auth_value,
                "candidate_asset_ids": frozenset(candidate_asset_ids),
                "comparison_score_provenance": provenance,
            },
            holdings=holding_map,
            holdings_as_of_date=trade_date.isoformat(),
        )
        result = run_strategy(strategy_code, strategy_input, actual_version)
        if len(result.portfolio_targets) != 1:
            raise ValueError("TASK09_TARGET_NOT_COMPLETE")
        target = result.portfolio_targets[0]
        result_status = "SUCCESS"
        target_kind = "PORTFOLIO"
    digest = target_digest(target)
    run_id = strategy_run_identity(
        trade_date=trade_date,
        strategy_code=result.definition.code,
        strategy_version=result.definition.version,
        rule_version_set_id=rule_version_set_id,
        parameter_set_id=parameter_set_id,
        input_snapshot_id=input_snapshot_id,
        parameters=result.parameters,
        authorization_input=authorization_input,
        authorization_status=auth_value,
        candidate_asset_ids=candidate_asset_ids,
        holdings=holding_map,
        comparison_score_provenance=provenance,
    )
    result_payload = result.to_dict()
    result_payload["authorization"] = auth_record
    result_json = _canonical_json(result_payload)
    result_digest = hashlib.sha256(_canonical_json({
        "schema": "system_b_strategy_result@1",
        "strategy_run_id": run_id,
        "result_status": result_status,
        "authorization_json": auth_record,
        "decisions": result_payload["decisions"],
        "target_digest": digest,
    }).encode("utf-8")).hexdigest()
    input_provenance = {
        "facts_producer_version": provenance["producer_version"],
        "comparison_score_provenance": provenance,
        "authorization": auth_record,
        "candidate_asset_ids": sorted(candidate_asset_ids),
    }
    timestamp = datetime.now(UTC)
    path = Path(duckdb_path)
    connection = duckdb.connect(str(path))
    transaction_started = False
    try:
        _create_task09_tables(connection)
        connection.execute("BEGIN TRANSACTION")
        transaction_started = True
        existing = connection.execute("SELECT result_digest FROM system_b_strategy_result WHERE strategy_run_id=?", [run_id]).fetchone()
        if existing and existing[0] != result_digest:
            raise ValueError("TASK09_STRATEGY_RUN_ID_COLLISION")
        existing_target = connection.execute(
            "SELECT target_digest, canonical_target_json, target_kind FROM system_b_strategy_target WHERE strategy_run_id=? AND target_identity=?",
            [run_id, digest],
        ).fetchone()
        target_json = canonical_target_json(target)
        if existing_target and tuple(existing_target) != (digest, target_json, target_kind):
            raise ValueError("TASK09_TARGET_IDENTITY_CONFLICT")
        connection.execute(
            """INSERT INTO system_b_strategy_result
            (strategy_run_id,trade_date,strategy_code,strategy_version,rule_version_set_id,parameter_set_id,
             input_snapshot_id,input_provenance_json,parameters_json,authorization_json,result_json,result_digest,result_status,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (strategy_run_id) DO NOTHING""",
            [run_id, trade_date, result.definition.code, result.definition.version, rule_version_set_id,
             parameter_set_id, input_snapshot_id, _canonical_json(input_provenance), _canonical_json(result.parameters), _canonical_json(auth_record), result_json, result_digest, result_status, timestamp],
        )
        connection.execute(
            """INSERT INTO system_b_strategy_target
            (strategy_run_id,target_identity,trade_date,strategy_code,strategy_version,target_kind,target_digest,canonical_target_json,created_at)
            VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT (strategy_run_id,target_identity) DO NOTHING""",
            [run_id, digest, trade_date, target.strategy_code, target.strategy_version, target_kind, digest, target_json, timestamp],
        )
        connection.execute("COMMIT")
    except Exception:
        if transaction_started:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    return {"strategy_run_id": run_id, "result_digest": result_digest, "target_identity": digest, "target": target, "status": result_status}


def closeout_strategy_daily(
    *,
    duckdb_path: str | Path,
    strategy_run_id: str | None = None,
    trade_date: date,
    result_digest: str | None = None,
    target_identity: str | None = None,
) -> str:
    connection = duckdb.connect(str(duckdb_path))
    transaction_started = False
    try:
        _create_task09_tables(connection)
        connection.execute("BEGIN TRANSACTION")
        transaction_started = True
        rows = connection.execute(
            """SELECT r.strategy_run_id, r.trade_date, r.result_digest, r.result_status,
                      r.strategy_code, r.strategy_version, r.rule_version_set_id,
                      r.parameter_set_id, r.input_snapshot_id, t.target_identity, t.target_digest
                 FROM system_b_strategy_result r
                 JOIN system_b_strategy_target t ON t.strategy_run_id=r.strategy_run_id
                WHERE r.trade_date=?""",
            [trade_date],
        ).fetchall()
        if not rows:
            raise ValueError("TASK09_CLOSEOUT_RESULT_UNAVAILABLE")
        if len(rows) != 1:
            raise ValueError("TASK09_CLOSEOUT_RESULT_AMBIGUOUS")
        result_row = rows[0]
        actual_run_id, _, actual_result_digest, completion_status, strategy_code, strategy_version, rule_version_set_id, parameter_set_id, snapshot_id, actual_target_identity, target_digest_value = result_row
        if target_digest_value != actual_target_identity:
            raise ValueError("TASK09_CLOSEOUT_TARGET_IDENTITY_INVALID")
        if ((strategy_run_id is not None and strategy_run_id != actual_run_id)
                or (result_digest is not None and result_digest != actual_result_digest)
                or (target_identity is not None and target_identity != actual_target_identity)):
            raise ValueError("TASK09_CLOSEOUT_RESULT_MISMATCH")
        completion_identity = hashlib.sha256(_canonical_json({
            "schema": "system_b_strategy_closeout@1",
            "strategy_run_id": actual_run_id,
            "trade_date": trade_date.isoformat(),
            "result_digest": actual_result_digest,
            "target_identity": actual_target_identity,
            "status": completion_status,
        }).encode("utf-8")).hexdigest()
        existing = connection.execute(
            "SELECT closeout_identity, result_digest, target_identity, completion_status FROM system_b_strategy_closeout WHERE strategy_run_id=?",
            [actual_run_id],
        ).fetchone()
        if existing is not None:
            if tuple(existing) != (completion_identity, actual_result_digest, actual_target_identity, completion_status):
                raise ValueError("TASK09_CLOSEOUT_IDENTITY_CONFLICT")
            connection.execute("COMMIT")
            return completion_identity
        connection.execute(
            """INSERT INTO system_b_strategy_closeout
            (closeout_identity,strategy_run_id,trade_date,strategy_code,strategy_version,result_digest,target_identity,target_digest,rule_version_set_id,parameter_set_id,input_snapshot_id,completion_identity,completion_status,completed_at,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [completion_identity, actual_run_id, trade_date, strategy_code, strategy_version, actual_result_digest, actual_target_identity, target_digest_value, rule_version_set_id, parameter_set_id, snapshot_id, completion_identity, completion_status, datetime.now(UTC), datetime.now(UTC)],
        )
        connection.execute("COMMIT")
    except Exception:
        if transaction_started:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    return completion_identity


__all__ = [
    "TASK09_FACTS_PRODUCER_VERSION",
    "canonical_target_payload",
    "canonical_target_json",
    "target_digest",
    "strategy_run_identity",
    "normalize_decision_facts",
    "persist_decision_facts",
    "load_persisted_decision_facts",
    "resolve_task09_target_date",
    "run_task09_daily",
    "closeout_strategy_daily",
]
