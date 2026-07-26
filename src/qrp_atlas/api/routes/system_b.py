"""Read-only System B state monitoring API."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from qrp_atlas.api.db import get_db
from qrp_atlas.contracts import (
    SYSTEM_B_2_0_PARAMETER_SET_ID,
    SYSTEM_B_2_0_RULE_VERSION_SET_ID,
    SYSTEM_B_LATEST_STATE_VIEW,
    SYSTEM_B_PRODUCTION_RUN_TABLE,
    SYSTEM_B_STATE_OBSERVATION_TABLE,
)
from qrp_atlas.pipeline.system_b.repository import table_exists


router = APIRouter(prefix="/api/v1/system-b", tags=["System B"])


def _normalize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for row in rows:
        for key, value in tuple(row.items()):
            if hasattr(value, "isoformat"):
                row[key] = value.isoformat()
            elif key in {"source_rule_ids", "diagnostics", "metrics"} and isinstance(value, str):
                try:
                    row[key] = json.loads(value)
                except json.JSONDecodeError:
                    pass
    return rows


def _fetch_dicts(cursor) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return _normalize([dict(zip(columns, row, strict=True)) for row in cursor.fetchall()])


def _require_schema(connection) -> None:
    required = (
        SYSTEM_B_STATE_OBSERVATION_TABLE,
        SYSTEM_B_PRODUCTION_RUN_TABLE,
        SYSTEM_B_LATEST_STATE_VIEW,
    )
    if not all(table_exists(connection, name) for name in required):
        raise HTTPException(status_code=503, detail="SYSTEM_B_SCHEMA_NOT_DEPLOYED")


def _history_sql(where: str) -> str:
    return f"""
        SELECT observation.*, run.completed_at AS production_completed_at
        FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} AS observation
        JOIN {SYSTEM_B_PRODUCTION_RUN_TABLE} AS run
          ON run.production_run_id = observation.production_run_id
         AND run.status = 'SUCCEEDED'
        WHERE {where}
        QUALIFY row_number() OVER (
            PARTITION BY observation.asset_id, observation.trade_date
            ORDER BY run.completed_at DESC NULLS LAST,
                     observation.created_at DESC,
                     observation.input_snapshot_id DESC
        ) = 1
    """


@router.get("/states/latest")
def latest_states(
    rule_version_set_id: str = Query(SYSTEM_B_2_0_RULE_VERSION_SET_ID),
    parameter_set_id: str = Query(SYSTEM_B_2_0_PARAMETER_SET_ID),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    connection = get_db()
    try:
        _require_schema(connection)
        cursor = connection.execute(
            f"""
            SELECT * FROM {SYSTEM_B_LATEST_STATE_VIEW}
            WHERE rule_version_set_id = ? AND parameter_set_id = ?
            ORDER BY asset_id
            LIMIT ? OFFSET ?
            """,
            [rule_version_set_id, parameter_set_id, limit, offset],
        )
        return _fetch_dicts(cursor)
    finally:
        connection.close()


@router.get("/states")
def states_by_date(
    trade_date: date,
    rule_version_set_id: str = Query(SYSTEM_B_2_0_RULE_VERSION_SET_ID),
    parameter_set_id: str = Query(SYSTEM_B_2_0_PARAMETER_SET_ID),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    connection = get_db()
    try:
        _require_schema(connection)
        cursor = connection.execute(
            _history_sql(
                "observation.trade_date = ? AND observation.rule_version_set_id = ? "
                "AND observation.parameter_set_id = ?"
            )
            + " ORDER BY asset_id LIMIT ? OFFSET ?",
            [trade_date, rule_version_set_id, parameter_set_id, limit, offset],
        )
        return _fetch_dicts(cursor)
    finally:
        connection.close()


@router.get("/assets/{asset_id}/history")
def asset_history(
    asset_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    rule_version_set_id: str = Query(SYSTEM_B_2_0_RULE_VERSION_SET_ID),
    parameter_set_id: str = Query(SYSTEM_B_2_0_PARAMETER_SET_ID),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    clauses = [
        "observation.asset_id = ?",
        "observation.rule_version_set_id = ?",
        "observation.parameter_set_id = ?",
    ]
    params: list[Any] = [asset_id, rule_version_set_id, parameter_set_id]
    if start_date is not None:
        clauses.append("observation.trade_date >= ?")
        params.append(start_date)
    if end_date is not None:
        clauses.append("observation.trade_date <= ?")
        params.append(end_date)
    params.extend([limit, offset])
    connection = get_db()
    try:
        _require_schema(connection)
        cursor = connection.execute(
            _history_sql(" AND ".join(clauses))
            + " ORDER BY trade_date DESC LIMIT ? OFFSET ?",
            params,
        )
        return _fetch_dicts(cursor)
    finally:
        connection.close()


@router.get("/transitions")
def transitions(
    trade_date: date,
    rule_version_set_id: str = Query(SYSTEM_B_2_0_RULE_VERSION_SET_ID),
    parameter_set_id: str = Query(SYSTEM_B_2_0_PARAMETER_SET_ID),
):
    connection = get_db()
    try:
        _require_schema(connection)
        cursor = connection.execute(
            "WITH selected AS ("
            + _history_sql(
                "observation.trade_date = ? AND observation.rule_version_set_id = ? "
                "AND observation.parameter_set_id = ?"
            )
            + ") SELECT previous_trend_state, trend_state, count(*) AS count "
            "FROM selected GROUP BY 1, 2 ORDER BY 1, 2",
            [trade_date, rule_version_set_id, parameter_set_id],
        )
        return _fetch_dicts(cursor)
    finally:
        connection.close()


@router.get("/summary")
def summary(
    trade_date: date,
    rule_version_set_id: str = Query(SYSTEM_B_2_0_RULE_VERSION_SET_ID),
    parameter_set_id: str = Query(SYSTEM_B_2_0_PARAMETER_SET_ID),
):
    connection = get_db()
    try:
        _require_schema(connection)
        cursor = connection.execute(
            "WITH selected AS ("
            + _history_sql(
                "observation.trade_date = ? AND observation.rule_version_set_id = ? "
                "AND observation.parameter_set_id = ?"
            )
            + """
            )
            SELECT
                count(*) FILTER (WHERE trend_state = 'NEW_LISTING_WARMUP') AS new_listing_warmup_count,
                count(*) FILTER (WHERE trend_state = 'BASE') AS base_count,
                count(*) FILTER (WHERE trend_state = 'CANDIDATE') AS candidate_count,
                count(*) FILTER (WHERE trend_state = 'ACTIVE') AS active_count,
                count(*) FILTER (WHERE previous_trend_state = 'BASE' AND trend_state = 'CANDIDATE') AS base_to_candidate_count,
                count(*) FILTER (WHERE previous_trend_state = 'CANDIDATE' AND trend_state = 'ACTIVE') AS candidate_to_active_count,
                count(*) FILTER (WHERE previous_trend_state = 'ACTIVE' AND trend_state = 'BASE') AS active_to_base_count,
                count(*) FILTER (WHERE previous_trend_state = 'ACTIVE' AND trend_state = 'ACTIVE') AS active_held_count,
                count(*) FILTER (WHERE is_trading_day = FALSE) AS suspended_hold_count,
                count(*) FILTER (WHERE diagnostics NOT IN ('[]', '["NON_TRADING_DAY_STATE_HELD"]')) AS anomaly_count,
                any_value(rule_version_set_id) AS rule_version_set_id,
                any_value(parameter_set_id) AS parameter_set_id,
                arg_max(production_run_id, created_at) AS production_run_id,
                max(production_completed_at) AS calculation_completed_at
            FROM selected
            """,
            [trade_date, rule_version_set_id, parameter_set_id],
        )
        rows = _fetch_dicts(cursor)
        return rows[0] if rows else {}
    finally:
        connection.close()


@router.get("/production-runs/latest")
def latest_production_run():
    connection = get_db()
    try:
        if not table_exists(connection, SYSTEM_B_PRODUCTION_RUN_TABLE):
            raise HTTPException(status_code=503, detail="SYSTEM_B_SCHEMA_NOT_DEPLOYED")
        cursor = connection.execute(
            f"SELECT * FROM {SYSTEM_B_PRODUCTION_RUN_TABLE} ORDER BY created_at DESC LIMIT 1"
        )
        rows = _fetch_dicts(cursor)
        return rows[0] if rows else None
    finally:
        connection.close()
