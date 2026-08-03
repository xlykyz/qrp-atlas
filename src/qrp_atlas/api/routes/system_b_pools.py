"""Pool snapshot API routes for System B stock pools (P0-6).

Exposes two endpoints that read from the pool database (attached as
``pool_db`` via ``get_db()``):

  - ``GET /api/v1/system-b/pools/snapshot?trade_date={date}``
  - ``GET /api/v1/system-b/pools/snapshot/latest``

Both endpoints query ``pool_db.system_b_pool_membership_daily`` directly
through the ATTACH'd read-only connection.  If ``pool_db`` is not attached
(path not configured or file missing), endpoints return HTTP 503 with a
clear ``detail`` message.

NOTE: The actual pool-db file path must be configured on the Linux server
via the ``QRP_POOL_DB_PATH`` environment variable.  See ``.env.example``.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, HTTPException

from qrp_atlas.api.db import (
    detach_database_if_attached,
    get_db,
    require_pool_db,
)
from qrp_atlas.api.schemas.system_b import PoolSnapshotResponse
from qrp_atlas.api.system_b_serialization import serialize_system_b_value
from qrp_atlas.contracts import (
    IN_POOL,
    POOL_CAPACITY,
    POOL_HEIGHT,
    POOL_RECOGNITION,
    SYSTEM_B_POOL_MEMBERSHIP_TABLE,
    SYSTEM_B_POOL_RUN_TABLE,
)

router = APIRouter(prefix="/api/v1/system-b", tags=["System B Pools"])

POOL_TYPES: tuple[str, ...] = (POOL_HEIGHT, POOL_CAPACITY, POOL_RECOGNITION)


def _require_pool_schema(connection) -> None:
    """Ensure both tables needed by the snapshot query are present."""
    try:
        for table in (SYSTEM_B_POOL_MEMBERSHIP_TABLE, SYSTEM_B_POOL_RUN_TABLE):
            connection.execute(f"SELECT 1 FROM pool_db.{table} LIMIT 0")
    except Exception as exc:
        raise HTTPException(status_code=503, detail="POOL_DB_SCHEMA_NOT_DEPLOYED") from exc


def _fetch_dicts(cursor) -> list[dict[str, Any]]:
    """Convert a DuckDB cursor result into a list of plain dicts."""
    columns = [item[0] for item in cursor.description]
    rows: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        record: dict[str, Any] = {}
        for key, value in zip(columns, row, strict=True):
            record[key] = serialize_system_b_value(value)
        rows.append(record)
    return rows


def _build_snapshot(connection, trade_date: date) -> dict[str, Any]:
    """Query pool_db for one trade date and build the snapshot response."""
    cursor = connection.execute(
        f"""
        SELECT
            asset_id,
            pool_type,
            membership_state,
            pool_cycle_no,
            entry_date,
            exit_date,
            episode_id
        FROM pool_db.{SYSTEM_B_POOL_MEMBERSHIP_TABLE}
        WHERE trade_date = ?
          AND membership_state = ?
        ORDER BY pool_type, asset_id
        """,
        [trade_date, IN_POOL],
    )
    rows = _fetch_dicts(cursor)

    members_by_pool: dict[str, list[dict[str, Any]]] = {
        pool: [] for pool in POOL_TYPES
    }
    for row in rows:
        pool_type = row.get("pool_type", "")
        if pool_type in members_by_pool:
            members_by_pool[pool_type].append(row)

    pools = [
        {
            "pool_type": pool,
            "count": len(members_by_pool[pool]),
            "members": members_by_pool[pool],
        }
        for pool in POOL_TYPES
    ]
    return {"trade_date": trade_date.isoformat(), "pools": pools}


def _require_completed_pool_snapshot(connection, trade_date: date) -> None:
    """Require one completed run for each of the three pool types."""
    try:
        row = connection.execute(
            f"""
            SELECT count(DISTINCT pool_type)
            FROM pool_db.{SYSTEM_B_POOL_RUN_TABLE}
            WHERE trade_date = ?
              AND status = 'COMPLETED'
              AND pool_type IN (?, ?, ?)
            """,
            [trade_date, *POOL_TYPES],
        ).fetchone()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="POOL_RUN_TABLE_NOT_AVAILABLE") from exc
    if not row or row[0] != len(POOL_TYPES):
        raise HTTPException(status_code=404, detail="POOL_SNAPSHOT_NOT_READY")


def _latest_completed_pool_date(connection) -> date | None:
    """Return the latest date with all three completed pool runs."""
    try:
        row = connection.execute(
            f"""
            SELECT trade_date
            FROM pool_db.{SYSTEM_B_POOL_RUN_TABLE}
            WHERE status = 'COMPLETED'
              AND pool_type IN (?, ?, ?)
            GROUP BY trade_date
            HAVING count(DISTINCT pool_type) = ?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            [*POOL_TYPES, len(POOL_TYPES)],
        ).fetchone()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="POOL_RUN_TABLE_NOT_AVAILABLE") from exc
    if not row:
        return None
    latest_date = row[0]
    if isinstance(latest_date, datetime):
        return latest_date.date()
    if isinstance(latest_date, date):
        return latest_date
    return date.fromisoformat(str(latest_date))


@router.get("/pools/snapshot", response_model=PoolSnapshotResponse)
def pool_snapshot(
    trade_date: date,
) -> dict[str, Any]:
    """Return the three-pool snapshot for the given trade date (P0-6)."""
    connection = get_db()
    attached_alias: str | None = None
    try:
        attached_alias = require_pool_db(connection)
        _require_pool_schema(connection)
        _require_completed_pool_snapshot(connection, trade_date)
        return _build_snapshot(connection, trade_date)
    finally:
        if attached_alias is not None:
            detach_database_if_attached(connection, attached_alias)
        connection.close()


@router.get("/pools/snapshot/latest", response_model=PoolSnapshotResponse)
def latest_pool_snapshot() -> dict[str, Any]:
    """Return the snapshot for the latest date where all three pools completed."""
    connection = get_db()
    attached_alias: str | None = None
    try:
        attached_alias = require_pool_db(connection)
        _require_pool_schema(connection)
        latest_date = _latest_completed_pool_date(connection)
        if latest_date is None:
            raise HTTPException(
                status_code=404,
                detail="NO_COMPLETED_POOL_RUN: no trade date has all three "
                "pools completed.",
            )
        return _build_snapshot(connection, latest_date)
    finally:
        if attached_alias is not None:
            detach_database_if_attached(connection, attached_alias)
        connection.close()
