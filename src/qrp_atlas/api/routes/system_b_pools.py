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

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException

from qrp_atlas.api.db import get_db
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


def _pool_db_available(connection) -> bool:
    """Check whether pool_db is attached and the membership table exists."""
    try:
        connection.execute(
            f"SELECT count(*) FROM pool_db.{SYSTEM_B_POOL_MEMBERSHIP_TABLE} LIMIT 1"
        ).fetchone()
        return True
    except Exception:
        return False


def _fetch_dicts(cursor) -> list[dict[str, Any]]:
    """Convert a DuckDB cursor result into a list of plain dicts."""
    columns = [item[0] for item in cursor.description]
    rows: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        record: dict[str, Any] = {}
        for key, value in zip(columns, row, strict=True):
            if hasattr(value, "isoformat"):
                record[key] = value.isoformat()
            else:
                record[key] = value
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


@router.get("/pools/snapshot")
def pool_snapshot(
    trade_date: date,
) -> dict[str, Any]:
    """Return the three-pool snapshot for the given trade date (P0-6)."""
    connection = get_db()
    try:
        if not _pool_db_available(connection):
            raise HTTPException(
                status_code=503,
                detail="POOL_DB_NOT_AVAILABLE: pool_db is not attached. "
                "Configure QRP_POOL_DB_PATH on the server.",
            )
        return _build_snapshot(connection, trade_date)
    finally:
        connection.close()


@router.get("/pools/snapshot/latest")
def latest_pool_snapshot() -> dict[str, Any]:
    """Return the snapshot for the latest date where all three pools completed."""
    connection = get_db()
    try:
        if not _pool_db_available(connection):
            raise HTTPException(
                status_code=503,
                detail="POOL_DB_NOT_AVAILABLE: pool_db is not attached. "
                "Configure QRP_POOL_DB_PATH on the server.",
            )

        # Find the latest trade_date where all three pool types have
        # COMPLETED status in pool_db.system_b_pool_run.
        try:
            row = connection.execute(
                f"""
                SELECT trade_date
                FROM pool_db.{SYSTEM_B_POOL_RUN_TABLE}
                WHERE status = 'COMPLETED'
                  AND pool_type IN ('{POOL_HEIGHT}', '{POOL_CAPACITY}', '{POOL_RECOGNITION}')
                GROUP BY trade_date
                HAVING count(DISTINCT pool_type) = 3
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ).fetchone()
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"POOL_RUN_TABLE_NOT_AVAILABLE: {exc}",
            ) from exc

        if not row:
            raise HTTPException(
                status_code=404,
                detail="NO_COMPLETED_POOL_RUN: no trade date has all three "
                "pools completed.",
            )

        latest_date = row[0]
        # DuckDB returns datetime.date for DATE columns; normalise just in case.
        if not isinstance(latest_date, date):
            latest_date = date.fromisoformat(str(latest_date))

        return _build_snapshot(connection, latest_date)
    finally:
        connection.close()
