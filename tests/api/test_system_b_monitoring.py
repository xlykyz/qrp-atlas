from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.api import db as api_db
from qrp_atlas.api.server import app
from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import (
    IN_POOL,
    POOL_CAPACITY,
    POOL_HEIGHT,
    POOL_RECOGNITION,
    SYSTEM_B_EPISODE,
    SYSTEM_B_EPISODE_OBSERVATION,
    SYSTEM_B_EPISODE_OBSERVATION_TABLE,
    SYSTEM_B_EPISODE_RULE_VERSION,
    SYSTEM_B_EPISODE_TABLE,
    SYSTEM_B_POOL_MEMBERSHIP,
    SYSTEM_B_POOL_MEMBERSHIP_TABLE,
    SYSTEM_B_POOL_RULE_VERSION,
    SYSTEM_B_POOL_RUN,
    SYSTEM_B_POOL_RUN_TABLE,
)

from .asgi_client import ASGITestClient


POOL_TYPES = (POOL_HEIGHT, POOL_CAPACITY, POOL_RECOGNITION)
TRADE_DATE = date(2026, 7, 31)
OTHER_DATE = date(2026, 8, 1)
NOW = datetime(2026, 8, 1, 10, 0, 0)


def _configure_api(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    episode_path: Path | None = None,
    pool_path: Path | None = None,
) -> ASGITestClient:
    main_path = tmp_path / "main.duckdb"
    main = duckdb.connect(str(main_path))
    main.close()
    overrides = {
        "QRP_DUCKDB_PATH": str(main_path),
        "QRP_READ_ONLY": "true",
    }
    if episode_path is not None:
        overrides["QRP_EPISODE_DB_PATH"] = str(episode_path)
    if pool_path is not None:
        overrides["QRP_POOL_DB_PATH"] = str(pool_path)
    settings = AppSettings.load(
        overrides=overrides,
        environ={},
        project_root=tmp_path,
    )
    monkeypatch.setattr(api_db, "get_settings", lambda: settings)
    return ASGITestClient(app)


def _create_pool_database(
    path: Path,
    statuses: dict[date, dict[str, str]],
    *,
    members: set[tuple[date, str]] | None = None,
) -> None:
    connection = duckdb.connect(str(path))
    connection.execute(SYSTEM_B_POOL_MEMBERSHIP.duckdb_create_sql())
    connection.execute(SYSTEM_B_POOL_RUN.duckdb_create_sql())
    for trade_date, date_statuses in statuses.items():
        for pool_type, status in date_statuses.items():
            run_id = f"run-{trade_date.isoformat()}-{pool_type}"
            connection.execute(
                f"""
                INSERT INTO {SYSTEM_B_POOL_RUN_TABLE} (
                    trade_date, pool_type, status, completed_run_id,
                    input_snapshot_id, asset_count, membership_row_count,
                    metrics, created_at, pool_completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    trade_date,
                    pool_type,
                    status,
                    run_id,
                    "test-input",
                    1,
                    1 if members and (trade_date, pool_type) in members else 0,
                    "{}",
                    NOW,
                    NOW if status == "COMPLETED" else None,
                ],
            )
    for trade_date, pool_type in members or set():
        connection.execute(
            f"""
            INSERT INTO {SYSTEM_B_POOL_MEMBERSHIP_TABLE} (
                trade_date, asset_id, pool_type, membership_state,
                pool_cycle_no, entry_date, exit_date, entry_reason,
                exit_reason, episode_id, metrics_json, completed_run_id,
                rule_version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                trade_date,
                f"ASSET-{pool_type}",
                pool_type,
                IN_POOL,
                1,
                trade_date,
                None,
                "test",
                None,
                f"episode-{pool_type}",
                "{}",
                f"run-{trade_date.isoformat()}-{pool_type}",
                SYSTEM_B_POOL_RULE_VERSION,
                NOW,
            ],
        )
    connection.close()


def _create_episode_database(path: Path) -> None:
    connection = duckdb.connect(str(path))
    connection.execute(SYSTEM_B_EPISODE.duckdb_create_sql())
    connection.execute(SYSTEM_B_EPISODE_OBSERVATION.duckdb_create_sql())
    connection.execute(
        f"""
        INSERT INTO {SYSTEM_B_EPISODE_TABLE} (
            episode_id, asset_id, episode_no, episode_start_date,
            episode_confirmed_date, episode_end_date, ma5_reentry_count,
            created_run_id, rule_version, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "episode-1",
            "ASSET-1",
            1,
            TRADE_DATE,
            TRADE_DATE,
            None,
            0,
            "episode-run-1",
            SYSTEM_B_EPISODE_RULE_VERSION,
            NOW,
        ],
    )
    connection.execute(
        f"""
        INSERT INTO {SYSTEM_B_EPISODE_OBSERVATION_TABLE} (
            trade_date, asset_id, episode_id, days_since_start,
            days_since_confirmed, close, ma5, ma10, trend_state,
            previous_trend_state, state_transition, episode_return,
            peak_return, drawdown_from_peak, ma5_reentry_count,
            is_episode_confirmed, is_episode_end, created_run_id,
            rule_version, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            TRADE_DATE,
            "ASSET-1",
            "episode-1",
            1,
            1,
            10.0,
            9.0,
            8.0,
            "ACTIVE",
            "CANDIDATE",
            "CANDIDATE->ACTIVE",
            0.1,
            0.12,
            -0.02,
            0,
            True,
            False,
            "episode-run-1",
            SYSTEM_B_EPISODE_RULE_VERSION,
            NOW,
        ],
    )
    connection.close()


def test_get_db_does_not_attach_auxiliary_databases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    episode_path = tmp_path / "episode.duckdb"
    pool_path = tmp_path / "pool.duckdb"
    _create_episode_database(episode_path)
    _create_pool_database(
        pool_path,
        {TRADE_DATE: {pool: "COMPLETED" for pool in POOL_TYPES}},
    )
    _configure_api(
        monkeypatch,
        tmp_path,
        episode_path=episode_path,
        pool_path=pool_path,
    )

    connection = api_db.get_db()
    try:
        attached = {
            row[0]
            for row in connection.execute(
                "SELECT database_name FROM duckdb_databases()"
            ).fetchall()
        }
    finally:
        connection.close()
    assert "episode_db" not in attached
    assert "pool_db" not in attached


def test_pool_snapshot_detaches_before_independent_writer_and_reads_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_path = tmp_path / "pool.duckdb"
    _create_pool_database(
        pool_path,
        {TRADE_DATE: {pool: "COMPLETED" for pool in POOL_TYPES}},
        members={(TRADE_DATE, POOL_HEIGHT)},
    )
    client = _configure_api(monkeypatch, tmp_path, pool_path=pool_path)

    first = client.get(
        "/api/v1/system-b/pools/snapshot",
        params={"trade_date": TRADE_DATE.isoformat()},
    )
    assert first.status_code == 200
    assert first.json()["pools"][0]["count"] == 1

    writer = duckdb.connect(str(pool_path))
    writer.execute(
        f"UPDATE {SYSTEM_B_POOL_MEMBERSHIP_TABLE} SET membership_state = 'EXITED'"
    )
    writer.close()

    second = client.get(
        "/api/v1/system-b/pools/snapshot",
        params={"trade_date": TRADE_DATE.isoformat()},
    )
    assert second.status_code == 200
    assert all(pool["count"] == 0 for pool in second.json()["pools"])


@pytest.mark.parametrize(
    "statuses",
    [
        {POOL_HEIGHT: "COMPLETED", POOL_CAPACITY: "COMPLETED"},
        {pool: "COMPLETED" for pool in POOL_TYPES[:-1]} | {POOL_RECOGNITION: "RUNNING"},
    ],
)
def test_pool_snapshot_requires_all_three_completed_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    statuses: dict[str, str],
) -> None:
    pool_path = tmp_path / "pool.duckdb"
    _create_pool_database(pool_path, {TRADE_DATE: statuses})
    client = _configure_api(monkeypatch, tmp_path, pool_path=pool_path)

    response = client.get(
        "/api/v1/system-b/pools/snapshot",
        params={"trade_date": TRADE_DATE.isoformat()},
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "POOL_SNAPSHOT_NOT_READY"


def test_pool_snapshot_all_completed_empty_is_valid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_path = tmp_path / "pool.duckdb"
    _create_pool_database(
        pool_path,
        {TRADE_DATE: {pool: "COMPLETED" for pool in POOL_TYPES}},
    )
    client = _configure_api(monkeypatch, tmp_path, pool_path=pool_path)

    response = client.get(
        "/api/v1/system-b/pools/snapshot",
        params={"trade_date": TRADE_DATE.isoformat()},
    )
    assert response.status_code == 200
    assert response.json() == {
        "trade_date": TRADE_DATE.isoformat(),
        "pools": [
            {"pool_type": pool, "count": 0, "members": []}
            for pool in POOL_TYPES
        ],
    }


def test_latest_pool_snapshot_uses_latest_fully_completed_date(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_path = tmp_path / "pool.duckdb"
    _create_pool_database(
        pool_path,
        {
            TRADE_DATE: {pool: "COMPLETED" for pool in POOL_TYPES},
            OTHER_DATE: {
                POOL_HEIGHT: "COMPLETED",
                POOL_CAPACITY: "COMPLETED",
                POOL_RECOGNITION: "FAILED",
            },
        },
        members={(TRADE_DATE, POOL_HEIGHT)},
    )
    client = _configure_api(monkeypatch, tmp_path, pool_path=pool_path)

    response = client.get("/api/v1/system-b/pools/snapshot/latest")
    assert response.status_code == 200
    assert response.json()["trade_date"] == TRADE_DATE.isoformat()


def test_pool_attach_failure_is_service_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_pool_path = tmp_path / "missing-pool.duckdb"
    client = _configure_api(monkeypatch, tmp_path, pool_path=missing_pool_path)

    response = client.get(
        "/api/v1/system-b/pools/snapshot",
        params={"trade_date": TRADE_DATE.isoformat()},
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "POOL_DB_NOT_AVAILABLE"


def test_active_episodes_attach_and_validate_response_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    episode_path = tmp_path / "episode.duckdb"
    _create_episode_database(episode_path)
    main_path = tmp_path / "main.duckdb"
    main = duckdb.connect(str(main_path))
    main.execute("CREATE TABLE stock_info (ticker VARCHAR, name VARCHAR)")
    main.execute("INSERT INTO stock_info VALUES ('ASSET-1', 'Test Asset')")
    main.close()
    client = _configure_api(monkeypatch, tmp_path, episode_path=episode_path)

    response = client.get(
        "/api/v1/system-b/active-episodes",
        params={"trade_date": TRADE_DATE.isoformat()},
    )
    assert response.status_code == 200
    assert response.json()[0]["asset_id"] == "ASSET-1"
    assert response.json()[0]["trade_date"] == TRADE_DATE.isoformat()
