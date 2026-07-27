from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.pipeline.system_b_pools import (
    SystemBPoolProductionError,
    build_stock_pools,
    get_daily_pool_snapshot,
    get_latest_completed_pool_snapshot,
    get_stock_pool_history,
    get_stock_pool_memberships,
)
from qrp_atlas.pipeline.system_b_pools.service import ensure_schema


def _input_database(path: Path) -> Path:
    con = duckdb.connect(str(path))
    con.execute("""
        CREATE TABLE system_b_state_observation (
            asset_id VARCHAR, trade_date DATE, trend_state VARCHAR,
            previous_trend_state VARCHAR, is_trading_day BOOLEAN,
            market_fact_status VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE system_b_episode (
            episode_id VARCHAR, episode_end_date DATE
        )
    """)
    con.execute("""
        CREATE TABLE system_b_episode_observation (
            trade_date DATE, asset_id VARCHAR, episode_id VARCHAR,
            episode_return DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE daily_market_snapshot (
            trade_date DATE, ticker VARCHAR, open DOUBLE, high DOUBLE,
            low DOUBLE, close DOUBLE, amount DOUBLE, float_cap DOUBLE,
            is_limit_up BOOLEAN
        )
    """)
    dates = pd.bdate_range("2026-01-01", periods=6)
    state_rows = []
    market_rows = []
    episode_rows = []
    for index, day in enumerate(dates):
        state_rows.append(("A", day.date(), "ACTIVE", "ACTIVE", True, "ACTUAL_TRADING"))
        state_rows.append(("B", day.date(), "BASE", "BASE", True, "ACTUAL_TRADING"))
        market_rows.append((day.date(), "A", 9 + index, 10 + index, 9.5 + index, 10 + index, 1000 + index, 30_000_000_000, index >= 1))
        market_rows.append((day.date(), "B", 9, 10, 9.5, 10, 1, 1, False))
        episode_rows.append((day.date(), "A", "A_EP_0001", 0.30 if index == 0 else 0.20))
    con.executemany("INSERT INTO system_b_state_observation VALUES (?, ?, ?, ?, ?, ?)", state_rows)
    con.executemany("INSERT INTO daily_market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", market_rows)
    con.executemany("INSERT INTO system_b_episode_observation VALUES (?, ?, ?, ?)", episode_rows)
    con.execute("INSERT INTO system_b_episode VALUES ('A_EP_0001', NULL)")
    con.close()
    return path


def test_build_and_queries_are_idempotent(tmp_path: Path):
    source = _input_database(tmp_path / "input.duckdb")
    output = tmp_path / "pools.duckdb"
    first = build_stock_pools(source.resolve(), output.resolve(), start_date=date(2026, 1, 1), end_date=date(2026, 1, 8))
    second = build_stock_pools(source.resolve(), output.resolve(), start_date=date(2026, 1, 1), end_date=date(2026, 1, 8))
    assert first["status"] == second["status"] == "COMPLETED"
    assert first["membership_rows"] == second["membership_rows"]
    latest, snapshot = get_latest_completed_pool_snapshot(output)
    assert latest == date(2026, 1, 8)
    assert set(snapshot) == {"HEIGHT", "CAPACITY", "RECOGNITION"}
    assert not get_daily_pool_snapshot(output, date(2026, 1, 2))["CAPACITY"].empty
    memberships = get_stock_pool_memberships(output, "A", date(2026, 1, 5))
    assert set(memberships["pool_type"]) == {"CAPACITY", "RECOGNITION", "HEIGHT"}


def test_incremental_build_preserves_history_and_replaces_target_date(
    tmp_path: Path,
):
    source = _input_database(tmp_path / "input.duckdb")
    output = tmp_path / "pools.duckdb"
    build_stock_pools(
        source.resolve(),
        output.resolve(),
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 7),
    )
    before = duckdb.connect(str(output), read_only=True)
    historical_members = before.execute("""
        SELECT * EXCLUDE (completed_run_id, created_at)
        FROM system_b_pool_membership_daily
        WHERE trade_date < DATE '2026-01-08'
        ORDER BY trade_date, asset_id, pool_type
    """).fetchall()
    historical_runs = before.execute("""
        SELECT trade_date, status, asset_count, membership_row_count
        FROM system_b_pool_run
        WHERE trade_date < DATE '2026-01-08'
        ORDER BY trade_date
    """).fetchall()
    before.close()

    build_stock_pools(
        source.resolve(),
        output.resolve(),
        start_date=date(2026, 1, 8),
        end_date=date(2026, 1, 8),
    )
    after = duckdb.connect(str(output), read_only=True)
    assert after.execute("""
        SELECT * EXCLUDE (completed_run_id, created_at)
        FROM system_b_pool_membership_daily
        WHERE trade_date < DATE '2026-01-08'
        ORDER BY trade_date, asset_id, pool_type
    """).fetchall() == historical_members
    assert after.execute("""
        SELECT trade_date, status, asset_count, membership_row_count
        FROM system_b_pool_run
        WHERE trade_date < DATE '2026-01-08'
        ORDER BY trade_date
    """).fetchall() == historical_runs
    assert after.execute("SELECT count(*) FROM system_b_pool_run").fetchone()[0] == 6
    target_business = after.execute("""
        SELECT * EXCLUDE (completed_run_id, created_at)
        FROM system_b_pool_membership_daily
        WHERE trade_date=DATE '2026-01-08'
        ORDER BY asset_id, pool_type
    """).fetchall()
    after.close()

    build_stock_pools(
        source.resolve(),
        output.resolve(),
        start_date=date(2026, 1, 8),
        end_date=date(2026, 1, 8),
    )
    repeated = duckdb.connect(str(output), read_only=True)
    assert repeated.execute("SELECT count(*) FROM system_b_pool_run").fetchone()[0] == 6
    assert repeated.execute("""
        SELECT * EXCLUDE (completed_run_id, created_at)
        FROM system_b_pool_membership_daily
        WHERE trade_date=DATE '2026-01-08'
        ORDER BY asset_id, pool_type
    """).fetchall() == target_business
    repeated.close()


def test_missing_input_has_no_output_side_effect(tmp_path: Path):
    with pytest.raises(SystemBPoolProductionError, match="POOL_INPUT_DATABASE_NOT_FOUND"):
        build_stock_pools(
            (tmp_path / "missing.duckdb").resolve(),
            (tmp_path / "output.duckdb").resolve(),
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 8),
        )
    assert not (tmp_path / "output.duckdb").exists()


def test_relative_input_is_rejected(tmp_path: Path):
    with pytest.raises(SystemBPoolProductionError, match="POOL_INPUT_DATABASE_MUST_BE_ABSOLUTE"):
        build_stock_pools(
            Path("input.duckdb"),
            (tmp_path / "output.duckdb").resolve(),
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 8),
        )


def test_stock_memberships_excludes_exit_records_but_history_keeps_them(tmp_path: Path):
    output = tmp_path / "pools.duckdb"
    con = duckdb.connect(str(output))
    ensure_schema(con)
    con.execute("""
        INSERT INTO system_b_pool_membership_daily VALUES (
            DATE '2026-01-08', 'A', 'HEIGHT', 'EXITED', 1,
            DATE '2026-01-05', DATE '2026-01-08', '{}', 'ACTIVE_TO_BASE',
            NULL, '{}', 'run-1', 'system_b_pools@1.0.0__user_20260727',
            TIMESTAMP '2026-01-08 16:00:00'
        )
    """)
    con.close()
    memberships = get_stock_pool_memberships(output.resolve(), "A", date(2026, 1, 8))
    history = get_stock_pool_history(output.resolve(), "A", "HEIGHT")
    assert memberships.empty
    assert history["membership_state"].tolist() == ["EXITED"]
