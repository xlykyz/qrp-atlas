"""Task06-A production boundary acceptance tests."""

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from qrp_atlas.contracts import POPULARITY_SOURCE_AVAILABILITY
from qrp_atlas.pipeline.system_b_asset_rank import (
    get_asset_rank_component_audit,
    get_asset_rank_snapshot,
    run_asset_rank_daily,
)
from qrp_atlas.pipeline.system_b.market_series import load_canonical_market_series


TARGET = date(2026, 1, 10)
TICKERS = ["000001.SZ", "000002.SZ", "600000.SH", "300001.SZ"]


def _quant(path: Path) -> Path:
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE stock_info (ticker VARCHAR PRIMARY KEY, list_date DATE, delist_date DATE, exchange VARCHAR, market VARCHAR)")
    con.executemany(
        "INSERT INTO stock_info VALUES (?, DATE '2026-01-01', NULL, ?, 'A')",
        [(ticker, "SH" if ticker.endswith("SH") else "SZ") for ticker in TICKERS]
        + [("00700.HK", "HK")],
    )
    con.execute("CREATE TABLE trading_calendar (trade_date DATE PRIMARY KEY, is_open BOOLEAN)")
    days = [TARGET - timedelta(days=day) for day in range(9, -1, -1)]
    con.executemany("INSERT INTO trading_calendar VALUES (?, TRUE)", [(day,) for day in days])
    con.execute("CREATE TABLE daily_market_snapshot (trade_date DATE, ticker VARCHAR, close DOUBLE, amount DOUBLE, volume DOUBLE, PRIMARY KEY (trade_date, ticker))")
    rows = []
    for index, ticker in enumerate(TICKERS):
        for day_index, day in enumerate(days):
            rows.append((day, ticker, 10.0 + index + day_index * (index + 1) / 10, 100.0 + index * 20 + day_index, 1.0))
    con.executemany("INSERT INTO daily_market_snapshot VALUES (?, ?, ?, ?, ?)", rows)
    con.execute(POPULARITY_SOURCE_AVAILABILITY.duckdb_create_sql())
    con.executemany(
        "INSERT INTO popularity_source_availability VALUES (?, ?, 'UNAVAILABLE', 0, '[]', ?, '{}', ?, TIMESTAMP '2026-01-10 16:00:00')",
        [(TARGET, source, f"{source}-v1", f"run-{source}") for source in ("dc_hot", "ths_hot")],
    )
    con.close()
    return path.resolve()


def _episode(path: Path) -> Path:
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE system_b_episode (episode_id VARCHAR PRIMARY KEY)")
    con.execute("INSERT INTO system_b_episode VALUES ('ep-1')")
    con.execute("CREATE TABLE system_b_episode_observation (trade_date DATE, asset_id VARCHAR, episode_return DOUBLE)")
    con.executemany(
        "INSERT INTO system_b_episode_observation VALUES (?, ?, ?)",
        [(TARGET, ticker, 0.1 + index / 10) for index, ticker in enumerate(TICKERS)],
    )
    con.close()
    return path.resolve()


def _pool(path: Path) -> Path:
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE system_b_pool_run (trade_date DATE, pool_type VARCHAR, status VARCHAR, completed_run_id VARCHAR)")
    con.execute("CREATE TABLE system_b_pool_membership_daily (trade_date DATE, asset_id VARCHAR, pool_type VARCHAR, membership_state VARCHAR, metrics_json VARCHAR)")
    con.executemany(
        "INSERT INTO system_b_pool_run VALUES (?, ?, 'COMPLETED', ?)",
        [(TARGET, pool, f"run-{pool}") for pool in ("CAPACITY", "HEIGHT", "RECOGNITION")],
    )
    rows = []
    for pool in ("CAPACITY", "HEIGHT", "RECOGNITION"):
        for index, ticker in enumerate(TICKERS):
            metrics = (
                '{"height_start_date":"2026-01-05","height_since_start_return":%s}' % (0.1 + index / 10)
                if pool == "HEIGHT" else "{}"
            )
            rows.append((TARGET, ticker, pool, "IN_POOL", metrics))
    con.executemany("INSERT INTO system_b_pool_membership_daily VALUES (?, ?, ?, ?, ?)", rows)
    con.close()
    return path.resolve()


def test_expected_hot_unavailability_completes_and_materializes_canonical_domain(tmp_path: Path) -> None:
    quant = _quant(tmp_path / "quant.duckdb")
    episode = _episode(tmp_path / "episode.duckdb")
    pool = _pool(tmp_path / "pool.duckdb")

    first = run_asset_rank_daily(
        quant_database=quant,
        pool_database=pool,
        episode_database=episode,
        trade_date=TARGET,
        production_run_id="asset-rank-run-1",
    )
    assert first["status"] == "COMPLETED"
    assert first["asset_count"] == 4
    assert first["diagnostics"] == ["DC_HOT_SOURCE_UNAVAILABLE", "THS_HOT_SOURCE_UNAVAILABLE"]

    snapshot = get_asset_rank_snapshot(quant, TARGET)
    audit = get_asset_rank_component_audit(quant, TARGET)
    assert snapshot["ticker"].tolist() == sorted(TICKERS)
    assert snapshot["m1_score"].notna().all()
    assert snapshot["m2_score"].notna().all()
    assert snapshot["m3_score"].isna().all()
    assert set(snapshot["m3_status"]) == {"INCOMPLETE_COMPONENTS"}
    assert len(audit) == 28
    assert "input_snapshot_id" in audit.loc[audit["component"] == "popularity", "source_provenance"].iloc[0]

    second = run_asset_rank_daily(
        quant_database=quant,
        pool_database=pool,
        episode_database=episode,
        trade_date=TARGET,
        production_run_id="asset-rank-run-2",
    )
    assert second["status"] == "COMPLETED"
    after = get_asset_rank_snapshot(quant, TARGET)
    pd.testing.assert_frame_equal(
        snapshot.drop(columns=["production_run_id", "created_at"]),
        after.drop(columns=["production_run_id", "created_at"]),
        check_dtype=False,
    )


def test_canonical_series_uses_last_actual_adjustment_factor(tmp_path: Path) -> None:
    database = tmp_path / "market.duckdb"
    connection = duckdb.connect(str(database))
    try:
        connection.execute(
            "CREATE TABLE daily_market_snapshot (trade_date DATE, ticker VARCHAR, close DOUBLE, volume DOUBLE)"
        )
        connection.execute(
            "CREATE TABLE trading_calendar (trade_date DATE, is_open BOOLEAN)"
        )
        connection.execute(
            "CREATE TABLE adj_factor_changes (ticker VARCHAR, trade_date DATE, adj_factor DOUBLE)"
        )
        connection.executemany(
            "INSERT INTO daily_market_snapshot VALUES (?, '000001.SZ', ?, ?)",
            [(date(2026, 1, 9), 10.0, 100.0), (date(2026, 1, 10), 9.0, 0.0)],
        )
        connection.executemany(
            "INSERT INTO trading_calendar VALUES (?, ?)",
            [(date(2026, 1, 9), True), (date(2026, 1, 10), False)],
        )
        connection.executemany(
            "INSERT INTO adj_factor_changes VALUES ('000001.SZ', ?, ?)",
            [(date(2026, 1, 1), 1.0), (date(2026, 1, 10), 2.0)],
        )
        frame = load_canonical_market_series(
            connection, date(2026, 1, 10), include_non_trading=True
        )
    finally:
        connection.close()

    actual = frame.loc[frame["trade_date"].eq(date(2026, 1, 9))].iloc[0]
    assert actual["close"] == 10.0
    assert actual["latest_adj_factor"] == 1.0
