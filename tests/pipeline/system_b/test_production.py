from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import random

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    SYSTEM_B_2_0_PARAMETER_SET_ID,
    SYSTEM_B_2_0_RULE_VERSION_SET_ID,
    init_database,
)
from qrp_atlas.pipeline.system_b.repository import (
    ACTUAL_TRADING,
    EXPLICIT_NON_TRADING,
    MARKET_FACT_STATUS,
    SystemBProductionError,
    ensure_system_b_schema,
    execute_standard_input,
)
from qrp_atlas.pipeline.system_b.service import initialize_history, run_daily
from qrp_atlas.pipeline.system_b.cli import _readiness


def _market_dates(count: int) -> list[date]:
    result: list[date] = []
    current = date(2026, 1, 5)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result


def _seed_market(path: Path, *, missing_day11_ma5_input: bool = False) -> list[date]:
    dates = _market_dates(14)
    connection = duckdb.connect(str(path))
    try:
        init_database(connection)
        connection.executemany(
            "INSERT INTO trading_calendar VALUES (?, TRUE, ?, ?, ?)",
            [(item, item.year, item.month, 1) for item in dates],
        )
        connection.executemany(
            "INSERT INTO stock_info VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("A", "A", "SZ", "MAIN", dates[0], None, True, None),
                ("B", "B", "SH", "MAIN", dates[0], None, True, None),
                ("NEW", "NEW", "SZ", "MAIN", dates[10], None, True, None),
            ],
        )
        factors = [("A", dates[0], 1.0), ("B", dates[0], 1.0), ("NEW", dates[10], 1.0)]
        if not missing_day11_ma5_input:
            connection.executemany("INSERT INTO adj_factor_changes VALUES (?, ?, ?)", factors)
        else:
            connection.executemany(
                "INSERT INTO adj_factor_changes VALUES (?, ?, ?)", factors[1:]
            )
        rows = []
        a_prices = [10.0] * 10 + [11.0, 12.0, 8.0, 7.0]
        b_prices = [20.0] * 10 + [21.0, 22.0, 23.0, 24.0]
        for ticker, prices in (("A", a_prices), ("B", b_prices)):
            for index, (trade_date, close) in enumerate(zip(dates, prices, strict=True)):
                if ticker == "B" and index == 11:
                    continue
                rows.append(
                    (
                        trade_date,
                        ticker,
                        ticker,
                        close,
                        close,
                        close,
                        close,
                        0.0,
                        close,
                        100,
                        1000.0,
                        1.0,
                        1.0,
                        1.0,
                        False,
                        False,
                        False,
                        None,
                    )
                )
        for index, trade_date in enumerate(dates[10:]):
            close = 5.0 + index
            rows.append(
                (
                    trade_date,
                    "NEW",
                    "NEW",
                    close,
                    close,
                    close,
                    close,
                    0.0,
                    close,
                    100,
                    1000.0,
                    1.0,
                    1.0,
                    1.0,
                    False,
                    False,
                    False,
                    None,
                )
            )
        connection.executemany(
            "INSERT INTO daily_market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        connection.execute(
            "INSERT INTO suspend_d VALUES (?, 'B', '全天', '停牌', NULL)", [dates[11]]
        )
    finally:
        connection.close()
    return dates




def _history(path: Path) -> pd.DataFrame:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        return connection.execute(
            """
            SELECT asset_id, trade_date, lifecycle_state, trend_state,
                   previous_trend_state, state_changed, market_fact_status,
                   is_trading_day, listing_trading_day_number,
                   latest_actual_is_above_or_equal_ma5,
                   previous_actual_is_above_or_equal_ma5,
                   state_basis_sequence_intact, actual_pair_contiguous, diagnostics
            FROM system_b_state_observation observation
            JOIN system_b_production_run run USING (production_run_id)
            WHERE run.status = 'SUCCEEDED'
            QUALIFY row_number() OVER (
                PARTITION BY asset_id, trade_date
                ORDER BY run.completed_at DESC, observation.created_at DESC
            ) = 1
            ORDER BY asset_id, trade_date
            """
        ).fetchdf()
    finally:
        connection.close()


def test_standard_input_contains_independent_historical_facts(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    connection = duckdb.connect(str(source), read_only=True)
    try:
        frame = execute_standard_input(connection, end_date=dates[-1]).fetchdf()
    finally:
        connection.close()
    suspended = frame.loc[(frame.asset_id == "B") & (frame.trade_date == pd.Timestamp(dates[11]))].iloc[0]
    assert suspended[MARKET_FACT_STATUS] == EXPLICIT_NON_TRADING
    assert not bool(suspended.is_trading_day)
    assert suspended.latest_actual_trade_date == pd.Timestamp(dates[10])
    assert bool(suspended.state_basis_sequence_intact)
    assert "trend_state" not in frame.columns


def test_full_initialization_matches_daily_without_state_checkpoint(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    full = tmp_path / "full.duckdb"
    daily = tmp_path / "daily.duckdb"
    first = initialize_history(
        source_database=source, output_database=full,
        staging_root=tmp_path / "full-stage", end_date=dates[-1], asset_batch_size=1,
    )
    assert first.status == "SUCCEEDED"
    repeated = initialize_history(
        source_database=source, output_database=full,
        staging_root=tmp_path / "repeat-stage", end_date=dates[-1], asset_batch_size=1,
    )
    assert repeated.status == "IDEMPOTENT_NOOP"

    ensure = duckdb.connect(str(daily))
    ensure_system_b_schema(ensure)
    ensure.close()
    for trade_date in dates:
        run_daily(
            source_database=source, output_database=daily,
            staging_root=tmp_path / "daily-stage", trade_date=trade_date,
        )
    pd.testing.assert_frame_equal(_history(full), _history(daily), check_dtype=False)


def test_daily_can_calculate_continuing_assets_with_empty_state_target(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    connection = duckdb.connect(str(output))
    ensure_system_b_schema(connection)
    connection.close()
    report = run_daily(
        source_database=source, output_database=output,
        staging_root=tmp_path / "stage", trade_date=dates[11],
    )
    assert report.status == "SUCCEEDED"
    assert report.output_row_count == 3


def test_backdated_daily_remains_fail_closed(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(
        source_database=source, output_database=output,
        staging_root=tmp_path / "init", end_date=dates[11],
    )
    with pytest.raises(SystemBProductionError) as exc_info:
        run_daily(
            source_database=source, output_database=output,
            staging_root=tmp_path / "backdated", trade_date=dates[10],
        )
    assert exc_info.value.code == "BACKDATED_RECOMPUTE_REQUIRED"


def test_same_date_revision_matches_independent_full_replay(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(
        source_database=source, output_database=output,
        staging_root=tmp_path / "init", end_date=dates[11],
    )
    connection = duckdb.connect(str(source))
    try:
        connection.execute(
            "UPDATE daily_market_snapshot SET close = close + 5 WHERE ticker = 'A' AND trade_date = ?",
            [dates[11]],
        )
    finally:
        connection.close()
    run_daily(
        source_database=source, output_database=output,
        staging_root=tmp_path / "revision", trade_date=dates[11],
    )
    replay = tmp_path / "replay.duckdb"
    initialize_history(
        source_database=source, output_database=replay,
        staging_root=tmp_path / "replay-stage", end_date=dates[11],
    )
    left = _history(output)
    left = left.loc[left.trade_date == pd.Timestamp(dates[11])].reset_index(drop=True)
    right = _history(replay)
    right = right.loc[right.trade_date == pd.Timestamp(dates[11])].reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right, check_dtype=False)


def test_unresolved_missing_is_persisted_as_null_with_diagnostic(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    connection = duckdb.connect(str(source))
    try:
        connection.execute(
            "DELETE FROM daily_market_snapshot WHERE ticker = 'A' AND trade_date = ?", [dates[11]]
        )
    finally:
        connection.close()
    readiness = _readiness(source, dates[11])
    assert readiness["unresolved_asset_count"] == 1
    output = tmp_path / "output.duckdb"
    report = initialize_history(
        source_database=source, output_database=output,
        staging_root=tmp_path / "stage", end_date=dates[11],
    )
    assert report.status == "SUCCEEDED"
    connection = duckdb.connect(str(output), read_only=True)
    try:
        row = connection.execute(
            "SELECT trend_state, diagnostics FROM system_b_state_observation WHERE asset_id='A' AND trade_date=?",
            [dates[11]],
        ).fetchone()
    finally:
        connection.close()
    assert row == (None, '["BROKEN_TRADING_SEQUENCE"]')


def test_suspension_and_zero_volume_derive_from_latest_actual_facts(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    connection = duckdb.connect(str(source))
    try:
        connection.execute(
            "UPDATE daily_market_snapshot SET volume=0 WHERE ticker='A' AND trade_date=?", [dates[11]]
        )
    finally:
        connection.close()
    output = tmp_path / "output.duckdb"
    initialize_history(
        source_database=source, output_database=output,
        staging_root=tmp_path / "stage", end_date=dates[11],
    )
    connection = duckdb.connect(str(output), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT asset_id, market_fact_status, trend_state, diagnostics
            FROM system_b_state_observation
            WHERE trade_date=? AND asset_id IN ('A','B') ORDER BY asset_id
            """, [dates[11]]
        ).fetchall()
    finally:
        connection.close()
    assert all(row[1] == EXPLICIT_NON_TRADING for row in rows)
    assert all(row[3] == '["NON_TRADING_DAY_FACT_DERIVED"]' for row in rows)


def test_random_single_date_inputs_match_full_history_common_rows(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    connection = duckdb.connect(str(source), read_only=True)
    try:
        full_input = execute_standard_input(connection, end_date=dates[-1]).fetchdf()
        from qrp_atlas.pipeline.system_b.service import _request
        from qrp_atlas.indicators.system_b import calculate_system_b_2_0_states
        full = calculate_system_b_2_0_states(_request(full_input)).frame
        population = [
            (asset_id, target_date)
            for asset_id in ("A", "B", "NEW")
            for target_date in dates
            if not full.loc[
                (full.asset_id == asset_id) & (full.trade_date == pd.Timestamp(target_date))
            ].empty
        ]
        for asset_id, target_date in random.Random(20260726).sample(population, 8):
            point_input = execute_standard_input(
                connection, end_date=target_date, target_date=target_date, asset_ids=[asset_id]
            ).fetchdf()
            point = calculate_system_b_2_0_states(_request(point_input)).frame
            point = point.loc[point.trade_date == pd.Timestamp(target_date)].iloc[0]
            expected = full.loc[(full.asset_id == asset_id) & (full.trade_date == pd.Timestamp(target_date))].iloc[0]
            for column in (
                "trend_state", "lifecycle_state", "market_fact_status",
                "latest_actual_is_above_or_equal_ma5",
                "previous_actual_is_above_or_equal_ma5",
                "state_basis_sequence_intact", "actual_pair_contiguous", "diagnostics",
            ):
                left, right = point[column], expected[column]
                assert (pd.isna(left) and pd.isna(right)) or left == right
    finally:
        connection.close()


def test_multiple_adjustments_scale_prices_without_changing_fact_state(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _market_dates(16)
    connection = duckdb.connect(str(source))
    try:
        init_database(connection)
        connection.executemany(
            "INSERT INTO trading_calendar VALUES (?, TRUE, ?, ?, ?)",
            [(item, item.year, item.month, 1) for item in dates],
        )
        connection.execute(
            "INSERT INTO stock_info VALUES ('A','A','SZ','MAIN',?,NULL,TRUE,NULL)", [dates[0]]
        )
        connection.executemany(
            "INSERT INTO adj_factor_changes VALUES ('A', ?, ?)",
            [(dates[0], 1.0), (dates[5], 2.0), (dates[11], 4.0), (dates[13], 8.0)],
        )
        connection.executemany(
            """
            INSERT INTO daily_market_snapshot
                (trade_date,ticker,name,open,high,low,close,volume)
            VALUES (?, 'A','A',?,?,?,?,100)
            """,
            [(item, 10 + index, 10 + index, 10 + index, 10 + index) for index, item in enumerate(dates)],
        )
        from qrp_atlas.pipeline.system_b.service import _request
        from qrp_atlas.indicators.system_b import calculate_system_b_2_0_states
        early_input = execute_standard_input(connection, end_date=dates[12]).fetchdf()
        late_input = execute_standard_input(connection, end_date=dates[-1]).fetchdf()
        early = calculate_system_b_2_0_states(_request(early_input)).frame
        late = calculate_system_b_2_0_states(_request(late_input)).frame
    finally:
        connection.close()
    late = late.loc[late.trade_date <= pd.Timestamp(dates[12])].reset_index(drop=True)
    for column in (
        "trend_state", "previous_trend_state", "state_changed",
        "lifecycle_state", "is_above_or_equal_ma5",
        "latest_actual_is_above_or_equal_ma5",
        "previous_actual_is_above_or_equal_ma5", "diagnostics",
    ):
        pd.testing.assert_series_equal(early[column], late[column], check_names=False)
    comparable = early.ma5.notna()
    close_scale = late.loc[comparable, "close"] / early.loc[comparable, "close"]
    ma5_scale = late.loc[comparable, "ma5"] / early.loc[comparable, "ma5"]
    assert close_scale.round(12).eq(ma5_scale.round(12)).all()
    assert (
        (early.loc[comparable, "close"] / early.loc[comparable, "ma5"]).round(12)
        == (late.loc[comparable, "close"] / late.loc[comparable, "ma5"]).round(12)
    ).all()


def test_initialization_is_bootstrap_only_and_exact_repeat_is_idempotent(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(source_database=source, output_database=output, staging_root=tmp_path / "a", end_date=dates[11])
    assert initialize_history(source_database=source, output_database=output, staging_root=tmp_path / "b", end_date=dates[11]).status == "IDEMPOTENT_NOOP"
    with pytest.raises(SystemBProductionError) as exc_info:
        initialize_history(source_database=source, output_database=output, staging_root=tmp_path / "c", end_date=dates[10])
    assert exc_info.value.code == "SYSTEM_B_INITIALIZATION_TARGET_NOT_EMPTY"


def test_schema_has_nullable_trend_and_no_checkpoint_table(tmp_path: Path) -> None:
    connection = duckdb.connect(str(tmp_path / "schema.duckdb"))
    try:
        ensure_system_b_schema(connection)
        nullable = connection.execute(
            "SELECT is_nullable FROM information_schema.columns WHERE table_name='system_b_state_observation' AND column_name='trend_state'"
        ).fetchone()[0]
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    finally:
        connection.close()
    assert nullable == "YES"
    assert "system_b_state_checkpoint" not in tables
