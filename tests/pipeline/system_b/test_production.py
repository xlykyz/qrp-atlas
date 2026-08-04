from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import random

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    SYSTEM_B_CALCULATION_VERSION,
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
    create_run,
    import_staging,
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
            """
            INSERT INTO stock_info (ticker, name, exchange, market, list_date, delist_date, is_active, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
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


def test_unresolved_gap_resets_ma5_window_and_explicit_non_trading_is_skipped(tmp_path: Path) -> None:
    source = tmp_path / "gap.duckdb"
    dates = _market_dates(20)
    connection = duckdb.connect(str(source))
    try:
        init_database(connection)
        connection.executemany(
            "INSERT INTO trading_calendar VALUES (?, TRUE, ?, ?, ?)",
            [(item, item.year, item.month, 1) for item in dates],
        )
        connection.execute(
            """
            INSERT INTO stock_info (ticker, name, exchange, market, list_date, delist_date, is_active, updated_at)
            VALUES ('A','A','SZ','MAIN',?,NULL,TRUE,NULL)
            """,
            [dates[0]],
        )
        connection.execute("INSERT INTO adj_factor_changes VALUES ('A', ?, 1.0)", [dates[0]])
        post_gap_prices = {12: 20.0, 14: 19.0, 15: 18.0, 16: 17.0, 17: 16.0, 18: 20.0, 19: 21.0}
        rows = []
        for index, trade_date in enumerate(dates):
            if index == 11:
                continue
            close = post_gap_prices.get(index, 100.0)
            volume = 0 if index == 13 else 100
            rows.append((trade_date, close, close, close, close, volume))
        connection.executemany(
            """
            INSERT INTO daily_market_snapshot
                (trade_date,ticker,name,open,high,low,close,volume)
            VALUES (?, 'A','A',?,?,?,?,?)
            """,
            rows,
        )
        frame = execute_standard_input(connection, end_date=dates[-1]).fetchdf()
        from qrp_atlas.pipeline.system_b.service import _request
        from qrp_atlas.indicators.system_b import calculate_system_b_2_0_states
        result = calculate_system_b_2_0_states(_request(frame)).frame
    finally:
        connection.close()

    gap = result.loc[result.trade_date == pd.Timestamp(dates[11])].iloc[0]
    suspended = result.loc[result.trade_date == pd.Timestamp(dates[13])].iloc[0]
    recovery = result.loc[
        result.trade_date.isin(pd.to_datetime([dates[12], dates[14], dates[15], dates[16]]))
    ]
    fifth = result.loc[result.trade_date == pd.Timestamp(dates[17])].iloc[0]
    sixth = result.loc[result.trade_date == pd.Timestamp(dates[18])].iloc[0]
    seventh = result.loc[result.trade_date == pd.Timestamp(dates[19])].iloc[0]
    assert gap.market_fact_status == "UNRESOLVED_MISSING"
    assert suspended.market_fact_status == "EXPLICIT_NON_TRADING"
    assert recovery.ma5.isna().all()
    assert not recovery.ma5_window_complete.any()
    assert fifth.ma5 == pytest.approx(18.0)
    assert bool(fifth.ma5_window_complete)
    assert fifth.trend_state == "BASE"
    assert sixth.trend_state == "CANDIDATE"
    assert seventh.trend_state == "ACTIVE"
    assert pd.isna(fifth.listing_trading_day_number)
    assert not bool(fifth.listing_trading_day_number_is_exact)
    assert fifth.confirmed_listing_trading_day_count == 16
    assert fifth.lifecycle_state == "NORMAL"


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
    assert repeated.status == "SUCCEEDED"

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
    report = run_daily(
        source_database=source, output_database=output,
        staging_root=tmp_path / "stage", trade_date=dates[11],
    )
    assert report.status == "SUCCEEDED"
    assert report.output_row_count == 3


def test_backdated_daily_independently_overwrites_without_touching_later_date(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(
        source_database=source, output_database=output,
        staging_root=tmp_path / "init", end_date=dates[11],
    )
    before = _history(output)
    later_before = before.loc[before.trade_date == pd.Timestamp(dates[11])].reset_index(drop=True)
    report = run_daily(
        source_database=source, output_database=output,
        staging_root=tmp_path / "backdated", trade_date=dates[10],
    )
    assert report.status == "SUCCEEDED"
    after = _history(output)
    later_after = after.loc[after.trade_date == pd.Timestamp(dates[11])].reset_index(drop=True)
    pd.testing.assert_frame_equal(later_before, later_after, check_dtype=False)


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
                "listing_trading_day_number", "confirmed_listing_trading_day_count",
                "listing_trading_day_number_is_exact", "ma5_window_complete",
                "latest_actual_is_above_or_equal_ma5",
                "latest_actual_ma5_window_complete",
                "previous_actual_is_above_or_equal_ma5",
                "previous_actual_ma5_window_complete",
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
            """
            INSERT INTO stock_info (ticker, name, exchange, market, list_date, delist_date, is_active, updated_at)
            VALUES ('A','A','SZ','MAIN',?,NULL,TRUE,NULL)
            """,
            [dates[0]],
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


def test_initialization_repeats_and_overwrites_only_requested_range(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(source_database=source, output_database=output, staging_root=tmp_path / "a", end_date=dates[11])
    before = _history(output)
    assert initialize_history(source_database=source, output_database=output, staging_root=tmp_path / "b", end_date=dates[11]).status == "SUCCEEDED"
    pd.testing.assert_frame_equal(before, _history(output), check_dtype=False)
    initialize_history(
        source_database=source,
        output_database=output,
        staging_root=tmp_path / "c",
        start_date=dates[10],
        end_date=dates[10],
    )
    after = _history(output)
    outside = before.loc[before.trade_date != pd.Timestamp(dates[10])].reset_index(drop=True)
    outside_after = after.loc[after.trade_date != pd.Timestamp(dates[10])].reset_index(drop=True)
    pd.testing.assert_frame_equal(outside, outside_after, check_dtype=False)


def test_range_overwrite_removes_stale_assets_and_failed_import_rolls_back(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(
        source_database=source, output_database=output,
        staging_root=tmp_path / "initial", end_date=dates[11],
    )
    source_connection = duckdb.connect(str(source))
    try:
        source_connection.execute("DELETE FROM stock_info WHERE ticker='B'")
    finally:
        source_connection.close()
    initialize_history(
        source_database=source, output_database=output,
        staging_root=tmp_path / "shrink", start_date=dates[11], end_date=dates[11],
    )
    connection = duckdb.connect(str(output))
    try:
        assets = {
            row[0] for row in connection.execute(
                "SELECT asset_id FROM system_b_state_observation WHERE trade_date=?", [dates[11]]
            ).fetchall()
        }
        assert assets == {"A", "NEW"}
        before = connection.execute(
            "SELECT * FROM system_b_state_observation ORDER BY asset_id, trade_date"
        ).fetchdf()
        bad_parquet = tmp_path / "bad.parquet"
        connection.register("bad_stage", pd.DataFrame({"asset_id": ["A"]}))
        connection.execute("COPY bad_stage TO ? (FORMAT PARQUET)", [str(bad_parquet)])
        create_run(
            connection,
            production_run_id="rollback-run",
            run_type="DAILY",
            target_start_date=dates[11],
            target_end_date=dates[11],
            rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
            parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
            calculation_version=SYSTEM_B_CALCULATION_VERSION,
        )
        with pytest.raises(Exception):
            import_staging(
                connection,
                parquet_glob=str(bad_parquet),
                production_run_id="rollback-run",
                input_snapshot_id="bad",
                calculation_version=SYSTEM_B_CALCULATION_VERSION,
                metrics={"asset_count": 1, "input_row_count": 1, "output_row_count": 1},
                replace_start_date=dates[11],
                replace_end_date=dates[11],
            )
        after = connection.execute(
            "SELECT * FROM system_b_state_observation ORDER BY asset_id, trade_date"
        ).fetchdf()
    finally:
        connection.close()
    pd.testing.assert_frame_equal(before, after, check_dtype=False)


def test_deleted_materialization_can_be_fully_rebuilt_from_market_facts(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(
        source_database=source, output_database=output,
        staging_root=tmp_path / "initial", end_date=dates[11],
    )
    expected = _history(output)
    connection = duckdb.connect(str(output))
    try:
        connection.execute("DELETE FROM system_b_state_observation")
    finally:
        connection.close()
    initialize_history(
        source_database=source, output_database=output,
        staging_root=tmp_path / "rebuild", end_date=dates[11],
    )
    pd.testing.assert_frame_equal(expected, _history(output), check_dtype=False)


def test_schema_has_nullable_trend_and_no_checkpoint_table(tmp_path: Path) -> None:
    connection = duckdb.connect(str(tmp_path / "schema.duckdb"))
    try:
        ensure_system_b_schema(connection)
        nullable = connection.execute(
            "SELECT is_nullable FROM information_schema.columns WHERE table_name='system_b_state_observation' AND column_name='trend_state'"
        ).fetchone()[0]
        primary_key_columns = [
            row[1]
            for row in connection.execute(
                "SELECT * FROM pragma_table_info('system_b_state_observation') WHERE pk"
            ).fetchall()
        ]
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    finally:
        connection.close()
    assert nullable == "YES"
    assert primary_key_columns == [
        "asset_id", "trade_date", "rule_version_set_id", "parameter_set_id"
    ]
    assert "system_b_state_checkpoint" not in tables
