from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    SYSTEM_B_2_0_PARAMETER_SET_ID,
    SYSTEM_B_2_0_RULE_VERSION_SET_ID,
    init_database,
)
from qrp_atlas.pipeline.system_b.repository import (
    SystemBProductionError,
    ensure_system_b_schema,
    execute_standard_input,
    load_checkpoints,
)
from qrp_atlas.pipeline.system_b.service import initialize_history, run_daily


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


def _latest_history(path: Path) -> pd.DataFrame:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        return connection.execute(
            """
            SELECT asset_id, trade_date, trend_state, previous_trend_state,
                   is_trading_day, listing_trading_day_number,
                   consecutive_above_ma5_days, consecutive_below_ma5_days
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


def test_standard_input_is_forward_adjusted_and_uses_actual_trading_days(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    connection = duckdb.connect(str(source), read_only=True)
    try:
        frame = execute_standard_input(connection, end_date=dates[-1]).fetchdf()
    finally:
        connection.close()
    first_four = frame.loc[(frame.asset_id == "A") & (frame.listing_trading_day_number <= 4)]
    assert first_four.ma5.isna().all()
    assert frame.loc[(frame.asset_id == "A") & (frame.listing_trading_day_number == 5), "ma5"].notna().all()
    suspended = frame.loc[(frame.asset_id == "B") & (frame.trade_date == pd.Timestamp(dates[11]))].iloc[0]
    assert bool(suspended.is_trading_day) is False
    assert suspended.listing_trading_day_number == 11
    assert pd.isna(suspended.close)
    assert pd.isna(suspended.ma5)
    resumed = frame.loc[(frame.asset_id == "B") & (frame.trade_date == pd.Timestamp(dates[12]))].iloc[0]
    assert resumed.listing_trading_day_number == 12
    new_rows = frame.loc[frame.asset_id == "NEW"]
    assert new_rows.trade_date.min().date() == dates[10]
    assert new_rows.listing_trading_day_number.tolist() == [1, 2, 3, 4]


def test_forward_adjusted_close_and_ma5_share_the_same_series(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _market_dates(5)
    connection = duckdb.connect(str(source))
    try:
        init_database(connection)
        connection.executemany(
            "INSERT INTO trading_calendar VALUES (?, TRUE, ?, ?, ?)",
            [(item, item.year, item.month, 1) for item in dates],
        )
        connection.execute(
            "INSERT INTO stock_info VALUES ('A', 'A', 'SZ', 'MAIN', ?, NULL, TRUE, NULL)",
            [dates[0]],
        )
        connection.executemany(
            "INSERT INTO adj_factor_changes VALUES ('A', ?, ?)",
            [(dates[0], 1.0), (dates[-1], 2.0)],
        )
        connection.executemany(
            """
            INSERT INTO daily_market_snapshot
                (trade_date, ticker, name, open, high, low, close, volume)
            VALUES (?, 'A', 'A', 10, 10, 10, 10, 100)
            """,
            [(item,) for item in dates],
        )
        frame = execute_standard_input(connection, end_date=dates[-1]).fetchdf()
    finally:
        connection.close()
    assert frame.close.tolist() == [5.0, 5.0, 5.0, 5.0, 10.0]
    assert frame.ma5.iloc[:4].isna().all()
    assert frame.ma5.iloc[4] == 6.0


def test_assets_without_adjustment_changes_use_the_unit_base_factor(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source, missing_day11_ma5_input=True)
    report = initialize_history(
        source_database=source,
        output_database=tmp_path / "output.duckdb",
        staging_root=tmp_path / "stage",
        end_date=dates[-1],
    )
    assert report.status == "SUCCEEDED"


def test_full_initialization_matches_daily_incremental_and_is_idempotent(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    full_output = tmp_path / "full.duckdb"
    incremental_output = tmp_path / "incremental.duckdb"

    full_report = initialize_history(
        source_database=source,
        output_database=full_output,
        staging_root=tmp_path / "full-stage",
        end_date=dates[-1],
        asset_batch_size=1,
    )
    assert full_report.sql_query_count == 1
    assert full_report.batch_count == 3
    assert full_report.input_row_count == full_report.output_row_count
    repeated_initialization = initialize_history(
        source_database=source,
        output_database=full_output,
        staging_root=tmp_path / "full-repeat-stage",
        end_date=dates[-1],
        asset_batch_size=1,
    )
    assert repeated_initialization.status == "IDEMPOTENT_NOOP"

    initialize_history(
        source_database=source,
        output_database=incremental_output,
        staging_root=tmp_path / "incremental-stage",
        end_date=dates[9],
        asset_batch_size=2,
    )
    for trade_date in dates[10:]:
        run_daily(
            source_database=source,
            output_database=incremental_output,
            staging_root=tmp_path / "daily-stage",
            trade_date=trade_date,
        )

    pd.testing.assert_frame_equal(
        _latest_history(full_output).reset_index(drop=True),
        _latest_history(incremental_output).reset_index(drop=True),
        check_dtype=False,
    )

    duplicate = run_daily(
        source_database=source,
        output_database=incremental_output,
        staging_root=tmp_path / "daily-repeat-stage",
        trade_date=dates[-1],
    )
    assert duplicate.status == "IDEMPOTENT_NOOP"
    connection = duckdb.connect(str(incremental_output), read_only=True)
    try:
        duplicate_keys = connection.execute(
            """
            SELECT count(*) - count(DISTINCT (asset_id, trade_date, rule_version_set_id,
                                               parameter_set_id, input_snapshot_id))
            FROM system_b_state_observation
            """
        ).fetchone()[0]
    finally:
        connection.close()
    assert duplicate_keys == 0


def test_daily_requires_prior_checkpoint_for_continuing_assets(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    connection = duckdb.connect(str(output))
    ensure_system_b_schema(connection)
    connection.close()
    with pytest.raises(SystemBProductionError) as exc_info:
        run_daily(
            source_database=source,
            output_database=output,
            staging_root=tmp_path / "stage",
            trade_date=dates[10],
        )
    assert exc_info.value.code == "MISSING_PREVIOUS_SUCCESS_STATE"


def test_latest_state_to_checkpoint_and_version_isolation(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(
        source_database=source,
        output_database=output,
        staging_root=tmp_path / "stage",
        end_date=dates[-1],
    )
    connection = duckdb.connect(str(output))
    try:
        checkpoints = load_checkpoints(
            connection,
            before_date=dates[-1] + timedelta(days=1),
            rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
            parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
            asset_ids=["A", "B"],
        )
        assert [item.asset_id for item in checkpoints] == ["A", "B"]
        original_count = connection.execute(
            "SELECT count(*) FROM system_b_latest_state"
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO system_b_production_run
            SELECT 'other-run', 'INITIALIZE', 'SUCCEEDED', target_start_date,
                   target_end_date, 'other-rule', parameter_set_id, 'other-snapshot',
                   calculation_version, 1, 1, 1, 0, '{}', NULL, NULL,
                   created_at, completed_at
            FROM system_b_production_run LIMIT 1
            """
        )
        connection.execute(
            """
            INSERT INTO system_b_state_observation
            SELECT asset_id, trade_date, trend_state, underlying_trend_state,
                   previous_trend_state, state_changed, is_trading_day,
                   listing_trading_day_number, close, ma5, is_above_or_equal_ma5,
                   consecutive_above_ma5_days, consecutive_below_ma5_days,
                   price_adjustment, 'other-rule', parameter_set_id, source_rule_ids,
                   diagnostics, 'other-run', 'other-snapshot', calculation_version, created_at
            FROM system_b_latest_state WHERE asset_id = 'A'
            """
        )
        assert connection.execute("SELECT count(*) FROM system_b_latest_state").fetchone()[0] == original_count + 1
    finally:
        connection.close()


def test_duplicate_state_primary_key_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    dates = _seed_market(source)
    output = tmp_path / "output.duckdb"
    initialize_history(
        source_database=source,
        output_database=output,
        staging_root=tmp_path / "stage",
        end_date=dates[-1],
    )
    connection = duckdb.connect(str(output))
    try:
        with pytest.raises(duckdb.ConstraintException):
            connection.execute(
                "INSERT INTO system_b_state_observation SELECT * FROM system_b_state_observation LIMIT 1"
            )
    finally:
        connection.close()


def test_schema_has_one_state_fact_and_no_checkpoint_table(tmp_path: Path) -> None:
    output = tmp_path / "output.duckdb"
    connection = duckdb.connect(str(output))
    try:
        ensure_system_b_schema(connection)
        names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    finally:
        connection.close()
    assert "system_b_state_observation" in names
    assert "system_b_latest_state" in names
    assert "system_b_state_checkpoint" not in names
