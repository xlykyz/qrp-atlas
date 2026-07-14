"""07-B1: cross-sectional momentum product loop tests."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.backtest.product import (
    PRODUCT_SUPPORTED_STRATEGY_CODES,
    BacktestProductService,
    BacktestTaskStore,
    BacktestTaskValidationError,
    CreateBacktestTaskRequest,
    execute_validated_task,
    list_strategy_catalog,
    validate_create_request,
)
from qrp_atlas.backtest.product.schemas import (
    BacktestCostConfigDTO,
    BacktestExecutionConfigDTO,
    BacktestPositionConfigDTO,
)
from qrp_atlas.backtest.product.timing import REASON_NO_EXECUTION_DATE_IN_RANGE


def _insert_df(con: duckdb.DuckDBPyConnection, table: str, frame: pd.DataFrame) -> None:
    con.register("_tmp_df", frame)
    con.execute(f"INSERT INTO {table} SELECT * FROM _tmp_df")
    con.unregister("_tmp_df")


def _make_cs_db(tmp_path: Path) -> Path:
    """Build prices + PIT index membership for a small CS universe."""

    db_path = tmp_path / "cs_product.duckdb"
    con = duckdb.connect(str(db_path))

    # Market tables match classic product fixtures (load_stock_prices).
    con.execute(
        """
        CREATE TABLE daily_market_snapshot (
            trade_date DATE,
            ticker VARCHAR,
            name VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            turnover DOUBLE,
            market_cap DOUBLE,
            float_cap DOUBLE,
            is_st BOOLEAN,
            is_limit_up BOOLEAN,
            is_limit_down BOOLEAN
        )
        """
    )
    con.execute(
        """
        CREATE TABLE suspend_d (
            trade_date DATE,
            ticker VARCHAR,
            suspend_timing VARCHAR,
            suspend_type VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    # Minimal PIT index table used by query_index_components_as_of.
    con.execute(
        """
        CREATE TABLE index_component_history (
            index_code VARCHAR,
            asset_id VARCHAR,
            snapshot_date DATE,
            weight DOUBLE,
            effective_from DATE,
            effective_to DATE,
            available_trade_date DATE,
            source VARCHAR,
            source_record_id VARCHAR,
            revision_id VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )

    dates = pd.bdate_range("2024-01-02", periods=50)
    tickers = ["AAA.SZ", "BBB.SZ", "CCC.SZ", "DDD.SZ", "EEE.SZ"]
    rows = []
    for i, d in enumerate(dates):
        for j, ticker in enumerate(tickers):
            close = 10 + i * (0.2 + j * 0.05) + j
            open_px = close - 0.05
            rows.append(
                (
                    d.date().isoformat(),
                    ticker,
                    ticker,
                    open_px,
                    close + 0.1,
                    close - 0.1,
                    close,
                    1_000_000.0,
                    1_000_000.0 * close,
                    0.01,
                    1e10,
                    5e9,
                    False,
                    False,
                    False,
                )
            )
    con.executemany(
        "INSERT INTO daily_market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )

    index_rows = []
    rid = 0

    def add_row(**kwargs):
        nonlocal rid
        rid += 1
        index_rows.append(
            {
                "index_code": kwargs["index_code"],
                "asset_id": kwargs["asset_id"],
                "snapshot_date": kwargs["snapshot_date"],
                "weight": kwargs["weight"],
                "effective_from": kwargs["snapshot_date"],
                "effective_to": None,
                "available_trade_date": kwargs["available_trade_date"],
                "source": "test",
                "source_record_id": f"src-{rid}",
                "revision_id": f"rev-{rid}",
                "ingested_at": kwargs["ingested_at"],
            }
        )

    for asset, w in [("AAA.SZ", 0.3), ("BBB.SZ", 0.3), ("CCC.SZ", 0.4)]:
        add_row(
            index_code="000300.SH",
            asset_id=asset,
            snapshot_date=date(2024, 1, 2),
            weight=w,
            available_trade_date=date(2024, 1, 2),
            ingested_at=datetime(2024, 1, 2, 8, 0, 0),
        )
    for asset, w in [("BBB.SZ", 0.3), ("CCC.SZ", 0.3), ("DDD.SZ", 0.4)]:
        add_row(
            index_code="000300.SH",
            asset_id=asset,
            snapshot_date=date(2024, 2, 1),
            weight=w,
            available_trade_date=date(2024, 2, 1),
            ingested_at=datetime(2024, 2, 1, 8, 0, 0),
        )
    add_row(
        index_code="000300.SH",
        asset_id="EEE.SZ",
        snapshot_date=date(2024, 3, 1),
        weight=1.0,
        available_trade_date=date(2024, 3, 1),
        ingested_at=datetime(2024, 3, 1, 8, 0, 0),
    )
    add_row(
        index_code="000905.SH",
        asset_id="EEE.SZ",
        snapshot_date=date(2024, 1, 2),
        weight=1.0,
        available_trade_date=date(2024, 1, 2),
        ingested_at=datetime(2024, 1, 2, 8, 0, 0),
    )
    _insert_df(con, "index_component_history", pd.DataFrame(index_rows))
    con.close()
    return db_path


def _cs_request(**overrides):
    base = dict(
        name="cs momentum product",
        strategy_code="cross_sectional_momentum_long_only",
        strategy_version="1.0.0",
        strategy_params={
            "top_n": 2,
            "momentum_lookback": 3,
            "rebalance_frequency": "weekly",
            "cash_buffer": 0.0,
            "ascending": False,
        },
        universe_mode="index_components",
        index_code="000300.SH",
        start_date="2024-01-15",
        end_date="2024-02-28",
        position=BacktestPositionConfigDTO(
            initial_cash=1_000_000,
            max_positions=2,
            max_weight_per_symbol=0.5,
        ),
        cost=BacktestCostConfigDTO(
            commission_rate=0.00025,
            stamp_tax_rate=0.0005,
            slippage_bps=0,
        ),
        execution=BacktestExecutionConfigDTO(entry_timing="next_open"),
    )
    base.update(overrides)
    return CreateBacktestTaskRequest(**base)


def test_catalog_includes_cross_sectional_and_not_multifactor():
    assert "cross_sectional_momentum_long_only" in PRODUCT_SUPPORTED_STRATEGY_CODES
    assert "multifactor_long_only" not in PRODUCT_SUPPORTED_STRATEGY_CODES
    items = list_strategy_catalog(product_only=True)
    codes = {item.code for item in items}
    assert "cross_sectional_momentum_long_only" in codes
    assert "dual_sma_trend" in codes
    assert "multifactor_long_only" not in codes
    cs = next(item for item in items if item.code == "cross_sectional_momentum_long_only")
    assert cs.family == "cross_sectional"
    assert cs.product_supported is True
    assert cs.requires_historical_universe is True
    assert cs.supported_universe_modes == ["index_components"]
    assert cs.supported_entry_timings == ["next_open"]
    assert "momentum_lookback" in cs.parameter_schema
    assert "explicit_dates_json" not in cs.parameter_schema


def test_validate_requires_index_code_and_next_open():
    with pytest.raises(BacktestTaskValidationError, match="index_code"):
        validate_create_request(_cs_request(index_code=None))
    with pytest.raises(BacktestTaskValidationError, match="index_components"):
        validate_create_request(_cs_request(universe_mode="tickers", tickers=["AAA.SZ"]))
    with pytest.raises(BacktestTaskValidationError, match="next_open"):
        validate_create_request(
            _cs_request(execution=BacktestExecutionConfigDTO(entry_timing="same_close"))
        )
    with pytest.raises(BacktestTaskValidationError, match="next_open"):
        validate_create_request(
            _cs_request(execution=BacktestExecutionConfigDTO(entry_timing="next_close"))
        )


def test_validate_top_n_vs_max_positions_ssot():
    with pytest.raises(BacktestTaskValidationError, match="top_n"):
        validate_create_request(
            _cs_request(
                strategy_params={"top_n": 5, "momentum_lookback": 3, "rebalance_frequency": "weekly"},
                position=BacktestPositionConfigDTO(
                    initial_cash=1_000_000,
                    max_positions=2,
                    max_weight_per_symbol=0.5,
                ),
            )
        )
    req = validate_create_request(_cs_request())
    # Portfolio SSOT overrides strategy capacity fields.
    assert req.strategy_params["max_positions"] == req.position.max_positions
    assert req.strategy_params["max_weight_per_asset"] == req.position.max_weight_per_symbol
    assert req.strategy_params["top_n"] == 2


def test_classic_request_still_valid():
    req = validate_create_request(
        CreateBacktestTaskRequest(
            strategy_code="dual_sma_trend",
            strategy_version="1.0.0",
            strategy_params={"fast_window": 3, "slow_window": 5},
            universe_mode="tickers",
            tickers=["AAA.SZ"],
            start_date="2024-01-15",
            end_date="2024-02-20",
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000, max_positions=1, max_weight_per_symbol=1.0
            ),
            cost=BacktestCostConfigDTO(),
            execution=BacktestExecutionConfigDTO(entry_timing="next_open"),
        )
    )
    assert req.universe_mode == "tickers"
    assert req.index_code is None


def test_product_cross_section_end_to_end(tmp_path: Path):
    db_path = _make_cs_db(tmp_path)
    runs_dir = tmp_path / "runs"
    request = validate_create_request(_cs_request())
    run_id, run_dir = execute_validated_task(
        request,
        run_id="cs_momentum_e2e",
        runs_dir=runs_dir,
        db_path=db_path,
    )
    assert run_id == "cs_momentum_e2e"
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "equity.json").exists()
    assert (run_dir / "trades.json").exists()
    assert (run_dir / "skipped.json").exists()
    assert (run_dir / "config.json").exists()

    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["product_request"]["universe_mode"] == "index_components"
    assert config["product_request"]["index_code"] == "000300.SH"
    assert config["entry_timing"] == "next_open"
    assert config["cross_section"]["product_timing_shift"] is False
    assert config["cross_section"]["date_mapping_owner"] == "strategy_rebalance_schedule"
    assert config["cross_section"]["momentum_factor"]["parameters"]["lookback"] == 3

    trades = json.loads((run_dir / "trades.json").read_text(encoding="utf-8"))
    for trade in trades:
        assert trade["entry_date"] > trade["signal_date"]
        # No double next_open shift to T+2 if weekly signals land on Fridays and Monday is next.
        # At minimum entry must be strictly after signal.
        assert trade["entry_date"] <= request.end_date
        assert trade["entry_date"] >= request.start_date

    # Formal equity path stays inside request range.
    equity = json.loads((run_dir / "equity.json").read_text(encoding="utf-8"))
    for point in equity:
        assert request.start_date <= point["date"] <= request.end_date


def test_future_index_membership_not_visible(tmp_path: Path):
    db_path = _make_cs_db(tmp_path)
    # Restrict window to January so Feb snapshot / EEE future membership are invisible.
    request = validate_create_request(
        _cs_request(
            start_date="2024-01-08",
            end_date="2024-01-31",
            strategy_params={
                "top_n": 3,
                "momentum_lookback": 3,
                "rebalance_frequency": "weekly",
            },
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000,
                max_positions=3,
                max_weight_per_symbol=0.4,
            ),
        )
    )
    _, run_dir = execute_validated_task(
        request,
        run_id="cs_pit_membership",
        runs_dir=tmp_path / "runs",
        db_path=db_path,
    )
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    # All January universe diagnostics should only see early members.
    for item in config["cross_section"]["universe_diagnostics"]:
        assert item["signal_date"] < "2024-02-01"
        assert item["component_count"] <= 3

    trades = json.loads((run_dir / "trades.json").read_text(encoding="utf-8"))
    traded = {t.get("asset_id") or t.get("ticker") for t in trades}
    # EEE only exists on 000905 / future 000300 snapshot.
    assert "EEE.SZ" not in traded
    assert "DDD.SZ" not in traded


def test_no_double_date_shift(tmp_path: Path):
    db_path = _make_cs_db(tmp_path)
    request = validate_create_request(
        _cs_request(
            strategy_params={
                "top_n": 1,
                "momentum_lookback": 3,
                "rebalance_frequency": "daily",
            },
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000,
                max_positions=1,
                max_weight_per_symbol=1.0,
            ),
            start_date="2024-01-10",
            end_date="2024-01-25",
        )
    )
    _, run_dir = execute_validated_task(
        request,
        run_id="cs_no_double_shift",
        runs_dir=tmp_path / "runs",
        db_path=db_path,
    )
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["cross_section"]["product_timing_shift"] is False
    trades = json.loads((run_dir / "trades.json").read_text(encoding="utf-8"))
    # For daily rebalance, execution should be the next trading day after signal.
    # Using market calendar from prices.
    if trades:
        # Collect signal/entry pairs; lag should be 1 business day typically.
        lags = []
        for trade in trades:
            sig = pd.Timestamp(trade["signal_date"])
            ent = pd.Timestamp(trade["entry_date"])
            lags.append((ent - sig).days)
        assert min(lags) >= 1
        # Should not systematically be 2+ calendar weeks etc; most lags small.
        assert max(lags) <= 5


def test_end_of_range_signal_skipped(tmp_path: Path):
    db_path = _make_cs_db(tmp_path)
    request = validate_create_request(
        _cs_request(
            strategy_params={
                "top_n": 1,
                "momentum_lookback": 3,
                "rebalance_frequency": "daily",
            },
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000,
                max_positions=1,
                max_weight_per_symbol=1.0,
            ),
            start_date="2024-01-10",
            end_date="2024-01-12",
        )
    )
    _, run_dir = execute_validated_task(
        request,
        run_id="cs_end_skip",
        runs_dir=tmp_path / "runs",
        db_path=db_path,
    )
    skipped = json.loads((run_dir / "skipped.json").read_text(encoding="utf-8"))
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    product_skips = [
        item for item in skipped if item.get("reason") == REASON_NO_EXECUTION_DATE_IN_RANGE
    ]
    # Last formal day is a daily signal with next open outside range.
    assert any(item.get("signal_date") == "2024-01-12" for item in product_skips)
    assert all(item.get("reason") == REASON_NO_EXECUTION_DATE_IN_RANGE for item in product_skips)
    assert summary["skipped_count"] >= len(skipped)
    for filename in ("orders.json", "fills.json", "equity.json", "trades.json"):
        payload = json.loads((run_dir / filename).read_text(encoding="utf-8"))
        if not payload:
            continue
        if filename == "equity.json":
            dates = [item["date"] for item in payload]
        else:
            dates = [item.get("trade_date") or item.get("entry_date") for item in payload if item]
        for d in dates:
            if d:
                assert d <= "2024-01-12"


def test_service_create_task_succeeded(tmp_path: Path):
    db_path = _make_cs_db(tmp_path)
    service = BacktestProductService(
        task_store=BacktestTaskStore(tmp_path / "tasks"),
        runs_dir=tmp_path / "runs",
        db_path=db_path,
        execute_inline=True,
    )
    task = service.create_task(_cs_request()).task
    assert task.status == "succeeded", task.error_message
    assert task.run_id
    assert task.universe_mode == "index_components"
    assert task.index_code == "000300.SH"
    assert task.request_snapshot["index_code"] == "000300.SH"


def test_momentum_not_affected_by_future_prices(tmp_path: Path):
    db_path = _make_cs_db(tmp_path)
    request = validate_create_request(
        _cs_request(
            strategy_params={
                "top_n": 1,
                "momentum_lookback": 3,
                "rebalance_frequency": "weekly",
            },
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000,
                max_positions=1,
                max_weight_per_symbol=1.0,
            ),
        )
    )
    _, run_dir_1 = execute_validated_task(
        request,
        run_id="cs_future_prices_a",
        runs_dir=tmp_path / "runs_a",
        db_path=db_path,
    )
    # Mutate far-future prices only.
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        UPDATE daily_market_snapshot
        SET close = close * 10, open = open * 10, high = high * 10, low = low * 10
        WHERE trade_date >= DATE '2024-03-01'
        """
    )
    con.close()
    _, run_dir_2 = execute_validated_task(
        request,
        run_id="cs_future_prices_b",
        runs_dir=tmp_path / "runs_b",
        db_path=db_path,
    )
    trades_1 = json.loads((run_dir_1 / "trades.json").read_text(encoding="utf-8"))
    trades_2 = json.loads((run_dir_2 / "trades.json").read_text(encoding="utf-8"))
    # Decision set for the original window should remain identical.
    keys_1 = sorted((t["signal_date"], t.get("asset_id") or t.get("ticker"), t["entry_date"]) for t in trades_1)
    keys_2 = sorted((t["signal_date"], t.get("asset_id") or t.get("ticker"), t["entry_date"]) for t in trades_2)
    assert keys_1 == keys_2


def _assert_terminal_skip(run_dir: Path, *, expected_signal: str, end_date: str) -> None:
    skipped = json.loads((run_dir / "skipped.json").read_text(encoding="utf-8"))
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    product_skips = [
        item for item in skipped if item.get("reason") == REASON_NO_EXECUTION_DATE_IN_RANGE
    ]
    assert any(item.get("signal_date") == expected_signal for item in product_skips), product_skips
    assert summary["skipped_count"] >= len(skipped)
    equity = json.loads((run_dir / "equity.json").read_text(encoding="utf-8"))
    for point in equity:
        assert point["date"] <= end_date


def test_weekly_terminal_signal_skipped(tmp_path: Path):
    """Friday weekly signal must map to next Monday and enter skipped when Monday > end_date."""
    db_path = _make_cs_db(tmp_path)
    # 2024-01-26 is Friday; next open is 2024-01-29.
    request = validate_create_request(
        _cs_request(
            strategy_params={
                "top_n": 1,
                "momentum_lookback": 3,
                "rebalance_frequency": "weekly",
            },
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000,
                max_positions=1,
                max_weight_per_symbol=1.0,
            ),
            start_date="2024-01-15",
            end_date="2024-01-26",
        )
    )
    _, run_dir = execute_validated_task(
        request,
        run_id="cs_weekly_end_skip",
        runs_dir=tmp_path / "runs",
        db_path=db_path,
    )
    _assert_terminal_skip(run_dir, expected_signal="2024-01-26", end_date="2024-01-26")
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["cross_section"]["product_timing_shift"] is False
    assert config["cross_section"]["mapping_calendar_extra_days"] >= 1


def test_monthly_terminal_signal_skipped(tmp_path: Path):
    """Month-end signal must map to first next-month open and skip when outside range."""
    db_path = _make_cs_db(tmp_path)
    # 2024-01-31 is month end Wed; next open 2024-02-01.
    request = validate_create_request(
        _cs_request(
            strategy_params={
                "top_n": 1,
                "momentum_lookback": 3,
                "rebalance_frequency": "monthly",
            },
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000,
                max_positions=1,
                max_weight_per_symbol=1.0,
            ),
            start_date="2024-01-02",
            end_date="2024-01-31",
        )
    )
    _, run_dir = execute_validated_task(
        request,
        run_id="cs_monthly_end_skip",
        runs_dir=tmp_path / "runs",
        db_path=db_path,
    )
    _assert_terminal_skip(run_dir, expected_signal="2024-01-31", end_date="2024-01-31")


def test_empty_historical_universe_cash_only_no_placeholder(tmp_path: Path):
    """Empty membership must yield deterministic all-cash result without any asset_id."""
    db_path = _make_cs_db(tmp_path)
    # Use an index with no history in the fixture.
    request = validate_create_request(
        _cs_request(
            index_code="399001.SZ",
            strategy_params={
                "top_n": 1,
                "momentum_lookback": 3,
                "rebalance_frequency": "weekly",
            },
            position=BacktestPositionConfigDTO(
                initial_cash=1_234_567,
                max_positions=1,
                max_weight_per_symbol=1.0,
            ),
            start_date="2024-01-15",
            end_date="2024-02-15",
        )
    )
    _, run_dir = execute_validated_task(
        request,
        run_id="cs_empty_universe",
        runs_dir=tmp_path / "runs_empty",
        db_path=db_path,
    )
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["cross_section"]["empty_historical_universe"] is True
    assert config["cross_section"]["cash_only_result"] is True
    assert all(item["component_count"] == 0 for item in config["cross_section"]["universe_diagnostics"])

    orders = json.loads((run_dir / "orders.json").read_text(encoding="utf-8"))
    fills = json.loads((run_dir / "fills.json").read_text(encoding="utf-8"))
    trades = json.loads((run_dir / "trades.json").read_text(encoding="utf-8"))
    equity = json.loads((run_dir / "equity.json").read_text(encoding="utf-8"))
    assert orders == []
    assert fills == []
    assert trades == []
    assert equity
    for point in equity:
        # equity curve is normalized to 1.0 in cash-only helper; absolute cash checked via snapshots if present
        assert point["date"] >= "2024-01-15"
        assert point["date"] <= "2024-02-15"
        assert abs(float(point["equity"]) - 1.0) < 1e-12

    # No real/placeholder assets appear in any result artifact.
    blob = (run_dir / "config.json").read_text(encoding="utf-8")
    for forbidden in ("AAA.SZ", "BBB.SZ", "CCC.SZ", "DDD.SZ", "EEE.SZ", "__CASH_ONLY__"):
        assert forbidden not in blob


def test_empty_universe_independent_of_market_row_order(tmp_path: Path):
    (tmp_path / "a").mkdir(parents=True, exist_ok=True)
    db1 = _make_cs_db(tmp_path / "a")
    db2 = tmp_path / "b" / "cs_product.duckdb"
    db2.parent.mkdir(parents=True, exist_ok=True)
    # Rebuild same schema but reverse insert order of market rows.
    import duckdb
    from datetime import date, datetime
    con = duckdb.connect(str(db2))
    con.execute(
        """
        CREATE TABLE daily_market_snapshot (
            trade_date DATE, ticker VARCHAR, name VARCHAR,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, amount DOUBLE, turnover DOUBLE,
            market_cap DOUBLE, float_cap DOUBLE,
            is_st BOOLEAN, is_limit_up BOOLEAN, is_limit_down BOOLEAN
        )
        """
    )
    con.execute(
        """
        CREATE TABLE suspend_d (
            trade_date DATE, ticker VARCHAR, suspend_timing VARCHAR,
            suspend_type VARCHAR, created_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE index_component_history (
            index_code VARCHAR, asset_id VARCHAR, snapshot_date DATE, weight DOUBLE,
            effective_from DATE, effective_to DATE, available_trade_date DATE,
            source VARCHAR, source_record_id VARCHAR, revision_id VARCHAR, ingested_at TIMESTAMP
        )
        """
    )
    dates = list(pd.bdate_range("2024-01-02", periods=40))
    tickers = ["ZZZ.SZ", "YYY.SZ", "XXX.SZ"]
    rows = []
    for i, d in enumerate(dates):
        for j, ticker in enumerate(tickers):
            close = 20 + i * 0.1 + j
            rows.append(
                (
                    d.date().isoformat(), ticker, ticker, close - 0.05, close + 0.1,
                    close - 0.1, close, 1e6, 1e6 * close, 0.01, 1e10, 5e9, False, False, False,
                )
            )
    # reverse physical insertion order
    con.executemany(
        "INSERT INTO daily_market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        list(reversed(rows)),
    )
    con.close()

    req = validate_create_request(
        _cs_request(
            index_code="399001.SZ",
            strategy_params={"top_n": 1, "momentum_lookback": 3, "rebalance_frequency": "weekly"},
            position=BacktestPositionConfigDTO(initial_cash=500000, max_positions=1, max_weight_per_symbol=1.0),
            start_date="2024-01-15",
            end_date="2024-02-10",
        )
    )
    _, run1 = execute_validated_task(req, run_id="empty_order_a", runs_dir=tmp_path / "runs1", db_path=db1)
    _, run2 = execute_validated_task(req, run_id="empty_order_b", runs_dir=tmp_path / "runs2", db_path=db2)
    eq1 = json.loads((run1 / "equity.json").read_text(encoding="utf-8"))
    eq2 = json.loads((run2 / "equity.json").read_text(encoding="utf-8"))
    assert [p["date"] for p in eq1] == [p["date"] for p in eq2]
    assert [p["equity"] for p in eq1] == [p["equity"] for p in eq2]
