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
    # Last daily signal has no next open inside range.
    assert product_skips
    assert summary["skipped_count"] >= len(skipped)


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
