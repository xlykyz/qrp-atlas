"""Product backtest loop: catalog, task persistence, real dual_sma execution."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.backtest.product import (
    BacktestProductService,
    BacktestTaskStore,
    BacktestTaskValidationError,
    CreateBacktestTaskRequest,
    execute_validated_task,
    list_indicator_catalog,
    list_strategy_catalog,
    validate_create_request,
)
from qrp_atlas.backtest.product.schemas import (
    BacktestCostConfigDTO,
    BacktestExecutionConfigDTO,
    BacktestPositionConfigDTO,
)
from qrp_atlas.backtest.results import (
    BacktestRunsLoader,
    BacktestSummary,
    get_equity,
    get_run_meta,
    get_summary,
)
from qrp_atlas.backtest.results import service as results_service
from qrp_atlas.backtest.results.service import set_loader_for_tests


def _make_price_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "product.duckdb"
    con = duckdb.connect(str(db_path))
    dates = pd.bdate_range("2024-01-02", periods=40)
    # Mild uptrend with a few oscillations for dual SMA crosses.
    closes = [10 + i * 0.15 + ((-1) ** i) * 0.2 for i in range(len(dates))]
    rows = []
    for d, close in zip(dates, closes):
        open_px = close - 0.05
        high = close + 0.1
        low = close - 0.1
        rows.append(
            (
                d.date().isoformat(),
                "000001.SZ",
                "Ping An Bank",
                open_px,
                high,
                low,
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
    con.executemany(
        "INSERT INTO daily_market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    con.close()
    return db_path


def _request(**overrides):
    base = dict(
        name="dual_sma product test",
        strategy_code="dual_sma_trend",
        strategy_version="1.0.0",
        strategy_params={"fast_window": 3, "slow_window": 5},
        universe_mode="tickers",
        tickers=["000001.SZ"],
        start_date="2024-01-15",
        end_date="2024-02-28",
        position=BacktestPositionConfigDTO(
            initial_cash=1_000_000,
            max_positions=1,
            max_weight_per_symbol=1.0,
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


def test_catalog_lists_product_strategies_and_indicators():
    strategies = list_strategy_catalog(product_only=True)
    codes = {item.code for item in strategies}
    assert "dual_sma_trend" in codes
    dual = next(item for item in strategies if item.code == "dual_sma_trend")
    assert dual.version == "1.0.0"
    assert "fast_window" in dual.parameter_schema
    assert dual.parameter_schema["fast_window"].type == "integer"

    indicators = list_indicator_catalog()
    assert any(item.code == "sma" for item in indicators)
    assert any(item.code == "system_b_trend_valid" for item in indicators)


def test_validate_rejects_unknown_strategy_and_bad_params():
    with pytest.raises(BacktestTaskValidationError, match="not supported|unknown"):
        validate_create_request(_request(strategy_code="not_a_strategy"))

    with pytest.raises(BacktestTaskValidationError):
        validate_create_request(
            _request(strategy_params={"fast_window": 10, "slow_window": 5})
        )

    with pytest.raises(BacktestTaskValidationError, match="tickers"):
        validate_create_request(_request(tickers=[]))

    with pytest.raises(BacktestTaskValidationError, match="preset"):
        validate_create_request(_request(universe_mode="preset", universe_preset="CSI300"))


def test_task_store_persists_status_and_request_snapshot(tmp_path: Path):
    store = BacktestTaskStore(tmp_path / "tasks")
    created = store.create(_request())
    assert created.status == "pending"
    assert created.is_mock is False
    assert created.request_snapshot["strategy_code"] == "dual_sma_trend"

    running = store.update(created.task_id, status="running")
    assert running.status == "running"
    succeeded = store.update(created.task_id, status="succeeded", run_id="run_abc")
    assert succeeded.status == "succeeded"
    assert succeeded.run_id == "run_abc"

    loaded = store.get(created.task_id)
    assert loaded.run_id == "run_abc"
    assert loaded.request_snapshot["start_date"] == "2024-01-15"
    listed = store.list()
    assert any(item.task_id == created.task_id for item in listed)


def test_real_dual_sma_execution_writes_reloadable_results(tmp_path: Path, monkeypatch):
    db_path = _make_price_db(tmp_path)
    runs_dir = tmp_path / "runs"
    request = validate_create_request(_request())
    run_id, run_dir = execute_validated_task(
        request,
        run_id="dual_sma_product_001",
        runs_dir=runs_dir,
        db_path=db_path,
    )
    assert run_id == "dual_sma_product_001"
    assert (run_dir / "run_meta.json").exists()
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "equity.json").exists()
    assert (run_dir / "trades.json").exists()
    assert (run_dir / "skipped.json").exists()
    assert (run_dir / "config.json").exists()
    assert (run_dir / "fills.json").exists()

    loader = BacktestRunsLoader(runs_dir)
    meta = loader.load_run_meta(run_id)
    summary = BacktestSummary.model_validate({"run_id": run_id, **loader.load_summary(run_id)})
    equity = loader.load_equity(run_id)
    assert meta["strategy_name"].startswith("dual_sma_trend@")
    assert meta["status"] in {"completed", "success", "succeeded"}
    assert isinstance(summary.trade_count, int)
    assert len(equity) >= 1

    # Point result service at this runs dir and re-read through public service API.
    monkeypatch.setenv("QRP_ATLAS_BACKTEST_RUNS_DIR", str(runs_dir))
    set_loader_for_tests(BacktestRunsLoader(runs_dir))
    try:
        reloaded_meta = get_run_meta(run_id)
        reloaded_summary = get_summary(run_id)
        reloaded_equity = get_equity(run_id)
        assert reloaded_meta.run_id == run_id
        assert reloaded_summary.run_id == run_id
        assert len(reloaded_equity) == len(equity)
    finally:
        set_loader_for_tests(None)


def test_insufficient_data_fails_task(tmp_path: Path):
    db_path = _make_price_db(tmp_path)
    service = BacktestProductService(
        task_store=BacktestTaskStore(tmp_path / "tasks"),
        runs_dir=tmp_path / "runs",
        db_path=db_path,
        execute_inline=True,
    )
    response = service.create_task(
        _request(tickers=["999999.SZ"], start_date="2024-01-15", end_date="2024-02-28")
    )
    task = response.task
    assert task.status == "failed"
    assert task.run_id is None
    assert task.error_message
    assert "missing market data" in task.error_message or "no market data" in task.error_message


def test_product_service_success_path(tmp_path: Path):
    db_path = _make_price_db(tmp_path)
    service = BacktestProductService(
        task_store=BacktestTaskStore(tmp_path / "tasks"),
        runs_dir=tmp_path / "runs",
        db_path=db_path,
        execute_inline=True,
    )
    response = service.create_task(_request())
    task = response.task
    assert task.status == "succeeded"
    assert task.run_id
    assert task.is_mock is False
    assert (tmp_path / "runs" / task.run_id / "summary.json").exists()
