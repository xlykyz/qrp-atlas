"""API tests for indicator/strategy catalog and product backtest tasks."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.api.server import app
from qrp_atlas.backtest.product import (
    BacktestProductService,
    BacktestTaskStore,
    reset_product_service_for_tests,
)
from qrp_atlas.backtest.results.service import set_loader_for_tests
from tests.api.asgi_client import ASGITestClient


def _make_price_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "api_product.duckdb"
    con = duckdb.connect(str(db_path))
    dates = pd.bdate_range("2024-01-02", periods=35)
    closes = [20 + i * 0.1 for i in range(len(dates))]
    rows = []
    for d, close in zip(dates, closes):
        rows.append(
            (
                d.date().isoformat(),
                "600519.SH",
                "Kweichow Moutai",
                close - 0.05,
                close + 0.1,
                close - 0.1,
                close,
                100000.0,
                100000.0 * close,
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


@pytest.fixture
def client(tmp_path: Path) -> ASGITestClient:
    db_path = _make_price_db(tmp_path)
    service = BacktestProductService(
        task_store=BacktestTaskStore(tmp_path / "tasks"),
        runs_dir=tmp_path / "runs",
        db_path=db_path,
        execute_inline=True,
    )
    reset_product_service_for_tests(service)
    try:
        yield ASGITestClient(app)
    finally:
        reset_product_service_for_tests(None)


def test_list_indicators_and_strategies(client: ASGITestClient):
    ind = client.get("/api/indicators")
    assert ind.status_code == 200
    indicators = ind.json()
    assert isinstance(indicators, list)
    assert any(item["code"] == "sma" for item in indicators)

    strat = client.get("/api/strategies")
    assert strat.status_code == 200
    strategies = strat.json()
    codes = {item["code"] for item in strategies}
    assert "dual_sma_trend" in codes
    dual = next(item for item in strategies if item["code"] == "dual_sma_trend")
    assert "parameter_schema" in dual
    assert "fast_window" in dual["parameter_schema"]

    one = client.get("/api/strategies/dual_sma_trend", params={"version": "1.0.0"})
    assert one.status_code == 200
    assert one.json()["code"] == "dual_sma_trend"


def test_create_and_get_task_success(client: ASGITestClient):
    payload = {
        "name": "api dual sma",
        "strategy_code": "dual_sma_trend",
        "strategy_version": "1.0.0",
        "strategy_params": {"fast_window": 3, "slow_window": 5},
        "universe_mode": "tickers",
        "tickers": ["600519.SH"],
        "start_date": "2024-01-15",
        "end_date": "2024-02-20",
        "position": {
            "initial_cash": 1000000,
            "max_positions": 1,
            "max_weight_per_symbol": 1.0,
        },
        "cost": {
            "commission_rate": 0.00025,
            "stamp_tax_rate": 0.0005,
            "slippage_bps": 0,
        },
        "execution": {"entry_timing": "next_open"},
    }
    created = client.post("/api/backtest/tasks", json=payload)
    assert created.status_code == 200, created.text
    body = created.json()
    task = body["task"]
    assert task["status"] == "succeeded"
    assert task["run_id"]
    assert task["is_mock"] is False
    assert task["request_snapshot"]["strategy_code"] == "dual_sma_trend"

    got = client.get(f"/api/backtest/tasks/{task['task_id']}")
    assert got.status_code == 200
    assert got.json()["task_id"] == task["task_id"]

    listed = client.get("/api/backtest/tasks")
    assert listed.status_code == 200
    assert any(item["task_id"] == task["task_id"] for item in listed.json())

    run_id = task["run_id"]
    from qrp_atlas.backtest.product import get_product_service
    from qrp_atlas.backtest.results.loader import BacktestRunsLoader
    from qrp_atlas.backtest.results.service import set_loader_for_tests

    runs_dir = get_product_service().runs_dir
    set_loader_for_tests(BacktestRunsLoader(runs_dir))
    try:
        meta = client.get(f"/api/backtest/runs/{run_id}")
        assert meta.status_code == 200, meta.text
        assert meta.json()["run_id"] == run_id
        summary = client.get(f"/api/backtest/runs/{run_id}/summary")
        assert summary.status_code == 200
        equity = client.get(f"/api/backtest/runs/{run_id}/equity")
        assert equity.status_code == 200
        assert isinstance(equity.json(), list)
    finally:
        set_loader_for_tests(None)


def test_create_task_invalid_strategy(client: ASGITestClient):
    payload = {
        "strategy_code": "nope",
        "strategy_version": "1.0.0",
        "strategy_params": {},
        "universe_mode": "tickers",
        "tickers": ["600519.SH"],
        "start_date": "2024-01-15",
        "end_date": "2024-02-20",
        "position": {
            "initial_cash": 1000000,
            "max_positions": 1,
            "max_weight_per_symbol": 1.0,
        },
        "cost": {
            "commission_rate": 0.00025,
            "stamp_tax_rate": 0.0005,
            "slippage_bps": 0,
        },
        "execution": {"entry_timing": "next_open"},
    }
    resp = client.post("/api/backtest/tasks", json=payload)
    assert resp.status_code == 400
    assert "detail" in resp.json()


def test_create_task_invalid_params(client: ASGITestClient):
    payload = {
        "strategy_code": "dual_sma_trend",
        "strategy_version": "1.0.0",
        "strategy_params": {"fast_window": 20, "slow_window": 5},
        "universe_mode": "tickers",
        "tickers": ["600519.SH"],
        "start_date": "2024-01-15",
        "end_date": "2024-02-20",
        "position": {
            "initial_cash": 1000000,
            "max_positions": 1,
            "max_weight_per_symbol": 1.0,
        },
        "cost": {
            "commission_rate": 0.00025,
            "stamp_tax_rate": 0.0005,
            "slippage_bps": 0,
        },
        "execution": {"entry_timing": "next_open"},
    }
    resp = client.post("/api/backtest/tasks", json=payload)
    assert resp.status_code == 400
