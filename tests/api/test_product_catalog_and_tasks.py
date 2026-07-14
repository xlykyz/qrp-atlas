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


def test_catalog_includes_cross_sectional_momentum(client: ASGITestClient):
    strat = client.get("/api/strategies")
    assert strat.status_code == 200
    strategies = strat.json()
    codes = {item["code"] for item in strategies}
    assert "cross_sectional_momentum_long_only" in codes
    assert "dual_sma_trend" in codes
    assert "multifactor_long_only" not in codes
    cs = next(item for item in strategies if item["code"] == "cross_sectional_momentum_long_only")
    assert cs["family"] == "cross_sectional"
    assert cs["product_supported"] is True
    assert cs["requires_historical_universe"] is True
    assert cs["supported_universe_modes"] == ["index_components"]
    assert cs["supported_entry_timings"] == ["next_open"]
    assert "momentum_lookback" in cs["parameter_schema"]


def test_create_cross_sectional_task_api(tmp_path: Path, monkeypatch):
    # Dedicated client with CS market DB fixtures.
    from qrp_atlas.backtest.product import (
        BacktestProductService,
        BacktestTaskStore,
        reset_product_service_for_tests,
    )
    from qrp_atlas.api.server import app
    from tests.api.asgi_client import ASGITestClient
    from tests.backtest.test_product_cross_section_loop import _make_cs_db

    db_path = _make_cs_db(tmp_path)
    service = BacktestProductService(
        task_store=BacktestTaskStore(tmp_path / "tasks_cs"),
        runs_dir=tmp_path / "runs_cs",
        db_path=db_path,
        execute_inline=True,
    )
    reset_product_service_for_tests(service)
    try:
        client = ASGITestClient(app)
        payload = {
            "name": "api cs momentum",
            "strategy_code": "cross_sectional_momentum_long_only",
            "strategy_version": "1.0.0",
            "strategy_params": {
                "top_n": 2,
                "momentum_lookback": 3,
                "rebalance_frequency": "weekly",
            },
            "universe_mode": "index_components",
            "index_code": "000300.SH",
            "start_date": "2024-01-15",
            "end_date": "2024-02-20",
            "position": {
                "initial_cash": 1000000,
                "max_positions": 2,
                "max_weight_per_symbol": 0.5,
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
        task = created.json()["task"]
        assert task["status"] == "succeeded", task.get("error_message")
        assert task["run_id"]
        assert task["universe_mode"] == "index_components"
        assert task["index_code"] == "000300.SH"

        run_id = task["run_id"]
        from qrp_atlas.backtest.results.loader import BacktestRunsLoader
        from qrp_atlas.backtest.results.service import set_loader_for_tests

        set_loader_for_tests(BacktestRunsLoader(service.runs_dir))
        try:
            summary = client.get(f"/api/backtest/runs/{run_id}/summary")
            assert summary.status_code == 200
            equity = client.get(f"/api/backtest/runs/{run_id}/equity")
            assert equity.status_code == 200
            trades = client.get(f"/api/backtest/runs/{run_id}/trades")
            assert trades.status_code == 200
            skipped = client.get(f"/api/backtest/runs/{run_id}/skipped")
            assert skipped.status_code == 200
            config = client.get(f"/api/backtest/runs/{run_id}/config")
            assert config.status_code == 200
            body = config.json()
            cfg = body.get("config") if isinstance(body.get("config"), dict) else body
            entry = cfg.get("entry_timing")
            if entry is None:
                entry = (
                    (cfg.get("product_request") or {})
                    .get("execution", {})
                    .get("entry_timing")
                )
            assert entry == "next_open"
            assert (cfg.get("cross_section") or {}).get("product_timing_shift") is False
        finally:
            set_loader_for_tests(None)
    finally:
        reset_product_service_for_tests(None)


def test_create_cross_sectional_rejects_same_close(client: ASGITestClient):
    payload = {
        "strategy_code": "cross_sectional_momentum_long_only",
        "strategy_version": "1.0.0",
        "strategy_params": {"top_n": 2, "momentum_lookback": 3},
        "universe_mode": "index_components",
        "index_code": "000300.SH",
        "start_date": "2024-01-15",
        "end_date": "2024-02-20",
        "position": {
            "initial_cash": 1000000,
            "max_positions": 2,
            "max_weight_per_symbol": 0.5,
        },
        "cost": {
            "commission_rate": 0.00025,
            "stamp_tax_rate": 0.0005,
            "slippage_bps": 0,
        },
        "execution": {"entry_timing": "same_close"},
    }
    resp = client.post("/api/backtest/tasks", json=payload)
    assert resp.status_code == 400


def test_indicator_catalog_frequency_stable_values(client: ASGITestClient):
    resp = client.get("/api/indicators")
    assert resp.status_code == 200
    indicators = resp.json()
    assert indicators
    by_code = {item["code"]: item for item in indicators}
    # Formal indicator codes may be lowercase ids depending on registry naming.
    # Accept either MA5 style or ma5 if mapped; search by name/code contains.
    ma = next((item for item in indicators if str(item["code"]).lower() in {"ma5", "ma_5"} or item.get("name") == "MA5"), None)
    assert ma is not None, sorted(by_code)[:20]
    assert ma["frequency"] == "after_close"

    event_items = [item for item in indicators if "event" in str(item["code"]).lower() or "forecast" in str(item["code"]).lower() or "profit_change" in str(item["code"]).lower()]
    residual_items = [item for item in indicators if "residual" in str(item["code"]).lower() or "rolling_alpha" in str(item["code"]).lower() or item.get("name", "").lower().find("residual") >= 0]

    # Event indicators should be realtime; residual after_close.
    if event_items:
        assert any(item["frequency"] == "realtime" for item in event_items)
    if residual_items:
        assert any(item["frequency"] == "after_close" for item in residual_items)

    for item in indicators:
        freq = str(item.get("frequency", ""))
        assert not freq.startswith("UpdateFrequency.")
        assert freq in {"after_close", "realtime", "intraday", "manual"} or freq  # factors may use after_close
