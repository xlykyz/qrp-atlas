"""07-C: standard product results package completion tests."""

from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import PortfolioBacktestConfig, PortfolioExecutionRule
from qrp_atlas.backtest.portfolio.engine import PortfolioBacktestEngine
from qrp_atlas.backtest.results.analytics import (
    align_benchmark_series,
    calmar_ratio,
    daily_returns_from_equity,
    json_safe,
    rolling_performance,
    sharpe_ratio,
    sortino_ratio,
)
from qrp_atlas.backtest.results.loader import BacktestRunsLoader
from qrp_atlas.backtest.results.service import compare_runs, get_costs, get_summary
from qrp_atlas.backtest.results.writer import BacktestRunWriter


def _prices() -> pd.DataFrame:
    days = pd.bdate_range("2024-01-02", periods=40)
    rows = []
    for i, d in enumerate(days):
        close = 10 + i * 0.1
        rows.append(
            {
                "trade_date": d.date().isoformat(),
                "asset_id": "AAA.SZ",
                "asset_name": "AAA",
                "asset_type": "stock",
                "open": close - 0.05,
                "high": close + 0.1,
                "low": close - 0.1,
                "close": close,
                "is_suspended": False,
                "is_limit_up": False,
                "is_limit_down": False,
            }
        )
    return pd.DataFrame(rows)


def _targets() -> pd.DataFrame:
    # buy early, hold, then exit on calendar dates present in _prices()
    return pd.DataFrame(
        [
            {"trade_date": "2024-01-03", "asset_id": "AAA.SZ", "target_weight": 0.5, "signal_date": "2024-01-02"},
            {"trade_date": "2024-01-19", "asset_id": "AAA.SZ", "target_weight": 0.0, "signal_date": "2024-01-18"},
        ]
    )


def test_json_safe_and_risk_metrics():
    assert json_safe({"a": float("nan"), "b": float("inf"), "c": 1.5}) == {
        "a": None,
        "b": None,
        "c": 1.5,
    }
    rets = [0.01, -0.005, 0.002, 0.003, -0.001, 0.004]
    assert sharpe_ratio(rets) is not None
    assert sortino_ratio(rets) is not None
    assert calmar_ratio(12.0, -20.0) == 0.6


def test_benchmark_alignment_no_fill():
    dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
    bench = pd.DataFrame(
        {
            "trade_date": ["2024-01-02", "2024-01-04"],
            "close": [100.0, 110.0],
        }
    )
    aligned, diag = align_benchmark_series(dates, bench)
    assert aligned[1]["benchmark_level"] is None
    assert any("benchmark_gap:2024-01-03" in d for d in diag)
    assert any("no_fill" in d for d in diag)


def test_writer_emits_extended_package(tmp_path: Path):
    engine = PortfolioBacktestEngine()
    result = engine.run(
        _prices(),
        _targets(),
        PortfolioBacktestConfig(
            name="results-completion",
            initial_cash=1_000_000,
            max_positions=5,
            max_weight_per_asset=0.5,
            cost=CostRule(commission_rate=0.0003, stamp_tax_rate=0.0005, slippage_bps=5),
            execution=PortfolioExecutionRule(price_field="open", mark_price_field="close"),
        ),
    )
    run_dir = BacktestRunWriter(tmp_path).write_portfolio_run(
        result,
        run_id="run_results_c",
        strategy_name="dual_sma_trend@1.0.0",
        universe="AAA.SZ",
        config_overlay={
            "product_request": {
                "strategy_code": "dual_sma_trend",
                "strategy_version": "1.0.0",
                "strategy_params": {"fast_window": 5, "slow_window": 20},
            }
        },
    )
    for name in [
        "summary.json",
        "equity.json",
        "daily_returns.json",
        "rolling_performance.json",
        "costs.json",
        "diagnostics.json",
        "orders.json",
        "fills.json",
        "snapshots.json",
        "config.json",
    ]:
        assert (run_dir / name).exists(), name

    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    for key in ["sharpe", "sortino", "calmar", "turnover", "total_cost", "commission"]:
        assert key in summary
        if summary[key] is not None:
            assert math.isfinite(float(summary[key]))

    # no NaN/Inf in written json files
    for path in run_dir.glob("*.json"):
        text = path.read_text(encoding="utf-8")
        assert "NaN" not in text
        assert "Infinity" not in text

    costs = json.loads((run_dir / "costs.json").read_text(encoding="utf-8"))
    assert abs(float(costs["total_cost"]) - (
        float(costs["commission"]) + float(costs["stamp_tax"]) + float(costs["slippage_cost"])
    )) < 1e-9

    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["reproducibility"]["locked_to_run_snapshot"] is True
    assert config["product_request"]["strategy_params"]["fast_window"] == 5

    from qrp_atlas.backtest.results.service import set_loader_for_tests

    loader = BacktestRunsLoader(tmp_path)
    set_loader_for_tests(loader)
    try:
        assert "run_results_c" in loader.list_run_ids()
        loaded_summary = get_summary("run_results_c")
        assert loaded_summary.run_id == "run_results_c"
        assert get_costs("run_results_c") is not None

        # compare works
        compare = compare_runs(["run_results_c", "missing_run"])
        assert len(compare.runs) == 1
        assert "missing_run" in compare.missing

        rolling = loader.load_rolling_performance("run_results_c")
        assert isinstance(rolling, list)
        assert rolling
        daily = daily_returns_from_equity(loader.load_equity("run_results_c"))
        assert daily[0]["daily_return"] == 0.0
        assert rolling_performance(loader.load_equity("run_results_c"))
    finally:
        set_loader_for_tests(None)


def test_old_package_costs_fallback(tmp_path: Path):
    run = tmp_path / "old_run"
    run.mkdir()
    (run / "run_meta.json").write_text(
        json.dumps(
            {
                "run_id": "old_run",
                "name": "old",
                "strategy_name": "x",
                "universe": "u",
                "start_date": "2024-01-01",
                "end_date": "2024-01-10",
                "created_at": "2024-01-01T00:00:00+00:00",
                "status": "completed",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "old_run",
                "total_return_pct": 1.0,
                "annual_return_pct": 2.0,
                "max_drawdown_pct": -3.0,
                "win_rate_pct": 50.0,
                "profit_loss_ratio": 1.2,
                "trade_count": 1,
                "avg_holding_days": 2,
                "max_trade_loss_pct": -1.0,
                "max_trade_profit_pct": 2.0,
                "skipped_count": 0,
                "commission": 1.0,
                "stamp_tax": 2.0,
                "slippage_cost": 3.0,
                "total_cost": 6.0,
                "turnover": 0.1,
                "final_equity": 100.0,
            }
        ),
        encoding="utf-8",
    )
    for name, payload in {
        "equity.json": [],
        "trades.json": [],
        "skipped.json": [],
        "config.json": {},
    }.items():
        (run / name).write_text(json.dumps(payload), encoding="utf-8")

    from qrp_atlas.backtest.results.service import set_loader_for_tests

    set_loader_for_tests(BacktestRunsLoader(tmp_path))
    try:
        costs = get_costs("old_run")
        assert costs is not None
        assert costs.total_cost == 6.0
        summary = get_summary("old_run")
        assert summary.sharpe is None  # old package may lack risk metrics
    finally:
        set_loader_for_tests(None)
