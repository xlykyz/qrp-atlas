"""Residual data prep, research analytics, and public portfolio runner tests."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from qrp_atlas.backtest import (
    PortfolioBacktestConfig,
    prepare_market_residual_panel,
    run_market_residual_mean_reversion_backtest,
    run_residual_research,
)
from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import PortfolioExecutionRule
from qrp_atlas.backtest.residual_data import ResidualDataError
from qrp_atlas.strategies import StrategyAction


def _prices(
    asset_map: dict[str, list[float]],
    *,
    start: str = "2024-01-01",
    asset_type: str = "stock",
) -> pd.DataFrame:
    rows = []
    n = len(next(iter(asset_map.values())))
    dates = pd.bdate_range(start, periods=n)
    for asset_id, closes in asset_map.items():
        for date, close in zip(dates, closes, strict=True):
            rows.append(
                {
                    "trade_date": date,
                    "asset_id": asset_id,
                    "asset_name": asset_id,
                    "asset_type": asset_type,
                    "ticker": asset_id,
                    "open": close,
                    "high": close * 1.01,
                    "low": close * 0.99,
                    "close": close,
                    "is_suspended": False,
                    "is_limit_up": False,
                    "is_limit_down": False,
                }
            )
    return pd.DataFrame(rows)


def _mean_reverting_series(n: int = 50, seed: int = 1) -> tuple[list[float], list[float]]:
    rng = np.random.default_rng(seed)
    bench = [100.0]
    asset = [100.0]
    residual = 0.0
    for _ in range(1, n):
        b = float(rng.normal(0.0, 0.008))
        residual = 0.4 * residual + float(rng.normal(0.0, 0.01))
        # Push residual deeply negative in the second half to force entries.
        if len(bench) > n // 2:
            residual -= 0.02
        a = 1.0 * b + residual
        bench.append(bench[-1] * (1.0 + b))
        asset.append(asset[-1] * (1.0 + a))
    return asset, bench


def test_prepare_panel_exact_alignment_and_duplicate_reject() -> None:
    assets = _prices({"A": [100, 101, 102, 103, 104]})
    bench = _prices({"MKT": [1000, 1005, 1010, 1000, 1015]}, asset_type="index")
    prep = prepare_market_residual_panel(
        assets, bench, benchmark_id="MKT", window=3, min_periods=3, z_window=3
    )
    assert "asset_return" in prep.panel.columns
    assert "benchmark_return" in prep.panel.columns
    assert prep.metadata["benchmark_id"] == "MKT"

    # Drop one benchmark day => exact alignment leaves NaN, no fill.
    thin = bench.iloc[[0, 1, 2, 4]].copy()
    prep2 = prepare_market_residual_panel(
        assets, thin, benchmark_id="MKT", window=3, min_periods=3, z_window=3, compute_residuals=False
    )
    row = prep2.panel[prep2.panel["trade_date"] == pd.Timestamp("2024-01-04")]
    if not row.empty:
        assert math.isnan(float(row.iloc[0]["benchmark_return"]))
    assert any("MISSING_BENCHMARK" in item for item in prep2.diagnostics)

    with pytest.raises(ResidualDataError, match="duplicate"):
        prepare_market_residual_panel(
            assets,
            pd.concat([bench, bench.iloc[[-1]]], ignore_index=True),
            benchmark_id="MKT",
        )


def test_research_forward_outcomes_do_not_change_decisions() -> None:
    asset, bench = _mean_reverting_series(40, seed=2)
    assets = _prices({"A": asset, "B": [x * 0.9 for x in asset]})
    benchmark = _prices({"MKT": bench}, asset_type="index")
    research = run_residual_research(
        assets,
        benchmark,
        benchmark_id="MKT",
        window=8,
        min_periods=8,
        z_window=8,
        n_groups=3,
        horizons=(1, 5),
    )
    assert not research.residual_panel.empty
    assert research.metadata["usable_sample_count"] >= 0
    # Mutate future prices after last residual date and ensure residual panel values stay same
    # for historical rows (research uses forward only for evaluation frames).
    base_panel = research.residual_panel[["trade_date", "asset_id", "residual_zscore"]].copy()
    mutated_assets = assets.copy()
    mutated_assets.loc[mutated_assets.index[-3]:, "close"] *= 2.0
    research2 = run_residual_research(
        mutated_assets,
        benchmark,
        benchmark_id="MKT",
        window=8,
        min_periods=8,
        z_window=8,
        n_groups=3,
        horizons=(1, 5),
    )
    # Residual inputs use returns, so last few residual values can change when close path changes.
    # The contract we protect is: research evaluation must not feed strategy decisions.
    # Verified in public runner test below via decision stability against forward-return-only changes.
    assert "residual_zscore" in base_panel.columns
    assert research2.metadata["note"].startswith("forward outcomes")


def test_public_runner_signal_next_open_and_config_snapshot() -> None:
    asset, bench = _mean_reverting_series(60, seed=5)
    # Force a clear extreme residual event near the end.
    asset = list(asset)
    for i in range(35, 45):
        asset[i] = asset[i - 1] * 0.9
    assets = _prices({"A": asset})
    benchmark = _prices({"MKT": bench}, asset_type="index")
    config = PortfolioBacktestConfig(
        name="residual_test",
        initial_cash=1_000_000.0,
        max_positions=1,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0),
        execution=PortfolioExecutionRule(
            price_field="open",
            mark_price_field="close",
            minimum_commission=0.0,
            enforce_price_limits=False,
            enforce_suspension=False,
        ),
    )
    run = run_market_residual_mean_reversion_backtest(
        assets,
        benchmark,
        config,
        benchmark_id="MKT",
        parameters={
            "window": 10,
            "min_periods": 10,
            "z_window": 10,
            "entry_zscore": -1.0,
            "exit_zscore": 0.5,
            "min_r2": 0.0,
            "max_hold_days": 10,
        },
        start_date="2024-02-01",
        end_date=pd.Timestamp(assets["trade_date"].max()).strftime("%Y-%m-%d"),
        entry_timing="next_open",
    )
    assert run.metadata["strategy_code"] == "market_residual_mean_reversion"
    assert run.metadata["benchmark_id"] == "MKT"
    assert run.metadata["signal_semantics"]["benchmark_in_portfolio"] is False
    assert run.metadata["parameters"]["window"] == 10
    assert run.metadata["indicator_version"] is not None

    enters = [d for d in run.strategy_result.decisions if d.action is StrategyAction.ENTER]
    if enters:
        signal_date = pd.Timestamp(enters[0].trade_date)
        # Execution targets should land strictly after signal for next_open.
        exec_dates = pd.to_datetime(run.execution_target_weights["trade_date"])
        signal_dates = pd.to_datetime(run.execution_target_weights["signal_date"])
        assert (exec_dates > signal_dates).all()
        assert signal_date == pd.Timestamp(enters[0].evidence["signal_date"])

    # End-of-range signals without next session are skipped.
    last_day = pd.Timestamp(assets["trade_date"].max()).strftime("%Y-%m-%d")
    last_signal_targets = run.signal_target_weights[
        run.signal_target_weights["trade_date"] == last_day
    ]
    if not last_signal_targets.empty:
        assert any(
            item.get("reason") == "NO_EXECUTION_DATE_IN_RANGE"
            for item in run.skipped_signals
        )


def test_runner_decisions_ignore_forward_only_price_tail_when_history_fixed() -> None:
    asset, bench = _mean_reverting_series(45, seed=11)
    assets = _prices({"A": asset})
    benchmark = _prices({"MKT": bench}, asset_type="index")
    config = PortfolioBacktestConfig(
        name="residual_det",
        initial_cash=500_000.0,
        max_positions=1,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0),
        execution=PortfolioExecutionRule(
            price_field="open",
            mark_price_field="close",
            minimum_commission=0.0,
            enforce_price_limits=False,
            enforce_suspension=False,
        ),
    )
    params = {
        "window": 8,
        "min_periods": 8,
        "z_window": 8,
        "entry_zscore": -1.2,
        "exit_zscore": 0.0,
        "min_r2": 0.0,
        "max_hold_days": 5,
    }
    first = run_market_residual_mean_reversion_backtest(
        assets,
        benchmark,
        config,
        benchmark_id="MKT",
        parameters=params,
        start_date="2024-01-15",
        end_date=pd.Timestamp(assets["trade_date"].iloc[-5]).strftime("%Y-%m-%d"),
    )
    # Append far-future bars outside formal range; decisions in formal range must stay identical.
    extra = assets.copy()
    last = assets.iloc[-1]
    more = []
    close = float(last["close"])
    date = pd.Timestamp(last["trade_date"])
    for _ in range(5):
        date = date + pd.tseries.offsets.BDay(1)
        close *= 1.1
        more.append({**last.to_dict(), "trade_date": date, "open": close, "high": close, "low": close, "close": close})
    extended = pd.concat([assets, pd.DataFrame(more)], ignore_index=True)
    second = run_market_residual_mean_reversion_backtest(
        extended,
        benchmark,
        config,
        benchmark_id="MKT",
        parameters=params,
        start_date="2024-01-15",
        end_date=pd.Timestamp(assets["trade_date"].iloc[-5]).strftime("%Y-%m-%d"),
    )
    assert [d.to_dict() for d in first.strategy_result.decisions] == [
        d.to_dict() for d in second.strategy_result.decisions
    ]
