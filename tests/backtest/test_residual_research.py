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
from qrp_atlas.backtest.research.residual import ResidualResearchError
from qrp_atlas.backtest.residual_data import ResidualDataError
from qrp_atlas.strategies import StrategyAction, StrategyInput, run_strategy


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
                    "open": float(close),
                    "high": float(close) * 1.01 if math.isfinite(float(close)) else float(close),
                    "low": float(close) * 0.99 if math.isfinite(float(close)) else float(close),
                    "close": float(close),
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
        if len(bench) > n // 2:
            residual -= 0.02
        a = 1.0 * b + residual
        bench.append(bench[-1] * (1.0 + b))
        asset.append(asset[-1] * (1.0 + a))
    return asset, bench


def _open_config(name: str = "residual_test", cash: float = 1_000_000.0) -> PortfolioBacktestConfig:
    return PortfolioBacktestConfig(
        name=name,
        initial_cash=cash,
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


def test_prepare_panel_exact_alignment_and_duplicate_reject() -> None:
    assets = _prices({"A": [100, 101, 102, 103, 104]})
    bench = _prices({"MKT": [1000, 1005, 1010, 1000, 1015]}, asset_type="index")
    prep = prepare_market_residual_panel(
        assets, bench, benchmark_id="MKT", window=3, min_periods=3, z_window=3
    )
    assert "asset_return" in prep.panel.columns
    assert "benchmark_return" in prep.panel.columns
    assert prep.metadata["benchmark_id"] == "MKT"

    thin = bench.iloc[[0, 1, 2, 4]].copy()
    prep2 = prepare_market_residual_panel(
        assets,
        thin,
        benchmark_id="MKT",
        window=3,
        min_periods=3,
        z_window=3,
        compute_residuals=False,
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


@pytest.mark.parametrize(
    "mutator,label",
    [
        (lambda closes: closes[:3] + [0.0] + closes[4:], "current_zero"),
        (lambda closes: closes[:3] + [-5.0] + closes[4:], "current_negative"),
        (lambda closes: closes[:2] + [0.0] + closes[3:], "previous_zero"),
        (lambda closes: closes[:3] + [math.inf] + closes[4:], "current_inf"),
        (lambda closes: closes[:3] + [-math.inf] + closes[4:], "current_neginf"),
    ],
)
def test_illegal_prices_produce_nan_returns_and_no_enter(mutator, label) -> None:
    base = [100, 101, 102, 103, 104, 105, 106, 107]
    assets = _prices({"A": mutator(base)})
    bench = _prices({"MKT": [1000 + i for i in range(len(base))]}, asset_type="index")
    prep = prepare_market_residual_panel(
        assets,
        bench,
        benchmark_id="MKT",
        window=3,
        min_periods=3,
        z_window=3,
        compute_residuals=True,
    )
    # At least one asset return should be NaN around illegal price.
    assert prep.panel["asset_return"].isna().any()
    # Residual must not become finite from illegal prices for those NaN return rows.
    bad = prep.panel[prep.panel["asset_return"].isna()]
    if not bad.empty:
        assert bad["residual_return"].isna().all()
    decisions = run_strategy(
        "market_residual_mean_reversion",
        StrategyInput(
            prep.panel.assign(columns={"asset_id": "ticker"}) if "ticker" not in prep.panel else prep.panel,
            parameters={
                "window": 3,
                "min_periods": 3,
                "z_window": 3,
                "entry_zscore": -0.01,
                "exit_zscore": 5.0,
                "min_r2": 0.0,
            },
        ),
    )
    # Illegal return rows should not create ENTER on those exact dates when residual NaN.
    for decision in decisions.decisions:
        if decision.action is StrategyAction.ENTER:
            row = prep.panel[
                (prep.panel["ticker"] == decision.asset_id)
                & (pd.to_datetime(prep.panel["trade_date"]).dt.strftime("%Y-%m-%d") == decision.trade_date)
            ]
            if not row.empty:
                assert math.isfinite(float(row.iloc[0]["residual_return"]))


@pytest.mark.parametrize(
    "mutator",
    [
        lambda closes: closes[:3] + [0.0] + closes[4:],
        lambda closes: closes[:3] + [-1.0] + closes[4:],
        lambda closes: closes[:2] + [0.0] + closes[3:],
        lambda closes: closes[:3] + [math.inf] + closes[4:],
    ],
)
def test_illegal_benchmark_prices_produce_nan_benchmark_return(mutator) -> None:
    base = [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007]
    assets = _prices({"A": [100 + i for i in range(len(base))]})
    bench = _prices({"MKT": mutator(base)}, asset_type="index")
    prep = prepare_market_residual_panel(
        assets,
        bench,
        benchmark_id="MKT",
        window=3,
        min_periods=3,
        z_window=3,
        compute_residuals=True,
    )
    assert prep.panel["benchmark_return"].isna().any()
    bad = prep.panel[prep.panel["benchmark_return"].isna()]
    assert bad["residual_return"].isna().all()


def test_public_runner_rejects_non_next_open_and_non_open_price() -> None:
    asset, bench = _mean_reverting_series(30, seed=3)
    assets = _prices({"A": asset})
    benchmark = _prices({"MKT": bench}, asset_type="index")
    open_cfg = _open_config()
    close_cfg = PortfolioBacktestConfig(
        name="close",
        initial_cash=1_000_000.0,
        max_positions=1,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0),
        execution=PortfolioExecutionRule(price_field="close", mark_price_field="close", minimum_commission=0.0),
    )
    params = {
        "window": 8,
        "min_periods": 8,
        "z_window": 8,
        "entry_zscore": -1.0,
        "exit_zscore": 0.5,
        "min_r2": 0.0,
        "max_hold_days": 5,
    }
    with pytest.raises(ResidualResearchError, match="entry_timing"):
        run_market_residual_mean_reversion_backtest(
            assets, benchmark, open_cfg, benchmark_id="MKT", parameters=params, entry_timing="same_close"
        )
    with pytest.raises(ResidualResearchError, match="entry_timing"):
        run_market_residual_mean_reversion_backtest(
            assets, benchmark, open_cfg, benchmark_id="MKT", parameters=params, entry_timing="next_close"
        )
    with pytest.raises(ResidualResearchError, match="price_field"):
        run_market_residual_mean_reversion_backtest(
            assets, benchmark, close_cfg, benchmark_id="MKT", parameters=params, entry_timing="next_open"
        )


def test_public_runner_next_open_fill_uses_open_price() -> None:
    # Deterministic prices with open != close so fill price can be verified.
    n = 25
    dates = pd.bdate_range("2024-01-01", periods=n)
    asset_close = [100.0 + i for i in range(n)]
    asset_open = [c + 7.0 for c in asset_close]
    # Create residual extremes mid-sample by crashing asset closes while benchmark steady.
    for i in range(12, 16):
        asset_close[i] = asset_close[i - 1] * 0.8
        asset_open[i] = asset_close[i] + 7.0
    assets = pd.DataFrame(
        {
            "trade_date": dates,
            "asset_id": "A",
            "asset_name": "A",
            "asset_type": "stock",
            "ticker": "A",
            "open": asset_open,
            "high": [c + 8 for c in asset_close],
            "low": [c - 1 for c in asset_close],
            "close": asset_close,
            "is_suspended": False,
            "is_limit_up": False,
            "is_limit_down": False,
        }
    )
    benchmark = _prices({"MKT": [1000.0 + 0.1 * i for i in range(n)]}, asset_type="index")
    run = run_market_residual_mean_reversion_backtest(
        assets,
        benchmark,
        _open_config(),
        benchmark_id="MKT",
        parameters={
            "window": 6,
            "min_periods": 6,
            "z_window": 6,
            "entry_zscore": -0.5,
            "exit_zscore": 5.0,
            "min_r2": 0.0,
            "max_hold_days": 10,
        },
        start_date="2024-01-10",
        end_date=pd.Timestamp(dates[-1]).strftime("%Y-%m-%d"),
        entry_timing="next_open",
    )
    if not run.execution_target_weights.empty:
        assert (
            pd.to_datetime(run.execution_target_weights["trade_date"])
            > pd.to_datetime(run.execution_target_weights["signal_date"])
        ).all()
    fills = run.portfolio_result.fills
    if fills:
        buy = next(fill for fill in fills if fill.side == "BUY")
        day = assets[pd.to_datetime(assets["trade_date"]) == pd.Timestamp(buy.trade_date)].iloc[0]
        assert buy.execution_price == pytest.approx(float(day["open"]))
        assert buy.execution_price != pytest.approx(float(day["close"]))


def test_forward_only_price_changes_do_not_change_formal_decisions() -> None:
    asset, bench = _mean_reverting_series(50, seed=11)
    assets = _prices({"A": asset})
    benchmark = _prices({"MKT": bench}, asset_type="index")
    params = {
        "window": 8,
        "min_periods": 8,
        "z_window": 8,
        "entry_zscore": -1.0,
        "exit_zscore": 0.0,
        "min_r2": 0.0,
        "max_hold_days": 5,
    }
    formal_start = "2024-01-15"
    formal_end = pd.Timestamp(assets["trade_date"].iloc[-8]).strftime("%Y-%m-%d")
    first = run_market_residual_mean_reversion_backtest(
        assets,
        benchmark,
        _open_config("det1"),
        benchmark_id="MKT",
        parameters=params,
        start_date=formal_start,
        end_date=formal_end,
    )
    # Mutate only prices after formal_end used by forward evaluation/research.
    mutated = assets.copy()
    post_mask = pd.to_datetime(mutated["trade_date"]) > pd.Timestamp(formal_end)
    mutated.loc[post_mask, "close"] = mutated.loc[post_mask, "close"] * 1.5
    mutated.loc[post_mask, "open"] = mutated.loc[post_mask, "open"] * 1.5
    second = run_market_residual_mean_reversion_backtest(
        mutated,
        benchmark,
        _open_config("det2"),
        benchmark_id="MKT",
        parameters=params,
        start_date=formal_start,
        end_date=formal_end,
    )
    assert [d.to_dict() for d in first.strategy_result.decisions] == [
        d.to_dict() for d in second.strategy_result.decisions
    ]
    pd.testing.assert_frame_equal(
        first.signal_target_weights.reset_index(drop=True),
        second.signal_target_weights.reset_index(drop=True),
    )

    research1 = run_residual_research(
        assets,
        benchmark,
        benchmark_id="MKT",
        window=8,
        min_periods=8,
        z_window=8,
        n_groups=3,
        horizons=(1, 5),
    )
    research2 = run_residual_research(
        mutated,
        benchmark,
        benchmark_id="MKT",
        window=8,
        min_periods=8,
        z_window=8,
        n_groups=3,
        horizons=(1, 5),
    )
    # Formal residual values before formal_end should match; later evaluation frames may differ.
    formal_mask1 = pd.to_datetime(research1.residual_panel["trade_date"]) <= pd.Timestamp(formal_end)
    formal_mask2 = pd.to_datetime(research2.residual_panel["trade_date"]) <= pd.Timestamp(formal_end)
    cols = ["trade_date", "asset_id", "residual_zscore"]
    pd.testing.assert_frame_equal(
        research1.residual_panel.loc[formal_mask1, cols].reset_index(drop=True),
        research2.residual_panel.loc[formal_mask2, cols].reset_index(drop=True),
    )


def test_public_runner_end_of_range_skipped_and_config_snapshot() -> None:
    asset, bench = _mean_reverting_series(40, seed=5)
    asset = list(asset)
    for i in range(20, 28):
        asset[i] = asset[i - 1] * 0.9
    assets = _prices({"A": asset})
    benchmark = _prices({"MKT": bench}, asset_type="index")
    run = run_market_residual_mean_reversion_backtest(
        assets,
        benchmark,
        _open_config(),
        benchmark_id="MKT",
        parameters={
            "window": 8,
            "min_periods": 8,
            "z_window": 8,
            "entry_zscore": -0.8,
            "exit_zscore": 0.5,
            "min_r2": 0.0,
            "max_hold_days": 8,
        },
        start_date="2024-01-15",
        end_date=pd.Timestamp(assets["trade_date"].max()).strftime("%Y-%m-%d"),
        entry_timing="next_open",
    )
    assert run.metadata["strategy_code"] == "market_residual_mean_reversion"
    assert run.metadata["entry_timing"] == "next_open"
    assert run.metadata["execution_price_field"] == "open"
    assert run.metadata["signal_semantics"]["benchmark_in_portfolio"] is False
    last_day = pd.Timestamp(assets["trade_date"].max()).strftime("%Y-%m-%d")
    last_signal_targets = run.signal_target_weights[
        run.signal_target_weights["trade_date"] == last_day
    ]
    if not last_signal_targets.empty:
        assert any(item.get("reason") == "NO_EXECUTION_DATE_IN_RANGE" for item in run.skipped_signals)
