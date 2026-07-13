from __future__ import annotations

import pandas as pd
import pytest

from qrp_atlas.backtest import (
    BacktestConfig,
    CostRule,
    EntryRule,
    ExitRule,
    PositionRule,
    StrategyBacktestRuntime,
    run_strategy_backtest,
)


def _config() -> BacktestConfig:
    return BacktestConfig(
        name="system-b-runtime",
        entry=EntryRule(timing="signal_close", price_field="close"),
        exit=ExitRule(type="hold_n_bars", bars=1, price_field="close"),
        position=PositionRule(100_000.0, 1.0, 1, False, False),
        cost=CostRule(commission_rate=0.001, stamp_tax_rate=0.001, slippage_bps=5.0),
    )


def _prices(asset_id: str = "000001.SZ") -> pd.DataFrame:
    closes = [10, 10, 10, 10, 10, 11, 12, 11, 10, 9, 8]
    return pd.DataFrame([
        {
            "asset_id": asset_id,
            "asset_name": asset_id,
            "asset_type": "stock",
            "trade_date": f"2024-01-{day:02d}",
            "open": close,
            "high": close + 0.5,
            "low": close - 0.5,
            "close": close,
        }
        for day, close in enumerate(closes, start=1)
    ])


def test_system_b_strategy_runtime_end_to_end_dynamic_exit_and_costs() -> None:
    run = StrategyBacktestRuntime().run("system_b_basic", _prices(), _config())
    decisions = run.strategy_result.decisions
    assert any(item.action.value == "ENTER" for item in decisions)
    assert any(item.action.value == "HOLD" for item in decisions)
    assert any(item.action.value == "EXIT" for item in decisions)

    result = run.backtest_result
    assert result.summary["trade_count"] == 1
    assert result.summary["skipped_count"] == 0
    trade = result.trades[0]
    assert (trade.entry_date, trade.entry_price) == ("2024-01-06", 11.0)
    assert (trade.exit_date, trade.exit_price) == ("2024-01-10", 9.0)
    assert trade.holding_bars == 4
    assert trade.net_return < trade.gross_return
    assert trade.meta["strategy_code"] == "system_b_basic"
    assert run_strategy_backtest("system_b_basic", _prices(), _config()).to_dict() == result.to_dict()


def test_strategy_runtime_keeps_assets_independent() -> None:
    first = _prices("000001.SZ")
    second = _prices("000002.SZ")
    second.loc[:, "close"] = [10, 10, 10, 10, 10, 9, 8, 7, 8, 9, 10]
    second.loc[:, "open"] = second["close"]
    second.loc[:, "high"] = second["close"] + 0.5
    second.loc[:, "low"] = second["close"] - 0.5
    result = StrategyBacktestRuntime().run("system_b_basic", pd.concat([first, second]), _config())
    assert {decision.asset_id for decision in result.strategy_result.decisions} == {"000001.SZ", "000002.SZ"}
    assert [trade.asset_id for trade in result.backtest_result.trades] == ["000001.SZ"]


def _classic_prices(closes: list[float], asset_id: str = "000003.SZ") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "asset_id": asset_id,
                "asset_name": asset_id,
                "asset_type": "stock",
                "trade_date": f"2024-02-{day:02d}",
                "open": close,
                "high": close + 0.1,
                "low": close - 0.1,
                "close": close,
            }
            for day, close in enumerate(closes, 1)
        ]
    )


@pytest.mark.parametrize(
    "code,closes,parameters",
    [
        ("time_series_momentum", [10, 10, 12, 13, 9], {"lookback": 1, "threshold": 0.0}),
        ("dual_sma_trend", [3, 2, 1, 2, 3, 4, 2, 1], {"fast_window": 2, "slow_window": 3}),
        ("donchian_breakout", [10, 10, 12, 13, 8], {"entry_window": 2, "exit_window": 2}),
        (
            "rolling_zscore_mean_reversion",
            [10, 10, 10, 8, 9, 10],
            {"lookback": 3, "entry_z": 1.0, "exit_z": 0.0},
        ),
    ],
)
def test_classic_strategies_run_dynamically_through_runtime(code, closes, parameters) -> None:
    run = StrategyBacktestRuntime().run(code, _classic_prices(closes), _config(), parameters=parameters)
    actions = [decision.action.value for decision in run.strategy_result.decisions]
    assert "ENTER" in actions
    assert "EXIT" in actions
    assert run.backtest_result.summary["trade_count"] == 1
    assert run.backtest_result.trades[0].meta["strategy_code"] == code


def test_runtime_source_has_no_concrete_indicator_or_strategy_preparation_branches() -> None:
    from pathlib import Path

    source = (
        Path(__file__).parents[2] / "src" / "qrp_atlas" / "backtest" / "runtime" / "strategy.py"
    ).read_text(encoding="utf-8")
    assert "system_b" not in source.lower()
    assert "ma5" not in source.lower()
    for code in (
        "time_series_momentum",
        "dual_sma_trend",
        "donchian_breakout",
        "rolling_zscore_mean_reversion",
    ):
        assert code not in source


def test_classic_strategy_next_open_execution_is_end_to_end() -> None:
    config = BacktestConfig(
        name="classic-next-open",
        entry=EntryRule(timing="next_open", price_field="open"),
        exit=ExitRule(type="hold_n_bars", bars=1, price_field="close"),
        position=PositionRule(100_000.0, 1.0, 1, False, False),
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0.0),
    )
    run = StrategyBacktestRuntime().run(
        "time_series_momentum",
        _classic_prices([10, 10, 12, 13, 9]),
        config,
        parameters={"lookback": 1, "threshold": 0.0},
    )
    trade = run.backtest_result.trades[0]
    assert (trade.entry_date, trade.entry_price) == ("2024-02-04", 13.0)
    assert (trade.exit_date, trade.exit_price) == ("2024-02-05", 9.0)
