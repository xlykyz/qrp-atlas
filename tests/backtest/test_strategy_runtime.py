from __future__ import annotations

import pandas as pd

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
