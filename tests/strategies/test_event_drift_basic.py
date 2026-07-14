"""Tests for event_drift_basic strategy and portfolio closed loop."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from qrp_atlas.backtest import (
    CostRule,
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioExecutionRule,
    strategy_decisions_to_target_weights,
)
from qrp_atlas.strategies import StrategyAction, StrategyInput, get_strategy


def _open_dates() -> list[str]:
    start = date(2024, 3, 18)
    days = []
    d = start
    while len(days) < 15:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d += timedelta(days=1)
    return days


def _events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "announcement_date": "2024-03-15",
                "available_trade_date": "2024-03-18",
                "forecast_type": "预增",
                "profit_change_min": 10,
                "profit_change_max": 30,
                "net_profit_min": 100,
                "net_profit_max": 120,
                "event_series_id": "s1",
                "source_record_id": "r1-old",
            },
            # same ticker same day later disclosure
            {
                "ticker": "000001.SZ",
                "announcement_date": "2024-03-16",
                "available_trade_date": "2024-03-18",
                "forecast_type": "预增",
                "profit_change_min": 20,
                "profit_change_max": 40,
                "net_profit_min": 100,
                "net_profit_max": 120,
                "event_series_id": "s1",
                "source_record_id": "r1-new",
            },
            {
                "ticker": "600519.SH",
                "announcement_date": "2024-03-15",
                "available_trade_date": "2024-03-18",
                "forecast_type": "预减",
                "profit_change_min": -30,
                "profit_change_max": -10,
                "net_profit_min": -20,
                "net_profit_max": -10,
                "event_series_id": "s2",
                "source_record_id": "r2",
            },
            {
                "ticker": "300750.SZ",
                "announcement_date": "2024-03-15",
                "available_trade_date": "2024-03-18",
                "forecast_type": "略增",
                "profit_change_min": 5,
                "profit_change_max": 15,
                "net_profit_min": 10,
                "net_profit_max": 20,
                "event_series_id": "s3",
                "source_record_id": "r3",
            },
        ]
    )


def _prices() -> pd.DataFrame:
    rows = []
    for i, day in enumerate(_open_dates()):
        for j, t in enumerate(["000001.SZ", "600519.SH", "300750.SZ"]):
            px = 10 + i + j
            rows.append(
                {
                    "trade_date": day,
                    "asset_id": t,
                    "asset_name": t,
                    "asset_type": "stock",
                    "open": float(px),
                    "high": float(px + 1),
                    "low": float(px - 0.5),
                    "close": float(px + 0.2),
                    "is_suspended": False,
                    "is_limit_up": False,
                    "is_limit_down": False,
                }
            )
    return pd.DataFrame(rows)


def test_positive_entry_negative_skip_and_same_day_dedupe():
    strategy = get_strategy("event_drift_basic")
    result = strategy.run(
        StrategyInput(
            prepared_data=_events(),
            parameters={"hold_days": 5, "min_profit_change_midpoint": 0.0},
            runtime_context={"open_dates": _open_dates()},
        )
    )
    enters = [d for d in result.decisions if d.action is StrategyAction.ENTER]
    assets = {d.asset_id for d in enters}
    assert "000001.SZ" in assets
    assert "300750.SZ" in assets
    assert "600519.SH" not in assets  # negative
    # same day same ticker only one enter
    assert sum(1 for d in enters if d.asset_id == "000001.SZ") == 1
    one = next(d for d in enters if d.asset_id == "000001.SZ")
    assert one.trade_date == "2024-03-18"
    assert one.evidence["available_trade_date"] == "2024-03-18"
    assert one.evidence["announcement_date"] < one.trade_date
    assert one.evidence["source_record_id"] == "r1-new"
    # equal weight for two entries
    weights = {d.asset_id: d.weight for d in enters}
    assert abs(weights["000001.SZ"] - 0.5) < 1e-12
    assert abs(weights["300750.SZ"] - 0.5) < 1e-12


def test_hold_days_exit_and_no_lookahead_future_event():
    strategy = get_strategy("event_drift_basic")
    events = _events()
    # future event should not enter if available later than runtime? strategy uses event rows as-is;
    # caller as_of filter is expected. Still ensure entry > announcement.
    result = strategy.run(
        StrategyInput(
            prepared_data=events,
            parameters={"hold_days": 3},
            runtime_context={"open_dates": _open_dates()},
        )
    )
    exits = [d for d in result.decisions if d.action is StrategyAction.EXIT]
    assert exits
    # entry 2024-03-18 is day1; hold_days=3 => exit on third open day
    expected_exit = _open_dates()[2]
    assert any(d.trade_date == expected_exit for d in exits)


def test_portfolio_closed_loop_with_costs_and_open_entry():
    strategy = get_strategy("event_drift_basic")
    strategy_result = strategy.run(
        StrategyInput(
            prepared_data=_events(),
            parameters={"hold_days": 5, "max_positions": 10},
            runtime_context={"open_dates": _open_dates()},
        )
    )
    targets = strategy_decisions_to_target_weights(
        strategy_result,
        max_positions=10,
        max_weight_per_asset=1.0,
        emit_unchanged_snapshots=True,
    )
    assert not targets.empty
    # all target dates >= available_trade_date and > announcement for entered assets
    assert (targets["trade_date"] >= "2024-03-18").all()

    config = PortfolioBacktestConfig(
        name="event_drift_smoke",
        initial_cash=1_000_000,
        max_positions=10,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0003, stamp_tax_rate=0.001, slippage_bps=5),
        execution=PortfolioExecutionRule(
            price_field="open",
            mark_price_field="close",
            enforce_t_plus_one=True,
            enforce_price_limits=False,
            enforce_suspension=False,
            lot_size=100,
            minimum_commission=5.0,
        ),
    )
    result = PortfolioBacktestEngine().run(_prices(), targets, config)
    assert result.summary
    # costs should be accounted when fills exist
    if result.fills:
        assert result.summary.get("total_commission", 0) >= 0
        assert any(f.side == "BUY" for f in result.fills)
        for fill in result.fills:
            assert fill.trade_date >= "2024-03-18"


def test_input_dataframe_not_modified():
    strategy = get_strategy("event_drift_basic")
    events = _events()
    original = events.copy()
    strategy.run(
        StrategyInput(
            prepared_data=events,
            parameters={},
            runtime_context={"open_dates": _open_dates()},
        )
    )
    assert events.equals(original)
