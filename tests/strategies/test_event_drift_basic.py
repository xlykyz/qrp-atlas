"""Tests for event_drift_basic strategy and public portfolio closed loop."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from qrp_atlas.backtest import (
    CostRule,
    PortfolioBacktestConfig,
    PortfolioExecutionRule,
    run_event_drift_portfolio_backtest,
    strategy_decisions_to_target_weights,
)
from qrp_atlas.backtest.pit_queries import to_earnings_forecast_event_frame
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
                "event_type": "earnings_forecast",
                "event_series_id": "s1",
                "report_period": "2023-12-31",
                "announcement_date": "2024-03-15",
                "available_trade_date": "2024-03-18",
                "forecast_type": "预增",
                "profit_change_min": 10,
                "profit_change_max": 30,
                "net_profit_min": 100,
                "net_profit_max": 120,
                "source_record_id": "r1-old",
                "revision_id": "v1",
            },
            {
                "ticker": "000001.SZ",
                "event_type": "earnings_forecast",
                "event_series_id": "s1",
                "report_period": "2023-12-31",
                "announcement_date": "2024-03-16",
                "available_trade_date": "2024-03-18",
                "forecast_type": "预增",
                "profit_change_min": 20,
                "profit_change_max": 40,
                "net_profit_min": 100,
                "net_profit_max": 120,
                "source_record_id": "r1-new",
                "revision_id": "v2",
            },
            {
                "ticker": "600519.SH",
                "event_type": "earnings_forecast",
                "event_series_id": "s2",
                "report_period": "2023-12-31",
                "announcement_date": "2024-03-15",
                "available_trade_date": "2024-03-18",
                "forecast_type": "预减",
                "profit_change_min": -30,
                "profit_change_max": -10,
                "net_profit_min": -20,
                "net_profit_max": -10,
                "source_record_id": "r2",
                "revision_id": "v3",
            },
            {
                "ticker": "300750.SZ",
                "event_type": "earnings_forecast",
                "event_series_id": "s3",
                "report_period": "2023-12-31",
                "announcement_date": "2024-03-15",
                "available_trade_date": "2024-03-18",
                "forecast_type": "略增",
                "profit_change_min": 5,
                "profit_change_max": 15,
                "net_profit_min": 10,
                "net_profit_max": 20,
                "source_record_id": "r3",
                "revision_id": "v4",
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


def _config(**kwargs) -> PortfolioBacktestConfig:
    payload = dict(
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
    payload.update(kwargs)
    return PortfolioBacktestConfig(**payload)


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
    assert "600519.SH" not in assets
    assert sum(1 for d in enters if d.asset_id == "000001.SZ") == 1
    one = next(d for d in enters if d.asset_id == "000001.SZ")
    assert one.trade_date == "2024-03-18"
    assert one.evidence["available_trade_date"] == "2024-03-18"
    assert one.evidence["announcement_date"] < one.trade_date
    assert one.evidence["source_record_id"] == "r1-new"
    # strategy no longer owns explicit same-day equal weights
    assert one.weight is None


def test_hold_days_exit_next_open_after_window():
    strategy = get_strategy("event_drift_basic")
    result = strategy.run(
        StrategyInput(
            prepared_data=_events(),
            parameters={"hold_days": 3},
            runtime_context={"open_dates": _open_dates()},
        )
    )
    exits = [d for d in result.decisions if d.action is StrategyAction.EXIT]
    assert exits
    # entry D0 day1..day3 hold, exit next open = open_dates[3]
    expected_exit = _open_dates()[3]
    assert any(d.trade_date == expected_exit for d in exits)


def test_hold_days_one_exits_next_open():
    strategy = get_strategy("event_drift_basic")
    result = strategy.run(
        StrategyInput(
            prepared_data=_events(),
            parameters={"hold_days": 1},
            runtime_context={"open_dates": _open_dates()},
        )
    )
    enters = [d for d in result.decisions if d.action is StrategyAction.ENTER]
    exits = [d for d in result.decisions if d.action is StrategyAction.EXIT]
    assert enters
    assert exits
    # not same-day exit; next open after entry
    for enter in enters:
        matching = [e for e in exits if e.asset_id == enter.asset_id]
        assert matching
        assert matching[0].trade_date == _open_dates()[1]
        assert matching[0].trade_date > enter.trade_date


def test_public_runner_equal_weight_capacity_and_costs():
    run = run_event_drift_portfolio_backtest(
        _events(),
        _prices(),
        _config(max_positions=10, max_weight_per_asset=1.0),
        trading_days=_open_dates(),
        parameters={"hold_days": 5},
    )
    assert not run.target_weights.empty
    day = "2024-03-18"
    day_targets = run.target_weights[run.target_weights.trade_date == day]
    active = day_targets[day_targets.target_weight > 0]
    assert set(active.asset_id) == {"000001.SZ", "300750.SZ"}
    assert abs(float(active.target_weight.sum()) - 1.0) < 1e-9
    assert abs(float(active.target_weight.iloc[0]) - 0.5) < 1e-9
    assert (run.target_weights["trade_date"] >= "2024-03-18").all()
    if run.portfolio_result.fills:
        assert run.portfolio_result.summary.get("total_cost", 0) >= 0
        for fill in run.portfolio_result.fills:
            assert fill.trade_date >= "2024-03-18"


def test_public_runner_respects_max_positions():
    run = run_event_drift_portfolio_backtest(
        _events(),
        _prices(),
        _config(max_positions=1, max_weight_per_asset=1.0),
        trading_days=_open_dates(),
        parameters={"hold_days": 5},
    )
    day_targets = run.target_weights[run.target_weights.trade_date == "2024-03-18"]
    active = day_targets[day_targets.target_weight > 0]
    assert len(active) == 1
    assert abs(float(active.target_weight.iloc[0]) - 1.0) < 1e-9


def test_input_dataframe_not_modified():
    events = _events()
    original = events.copy()
    run_event_drift_portfolio_backtest(
        events,
        _prices(),
        _config(),
        trading_days=_open_dates(),
        parameters={},
    )
    assert events.equals(original)


def test_asof_query_to_eventframe_integration(tmp_path):
    """End-to-end path: 05-A as_of style frame → EventFrame → public runner."""
    import duckdb
    from qrp_atlas.contracts import init_database
    from qrp_atlas.backtest import query_earnings_forecast_as_of
    from qrp_atlas.pipeline.earnings_forecast.clean import clean_earnings_forecast
    from qrp_atlas.pipeline.earnings_forecast.load_duckdb import load_earnings_forecast
    from qrp_atlas.pipeline.pit_utils import NextTradeDateResolver
    from datetime import datetime

    db = tmp_path / "ef.duckdb"
    con = duckdb.connect(str(db))
    init_database(con)
    for d in _open_dates():
        con.execute("INSERT INTO trading_calendar (trade_date, is_open) VALUES (?, TRUE)", [d])
    con.close()

    raw = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "ann_date": "20240315",
                "end_date": "20231231",
                "type": "预增",
                "p_change_min": 10,
                "p_change_max": 20,
                "net_profit_min": 100,
                "net_profit_max": 120,
                "last_parent_net": 80,
                "first_ann_date": "20240315",
                "summary": "up",
                "change_reason": "x",
            },
            {
                "ts_code": "600519.SH",
                "ann_date": "20240315",
                "end_date": "20231231",
                "type": "预减",
                "p_change_min": -20,
                "p_change_max": -10,
                "net_profit_min": -50,
                "net_profit_max": -40,
                "last_parent_net": 10,
                "first_ann_date": "20240315",
                "summary": "down",
                "change_reason": "y",
            },
        ]
    )
    resolver = NextTradeDateResolver([date.fromisoformat(d) for d in _open_dates()])
    cleaned = clean_earnings_forecast(raw, trade_date_resolver=resolver, ingested_at=datetime(2024, 1, 1))
    load_earnings_forecast(cleaned, db_path=db, init=True)

    # announcement day: future/unavailable
    pre = query_earnings_forecast_as_of(as_of_date="2024-03-15", db_path=db, as_event_frame=True)
    assert pre.empty

    # available day
    frame = query_earnings_forecast_as_of(as_of_date="2024-03-18", db_path=db, as_event_frame=True)
    assert not frame.empty
    assert set(frame.columns) >= set(to_earnings_forecast_event_frame(frame).columns)

    run = run_event_drift_portfolio_backtest(
        frame,
        _prices(),
        _config(),
        trading_days=_open_dates(),
        parameters={"hold_days": 2},
    )
    enters = [d for d in run.strategy_result.decisions if d.action is StrategyAction.ENTER]
    assert any(d.asset_id == "000001.SZ" for d in enters)
    assert all(d.asset_id != "600519.SH" for d in enters)


def test_public_runner_reweights_full_concurrent_book():
    """New entry mid-hold should equal-weight the full active book, not only same-day entries."""
    events = pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "event_type": "earnings_forecast",
                "event_series_id": "s1",
                "report_period": "2023-12-31",
                "announcement_date": "2024-03-15",
                "available_trade_date": "2024-03-18",
                "forecast_type": "预增",
                "profit_change_min": 20,
                "profit_change_max": 40,
                "net_profit_min": 100,
                "net_profit_max": 120,
                "source_record_id": "r1",
                "revision_id": "v1",
            },
            {
                "ticker": "300750.SZ",
                "event_type": "earnings_forecast",
                "event_series_id": "s3",
                "report_period": "2023-12-31",
                "announcement_date": "2024-03-18",
                "available_trade_date": "2024-03-19",
                "forecast_type": "略增",
                "profit_change_min": 5,
                "profit_change_max": 15,
                "net_profit_min": 10,
                "net_profit_max": 20,
                "source_record_id": "r3",
                "revision_id": "v3",
            },
        ]
    )
    run = run_event_drift_portfolio_backtest(
        events,
        _prices(),
        _config(max_positions=10, max_weight_per_asset=1.0),
        trading_days=_open_dates(),
        parameters={"hold_days": 5},
    )
    day0 = run.target_weights[run.target_weights.trade_date == "2024-03-18"]
    active0 = day0[day0.target_weight > 0]
    assert set(active0.asset_id) == {"000001.SZ"}
    assert abs(float(active0.target_weight.iloc[0]) - 1.0) < 1e-9

    day1 = run.target_weights[run.target_weights.trade_date == "2024-03-19"]
    active1 = day1[day1.target_weight > 0]
    assert set(active1.asset_id) == {"000001.SZ", "300750.SZ"}
    assert abs(float(active1.target_weight.sum()) - 1.0) < 1e-9
    assert all(abs(float(w) - 0.5) < 1e-9 for w in active1.target_weight.tolist())
