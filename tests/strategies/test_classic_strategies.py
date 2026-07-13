from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.strategies import (
    StrategyAction,
    StrategyInput,
    StrategyValidationError,
    get_strategy,
    run_strategy,
)


@pytest.mark.parametrize(
    "code,parameters,indicator_rows,expected",
    [
        (
            "time_series_momentum",
            {"lookback": 2, "threshold": 0.1},
            [{"momentum": math.nan}, {"momentum": 0.2}, {"momentum": 0.3}, {"momentum": -0.1}],
            [StrategyAction.NO_ACTION, StrategyAction.ENTER, StrategyAction.HOLD, StrategyAction.EXIT],
        ),
        (
            "dual_sma_trend",
            {"fast_window": 2, "slow_window": 3},
            [
                {"fast_sma": 1.0, "slow_sma": math.nan},
                {"fast_sma": 3.0, "slow_sma": 2.0},
                {"fast_sma": 4.0, "slow_sma": 3.0},
                {"fast_sma": 2.0, "slow_sma": 3.0},
            ],
            [StrategyAction.NO_ACTION, StrategyAction.ENTER, StrategyAction.HOLD, StrategyAction.EXIT],
        ),
        (
            "donchian_breakout",
            {"entry_window": 2, "exit_window": 2},
            [
                {"close": 10.0, "entry_channel": math.nan, "exit_channel": math.nan},
                {"close": 12.0, "entry_channel": 11.0, "exit_channel": 9.0},
                {"close": 13.0, "entry_channel": 12.0, "exit_channel": 10.0},
                {"close": 8.0, "entry_channel": 13.0, "exit_channel": 9.0},
            ],
            [StrategyAction.NO_ACTION, StrategyAction.ENTER, StrategyAction.HOLD, StrategyAction.EXIT],
        ),
        (
            "rolling_zscore_mean_reversion",
            {"lookback": 3, "entry_z": 1.0, "exit_z": 0.0},
            [{"zscore": math.nan}, {"zscore": -1.2}, {"zscore": -1.5}, {"zscore": 0.1}],
            [StrategyAction.NO_ACTION, StrategyAction.ENTER, StrategyAction.HOLD, StrategyAction.EXIT],
        ),
    ],
)
def test_classic_strategy_entry_hold_exit_warmup_and_no_duplicates(
    code, parameters, indicator_rows, expected
) -> None:
    frame = pd.DataFrame(indicator_rows).assign(
        ticker="A", trade_date=["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
    )
    result = run_strategy(code, StrategyInput(frame, parameters=parameters))
    assert [decision.action for decision in result.decisions] == expected
    assert result.decisions[0].reason_code == "INDICATOR_WARMUP"
    assert sum(decision.action is StrategyAction.ENTER for decision in result.decisions) == 1
    assert sum(decision.action is StrategyAction.EXIT for decision in result.decisions) == 1


def test_classic_strategy_multi_ticker_and_input_order_are_stable() -> None:
    rows = []
    for ticker, values in (("B", [math.nan, -0.5, 0.5]), ("A", [math.nan, 0.5, -0.5])):
        for day, value in enumerate(values, 1):
            rows.append({"ticker": ticker, "trade_date": f"2024-01-0{day}", "momentum": value})
    shuffled = pd.DataFrame(rows).sample(frac=1, random_state=11)
    result = run_strategy(
        "time_series_momentum",
        StrategyInput(shuffled, parameters={"lookback": 1, "threshold": 0.0}),
    )
    assert [(item.asset_id, item.trade_date) for item in result.decisions] == sorted(
        (item.asset_id, item.trade_date) for item in result.decisions
    )
    by_asset = {ticker: [item.action for item in result.decisions if item.asset_id == ticker] for ticker in ("A", "B")}
    assert by_asset["A"] == [StrategyAction.NO_ACTION, StrategyAction.ENTER, StrategyAction.EXIT]
    assert by_asset["B"] == [StrategyAction.NO_ACTION, StrategyAction.NO_ACTION, StrategyAction.ENTER]


@pytest.mark.parametrize(
    "code,parameters,message",
    [
        ("time_series_momentum", {"lookback": 0}, "below minimum"),
        ("dual_sma_trend", {"fast_window": 4, "slow_window": 3}, "less than"),
        ("donchian_breakout", {"entry_window": 1}, "below minimum"),
        ("rolling_zscore_mean_reversion", {"entry_z": 1.0, "exit_z": 1.0}, "less than"),
    ],
)
def test_classic_strategy_parameters_are_rejected(code, parameters, message) -> None:
    definition = get_strategy(code).definition
    columns = {"ticker": ["A"], "trade_date": ["2024-01-01"]}
    for request in definition.indicator_requests:
        columns[request.alias] = [math.nan]
    if code == "donchian_breakout":
        columns["close"] = [10.0]
    with pytest.raises(StrategyValidationError, match=message):
        run_strategy(code, StrategyInput(pd.DataFrame(columns), parameters=parameters))


def test_no_trade_when_entry_condition_is_not_met() -> None:
    frame = pd.DataFrame(
        {"ticker": ["A", "A"], "trade_date": ["2024-01-01", "2024-01-02"], "momentum": [math.nan, -0.1]}
    )
    result = run_strategy(
        "time_series_momentum", StrategyInput(frame, parameters={"lookback": 1, "threshold": 0.0})
    )
    assert all(item.action is StrategyAction.NO_ACTION for item in result.decisions)
