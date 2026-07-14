from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.strategies import (
    StrategyAction,
    StrategyInput,
    StrategyValidationError,
    get_strategy,
    list_strategies,
    run_strategy,
)


CASES = [
    (
        "dual_ema_trend",
        {"fast_window": 2, "slow_window": 3},
        [
            {"fast_ema": math.nan, "slow_ema": math.nan},
            {"fast_ema": 2.0, "slow_ema": 1.0},
            {"fast_ema": 3.0, "slow_ema": 2.0},
            {"fast_ema": 1.0, "slow_ema": 2.0},
        ],
    ),
    (
        "macd_trend",
        {"fast_window": 2, "slow_window": 3, "signal_window": 2},
        [
            {"macd_line": math.nan, "macd_signal": math.nan, "macd_histogram": math.nan},
            {"macd_line": 1.0, "macd_signal": 0.0, "macd_histogram": 1.0},
            {"macd_line": 2.0, "macd_signal": 1.0, "macd_histogram": 1.0},
            {"macd_line": 0.0, "macd_signal": 1.0, "macd_histogram": -1.0},
        ],
    ),
    (
        "rsi_mean_reversion",
        {"window": 2, "entry_rsi": 30.0, "exit_rsi": 50.0},
        [{"rsi": math.nan}, {"rsi": 25.0}, {"rsi": 35.0}, {"rsi": 55.0}],
    ),
    (
        "bollinger_mean_reversion",
        {"window": 2, "multiplier": 2.0},
        [
            {"close": 10.0, "bb_middle": math.nan, "bb_upper": math.nan, "bb_lower": math.nan, "bb_bandwidth": math.nan, "bb_percent_b": math.nan},
            {"close": 8.0, "bb_middle": 10.0, "bb_upper": 11.0, "bb_lower": 9.0, "bb_bandwidth": 0.2, "bb_percent_b": -0.5},
            {"close": 9.0, "bb_middle": 10.0, "bb_upper": 12.0, "bb_lower": 8.0, "bb_bandwidth": 0.4, "bb_percent_b": 0.25},
            {"close": 11.0, "bb_middle": 10.0, "bb_upper": 12.0, "bb_lower": 8.0, "bb_bandwidth": 0.4, "bb_percent_b": 0.75},
        ],
    ),
    (
        "stochastic_mean_reversion",
        {"window": 2, "d_window": 1, "entry_level": 20.0, "exit_level": 50.0},
        [
            {"stochastic_percent_k": math.nan, "stochastic_percent_d": math.nan},
            {"stochastic_percent_k": 10.0, "stochastic_percent_d": 15.0},
            {"stochastic_percent_k": 30.0, "stochastic_percent_d": 25.0},
            {"stochastic_percent_k": 60.0, "stochastic_percent_d": 55.0},
        ],
    ),
    (
        "adx_directional_trend",
        {"window": 2, "entry_adx": 25.0, "exit_adx": 20.0},
        [
            {"direction_adx": math.nan, "direction_plus_di": math.nan, "direction_minus_di": math.nan},
            {"direction_adx": 30.0, "direction_plus_di": 25.0, "direction_minus_di": 10.0},
            {"direction_adx": 28.0, "direction_plus_di": 20.0, "direction_minus_di": 15.0},
            {"direction_adx": 15.0, "direction_plus_di": 10.0, "direction_minus_di": 20.0},
        ],
    ),
    (
        "keltner_breakout",
        {"ema_window": 2, "atr_window": 2, "multiplier": 1.0},
        [
            {"close": 10.0, "keltner_middle": math.nan, "keltner_upper": math.nan, "keltner_lower": math.nan, "keltner_atr": math.nan},
            {"close": 13.0, "keltner_middle": 10.0, "keltner_upper": 12.0, "keltner_lower": 8.0, "keltner_atr": 2.0},
            {"close": 12.0, "keltner_middle": 11.0, "keltner_upper": 13.0, "keltner_lower": 9.0, "keltner_atr": 2.0},
            {"close": 9.0, "keltner_middle": 10.0, "keltner_upper": 12.0, "keltner_lower": 8.0, "keltner_atr": 2.0},
        ],
    ),
    (
        "atr_volatility_breakout",
        {"window": 2, "multiplier": 1.0},
        [
            {"close": 10.0, "atr_breakout_upper": math.nan, "atr_breakout_lower": math.nan, "atr_breakout_atr": math.nan},
            {"close": 13.0, "atr_breakout_upper": 12.0, "atr_breakout_lower": 8.0, "atr_breakout_atr": 2.0},
            {"close": 12.0, "atr_breakout_upper": 14.0, "atr_breakout_lower": 10.0, "atr_breakout_atr": 2.0},
            {"close": 8.0, "atr_breakout_upper": 14.0, "atr_breakout_lower": 10.0, "atr_breakout_atr": 2.0},
        ],
    ),
    (
        "linear_regression_trend",
        {"window": 3, "entry_slope": 0.01, "exit_slope": 0.0, "entry_r_squared": 0.5, "exit_r_squared": 0.2},
        [
            {"regression_slope": math.nan, "regression_normalized_slope": math.nan, "regression_r_squared": math.nan},
            {"regression_slope": 2.0, "regression_normalized_slope": 0.02, "regression_r_squared": 0.8},
            {"regression_slope": 1.0, "regression_normalized_slope": 0.015, "regression_r_squared": 0.7},
            {"regression_slope": -1.0, "regression_normalized_slope": -0.01, "regression_r_squared": 0.7},
        ],
    ),
    (
        "volatility_adjusted_momentum",
        {"lookback": 2, "volatility_window": 2, "annualization": 1.0, "entry_score": 1.0, "exit_score": 0.0},
        [
            {"momentum": math.nan, "volatility": math.nan},
            {"momentum": 0.2, "volatility": 0.1},
            {"momentum": 0.15, "volatility": 0.1},
            {"momentum": -0.1, "volatility": 0.1},
        ],
    ),
    (
        "volume_confirmed_ema_trend",
        {"fast_window": 2, "slow_window": 3, "volume_window": 2, "min_relative_volume": 1.0},
        [
            {"fast_ema": math.nan, "slow_ema": math.nan, "relative_volume": math.nan},
            {"fast_ema": 2.0, "slow_ema": 1.0, "relative_volume": 1.2},
            {"fast_ema": 3.0, "slow_ema": 2.0, "relative_volume": 0.5},
            {"fast_ema": 1.0, "slow_ema": 2.0, "relative_volume": 0.5},
        ],
    ),
]


@pytest.mark.parametrize("code,parameters,rows", CASES)
def test_technical_strategies_have_deterministic_warmup_enter_hold_exit(code, parameters, rows) -> None:
    frame = pd.DataFrame(rows).assign(
        ticker="A",
        trade_date=["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
    )
    result = run_strategy(code, StrategyInput(frame, parameters=parameters))
    assert [decision.action for decision in result.decisions] == [
        StrategyAction.NO_ACTION,
        StrategyAction.ENTER,
        StrategyAction.HOLD,
        StrategyAction.EXIT,
    ]
    assert result.decisions[0].reason_code == "INDICATOR_WARMUP"


def test_technical_strategies_are_registered_and_declare_indicator_requests() -> None:
    codes = {definition.code for definition in list_strategies()}
    expected = {case[0] for case in CASES}
    assert expected <= codes
    for code in expected:
        definition = get_strategy(code).definition
        assert definition.indicator_requests
        assert definition.required_indicators == ()


@pytest.mark.parametrize(
    "code,parameters,message",
    [
        ("dual_ema_trend", {"fast_window": 3, "slow_window": 3}, "less than"),
        ("macd_trend", {"fast_window": 3, "slow_window": 3}, "less than"),
        ("rsi_mean_reversion", {"entry_rsi": 50.0, "exit_rsi": 50.0}, "less than"),
        ("stochastic_mean_reversion", {"entry_level": 50.0, "exit_level": 20.0}, "less than"),
        ("adx_directional_trend", {"entry_adx": 20.0, "exit_adx": 25.0}, "less than or equal"),
        ("linear_regression_trend", {"entry_slope": 0.0, "exit_slope": 0.1}, "less than or equal"),
        ("volatility_adjusted_momentum", {"entry_score": 0.0, "exit_score": 0.0}, "less than"),
        ("volume_confirmed_ema_trend", {"fast_window": 3, "slow_window": 3}, "less than"),
        ("bollinger_mean_reversion", {"multiplier": 0.0}, "below minimum"),
        ("keltner_breakout", {"multiplier": 0.0}, "below minimum"),
        ("atr_volatility_breakout", {"multiplier": 0.0}, "below minimum"),
    ],
)
def test_technical_strategy_parameter_relationships_are_explicit(code, parameters, message) -> None:
    definition = get_strategy(code).definition
    columns = {"ticker": ["A"], "trade_date": ["2024-01-01"], "close": [10.0]}
    for request in definition.indicator_requests:
        if request.output_fields:
            names = request.output_fields.values()
        else:
            from qrp_atlas.indicators import get_calculation_definition

            outputs = get_calculation_definition(request.code).outputs
            names = [request.alias] if len(outputs) == 1 else [f"{request.alias}_{name}" for name in outputs]
        for name in names:
            columns[name] = [math.nan]
    with pytest.raises(StrategyValidationError, match=message):
        run_strategy(code, StrategyInput(pd.DataFrame(columns), parameters=parameters))


def test_volume_confirmation_does_not_require_volume_to_exit_an_existing_position() -> None:
    frame = pd.DataFrame(
        {
            "ticker": ["A"],
            "trade_date": ["2024-01-01"],
            "fast_ema": [1.0],
            "slow_ema": [2.0],
            "relative_volume": [math.nan],
        }
    )
    result = run_strategy(
        "volume_confirmed_ema_trend",
        StrategyInput(frame, initial_positions={"A": True}, parameters={"fast_window": 2, "slow_window": 3, "volume_window": 2}),
    )
    assert result.decisions[0].action is StrategyAction.EXIT
