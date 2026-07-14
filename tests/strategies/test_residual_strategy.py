"""Tests for market_residual_mean_reversion strategy."""

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
from qrp_atlas.strategies.builtin.residual import (
    REASON_ENTRY_CONDITION_NOT_MET,
    REASON_INSUFFICIENT_HISTORY,
    REASON_INVALID_INDICATOR,
    REASON_MAX_HOLD_EXIT,
    REASON_MEAN_REVERSION_EXIT,
    REASON_MISSING_BENCHMARK,
    REASON_RELATIONSHIP_INVALID,
    REASON_RESIDUAL_EXTREME_ENTRY,
    STRATEGY_CODE,
)


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _valid_row(trade_date: str, **overrides) -> dict:
    row = {
        "ticker": "A",
        "trade_date": trade_date,
        "rolling_alpha": 0.0,
        "rolling_beta": 1.0,
        "rolling_r2": 0.5,
        "residual_return": -0.03,
        "residual_zscore": -2.5,
        "benchmark_id": "MKT",
        "diagnostic_code": "OK",
    }
    row.update(overrides)
    return row


def test_strategy_is_registered() -> None:
    strategy = get_strategy(STRATEGY_CODE)
    assert strategy.definition.code == STRATEGY_CODE
    assert strategy.definition.version == "1.0.0"


def test_extreme_negative_zscore_enters() -> None:
    frame = _frame([_valid_row("2024-01-01")])
    result = run_strategy(
        STRATEGY_CODE,
        StrategyInput(frame, parameters={"entry_zscore": -2.0, "exit_zscore": -0.25, "min_r2": 0.1}),
    )
    assert result.decisions[0].action is StrategyAction.ENTER
    assert result.decisions[0].reason_code == REASON_RESIDUAL_EXTREME_ENTRY
    assert result.decisions[0].evidence["residual_zscore"] == -2.5
    assert result.decisions[0].evidence["indicator_diagnostic_code"] == "OK"


@pytest.mark.parametrize(
    "overrides,expected_reason,held",
    [
        (
            {
                "rolling_alpha": math.nan,
                "rolling_beta": math.nan,
                "rolling_r2": math.nan,
                "residual_return": math.nan,
                "residual_zscore": math.nan,
                "diagnostic_code": "INSUFFICIENT_HISTORY",
            },
            REASON_INSUFFICIENT_HISTORY,
            False,
        ),
        (
            {
                "residual_return": math.nan,
                "residual_zscore": math.nan,
                "diagnostic_code": "MISSING_BENCHMARK",
            },
            REASON_MISSING_BENCHMARK,
            False,
        ),
        (
            {
                "residual_return": math.nan,
                "residual_zscore": math.nan,
                "diagnostic_code": "MISSING_CURRENT_RETURN",
            },
            REASON_INVALID_INDICATOR,
            False,
        ),
        (
            {
                "rolling_alpha": math.nan,
                "rolling_beta": math.nan,
                "rolling_r2": math.nan,
                "residual_return": math.nan,
                "residual_zscore": math.nan,
                "diagnostic_code": "ZERO_BENCHMARK_VARIANCE",
            },
            REASON_INVALID_INDICATOR,
            False,
        ),
        (
            {
                "rolling_alpha": math.nan,
                "rolling_beta": math.nan,
                "rolling_r2": math.nan,
                "residual_return": math.nan,
                "residual_zscore": math.nan,
                "diagnostic_code": "RANK_DEFICIENT",
            },
            REASON_INVALID_INDICATOR,
            False,
        ),
        (
            {
                "rolling_r2": 0.01,
                "residual_zscore": -3.0,
                "diagnostic_code": "OK",
            },
            REASON_RELATIONSHIP_INVALID,
            False,
        ),
        (
            {
                "residual_zscore": -1.0,
                "diagnostic_code": "OK",
            },
            REASON_ENTRY_CONDITION_NOT_MET,
            False,
        ),
    ],
)
def test_flat_state_reason_codes_from_diagnostic(overrides, expected_reason, held) -> None:
    frame = _frame([_valid_row("2024-01-01", **overrides)])
    result = run_strategy(
        STRATEGY_CODE,
        StrategyInput(
            frame,
            parameters={"entry_zscore": -2.0, "exit_zscore": 0.0, "min_r2": 0.2, "max_hold_days": 10},
            initial_positions={"A": held},
        ),
    )
    decision = result.decisions[0]
    assert decision.action is StrategyAction.NO_ACTION
    assert decision.reason_code == expected_reason
    assert decision.evidence["indicator_diagnostic_code"] == overrides["diagnostic_code"]


@pytest.mark.parametrize(
    "overrides,expected_reason",
    [
        (
            {
                "rolling_alpha": math.nan,
                "rolling_beta": math.nan,
                "rolling_r2": math.nan,
                "residual_return": math.nan,
                "residual_zscore": math.nan,
                "diagnostic_code": "INSUFFICIENT_HISTORY",
            },
            REASON_INSUFFICIENT_HISTORY,
        ),
        (
            {
                "residual_return": math.nan,
                "residual_zscore": math.nan,
                "diagnostic_code": "MISSING_BENCHMARK",
            },
            REASON_MISSING_BENCHMARK,
        ),
        (
            {
                "residual_return": math.nan,
                "residual_zscore": math.nan,
                "diagnostic_code": "MISSING_CURRENT_RETURN",
            },
            REASON_INVALID_INDICATOR,
        ),
        (
            {
                "rolling_alpha": math.nan,
                "rolling_beta": math.nan,
                "rolling_r2": math.nan,
                "residual_return": math.nan,
                "residual_zscore": math.nan,
                "diagnostic_code": "ZERO_BENCHMARK_VARIANCE",
            },
            REASON_INVALID_INDICATOR,
        ),
        (
            {
                "rolling_alpha": math.nan,
                "rolling_beta": math.nan,
                "rolling_r2": math.nan,
                "residual_return": math.nan,
                "residual_zscore": math.nan,
                "diagnostic_code": "RANK_DEFICIENT",
            },
            REASON_INVALID_INDICATOR,
        ),
        (
            {
                "rolling_r2": 0.01,
                "residual_zscore": -1.5,
                "diagnostic_code": "OK",
            },
            REASON_RELATIONSHIP_INVALID,
        ),
    ],
)
def test_held_state_exits_with_diagnostic_reasons(overrides, expected_reason) -> None:
    frame = _frame(
        [
            _valid_row("2024-01-01"),
            _valid_row("2024-01-02", **overrides),
        ]
    )
    result = run_strategy(
        STRATEGY_CODE,
        StrategyInput(
            frame,
            parameters={"entry_zscore": -2.0, "exit_zscore": 0.0, "min_r2": 0.2, "max_hold_days": 10},
        ),
    )
    assert result.decisions[0].action is StrategyAction.ENTER
    assert result.decisions[1].action is StrategyAction.EXIT
    assert result.decisions[1].reason_code == expected_reason
    assert result.decisions[1].evidence["indicator_diagnostic_code"] == overrides["diagnostic_code"]


def test_mean_reversion_and_max_hold_exit() -> None:
    frame = _frame(
        [
            _valid_row("2024-01-01", residual_zscore=-2.5),
            _valid_row("2024-01-02", residual_zscore=-0.1, residual_return=-0.001),
        ]
    )
    result = run_strategy(
        STRATEGY_CODE,
        StrategyInput(
            frame,
            parameters={
                "entry_zscore": -2.0,
                "exit_zscore": -0.25,
                "min_r2": 0.1,
                "max_hold_days": 10,
            },
        ),
    )
    assert [d.action for d in result.decisions] == [
        StrategyAction.ENTER,
        StrategyAction.EXIT,
    ]
    assert result.decisions[1].reason_code == REASON_MEAN_REVERSION_EXIT

    frame = _frame(
        [
            _valid_row("2024-01-01", residual_zscore=-2.5),
            _valid_row("2024-01-02", residual_zscore=-1.5),
            _valid_row("2024-01-03", residual_zscore=-1.4),
        ]
    )
    result = run_strategy(
        STRATEGY_CODE,
        StrategyInput(
            frame,
            parameters={
                "entry_zscore": -2.0,
                "exit_zscore": 0.0,
                "min_r2": 0.1,
                "max_hold_days": 2,
            },
        ),
    )
    assert [d.action for d in result.decisions] == [
        StrategyAction.ENTER,
        StrategyAction.HOLD,
        StrategyAction.EXIT,
    ]
    assert result.decisions[2].reason_code == REASON_MAX_HOLD_EXIT


def test_parameter_validation_and_determinism() -> None:
    frame = _frame(
        [
            _valid_row("2024-01-01", residual_zscore=-2.2),
            _valid_row("2024-01-02", residual_zscore=-1.0, residual_return=-0.01),
        ]
    )
    with pytest.raises(StrategyValidationError, match="entry_zscore"):
        run_strategy(
            STRATEGY_CODE,
            StrategyInput(frame, parameters={"entry_zscore": -0.1, "exit_zscore": -0.5}),
        )
    first = run_strategy(STRATEGY_CODE, StrategyInput(frame, parameters={"entry_zscore": -2.0}))
    second = run_strategy(
        STRATEGY_CODE,
        StrategyInput(frame.sample(frac=1, random_state=2), parameters={"entry_zscore": -2.0}),
    )
    assert [d.to_dict() for d in first.decisions] == [d.to_dict() for d in second.decisions]
