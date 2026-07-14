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
    REASON_MAX_HOLD_EXIT,
    REASON_MEAN_REVERSION_EXIT,
    REASON_RELATIONSHIP_INVALID,
    REASON_RESIDUAL_EXTREME_ENTRY,
    STRATEGY_CODE,
)


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_strategy_is_registered() -> None:
    strategy = get_strategy(STRATEGY_CODE)
    assert strategy.definition.code == STRATEGY_CODE
    assert strategy.definition.version == "1.0.0"


def test_extreme_negative_zscore_enters() -> None:
    frame = _frame(
        [
            {
                "ticker": "A",
                "trade_date": "2024-01-01",
                "rolling_alpha": 0.0,
                "rolling_beta": 1.0,
                "rolling_r2": 0.5,
                "residual_return": -0.03,
                "residual_zscore": -2.5,
                "benchmark_id": "MKT",
            }
        ]
    )
    result = run_strategy(
        STRATEGY_CODE,
        StrategyInput(frame, parameters={"entry_zscore": -2.0, "exit_zscore": -0.25, "min_r2": 0.1}),
    )
    assert result.decisions[0].action is StrategyAction.ENTER
    assert result.decisions[0].reason_code == REASON_RESIDUAL_EXTREME_ENTRY
    assert result.decisions[0].evidence["residual_zscore"] == -2.5


def test_min_r2_blocks_entry() -> None:
    frame = _frame(
        [
            {
                "ticker": "A",
                "trade_date": "2024-01-01",
                "rolling_alpha": 0.0,
                "rolling_beta": 1.0,
                "rolling_r2": 0.01,
                "residual_return": -0.03,
                "residual_zscore": -3.0,
                "benchmark_id": "MKT",
            }
        ]
    )
    result = run_strategy(
        STRATEGY_CODE,
        StrategyInput(frame, parameters={"entry_zscore": -2.0, "min_r2": 0.2}),
    )
    assert result.decisions[0].action is StrategyAction.NO_ACTION


def test_mean_reversion_and_max_hold_and_invalid_exit() -> None:
    base = {
        "ticker": "A",
        "rolling_alpha": 0.0,
        "rolling_beta": 1.0,
        "rolling_r2": 0.5,
        "residual_return": -0.02,
        "benchmark_id": "MKT",
    }
    # mean reversion
    frame = _frame(
        [
            {**base, "trade_date": "2024-01-01", "residual_zscore": -2.5},
            {**base, "trade_date": "2024-01-02", "residual_zscore": -0.1},
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

    # max hold
    frame = _frame(
        [
            {**base, "trade_date": "2024-01-01", "residual_zscore": -2.5},
            {**base, "trade_date": "2024-01-02", "residual_zscore": -1.5},
            {**base, "trade_date": "2024-01-03", "residual_zscore": -1.4},
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

    # invalid indicator while held
    frame = _frame(
        [
            {**base, "trade_date": "2024-01-01", "residual_zscore": -2.5},
            {
                **base,
                "trade_date": "2024-01-02",
                "residual_zscore": math.nan,
                "rolling_r2": math.nan,
            },
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
                "max_hold_days": 10,
            },
        ),
    )
    assert result.decisions[1].action is StrategyAction.EXIT
    assert result.decisions[1].reason_code in {
        REASON_RELATIONSHIP_INVALID,
        "INVALID_INDICATOR",
        "INSUFFICIENT_HISTORY",
    }


def test_parameter_validation_and_determinism() -> None:
    frame = _frame(
        [
            {
                "ticker": "A",
                "trade_date": "2024-01-01",
                "rolling_alpha": 0.0,
                "rolling_beta": 1.0,
                "rolling_r2": 0.4,
                "residual_return": -0.02,
                "residual_zscore": -2.2,
            },
            {
                "ticker": "A",
                "trade_date": "2024-01-02",
                "rolling_alpha": 0.0,
                "rolling_beta": 1.0,
                "rolling_r2": 0.4,
                "residual_return": -0.01,
                "residual_zscore": -1.0,
            },
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
