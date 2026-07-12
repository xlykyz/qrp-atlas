from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import TICKER, TRADE_DATE
from qrp_atlas.indicators.system_b.detector import (
    SYSTEM_B_EXIT_TRIGGERED,
    SYSTEM_B_TREND_VALID,
)
from qrp_atlas.strategies import (
    StrategyAction,
    StrategyInput,
    StrategyValidationError,
    run_strategy,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {TICKER: "A", TRADE_DATE: "2024-01-04", SYSTEM_B_TREND_VALID: False, SYSTEM_B_EXIT_TRIGGERED: True},
            {TICKER: "A", TRADE_DATE: "2024-01-02", SYSTEM_B_TREND_VALID: True, SYSTEM_B_EXIT_TRIGGERED: False},
            {TICKER: "B", TRADE_DATE: "2024-01-02", SYSTEM_B_TREND_VALID: False, SYSTEM_B_EXIT_TRIGGERED: False},
            {TICKER: "A", TRADE_DATE: "2024-01-01", SYSTEM_B_TREND_VALID: False, SYSTEM_B_EXIT_TRIGGERED: False},
            {TICKER: "A", TRADE_DATE: "2024-01-03", SYSTEM_B_TREND_VALID: False, SYSTEM_B_EXIT_TRIGGERED: False},
            {TICKER: "B", TRADE_DATE: "2024-01-03", SYSTEM_B_TREND_VALID: True, SYSTEM_B_EXIT_TRIGGERED: False},
        ]
    )


def test_system_b_generates_all_state_actions_and_stable_evidence() -> None:
    result = run_strategy("system_b_basic", StrategyInput(_frame()))
    by_asset = {decision.asset_id: [] for decision in result.decisions}
    for decision in result.decisions:
        by_asset[decision.asset_id].append(decision)

    assert [item.action for item in by_asset["A"]] == [
        StrategyAction.NO_ACTION,
        StrategyAction.ENTER,
        StrategyAction.HOLD,
        StrategyAction.EXIT,
    ]
    assert [item.reason_code for item in by_asset["A"]] == [
        "ENTRY_CONDITION_NOT_MET", "TREND_CONFIRMED", "POSITION_CONTINUES", "EXIT_TRIGGERED"
    ]
    assert [item.action for item in by_asset["B"]] == [
        StrategyAction.NO_ACTION,
        StrategyAction.ENTER,
    ]
    enter = by_asset["A"][1]
    assert enter.strategy_code == "system_b_basic"
    assert enter.strategy_version == "1.0.0"
    assert enter.evidence == {
        SYSTEM_B_TREND_VALID: True,
        SYSTEM_B_EXIT_TRIGGERED: False,
    }
    assert result.to_dict() == run_strategy("system_b_basic", StrategyInput(_frame())).to_dict()


def test_system_b_supports_initial_positions_and_empty_input() -> None:
    result = run_strategy(
        "system_b_basic",
        StrategyInput(_frame().iloc[:1], initial_positions={"A": True}),
    )
    assert result.decisions[0].action is StrategyAction.EXIT
    assert run_strategy("system_b_basic", StrategyInput(_frame().iloc[:0])).decisions == ()


@pytest.mark.parametrize(
    "frame, message",
    [
        (_frame().drop(columns=[SYSTEM_B_TREND_VALID]), "missing required columns"),
        (_frame().assign(**{SYSTEM_B_TREND_VALID: [np.nan] * len(_frame())}), "missing values"),
        (_frame().assign(**{SYSTEM_B_TREND_VALID: [np.inf] * len(_frame())}), "non-finite"),
        (pd.concat([_frame(), _frame().iloc[[0]]]), "duplicate"),
    ],
)
def test_system_b_rejects_invalid_prepared_inputs(frame: pd.DataFrame, message: str) -> None:
    with pytest.raises(StrategyValidationError, match=message):
        run_strategy("system_b_basic", StrategyInput(frame))


def test_system_b_strategy_consumes_indicator_outputs_without_recalculation() -> None:
    from pathlib import Path

    implementation = Path(__file__).parents[2] / "src" / "qrp_atlas" / "strategies" / "builtin" / "system_b_basic.py"
    source = implementation.read_text(encoding="utf-8")
    assert "calculate_stock_trend" not in source
    assert "calculate_system_b_basic_states" not in source
