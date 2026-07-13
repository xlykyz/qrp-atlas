import pytest

from qrp_atlas.backtest.portfolio import strategy_decisions_to_target_weights
from qrp_atlas.strategies import (
    StrategyAction,
    StrategyDecision,
    StrategyDefinition,
    StrategyRunResult,
    StrategyType,
)


def _decision(
    trade_date: str,
    asset_id: str,
    action: StrategyAction,
    *,
    score: float | None = None,
    weight: float | None = None,
    direction: str = "long",
) -> StrategyDecision:
    return StrategyDecision(
        trade_date=trade_date,
        asset_id=asset_id,
        action=action,
        direction=direction,
        strategy_code="test_strategy",
        strategy_version="1.0.0",
        reason_code="test",
        score=score,
        weight=weight,
    )


def _result(*decisions: StrategyDecision) -> StrategyRunResult:
    definition = StrategyDefinition(
        code="test_strategy",
        name="Test Strategy",
        version="1.0.0",
        description="portfolio adapter fixture",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(),
        required_indicators=(),
    )
    return StrategyRunResult(
        definition=definition,
        parameters={},
        decisions=tuple(decisions),
    )


def test_enter_hold_exit_becomes_full_target_snapshots():
    strategy_result = _result(
        _decision("2024-01-02", "A", StrategyAction.ENTER),
        _decision("2024-01-03", "A", StrategyAction.HOLD),
        _decision("2024-01-04", "A", StrategyAction.EXIT),
    )

    targets = strategy_decisions_to_target_weights(
        strategy_result,
        max_positions=1,
        max_weight_per_asset=1.0,
    )

    assert targets.to_dict("records") == [
        {
            "trade_date": "2024-01-02",
            "asset_id": "A",
            "target_weight": 1.0,
            "priority": 0.0,
        },
        {
            "trade_date": "2024-01-04",
            "asset_id": "A",
            "target_weight": 0.0,
            "priority": 0.0,
        },
    ]


def test_score_ranking_displaces_previous_selection_with_zero_target():
    strategy_result = _result(
        _decision("2024-01-02", "A", StrategyAction.ENTER, score=1.0),
        _decision("2024-01-02", "B", StrategyAction.ENTER, score=2.0),
        _decision("2024-01-03", "A", StrategyAction.HOLD, score=3.0),
    )

    targets = strategy_decisions_to_target_weights(
        strategy_result,
        max_positions=1,
        max_weight_per_asset=1.0,
    )

    day_one = targets[targets["trade_date"] == "2024-01-02"]
    day_two = targets[targets["trade_date"] == "2024-01-03"]
    assert day_one[["asset_id", "target_weight"]].to_dict("records") == [
        {"asset_id": "B", "target_weight": 1.0},
    ]
    assert day_two[["asset_id", "target_weight"]].to_dict("records") == [
        {"asset_id": "A", "target_weight": 1.0},
        {"asset_id": "B", "target_weight": 0.0},
    ]


def test_explicit_weights_are_capped_then_scaled_to_one():
    strategy_result = _result(
        _decision("2024-01-02", "A", StrategyAction.ENTER, weight=0.8),
        _decision("2024-01-02", "B", StrategyAction.ENTER, weight=0.8),
    )

    targets = strategy_decisions_to_target_weights(
        strategy_result,
        max_positions=2,
        max_weight_per_asset=0.7,
    )

    assert targets["target_weight"].tolist() == pytest.approx([0.5, 0.5])


def test_non_long_decision_is_rejected():
    strategy_result = _result(
        _decision(
            "2024-01-02",
            "A",
            StrategyAction.ENTER,
            direction="short",
        )
    )

    with pytest.raises(ValueError, match="long decisions only"):
        strategy_decisions_to_target_weights(
            strategy_result,
            max_positions=1,
            max_weight_per_asset=1.0,
        )
