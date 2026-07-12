from __future__ import annotations

import pandas as pd
import pytest

from qrp_atlas.contracts import AMOUNT, TICKER, TRADE_DATE
from qrp_atlas.indicators.system_b.detector import (
    SYSTEM_B_EXIT_TRIGGERED,
    SYSTEM_B_TREND_VALID,
)
from qrp_atlas.strategies import StrategyAction, StrategyInput, StrategyValidationError
from qrp_atlas.strategies.declarative import DeclarativeStrategy


def _payload() -> dict:
    return {
        "code": "custom_trend_demo",
        "name": "Custom trend demo",
        "version": "1.0.0",
        "strategy_type": "declarative",
        "required_fields": [TICKER, TRADE_DATE, AMOUNT],
        "required_indicators": [SYSTEM_B_TREND_VALID, SYSTEM_B_EXIT_TRIGGERED],
        "parameters": {"min_amount": {"type": "number", "default": 100.0, "minimum": 0}},
        "entry": {
            "all": [
                {"left": {"source_type": "indicator", "code": SYSTEM_B_TREND_VALID}, "operator": "eq", "right": {"source_type": "literal", "value": True}},
                {"any": [
                    {"left": {"source_type": "field", "code": AMOUNT}, "operator": "gte", "right": {"source_type": "parameter", "code": "min_amount"}},
                    {"not": {"left": {"source_type": "field", "code": AMOUNT}, "operator": "lt", "right": {"source_type": "literal", "value": 0}}},
                ]},
            ]
        },
        "exit": {"left": {"source_type": "indicator", "code": SYSTEM_B_EXIT_TRIGGERED}, "operator": "eq", "right": {"source_type": "literal", "value": True}},
    }


def _data() -> pd.DataFrame:
    return pd.DataFrame([
        {TICKER: "A", TRADE_DATE: "2024-01-01", AMOUNT: 150.0, SYSTEM_B_TREND_VALID: True, SYSTEM_B_EXIT_TRIGGERED: False},
        {TICKER: "A", TRADE_DATE: "2024-01-02", AMOUNT: 150.0, SYSTEM_B_TREND_VALID: False, SYSTEM_B_EXIT_TRIGGERED: False},
        {TICKER: "A", TRADE_DATE: "2024-01-03", AMOUNT: 150.0, SYSTEM_B_TREND_VALID: False, SYSTEM_B_EXIT_TRIGGERED: True},
    ])


def test_declarative_strategy_supports_safe_references_comparison_and_combinators() -> None:
    strategy = DeclarativeStrategy.from_dict(_payload())
    result = strategy.run(StrategyInput(_data()))
    assert [decision.action for decision in result.decisions] == [
        StrategyAction.ENTER, StrategyAction.HOLD, StrategyAction.EXIT
    ]
    assert strategy.spec.to_dict()["code"] == "custom_trend_demo"


@pytest.mark.parametrize(
    "parameters, message",
    [({"min_amount": "bad"}, "must be number"), ({"min_amount": -1}, "below minimum")],
)
def test_declarative_parameter_validation(parameters: dict, message: str) -> None:
    with pytest.raises(StrategyValidationError, match=message):
        DeclarativeStrategy.from_dict(_payload()).run(StrategyInput(_data(), parameters=parameters))


def test_declarative_missing_required_parameter_is_rejected() -> None:
    payload = _payload()
    payload["parameters"] = {"required_value": {"type": "integer", "required": True}}
    payload["entry"] = {"left": {"source_type": "parameter", "code": "required_value"}, "operator": "gt", "right": {"source_type": "literal", "value": 1}}
    with pytest.raises(StrategyValidationError, match="missing required parameter"):
        DeclarativeStrategy.from_dict(payload).run(StrategyInput(_data()))


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda payload: payload.update({"entry": {"left": {"source_type": "field", "code": "amount"}, "operator": "python", "right": {"source_type": "literal", "value": 1}}}), "unsupported declarative operator"),
        (lambda payload: payload["entry"]["all"][0]["left"].update({"code": "not_an_indicator"}), "undeclared indicator"),
        (lambda payload: payload["entry"]["all"][1]["any"][0]["left"].update({"source_type": "python"}), "invalid source_type"),
    ],
)
def test_declarative_rejects_invalid_operators_references_and_code_execution_paths(mutate, message: str) -> None:
    payload = _payload()
    mutate(payload)
    with pytest.raises((StrategyValidationError, ValueError), match=message):
        DeclarativeStrategy.from_dict(payload)


@pytest.mark.parametrize(
    ("operator", "amount", "expected"),
    [
        ("eq", 10, True),
        ("ne", 10, False),
        ("gt", 11, True),
        ("gte", 10, True),
        ("lt", 9, True),
        ("lte", 10, True),
    ],
)
def test_declarative_supports_every_whitelisted_comparison_operator(
    operator: str, amount: float, expected: bool
) -> None:
    payload = _payload()
    payload["entry"] = {
        "left": {"source_type": "field", "code": AMOUNT},
        "operator": operator,
        "right": {"source_type": "literal", "value": 10},
    }
    strategy = DeclarativeStrategy.from_dict(payload)
    data = _data().iloc[:1].assign(**{AMOUNT: [amount]})
    action = strategy.run(StrategyInput(data)).decisions[0].action
    assert (action is StrategyAction.ENTER) is expected


def test_declarative_rejects_known_but_undeclared_field_reference() -> None:
    payload = _payload()
    payload["entry"] = {
        "left": {"source_type": "field", "code": "close"},
        "operator": "gt",
        "right": {"source_type": "literal", "value": 1},
    }
    with pytest.raises(StrategyValidationError, match="undeclared field"):
        DeclarativeStrategy.from_dict(payload)
