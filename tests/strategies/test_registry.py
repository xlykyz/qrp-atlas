from __future__ import annotations

import pytest

from qrp_atlas.contracts import TICKER, TRADE_DATE
from qrp_atlas.strategies import (
    DuplicateStrategyError,
    StrategyDefinition,
    StrategyInput,
    StrategyRegistry,
    StrategyRunResult,
    StrategyType,
    get_strategy,
    list_strategies,
)
from qrp_atlas.strategies.registry import StrategyNotFoundError


class StubStrategy:
    def __init__(self, code: str, version: str) -> None:
        self.definition = StrategyDefinition(
            code=code,
            name=code,
            version=version,
            description="stub",
            strategy_type=StrategyType.BUILTIN,
            required_fields=(TICKER, TRADE_DATE),
            required_indicators=(),
        )

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        return StrategyRunResult(self.definition, {}, ())


def test_definition_is_serializable_and_builtin_is_registered() -> None:
    definition = get_strategy("system_b_basic").definition
    payload = definition.to_dict()
    assert payload["code"] == "system_b_basic"
    assert payload["strategy_type"] == "builtin"
    assert [item.code for item in list_strategies()] == ["system_b_basic"]


def test_registry_rejects_duplicates_and_unknown_codes() -> None:
    registry = StrategyRegistry()
    registry.register(StubStrategy("alpha", "1.0.0"))
    with pytest.raises(DuplicateStrategyError):
        registry.register(StubStrategy("alpha", "1.0.0"))
    with pytest.raises(StrategyNotFoundError):
        registry.get("missing")
    with pytest.raises(StrategyNotFoundError):
        registry.get("alpha", "9.9.9")


def test_registry_version_lookup_and_stable_sorting() -> None:
    registry = StrategyRegistry()
    registry.register(StubStrategy("zeta", "1.0.0"))
    registry.register(StubStrategy("alpha", "1.0.0"))
    registry.register(StubStrategy("alpha", "2.0.0"))
    assert [(item.code, item.version) for item in registry.list()] == [
        ("alpha", "1.0.0"),
        ("alpha", "2.0.0"),
        ("zeta", "1.0.0"),
    ]
    assert registry.get("alpha").definition.version == "2.0.0"
    assert registry.get("alpha", "1.0.0").definition.version == "1.0.0"
