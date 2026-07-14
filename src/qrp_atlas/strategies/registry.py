"""Versioned registry and public lookup helpers for executable strategies."""

from __future__ import annotations

from .models import StrategyInput, StrategyRunResult
from .protocol import StrategyProtocol
from .validation import validate_definition


class StrategyNotFoundError(KeyError):
    """Raised when a requested strategy code/version is not registered."""


class DuplicateStrategyError(ValueError):
    """Raised when the same strategy code and version are registered twice."""


class StrategyRegistry:
    """In-memory registry shared by Python and declarative strategy instances."""

    def __init__(self) -> None:
        self._strategies: dict[tuple[str, str], StrategyProtocol] = {}

    def register(self, strategy: StrategyProtocol) -> None:
        validate_definition(strategy.definition)
        key = (strategy.definition.code, strategy.definition.version)
        if key in self._strategies:
            raise DuplicateStrategyError(
                f"strategy already registered: {strategy.definition.code}@{strategy.definition.version}"
            )
        self._strategies[key] = strategy

    def list(self):
        """Return strategy definitions sorted stably by code then version."""

        return [
            strategy.definition
            for _, strategy in sorted(self._strategies.items(), key=lambda item: item[0])
        ]

    def get(self, code: str, version: str | None = None) -> StrategyProtocol:
        if version is not None:
            try:
                return self._strategies[(code, version)]
            except KeyError:
                raise StrategyNotFoundError(f"unknown strategy: {code}@{version}") from None

        matches = [
            strategy
            for (registered_code, _), strategy in self._strategies.items()
            if registered_code == code
        ]
        if not matches:
            raise StrategyNotFoundError(f"unknown strategy code: {code}")
        return sorted(matches, key=lambda strategy: strategy.definition.version)[-1]

    def run(
        self, code: str, strategy_input: StrategyInput, version: str | None = None
    ) -> StrategyRunResult:
        return self.get(code, version).run(strategy_input)


def _build_default_registry() -> StrategyRegistry:
    from .builtin.classic import (
        DonchianBreakoutStrategy,
        DualSmaTrendStrategy,
        RollingZscoreMeanReversionStrategy,
        TimeSeriesMomentumStrategy,
    )
    from .builtin.cross_section import (
        CrossSectionalMomentumLongOnlyStrategy,
        MultifactorLongOnlyStrategy,
    )
    from .builtin.system_b_basic import SystemBBasicStrategy
    from .builtin.event_drift import EventDriftBasicStrategy

    registry = StrategyRegistry()
    registry.register(SystemBBasicStrategy())
    registry.register(TimeSeriesMomentumStrategy())
    registry.register(DualSmaTrendStrategy())
    registry.register(DonchianBreakoutStrategy())
    registry.register(RollingZscoreMeanReversionStrategy())
    registry.register(CrossSectionalMomentumLongOnlyStrategy())
    registry.register(MultifactorLongOnlyStrategy())
    registry.register(EventDriftBasicStrategy())
    return registry


DEFAULT_REGISTRY = _build_default_registry()


def list_strategies():
    """List registered strategy definitions in stable order."""

    return DEFAULT_REGISTRY.list()


def get_strategy(code: str, version: str | None = None) -> StrategyProtocol:
    """Look up a registered strategy, defaulting to its latest lexical version."""

    return DEFAULT_REGISTRY.get(code, version)


def run_strategy(
    code: str, strategy_input: StrategyInput, version: str | None = None
) -> StrategyRunResult:
    """Run a registered strategy against already prepared data."""

    return DEFAULT_REGISTRY.run(code, strategy_input, version)
