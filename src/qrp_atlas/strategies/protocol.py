"""Protocol shared by built-in and declarative executable strategies."""

from __future__ import annotations

from typing import Protocol

from .models import StrategyDefinition, StrategyInput, StrategyRunResult


class StrategyProtocol(Protocol):
    """An executable strategy that consumes only already-prepared data."""

    definition: StrategyDefinition

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        """Run the strategy without loading data or querying a database."""
