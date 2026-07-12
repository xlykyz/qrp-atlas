"""Serializable domain models for QRP trading strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

import pandas as pd


class StrategyType(str, Enum):
    """The supported origins of a strategy definition."""

    BUILTIN = "builtin"
    DECLARATIVE = "declarative"


class StrategyAction(str, Enum):
    """Actions a strategy can request from a trading runtime."""

    ENTER = "ENTER"
    HOLD = "HOLD"
    EXIT = "EXIT"
    NO_ACTION = "NO_ACTION"


@dataclass(frozen=True)
class ParameterSpec:
    """A serializable parameter contract with optional bounds."""

    type: str
    required: bool = False
    default: Any = None
    has_default: bool = False
    minimum: float | None = None
    maximum: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "required": self.required,
            "default": self.default,
            "has_default": self.has_default,
            "minimum": self.minimum,
            "maximum": self.maximum,
        }


@dataclass(frozen=True)
class StrategyDefinition:
    """Machine-readable, versioned declaration of one strategy."""

    code: str
    name: str
    version: str
    description: str
    strategy_type: StrategyType
    required_fields: tuple[str, ...]
    required_indicators: tuple[str, ...]
    parameter_schema: Mapping[str, ParameterSpec] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "strategy_type": self.strategy_type.value,
            "required_fields": list(self.required_fields),
            "required_indicators": list(self.required_indicators),
            "parameter_schema": {
                code: spec.to_dict()
                for code, spec in sorted(self.parameter_schema.items())
            },
        }


@dataclass(frozen=True)
class StrategyInput:
    """Prepared, database-free input for one deterministic strategy run."""

    prepared_data: pd.DataFrame
    parameters: Mapping[str, Any] = field(default_factory=dict)
    initial_positions: Mapping[str, bool] = field(default_factory=dict)
    runtime_context: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyDecision:
    """One strategy decision; execution results deliberately do not belong here."""

    trade_date: str
    asset_id: str
    action: StrategyAction
    direction: str
    strategy_code: str
    strategy_version: str
    reason_code: str
    score: float | None = None
    weight: float | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "asset_id": self.asset_id,
            "action": self.action.value,
            "direction": self.direction,
            "strategy_code": self.strategy_code,
            "strategy_version": self.strategy_version,
            "reason_code": self.reason_code,
            "score": self.score,
            "weight": self.weight,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True)
class StrategyRunResult:
    """The complete deterministic output of a strategy invocation."""

    definition: StrategyDefinition
    parameters: Mapping[str, Any]
    decisions: tuple[StrategyDecision, ...]
    diagnostics: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "definition": self.definition.to_dict(),
            "parameters": dict(self.parameters),
            "decisions": [decision.to_dict() for decision in self.decisions],
            "diagnostics": list(self.diagnostics),
        }
