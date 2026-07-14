"""Safe evaluator for the intentionally small declarative strategy condition tree."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from qrp_atlas.contracts import TICKER, TRADE_DATE

from ..models import (
    StrategyAction,
    StrategyDecision,
    StrategyInput,
    StrategyRunResult,
)
from ..validation import (
    StrategyValidationError,
    resolve_parameters,
    validate_definition,
    validate_strategy_input,
)
from .models import Comparison, Condition, DeclarativeStrategySpec, InputReference, SourceType

_ALLOWED_COMPARISONS = frozenset({"eq", "ne", "gt", "gte", "lt", "lte"})


class DeclarativeStrategy:
    """Execute a validated JSON-shaped strategy without eval or arbitrary code."""

    def __init__(self, spec: DeclarativeStrategySpec) -> None:
        self.spec = spec
        self.definition = spec.definition
        validate_definition(self.definition)
        self._validate_condition(spec.entry)
        self._validate_condition(spec.exit)
        if spec.hold is not None:
            self._validate_condition(spec.hold)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "DeclarativeStrategy":
        return cls(DeclarativeStrategySpec.from_dict(payload))

    def _validate_condition(self, condition: Condition) -> None:
        if isinstance(condition, Comparison):
            if condition.operator not in _ALLOWED_COMPARISONS:
                raise StrategyValidationError(
                    f"unsupported declarative operator: {condition.operator!r}"
                )
            self._validate_reference(condition.left)
            self._validate_reference(condition.right)
            return
        operator, children = condition
        if operator not in {"all", "any", "not"}:
            raise StrategyValidationError(f"unsupported condition operator: {operator!r}")
        if operator == "not" and len(children) != 1:
            raise StrategyValidationError("not requires exactly one condition")
        if operator in {"all", "any"} and not children:
            raise StrategyValidationError(f"{operator} requires at least one condition")
        for child in children:
            self._validate_condition(child)

    def _validate_reference(self, reference: InputReference) -> None:
        if reference.source_type is SourceType.FIELD:
            if reference.code not in self.definition.required_fields:
                raise StrategyValidationError(
                    f"undeclared field reference: {reference.code!r}"
                )
        elif reference.source_type is SourceType.INDICATOR:
            if reference.code not in self.definition.required_indicators:
                raise StrategyValidationError(
                    f"undeclared indicator reference: {reference.code!r}"
                )
        elif reference.source_type is SourceType.PARAMETER:
            if reference.code not in self.definition.parameter_schema:
                raise StrategyValidationError(
                    f"unknown parameter reference: {reference.code!r}"
                )

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        prepared = validate_strategy_input(self.definition, strategy_input)
        parameters = resolve_parameters(self.definition, strategy_input.parameters)
        positions = dict(strategy_input.initial_positions)
        decisions: list[StrategyDecision] = []

        for row in prepared.to_dict(orient="records"):
            asset_id = str(row[TICKER])
            trade_date = str(row[TRADE_DATE])
            held = positions.get(asset_id, False)
            entry_match = self._evaluate(self.spec.entry, row, parameters)
            exit_match = self._evaluate(self.spec.exit, row, parameters)
            if held and exit_match:
                action, reason = StrategyAction.EXIT, "DECLARATIVE_EXIT_MATCH"
                positions[asset_id] = False
            elif held:
                if self.spec.hold is not None and not self._evaluate(self.spec.hold, row, parameters):
                    action, reason = StrategyAction.EXIT, "DECLARATIVE_HOLD_NOT_MET"
                    positions[asset_id] = False
                else:
                    action, reason = StrategyAction.HOLD, "DECLARATIVE_POSITION_CONTINUES"
            elif entry_match:
                action, reason = StrategyAction.ENTER, "DECLARATIVE_ENTRY_MATCH"
                positions[asset_id] = True
            else:
                action, reason = StrategyAction.NO_ACTION, "DECLARATIVE_ENTRY_NOT_MET"
            evidence = {
                code: row[code]
                for code in (*self.definition.required_fields, *self.definition.required_indicators)
            }
            decisions.append(
                StrategyDecision(
                    trade_date=trade_date,
                    asset_id=asset_id,
                    action=action,
                    direction="long",
                    strategy_code=self.definition.code,
                    strategy_version=self.definition.version,
                    reason_code=reason,
                    evidence=evidence,
                )
            )
        return StrategyRunResult(
            definition=self.definition,
            parameters=parameters,
            decisions=tuple(decisions),
        )

    def _evaluate(self, condition: Condition, row: Mapping[str, Any], parameters: Mapping[str, Any]) -> bool:
        if isinstance(condition, Comparison):
            left = self._resolve_reference(condition.left, row, parameters)
            right = self._resolve_reference(condition.right, row, parameters)
            return {
                "eq": lambda: left == right,
                "ne": lambda: left != right,
                "gt": lambda: left > right,
                "gte": lambda: left >= right,
                "lt": lambda: left < right,
                "lte": lambda: left <= right,
            }[condition.operator]()
        operator, children = condition
        if operator == "all":
            return all(self._evaluate(child, row, parameters) for child in children)
        if operator == "any":
            return any(self._evaluate(child, row, parameters) for child in children)
        return not self._evaluate(children[0], row, parameters)

    @staticmethod
    def _resolve_reference(
        reference: InputReference, row: Mapping[str, Any], parameters: Mapping[str, Any]
    ) -> Any:
        if reference.source_type is SourceType.LITERAL:
            return reference.value
        if reference.source_type is SourceType.PARAMETER:
            return parameters[reference.code or ""]
        return row[reference.code or ""]
