"""Serializable JSON-shaped building blocks for declarative strategies."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from qrp_atlas.indicators import IndicatorRequest

from ..models import ParameterSpec, StrategyDefinition, StrategyType


class SourceType(str, Enum):
    FIELD = "field"
    INDICATOR = "indicator"
    PARAMETER = "parameter"
    LITERAL = "literal"


@dataclass(frozen=True)
class InputReference:
    """One safe reference to a prepared field, indicator, parameter, or literal."""

    source_type: SourceType
    code: str | None = None
    value: Any = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "InputReference":
        try:
            source_type = SourceType(payload["source_type"])
        except (KeyError, ValueError) as exc:
            raise ValueError("reference has invalid source_type") from exc
        if source_type is SourceType.LITERAL:
            return cls(source_type=source_type, value=payload.get("value"))
        code = payload.get("code")
        if not isinstance(code, str) or not code:
            raise ValueError("non-literal reference requires a non-empty code")
        return cls(source_type=source_type, code=code)

    def to_dict(self) -> dict[str, Any]:
        result = {"source_type": self.source_type.value}
        if self.source_type is SourceType.LITERAL:
            result["value"] = self.value
        else:
            result["code"] = self.code
        return result


@dataclass(frozen=True)
class Comparison:
    """A comparison expression with a fixed whitelist of operators."""

    left: InputReference
    operator: str
    right: InputReference

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "Comparison":
        return cls(
            left=InputReference.from_dict(payload["left"]),
            operator=str(payload["operator"]),
            right=InputReference.from_dict(payload["right"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "left": self.left.to_dict(),
            "operator": self.operator,
            "right": self.right.to_dict(),
        }


Condition = Comparison | tuple[str, tuple["Condition", ...]]


def parse_condition(payload: Mapping[str, Any]) -> Condition:
    """Parse the small, JSON-safe condition tree without executing code."""

    for operator in ("all", "any"):
        if operator in payload:
            children = payload[operator]
            if not isinstance(children, list):
                raise ValueError(f"{operator} requires a list")
            return (operator, tuple(parse_condition(child) for child in children))
    if "not" in payload:
        child = payload["not"]
        if not isinstance(child, Mapping):
            raise ValueError("not requires one condition object")
        return ("not", (parse_condition(child),))
    return Comparison.from_dict(payload)


def condition_to_dict(condition: Condition) -> dict[str, Any]:
    if isinstance(condition, Comparison):
        return condition.to_dict()
    operator, children = condition
    if operator == "not":
        return {"not": condition_to_dict(children[0])}
    return {operator: [condition_to_dict(child) for child in children]}


@dataclass(frozen=True)
class DeclarativeStrategySpec:
    """Frontend-constructible declarative strategy payload."""

    definition: StrategyDefinition
    entry: Condition
    exit: Condition
    hold: Condition | None = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "DeclarativeStrategySpec":
        parameters_payload = payload.get("parameters", payload.get("parameter_schema", {}))
        if not isinstance(parameters_payload, Mapping):
            raise ValueError("parameters must be an object")
        parameter_schema = {
            str(code): ParameterSpec(
                type=str(spec["type"]),
                required=bool(spec.get("required", False)),
                default=spec.get("default"),
                has_default="default" in spec,
                minimum=spec.get("minimum"),
                maximum=spec.get("maximum"),
            )
            for code, spec in parameters_payload.items()
            if isinstance(spec, Mapping)
        }
        if len(parameter_schema) != len(parameters_payload):
            raise ValueError("every parameter specification must be an object")
        indicator_requests_payload = payload.get("indicator_requests", ())
        if not isinstance(indicator_requests_payload, (list, tuple)):
            raise ValueError("indicator_requests must be a list")
        indicator_requests_list: list[IndicatorRequest] = []
        for item in indicator_requests_payload:
            if isinstance(item, IndicatorRequest):
                indicator_requests_list.append(item)
                continue
            if not isinstance(item, Mapping):
                raise ValueError("each indicator_request must be an object")
            code = str(item.get("code") or "").strip()
            if not code:
                raise ValueError("indicator_request.code is required")
            params = item.get("parameters") or {}
            if not isinstance(params, Mapping):
                raise ValueError("indicator_request.parameters must be an object")
            alias = item.get("alias")
            if alias is not None:
                alias = str(alias).strip() or None
            output_fields = item.get("output_fields") or {}
            if not isinstance(output_fields, Mapping):
                raise ValueError("indicator_request.output_fields must be an object")
            indicator_requests_list.append(
                IndicatorRequest(
                    code=code,
                    parameters=dict(params),
                    alias=alias,
                    output_fields={str(k): str(v) for k, v in output_fields.items()},
                )
            )
        indicator_requests = tuple(indicator_requests_list)
        aliases = [req.alias for req in indicator_requests if getattr(req, "alias", None)]
        if len(aliases) != len(set(aliases)):
            raise ValueError("indicator request aliases must be unique")
        definition = StrategyDefinition(
            code=str(payload["code"]),
            name=str(payload["name"]),
            version=str(payload["version"]),
            description=str(payload.get("description", "")),
            strategy_type=StrategyType(payload.get("strategy_type", "declarative")),
            required_fields=tuple(payload.get("required_fields", ())),
            required_indicators=tuple(payload.get("required_indicators", ())),
            parameter_schema=parameter_schema,
            indicator_requests=indicator_requests,
        )
        if definition.strategy_type is not StrategyType.DECLARATIVE:
            raise ValueError("declarative spec must have strategy_type='declarative'")
        hold_payload = payload.get("hold")
        hold = parse_condition(hold_payload) if isinstance(hold_payload, Mapping) else None
        return cls(
            definition=definition,
            entry=parse_condition(payload["entry"]),
            exit=parse_condition(payload["exit"]),
            hold=hold,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = self.definition.to_dict()
        payload["parameters"] = payload.pop("parameter_schema")
        payload["entry"] = condition_to_dict(self.entry)
        payload["exit"] = condition_to_dict(self.exit)
        if self.hold is not None:
            payload["hold"] = condition_to_dict(self.hold)
        return payload
