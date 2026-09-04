"""Validation for declarations, prepared inputs, and strategy parameters."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

import pandas as pd

from qrp_atlas.contracts import TICKER, TRADE_DATE
from qrp_atlas.contracts import fields as contract_fields
from qrp_atlas.indicators import (
    IndicatorParameterBinding,
    IndicatorRequestError,
    get_calculation_definition,
    indicator_output_fields,
)
from qrp_atlas.indicators.registry import get_indicator

from .models import ParameterSpec, StrategyDefinition, StrategyInput, StrategyInputScope



class StrategyValidationError(ValueError):
    """Raised when a strategy declaration or invocation violates its contract."""


def known_contract_fields() -> frozenset[str]:
    """Return canonical field codes exported by the contracts SSOT."""

    return frozenset(
        value
        for name, value in vars(contract_fields).items()
        if name.isupper() and isinstance(value, str)
    )


def validate_definition(definition: StrategyDefinition) -> None:
    """Validate a strategy declaration against contracts and indicator metadata."""

    if not definition.code or not definition.version:
        raise StrategyValidationError("strategy code and version must be non-empty")
    if not isinstance(definition.input_scope, StrategyInputScope):
        raise StrategyValidationError(f"invalid input_scope: {definition.input_scope!r}")
    if len(set(definition.required_fields)) != len(definition.required_fields):
        raise StrategyValidationError("required_fields must not contain duplicates")
    if len(set(definition.required_indicators)) != len(definition.required_indicators):
        raise StrategyValidationError("required_indicators must not contain duplicates")

    unknown_fields = sorted(set(definition.required_fields) - known_contract_fields())
    if unknown_fields:
        raise StrategyValidationError(f"unknown contract fields: {unknown_fields}")

    for code in definition.required_indicators:
        try:
            get_indicator(code)
        except KeyError as exc:
            raise StrategyValidationError(str(exc)) from exc

    request_aliases: set[str] = set()
    request_outputs: set[str] = set(definition.required_indicators)
    for request in definition.indicator_requests:
        try:
            calculation = get_calculation_definition(request.code)
        except IndicatorRequestError as exc:
            raise StrategyValidationError(str(exc)) from exc
        unknown = sorted(set(request.parameters) - set(calculation.parameter_schema))
        if unknown:
            raise StrategyValidationError(
                f"indicator {request.code!r} has unknown parameters: {unknown}"
            )
        for value in request.parameters.values():
            if isinstance(value, IndicatorParameterBinding) and value.parameter not in definition.parameter_schema:
                raise StrategyValidationError(
                    f"indicator {request.code!r} references unknown strategy parameter {value.parameter!r}"
                )
            if isinstance(value, IndicatorParameterBinding) and request.alias is None:
                raise StrategyValidationError(
                    f"indicator {request.code!r} with parameter bindings requires an explicit alias"
                )
        alias = request.alias
        if alias is not None:
            if alias in request_aliases:
                raise StrategyValidationError(f"duplicate indicator alias: {alias}")
            request_aliases.add(alias)
    try:
        output_columns = indicator_output_fields(definition.indicator_requests)
    except IndicatorRequestError as exc:
        raise StrategyValidationError(str(exc)) from exc
    for output in output_columns:
        if output in request_outputs:
            raise StrategyValidationError(f"duplicate indicator output field: {output}")
        request_outputs.add(output)

    for code, spec in definition.parameter_schema.items():
        if not code:
            raise StrategyValidationError("parameter code must be non-empty")
        _validate_parameter_spec(code, spec)


def _validate_parameter_spec(code: str, spec: ParameterSpec) -> None:
    if spec.type not in {"number", "integer", "string", "boolean"}:
        raise StrategyValidationError(f"parameter {code!r} has unsupported type {spec.type!r}")
    if spec.minimum is not None and spec.maximum is not None and spec.minimum > spec.maximum:
        raise StrategyValidationError(f"parameter {code!r} minimum exceeds maximum")
    if spec.has_default:
        _validate_parameter_value(code, spec.default, spec)


def resolve_parameters(
    definition: StrategyDefinition, parameters: Mapping[str, Any]
) -> dict[str, Any]:
    """Apply defaults and validate caller-supplied strategy parameters."""

    if not isinstance(parameters, Mapping):
        raise StrategyValidationError("parameters must be a mapping")
    unknown = sorted(set(parameters) - set(definition.parameter_schema))
    if unknown:
        raise StrategyValidationError(f"unknown strategy parameters: {unknown}")

    resolved: dict[str, Any] = {}
    for code, spec in definition.parameter_schema.items():
        if code in parameters:
            value = parameters[code]
        elif spec.has_default:
            value = spec.default
        elif spec.required:
            raise StrategyValidationError(f"missing required parameter: {code}")
        else:
            value = None
        if value is not None:
            _validate_parameter_value(code, value, spec)
        resolved[code] = value
    return resolved


def _validate_parameter_value(code: str, value: Any, spec: ParameterSpec) -> None:
    valid_type = {
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "string": lambda v: isinstance(v, str),
        "boolean": lambda v: isinstance(v, bool),
    }[spec.type]
    if not valid_type(value):
        raise StrategyValidationError(
            f"parameter {code!r} must be {spec.type}, got {type(value).__name__}"
        )
    if spec.type in {"number", "integer"}:
        numeric = float(value)
        if not math.isfinite(numeric):
            raise StrategyValidationError(f"parameter {code!r} must be finite")
        if spec.minimum is not None and numeric < spec.minimum:
            raise StrategyValidationError(f"parameter {code!r} is below minimum {spec.minimum}")
        if spec.maximum is not None and numeric > spec.maximum:
            raise StrategyValidationError(f"parameter {code!r} is above maximum {spec.maximum}")


def validate_strategy_input(definition: StrategyDefinition, strategy_input: StrategyInput) -> pd.DataFrame:
    """Validate and canonically order a prepared input frame for deterministic runs."""

    if not isinstance(strategy_input, StrategyInput):
        raise StrategyValidationError("strategy_input must be a StrategyInput instance")
    df = strategy_input.prepared_data
    if not isinstance(df, pd.DataFrame):
        raise StrategyValidationError("prepared_data must be a pandas DataFrame")

    parameterized_outputs = indicator_output_fields(definition.indicator_requests)
    required_columns = tuple(
        dict.fromkeys((*definition.required_fields, *definition.required_indicators, *parameterized_outputs))
    )
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise StrategyValidationError(f"prepared_data missing required columns: {missing}")

    scope = getattr(definition, "input_scope", StrategyInputScope.ASSET)
    if scope is StrategyInputScope.MARKET:
        identity_fields = (TRADE_DATE,)
    else:
        identity_fields = (TICKER, TRADE_DATE)

    identity_missing = [column for column in identity_fields if column not in df.columns]
    if identity_missing:
        raise StrategyValidationError(
            f"prepared_data must include identity fields: {identity_missing}"
        )

    if df.empty:
        return df.copy()

    result = df.copy()
    parsed_dates = pd.to_datetime(result[TRADE_DATE], errors="coerce", format="mixed")
    if parsed_dates.isna().any():
        raise StrategyValidationError("prepared_data contains invalid trade_date values")
    result[TRADE_DATE] = parsed_dates.dt.strftime("%Y-%m-%d")


    if scope is StrategyInputScope.MARKET:
        duplicate_count = int(result.duplicated(subset=[TRADE_DATE], keep=False).sum())
        if duplicate_count:
            raise StrategyValidationError(
                f"prepared_data has {duplicate_count} duplicate trade_date rows"
            )
    else:
        if result[TICKER].isna().any() or (result[TICKER].astype(str).str.strip() == "").any():
            raise StrategyValidationError("prepared_data contains missing ticker values")
        result[TICKER] = result[TICKER].astype(str)
        duplicate_count = int(result.duplicated(subset=[TICKER, TRADE_DATE], keep=False).sum())
        if duplicate_count:
            raise StrategyValidationError(
                f"prepared_data has {duplicate_count} duplicate (ticker, trade_date) rows"
            )


    strict_columns = tuple(dict.fromkeys((*definition.required_fields, *definition.required_indicators)))
    for column in strict_columns:
        values = result[column]
        if values.isna().any():
            raise StrategyValidationError(f"prepared_data contains missing values for {column!r}")
        if pd.api.types.is_numeric_dtype(values) and not pd.api.types.is_bool_dtype(values):
            numeric = pd.to_numeric(values, errors="raise")
            if not numeric.map(math.isfinite).all():
                raise StrategyValidationError(
                    f"prepared_data contains non-finite values for {column!r}"
                )

    if not isinstance(strategy_input.initial_positions, Mapping):
        raise StrategyValidationError("initial_positions must be a mapping")
    if any(not isinstance(value, bool) for value in strategy_input.initial_positions.values()):
        raise StrategyValidationError("initial_positions values must be bool")

    if scope is StrategyInputScope.MARKET:
        return result.sort_values([TRADE_DATE], kind="mergesort").reset_index(drop=True)
    return result.sort_values([TICKER, TRADE_DATE], kind="mergesort").reset_index(drop=True)

