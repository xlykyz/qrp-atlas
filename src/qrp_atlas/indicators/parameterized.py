"""Parameterized, dependency-aware indicator calculation for stock time series."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from qrp_atlas.contracts import AMOUNT, CLOSE, HIGH, LOW, TICKER, TRADE_DATE, VOLUME
from qrp_atlas.indicators.stock import calculate_stock_trend
from qrp_atlas.indicators.stock.residual import (
    RESIDUAL_OUTPUT_COLUMNS,
    market_residual_calculator,
)
from qrp_atlas.indicators.stock.momentum import (
    adx_calculator,
    cci_calculator,
    rsi_calculator,
    stochastic_oscillator_calculator,
    williams_r_calculator,
)
from qrp_atlas.indicators.stock.moving import (
    ema_calculator,
    kaufman_efficiency_ratio_calculator,
    linear_regression_trend_calculator,
    macd_calculator,
    wma_calculator,
)
from qrp_atlas.indicators.stock.volatility import (
    atr_breakout_bands_calculator,
    atr_calculator,
    bollinger_bands_calculator,
    downside_volatility_calculator,
    keltner_channel_calculator,
    return_volatility_calculator,
    rolling_current_drawdown_calculator,
    rolling_max_drawdown_calculator,
    true_range_calculator,
    ulcer_index_calculator,
)
from qrp_atlas.indicators.stock.volume import (
    amihud_illiquidity_calculator,
    cmf_calculator,
    mfi_calculator,
    obv_calculator,
    price_volume_correlation_calculator,
    relative_volume_calculator,
    rolling_vwap_calculator,
    volume_sma_calculator,
)
from qrp_atlas.indicators.system_b import calculate_system_b_basic_states_from_prices


class IndicatorRequestError(ValueError):
    """Raised when an indicator request cannot be resolved or calculated."""


class UnknownIndicatorError(IndicatorRequestError):
    """Raised for an unregistered calculation code."""


class IndicatorConflictError(IndicatorRequestError):
    """Raised when aliases or output columns are ambiguous."""


@dataclass(frozen=True)
class IndicatorParameterSpec:
    """Serializable validation rules for one indicator parameter."""

    type: str
    default: Any = None
    has_default: bool = False
    minimum: float | None = None
    maximum: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "default": self.default,
            "has_default": self.has_default,
            "minimum": self.minimum,
            "maximum": self.maximum,
        }


@dataclass(frozen=True)
class IndicatorParameterBinding:
    """Bind an indicator parameter to a strategy parameter at runtime."""

    parameter: str

    def to_dict(self) -> dict[str, str]:
        return {"parameter": self.parameter}


@dataclass(frozen=True)
class IndicatorRequest:
    """A stable request for one parameterized indicator instance."""

    code: str
    parameters: Mapping[str, Any] = field(default_factory=dict)
    alias: str | None = None
    output_fields: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "parameters": {
                key: value.to_dict() if isinstance(value, IndicatorParameterBinding) else value
                for key, value in sorted(self.parameters.items())
            },
            "alias": self.alias,
            "output_fields": dict(sorted(self.output_fields.items())),
        }


@dataclass(frozen=True)
class IndicatorCalculationDefinition:
    """Metadata and implementation for one calculable indicator family."""

    code: str
    parameter_schema: Mapping[str, IndicatorParameterSpec]
    required_fields: tuple[str, ...]
    outputs: tuple[str, ...]
    calculator: Callable[[pd.DataFrame, Mapping[str, Any]], Mapping[str, pd.Series]]
    parameter_validator: Callable[[Mapping[str, Any]], None] | None = None


@dataclass(frozen=True)
class ResolvedIndicatorRequest:
    request: IndicatorRequest
    alias: str
    parameters: Mapping[str, Any]
    output_fields: Mapping[str, str]


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str, label: str) -> None:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise IndicatorRequestError(f"{label} must be a stable identifier: {value!r}")


def _stable_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return format(value, ".12g").replace("-", "neg_").replace(".", "_")
    return str(value).replace("-", "neg_").replace(".", "_")


def _default_alias(code: str, parameters: Mapping[str, Any]) -> str:
    suffix = "_".join(f"{key}_{_stable_value(value)}" for key, value in sorted(parameters.items()))
    return f"{code}_{suffix}" if suffix else code


def _validate_parameter(code: str, name: str, value: Any, spec: IndicatorParameterSpec) -> None:
    if spec.type == "integer":
        valid = isinstance(value, int) and not isinstance(value, bool)
    elif spec.type == "number":
        valid = isinstance(value, (int, float)) and not isinstance(value, bool)
    elif spec.type == "string":
        valid = isinstance(value, str)
    elif spec.type == "boolean":
        valid = isinstance(value, bool)
    else:
        raise IndicatorRequestError(f"indicator {code!r} has invalid parameter schema for {name!r}")
    if not valid:
        raise IndicatorRequestError(
            f"indicator {code!r} parameter {name!r} must be {spec.type}"
        )
    if spec.type in {"integer", "number"}:
        numeric = float(value)
        if not math.isfinite(numeric):
            raise IndicatorRequestError(f"indicator {code!r} parameter {name!r} must be finite")
        if spec.minimum is not None and numeric < spec.minimum:
            raise IndicatorRequestError(
                f"indicator {code!r} parameter {name!r} is below minimum {spec.minimum}"
            )
        if spec.maximum is not None and numeric > spec.maximum:
            raise IndicatorRequestError(
                f"indicator {code!r} parameter {name!r} is above maximum {spec.maximum}"
            )


def _ordered_prices(df: pd.DataFrame, required_fields: Sequence[str]) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise IndicatorRequestError("indicator input must be a pandas DataFrame")
    required = tuple(dict.fromkeys((TICKER, TRADE_DATE, *required_fields)))
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise IndicatorRequestError(f"indicator input missing required fields: {missing}")
    result = df.copy()
    dates = pd.to_datetime(result[TRADE_DATE], errors="coerce")
    if dates.isna().any():
        raise IndicatorRequestError("indicator input contains invalid trade_date values")
    result[TRADE_DATE] = dates
    return result.sort_values([TICKER, TRADE_DATE], kind="mergesort").reset_index(drop=True)


def _rolling(source: str, operation: str) -> Callable[[pd.DataFrame, Mapping[str, Any]], Mapping[str, pd.Series]]:
    def calculate(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
        window = int(parameters["window"])
        grouped = df.groupby(TICKER, sort=False)[source]
        if operation == "mean":
            values = grouped.transform(lambda series: series.rolling(window, min_periods=window).mean())
        elif operation == "std":
            values = grouped.transform(
                lambda series: series.rolling(window, min_periods=window).std(ddof=0)
            )
        elif operation == "max_previous":
            values = grouped.transform(
                lambda series: series.shift(1).rolling(window, min_periods=window).max()
            )
        elif operation == "min_previous":
            values = grouped.transform(
                lambda series: series.shift(1).rolling(window, min_periods=window).min()
            )
        else:  # pragma: no cover - definitions below control this value
            raise AssertionError(operation)
        return {"value": values}

    return calculate


def _period_return(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    lookback = int(parameters["lookback"])
    values = df.groupby(TICKER, sort=False)[CLOSE].transform(
        lambda series: series.div(series.shift(lookback)).sub(1.0)
    )
    return {"value": values.replace([math.inf, -math.inf], math.nan)}


def _rolling_zscore(df: pd.DataFrame, parameters: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    window = int(parameters["window"])
    grouped = df.groupby(TICKER, sort=False)[CLOSE]
    mean = grouped.transform(lambda series: series.rolling(window, min_periods=window).mean())
    std = grouped.transform(
        lambda series: series.rolling(window, min_periods=window).std(ddof=0)
    )
    zscore = (df[CLOSE] - mean).div(std.where(std > 0))
    return {"value": zscore.replace([math.inf, -math.inf], math.nan)}


def _legacy_trend_adapter(df: pd.DataFrame, _: Mapping[str, Any]) -> Mapping[str, pd.Series]:
    """Expose the pre-parameterization trend outputs through the generic registry."""

    trend = calculate_stock_trend(df)
    return {
        "ma5": trend["ma5"],
        "close_above_ma5": trend["close_above_ma5"],
        "close_below_ma5": trend["close_below_ma5"],
        "close_above_ma5_days": trend["close_above_ma5_days"],
        "close_below_ma5_days": trend["close_below_ma5_days"],
    }


def _legacy_system_b_adapter(
    df: pd.DataFrame, _: Mapping[str, Any]
) -> Mapping[str, pd.Series]:
    """Expose existing System B indicator states through the generic registry."""

    states = calculate_system_b_basic_states_from_prices(df)
    return {
        "system_b_trend_valid": states["system_b_trend_valid"],
        "system_b_exit_triggered": states["system_b_exit_triggered"],
    }


_WINDOW = {"window": IndicatorParameterSpec("integer", 20, True, 2, 10000)}
_LOOKBACK = {"lookback": IndicatorParameterSpec("integer", 20, True, 1, 10000)}
_POSITIVE_MULTIPLIER = IndicatorParameterSpec("number", 2.0, True, 0.000001, 1000.0)


def _validate_fast_slow(parameters: Mapping[str, Any]) -> None:
    if parameters["fast_window"] >= parameters["slow_window"]:
        raise IndicatorRequestError("fast_window must be less than slow_window")


CALCULATION_REGISTRY: dict[str, IndicatorCalculationDefinition] = {
    # Existing compatibility calculations.
    "sma": IndicatorCalculationDefinition("sma", _WINDOW, (CLOSE,), ("value",), _rolling(CLOSE, "mean")),
    "period_return": IndicatorCalculationDefinition("period_return", _LOOKBACK, (CLOSE,), ("value",), _period_return),
    "roc": IndicatorCalculationDefinition("roc", _LOOKBACK, (CLOSE,), ("value",), _period_return),
    "donchian_high": IndicatorCalculationDefinition("donchian_high", _WINDOW, (HIGH,), ("value",), _rolling(HIGH, "max_previous")),
    "donchian_low": IndicatorCalculationDefinition("donchian_low", _WINDOW, (LOW,), ("value",), _rolling(LOW, "min_previous")),
    "rolling_mean": IndicatorCalculationDefinition("rolling_mean", _WINDOW, (CLOSE,), ("value",), _rolling(CLOSE, "mean")),
    "rolling_std": IndicatorCalculationDefinition("rolling_std", _WINDOW, (CLOSE,), ("value",), _rolling(CLOSE, "std")),
    "rolling_zscore": IndicatorCalculationDefinition("rolling_zscore", _WINDOW, (CLOSE,), ("value",), _rolling_zscore),
    # Trend and moving averages.
    "ema": IndicatorCalculationDefinition("ema", _WINDOW, (CLOSE,), ("value",), ema_calculator),
    "wma": IndicatorCalculationDefinition("wma", _WINDOW, (CLOSE,), ("value",), wma_calculator),
    "macd": IndicatorCalculationDefinition(
        "macd",
        {
            "fast_window": IndicatorParameterSpec("integer", 12, True, 2, 10000),
            "slow_window": IndicatorParameterSpec("integer", 26, True, 3, 10000),
            "signal_window": IndicatorParameterSpec("integer", 9, True, 2, 10000),
        },
        (CLOSE,),
        ("line", "signal", "histogram"),
        macd_calculator,
        _validate_fast_slow,
    ),
    "linear_regression_trend": IndicatorCalculationDefinition(
        "linear_regression_trend", _WINDOW, (CLOSE,),
        ("slope", "normalized_slope", "r_squared"), linear_regression_trend_calculator,
    ),
    "kaufman_efficiency_ratio": IndicatorCalculationDefinition(
        "kaufman_efficiency_ratio", _WINDOW, (CLOSE,), ("value",),
        kaufman_efficiency_ratio_calculator,
    ),
    # Volatility, ranges, channels, and drawdowns.
    "true_range": IndicatorCalculationDefinition(
        "true_range", {}, (HIGH, LOW, CLOSE), ("value",), true_range_calculator,
    ),
    "atr": IndicatorCalculationDefinition(
        "atr", _WINDOW, (HIGH, LOW, CLOSE), ("value",), atr_calculator,
    ),
    "bollinger_bands": IndicatorCalculationDefinition(
        "bollinger_bands",
        {"window": _WINDOW["window"], "multiplier": _POSITIVE_MULTIPLIER},
        (CLOSE,),
        ("middle", "upper", "lower", "bandwidth", "percent_b"),
        bollinger_bands_calculator,
    ),
    "keltner_channel": IndicatorCalculationDefinition(
        "keltner_channel",
        {
            "ema_window": IndicatorParameterSpec("integer", 20, True, 2, 10000),
            "atr_window": IndicatorParameterSpec("integer", 10, True, 2, 10000),
            "multiplier": _POSITIVE_MULTIPLIER,
        },
        (HIGH, LOW, CLOSE),
        ("middle", "upper", "lower", "atr"),
        keltner_channel_calculator,
    ),
    "return_volatility": IndicatorCalculationDefinition(
        "return_volatility",
        {
            "window": _WINDOW["window"],
            "annualization": IndicatorParameterSpec("number", 252.0, True, 0.000001, 100000.0),
        },
        (CLOSE,), ("value",), return_volatility_calculator,
    ),
    "downside_volatility": IndicatorCalculationDefinition(
        "downside_volatility",
        {
            "window": _WINDOW["window"],
            "annualization": IndicatorParameterSpec("number", 252.0, True, 0.000001, 100000.0),
            "target": IndicatorParameterSpec("number", 0.0, True, -1000.0, 1000.0),
        },
        (CLOSE,), ("value",), downside_volatility_calculator,
    ),
    "rolling_current_drawdown": IndicatorCalculationDefinition(
        "rolling_current_drawdown", _WINDOW, (CLOSE,), ("value",),
        rolling_current_drawdown_calculator,
    ),
    "rolling_max_drawdown": IndicatorCalculationDefinition(
        "rolling_max_drawdown", _WINDOW, (CLOSE,), ("value",),
        rolling_max_drawdown_calculator,
    ),
    "ulcer_index": IndicatorCalculationDefinition(
        "ulcer_index", _WINDOW, (CLOSE,), ("value",), ulcer_index_calculator,
    ),
    "atr_breakout_bands": IndicatorCalculationDefinition(
        "atr_breakout_bands",
        {"window": _WINDOW["window"], "multiplier": _POSITIVE_MULTIPLIER},
        (HIGH, LOW, CLOSE), ("upper", "lower", "atr"), atr_breakout_bands_calculator,
    ),
    # Momentum and oscillators.
    "rsi": IndicatorCalculationDefinition("rsi", _WINDOW, (CLOSE,), ("value",), rsi_calculator),
    "stochastic_oscillator": IndicatorCalculationDefinition(
        "stochastic_oscillator",
        {
            "window": IndicatorParameterSpec("integer", 14, True, 2, 10000),
            "d_window": IndicatorParameterSpec("integer", 3, True, 1, 10000),
        },
        (HIGH, LOW, CLOSE), ("percent_k", "percent_d"), stochastic_oscillator_calculator,
    ),
    "williams_r": IndicatorCalculationDefinition(
        "williams_r", _WINDOW, (HIGH, LOW, CLOSE), ("value",), williams_r_calculator,
    ),
    "cci": IndicatorCalculationDefinition(
        "cci",
        {
            "window": _WINDOW["window"],
            "constant": IndicatorParameterSpec("number", 0.015, True, 0.000001, 1000.0),
        },
        (HIGH, LOW, CLOSE), ("value",), cci_calculator,
    ),
    "adx": IndicatorCalculationDefinition(
        "adx", _WINDOW, (HIGH, LOW, CLOSE), ("adx", "plus_di", "minus_di"), adx_calculator,
    ),
    # Volume and liquidity.
    "obv": IndicatorCalculationDefinition("obv", {}, (CLOSE, VOLUME), ("value",), obv_calculator),
    "rolling_vwap": IndicatorCalculationDefinition(
        "rolling_vwap", _WINDOW, (CLOSE, VOLUME), ("value",), rolling_vwap_calculator,
    ),
    "volume_sma": IndicatorCalculationDefinition(
        "volume_sma", _WINDOW, (VOLUME,), ("value",), volume_sma_calculator,
    ),
    "relative_volume": IndicatorCalculationDefinition(
        "relative_volume", _WINDOW, (VOLUME,), ("value",), relative_volume_calculator,
    ),
    "mfi": IndicatorCalculationDefinition(
        "mfi", _WINDOW, (HIGH, LOW, CLOSE, VOLUME), ("value",), mfi_calculator,
    ),
    "cmf": IndicatorCalculationDefinition(
        "cmf", _WINDOW, (HIGH, LOW, CLOSE, VOLUME), ("value",), cmf_calculator,
    ),
    "amihud_illiquidity": IndicatorCalculationDefinition(
        "amihud_illiquidity",
        {
            "window": _WINDOW["window"],
            "scale": IndicatorParameterSpec("number", 1.0, True, 0.000001, 1e20),
        },
        (CLOSE, AMOUNT), ("value",), amihud_illiquidity_calculator,
    ),
    "price_volume_correlation": IndicatorCalculationDefinition(
        "price_volume_correlation", _WINDOW, (CLOSE, VOLUME), ("value",),
        price_volume_correlation_calculator,
    ),
    "market_residual": IndicatorCalculationDefinition(
        "market_residual",
        {
            "window": IndicatorParameterSpec("integer", 60, True, 2, 10000),
            "min_periods": IndicatorParameterSpec("integer", 60, True, 2, 10000),
            "z_window": IndicatorParameterSpec("integer", 60, True, 2, 10000),
            "fit_intercept": IndicatorParameterSpec("boolean", True, True),
        },
        ("asset_return", "benchmark_return"),
        RESIDUAL_OUTPUT_COLUMNS,
        market_residual_calculator,
    ),
    "stock_trend_legacy": IndicatorCalculationDefinition(
        "stock_trend_legacy", {}, (CLOSE,),
        ("ma5", "close_above_ma5", "close_below_ma5", "close_above_ma5_days", "close_below_ma5_days"),
        _legacy_trend_adapter,
    ),
    "system_b_states": IndicatorCalculationDefinition(
        "system_b_states", {}, (CLOSE,),
        ("system_b_trend_valid", "system_b_exit_triggered"), _legacy_system_b_adapter,
    ),
}

LEGACY_INDICATOR_REQUESTS: dict[str, IndicatorRequest] = {
    output: IndicatorRequest("stock_trend_legacy", alias=output, output_fields={output: output})
    for output in CALCULATION_REGISTRY["stock_trend_legacy"].outputs
}
LEGACY_INDICATOR_REQUESTS.update(
    {
        output: IndicatorRequest("system_b_states", alias=output, output_fields={output: output})
        for output in CALCULATION_REGISTRY["system_b_states"].outputs
    }
)


def get_calculation_definition(code: str) -> IndicatorCalculationDefinition:
    try:
        return CALCULATION_REGISTRY[code]
    except KeyError:
        raise UnknownIndicatorError(f"unknown indicator code: {code}") from None


def bind_indicator_request(
    request: IndicatorRequest, strategy_parameters: Mapping[str, Any]
) -> IndicatorRequest:
    """Resolve strategy-parameter bindings without changing alias/output identity."""

    bound: dict[str, Any] = {}
    for name, value in request.parameters.items():
        if isinstance(value, IndicatorParameterBinding):
            if value.parameter not in strategy_parameters:
                raise IndicatorRequestError(
                    f"indicator {request.code!r} references unknown strategy parameter {value.parameter!r}"
                )
            bound[name] = strategy_parameters[value.parameter]
        else:
            bound[name] = value
    return IndicatorRequest(request.code, bound, request.alias, request.output_fields)


def resolve_indicator_requests(
    requests: Sequence[IndicatorRequest], *, existing_columns: Sequence[str] = ()
) -> tuple[ResolvedIndicatorRequest, ...]:
    """Validate requests and produce deterministic aliases and output columns."""

    aliases: set[str] = set()
    outputs = set(existing_columns)
    resolved: list[ResolvedIndicatorRequest] = []
    for request in requests:
        if not isinstance(request, IndicatorRequest):
            raise IndicatorRequestError("indicator requests must contain IndicatorRequest values")
        definition = get_calculation_definition(request.code)
        unknown = sorted(set(request.parameters) - set(definition.parameter_schema))
        if unknown:
            raise IndicatorRequestError(
                f"indicator {request.code!r} has unknown parameters: {unknown}"
            )
        parameters: dict[str, Any] = {}
        for name, spec in definition.parameter_schema.items():
            if name in request.parameters:
                value = request.parameters[name]
            elif spec.has_default:
                value = spec.default
            else:
                raise IndicatorRequestError(
                    f"indicator {request.code!r} missing required parameter {name!r}"
                )
            if isinstance(value, IndicatorParameterBinding):
                raise IndicatorRequestError(
                    f"indicator {request.code!r} contains an unbound strategy parameter"
                )
            _validate_parameter(request.code, name, value, spec)
            parameters[name] = value
        if definition.parameter_validator is not None:
            definition.parameter_validator(parameters)

        alias = request.alias or _default_alias(request.code, parameters)
        _validate_identifier(alias, "indicator alias")
        if alias in aliases:
            raise IndicatorConflictError(f"duplicate indicator alias: {alias}")
        aliases.add(alias)

        unknown_outputs = sorted(set(request.output_fields) - set(definition.outputs))
        if unknown_outputs:
            raise IndicatorRequestError(
                f"indicator {request.code!r} has unknown output fields: {unknown_outputs}"
            )
        if request.output_fields:
            output_fields = dict(request.output_fields)
        elif len(definition.outputs) == 1:
            output_fields = {definition.outputs[0]: alias}
        else:
            output_fields = {name: f"{alias}_{name}" for name in definition.outputs}
        for canonical, output in output_fields.items():
            _validate_identifier(output, f"output field {canonical!r}")
            if output in outputs:
                raise IndicatorConflictError(f"indicator output field conflict: {output}")
            outputs.add(output)
        resolved.append(ResolvedIndicatorRequest(request, alias, parameters, output_fields))
    return tuple(resolved)


def indicator_output_fields(requests: Sequence[IndicatorRequest]) -> tuple[str, ...]:
    """Return declared output names; parameter bindings do not affect explicit aliases."""

    materialized = []
    for request in requests:
        parameters = {
            key: (1 if isinstance(value, IndicatorParameterBinding) else value)
            for key, value in request.parameters.items()
        }
        definition = get_calculation_definition(request.code)
        for name, spec in definition.parameter_schema.items():
            if name not in parameters and spec.has_default:
                parameters[name] = spec.default
        alias = request.alias or _default_alias(request.code, parameters)
        if request.output_fields:
            materialized.extend(request.output_fields.values())
        elif len(definition.outputs) == 1:
            materialized.append(alias)
        else:
            materialized.extend(f"{alias}_{name}" for name in definition.outputs)
    return tuple(materialized)


def calculate_indicators(
    df: pd.DataFrame, requests: Sequence[IndicatorRequest]
) -> pd.DataFrame:
    """Calculate all requests by ticker/date without modifying caller input."""

    resolved = resolve_indicator_requests(requests, existing_columns=df.columns)
    required_fields = tuple(
        dict.fromkeys(
            field
            for item in resolved
            for field in get_calculation_definition(item.request.code).required_fields
        )
    )
    result = _ordered_prices(df, required_fields)
    for item in resolved:
        definition = get_calculation_definition(item.request.code)
        calculated = definition.calculator(result, item.parameters)
        for canonical, output in item.output_fields.items():
            values = calculated[canonical]
            if pd.api.types.is_numeric_dtype(values) and not pd.api.types.is_bool_dtype(values):
                values = values.replace([math.inf, -math.inf], math.nan)
            result[output] = values.to_numpy()
    return result


def requests_for_legacy_indicators(codes: Sequence[str]) -> tuple[IndicatorRequest, ...]:
    """Translate legacy output-code declarations into generic calculation requests."""

    requests: list[IndicatorRequest] = []
    for code in codes:
        try:
            requests.append(LEGACY_INDICATOR_REQUESTS[code])
        except KeyError:
            raise UnknownIndicatorError(f"unknown legacy indicator code: {code}") from None
    return tuple(requests)
