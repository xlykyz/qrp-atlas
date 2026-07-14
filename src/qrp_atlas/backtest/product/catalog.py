"""Product-facing catalog serialization for indicators and strategies."""

from __future__ import annotations

from typing import Any

from qrp_atlas.indicators import list_indicators
from qrp_atlas.indicators.parameterized import CALCULATION_REGISTRY
from qrp_atlas.indicators.cross_section import list_factors
from qrp_atlas.strategies import get_strategy, list_strategies
from qrp_atlas.strategies.registry import StrategyNotFoundError

from .schemas import IndicatorCatalogItem, ParameterSpecDTO, StrategyCatalogItem

_STRATEGY_FAMILY: dict[str, str] = {
    "system_b_basic": "trend",
    "time_series_momentum": "trend",
    "dual_sma_trend": "trend",
    "donchian_breakout": "breakout",
    "rolling_zscore_mean_reversion": "mean_reversion",
    "cross_sectional_momentum_long_only": "cross_sectional",
    "multifactor_long_only": "cross_sectional",
}

_STRATEGY_SCOPE: dict[str, str] = {
    "system_b_basic": "单标的趋势跟踪；需要 System B 状态字段。",
    "time_series_momentum": "单标的/多标的独立信号；基于滚动收益阈值。",
    "dual_sma_trend": "价格行为趋势类；适合中长期单标的或多标的并行。",
    "donchian_breakout": "突破类价格行为；适合趋势启动段。",
    "rolling_zscore_mean_reversion": "均值回归；更适合震荡市。",
    "cross_sectional_momentum_long_only": (
        "横截面动量选股；使用历史指数成分股票池与 PIT 语义；"
        "信号日 T 收盘后选股，下一合法交易日 open 成交。"
    ),
    "multifactor_long_only": "多因子选股；07-B1 暂不作为产品入口。",
}

PRODUCT_SUPPORTED_STRATEGY_CODES: frozenset[str] = frozenset(
    {
        "dual_sma_trend",
        "system_b_basic",
        "time_series_momentum",
        "donchian_breakout",
        "rolling_zscore_mean_reversion",
        "cross_sectional_momentum_long_only",
    }
)

_REQUIRES_HISTORICAL_UNIVERSE: frozenset[str] = frozenset(
    {
        "cross_sectional_momentum_long_only",
    }
)

_SUPPORTED_UNIVERSE_MODES: dict[str, list[str]] = {
    "cross_sectional_momentum_long_only": ["index_components"],
}

_SUPPORTED_ENTRY_TIMINGS: dict[str, list[str]] = {
    "cross_sectional_momentum_long_only": ["next_open"],
}


def _parameter_spec_dto(spec: Any) -> ParameterSpecDTO:
    if hasattr(spec, "to_dict"):
        raw = dict(spec.to_dict())
    elif isinstance(spec, dict):
        raw = dict(spec)
    else:
        raw = {
            "type": getattr(spec, "type", "string"),
            "required": bool(getattr(spec, "required", False)),
            "default": getattr(spec, "default", None),
            "has_default": bool(getattr(spec, "has_default", False)),
            "minimum": getattr(spec, "minimum", None),
            "maximum": getattr(spec, "maximum", None),
            "description": getattr(spec, "description", None),
            "label": getattr(spec, "label", None),
            "enum": getattr(spec, "enum", None),
        }
    return ParameterSpecDTO(
        type=str(raw.get("type") or "string"),
        required=bool(raw.get("required", False)),
        default=raw.get("default"),
        has_default=bool(raw.get("has_default", "default" in raw)),
        minimum=raw.get("minimum"),
        maximum=raw.get("maximum"),
        description=raw.get("description"),
        label=raw.get("label"),
        enum=raw.get("enum"),
    )


def list_indicator_catalog() -> list[IndicatorCatalogItem]:
    """List indicators + parameterized calculations + formal factors."""

    items: list[IndicatorCatalogItem] = []
    seen: set[str] = set()

    for item in list_indicators():
        code = getattr(item, "code", None) or getattr(item, "indicator_id", None)
        if not code or code in seen:
            continue
        items.append(
            IndicatorCatalogItem(
                code=str(code),
                name=str(getattr(item, "name", code)),
                layer=str(getattr(getattr(item, "layer", None), "value", getattr(item, "layer", "basic"))),
                scope=str(getattr(getattr(item, "scope", None), "value", getattr(item, "scope", "stock"))),
                frequency=str(
                    getattr(
                        getattr(item, "update_frequency", None),
                        "value",
                        getattr(item, "frequency", "after_close"),
                    )
                ),
                description=str(getattr(item, "description", "") or ""),
            )
        )
        seen.add(str(code))

    for code, calc in CALCULATION_REGISTRY.items():
        if code in seen:
            continue
        items.append(
            IndicatorCatalogItem(
                code=code,
                name=code,
                layer="basic",
                scope="stock",
                frequency="after_close",
                description=f"Parameterized indicator calculation outputs: {', '.join(calc.outputs)}",
            )
        )
        seen.add(code)

    for factor in list_factors():
        if factor.code in seen:
            continue
        items.append(
            IndicatorCatalogItem(
                code=factor.code,
                name=factor.name,
                layer="basic",
                scope="stock",
                frequency="after_close",
                description=getattr(factor, "description", "") or "Cross-section factor",
            )
        )
        seen.add(factor.code)

    return items


def _strategy_to_catalog_item(definition: Any) -> StrategyCatalogItem:
    code = definition.code
    schema = {
        key: _parameter_spec_dto(spec)
        for key, spec in (definition.parameter_schema or {}).items()
    }
    # Hide research-only explicit dates JSON from product forms.
    if code == "cross_sectional_momentum_long_only":
        schema.pop("explicit_dates_json", None)
        schema.pop("score_column", None)

    indicator_requests: list[dict[str, Any]] = []
    for request in definition.indicator_requests or ():
        if hasattr(request, "to_dict"):
            indicator_requests.append(request.to_dict())
        elif isinstance(request, dict):
            indicator_requests.append(dict(request))
        else:
            indicator_requests.append(
                {
                    "code": getattr(request, "code", None),
                    "parameters": dict(getattr(request, "parameters", {}) or {}),
                    "alias": getattr(request, "alias", None),
                }
            )

    product_supported = code in PRODUCT_SUPPORTED_STRATEGY_CODES
    return StrategyCatalogItem(
        code=code,
        name=definition.name,
        version=definition.version,
        family=_STRATEGY_FAMILY.get(code, "other"),
        description=definition.description,
        scope=_STRATEGY_SCOPE.get(code, "Registered strategy definition."),
        strategy_type=(
            definition.strategy_type.value
            if hasattr(definition.strategy_type, "value")
            else str(definition.strategy_type)
        ),
        required_fields=list(definition.required_fields or ()),
        required_indicators=list(definition.required_indicators or ()),
        parameter_schema=schema,
        indicator_requests=indicator_requests,
        product_supported=product_supported,
        requires_historical_universe=code in _REQUIRES_HISTORICAL_UNIVERSE,
        supported_universe_modes=_SUPPORTED_UNIVERSE_MODES.get(code, ["tickers"]),
        supported_entry_timings=_SUPPORTED_ENTRY_TIMINGS.get(
            code, ["next_open", "same_close", "next_close"]
        ),
        requires_portfolio_config=True,
    )


def list_strategy_catalog(*, product_only: bool = True) -> list[StrategyCatalogItem]:
    """List strategy catalog items from the live registry (no second catalog copy)."""

    items = [_strategy_to_catalog_item(definition) for definition in list_strategies()]
    if product_only:
        items = [item for item in items if item.code in PRODUCT_SUPPORTED_STRATEGY_CODES]
    return items


def get_strategy_catalog_item(code: str, version: str | None = None) -> StrategyCatalogItem:
    try:
        strategy = get_strategy(code, version)
    except StrategyNotFoundError as exc:
        raise KeyError(str(exc)) from exc
    return _strategy_to_catalog_item(strategy.definition)
