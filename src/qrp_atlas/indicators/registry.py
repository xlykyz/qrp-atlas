"""Central registry for trading-system indicator definitions.

This module intentionally starts as an empty dictionary. Indicator definitions
can be added later without changing callers that use list_indicators() or
get_indicator().
"""

from qrp_atlas.indicators.definitions import IndicatorDefinition, IndicatorLayer


ALL_INDICATORS: tuple[IndicatorDefinition, ...] = ()


INDICATOR_BY_CODE: dict[str, IndicatorDefinition] = {
    indicator.code: indicator for indicator in ALL_INDICATORS
}


if len(INDICATOR_BY_CODE) != len(ALL_INDICATORS):
    raise ValueError("Duplicate indicator code found in indicator registry")


def list_indicators(layer: IndicatorLayer | None = None) -> list[IndicatorDefinition]:
    """Return registered indicator definitions, optionally filtered by layer."""

    indicators = list(ALL_INDICATORS)

    if layer is None:
        return indicators

    return [indicator for indicator in indicators if indicator.layer == layer]


def get_indicator(code: str) -> IndicatorDefinition:
    """Return one registered indicator definition by its unique code."""

    try:
        return INDICATOR_BY_CODE[code]
    except KeyError:
        raise KeyError(f"Unknown indicator code: {code}") from None
