"""Central registry for trading-system indicator definitions."""

from qrp_atlas.indicators.definitions import (
    IndicatorDefinition,
    IndicatorLayer,
    IndicatorScope,
    UpdateFrequency,
)
from qrp_atlas.indicators.stock.trend import (
    CLOSE_ABOVE_MA5,
    CLOSE_ABOVE_MA5_DAYS,
    CLOSE_BELOW_MA5,
    CLOSE_BELOW_MA5_DAYS,
    MA5,
)
from qrp_atlas.indicators.system_b.detector import (
    SYSTEM_B_EXIT_TRIGGERED,
    SYSTEM_B_TREND_VALID,
)
from qrp_atlas.indicators.events.earnings_forecast import (
    DIRECTION_SCORE,
    EVENT_AGE,
    EVENT_WINDOW,
    NET_PROFIT_MIDPOINT,
    NET_PROFIT_RANGE,
    PROFIT_CHANGE_MIDPOINT,
    PROFIT_CHANGE_RANGE,
)


ALL_INDICATORS: tuple[IndicatorDefinition, ...] = (
    IndicatorDefinition(MA5, "MA5", IndicatorLayer.BASIC, IndicatorScope.STOCK, UpdateFrequency.AFTER_CLOSE, "Five-session moving average."),
    IndicatorDefinition(CLOSE_ABOVE_MA5, "Close above MA5", IndicatorLayer.BASIC, IndicatorScope.STOCK, UpdateFrequency.AFTER_CLOSE, "Whether close is above MA5."),
    IndicatorDefinition(CLOSE_BELOW_MA5, "Close below MA5", IndicatorLayer.BASIC, IndicatorScope.STOCK, UpdateFrequency.AFTER_CLOSE, "Whether close is below MA5."),
    IndicatorDefinition(CLOSE_ABOVE_MA5_DAYS, "Consecutive closes above MA5", IndicatorLayer.BASIC, IndicatorScope.STOCK, UpdateFrequency.AFTER_CLOSE, "Consecutive sessions with close above MA5."),
    IndicatorDefinition(CLOSE_BELOW_MA5_DAYS, "Consecutive closes below MA5", IndicatorLayer.BASIC, IndicatorScope.STOCK, UpdateFrequency.AFTER_CLOSE, "Consecutive sessions with close below MA5."),
    IndicatorDefinition(SYSTEM_B_TREND_VALID, "System B trend valid", IndicatorLayer.SYSTEM_B, IndicatorScope.STOCK, UpdateFrequency.AFTER_CLOSE, "Two consecutive sessions with close at or above MA5."),
    IndicatorDefinition(SYSTEM_B_EXIT_TRIGGERED, "System B exit triggered", IndicatorLayer.SYSTEM_B, IndicatorScope.STOCK, UpdateFrequency.AFTER_CLOSE, "Two consecutive sessions with close below MA5."),
    IndicatorDefinition(
        PROFIT_CHANGE_MIDPOINT,
        "Profit change midpoint",
        IndicatorLayer.BASIC,
        IndicatorScope.STOCK,
        UpdateFrequency.REALTIME,
        "Midpoint of earnings-forecast profit change range. Usable before open on available_trade_date.",
        unit="percent",
        allow_intraday_decision=True,
    ),
    IndicatorDefinition(
        PROFIT_CHANGE_RANGE,
        "Profit change range",
        IndicatorLayer.BASIC,
        IndicatorScope.STOCK,
        UpdateFrequency.REALTIME,
        "Width of earnings-forecast profit change range. Usable before open on available_trade_date.",
        unit="percent",
        allow_intraday_decision=True,
    ),
    IndicatorDefinition(
        NET_PROFIT_MIDPOINT,
        "Net profit midpoint",
        IndicatorLayer.BASIC,
        IndicatorScope.STOCK,
        UpdateFrequency.REALTIME,
        "Midpoint of earnings-forecast net profit range. Usable before open on available_trade_date.",
        unit="万元",
        allow_intraday_decision=True,
    ),
    IndicatorDefinition(
        NET_PROFIT_RANGE,
        "Net profit range",
        IndicatorLayer.BASIC,
        IndicatorScope.STOCK,
        UpdateFrequency.REALTIME,
        "Width of earnings-forecast net profit range. Usable before open on available_trade_date.",
        unit="万元",
        allow_intraday_decision=True,
    ),
    IndicatorDefinition(
        DIRECTION_SCORE,
        "Earnings forecast direction score",
        IndicatorLayer.BASIC,
        IndicatorScope.STOCK,
        UpdateFrequency.REALTIME,
        "Mapped direction of earnings-forecast type: +1/0/-1. Usable before open on available_trade_date.",
        allow_intraday_decision=True,
    ),
    IndicatorDefinition(
        EVENT_AGE,
        "Event age",
        IndicatorLayer.BASIC,
        IndicatorScope.STOCK,
        UpdateFrequency.REALTIME,
        "Open trading days since available_trade_date (0 on availability day). Evaluated for decision-time trade dates.",
        unit="trading_days",
        allow_intraday_decision=True,
    ),
    IndicatorDefinition(
        EVENT_WINDOW,
        "Event window flag",
        IndicatorLayer.BASIC,
        IndicatorScope.STOCK,
        UpdateFrequency.REALTIME,
        "Whether 0 <= event_age < window_days. Usable before open on available_trade_date.",
        allow_intraday_decision=True,
    ),
)


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
