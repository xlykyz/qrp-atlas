"""Additional classic deterministic long-only technical strategies.

These strategies only evaluate aliases produced by ``IndicatorRequest``. They do
not calculate or duplicate indicator algorithms.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd

from qrp_atlas.contracts import CLOSE, TICKER, TRADE_DATE
from qrp_atlas.indicators import IndicatorParameterBinding, IndicatorRequest

from ..models import ParameterSpec, StrategyDefinition, StrategyType
from ..validation import StrategyValidationError
from .classic import _ClassicLongOnlyStrategy, _RuleResult


def _integer(default: int, minimum: int = 2) -> ParameterSpec:
    return ParameterSpec(
        "integer", default=default, has_default=True, minimum=minimum, maximum=10000
    )


def _number(
    default: float, minimum: float = -1000.0, maximum: float = 1000.0
) -> ParameterSpec:
    return ParameterSpec(
        "number", default=default, has_default=True, minimum=minimum, maximum=maximum
    )


class DualEmaTrendStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="dual_ema_trend",
        name="Dual EMA Trend",
        version="1.0.0",
        description="Long while a fast SMA-seeded EMA is above a slower EMA.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={"fast_window": _integer(12), "slow_window": _integer(26)},
        indicator_requests=(
            IndicatorRequest("ema", {"window": IndicatorParameterBinding("fast_window")}, alias="fast_ema"),
            IndicatorRequest("ema", {"window": IndicatorParameterBinding("slow_window")}, alias="slow_ema"),
        ),
    )
    enter_reason = "FAST_EMA_ABOVE_SLOW_EMA"
    exit_reason = "FAST_EMA_NOT_ABOVE_SLOW_EMA"

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if parameters["fast_window"] >= parameters["slow_window"]:
            raise StrategyValidationError("fast_window must be less than slow_window")

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        fast, slow = getattr(row, "fast_ema"), getattr(row, "slow_ema")
        warmup = pd.isna(fast) or pd.isna(slow)
        above = not warmup and float(fast) > float(slow)
        return _RuleResult(
            above,
            not warmup and not above,
            warmup,
            {"fast_ema": None if pd.isna(fast) else float(fast), "slow_ema": None if pd.isna(slow) else float(slow)},
        )


class MacdTrendStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="macd_trend",
        name="MACD Trend",
        version="1.0.0",
        description="Long while the MACD line is above its signal line.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={
            "fast_window": _integer(12),
            "slow_window": _integer(26, 3),
            "signal_window": _integer(9),
        },
        indicator_requests=(
            IndicatorRequest(
                "macd",
                {
                    "fast_window": IndicatorParameterBinding("fast_window"),
                    "slow_window": IndicatorParameterBinding("slow_window"),
                    "signal_window": IndicatorParameterBinding("signal_window"),
                },
                alias="macd",
            ),
        ),
    )
    enter_reason = "MACD_LINE_ABOVE_SIGNAL"
    exit_reason = "MACD_LINE_NOT_ABOVE_SIGNAL"

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if parameters["fast_window"] >= parameters["slow_window"]:
            raise StrategyValidationError("fast_window must be less than slow_window")

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        line, signal = getattr(row, "macd_line"), getattr(row, "macd_signal")
        warmup = pd.isna(line) or pd.isna(signal)
        above = not warmup and float(line) > float(signal)
        return _RuleResult(
            above,
            not warmup and not above,
            warmup,
            {"macd_line": None if pd.isna(line) else float(line), "macd_signal": None if pd.isna(signal) else float(signal)},
        )


class RsiMeanReversionStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="rsi_mean_reversion",
        name="RSI Mean Reversion",
        version="1.0.0",
        description="Enter at an oversold Wilder RSI and exit after recovery.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={
            "window": _integer(14),
            "entry_rsi": _number(30.0, 0.0, 100.0),
            "exit_rsi": _number(50.0, 0.0, 100.0),
        },
        indicator_requests=(
            IndicatorRequest("rsi", {"window": IndicatorParameterBinding("window")}, alias="rsi"),
        ),
    )
    enter_reason = "RSI_AT_OR_BELOW_ENTRY"
    exit_reason = "RSI_AT_OR_ABOVE_EXIT"

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if parameters["entry_rsi"] >= parameters["exit_rsi"]:
            raise StrategyValidationError("entry_rsi must be less than exit_rsi")

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        value = getattr(row, "rsi")
        warmup = pd.isna(value)
        return _RuleResult(
            not warmup and float(value) <= float(parameters["entry_rsi"]),
            not warmup and float(value) >= float(parameters["exit_rsi"]),
            warmup,
            {"rsi": None if warmup else float(value), "entry_rsi": float(parameters["entry_rsi"]), "exit_rsi": float(parameters["exit_rsi"])},
        )


class BollingerMeanReversionStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="bollinger_mean_reversion",
        name="Bollinger Bands Mean Reversion",
        version="1.0.0",
        description="Enter at/below the lower band and exit at/above the middle band.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE, CLOSE),
        required_indicators=(),
        parameter_schema={
            "window": _integer(20),
            "multiplier": _number(2.0, 0.000001, 1000.0),
        },
        indicator_requests=(
            IndicatorRequest(
                "bollinger_bands",
                {"window": IndicatorParameterBinding("window"), "multiplier": IndicatorParameterBinding("multiplier")},
                alias="bb",
            ),
        ),
    )
    enter_reason = "CLOSE_AT_OR_BELOW_LOWER_BAND"
    exit_reason = "CLOSE_AT_OR_ABOVE_MIDDLE_BAND"

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        close = float(getattr(row, CLOSE))
        middle, lower = getattr(row, "bb_middle"), getattr(row, "bb_lower")
        warmup = pd.isna(middle) or pd.isna(lower)
        return _RuleResult(
            not warmup and close <= float(lower),
            not warmup and close >= float(middle),
            warmup,
            {"close": close, "middle": None if pd.isna(middle) else float(middle), "lower": None if pd.isna(lower) else float(lower)},
        )


class StochasticMeanReversionStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="stochastic_mean_reversion",
        name="Stochastic Mean Reversion",
        version="1.0.0",
        description="Enter at an oversold stochastic %K and exit after %K recovers.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={
            "window": _integer(14),
            "d_window": _integer(3, 1),
            "entry_level": _number(20.0, 0.0, 100.0),
            "exit_level": _number(50.0, 0.0, 100.0),
        },
        indicator_requests=(
            IndicatorRequest(
                "stochastic_oscillator",
                {"window": IndicatorParameterBinding("window"), "d_window": IndicatorParameterBinding("d_window")},
                alias="stochastic",
            ),
        ),
    )
    enter_reason = "STOCHASTIC_K_AT_OR_BELOW_ENTRY"
    exit_reason = "STOCHASTIC_K_AT_OR_ABOVE_EXIT"

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if parameters["entry_level"] >= parameters["exit_level"]:
            raise StrategyValidationError("entry_level must be less than exit_level")

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        percent_k = getattr(row, "stochastic_percent_k")
        percent_d = getattr(row, "stochastic_percent_d")
        warmup = pd.isna(percent_k) or pd.isna(percent_d)
        return _RuleResult(
            not warmup and float(percent_k) <= float(parameters["entry_level"]),
            not warmup and float(percent_k) >= float(parameters["exit_level"]),
            warmup,
            {"percent_k": None if pd.isna(percent_k) else float(percent_k), "percent_d": None if pd.isna(percent_d) else float(percent_d)},
        )


class AdxDirectionalTrendStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="adx_directional_trend",
        name="ADX Directional Trend",
        version="1.0.0",
        description="Enter a strong +DI trend and exit on directional reversal or weak ADX.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={
            "window": _integer(14),
            "entry_adx": _number(25.0, 0.0, 100.0),
            "exit_adx": _number(20.0, 0.0, 100.0),
        },
        indicator_requests=(
            IndicatorRequest("adx", {"window": IndicatorParameterBinding("window")}, alias="direction"),
        ),
    )
    enter_reason = "ADX_STRONG_POSITIVE_DIRECTION"
    exit_reason = "ADX_WEAK_OR_DIRECTION_REVERSED"

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if parameters["exit_adx"] > parameters["entry_adx"]:
            raise StrategyValidationError("exit_adx must be less than or equal to entry_adx")

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        adx = getattr(row, "direction_adx")
        plus_di = getattr(row, "direction_plus_di")
        minus_di = getattr(row, "direction_minus_di")
        warmup = pd.isna(adx) or pd.isna(plus_di) or pd.isna(minus_di)
        return _RuleResult(
            not warmup and float(adx) >= float(parameters["entry_adx"]) and float(plus_di) > float(minus_di),
            not warmup and (float(adx) < float(parameters["exit_adx"]) or float(plus_di) <= float(minus_di)),
            warmup,
            {"adx": None if pd.isna(adx) else float(adx), "plus_di": None if pd.isna(plus_di) else float(plus_di), "minus_di": None if pd.isna(minus_di) else float(minus_di)},
        )


class KeltnerBreakoutStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="keltner_breakout",
        name="Keltner Channel Breakout",
        version="1.0.0",
        description="Enter above the upper Keltner channel and exit below its EMA center.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE, CLOSE),
        required_indicators=(),
        parameter_schema={
            "ema_window": _integer(20),
            "atr_window": _integer(10),
            "multiplier": _number(2.0, 0.000001, 1000.0),
        },
        indicator_requests=(
            IndicatorRequest(
                "keltner_channel",
                {
                    "ema_window": IndicatorParameterBinding("ema_window"),
                    "atr_window": IndicatorParameterBinding("atr_window"),
                    "multiplier": IndicatorParameterBinding("multiplier"),
                },
                alias="keltner",
            ),
        ),
    )
    enter_reason = "CLOSE_ABOVE_KELTNER_UPPER"
    exit_reason = "CLOSE_BELOW_KELTNER_MIDDLE"

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        close = float(getattr(row, CLOSE))
        upper, middle = getattr(row, "keltner_upper"), getattr(row, "keltner_middle")
        warmup = pd.isna(upper) or pd.isna(middle)
        return _RuleResult(
            not warmup and close > float(upper),
            not warmup and close < float(middle),
            warmup,
            {"close": close, "upper": None if pd.isna(upper) else float(upper), "middle": None if pd.isna(middle) else float(middle)},
        )


class AtrVolatilityBreakoutStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="atr_volatility_breakout",
        name="ATR Volatility Breakout",
        version="1.0.0",
        description="Enter/exit on prior-close bands built from prior Wilder ATR.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE, CLOSE),
        required_indicators=(),
        parameter_schema={
            "window": _integer(14),
            "multiplier": _number(1.0, 0.000001, 1000.0),
        },
        indicator_requests=(
            IndicatorRequest(
                "atr_breakout_bands",
                {"window": IndicatorParameterBinding("window"), "multiplier": IndicatorParameterBinding("multiplier")},
                alias="atr_breakout",
            ),
        ),
    )
    enter_reason = "CLOSE_ABOVE_PRIOR_ATR_BAND"
    exit_reason = "CLOSE_BELOW_PRIOR_ATR_BAND"

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        close = float(getattr(row, CLOSE))
        upper, lower = getattr(row, "atr_breakout_upper"), getattr(row, "atr_breakout_lower")
        warmup = pd.isna(upper) or pd.isna(lower)
        return _RuleResult(
            not warmup and close > float(upper),
            not warmup and close < float(lower),
            warmup,
            {"close": close, "upper": None if pd.isna(upper) else float(upper), "lower": None if pd.isna(lower) else float(lower)},
        )


class LinearRegressionTrendStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="linear_regression_trend",
        name="Linear Regression Trend",
        version="1.0.0",
        description="Enter a positive normalized regression trend with adequate fit quality.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={
            "window": _integer(20),
            "entry_slope": _number(0.001, -10.0, 10.0),
            "exit_slope": _number(0.0, -10.0, 10.0),
            "entry_r_squared": _number(0.5, 0.0, 1.0),
            "exit_r_squared": _number(0.2, 0.0, 1.0),
        },
        indicator_requests=(
            IndicatorRequest(
                "linear_regression_trend",
                {"window": IndicatorParameterBinding("window")},
                alias="regression",
            ),
        ),
    )
    enter_reason = "REGRESSION_TREND_CONFIRMED"
    exit_reason = "REGRESSION_TREND_LOST"

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if parameters["exit_slope"] > parameters["entry_slope"]:
            raise StrategyValidationError("exit_slope must be less than or equal to entry_slope")
        if parameters["exit_r_squared"] > parameters["entry_r_squared"]:
            raise StrategyValidationError("exit_r_squared must be less than or equal to entry_r_squared")

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        slope = getattr(row, "regression_normalized_slope")
        r_squared = getattr(row, "regression_r_squared")
        warmup = pd.isna(slope) or pd.isna(r_squared)
        return _RuleResult(
            not warmup and float(slope) >= float(parameters["entry_slope"]) and float(r_squared) >= float(parameters["entry_r_squared"]),
            not warmup and (float(slope) <= float(parameters["exit_slope"]) or float(r_squared) < float(parameters["exit_r_squared"])),
            warmup,
            {"normalized_slope": None if pd.isna(slope) else float(slope), "r_squared": None if pd.isna(r_squared) else float(r_squared)},
        )


class VolatilityAdjustedMomentumStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="volatility_adjusted_momentum",
        name="Volatility-Adjusted Time-Series Momentum",
        version="1.0.0",
        description="Long when trailing return divided by realized volatility exceeds a threshold.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={
            "lookback": _integer(60, 1),
            "volatility_window": _integer(20),
            "annualization": _number(252.0, 0.000001, 100000.0),
            "entry_score": _number(0.5, -1000.0, 1000.0),
            "exit_score": _number(0.0, -1000.0, 1000.0),
        },
        indicator_requests=(
            IndicatorRequest("period_return", {"lookback": IndicatorParameterBinding("lookback")}, alias="momentum"),
            IndicatorRequest(
                "return_volatility",
                {"window": IndicatorParameterBinding("volatility_window"), "annualization": IndicatorParameterBinding("annualization")},
                alias="volatility",
            ),
        ),
    )
    enter_reason = "VOLATILITY_ADJUSTED_MOMENTUM_ABOVE_ENTRY"
    exit_reason = "VOLATILITY_ADJUSTED_MOMENTUM_AT_OR_BELOW_EXIT"

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if parameters["exit_score"] >= parameters["entry_score"]:
            raise StrategyValidationError("exit_score must be less than entry_score")

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        momentum, volatility = getattr(row, "momentum"), getattr(row, "volatility")
        warmup = pd.isna(momentum) or pd.isna(volatility) or float(volatility) <= 0.0
        score = None if warmup else float(momentum) / float(volatility)
        return _RuleResult(
            not warmup and score is not None and score > float(parameters["entry_score"]),
            not warmup and score is not None and score <= float(parameters["exit_score"]),
            warmup,
            {"momentum": None if pd.isna(momentum) else float(momentum), "volatility": None if pd.isna(volatility) else float(volatility), "score": score},
        )


class VolumeConfirmedEmaTrendStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="volume_confirmed_ema_trend",
        name="Volume-Confirmed EMA Trend",
        version="1.0.0",
        description="Enter an EMA uptrend only when current volume exceeds its prior average.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={
            "fast_window": _integer(12),
            "slow_window": _integer(26),
            "volume_window": _integer(20),
            "min_relative_volume": _number(1.0, 0.0, 1000.0),
        },
        indicator_requests=(
            IndicatorRequest("ema", {"window": IndicatorParameterBinding("fast_window")}, alias="fast_ema"),
            IndicatorRequest("ema", {"window": IndicatorParameterBinding("slow_window")}, alias="slow_ema"),
            IndicatorRequest("relative_volume", {"window": IndicatorParameterBinding("volume_window")}, alias="relative_volume"),
        ),
    )
    enter_reason = "EMA_TREND_CONFIRMED_BY_VOLUME"
    exit_reason = "EMA_TREND_REVERSED"

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if parameters["fast_window"] >= parameters["slow_window"]:
            raise StrategyValidationError("fast_window must be less than slow_window")

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        fast, slow = getattr(row, "fast_ema"), getattr(row, "slow_ema")
        relative_volume = getattr(row, "relative_volume")
        warmup = pd.isna(fast) or pd.isna(slow)
        trend = not warmup and float(fast) > float(slow)
        volume_confirmed = not pd.isna(relative_volume) and float(relative_volume) >= float(parameters["min_relative_volume"])
        return _RuleResult(
            trend and volume_confirmed,
            not warmup and not trend,
            warmup,
            {"fast_ema": None if pd.isna(fast) else float(fast), "slow_ema": None if pd.isna(slow) else float(slow), "relative_volume": None if pd.isna(relative_volume) else float(relative_volume)},
        )


__all__ = [
    "AdxDirectionalTrendStrategy",
    "AtrVolatilityBreakoutStrategy",
    "BollingerMeanReversionStrategy",
    "DualEmaTrendStrategy",
    "KeltnerBreakoutStrategy",
    "LinearRegressionTrendStrategy",
    "MacdTrendStrategy",
    "RsiMeanReversionStrategy",
    "StochasticMeanReversionStrategy",
    "VolatilityAdjustedMomentumStrategy",
    "VolumeConfirmedEmaTrendStrategy",
]
