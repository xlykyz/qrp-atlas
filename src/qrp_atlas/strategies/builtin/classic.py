"""Classic deterministic long-only strategies using prepared indicator aliases."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qrp_atlas.contracts import CLOSE, TICKER, TRADE_DATE
from qrp_atlas.indicators import IndicatorParameterBinding, IndicatorRequest

from ..models import (
    ParameterSpec,
    StrategyAction,
    StrategyDecision,
    StrategyDefinition,
    StrategyInput,
    StrategyRunResult,
    StrategyType,
)
from ..validation import (
    StrategyValidationError,
    resolve_parameters,
    validate_definition,
    validate_strategy_input,
)


def _integer(default: int, minimum: int = 2) -> ParameterSpec:
    return ParameterSpec("integer", default=default, has_default=True, minimum=minimum, maximum=10000)


def _number(default: float, minimum: float = 0.0) -> ParameterSpec:
    return ParameterSpec("number", default=default, has_default=True, minimum=minimum, maximum=1000.0)


@dataclass(frozen=True)
class _RuleResult:
    enter: bool
    exit: bool
    warmup: bool
    evidence: Mapping[str, Any]


class _ClassicLongOnlyStrategy:
    definition: StrategyDefinition
    enter_reason: str
    exit_reason: str

    def __init__(self) -> None:
        validate_definition(self.definition)

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        return None

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        raise NotImplementedError

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        parameters = resolve_parameters(self.definition, strategy_input.parameters)
        self._validate_relationships(parameters)
        prepared = validate_strategy_input(self.definition, strategy_input)
        positions = dict(strategy_input.initial_positions)
        decisions: list[StrategyDecision] = []

        for row in prepared.itertuples(index=False):
            asset_id = str(getattr(row, TICKER))
            held = positions.get(asset_id, False)
            rules = self._rules(row, parameters)
            if rules.warmup:
                action = StrategyAction.HOLD if held else StrategyAction.NO_ACTION
                reason = "INDICATOR_WARMUP"
            elif not held and rules.enter:
                action = StrategyAction.ENTER
                reason = self.enter_reason
                positions[asset_id] = True
            elif held and rules.exit:
                action = StrategyAction.EXIT
                reason = self.exit_reason
                positions[asset_id] = False
            elif held:
                action = StrategyAction.HOLD
                reason = "POSITION_CONTINUES"
            else:
                action = StrategyAction.NO_ACTION
                reason = "ENTRY_CONDITION_NOT_MET"
            decisions.append(
                StrategyDecision(
                    trade_date=str(getattr(row, TRADE_DATE)),
                    asset_id=asset_id,
                    action=action,
                    direction="long",
                    strategy_code=self.definition.code,
                    strategy_version=self.definition.version,
                    reason_code=reason,
                    evidence=dict(rules.evidence),
                )
            )
        return StrategyRunResult(self.definition, parameters, tuple(decisions))


class TimeSeriesMomentumStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="time_series_momentum",
        name="Time Series Momentum",
        version="1.0.0",
        description="Long when trailing close-to-close return exceeds a threshold; exit below it.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={
            "lookback": _integer(20, 1),
            "threshold": _number(0.0),
        },
        indicator_requests=(
            IndicatorRequest(
                "period_return",
                {"lookback": IndicatorParameterBinding("lookback")},
                alias="momentum",
            ),
        ),
    )
    enter_reason = "MOMENTUM_ABOVE_THRESHOLD"
    exit_reason = "MOMENTUM_BELOW_THRESHOLD"

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        momentum = getattr(row, "momentum")
        warmup = pd.isna(momentum)
        threshold = float(parameters["threshold"])
        return _RuleResult(
            not warmup and float(momentum) > threshold,
            not warmup and float(momentum) <= threshold,
            warmup,
            {"momentum": None if warmup else float(momentum), "threshold": threshold},
        )


class DualSmaTrendStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="dual_sma_trend",
        name="Dual SMA Trend",
        version="1.0.0",
        description="Long while a fast simple moving average is above a slower average.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={"fast_window": _integer(20), "slow_window": _integer(60)},
        indicator_requests=(
            IndicatorRequest("sma", {"window": IndicatorParameterBinding("fast_window")}, alias="fast_sma"),
            IndicatorRequest("sma", {"window": IndicatorParameterBinding("slow_window")}, alias="slow_sma"),
        ),
    )
    enter_reason = "FAST_SMA_ABOVE_SLOW_SMA"
    exit_reason = "FAST_SMA_NOT_ABOVE_SLOW_SMA"

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if parameters["fast_window"] >= parameters["slow_window"]:
            raise StrategyValidationError("fast_window must be less than slow_window")

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        fast, slow = getattr(row, "fast_sma"), getattr(row, "slow_sma")
        warmup = pd.isna(fast) or pd.isna(slow)
        above = not warmup and float(fast) > float(slow)
        return _RuleResult(
            above, not warmup and not above, warmup,
            {"fast_sma": None if pd.isna(fast) else float(fast), "slow_sma": None if pd.isna(slow) else float(slow)},
        )


class DonchianBreakoutStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="donchian_breakout",
        name="Donchian Breakout",
        version="1.0.0",
        description="Enter above the prior high channel and exit below the prior low channel.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE, CLOSE),
        required_indicators=(),
        parameter_schema={"entry_window": _integer(20), "exit_window": _integer(10)},
        indicator_requests=(
            IndicatorRequest("donchian_high", {"window": IndicatorParameterBinding("entry_window")}, alias="entry_channel"),
            IndicatorRequest("donchian_low", {"window": IndicatorParameterBinding("exit_window")}, alias="exit_channel"),
        ),
    )
    enter_reason = "CLOSE_ABOVE_PRIOR_DONCHIAN_HIGH"
    exit_reason = "CLOSE_BELOW_PRIOR_DONCHIAN_LOW"

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        close = float(getattr(row, CLOSE))
        high, low = getattr(row, "entry_channel"), getattr(row, "exit_channel")
        warmup = pd.isna(high) or pd.isna(low)
        return _RuleResult(
            not warmup and close > float(high),
            not warmup and close < float(low),
            warmup,
            {"close": close, "entry_channel": None if pd.isna(high) else float(high), "exit_channel": None if pd.isna(low) else float(low)},
        )


class RollingZscoreMeanReversionStrategy(_ClassicLongOnlyStrategy):
    definition = StrategyDefinition(
        code="rolling_zscore_mean_reversion",
        name="Rolling Z-Score Mean Reversion",
        version="1.0.0",
        description="Long extreme negative rolling z-scores and exit after mean reversion.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={
            "lookback": _integer(20),
            "entry_z": _number(2.0, 0.01),
            "exit_z": _number(0.0),
        },
        indicator_requests=(
            IndicatorRequest("rolling_zscore", {"window": IndicatorParameterBinding("lookback")}, alias="zscore"),
        ),
    )
    enter_reason = "ZSCORE_BELOW_NEGATIVE_ENTRY"
    exit_reason = "ZSCORE_REVERTED_TO_EXIT"

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if parameters["exit_z"] >= parameters["entry_z"]:
            raise StrategyValidationError("exit_z must be less than entry_z")

    def _rules(self, row: Any, parameters: Mapping[str, Any]) -> _RuleResult:
        zscore = getattr(row, "zscore")
        warmup = pd.isna(zscore)
        entry = -float(parameters["entry_z"])
        exit_level = -float(parameters["exit_z"])
        return _RuleResult(
            not warmup and float(zscore) <= entry,
            not warmup and float(zscore) >= exit_level,
            warmup,
            {"zscore": None if warmup else float(zscore), "entry_level": entry, "exit_level": exit_level},
        )
