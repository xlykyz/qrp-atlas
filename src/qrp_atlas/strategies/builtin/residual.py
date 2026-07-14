"""Long-only market residual mean-reversion research strategy."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

import pandas as pd

from qrp_atlas.contracts import TICKER, TRADE_DATE
from qrp_atlas.indicators import IndicatorParameterBinding, IndicatorRequest
from qrp_atlas.indicators.stock.residual import (
    BENCHMARK_ID,
    DIAGNOSTIC_CODE,
    REASON_INSUFFICIENT_HISTORY as DIAG_INSUFFICIENT_HISTORY,
    REASON_MISSING_BENCHMARK as DIAG_MISSING_BENCHMARK,
    REASON_MISSING_CURRENT_RETURN as DIAG_MISSING_CURRENT_RETURN,
    REASON_NON_FINITE_INPUT as DIAG_NON_FINITE_INPUT,
    REASON_OK as DIAG_OK,
    REASON_RANK_DEFICIENT as DIAG_RANK_DEFICIENT,
    REASON_ZERO_BENCHMARK_VARIANCE as DIAG_ZERO_BENCHMARK_VARIANCE,
    RESIDUAL_RETURN,
    RESIDUAL_ZSCORE,
    ROLLING_ALPHA,
    ROLLING_BETA,
    ROLLING_R2,
)

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

STRATEGY_CODE = "market_residual_mean_reversion"
STRATEGY_VERSION = "1.0.0"

REASON_RESIDUAL_EXTREME_ENTRY = "RESIDUAL_EXTREME_ENTRY"
REASON_MEAN_REVERSION_EXIT = "MEAN_REVERSION_EXIT"
REASON_MAX_HOLD_EXIT = "MAX_HOLD_EXIT"
REASON_RELATIONSHIP_INVALID = "RELATIONSHIP_INVALID"
REASON_INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
REASON_MISSING_BENCHMARK = "MISSING_BENCHMARK"
REASON_INVALID_INDICATOR = "INVALID_INDICATOR"
REASON_POSITION_CONTINUES = "POSITION_CONTINUES"
REASON_ENTRY_CONDITION_NOT_MET = "ENTRY_CONDITION_NOT_MET"

_DIAG_TO_REASON = {
    DIAG_INSUFFICIENT_HISTORY: REASON_INSUFFICIENT_HISTORY,
    DIAG_MISSING_BENCHMARK: REASON_MISSING_BENCHMARK,
    DIAG_MISSING_CURRENT_RETURN: REASON_INVALID_INDICATOR,
    DIAG_NON_FINITE_INPUT: REASON_INVALID_INDICATOR,
    DIAG_ZERO_BENCHMARK_VARIANCE: REASON_INVALID_INDICATOR,
    DIAG_RANK_DEFICIENT: REASON_INVALID_INDICATOR,
}


def _integer(default: int, minimum: int = 1, maximum: int = 10000) -> ParameterSpec:
    return ParameterSpec(
        "integer",
        default=default,
        has_default=True,
        minimum=minimum,
        maximum=maximum,
    )


def _number(
    default: float,
    minimum: float | None = None,
    maximum: float | None = None,
) -> ParameterSpec:
    return ParameterSpec(
        "number",
        default=default,
        has_default=True,
        minimum=minimum,
        maximum=maximum,
    )


def _boolean(default: bool) -> ParameterSpec:
    return ParameterSpec("boolean", default=default, has_default=True)


def _finite_or_none(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _diagnostic_code(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def _reason_from_diagnostic(diagnostic: str | None) -> str | None:
    if diagnostic is None or diagnostic == DIAG_OK:
        return None
    return _DIAG_TO_REASON.get(diagnostic, REASON_INVALID_INDICATOR)


class MarketResidualMeanReversionStrategy:
    """Long residual underperformance; exit on reversion, max hold, or invalid relation.

    Benchmark is used only for residual signal construction. The strategy itself
    is long-only on equities and does not create a short benchmark hedge.
    """

    definition = StrategyDefinition(
        code=STRATEGY_CODE,
        name="Market Residual Mean Reversion",
        version=STRATEGY_VERSION,
        description=(
            "Long-only research strategy that enters extreme negative market "
            "residuals and exits after mean reversion or relationship failure."
        ),
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(),
        parameter_schema={
            "window": _integer(60, 2),
            "min_periods": _integer(60, 2),
            "z_window": _integer(60, 2),
            "fit_intercept": _boolean(True),
            "entry_zscore": _number(-2.0, minimum=-20.0, maximum=0.0),
            "exit_zscore": _number(-0.25, minimum=-20.0, maximum=20.0),
            "min_r2": _number(0.05, minimum=0.0, maximum=1.0),
            "max_hold_days": _integer(20, 1),
        },
        indicator_requests=(
            IndicatorRequest(
                "market_residual",
                {
                    "window": IndicatorParameterBinding("window"),
                    "min_periods": IndicatorParameterBinding("min_periods"),
                    "z_window": IndicatorParameterBinding("z_window"),
                    "fit_intercept": IndicatorParameterBinding("fit_intercept"),
                },
                alias="residual",
                output_fields={
                    ROLLING_ALPHA: ROLLING_ALPHA,
                    ROLLING_BETA: ROLLING_BETA,
                    ROLLING_R2: ROLLING_R2,
                    RESIDUAL_RETURN: RESIDUAL_RETURN,
                    RESIDUAL_ZSCORE: RESIDUAL_ZSCORE,
                },
            ),
        ),
    )

    def __init__(self) -> None:
        validate_definition(self.definition)

    def _validate_relationships(self, parameters: Mapping[str, Any]) -> None:
        if float(parameters["entry_zscore"]) >= float(parameters["exit_zscore"]):
            raise StrategyValidationError("entry_zscore must be strictly less than exit_zscore")
        if int(parameters["min_periods"]) > int(parameters["window"]):
            raise StrategyValidationError("min_periods cannot exceed window")

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        parameters = resolve_parameters(self.definition, strategy_input.parameters)
        self._validate_relationships(parameters)
        prepared = validate_strategy_input(self.definition, strategy_input)

        entry_z = float(parameters["entry_zscore"])
        exit_z = float(parameters["exit_zscore"])
        min_r2 = float(parameters["min_r2"])
        max_hold_days = int(parameters["max_hold_days"])
        benchmark_id = strategy_input.runtime_context.get("benchmark_id")

        positions: dict[str, bool] = {
            str(key): bool(value) for key, value in strategy_input.initial_positions.items()
        }
        hold_days: dict[str, int] = {}
        decisions: list[StrategyDecision] = []
        diagnostics: list[str] = []

        for row in prepared.itertuples(index=False):
            asset_id = str(getattr(row, TICKER))
            trade_date = str(getattr(row, TRADE_DATE))
            held = positions.get(asset_id, False)

            alpha = _finite_or_none(getattr(row, ROLLING_ALPHA, None))
            beta = _finite_or_none(getattr(row, ROLLING_BETA, None))
            r2 = _finite_or_none(getattr(row, ROLLING_R2, None))
            residual = _finite_or_none(getattr(row, RESIDUAL_RETURN, None))
            zscore = _finite_or_none(getattr(row, RESIDUAL_ZSCORE, None))
            diagnostic = _diagnostic_code(getattr(row, DIAGNOSTIC_CODE, None))
            if diagnostic is None and hasattr(row, "diagnostic_code"):
                diagnostic = _diagnostic_code(getattr(row, "diagnostic_code"))

            row_benchmark = getattr(row, BENCHMARK_ID, None) if hasattr(row, BENCHMARK_ID) else None
            evidence_benchmark = (
                None
                if row_benchmark is None
                or (isinstance(row_benchmark, float) and math.isnan(row_benchmark))
                else str(row_benchmark)
            )
            if evidence_benchmark is None and benchmark_id is not None:
                evidence_benchmark = str(benchmark_id)

            indicators_valid = (
                alpha is not None
                and beta is not None
                and r2 is not None
                and residual is not None
                and zscore is not None
                and (diagnostic in (None, DIAG_OK))
            )
            relationship_valid = indicators_valid and r2 >= min_r2
            failure_reason = _reason_from_diagnostic(diagnostic)
            if failure_reason is None and not indicators_valid:
                failure_reason = REASON_INVALID_INDICATOR
            if indicators_valid and r2 is not None and r2 < min_r2:
                failure_reason = REASON_RELATIONSHIP_INVALID

            evidence = {
                "signal_date": trade_date,
                "rolling_alpha": alpha,
                "rolling_beta": beta,
                "rolling_r2": r2,
                "residual_return": residual,
                "residual_zscore": zscore,
                "benchmark_id": evidence_benchmark,
                "indicator_diagnostic_code": diagnostic,
                "entry_zscore": entry_z,
                "exit_zscore": exit_z,
                "min_r2": min_r2,
                "max_hold_days": max_hold_days,
                "window": int(parameters["window"]),
                "min_periods": int(parameters["min_periods"]),
                "z_window": int(parameters["z_window"]),
                "fit_intercept": bool(parameters["fit_intercept"]),
                "hold_days": hold_days.get(asset_id, 0),
            }

            if held:
                next_hold = hold_days.get(asset_id, 0) + 1
                hold_days[asset_id] = next_hold
                evidence["hold_days"] = next_hold

                if not relationship_valid:
                    action = StrategyAction.EXIT
                    reason = failure_reason or REASON_INVALID_INDICATOR
                    positions[asset_id] = False
                    hold_days.pop(asset_id, None)
                elif next_hold >= max_hold_days:
                    action = StrategyAction.EXIT
                    reason = REASON_MAX_HOLD_EXIT
                    positions[asset_id] = False
                    hold_days.pop(asset_id, None)
                elif zscore is not None and zscore >= exit_z:
                    action = StrategyAction.EXIT
                    reason = REASON_MEAN_REVERSION_EXIT
                    positions[asset_id] = False
                    hold_days.pop(asset_id, None)
                else:
                    action = StrategyAction.HOLD
                    reason = REASON_POSITION_CONTINUES
            else:
                if not relationship_valid:
                    action = StrategyAction.NO_ACTION
                    reason = failure_reason or REASON_ENTRY_CONDITION_NOT_MET
                    if reason in {
                        REASON_INSUFFICIENT_HISTORY,
                        REASON_MISSING_BENCHMARK,
                        REASON_INVALID_INDICATOR,
                        REASON_RELATIONSHIP_INVALID,
                    }:
                        diagnostics.append(f"{asset_id}|{trade_date}|{reason}")
                elif zscore is not None and zscore <= entry_z:
                    action = StrategyAction.ENTER
                    reason = REASON_RESIDUAL_EXTREME_ENTRY
                    positions[asset_id] = True
                    hold_days[asset_id] = 0
                    evidence["hold_days"] = 0
                else:
                    action = StrategyAction.NO_ACTION
                    reason = REASON_ENTRY_CONDITION_NOT_MET

            decisions.append(
                StrategyDecision(
                    trade_date=trade_date,
                    asset_id=asset_id,
                    action=action,
                    direction="long",
                    strategy_code=self.definition.code,
                    strategy_version=self.definition.version,
                    reason_code=reason,
                    score=zscore,
                    evidence=evidence,
                )
            )

        return StrategyRunResult(
            definition=self.definition,
            parameters=parameters,
            decisions=tuple(decisions),
            diagnostics=tuple(diagnostics),
        )


__all__ = [
    "MarketResidualMeanReversionStrategy",
    "REASON_ENTRY_CONDITION_NOT_MET",
    "REASON_INSUFFICIENT_HISTORY",
    "REASON_INVALID_INDICATOR",
    "REASON_MAX_HOLD_EXIT",
    "REASON_MEAN_REVERSION_EXIT",
    "REASON_MISSING_BENCHMARK",
    "REASON_POSITION_CONTINUES",
    "REASON_RELATIONSHIP_INVALID",
    "REASON_RESIDUAL_EXTREME_ENTRY",
    "STRATEGY_CODE",
    "STRATEGY_VERSION",
]
