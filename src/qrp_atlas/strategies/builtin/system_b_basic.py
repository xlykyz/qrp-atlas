"""Minimal System B strategy used to verify the strategy/backtest boundary."""

from __future__ import annotations

from qrp_atlas.contracts import TICKER, TRADE_DATE
from qrp_atlas.indicators.system_b.detector import (
    SYSTEM_B_EXIT_TRIGGERED,
    SYSTEM_B_TREND_VALID,
)

from ..models import (
    StrategyAction,
    StrategyDecision,
    StrategyDefinition,
    StrategyInput,
    StrategyRunResult,
    StrategyType,
)
from ..validation import resolve_parameters, validate_definition, validate_strategy_input


class SystemBBasicStrategy:
    """Use precomputed System B states; it deliberately never recalculates indicators."""

    definition = StrategyDefinition(
        code="system_b_basic",
        name="System B Basic",
        version="1.0.0",
        description="Minimal state-driven System B strategy for architecture validation.",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(TICKER, TRADE_DATE),
        required_indicators=(SYSTEM_B_TREND_VALID, SYSTEM_B_EXIT_TRIGGERED),
    )

    def __init__(self) -> None:
        validate_definition(self.definition)

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        prepared = validate_strategy_input(self.definition, strategy_input)
        parameters = resolve_parameters(self.definition, strategy_input.parameters)
        positions = dict(strategy_input.initial_positions)
        decisions: list[StrategyDecision] = []

        for row in prepared.itertuples(index=False):
            asset_id = str(getattr(row, TICKER))
            trade_date = str(getattr(row, TRADE_DATE))
            trend_valid = bool(getattr(row, SYSTEM_B_TREND_VALID))
            exit_triggered = bool(getattr(row, SYSTEM_B_EXIT_TRIGGERED))
            held = positions.get(asset_id, False)

            if not held and trend_valid:
                action = StrategyAction.ENTER
                reason_code = "TREND_CONFIRMED"
                positions[asset_id] = True
            elif held and exit_triggered:
                action = StrategyAction.EXIT
                reason_code = "EXIT_TRIGGERED"
                positions[asset_id] = False
            elif held:
                action = StrategyAction.HOLD
                reason_code = "POSITION_CONTINUES"
            else:
                action = StrategyAction.NO_ACTION
                reason_code = "ENTRY_CONDITION_NOT_MET"

            decisions.append(
                StrategyDecision(
                    trade_date=trade_date,
                    asset_id=asset_id,
                    action=action,
                    direction="long",
                    strategy_code=self.definition.code,
                    strategy_version=self.definition.version,
                    reason_code=reason_code,
                    evidence={
                        SYSTEM_B_TREND_VALID: trend_valid,
                        SYSTEM_B_EXIT_TRIGGERED: exit_triggered,
                    },
                )
            )

        return StrategyRunResult(
            definition=self.definition,
            parameters=parameters,
            decisions=tuple(decisions),
        )
