"""System B market-level new position authorization strategy."""

from __future__ import annotations

import numpy as np

from qrp_atlas.contracts import PHASE, TRADE_DATE, V_TRIGGERED_LOWER

from ..models import (
    StrategyAuthorization,
    StrategyDefinition,
    StrategyInput,
    StrategyInputScope,
    StrategyRunResult,
    StrategyType,
)
from ..validation import (
    StrategyValidationError,
    resolve_parameters,
    validate_definition,
    validate_strategy_input,
)

VALID_PHASES = frozenset({"A", "B", "C", "UNRESOLVED"})


class SystemBAuthorizationStrategy:
    """Determine whether System B allows new positions on a given trade date."""

    definition = StrategyDefinition(
        code="system_b_authorization",
        name="System B Authorization",
        version="1.0.0",
        description="System B market-level new position authorization strategy.",
        strategy_type=StrategyType.BUILTIN,
        input_scope=StrategyInputScope.MARKET,
        required_fields=(TRADE_DATE, PHASE, V_TRIGGERED_LOWER),
        required_indicators=(),
    )

    def __init__(self) -> None:
        validate_definition(self.definition)

    def run(self, strategy_input: StrategyInput) -> StrategyRunResult:
        prepared = validate_strategy_input(self.definition, strategy_input)
        parameters = resolve_parameters(self.definition, strategy_input.parameters)

        if prepared.empty:
            return StrategyRunResult(
                definition=self.definition,
                parameters=parameters,
                decisions=(),
                authorizations=(),
            )

        authorizations: list[StrategyAuthorization] = []
        for row in prepared.itertuples(index=False):
            trade_date = str(getattr(row, TRADE_DATE))
            phase = getattr(row, PHASE)
            v_val = getattr(row, V_TRIGGERED_LOWER)

            if not isinstance(phase, str) or phase not in VALID_PHASES:
                raise StrategyValidationError(
                    f"invalid market phase: {phase!r}, must be one of {sorted(VALID_PHASES)}"
                )

            if not isinstance(v_val, (bool, np.bool_)):
                raise StrategyValidationError(
                    f"v_triggered must be a boolean, got {type(v_val).__name__} ({v_val!r})"
                )
            v_triggered = bool(v_val)

            if v_triggered:
                is_authorized = False
                reason_code = "V_RULE_REVOKED"
            elif phase == "B":
                is_authorized = True
                reason_code = "PHASE_B_AUTHORIZED"
            elif phase == "A":
                is_authorized = False
                reason_code = "PHASE_A_NOT_AUTHORIZED"
            elif phase == "C":
                is_authorized = False
                reason_code = "PHASE_C_NOT_AUTHORIZED"
            elif phase == "UNRESOLVED":
                is_authorized = False
                reason_code = "PHASE_UNRESOLVED"
            else:
                raise StrategyValidationError(f"unhandled phase: {phase!r}")

            evidence = {
                "market_phase": phase,
                "v_triggered": v_triggered,
                "semantic_owner": "SYSTEM_B",
                "delivery_mode": "BUILTIN",
                "capability_type": "STRATEGY",
            }

            authorizations.append(
                StrategyAuthorization(
                    trade_date=trade_date,
                    strategy_code=self.definition.code,
                    strategy_version=self.definition.version,
                    authorization_type="NEW_POSITION",
                    is_authorized=is_authorized,
                    reason_codes=(reason_code,),
                    evidence=evidence,
                )
            )

        return StrategyRunResult(
            definition=self.definition,
            parameters=parameters,
            decisions=(),
            authorizations=tuple(authorizations),
        )
