from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import PHASE, TICKER, TRADE_DATE, V_TRIGGERED
from qrp_atlas.strategies import (
    StrategyAuthorization,
    StrategyInput,
    StrategyInputScope,
    StrategyRunResult,
    StrategyValidationError,
    get_strategy,
    run_strategy,
)
from qrp_atlas.strategies.builtin.system_b_authorization import SystemBAuthorizationStrategy


def _market_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {TRADE_DATE: "2024-01-04", PHASE: "B", V_TRIGGERED: True},
            {TRADE_DATE: "2024-01-02", PHASE: "B", V_TRIGGERED: False},
            {TRADE_DATE: "2024-01-01", PHASE: "A", V_TRIGGERED: False},
            {TRADE_DATE: "2024-01-03", PHASE: "C", V_TRIGGERED: False},
            {TRADE_DATE: "2024-01-05", PHASE: "UNRESOLVED", V_TRIGGERED: False},
        ]
    )


def test_registry_discovers_system_b_authorization() -> None:
    strategy = get_strategy("system_b_authorization", "1.0.0")
    assert isinstance(strategy, SystemBAuthorizationStrategy)
    definition = strategy.definition
    assert definition.code == "system_b_authorization"
    assert definition.version == "1.0.0"
    assert definition.input_scope is StrategyInputScope.MARKET
    assert definition.required_fields == (TRADE_DATE, PHASE, V_TRIGGERED)
    assert definition.required_indicators == ()

    payload = definition.to_dict()
    assert payload["input_scope"] == "MARKET"
    assert payload["code"] == "system_b_authorization"


def test_market_scope_does_not_require_ticker() -> None:
    df = _market_frame()
    assert TICKER not in df.columns
    result = run_strategy("system_b_authorization", StrategyInput(df))
    assert isinstance(result, StrategyRunResult)
    assert len(result.authorizations) == 5


def test_market_scope_ignores_extra_ticker_column() -> None:
    df = _market_frame().assign(ticker="000001.SZ")
    result = run_strategy("system_b_authorization", StrategyInput(df))
    assert len(result.authorizations) == 5


def test_asset_scope_still_requires_ticker() -> None:
    from qrp_atlas.strategies import StrategyDefinition, StrategyType
    from qrp_atlas.strategies.validation import validate_strategy_input

    dummy_definition = StrategyDefinition(
        code="dummy_asset",
        name="dummy",
        version="1.0.0",
        description="dummy",
        strategy_type=StrategyType.BUILTIN,
        input_scope=StrategyInputScope.ASSET,
        required_fields=(TRADE_DATE,),
        required_indicators=(),
    )
    df_without_ticker = pd.DataFrame([{TRADE_DATE: "2024-01-01"}])
    with pytest.raises(StrategyValidationError, match="identity fields"):
        validate_strategy_input(dummy_definition, StrategyInput(df_without_ticker))

    # Also verify existing asset-level strategy rejects input without ticker
    with pytest.raises(StrategyValidationError, match="missing required columns"):
        run_strategy(
            "system_b_basic",
            StrategyInput(
                pd.DataFrame(
                    [
                        {TRADE_DATE: "2024-01-01", "system_b_trend_valid": True, "system_b_exit_triggered": False},
                    ]
                )
            ),
        )


def test_market_scope_rejects_duplicate_trade_dates() -> None:
    duplicate_df = pd.DataFrame(
        [
            {TRADE_DATE: "2024-01-01", PHASE: "A", V_TRIGGERED: False},
            {TRADE_DATE: "2024-01-01", PHASE: "B", V_TRIGGERED: False},
        ]
    )
    with pytest.raises(StrategyValidationError, match="duplicate trade_date rows"):
        run_strategy("system_b_authorization", StrategyInput(duplicate_df))


def test_market_scope_canonicalizes_before_duplicate_check() -> None:
    # 2024-01-01 and 2024/01/01 represent the same date and must be rejected as duplicate
    df = pd.DataFrame(
        [
            {TRADE_DATE: "2024-01-01", PHASE: "A", V_TRIGGERED: False},
            {TRADE_DATE: "2024/01/01", PHASE: "B", V_TRIGGERED: False},
        ]
    )
    with pytest.raises(StrategyValidationError, match="duplicate trade_date rows"):
        run_strategy("system_b_authorization", StrategyInput(df))


def test_asset_scope_canonicalizes_before_duplicate_check() -> None:
    from qrp_atlas.indicators.system_b.detector import (
        SYSTEM_B_EXIT_TRIGGERED,
        SYSTEM_B_TREND_VALID,
    )

    # Same ticker with dates '2024-01-01' and '2024/01/01' must be rejected as duplicate
    df = pd.DataFrame(
        [
            {TICKER: "000001.SZ", TRADE_DATE: "2024-01-01", SYSTEM_B_TREND_VALID: False, SYSTEM_B_EXIT_TRIGGERED: False},
            {TICKER: "000001.SZ", TRADE_DATE: "2024/01/01", SYSTEM_B_TREND_VALID: True, SYSTEM_B_EXIT_TRIGGERED: False},
        ]
    )
    with pytest.raises(StrategyValidationError, match="duplicate \\(ticker, trade_date\\) rows"):
        run_strategy("system_b_basic", StrategyInput(df))


def test_authorization_all_phases_and_reasons() -> None:
    df = pd.DataFrame(
        [
            {TRADE_DATE: "2024-01-01", PHASE: "A", V_TRIGGERED: False},
            {TRADE_DATE: "2024-01-02", PHASE: "B", V_TRIGGERED: False},
            {TRADE_DATE: "2024-01-03", PHASE: "C", V_TRIGGERED: False},
            {TRADE_DATE: "2024-01-04", PHASE: "UNRESOLVED", V_TRIGGERED: False},
        ]
    )
    result = run_strategy("system_b_authorization", StrategyInput(df))

    auths = {auth.trade_date: auth for auth in result.authorizations}

    # Phase A: not authorized
    assert auths["2024-01-01"].is_authorized is False
    assert auths["2024-01-01"].reason_codes == ("PHASE_A_NOT_AUTHORIZED",)
    assert auths["2024-01-01"].authorization_type == "NEW_POSITION"

    # Phase B: authorized
    assert auths["2024-01-02"].is_authorized is True
    assert auths["2024-01-02"].reason_codes == ("PHASE_B_AUTHORIZED",)

    # Phase C: not authorized
    assert auths["2024-01-03"].is_authorized is False
    assert auths["2024-01-03"].reason_codes == ("PHASE_C_NOT_AUTHORIZED",)

    # UNRESOLVED: not authorized
    assert auths["2024-01-04"].is_authorized is False
    assert auths["2024-01-04"].reason_codes == ("PHASE_UNRESOLVED",)


def test_v_rule_revokes_phase_b_authorization() -> None:
    df = pd.DataFrame(
        [
            {TRADE_DATE: "2024-01-01", PHASE: "B", V_TRIGGERED: True},
            {TRADE_DATE: "2024-01-02", PHASE: "A", V_TRIGGERED: True},
        ]
    )
    result = run_strategy("system_b_authorization", StrategyInput(df))

    # Even in Phase B, v_triggered=True revokes authorization
    b_auth = result.authorizations[0]
    assert b_auth.trade_date == "2024-01-01"
    assert b_auth.is_authorized is False
    assert b_auth.reason_codes == ("V_RULE_REVOKED",)
    assert b_auth.evidence == {
        "market_phase": "B",
        V_TRIGGERED: True,
        "semantic_owner": "SYSTEM_B",
        "delivery_mode": "BUILTIN",
        "capability_type": "STRATEGY",
    }

    # In Phase A, v_triggered=True also yields V_RULE_REVOKED
    a_auth = result.authorizations[1]
    assert a_auth.is_authorized is False
    assert a_auth.reason_codes == ("V_RULE_REVOKED",)


def test_authorization_never_generates_strategy_decisions() -> None:
    result = run_strategy("system_b_authorization", StrategyInput(_market_frame()))
    assert result.decisions == ()


def test_empty_input_returns_empty_authorizations() -> None:
    empty_df = _market_frame().iloc[:0]
    result = run_strategy("system_b_authorization", StrategyInput(empty_df))
    assert result.authorizations == ()
    assert result.decisions == ()


@pytest.mark.parametrize(
    "invalid_phase",
    ["D", "INVALID", "", "b", "a", 123],
)
def test_rejects_invalid_phase(invalid_phase: object) -> None:
    df = pd.DataFrame(
        [
            {TRADE_DATE: "2024-01-01", PHASE: invalid_phase, V_TRIGGERED: False},
        ]
    )
    with pytest.raises(StrategyValidationError, match="invalid market phase"):
        run_strategy("system_b_authorization", StrategyInput(df))


@pytest.mark.parametrize(
    "invalid_v",
    [1, 0, "true", "True", "false", 1.0, 0.0],
)
def test_rejects_non_boolean_v_triggered(invalid_v: object) -> None:
    df = pd.DataFrame(
        [
            {TRADE_DATE: "2024-01-01", PHASE: "B", V_TRIGGERED: invalid_v},
        ]
    )
    with pytest.raises(StrategyValidationError, match="must be a boolean"):
        run_strategy("system_b_authorization", StrategyInput(df))


def test_rejects_missing_v_triggered_values() -> None:
    df = pd.DataFrame(
        [
            {TRADE_DATE: "2024-01-01", PHASE: "B", V_TRIGGERED: np.nan},
        ]
    )
    with pytest.raises(StrategyValidationError, match="missing values"):
        run_strategy("system_b_authorization", StrategyInput(df))


def test_rejects_invalid_trade_date() -> None:
    df = pd.DataFrame(
        [
            {TRADE_DATE: "not-a-date", PHASE: "B", V_TRIGGERED: False},
        ]
    )
    with pytest.raises(StrategyValidationError, match="invalid trade_date values"):
        run_strategy("system_b_authorization", StrategyInput(df))


def test_identical_input_is_deterministic_and_sorted() -> None:
    input_df = _market_frame()
    first_run = run_strategy("system_b_authorization", StrategyInput(input_df))
    second_run = run_strategy("system_b_authorization", StrategyInput(input_df))

    assert first_run.to_dict() == second_run.to_dict()

    dates = [auth.trade_date for auth in first_run.authorizations]
    assert dates == sorted(dates)


def test_strategy_authorization_dataclass_serialization() -> None:
    auth = StrategyAuthorization(
        trade_date="2024-01-02",
        strategy_code="system_b_authorization",
        strategy_version="1.0.0",
        authorization_type="NEW_POSITION",
        is_authorized=True,
        reason_codes=("PHASE_B_AUTHORIZED",),
        evidence={"market_phase": "B", V_TRIGGERED: False},
    )
    payload = auth.to_dict()
    assert payload == {
        "trade_date": "2024-01-02",
        "strategy_code": "system_b_authorization",
        "strategy_version": "1.0.0",
        "authorization_type": "NEW_POSITION",
        "is_authorized": True,
        "reason_codes": ["PHASE_B_AUTHORIZED"],
        "evidence": {"market_phase": "B", V_TRIGGERED: False},
    }
