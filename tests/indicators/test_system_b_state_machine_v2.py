from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from qrp_atlas.contracts import (
    ACTUAL_PAIR_CONTIGUOUS,
    ASSET_ID,
    CLOSE,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    DIAGNOSTICS,
    IS_TRADING_DAY,
    LATEST_ACTUAL_CLOSE,
    LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    LATEST_ACTUAL_MA5,
    LATEST_ACTUAL_MA5_WINDOW_COMPLETE,
    LATEST_ACTUAL_TRADE_DATE,
    LIFECYCLE_STATE,
    LISTING_TRADING_DAY_NUMBER,
    LISTING_TRADING_DAY_NUMBER_IS_EXACT,
    MA5,
    MA5_WINDOW_COMPLETE,
    MARKET_FACT_STATUS,
    PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    PREVIOUS_ACTUAL_MA5_WINDOW_COMPLETE,
    PREVIOUS_ACTUAL_TRADE_DATE,
    PREVIOUS_TREND_STATE,
    STATE_BASIS_SEQUENCE_INTACT,
    STATE_CHANGED,
    SYSTEM_B_2_0_PARAMETER_SET_ID,
    SYSTEM_B_2_0_PARAMETERS,
    SYSTEM_B_2_0_RULE_VERSION_SET_ID,
    SYSTEM_B_STATE_OUTPUT_COLUMNS,
    TRADE_DATE,
    TREND_STATE,
    PriceAdjustment,
    SystemBLifecycleState,
    SystemBMarketFactStatus,
    SystemBStateMachineParameters,
    SystemBStateMachineRequest,
    SystemBTrendState,
)
from qrp_atlas.indicators.system_b import calculate_system_b_2_0_states
from qrp_atlas.indicators.system_b.state_machine_v2 import SystemBStateMachineError
from qrp_atlas.indicators.system_b.detector import detect_system_b_basic_state
from qrp_atlas.indicators.stock.trend import calculate_stock_trend


def _row(
    day: int,
    *,
    asset_id: str = "A",
    listing_day: int | None = None,
    status: SystemBMarketFactStatus = SystemBMarketFactStatus.ACTUAL_TRADING,
    latest_above: bool | None = False,
    previous_above: bool | None = False,
    basis_intact: bool = True,
    pair_contiguous: bool = True,
    listing_day_exact: bool = True,
    latest_ma5_complete: bool | None = None,
    previous_ma5_complete: bool | None = None,
) -> dict[str, object]:
    trade_date = pd.Timestamp("2026-01-01") + pd.Timedelta(days=day - 1)
    actual = status is SystemBMarketFactStatus.ACTUAL_TRADING
    latest_date = trade_date if actual else trade_date - pd.Timedelta(days=1)
    effective_listing_day = listing_day if listing_day is not None else day
    latest_complete = (
        latest_ma5_complete
        if latest_ma5_complete is not None
        else latest_above is not None and effective_listing_day >= 5
    )
    previous_complete = (
        previous_ma5_complete
        if previous_ma5_complete is not None
        else previous_above is not None and effective_listing_day >= 6
    )
    effective_latest_above = latest_above if latest_complete else None
    effective_previous_above = previous_above if previous_complete else None
    return {
        ASSET_ID: asset_id,
        TRADE_DATE: trade_date,
        MARKET_FACT_STATUS: status.value,
        IS_TRADING_DAY: actual,
        LISTING_TRADING_DAY_NUMBER: effective_listing_day if listing_day_exact else None,
        CONFIRMED_LISTING_TRADING_DAY_COUNT: effective_listing_day,
        LISTING_TRADING_DAY_NUMBER_IS_EXACT: listing_day_exact,
        CLOSE: 11.0 if actual and latest_above is True else (9.0 if actual else None),
        MA5: 10.0 if actual and latest_complete else None,
        MA5_WINDOW_COMPLETE: actual and latest_complete,
        LATEST_ACTUAL_TRADE_DATE: latest_date if latest_above is not None else None,
        LATEST_ACTUAL_CLOSE: 11.0 if latest_above is True else (9.0 if latest_above is False else None),
        LATEST_ACTUAL_MA5: 10.0 if latest_complete else None,
        LATEST_ACTUAL_MA5_WINDOW_COMPLETE: latest_complete,
        LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5: effective_latest_above,
        PREVIOUS_ACTUAL_TRADE_DATE: latest_date - pd.Timedelta(days=1) if previous_above is not None else None,
        PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5: effective_previous_above,
        PREVIOUS_ACTUAL_MA5_WINDOW_COMPLETE: previous_complete,
        STATE_BASIS_SEQUENCE_INTACT: basis_intact,
        ACTUAL_PAIR_CONTIGUOUS: pair_contiguous,
    }


def _request(frame: pd.DataFrame, *, parameters=SYSTEM_B_2_0_PARAMETERS) -> SystemBStateMachineRequest:
    return SystemBStateMachineRequest(
        observations=frame,
        parameters=parameters,
        input_price_adjustment=PriceAdjustment.FORWARD_ADJUSTED,
        rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
        parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
    )


def test_warmup_is_lifecycle_and_trend_is_null() -> None:
    result = calculate_system_b_2_0_states(_request(pd.DataFrame([_row(10, latest_above=True)])))
    row = result.frame.iloc[0]
    assert row[LIFECYCLE_STATE] == SystemBLifecycleState.NEW_LISTING_WARMUP.value
    assert pd.isna(row[TREND_STATE])
    assert row[DIAGNOSTICS] == ("NEW_LISTING_WARMUP",)
    assert pd.isna(row[STATE_CHANGED])


def test_uncertain_listing_day_does_not_leave_warmup_early() -> None:
    result = calculate_system_b_2_0_states(
        _request(
            pd.DataFrame(
                [_row(8, listing_day=8, listing_day_exact=False, latest_above=False)]
            )
        )
    ).frame.iloc[0]
    assert pd.isna(result[LISTING_TRADING_DAY_NUMBER])
    assert pd.isna(result[LIFECYCLE_STATE])
    assert pd.isna(result[TREND_STATE])
    assert result[DIAGNOSTICS] == ("UNCERTAIN_LISTING_TRADING_DAY_NUMBER",)


def test_inconsistent_ma5_window_proof_is_rejected() -> None:
    row = _row(11, latest_above=True, previous_above=True)
    row[MA5_WINDOW_COMPLETE] = False
    with pytest.raises(SystemBStateMachineError) as exc_info:
        calculate_system_b_2_0_states(_request(pd.DataFrame([row])))
    assert exc_info.value.code == "INCONSISTENT_MA5_WINDOW_FACT"


@pytest.mark.parametrize(
    ("latest_above", "previous_above", "expected"),
    [
        (False, True, SystemBTrendState.BASE.value),
        (True, False, SystemBTrendState.CANDIDATE.value),
        (True, True, SystemBTrendState.ACTIVE.value),
    ],
)
def test_three_states_are_independent_fact_predicates(
    latest_above: bool,
    previous_above: bool,
    expected: str,
) -> None:
    result = calculate_system_b_2_0_states(
        _request(pd.DataFrame([_row(11, latest_above=latest_above, previous_above=previous_above)]))
    )
    assert result.frame.loc[0, TREND_STATE] == expected


def test_day_11_uses_day_10_facts_without_base_initialization() -> None:
    candidate = calculate_system_b_2_0_states(
        _request(pd.DataFrame([_row(11, latest_above=True, previous_above=False)]))
    )
    active = calculate_system_b_2_0_states(
        _request(pd.DataFrame([_row(11, latest_above=True, previous_above=True)]))
    )
    assert candidate.frame.loc[0, TREND_STATE] == SystemBTrendState.CANDIDATE.value
    assert active.frame.loc[0, TREND_STATE] == SystemBTrendState.ACTIVE.value


def test_online_without_previous_actual_fact_is_null() -> None:
    result = calculate_system_b_2_0_states(
        _request(pd.DataFrame([_row(11, latest_above=True, previous_above=None, pair_contiguous=False)]))
    )
    assert pd.isna(result.frame.loc[0, TREND_STATE])
    assert result.frame.loc[0, DIAGNOSTICS] == ("MISSING_PREVIOUS_ACTUAL_TRADING_FACT",)


def test_unresolved_gap_breaks_then_facts_recover_without_state_carry() -> None:
    frame = pd.DataFrame(
        [
            _row(11, latest_above=True, previous_above=True),
            _row(12, listing_day=11, status=SystemBMarketFactStatus.UNRESOLVED_MISSING, latest_above=True, previous_above=True, basis_intact=False),
            _row(13, listing_day=12, latest_above=True, previous_above=True, pair_contiguous=False),
            _row(14, listing_day=13, latest_above=True, previous_above=True),
            _row(15, listing_day=14, latest_above=False, previous_above=True),
            _row(16, listing_day=15, latest_above=True, previous_above=False),
        ]
    )
    result = calculate_system_b_2_0_states(_request(frame)).frame
    assert result.loc[0, TREND_STATE] == "ACTIVE"
    assert result.loc[1:2, TREND_STATE].isna().all()
    assert result.loc[3:, TREND_STATE].tolist() == ["ACTIVE", "BASE", "CANDIDATE"]
    assert result.loc[1, DIAGNOSTICS] == ("BROKEN_TRADING_SEQUENCE",)
    assert result.loc[2, DIAGNOSTICS] == ("BROKEN_TRADING_SEQUENCE",)


def test_explicit_non_trading_derives_from_latest_actual_facts() -> None:
    result = calculate_system_b_2_0_states(
        _request(pd.DataFrame([_row(12, listing_day=11, status=SystemBMarketFactStatus.EXPLICIT_NON_TRADING, latest_above=True, previous_above=True)]))
    ).frame.iloc[0]
    assert result[TREND_STATE] == "ACTIVE"
    assert result[DIAGNOSTICS] == ("NON_TRADING_DAY_FACT_DERIVED",)
    assert result[IS_TRADING_DAY] is False or not bool(result[IS_TRADING_DAY])


def test_audit_comparison_is_nullable_and_never_drives_state() -> None:
    frame = pd.DataFrame([
        _row(11, latest_above=True, previous_above=None, pair_contiguous=False),
        _row(12, latest_above=False, previous_above=True),
        _row(13, latest_above=True, previous_above=False),
        _row(14, latest_above=True, previous_above=True),
    ])
    result = calculate_system_b_2_0_states(_request(frame)).frame
    assert result.loc[:1, PREVIOUS_TREND_STATE].isna().all()
    assert result.loc[2:, PREVIOUS_TREND_STATE].tolist() == ["BASE", "CANDIDATE"]
    assert result.loc[:1, STATE_CHANGED].isna().all()
    assert result.loc[2:, STATE_CHANGED].tolist() == [True, True]


def test_single_date_and_full_history_have_same_fact_derived_result() -> None:
    rows = [_row(day, latest_above=(day % 3 != 0), previous_above=(day % 3 == 2)) for day in range(1, 16)]
    full = calculate_system_b_2_0_states(_request(pd.DataFrame(rows))).frame
    for index in (10, 11, 13, 14):
        single = calculate_system_b_2_0_states(_request(pd.DataFrame([rows[index]]))).frame.iloc[0]
        expected = full.iloc[index]
        for column in (
            TREND_STATE, LIFECYCLE_STATE, MARKET_FACT_STATUS,
            LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
            PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
            STATE_BASIS_SEQUENCE_INTACT, ACTUAL_PAIR_CONTIGUOUS, DIAGNOSTICS,
        ):
            assert single[column] == expected[column]


def test_multiple_assets_are_isolated_and_input_is_sorted() -> None:
    frame = pd.DataFrame([_row(11, asset_id="B", latest_above=True, previous_above=True), _row(11, asset_id="A", latest_above=False)])
    result = calculate_system_b_2_0_states(_request(frame))
    assert result.frame[ASSET_ID].tolist() == ["A", "B"]
    assert result.diagnostics == ("INPUT_SORTED_BY_ASSET_AND_DATE",)


def test_output_contract_allows_null_trend_and_state_changed() -> None:
    result = calculate_system_b_2_0_states(_request(pd.DataFrame([_row(1)]))).frame
    assert tuple(result.columns) == SYSTEM_B_STATE_OUTPUT_COLUMNS
    assert pd.isna(result.loc[0, TREND_STATE])
    assert pd.isna(result.loc[0, STATE_CHANGED])


def test_unapproved_parameters_are_rejected() -> None:
    wrong = SystemBStateMachineParameters(
        price_adjustment=PriceAdjustment.FORWARD_ADJUSTED,
        warmup_trading_days=9,
        ma_period=5,
    )
    with pytest.raises(SystemBStateMachineError) as exc_info:
        calculate_system_b_2_0_states(_request(pd.DataFrame([_row(1)]), parameters=wrong))
    assert exc_info.value.code == "UNSUPPORTED_PARAMETER_SET"


def test_same_input_is_deterministic() -> None:
    frame = pd.DataFrame([_row(day) for day in range(1, 13)])
    first = calculate_system_b_2_0_states(_request(frame))
    second = calculate_system_b_2_0_states(_request(frame.copy()))
    assert_frame_equal(first.frame, second.frame)
    assert first.metadata == second.metadata


def test_legacy_detector_remains_separate() -> None:
    price_frame = pd.DataFrame([
        {"ticker": "A", TRADE_DATE: f"2026-01-{day:02d}", CLOSE: close}
        for day, close in enumerate([10, 10, 10, 10, 10, 11, 12], start=1)
    ])
    assert bool(detect_system_b_basic_state(calculate_stock_trend(price_frame)).iloc[0]["system_b_trend_valid"])


def test_implementation_has_no_state_recursion_or_default_base() -> None:
    source = (Path(__file__).parents[2] / "src/qrp_atlas/indicators/system_b/state_machine_v2.py").read_text(encoding="utf-8")
    assert "_next_normal_state" not in source
    assert "underlying_trend_state" not in source
    assert "initial_states" not in source
    assert "previous_state is SystemBTrendState" not in source
