from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from qrp_atlas.contracts import (
    ASSET_ID,
    CLOSE,
    CONSECUTIVE_ABOVE_MA5_DAYS,
    CONSECUTIVE_BELOW_MA5_DAYS,
    DIAGNOSTICS,
    IS_ABOVE_OR_EQUAL_MA5,
    IS_TRADING_DAY,
    LISTING_TRADING_DAY_NUMBER,
    MA5,
    PARAMETER_SET_ID,
    PREVIOUS_TREND_STATE,
    PRICE_ADJUSTMENT,
    RULE_VERSION_SET_ID,
    SOURCE_RULE_IDS,
    STATE_CHANGED,
    SYSTEM_B_2_0_PARAMETER_SET_ID,
    SYSTEM_B_2_0_PARAMETERS,
    SYSTEM_B_2_0_RULE_VERSION_SET_ID,
    SYSTEM_B_2_0_SOURCE_RULE_IDS,
    SYSTEM_B_STATE_OUTPUT_COLUMNS,
    TRADE_DATE,
    TREND_STATE,
    UNDERLYING_TREND_STATE,
    PriceAdjustment,
    SystemBStateCheckpoint,
    SystemBStateMachineParameters,
    SystemBStateMachineRequest,
    SystemBTrendState,
)
from qrp_atlas.indicators.stock import calculate_stock_trend
from qrp_atlas.indicators.system_b import (
    DIAGNOSTIC_INPUT_SORTED,
    DIAGNOSTIC_NON_TRADING_DAY,
    SystemBStateMachineError,
    calculate_system_b_2_0_states,
    detect_system_b_basic_state,
)
from qrp_atlas.indicators.system_b.detector import SYSTEM_B_TREND_VALID


def _request(
    observations: pd.DataFrame,
    *,
    initial_states: tuple[SystemBStateCheckpoint, ...] = (),
    parameters: SystemBStateMachineParameters = SYSTEM_B_2_0_PARAMETERS,
    input_price_adjustment: PriceAdjustment = PriceAdjustment.FORWARD_ADJUSTED,
) -> SystemBStateMachineRequest:
    return SystemBStateMachineRequest(
        observations=observations,
        parameters=parameters,
        input_price_adjustment=input_price_adjustment,
        rule_version_set_id=SYSTEM_B_2_0_RULE_VERSION_SET_ID,
        parameter_set_id=SYSTEM_B_2_0_PARAMETER_SET_ID,
        initial_states=initial_states,
    )


def _row(
    asset_id: str,
    trade_date: object,
    listing_day: int,
    *,
    above: bool = True,
    is_trading_day: bool = True,
) -> dict[str, object]:
    return {
        ASSET_ID: asset_id,
        TRADE_DATE: trade_date,
        IS_TRADING_DAY: is_trading_day,
        LISTING_TRADING_DAY_NUMBER: listing_day,
        CLOSE: 10.0 if above else 9.0,
        MA5: 10.0,
    }


def _checkpoint(
    state: SystemBTrendState,
    *,
    asset_id: str = "A",
    listing_day: int = 20,
    above_days: int = 0,
    below_days: int = 0,
    last_date: str = "2026-01-01",
) -> SystemBStateCheckpoint:
    return SystemBStateCheckpoint(
        asset_id=asset_id,
        last_observation_date=pd.Timestamp(last_date),
        trend_state=state,
        underlying_trend_state=(
            SystemBTrendState.BASE
            if state is SystemBTrendState.NEW_LISTING_WARMUP
            else state
        ),
        listing_trading_day_number=listing_day,
        consecutive_above_ma5_days=above_days,
        consecutive_below_ma5_days=below_days,
    )


def _normal_rows(relations: list[bool], *, asset_id: str = "A", start_day: int = 21) -> pd.DataFrame:
    return pd.DataFrame(
        [
            _row(
                asset_id,
                pd.Timestamp("2026-01-01") + pd.Timedelta(days=index + 1),
                start_day + index,
                above=above,
            )
            for index, above in enumerate(relations)
        ]
    )


def test_listing_days_one_through_ten_are_warmup_and_day_eleven_enters_normal_machine() -> None:
    frame = pd.DataFrame(
        [_row("NEW", f"2026-02-{day:02d}", day, above=True) for day in range(1, 12)]
    )

    result = calculate_system_b_2_0_states(_request(frame))

    assert result.frame[TREND_STATE].tolist() == [
        *([SystemBTrendState.NEW_LISTING_WARMUP.value] * 10),
        SystemBTrendState.CANDIDATE.value,
    ]
    assert result.frame.loc[:9, UNDERLYING_TREND_STATE].eq(SystemBTrendState.BASE.value).all()
    assert result.frame.loc[:9, CONSECUTIVE_ABOVE_MA5_DAYS].eq(0).all()
    assert result.frame.loc[9, LISTING_TRADING_DAY_NUMBER] == 10
    assert result.frame.loc[10, PREVIOUS_TREND_STATE] == SystemBTrendState.NEW_LISTING_WARMUP.value
    assert bool(result.frame.loc[10, STATE_CHANGED]) is True


def test_warmup_allows_missing_ma5_before_standard_window_exists() -> None:
    rows = [_row("NEW", f"2026-02-{day:02d}", day, above=True) for day in range(1, 12)]
    for row in rows[:4]:
        row[MA5] = np.nan

    result = calculate_system_b_2_0_states(_request(pd.DataFrame(rows)))
    early_warmup = result.frame.iloc[:4]
    later_warmup = result.frame.iloc[4:10]
    day_eleven = result.frame.iloc[10]

    assert early_warmup[TREND_STATE].eq(SystemBTrendState.NEW_LISTING_WARMUP.value).all()
    assert early_warmup[UNDERLYING_TREND_STATE].eq(SystemBTrendState.BASE.value).all()
    assert early_warmup[MA5].isna().all()
    assert early_warmup[IS_ABOVE_OR_EQUAL_MA5].isna().all()
    assert early_warmup[CONSECUTIVE_ABOVE_MA5_DAYS].eq(0).all()
    assert early_warmup[CONSECUTIVE_BELOW_MA5_DAYS].eq(0).all()
    assert later_warmup[IS_ABOVE_OR_EQUAL_MA5].eq(True).all()
    assert later_warmup[CONSECUTIVE_ABOVE_MA5_DAYS].eq(0).all()
    assert day_eleven[TREND_STATE] == SystemBTrendState.CANDIDATE.value
    assert day_eleven[CONSECUTIVE_ABOVE_MA5_DAYS] == 1


def test_day_eleven_requires_finite_ma5() -> None:
    rows = [_row("NEW", f"2026-02-{day:02d}", day, above=True) for day in range(1, 12)]
    rows[10][MA5] = np.nan

    with pytest.raises(SystemBStateMachineError) as exc_info:
        calculate_system_b_2_0_states(_request(pd.DataFrame(rows)))

    assert exc_info.value.code == "MISSING_NUMERIC_INPUT"


def test_warmup_counts_actual_trading_days_and_crosses_suspensions() -> None:
    rows: list[dict[str, object]] = []
    listing_day = 0
    suspended_calendar_days = {3, 8}
    for calendar_day in range(1, 14):
        is_trading = calendar_day not in suspended_calendar_days
        if is_trading:
            listing_day += 1
        rows.append(
            _row(
                "NEW",
                f"2026-03-{calendar_day:02d}",
                listing_day,
                above=True,
                is_trading_day=is_trading,
            )
        )

    result = calculate_system_b_2_0_states(_request(pd.DataFrame(rows)))
    trading = result.frame[result.frame[IS_TRADING_DAY]].reset_index(drop=True)
    suspended = result.frame[~result.frame[IS_TRADING_DAY]].reset_index(drop=True)

    assert trading.loc[:9, TREND_STATE].eq(SystemBTrendState.NEW_LISTING_WARMUP.value).all()
    assert trading.loc[9, LISTING_TRADING_DAY_NUMBER] == 10
    assert trading.loc[10, LISTING_TRADING_DAY_NUMBER] == 11
    assert trading.loc[10, TREND_STATE] == SystemBTrendState.CANDIDATE.value
    assert suspended[TREND_STATE].eq(SystemBTrendState.NEW_LISTING_WARMUP.value).all()
    assert suspended[DIAGNOSTICS].map(lambda value: value == (DIAGNOSTIC_NON_TRADING_DAY,)).all()


@pytest.mark.parametrize(
    (
        "checkpoint",
        "relations",
        "expected_states",
        "expected_above_days",
        "expected_below_days",
    ),
    [
        (
            _checkpoint(SystemBTrendState.BASE, below_days=3),
            [False],
            [SystemBTrendState.BASE.value],
            [0],
            [4],
        ),
        (
            _checkpoint(SystemBTrendState.BASE),
            [True, True],
            [SystemBTrendState.CANDIDATE.value, SystemBTrendState.ACTIVE.value],
            [1, 2],
            [0, 0],
        ),
        (
            _checkpoint(SystemBTrendState.CANDIDATE, above_days=1),
            [False],
            [SystemBTrendState.BASE.value],
            [0],
            [1],
        ),
        (
            _checkpoint(SystemBTrendState.ACTIVE, above_days=4),
            [False],
            [SystemBTrendState.ACTIVE.value],
            [0],
            [1],
        ),
        (
            _checkpoint(SystemBTrendState.ACTIVE, below_days=1),
            [False],
            [SystemBTrendState.BASE.value],
            [0],
            [2],
        ),
        (
            _checkpoint(SystemBTrendState.ACTIVE, below_days=1),
            [True],
            [SystemBTrendState.ACTIVE.value],
            [1],
            [0],
        ),
    ],
)
def test_state_transition_table(
    checkpoint: SystemBStateCheckpoint,
    relations: list[bool],
    expected_states: list[str],
    expected_above_days: list[int],
    expected_below_days: list[int],
) -> None:
    result = calculate_system_b_2_0_states(
        _request(_normal_rows(relations), initial_states=(checkpoint,))
    )

    assert result.frame[TREND_STATE].tolist() == expected_states
    assert result.frame[CONSECUTIVE_ABOVE_MA5_DAYS].tolist() == expected_above_days
    assert result.frame[CONSECUTIVE_BELOW_MA5_DAYS].tolist() == expected_below_days


@pytest.mark.parametrize(
    ("checkpoint", "relations", "expected_states", "streak_column", "expected_streaks"),
    [
        (
            _checkpoint(SystemBTrendState.BASE),
            [True, None, True],
            [
                SystemBTrendState.CANDIDATE.value,
                SystemBTrendState.CANDIDATE.value,
                SystemBTrendState.ACTIVE.value,
            ],
            CONSECUTIVE_ABOVE_MA5_DAYS,
            [1, 1, 2],
        ),
        (
            _checkpoint(SystemBTrendState.ACTIVE),
            [False, None, False],
            [
                SystemBTrendState.ACTIVE.value,
                SystemBTrendState.ACTIVE.value,
                SystemBTrendState.BASE.value,
            ],
            CONSECUTIVE_BELOW_MA5_DAYS,
            [1, 1, 2],
        ),
    ],
)
def test_suspension_holds_state_and_does_not_break_trading_day_streaks(
    checkpoint: SystemBStateCheckpoint,
    relations: list[bool | None],
    expected_states: list[str],
    streak_column: str,
    expected_streaks: list[int],
) -> None:
    rows = []
    listing_day = checkpoint.listing_trading_day_number
    for index, relation in enumerate(relations, start=1):
        is_trading = relation is not None
        if is_trading:
            listing_day += 1
        rows.append(
            _row(
                "A",
                pd.Timestamp("2026-01-01") + pd.Timedelta(days=index),
                listing_day,
                above=True if relation is None else relation,
                is_trading_day=is_trading,
            )
        )

    result = calculate_system_b_2_0_states(
        _request(pd.DataFrame(rows), initial_states=(checkpoint,))
    )

    assert result.frame[TREND_STATE].tolist() == expected_states
    assert result.frame[streak_column].tolist() == expected_streaks
    suspended = result.frame.loc[1]
    assert pd.isna(suspended[CLOSE])
    assert pd.isna(suspended[MA5])
    assert suspended[IS_ABOVE_OR_EQUAL_MA5] is None


def test_close_equal_to_ma5_is_an_above_line_day() -> None:
    frame = pd.DataFrame([_row("A", "2026-01-02", 21, above=True)])
    result = calculate_system_b_2_0_states(
        _request(frame, initial_states=(_checkpoint(SystemBTrendState.BASE),))
    )

    row = result.frame.iloc[0]
    assert row[CLOSE] == row[MA5]
    assert bool(row[IS_ABOVE_OR_EQUAL_MA5]) is True
    assert row[TREND_STATE] == SystemBTrendState.CANDIDATE.value


@pytest.mark.parametrize(
    ("mutate", "error_code"),
    [
        (lambda frame: frame.drop(columns=[CLOSE]), "MISSING_REQUIRED_COLUMNS"),
        (lambda frame: frame.assign(**{CLOSE: np.nan}), "MISSING_NUMERIC_INPUT"),
        (lambda frame: frame.assign(**{MA5: np.nan}), "MISSING_NUMERIC_INPUT"),
        (lambda frame: frame.assign(**{IS_TRADING_DAY: None}), "MISSING_TRADING_DAY_STATUS"),
        (lambda frame: frame.assign(**{LISTING_TRADING_DAY_NUMBER: None}), "INVALID_INTEGER"),
    ],
)
def test_incomplete_inputs_fail_explicitly(mutate, error_code: str) -> None:
    frame = pd.DataFrame([_row("A", "2026-01-02", 21)])
    with pytest.raises(SystemBStateMachineError) as exc_info:
        calculate_system_b_2_0_states(
            _request(mutate(frame), initial_states=(_checkpoint(SystemBTrendState.BASE),))
        )
    assert exc_info.value.code == error_code


def test_multi_asset_batch_is_sorted_and_state_isolated() -> None:
    rows = []
    for asset_id in ("B", "A"):
        for day in range(1, 13):
            if day <= 10:
                above = asset_id == "A"
            elif asset_id == "A":
                above = True
            else:
                above = day == 12
            rows.append(_row(asset_id, f"2026-04-{day:02d}", day, above=above))
    frame = pd.DataFrame(rows).sample(frac=1.0, random_state=7).reset_index(drop=True)

    result = calculate_system_b_2_0_states(_request(frame))
    last = result.frame.groupby(ASSET_ID, sort=False).tail(1).set_index(ASSET_ID)

    assert result.diagnostics == (DIAGNOSTIC_INPUT_SORTED,)
    assert last.loc["A", TREND_STATE] == SystemBTrendState.ACTIVE.value
    assert last.loc["B", TREND_STATE] == SystemBTrendState.CANDIDATE.value
    assert [checkpoint.asset_id for checkpoint in result.final_states] == ["A", "B"]


def test_full_recalculation_matches_incremental_continuation() -> None:
    rows = []
    listing_day = 0
    relations: list[bool | None] = [
        True,
        True,
        False,
        True,
        True,
        False,
        True,
        True,
        False,
        True,
        True,
        None,
        True,
        False,
        False,
    ]
    for index, relation in enumerate(relations, start=1):
        is_trading = relation is not None
        if is_trading:
            listing_day += 1
        rows.append(
            _row(
                "A",
                f"2026-05-{index:02d}",
                listing_day,
                above=True if relation is None else relation,
                is_trading_day=is_trading,
            )
        )
    frame = pd.DataFrame(rows)

    full = calculate_system_b_2_0_states(_request(frame))
    first = calculate_system_b_2_0_states(_request(frame.iloc[:12].copy()))
    second = calculate_system_b_2_0_states(
        _request(frame.iloc[12:].copy(), initial_states=first.final_states)
    )
    incremental = pd.concat([first.frame, second.frame], ignore_index=True)

    assert_frame_equal(full.frame.reset_index(drop=True), incremental, check_dtype=True)
    assert full.final_states == second.final_states


def test_timezone_aware_trade_date_preserves_local_calendar_label() -> None:
    frame = pd.DataFrame(
        [_row("TZ", pd.Timestamp("2026-07-26 00:30:00", tz="Asia/Shanghai"), 1)]
    )

    result = calculate_system_b_2_0_states(_request(frame))

    assert result.frame.loc[0, TRADE_DATE] == pd.Timestamp("2026-07-26")


def test_duplicate_normalized_asset_date_is_rejected() -> None:
    frame = pd.DataFrame(
        [
            _row("A", "2026-01-01", 1),
            _row("A", pd.Timestamp("2026-01-01 12:00:00", tz="Asia/Shanghai"), 2),
        ]
    )
    with pytest.raises(SystemBStateMachineError) as exc_info:
        calculate_system_b_2_0_states(_request(frame))
    assert exc_info.value.code == "DUPLICATE_OBSERVATION"


def test_partial_history_without_checkpoint_is_rejected() -> None:
    frame = pd.DataFrame([_row("A", "2026-01-20", 20)])
    with pytest.raises(SystemBStateMachineError) as exc_info:
        calculate_system_b_2_0_states(_request(frame))
    assert exc_info.value.code == "INITIAL_STATE_REQUIRED"


def test_rule_parameter_and_source_audit_fields_are_frozen() -> None:
    result = calculate_system_b_2_0_states(
        _request(pd.DataFrame([_row("A", "2026-01-01", 1)]))
    )
    row = result.frame.iloc[0]

    assert tuple(result.frame.columns) == SYSTEM_B_STATE_OUTPUT_COLUMNS
    assert row[RULE_VERSION_SET_ID] == SYSTEM_B_2_0_RULE_VERSION_SET_ID
    assert row[PARAMETER_SET_ID] == SYSTEM_B_2_0_PARAMETER_SET_ID
    assert row[PRICE_ADJUSTMENT] == PriceAdjustment.FORWARD_ADJUSTED.value
    assert row[SOURCE_RULE_IDS] == SYSTEM_B_2_0_SOURCE_RULE_IDS
    assert set(row[SOURCE_RULE_IDS]) == {
        "SB20.DATA.001",
        "SB20.DATA.002",
        "SB20.STATE.001",
        "SB20.STATE.002",
    }
    assert not any("DEFERRED" in rule_id for rule_id in row[SOURCE_RULE_IDS])
    assert not any("episode" in column.lower() for column in result.frame.columns)
    assert result.metadata["parameters"] == SYSTEM_B_2_0_PARAMETERS.to_dict()


def test_unapproved_parameter_or_price_adjustment_cannot_be_implicit() -> None:
    wrong_parameters = SystemBStateMachineParameters(
        price_adjustment=PriceAdjustment.FORWARD_ADJUSTED,
        warmup_trading_days=9,
        ma_period=5,
        active_confirm_days=2,
        exit_confirm_days=2,
    )
    frame = pd.DataFrame([_row("A", "2026-01-01", 1)])

    with pytest.raises(SystemBStateMachineError) as parameter_error:
        calculate_system_b_2_0_states(_request(frame, parameters=wrong_parameters))
    assert parameter_error.value.code == "UNSUPPORTED_PARAMETER_SET"

    with pytest.raises(SystemBStateMachineError) as adjustment_error:
        calculate_system_b_2_0_states(
            _request(frame, input_price_adjustment="UNADJUSTED")  # type: ignore[arg-type]
        )
    assert adjustment_error.value.code == "INVALID_PRICE_ADJUSTMENT"


def test_same_input_is_deterministic() -> None:
    frame = pd.DataFrame([_row("A", f"2026-06-{day:02d}", day) for day in range(1, 12)])
    first = calculate_system_b_2_0_states(_request(frame))
    second = calculate_system_b_2_0_states(_request(frame.copy()))

    assert_frame_equal(first.frame, second.frame)
    assert first.final_states == second.final_states
    assert first.metadata == second.metadata


def test_legacy_system_b_basic_1_0_behavior_and_implementation_remain_separate() -> None:
    price_frame = pd.DataFrame(
        [
            {"ticker": "A", TRADE_DATE: f"2026-01-{day:02d}", CLOSE: close}
            for day, close in enumerate([10.0, 10.0, 10.0, 10.0, 10.0, 11.0, 12.0], start=1)
        ]
    )
    legacy = detect_system_b_basic_state(calculate_stock_trend(price_frame))

    assert bool(legacy.iloc[0][SYSTEM_B_TREND_VALID]) is True
    legacy_source = (
        Path(__file__).parents[2]
        / "src"
        / "qrp_atlas"
        / "indicators"
        / "system_b"
        / "detector.py"
    ).read_text(encoding="utf-8")
    assert "NEW_LISTING_WARMUP" not in legacy_source
    assert "calculate_system_b_2_0_states" not in legacy_source


def test_state_machine_has_no_database_clock_or_deferred_domain_dependencies() -> None:
    implementation = (
        Path(__file__).parents[2]
        / "src"
        / "qrp_atlas"
        / "indicators"
        / "system_b"
        / "state_machine_v2.py"
    )
    source = implementation.read_text(encoding="utf-8").lower()

    for forbidden in (
        "import duckdb",
        "import psycopg",
        "import tushare",
        "import akshare",
        "qmt",
        "datetime.now",
        "timestamp.now",
        "episode_status",
        "episode_id",
        "from qrp_atlas.strategies",
        "from qrp_atlas.backtest",
        "from qrp_atlas.execution",
    ):
        assert forbidden not in source
