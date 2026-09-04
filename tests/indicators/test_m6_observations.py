"""Unit tests for M6 Market Sentiment pure calculation."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    CALCULATION_VERSION,
    CLOSE,
    CONSECUTIVE_LIMIT_UP_COUNT,
    CREATED_AT,
    INPUT_SNAPSHOT_ID,
    IS_LIMIT_DOWN,
    IS_LIMIT_UP,
    LIMIT_DOWN_COUNT,
    LIMIT_UP_COUNT,
    M6_CALCULATION_VERSION,
    MARKET_SCOPE,
    MARKET_SCOPE_ALL_MARKET,
    MARKET_SCOPE_BSE,
    MARKET_SCOPE_CHINEXT,
    MARKET_SCOPE_MAIN_BOARD,
    MARKET_SCOPE_STAR_MARKET,
    MARKET_SCOPES,
    MAX_CONSECUTIVE_LIMIT_UP_HEIGHT,
    PRE_LIMIT_UP_PREMIUM,
    PRODUCTION_RUN_ID,
    TICKER,
    TRADE_DATE,
)
from qrp_atlas.indicators.m6 import (
    M6ObservationError,
    calculate_market_m6_observations,
)


def _make_today_market(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for col in (TICKER, MARKET_SCOPE, IS_LIMIT_UP, IS_LIMIT_DOWN, CLOSE, "is_trading"):
        if col not in df.columns:
            df[col] = None
    return df


def test_calculate_market_m6_observations_outputs_five_scopes() -> None:
    target_date = date(2026, 8, 10)
    today_df = _make_today_market(
        [
            {TICKER: "000001.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 11.0, "is_trading": True},
            {TICKER: "300750.SZ", MARKET_SCOPE: MARKET_SCOPE_CHINEXT, IS_LIMIT_UP: False, IS_LIMIT_DOWN: False, CLOSE: 200.0, "is_trading": True},
            {TICKER: "688981.SH", MARKET_SCOPE: MARKET_SCOPE_STAR_MARKET, IS_LIMIT_UP: False, IS_LIMIT_DOWN: True, CLOSE: 50.0, "is_trading": True},
            {TICKER: "830799.BJ", MARKET_SCOPE: MARKET_SCOPE_BSE, IS_LIMIT_UP: False, IS_LIMIT_DOWN: False, CLOSE: 15.0, "is_trading": True},
        ]
    )
    result = calculate_market_m6_observations(
        trade_date=target_date,
        today_market=today_df,
        consecutive_streaks={"000001.SZ": 2},
        yesterday_limit_up_tickers={"000001.SZ"},
        yesterday_closes={"000001.SZ": 10.0},
        production_run_id="run-1",
        input_snapshot_id="snap-1",
    )

    assert len(result) == 5
    assert set(result[MARKET_SCOPE].tolist()) == set(MARKET_SCOPES)
    assert (result[TRADE_DATE] == target_date).all()
    assert (result[CALCULATION_VERSION] == M6_CALCULATION_VERSION).all()
    assert (result[PRODUCTION_RUN_ID] == "run-1").all()
    assert (result[INPUT_SNAPSHOT_ID] == "snap-1").all()


def test_limit_up_and_down_counts() -> None:
    target_date = date(2026, 8, 10)
    today_df = _make_today_market(
        [
            {TICKER: "000001.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 11.0, "is_trading": True},
            {TICKER: "000002.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: False, IS_LIMIT_DOWN: True, CLOSE: 9.0, "is_trading": True},
            {TICKER: "000003.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: False, IS_LIMIT_DOWN: False, CLOSE: 10.0, "is_trading": True},
            {TICKER: "300750.SZ", MARKET_SCOPE: MARKET_SCOPE_CHINEXT, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 220.0, "is_trading": True},
        ]
    )
    result = calculate_market_m6_observations(
        trade_date=target_date,
        today_market=today_df,
        consecutive_streaks={},
        yesterday_limit_up_tickers=set(),
        yesterday_closes={},
    )
    res_dict = result.set_index(MARKET_SCOPE).to_dict(orient="index")

    # ALL_MARKET: 2 up, 1 down
    assert res_dict[MARKET_SCOPE_ALL_MARKET][LIMIT_UP_COUNT] == 2
    assert res_dict[MARKET_SCOPE_ALL_MARKET][LIMIT_DOWN_COUNT] == 1

    # MAIN_BOARD: 1 up, 1 down
    assert res_dict[MARKET_SCOPE_MAIN_BOARD][LIMIT_UP_COUNT] == 1
    assert res_dict[MARKET_SCOPE_MAIN_BOARD][LIMIT_DOWN_COUNT] == 1

    # CHINEXT: 1 up, 0 down
    assert res_dict[MARKET_SCOPE_CHINEXT][LIMIT_UP_COUNT] == 1
    assert res_dict[MARKET_SCOPE_CHINEXT][LIMIT_DOWN_COUNT] == 0

    # STAR_MARKET & BSE: 0 up, 0 down
    assert res_dict[MARKET_SCOPE_STAR_MARKET][LIMIT_UP_COUNT] == 0
    assert res_dict[MARKET_SCOPE_STAR_MARKET][LIMIT_DOWN_COUNT] == 0
    assert res_dict[MARKET_SCOPE_BSE][LIMIT_UP_COUNT] == 0
    assert res_dict[MARKET_SCOPE_BSE][LIMIT_DOWN_COUNT] == 0


def test_consecutive_limit_up_and_max_height_semantics() -> None:
    target_date = date(2026, 8, 10)
    # Case A: Only 1-board (height = 1).
    # consecutive_limit_up_count must be 0, and max_consecutive_limit_up_height must be 0!
    today_df_1 = _make_today_market(
        [
            {TICKER: "000001.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 11.0, "is_trading": True},
            {TICKER: "000002.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 12.0, "is_trading": True},
        ]
    )
    result_1 = calculate_market_m6_observations(
        trade_date=target_date,
        today_market=today_df_1,
        consecutive_streaks={"000001.SZ": 1, "000002.SZ": 1},
        yesterday_limit_up_tickers=set(),
        yesterday_closes={},
    )
    mb_1 = result_1.set_index(MARKET_SCOPE).loc[MARKET_SCOPE_MAIN_BOARD]
    assert mb_1[LIMIT_UP_COUNT] == 2
    assert mb_1[CONSECUTIVE_LIMIT_UP_COUNT] == 0
    assert mb_1[MAX_CONSECUTIVE_LIMIT_UP_HEIGHT] == 0

    # Case B: Multi-board stocks (heights 1, 2, 4)
    today_df_2 = _make_today_market(
        [
            {TICKER: "000001.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 11.0, "is_trading": True},
            {TICKER: "000002.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 12.0, "is_trading": True},
            {TICKER: "000003.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 13.0, "is_trading": True},
        ]
    )
    result_2 = calculate_market_m6_observations(
        trade_date=target_date,
        today_market=today_df_2,
        consecutive_streaks={"000001.SZ": 1, "000002.SZ": 2, "000003.SZ": 4},
        yesterday_limit_up_tickers=set(),
        yesterday_closes={},
    )
    mb_2 = result_2.set_index(MARKET_SCOPE).loc[MARKET_SCOPE_MAIN_BOARD]
    assert mb_2[LIMIT_UP_COUNT] == 3
    assert mb_2[CONSECUTIVE_LIMIT_UP_COUNT] == 2  # 000002.SZ (2) and 000003.SZ (4)
    assert mb_2[MAX_CONSECUTIVE_LIMIT_UP_HEIGHT] == 4


def test_pre_limit_up_premium_aggregation_and_suspension_exclusion() -> None:
    target_date = date(2026, 8, 10)
    today_df = _make_today_market(
        [
            # Stock A: in D-1 limit-up, traded on D, close 11.0 vs y_close 10.0 (+10%)
            {TICKER: "000001.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 11.0, "is_trading": True},
            # Stock B: in D-1 limit-up, traded on D, close 9.0 vs y_close 10.0 (-10%)
            {TICKER: "000002.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: False, IS_LIMIT_DOWN: True, CLOSE: 9.0, "is_trading": True},
            # Stock C: in D-1 limit-up, suspended on D (is_trading=False), must be dropped from denominator
            {TICKER: "000003.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: False, IS_LIMIT_DOWN: False, CLOSE: 10.0, "is_trading": False},
            # Stock D: NOT in D-1 limit-up, traded on D, should not enter premium sample
            {TICKER: "000004.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 15.0, "is_trading": True},
            # Stock E in ChiNext: in D-1 limit-up, traded on D, close 24.0 vs y_close 20.0 (+20%)
            {TICKER: "300750.SZ", MARKET_SCOPE: MARKET_SCOPE_CHINEXT, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 24.0, "is_trading": True},
        ]
    )

    y_limit_up = {"000001.SZ", "000002.SZ", "000003.SZ", "300750.SZ"}
    y_closes = {"000001.SZ": 10.0, "000002.SZ": 10.0, "000003.SZ": 10.0, "300750.SZ": 20.0}

    result = calculate_market_m6_observations(
        trade_date=target_date,
        today_market=today_df,
        consecutive_streaks={"000001.SZ": 2, "000004.SZ": 1, "300750.SZ": 2},
        yesterday_limit_up_tickers=y_limit_up,
        yesterday_closes=y_closes,
    )
    res_dict = result.set_index(MARKET_SCOPE).to_dict(orient="index")

    # MAIN_BOARD premium:
    # 000001.SZ (+0.10) and 000002.SZ (-0.10), 000003.SZ excluded because suspended.
    # Mean: (0.10 + (-0.10)) / 2 = 0.00
    assert pytest.approx(res_dict[MARKET_SCOPE_MAIN_BOARD][PRE_LIMIT_UP_PREMIUM], abs=1e-6) == 0.0

    # CHINEXT premium:
    # 300750.SZ (+0.20)
    assert pytest.approx(res_dict[MARKET_SCOPE_CHINEXT][PRE_LIMIT_UP_PREMIUM], abs=1e-6) == 0.20

    # ALL_MARKET premium:
    # Directly averages ALL valid samples: 000001 (+0.10), 000002 (-0.10), 300750 (+0.20)
    # Mean = (0.10 - 0.10 + 0.20) / 3 = 0.20 / 3 ≈ 0.0666667
    # Note: If it were average of submarket means, it would be (0.00 + 0.20) / 2 = 0.10.
    # We assert it equals 0.20 / 3 !
    expected_all_market = (0.10 + (-0.10) + 0.20) / 3.0
    assert pytest.approx(res_dict[MARKET_SCOPE_ALL_MARKET][PRE_LIMIT_UP_PREMIUM], abs=1e-6) == expected_all_market

    # STAR_MARKET & BSE have no samples -> must be None (NULL)
    assert res_dict[MARKET_SCOPE_STAR_MARKET][PRE_LIMIT_UP_PREMIUM] is None
    assert res_dict[MARKET_SCOPE_BSE][PRE_LIMIT_UP_PREMIUM] is None


def test_inputs_are_not_mutated() -> None:
    target_date = date(2026, 8, 10)
    today_df = _make_today_market(
        [
            {TICKER: "000001.SZ", MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD, IS_LIMIT_UP: True, IS_LIMIT_DOWN: False, CLOSE: 11.0, "is_trading": True},
        ]
    )
    df_before = today_df.copy(deep=True)
    streaks = {"000001.SZ": 2}
    streaks_before = dict(streaks)

    calculate_market_m6_observations(
        trade_date=target_date,
        today_market=today_df,
        consecutive_streaks=streaks,
        yesterday_limit_up_tickers={"000001.SZ"},
        yesterday_closes={"000001.SZ": 10.0},
    )

    pd.testing.assert_frame_equal(today_df, df_before)
    assert streaks == streaks_before


def test_missing_required_columns_raises_error() -> None:
    target_date = date(2026, 8, 10)
    invalid_df = pd.DataFrame([{TICKER: "000001.SZ", CLOSE: 10.0}])
    with pytest.raises(M6ObservationError) as exc_info:
        calculate_market_m6_observations(
            trade_date=target_date,
            today_market=invalid_df,
            consecutive_streaks={},
            yesterday_limit_up_tickers=set(),
            yesterday_closes={},
        )
    assert exc_info.value.code == "MISSING_COLUMNS"


def test_pre_limit_up_premium_numerical_scale_and_formula() -> None:
    """验证昨日涨停溢价率封板公式与数值尺度：
    premium_i(D) = close_i(D) / close_i(D-1) - 1
    正式数值尺度为小数（如 0.0125 = 1.25%），验证 D-1 close = 10, D close = 10.5 -> premium = 0.05（非 5.0）。
    """
    target_date = date(2026, 8, 10)
    today_df = _make_today_market(
        [
            {
                TICKER: "000001.SZ",
                MARKET_SCOPE: MARKET_SCOPE_MAIN_BOARD,
                IS_LIMIT_UP: False,
                IS_LIMIT_DOWN: False,
                CLOSE: 10.5,
                "is_trading": True,
            },
        ]
    )
    result = calculate_market_m6_observations(
        trade_date=target_date,
        today_market=today_df,
        consecutive_streaks={},
        yesterday_limit_up_tickers={"000001.SZ"},
        yesterday_closes={"000001.SZ": 10.0},
    )
    mb = result.set_index(MARKET_SCOPE).loc[MARKET_SCOPE_MAIN_BOARD]
    assert mb[PRE_LIMIT_UP_PREMIUM] is not None
    assert pytest.approx(mb[PRE_LIMIT_UP_PREMIUM], abs=1e-6) == 0.05
    assert mb[PRE_LIMIT_UP_PREMIUM] != 5.0

