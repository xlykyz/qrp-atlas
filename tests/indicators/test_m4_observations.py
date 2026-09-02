"""Tests for M4 Raw Observations calculation, limit-up counts, and comparison universe ranking."""

from __future__ import annotations

from datetime import date
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    ASSET_ID,
    COLLECTION_ID,
    COMPARISON_UNIVERSE_SIZE,
    COMPARISON_UNIVERSE_VERSION,
    COMPARISON_UNIVERSE_VERSION_V1,
    IS_LIMIT_UP,
    IS_M4_EFFECTIVE_MEMBER,
    QUALIFICATION_STATUS,
    QUALIFICATION_STATUS_NOT_CONFIGURED,
    THEME_DAILY_RETURN,
    THEME_ID,
    THEME_LIMIT_UP_COUNT,
    THEME_RETURN_RANK,
    TRADE_DATE,
)
from qrp_atlas.indicators.m4.observations import calculate_m4_raw_observations


def test_m4_raw_observations_returns_limit_up_and_rank():
    """Verify M4 observations:
    - theme_daily_return equals theme index return.
    - theme_limit_up_count counts only closing limit-ups of effective members.
    - theme_return_rank computes 1-based cross-sectional rank in universe.
    - qualification_status is fixed to 'NOT_CONFIGURED'.
    """
    t_date = date(2026, 8, 10)

    # 2 Themes: TH_1 (return +8%), TH_2 (return +3%)
    theme_indices = pd.DataFrame([
        {THEME_ID: "TH_1", COLLECTION_ID: "COLL:THEME:QRP:TH_1", TRADE_DATE: t_date, THEME_DAILY_RETURN: 0.08, "effective_member_count": 2, "total_member_count": 2},
        {THEME_ID: "TH_2", COLLECTION_ID: "COLL:THEME:QRP:TH_2", TRADE_DATE: t_date, THEME_DAILY_RETURN: 0.03, "effective_member_count": 2, "total_member_count": 2},
    ])

    # Members:
    # TH_1: Stock A (effective, limit-up=True), Stock B (effective, limit-up=False) -> Limit-up count = 1
    # TH_2: Stock C (effective, limit-up=False), Stock D (NOT effective, limit-up=True) -> Limit-up count = 0 (D excluded!)
    effective_members = pd.DataFrame([
        {THEME_ID: "TH_1", COLLECTION_ID: "COLL:THEME:QRP:TH_1", ASSET_ID: "A", TRADE_DATE: t_date, IS_M4_EFFECTIVE_MEMBER: True},
        {THEME_ID: "TH_1", COLLECTION_ID: "COLL:THEME:QRP:TH_1", ASSET_ID: "B", TRADE_DATE: t_date, IS_M4_EFFECTIVE_MEMBER: True},
        {THEME_ID: "TH_2", COLLECTION_ID: "COLL:THEME:QRP:TH_2", ASSET_ID: "C", TRADE_DATE: t_date, IS_M4_EFFECTIVE_MEMBER: True},
        {THEME_ID: "TH_2", COLLECTION_ID: "COLL:THEME:QRP:TH_2", ASSET_ID: "D", TRADE_DATE: t_date, IS_M4_EFFECTIVE_MEMBER: False},
    ])

    market_snapshot = pd.DataFrame([
        {ASSET_ID: "A", TRADE_DATE: t_date, IS_LIMIT_UP: True},
        {ASSET_ID: "B", TRADE_DATE: t_date, IS_LIMIT_UP: False},
        {ASSET_ID: "C", TRADE_DATE: t_date, IS_LIMIT_UP: False},
        {ASSET_ID: "D", TRADE_DATE: t_date, IS_LIMIT_UP: True},
    ])

    # Comparison universe boards: Board X (+10%), Board Y (+5%), Board Z (+1%)
    # Overall universe: Board X (+10%), TH_1 (+8%), Board Y (+5%), TH_2 (+3%), Board Z (+1%)
    # Total size = 5
    # TH_1 rank = 2
    # TH_2 rank = 4
    comparison_boards = pd.DataFrame([
        {"board_id": "BOARD_X", TRADE_DATE: t_date, "board_return": 0.10},
        {"board_id": "BOARD_Y", TRADE_DATE: t_date, "board_return": 0.05},
        {"board_id": "BOARD_Z", TRADE_DATE: t_date, "board_return": 0.01},
    ])

    m4_obs = calculate_m4_raw_observations(
        theme_indices=theme_indices,
        effective_members=effective_members,
        market_snapshot=market_snapshot,
        comparison_boards=comparison_boards,
        comparison_universe_version=COMPARISON_UNIVERSE_VERSION_V1,
    )

    assert len(m4_obs) == 2

    # TH_1
    row_1 = m4_obs[m4_obs[THEME_ID] == "TH_1"].iloc[0]
    assert pytest.approx(row_1[THEME_DAILY_RETURN], rel=1e-6) == 0.08
    assert row_1[THEME_LIMIT_UP_COUNT] == 1
    assert row_1[THEME_RETURN_RANK] == 2
    assert row_1[COMPARISON_UNIVERSE_SIZE] == 5
    assert row_1[QUALIFICATION_STATUS] == QUALIFICATION_STATUS_NOT_CONFIGURED
    assert row_1[COMPARISON_UNIVERSE_VERSION] == COMPARISON_UNIVERSE_VERSION_V1

    # TH_2
    row_2 = m4_obs[m4_obs[THEME_ID] == "TH_2"].iloc[0]
    assert pytest.approx(row_2[THEME_DAILY_RETURN], rel=1e-6) == 0.03
    assert row_2[THEME_LIMIT_UP_COUNT] == 0  # D was not effective, so excluded
    assert row_2[THEME_RETURN_RANK] == 4
    assert row_2[COMPARISON_UNIVERSE_SIZE] == 5
    assert row_2[QUALIFICATION_STATUS] == QUALIFICATION_STATUS_NOT_CONFIGURED
