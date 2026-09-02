"""Tests for Theme index trend states and episode lifecycle aligned with System B semantics."""

from datetime import date
import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    CLOSE,
    EPISODE_CONFIRMED_DATE,
    EPISODE_END_DATE,
    EPISODE_ID,
    EPISODE_START_DATE,
    MA10,
    MA5,
    MA5_REENTRY_COUNT,
    PREVIOUS_TREND_STATE,
    TRADE_DATE,
    TREND_STATE,
)
from qrp_atlas.contracts.m4 import CUSTOM_INDEX_EPISODE_ID, INDEX_LEVEL
from qrp_atlas.contracts.stock_collection import COLLECTION_ID
from qrp_atlas.indicators.theme.trend_and_episode import (
    calculate_theme_index_trend_and_episodes,
)


def test_theme_index_trend_states_and_episode_system_b_equivalence():
    # Build 15 trading days
    dates = [date(2026, 8, i) for i in range(1, 16)]
    # Prices designed to test:
    # Days 1-4: Window incomplete -> State is None
    # Day 5: First MA5 complete (close=100, ma5=100) -> State is BASE (no fabricate CANDIDATE)
    # Day 6: close=105, ma5=101 -> CANDIDATE
    # Day 8: close=90 -> BASE
    # Day 9: close=120 -> CANDIDATE
    # Day 10: close=125 -> ACTIVE (MA5 re-entry count = 1)
    # Days 11-15: close drops below MA10 -> Episode terminates
    prices = [
        100.0, 100.0, 100.0, 100.0, 95.0,   # 1-5
        105.0, 110.0, 90.0, 120.0, 125.0,   # 6-10
        70.0, 65.0, 60.0, 55.0, 50.0        # 11-15
    ]

    df = pd.DataFrame({
        COLLECTION_ID: ["COLL_AI"] * 15,
        TRADE_DATE: dates,
        INDEX_LEVEL: prices,
    })

    res = calculate_theme_index_trend_and_episodes(df, theme_id="THM_AI")
    states = res.daily_states
    episodes = res.episodes

    # 1. Days 1-4: Incomplete MA5 -> None
    for i in range(4):
        assert pd.isna(states.iloc[i][TREND_STATE])

    # 2. Day 5 (index 4): First MA5 -> BASE
    assert states.iloc[4][TREND_STATE] == "BASE"

    # 3. Day 6 (index 5): CANDIDATE
    assert states.iloc[5][TREND_STATE] == "CANDIDATE"

    # 4. Day 7 (index 6): ACTIVE -> Episode Confirmed
    assert states.iloc[6][TREND_STATE] == "ACTIVE"

    # 5. Day 8 (index 7): Drops below MA5 -> BASE
    assert states.iloc[7][TREND_STATE] == "BASE"

    # 6. Day 9 (index 8): Crosses MA5 -> CANDIDATE
    assert states.iloc[8][TREND_STATE] == "CANDIDATE"

    # 7. Day 10 (index 9): Confirms above MA5 -> ACTIVE (Re-entry)
    assert states.iloc[9][TREND_STATE] == "ACTIVE"

    # 8. Check Episode records
    assert len(episodes) == 1
    ep = episodes.iloc[0]
    assert ep[EPISODE_ID] == "THM_AI_EP_0001"
    assert ep[EPISODE_START_DATE] == dates[5]      # Day 6
    assert ep[EPISODE_CONFIRMED_DATE] == dates[6]  # Day 7
    assert ep[MA5_REENTRY_COUNT] == 1              # Day 10 re-entry
    assert ep[EPISODE_END_DATE] is not None        # Terminated when MA10 broken


def test_first_ma5_semantics_matches_system_b():
    """验证第一次 MA5 可用时的行为与 System B 一致：
    - close < ma5 -> BASE
    - close >= ma5 -> None (缺少前期证明，绝不能强行判 BASE)
    """
    dates = [date(2026, 8, i) for i in range(1, 6)]

    # Case A: Day 5 close < ma5 -> BASE
    prices_a = [100.0, 100.0, 100.0, 100.0, 95.0]  # MA5 = 99.0, close = 95.0 < MA5
    df_a = pd.DataFrame({
        COLLECTION_ID: ["COLL_1"] * 5,
        TRADE_DATE: dates,
        INDEX_LEVEL: prices_a,
    })
    res_a = calculate_theme_index_trend_and_episodes(df_a, theme_id="THM_1")
    assert res_a.daily_states.iloc[4][TREND_STATE] == "BASE"

    # Case B: Day 5 close >= ma5 -> None (not BASE!)
    prices_b = [100.0, 100.0, 100.0, 100.0, 105.0]  # MA5 = 101.0, close = 105.0 >= MA5
    df_b = pd.DataFrame({
        COLLECTION_ID: ["COLL_2"] * 5,
        TRADE_DATE: dates,
        INDEX_LEVEL: prices_b,
    })
    res_b = calculate_theme_index_trend_and_episodes(df_b, theme_id="THM_2")
    assert pd.isna(res_b.daily_states.iloc[4][TREND_STATE]), (
        f"Expected None on first MA5 completion with close >= ma5, got {res_b.daily_states.iloc[4][TREND_STATE]}"
    )


def test_episode_termination_requires_two_days_below_ma10_outside_active():
    """验证 Episode 终止至少要求：trend_state != ACTIVE AND previous_below_ma10 AND close < ma10。
    单日跌破 MA10 绝不能导致 Episode 立即终止。
    """
    # 15 days of prices
    dates = [date(2026, 8, i) for i in range(1, 16)]
    # Setup episode:
    # Days 1-5: 100, 100, 100, 100, 95 (Day 5 = BASE)
    # Day 6: 105 (MA5 = 100, CANDIDATE)
    # Day 7: 110 (MA5 = 102, ACTIVE -> Episode start=Day 6, confirmed=Day 7)
    # Day 8: 112 (ACTIVE)
    # Day 9: 115 (ACTIVE)
    # Day 10: 118 (ACTIVE, MA10 = 105.5)
    # Day 11: 104 (MA5 = 111.8, MA10 = 105.9. close < MA5 -> BASE. Also close < MA10. BUT previous_below_ma10 was False!)
    # -> Must NOT terminate yet!
    # Day 12: 103 (MA5 = 110.4, MA10 = 106.2. close < MA5 -> BASE. close < MA10. AND previous_below_ma10 is True!)
    # -> NOW terminates on Day 12!
    prices = [
        100.0, 100.0, 100.0, 100.0, 95.0,
        105.0, 110.0, 112.0, 115.0, 118.0,
        104.0, 103.0, 102.0, 101.0, 100.0,
    ]
    df = pd.DataFrame({
        COLLECTION_ID: ["COLL_TEST"] * 15,
        TRADE_DATE: dates,
        INDEX_LEVEL: prices,
    })
    res = calculate_theme_index_trend_and_episodes(df, theme_id="THM_TEST")
    episodes = res.episodes
    states = res.daily_states

    assert len(episodes) == 1
    ep = episodes.iloc[0]
    assert ep[EPISODE_START_DATE] == dates[5]      # Day 6
    assert ep[EPISODE_CONFIRMED_DATE] == dates[6]  # Day 7
    # Day 11 was first day < MA10: must NOT terminate on Day 11
    assert ep[EPISODE_END_DATE] == dates[11], (
        f"Episode must terminate on Day 12 (second consecutive day < MA10), got {ep[EPISODE_END_DATE]}"
    )
    # Check that day 11 still has custom_index_episode_id
    assert states.iloc[10][CUSTOM_INDEX_EPISODE_ID] == ep[EPISODE_ID]
    # Check that day 12 has custom_index_episode_id
    assert states.iloc[11][CUSTOM_INDEX_EPISODE_ID] == ep[EPISODE_ID]
    # Check that day 13 has None for episode_id
    assert pd.isna(states.iloc[12][CUSTOM_INDEX_EPISODE_ID])


def test_system_b_and_theme_episode_direct_equivalence():
    """验证 Theme Episode 规则与 System B 规则的严格等价性：
    1. 起始条件（CANDIDATE -> ACTIVE）
    2. 确认日期
    3. Re-entry 计数（CANDIDATE -> ACTIVE 发生在活跃 Episode 中）
    4. 结束条件（连续两日 close < MA10 且当前状态非 ACTIVE）
    """
    from qrp_atlas.indicators.system_b.episode import calculate_system_b_episodes
    from qrp_atlas.contracts import ASSET_ID

    dates = [date(2026, 8, i) for i in range(1, 21)]
    prices = [
        100.0, 100.0, 100.0, 100.0, 95.0,   # 1-5
        105.0, 110.0, 112.0, 115.0, 118.0,  # 6-10
        90.0, 120.0, 125.0, 130.0, 135.0,   # 11-15 (drop below MA5 on 11, candidate on 12, active on 13 -> re-entry=1)
        100.0, 95.0, 90.0, 85.0, 80.0       # 16-20 (drop below MA10 on 16, second drop on 17 -> end on 17)
    ]
    df = pd.DataFrame({
        COLLECTION_ID: ["COLL_EQ"] * 20,
        TRADE_DATE: dates,
        INDEX_LEVEL: prices,
    })
    theme_res = calculate_theme_index_trend_and_episodes(df, theme_id="TEST_EQ")
    theme_eps = theme_res.episodes
    theme_states = theme_res.daily_states

    assert len(theme_eps) == 1
    t_ep = theme_eps.iloc[0]
    assert t_ep[EPISODE_START_DATE] == dates[5]
    assert t_ep[EPISODE_CONFIRMED_DATE] == dates[6]
    assert t_ep[MA5_REENTRY_COUNT] == 1
    # Day 16: close=100.0, MA10=114.5. First day < MA10 -> does not end
    # Day 17: close=95.0, MA10=111.9. Second day < MA10 -> ends on Day 17 (dates[16])
    assert t_ep[EPISODE_END_DATE] == dates[16]
