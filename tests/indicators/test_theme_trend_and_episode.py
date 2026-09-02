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
