"""Tests for Theme Index trend states and episode derivations."""

from __future__ import annotations

from datetime import date, timedelta
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    COLLECTION_ID,
    INDEX_LEVEL,
    THEME_ID,
    TRADE_DATE,
    TREND_STATE,
)
from qrp_atlas.indicators.theme.trend_and_episode import calculate_theme_index_trend_and_episodes


def test_theme_index_trend_states_and_episode_lifecycle():
    """Verify MA5/MA10 rolling calculations, CANDIDATE -> ACTIVE trigger, episode creation, and reentry."""
    # Generate 15 days of price series for Theme
    base_date = date(2026, 8, 1)
    dates = [base_date + timedelta(days=i) for i in range(15)]

    # Prices designed to:
    # Days 0-3: 1000, 1000, 1000, 1000 (MA5 = NaN -> BASE)
    # Day 4: 1000 (MA5 = 1000, close=1000, but prev was NaN -> CANDIDATE)
    # Day 5: 1050 (MA5 = 1010, close=1050, prev>=prev_ma5 -> ACTIVE, triggers EPISODE!)
    # Day 6: 1060 (MA5 = 1022, close=1060 -> ACTIVE)
    # Day 7: 1000 (MA5 = 1022, close=1000 -> BASE, inside episode)
    # Day 8: 1080 (MA5 = 1038, close=1080 -> CANDIDATE)
    # Day 9: 1100 (MA5 = 1058, close=1100 -> ACTIVE, Reentry Count = 1!)
    # Day 10-14: Prices drop to 900 -> below MA10 -> Episode Ends.
    prices = [
        1000.0, 1000.0, 1000.0, 1000.0, 1000.0,
        1050.0, 1060.0, 1000.0, 1080.0, 1100.0,
        950.0, 920.0, 900.0, 890.0, 880.0
    ]

    index_df = pd.DataFrame([
        {
            THEME_ID: "TH_AI",
            COLLECTION_ID: "COLL:THEME:QRP:AI",
            TRADE_DATE: d,
            INDEX_LEVEL: p,
        }
        for d, p in zip(dates, prices)
    ])

    res = calculate_theme_index_trend_and_episodes(index_df)
    states = res.states
    episodes = res.episodes

    assert len(states) == 15

    # Day 4: CANDIDATE
    assert states.loc[4, TREND_STATE] == "CANDIDATE"

    # Day 5: ACTIVE
    assert states.loc[5, TREND_STATE] == "ACTIVE"

    # Episodes should be created
    assert len(episodes) >= 1
    ep1 = episodes.iloc[0]
    assert ep1["theme_id"] == "TH_AI"
    assert ep1["episode_no"] == 1
    assert ep1["episode_start_date"] == dates[4]
    assert ep1["episode_confirmed_date"] == dates[5]
    # Reentry count on Day 9 should be at least 1
    assert ep1["ma5_reentry_count"] >= 1
    # Episode should be ended when price drops below MA10
    assert ep1["episode_end_date"] is not None
