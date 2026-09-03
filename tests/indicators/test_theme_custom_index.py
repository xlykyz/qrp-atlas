"""Tests for Theme custom equal-weight index calculation and missing semantics."""

from datetime import date
import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.contracts.m4 import (
    EFFECTIVE_MEMBER_COUNT,
    INDEX_LEVEL,
    IS_M4_EFFECTIVE_MEMBER,
    THEME_DAILY_RETURN,
)
from qrp_atlas.contracts.stock_collection import COLLECTION_ID
from qrp_atlas.indicators.theme.custom_index import calculate_theme_equal_weight_index


def test_theme_equal_weight_index_strict_completeness_and_gap():
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)
    d3 = date(2026, 8, 5)

    effective_members = pd.DataFrame(
        [
            # Day 1: 2 effective members, both have returns
            {COLLECTION_ID: "COLL_A", ASSET_ID: "STK_1", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
            {COLLECTION_ID: "COLL_A", ASSET_ID: "STK_2", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
            # Day 2: 2 effective members, but only STK_1 has return (STK_2 missing) -> should be NaN
            {COLLECTION_ID: "COLL_A", ASSET_ID: "STK_1", TRADE_DATE: d2, IS_M4_EFFECTIVE_MEMBER: True},
            {COLLECTION_ID: "COLL_A", ASSET_ID: "STK_2", TRADE_DATE: d2, IS_M4_EFFECTIVE_MEMBER: True},
            # Day 3: 2 effective members, both have returns -> should recover compounding
            {COLLECTION_ID: "COLL_A", ASSET_ID: "STK_1", TRADE_DATE: d3, IS_M4_EFFECTIVE_MEMBER: True},
            {COLLECTION_ID: "COLL_A", ASSET_ID: "STK_2", TRADE_DATE: d3, IS_M4_EFFECTIVE_MEMBER: True},
        ]
    )

    market_returns = pd.DataFrame(
        [
            {ASSET_ID: "STK_1", TRADE_DATE: d1, "daily_return": 0.02},
            {ASSET_ID: "STK_2", TRADE_DATE: d1, "daily_return": 0.04},
            # Day 2: STK_2 missing
            {ASSET_ID: "STK_1", TRADE_DATE: d2, "daily_return": 0.01},
            # Day 3: both present
            {ASSET_ID: "STK_1", TRADE_DATE: d3, "daily_return": 0.03},
            {ASSET_ID: "STK_2", TRADE_DATE: d3, "daily_return": 0.05},
        ]
    )

    res = calculate_theme_equal_weight_index(effective_members, market_returns, base_level=1000.0)

    # Day 1: mean(0.02, 0.04) = 0.03 -> level = 1000 * 1.03 = 1030.0
    r1 = res[res[TRADE_DATE] == d1].iloc[0]
    assert np.isclose(r1[THEME_DAILY_RETURN], 0.03)
    assert np.isclose(r1[INDEX_LEVEL], 1030.0)

    # Day 2: Missing member -> return is NaN, level is NaN (gap)
    r2 = res[res[TRADE_DATE] == d2].iloc[0]
    assert pd.isna(r2[THEME_DAILY_RETURN])
    assert pd.isna(r2[INDEX_LEVEL])

    # Day 3: mean(0.03, 0.05) = 0.04 -> level compounds from Day 1 level (1030.0 * 1.04 = 1071.2)
    r3 = res[res[TRADE_DATE] == d3].iloc[0]
    assert np.isclose(r3[THEME_DAILY_RETURN], 0.04)
    assert np.isclose(r3[INDEX_LEVEL], 1071.2)
