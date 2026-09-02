"""Tests for custom Theme equal-weight index calculation."""

from __future__ import annotations

from datetime import date
import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    ASSET_ID,
    COLLECTION_ID,
    INDEX_LEVEL,
    IS_M4_EFFECTIVE_MEMBER,
    THEME_DAILY_RETURN,
    THEME_ID,
    TRADE_DATE,
)
from qrp_atlas.indicators.theme.custom_index import calculate_theme_equal_weight_index


def test_theme_equal_weight_index_arithmetic_mean_and_compounding():
    """Verify arithmetic mean return calculation, continuous compounding, and 0-member NULL handling."""
    dates = [date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12)]

    # 2 members: Stock A (+10%, +5%, +0%), Stock B (+0%, -5%, +0%)
    # Day 1: A (+10%), B (+0%) -> Mean = +5% (0.05) -> Index: 1000 * 1.05 = 1050.0
    # Day 2: A is effective (+5%), B is NOT effective -> Mean = +5% (0.05) -> Index: 1050 * 1.05 = 1102.5
    # Day 3: Neither A nor B is effective -> Return = NULL -> Index: remains 1102.5

    effective_members = pd.DataFrame([
        {THEME_ID: "TH_1", COLLECTION_ID: "COLL:THEME:QRP:TH_1", ASSET_ID: "A", TRADE_DATE: dates[0], IS_M4_EFFECTIVE_MEMBER: True},
        {THEME_ID: "TH_1", COLLECTION_ID: "COLL:THEME:QRP:TH_1", ASSET_ID: "B", TRADE_DATE: dates[0], IS_M4_EFFECTIVE_MEMBER: True},
        {THEME_ID: "TH_1", COLLECTION_ID: "COLL:THEME:QRP:TH_1", ASSET_ID: "A", TRADE_DATE: dates[1], IS_M4_EFFECTIVE_MEMBER: True},
        {THEME_ID: "TH_1", COLLECTION_ID: "COLL:THEME:QRP:TH_1", ASSET_ID: "B", TRADE_DATE: dates[1], IS_M4_EFFECTIVE_MEMBER: False},
        {THEME_ID: "TH_1", COLLECTION_ID: "COLL:THEME:QRP:TH_1", ASSET_ID: "A", TRADE_DATE: dates[2], IS_M4_EFFECTIVE_MEMBER: False},
        {THEME_ID: "TH_1", COLLECTION_ID: "COLL:THEME:QRP:TH_1", ASSET_ID: "B", TRADE_DATE: dates[2], IS_M4_EFFECTIVE_MEMBER: False},
    ])

    market_snapshot = pd.DataFrame([
        {ASSET_ID: "A", TRADE_DATE: dates[0], "return_ratio": 0.10},
        {ASSET_ID: "B", TRADE_DATE: dates[0], "return_ratio": 0.00},
        {ASSET_ID: "A", TRADE_DATE: dates[1], "return_ratio": 0.05},
        {ASSET_ID: "B", TRADE_DATE: dates[1], "return_ratio": -0.05},
        {ASSET_ID: "A", TRADE_DATE: dates[2], "return_ratio": 0.02},
        {ASSET_ID: "B", TRADE_DATE: dates[2], "return_ratio": 0.02},
    ])

    result = calculate_theme_equal_weight_index(effective_members, market_snapshot, base_level=1000.0)

    assert len(result) == 3

    # Day 1
    assert pytest.approx(result.loc[0, THEME_DAILY_RETURN], rel=1e-6) == 0.05
    assert pytest.approx(result.loc[0, INDEX_LEVEL], rel=1e-6) == 1050.0

    # Day 2
    assert pytest.approx(result.loc[1, THEME_DAILY_RETURN], rel=1e-6) == 0.05
    assert pytest.approx(result.loc[1, INDEX_LEVEL], rel=1e-6) == 1102.5

    # Day 3 (0 effective members -> return must be None / NaN, NOT 0.0)
    assert result.loc[2, THEME_DAILY_RETURN] is None or np.isnan(result.loc[2, THEME_DAILY_RETURN])
    assert pytest.approx(result.loc[2, INDEX_LEVEL], rel=1e-6) == 1102.5
