"""Tests for M4 Effective Member filtering rules and invariants."""

from __future__ import annotations

from datetime import date
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    ASSET_ID,
    COLLECTION_ID,
    EXCLUSION_REASON,
    EXCLUSION_REASON_NEW_LISTING_LE_5,
    EXCLUSION_REASON_SUSPENDED,
    IS_M4_EFFECTIVE_MEMBER,
    IS_THEME_MEMBER,
    THEME_ID,
    TRADE_DATE,
)
from qrp_atlas.indicators.theme.effective_members import calculate_m4_effective_members


def test_effective_members_new_listing_and_suspension_rules():
    """Verify that:
    - New listing (actual trading days <= 5) has is_theme_member=True, is_m4_effective_member=False, reason=NEW_LISTING_LE_5.
    - Day 6 becomes is_m4_effective_member=True.
    - Suspended stock has is_theme_member=True, is_m4_effective_member=False, reason=SUSPENDED.
    - Resumed stock becomes is_m4_effective_member=True.
    - Theme membership itself is NEVER mutated.
    """
    # 4 trading dates: Day 1 (Day 4 of listing), Day 2 (Day 5), Day 3 (Day 6), Day 4 (Day 7, but suspended)
    dates = [date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13)]

    memberships = pd.DataFrame([
        {THEME_ID: "TH_AI", COLLECTION_ID: "COLL:THEME:QRP:AI", ASSET_ID: "000001.SZ", TRADE_DATE: d}
        for d in dates
    ])

    listing_days = pd.DataFrame([
        {ASSET_ID: "000001.SZ", TRADE_DATE: dates[0], "confirmed_listing_trading_day_count": 4},
        {ASSET_ID: "000001.SZ", TRADE_DATE: dates[1], "confirmed_listing_trading_day_count": 5},
        {ASSET_ID: "000001.SZ", TRADE_DATE: dates[2], "confirmed_listing_trading_day_count": 6},
        {ASSET_ID: "000001.SZ", TRADE_DATE: dates[3], "confirmed_listing_trading_day_count": 7},
    ])

    suspensions = pd.DataFrame([
        {ASSET_ID: "000001.SZ", TRADE_DATE: dates[0], "is_suspended": False},
        {ASSET_ID: "000001.SZ", TRADE_DATE: dates[1], "is_suspended": False},
        {ASSET_ID: "000001.SZ", TRADE_DATE: dates[2], "is_suspended": False},
        {ASSET_ID: "000001.SZ", TRADE_DATE: dates[3], "is_suspended": True},  # Suspended on Day 4
    ])

    result = calculate_m4_effective_members(memberships, listing_days, suspensions)

    assert len(result) == 4
    # All days: is_theme_member must be True
    assert (result[IS_THEME_MEMBER] == True).all()

    # Day 1: listing day 4 -> excluded
    assert result.loc[0, IS_M4_EFFECTIVE_MEMBER] == False
    assert result.loc[0, EXCLUSION_REASON] == EXCLUSION_REASON_NEW_LISTING_LE_5

    # Day 2: listing day 5 -> excluded
    assert result.loc[1, IS_M4_EFFECTIVE_MEMBER] == False
    assert result.loc[1, EXCLUSION_REASON] == EXCLUSION_REASON_NEW_LISTING_LE_5

    # Day 3: listing day 6 -> ELIGIBLE
    assert result.loc[2, IS_M4_EFFECTIVE_MEMBER] == True
    assert result.loc[2, EXCLUSION_REASON] is None

    # Day 4: listing day 7, but suspended -> excluded
    assert result.loc[3, IS_M4_EFFECTIVE_MEMBER] == False
    assert result.loc[3, EXCLUSION_REASON] == EXCLUSION_REASON_SUSPENDED
