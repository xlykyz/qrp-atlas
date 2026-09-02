"""Tests for M4 effective member eligibility calculation."""

from datetime import date
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    ASSET_ID,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    IS_SUSPENDED,
    TRADE_DATE,
)
from qrp_atlas.contracts.m4 import (
    EXCLUSION_REASON,
    EXCLUSION_REASON_NEW_LISTING_LE_5,
    EXCLUSION_REASON_SUSPENDED,
    IS_M4_EFFECTIVE_MEMBER,
    IS_THEME_MEMBER,
)
from qrp_atlas.contracts.stock_collection import COLLECTION_ID
from qrp_atlas.indicators.theme.effective_members import (
    DIAGNOSTIC_UNCONFIRMED_LISTING_DAYS,
    calculate_m4_effective_members,
)


def test_m4_effective_members_fail_closed_and_rules():
    trade_date = date(2026, 8, 3)
    memberships = pd.DataFrame(
        [
            {COLLECTION_ID: "COLL_1", ASSET_ID: "STOCK_NORMAL", TRADE_DATE: trade_date, IS_THEME_MEMBER: True},
            {COLLECTION_ID: "COLL_1", ASSET_ID: "STOCK_NEW", TRADE_DATE: trade_date, IS_THEME_MEMBER: True},
            {COLLECTION_ID: "COLL_1", ASSET_ID: "STOCK_SUSP", TRADE_DATE: trade_date, IS_THEME_MEMBER: True},
            {COLLECTION_ID: "COLL_1", ASSET_ID: "STOCK_NO_FACT", TRADE_DATE: trade_date, IS_THEME_MEMBER: True},
        ]
    )

    listing_days = pd.DataFrame(
        [
            {ASSET_ID: "STOCK_NORMAL", TRADE_DATE: trade_date, CONFIRMED_LISTING_TRADING_DAY_COUNT: 20},
            {ASSET_ID: "STOCK_NEW", TRADE_DATE: trade_date, CONFIRMED_LISTING_TRADING_DAY_COUNT: 4},
            {ASSET_ID: "STOCK_SUSP", TRADE_DATE: trade_date, CONFIRMED_LISTING_TRADING_DAY_COUNT: 50},
            # STOCK_NO_FACT has missing listing fact
        ]
    )

    suspensions = pd.DataFrame(
        [
            {ASSET_ID: "STOCK_SUSP", TRADE_DATE: trade_date, IS_SUSPENDED: True},
        ]
    )

    res = calculate_m4_effective_members(memberships, listing_days, suspensions)
    res_map = {row[ASSET_ID]: row for _, row in res.iterrows()}

    # 1. Normal stock is eligible
    assert res_map["STOCK_NORMAL"][IS_M4_EFFECTIVE_MEMBER] == True
    assert pd.isna(res_map["STOCK_NORMAL"][EXCLUSION_REASON])

    # 2. New listing <= 5 is excluded with NEW_LISTING_LE_5
    assert res_map["STOCK_NEW"][IS_M4_EFFECTIVE_MEMBER] == False
    assert res_map["STOCK_NEW"][EXCLUSION_REASON] == EXCLUSION_REASON_NEW_LISTING_LE_5

    # 3. Suspended stock is excluded with SUSPENDED
    assert res_map["STOCK_SUSP"][IS_M4_EFFECTIVE_MEMBER] == False
    assert res_map["STOCK_SUSP"][EXCLUSION_REASON] == EXCLUSION_REASON_SUSPENDED

    # 4. Missing listing fact fails closed with UNCONFIRMED_LISTING_DAYS
    assert res_map["STOCK_NO_FACT"][IS_M4_EFFECTIVE_MEMBER] == False
    assert res_map["STOCK_NO_FACT"][EXCLUSION_REASON] == DIAGNOSTIC_UNCONFIRMED_LISTING_DAYS
