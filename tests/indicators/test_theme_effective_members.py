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


def test_m4_effective_members_day_5_vs_day_6_boundary():
    """精确验证第 5 与第 6 实际交易日的资格边界：
    - Day 5: 必须被排除 (NEW_LISTING_LE_5)
    - Day 6: 必须具备资格 (IS_M4_EFFECTIVE_MEMBER = True)
    """
    trade_date = date(2026, 8, 3)
    memberships = pd.DataFrame([
        {COLLECTION_ID: "COLL_1", ASSET_ID: "DAY_5_STOCK", TRADE_DATE: trade_date, IS_THEME_MEMBER: True},
        {COLLECTION_ID: "COLL_1", ASSET_ID: "DAY_6_STOCK", TRADE_DATE: trade_date, IS_THEME_MEMBER: True},
    ])
    listing_days = pd.DataFrame([
        {ASSET_ID: "DAY_5_STOCK", TRADE_DATE: trade_date, CONFIRMED_LISTING_TRADING_DAY_COUNT: 5},
        {ASSET_ID: "DAY_6_STOCK", TRADE_DATE: trade_date, CONFIRMED_LISTING_TRADING_DAY_COUNT: 6},
    ])
    suspensions = pd.DataFrame(columns=[ASSET_ID, TRADE_DATE, IS_SUSPENDED])

    res = calculate_m4_effective_members(memberships, listing_days, suspensions)
    res_map = {row[ASSET_ID]: row for _, row in res.iterrows()}

    assert res_map["DAY_5_STOCK"][IS_M4_EFFECTIVE_MEMBER] == False
    assert res_map["DAY_5_STOCK"][EXCLUSION_REASON] == EXCLUSION_REASON_NEW_LISTING_LE_5

    assert res_map["DAY_6_STOCK"][IS_M4_EFFECTIVE_MEMBER] == True
    assert pd.isna(res_map["DAY_6_STOCK"][EXCLUSION_REASON])


def test_m4_effective_members_system_b_market_fact_status():
    """验证直接复用 System B 的 market_fact_status：
    - ACTUAL_TRADING 且 count > 5 -> 具备资格
    - EXPLICIT_NON_TRADING -> 判定为停牌 (SUSPENDED)
    - UNRESOLVED_MISSING -> fail closed (UNCONFIRMED_LISTING_DAYS)
    """
    trade_date = date(2026, 8, 3)
    memberships = pd.DataFrame([
        {COLLECTION_ID: "COLL_1", ASSET_ID: "STOCK_TRADING", TRADE_DATE: trade_date, IS_THEME_MEMBER: True},
        {COLLECTION_ID: "COLL_1", ASSET_ID: "STOCK_EXPLICIT_NON", TRADE_DATE: trade_date, IS_THEME_MEMBER: True},
        {COLLECTION_ID: "COLL_1", ASSET_ID: "STOCK_UNRESOLVED", TRADE_DATE: trade_date, IS_THEME_MEMBER: True},
    ])
    listing_days = pd.DataFrame([
        {
            ASSET_ID: "STOCK_TRADING",
            TRADE_DATE: trade_date,
            CONFIRMED_LISTING_TRADING_DAY_COUNT: 10,
            "market_fact_status": "ACTUAL_TRADING",
        },
        {
            ASSET_ID: "STOCK_EXPLICIT_NON",
            TRADE_DATE: trade_date,
            CONFIRMED_LISTING_TRADING_DAY_COUNT: 10,
            "market_fact_status": "EXPLICIT_NON_TRADING",
        },
        {
            ASSET_ID: "STOCK_UNRESOLVED",
            TRADE_DATE: trade_date,
            CONFIRMED_LISTING_TRADING_DAY_COUNT: 10,
            "market_fact_status": "UNRESOLVED_MISSING",
        },
    ])
    suspensions = pd.DataFrame(columns=[ASSET_ID, TRADE_DATE, IS_SUSPENDED])

    res = calculate_m4_effective_members(memberships, listing_days, suspensions)
    res_map = {row[ASSET_ID]: row for _, row in res.iterrows()}

    assert res_map["STOCK_TRADING"][IS_M4_EFFECTIVE_MEMBER] == True
    assert pd.isna(res_map["STOCK_TRADING"][EXCLUSION_REASON])

    assert res_map["STOCK_EXPLICIT_NON"][IS_M4_EFFECTIVE_MEMBER] == False
    assert res_map["STOCK_EXPLICIT_NON"][EXCLUSION_REASON] == EXCLUSION_REASON_SUSPENDED

    assert res_map["STOCK_UNRESOLVED"][IS_M4_EFFECTIVE_MEMBER] == False
    assert res_map["STOCK_UNRESOLVED"][EXCLUSION_REASON] == DIAGNOSTIC_UNCONFIRMED_LISTING_DAYS
