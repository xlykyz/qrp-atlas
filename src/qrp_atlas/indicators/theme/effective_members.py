"""Pure fact-derived calculation of Theme M4 Effective Members."""

from __future__ import annotations

import numpy as np
import pandas as pd

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

DIAGNOSTIC_UNCONFIRMED_LISTING_DAYS = "UNCONFIRMED_LISTING_DAYS"


def calculate_m4_effective_members(
    theme_memberships: pd.DataFrame,
    listing_trading_days: pd.DataFrame,
    suspensions: pd.DataFrame,
) -> pd.DataFrame:
    """Derive M4 calculation eligibility without mutating underlying Theme Membership facts.

    Eligibility Rules:
    1. Stock must be a point-in-time Theme Member (is_theme_member == True).
    2. Stock must have provable confirmed listing trading days > 5. If listing fact is missing,
       it fails closed (ineligible) with UNCONFIRMED_LISTING_DAYS.
    3. Stock must NOT be suspended on the target trade_date (~is_suspended).
    """
    if theme_memberships.empty:
        return pd.DataFrame(
            columns=[
                COLLECTION_ID,
                ASSET_ID,
                TRADE_DATE,
                IS_THEME_MEMBER,
                CONFIRMED_LISTING_TRADING_DAY_COUNT,
                IS_SUSPENDED,
                IS_M4_EFFECTIVE_MEMBER,
                EXCLUSION_REASON,
            ]
        )

    df = theme_memberships.copy()
    if IS_THEME_MEMBER not in df.columns:
        df[IS_THEME_MEMBER] = True

    # Join listing trading day facts strictly
    if not listing_trading_days.empty and CONFIRMED_LISTING_TRADING_DAY_COUNT in listing_trading_days.columns:
        l_df = listing_trading_days[[ASSET_ID, TRADE_DATE, CONFIRMED_LISTING_TRADING_DAY_COUNT]].copy()
        df = df.merge(l_df, on=[ASSET_ID, TRADE_DATE], how="left")
    else:
        df[CONFIRMED_LISTING_TRADING_DAY_COUNT] = pd.NA

    # Join suspension facts strictly
    if not suspensions.empty and IS_SUSPENDED in suspensions.columns:
        s_df = suspensions[[ASSET_ID, TRADE_DATE, IS_SUSPENDED]].copy()
        df = df.merge(s_df, on=[ASSET_ID, TRADE_DATE], how="left")
    else:
        df[IS_SUSPENDED] = False

    df[IS_SUSPENDED] = df[IS_SUSPENDED].fillna(False).astype(bool)

    listing_counts = pd.to_numeric(df[CONFIRMED_LISTING_TRADING_DAY_COUNT], errors="coerce")
    is_missing_listing = listing_counts.isna()
    is_le_5 = (~is_missing_listing) & (listing_counts <= 5)
    is_suspended = df[IS_SUSPENDED].astype(bool)

    # Calculate eligibility
    is_eligible = (~is_missing_listing) & (~is_le_5) & (~is_suspended) & df[IS_THEME_MEMBER].astype(bool)
    df[IS_M4_EFFECTIVE_MEMBER] = is_eligible.astype(bool)

    reasons = pd.Series(pd.NA, index=df.index, dtype="string")
    reasons.loc[is_missing_listing] = DIAGNOSTIC_UNCONFIRMED_LISTING_DAYS
    reasons.loc[is_le_5] = EXCLUSION_REASON_NEW_LISTING_LE_5
    reasons.loc[(~is_missing_listing) & (~is_le_5) & is_suspended] = EXCLUSION_REASON_SUSPENDED
    df[EXCLUSION_REASON] = reasons

    return df[
        [
            COLLECTION_ID,
            ASSET_ID,
            TRADE_DATE,
            IS_THEME_MEMBER,
            CONFIRMED_LISTING_TRADING_DAY_COUNT,
            IS_SUSPENDED,
            IS_M4_EFFECTIVE_MEMBER,
            EXCLUSION_REASON,
        ]
    ]
