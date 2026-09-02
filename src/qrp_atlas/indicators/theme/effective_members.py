"""Pure calculation of M4 effective member eligibility from Theme memberships and market facts."""

from __future__ import annotations

import pandas as pd

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

_REQUIRED_MEMBERSHIP_COLUMNS = (THEME_ID, COLLECTION_ID, ASSET_ID, TRADE_DATE)


class M4EffectiveMemberCalculationError(ValueError):
    """Raised when calculation inputs are invalid."""


def calculate_m4_effective_members(
    theme_memberships: pd.DataFrame,
    listing_trading_days: pd.DataFrame,
    suspensions: pd.DataFrame,
) -> pd.DataFrame:
    """Determine M4 calculation eligibility for PIT Theme members.

    Invariant:
        Theme Membership != M4 Calculation Eligibility
        - Newly listed stocks (actual trading days <= 5) remain valid Theme members, but are excluded from M4 calculations.
        - Suspended stocks remain valid Theme members, but are excluded from that day's M4 calculations.

    Args:
        theme_memberships: DataFrame containing (theme_id, collection_id, asset_id, trade_date).
        listing_trading_days: DataFrame containing (asset_id, trade_date, confirmed_listing_trading_day_count).
        suspensions: DataFrame containing (asset_id, trade_date, is_suspended).

    Returns:
        DataFrame with columns:
        (trade_date, theme_id, collection_id, asset_id, is_theme_member,
         confirmed_listing_trading_day_count, is_suspended, is_m4_effective_member, exclusion_reason)
    """
    if not isinstance(theme_memberships, pd.DataFrame):
        raise M4EffectiveMemberCalculationError("theme_memberships must be a DataFrame")
    if theme_memberships.empty:
        return pd.DataFrame(
            columns=[
                TRADE_DATE,
                THEME_ID,
                COLLECTION_ID,
                ASSET_ID,
                IS_THEME_MEMBER,
                "confirmed_listing_trading_day_count",
                "is_suspended",
                IS_M4_EFFECTIVE_MEMBER,
                EXCLUSION_REASON,
            ]
        )

    for col in _REQUIRED_MEMBERSHIP_COLUMNS:
        if col not in theme_memberships.columns:
            raise M4EffectiveMemberCalculationError(f"theme_memberships missing column: '{col}'")

    df = theme_memberships.loc[:, list(_REQUIRED_MEMBERSHIP_COLUMNS)].copy()
    df[IS_THEME_MEMBER] = True

    # Normalize dates
    df[TRADE_DATE] = pd.to_datetime(df[TRADE_DATE]).dt.date

    # Merge listing actual trading days
    if (
        not listing_trading_days.empty
        and "confirmed_listing_trading_day_count" in listing_trading_days.columns
    ):
        l_df = listing_trading_days.copy()
        l_df[TRADE_DATE] = pd.to_datetime(l_df[TRADE_DATE]).dt.date
        df = df.merge(
            l_df[[ASSET_ID, TRADE_DATE, "confirmed_listing_trading_day_count"]],
            on=[ASSET_ID, TRADE_DATE],
            how="left",
        )
    else:
        df["confirmed_listing_trading_day_count"] = 999999

    df["confirmed_listing_trading_day_count"] = df["confirmed_listing_trading_day_count"].fillna(999999).astype(int)

    # Merge suspensions
    if not suspensions.empty and "is_suspended" in suspensions.columns:
        s_df = suspensions.copy()
        s_df[TRADE_DATE] = pd.to_datetime(s_df[TRADE_DATE]).dt.date
        df = df.merge(
            s_df[[ASSET_ID, TRADE_DATE, "is_suspended"]],
            on=[ASSET_ID, TRADE_DATE],
            how="left",
        )
    else:
        df["is_suspended"] = False

    df["is_suspended"] = df["is_suspended"].fillna(False).astype(bool)

    # Determine eligibility and exclusion reasons
    is_warmup = df["confirmed_listing_trading_day_count"] <= 5
    is_susp = df["is_suspended"]

    df[IS_M4_EFFECTIVE_MEMBER] = (~is_warmup) & (~is_susp)

    # Exclusion reason (Warmup takes precedence if both, else Suspended, else None)
    df[EXCLUSION_REASON] = None
    df.loc[is_warmup, EXCLUSION_REASON] = EXCLUSION_REASON_NEW_LISTING_LE_5
    df.loc[(~is_warmup) & is_susp, EXCLUSION_REASON] = EXCLUSION_REASON_SUSPENDED

    return df.sort_values([TRADE_DATE, THEME_ID, ASSET_ID], kind="mergesort").reset_index(drop=True)
