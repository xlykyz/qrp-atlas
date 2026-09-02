"""Pure calculation of custom Theme equal-weight index from effective member returns."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    BASE_LEVEL,
    CALCULATION_VERSION,
    COLLECTION_ID,
    DEFAULT_BASE_LEVEL,
    EFFECTIVE_MEMBER_COUNT,
    INDEX_LEVEL,
    IS_M4_EFFECTIVE_MEMBER,
    PCT_CHANGE,
    THEME_CUSTOM_INDEX_VERSION,
    THEME_DAILY_RETURN,
    THEME_ID,
    TOTAL_MEMBER_COUNT,
    TRADE_DATE,
)


class ThemeIndexCalculationError(ValueError):
    """Raised when theme index calculation inputs are invalid."""


def calculate_theme_equal_weight_index(
    effective_members: pd.DataFrame,
    market_snapshot: pd.DataFrame,
    base_level: float = DEFAULT_BASE_LEVEL,
    calculation_version: str = THEME_CUSTOM_INDEX_VERSION,
) -> pd.DataFrame:
    """Calculate continuous Theme equal-weight index from effective members' daily returns.

    Rules:
    1. Only rows where is_m4_effective_member == True are included in the return average.
    2. theme_daily_return = arithmetic mean(member daily returns). Daily return from pct_change is expressed as ratio (e.g. 0.05 for +5%).
    3. If effective_member_count == 0: theme_daily_return = np.nan (NOT 0).
    4. Index level is compounded chronologically: index_level_t = index_level_{t-1} * (1 + theme_daily_return_t).
    """
    if effective_members.empty:
        return pd.DataFrame(
            columns=[
                THEME_ID,
                COLLECTION_ID,
                TRADE_DATE,
                THEME_DAILY_RETURN,
                INDEX_LEVEL,
                BASE_LEVEL,
                EFFECTIVE_MEMBER_COUNT,
                TOTAL_MEMBER_COUNT,
                CALCULATION_VERSION,
            ]
        )

    # Prepare return data: support close/pre_close or pct_change
    mkt = market_snapshot.copy()
    mkt[TRADE_DATE] = pd.to_datetime(mkt[TRADE_DATE]).dt.date

    if "return_ratio" not in mkt.columns:
        if PCT_CHANGE in mkt.columns:
            # If pct_change is given as percentage (e.g., 5.0 for 5%), convert if > 1.0 on average or keep standard ratio
            # In QRP snapshot, pct_change is standard percentage (e.g. 5.0) or ratio? Let's check close / pre_close if available
            if "close" in mkt.columns and "pre_close" in mkt.columns:
                mkt["return_ratio"] = (mkt["close"] - mkt["pre_close"]) / mkt["pre_close"]
            else:
                mkt["return_ratio"] = mkt[PCT_CHANGE] / 100.0 if (mkt[PCT_CHANGE].abs().max() > 1.5) else mkt[PCT_CHANGE]
        elif "close" in mkt.columns and "pre_close" in mkt.columns:
            mkt["return_ratio"] = (mkt["close"] - mkt["pre_close"]) / mkt["pre_close"]
        else:
            raise ThemeIndexCalculationError("market_snapshot must contain return_ratio, pct_change, or close + pre_close")

    # Match asset column (ticker or asset_id)
    asset_col = "ticker" if "ticker" in mkt.columns else ASSET_ID
    mkt[ASSET_ID] = mkt[asset_col]

    members = effective_members.copy()
    members[TRADE_DATE] = pd.to_datetime(members[TRADE_DATE]).dt.date

    # Merge returns
    merged = members.merge(
        mkt[[ASSET_ID, TRADE_DATE, "return_ratio"]],
        on=[ASSET_ID, TRADE_DATE],
        how="left",
    )

    results: list[dict[str, object]] = []

    for (theme_id, collection_id), group in merged.groupby([THEME_ID, COLLECTION_ID], sort=False):
        dates = sorted(group[TRADE_DATE].unique())
        current_level = float(base_level)

        for t_date in dates:
            day_group = group[group[TRADE_DATE] == t_date]
            total_count = len(day_group)
            effective_group = day_group[day_group[IS_M4_EFFECTIVE_MEMBER] == True]
            effective_count = len(effective_group)

            if effective_count == 0:
                daily_return = np.nan
                # Level remains unchanged or becomes NaN? Continuous index compounds on available returns
                index_val = current_level
            else:
                valid_returns = effective_group["return_ratio"].dropna()
                if valid_returns.empty:
                    daily_return = np.nan
                    index_val = current_level
                else:
                    daily_return = float(valid_returns.mean())
                    current_level = current_level * (1.0 + daily_return)
                    index_val = current_level

            results.append(
                {
                    THEME_ID: theme_id,
                    COLLECTION_ID: collection_id,
                    TRADE_DATE: t_date,
                    THEME_DAILY_RETURN: daily_return if not np.isnan(daily_return) else None,
                    INDEX_LEVEL: index_val,
                    BASE_LEVEL: float(base_level),
                    EFFECTIVE_MEMBER_COUNT: int(effective_count),
                    TOTAL_MEMBER_COUNT: int(total_count),
                    CALCULATION_VERSION: calculation_version,
                }
            )

    res_df = pd.DataFrame(results)
    if res_df.empty:
        return res_df
    return res_df.sort_values([THEME_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
