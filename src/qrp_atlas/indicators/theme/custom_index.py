"""Pure calculation of custom Theme Equal-Weight Index with strict missing input semantics."""

from __future__ import annotations

import math
import numpy as np
import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.contracts.m4 import (
    BASE_LEVEL,
    DEFAULT_BASE_LEVEL,
    EFFECTIVE_MEMBER_COUNT,
    INDEX_LEVEL,
    IS_M4_EFFECTIVE_MEMBER,
    THEME_DAILY_RETURN,
    TOTAL_MEMBER_COUNT,
)
from qrp_atlas.contracts.stock_collection import COLLECTION_ID


def calculate_theme_equal_weight_index(
    effective_members: pd.DataFrame,
    market_returns: pd.DataFrame,
    base_level: float = DEFAULT_BASE_LEVEL,
    previous_cumulative_index_level: float | None = None,
) -> pd.DataFrame:
    """Calculate arithmetic mean equal-weight returns and continuous compounding index levels.

    Strict Missing Input Policy:
    1. If effective_member_count == 0, theme_daily_return is NaN.
    2. If effective_member_count > 0, ALL effective members MUST have valid non-null returns.
       If any member return is missing, the entire Theme's daily return is NaN (unresolved).
    3. On NaN return days, index_level is NaN (gap). No carry-forward to fabricate flat return.
    """
    if effective_members.empty:
        return pd.DataFrame(
            columns=[
                COLLECTION_ID,
                TRADE_DATE,
                THEME_DAILY_RETURN,
                INDEX_LEVEL,
                BASE_LEVEL,
                EFFECTIVE_MEMBER_COUNT,
                TOTAL_MEMBER_COUNT,
            ]
        )

    # Prepare market returns: asset_id, trade_date, daily_return (in ratio, e.g. 0.05 for +5%)
    # Support either 'daily_return' or 'pct_change' (where pct_change in percent is converted to ratio if needed)
    m_df = market_returns.copy()
    if "daily_return" in m_df.columns:
        m_df["ret"] = pd.to_numeric(m_df["daily_return"], errors="coerce")
    elif "pct_change" in m_df.columns:
        # daily_market_snapshot pct_change is percent (e.g. 5.0 -> 0.05)
        m_df["ret"] = pd.to_numeric(m_df["pct_change"], errors="coerce") / 100.0
    else:
        raise ValueError("market_returns must contain 'daily_return' or 'pct_change'")

    # Group by collection and trade_date
    results = []
    for (coll_id, trade_date), group in effective_members.groupby(
        [COLLECTION_ID, TRADE_DATE], sort=True
    ):
        total_count = len(group)
        eff_group = group[group[IS_M4_EFFECTIVE_MEMBER].astype(bool)]
        eff_count = len(eff_group)

        if eff_count == 0:
            theme_return = np.nan
        else:
            # Join with market returns
            merged = eff_group.merge(
                m_df[[ASSET_ID, TRADE_DATE, "ret"]],
                on=[ASSET_ID, TRADE_DATE],
                how="left",
            )
            valid_returns = merged["ret"].dropna()
            # Strict completeness: all effective members must have a valid return
            if len(valid_returns) == eff_count:
                theme_return = float(valid_returns.mean())
            else:
                # Incomplete returns -> fail closed
                theme_return = np.nan

        results.append(
            {
                COLLECTION_ID: coll_id,
                TRADE_DATE: trade_date,
                THEME_DAILY_RETURN: theme_return,
                BASE_LEVEL: base_level,
                EFFECTIVE_MEMBER_COUNT: eff_count,
                TOTAL_MEMBER_COUNT: total_count,
            }
        )

    out = pd.DataFrame(results).sort_values([COLLECTION_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)

    # Calculate continuous compounded index level per collection
    index_levels = []
    for coll_id, coll_df in out.groupby(COLLECTION_ID, sort=False):
        current_level = previous_cumulative_index_level if previous_cumulative_index_level is not None else base_level
        levels = []
        for ret in coll_df[THEME_DAILY_RETURN]:
            if pd.isna(ret) or not math.isfinite(ret):
                levels.append(np.nan)
            else:
                current_level = current_level * (1.0 + ret)
                levels.append(current_level)
        index_levels.extend(levels)

    out[INDEX_LEVEL] = index_levels
    return out[
        [
            COLLECTION_ID,
            TRADE_DATE,
            THEME_DAILY_RETURN,
            INDEX_LEVEL,
            BASE_LEVEL,
            EFFECTIVE_MEMBER_COUNT,
            TOTAL_MEMBER_COUNT,
        ]
    ]
