"""Pure calculation of Theme M4 Raw Observations."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    CLOSE,
    IS_LIMIT_UP,
    TRADE_DATE,
    TREND_STATE,
)
from qrp_atlas.contracts.m4 import (
    COMPARISON_UNIVERSE_SIZE,
    COMPARISON_UNIVERSE_VERSION,
    COMPARISON_UNIVERSE_VERSION_V1,
    CUSTOM_INDEX_EPISODE_ID,
    CUSTOM_INDEX_TREND_RUN_DAYS,
    CUSTOM_INDEX_TREND_STATE,
    EFFECTIVE_MEMBER_COUNT,
    IS_M4_EFFECTIVE_MEMBER,
    QUALIFICATION_STATUS,
    QUALIFICATION_STATUS_NOT_CONFIGURED,
    THEME_DAILY_RETURN,
    THEME_LIMIT_UP_COUNT,
    THEME_RETURN_RANK,
    TOTAL_MEMBER_COUNT,
)
from qrp_atlas.contracts.stock_collection import COLLECTION_ID


class M4ObservationError(ValueError):
    """Raised when M4 observation inputs are inconsistent or universe data is missing."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


def calculate_m4_raw_observations(
    theme_index_daily: pd.DataFrame,
    effective_members: pd.DataFrame,
    market_snapshot: pd.DataFrame,
    comparison_boards: pd.DataFrame,
    comparison_universe_version: str = COMPARISON_UNIVERSE_VERSION_V1,
) -> pd.DataFrame:
    """Calculate point-in-time M4 Raw Observations.

    Contract Rules:
    1. Returns must be standard decimal ratios (e.g. 0.05 for +5%). No heuristics guessing units.
    2. Comparison universe requires reliable board returns (THS + QRP themes). Missing data fails closed.
    3. theme_limit_up_count counts effective members closing at limit up (is_limit_up == True).
    4. qualification_status is strictly frozen as 'NOT_CONFIGURED'.
    """
    if theme_index_daily.empty:
        return pd.DataFrame(
            columns=[
                COLLECTION_ID,
                TRADE_DATE,
                THEME_DAILY_RETURN,
                THEME_LIMIT_UP_COUNT,
                THEME_RETURN_RANK,
                EFFECTIVE_MEMBER_COUNT,
                TOTAL_MEMBER_COUNT,
                COMPARISON_UNIVERSE_SIZE,
                COMPARISON_UNIVERSE_VERSION,
                CUSTOM_INDEX_TREND_STATE,
                CUSTOM_INDEX_TREND_RUN_DAYS,
                CUSTOM_INDEX_EPISODE_ID,
                QUALIFICATION_STATUS,
            ]
        )

    # Validate comparison universe
    if comparison_boards.empty or "board_return" not in comparison_boards.columns:
        raise M4ObservationError(
            "MISSING_COMPARISON_UNIVERSE",
            f"comparison_boards cannot be empty for version {comparison_universe_version}",
        )

    # 1. Limit Up counts per (collection_id, trade_date)
    m_snap = market_snapshot.copy()
    if IS_LIMIT_UP not in m_snap.columns:
        m_snap[IS_LIMIT_UP] = False
    m_snap[IS_LIMIT_UP] = m_snap[IS_LIMIT_UP].fillna(False).astype(bool)

    eff_merged = effective_members[
        effective_members[IS_M4_EFFECTIVE_MEMBER].astype(bool)
    ].merge(m_snap[[ASSET_ID, TRADE_DATE, IS_LIMIT_UP]], on=[ASSET_ID, TRADE_DATE], how="left")

    eff_merged[IS_LIMIT_UP] = eff_merged[IS_LIMIT_UP].fillna(False).astype(bool)
    limit_up_counts = (
        eff_merged.groupby([COLLECTION_ID, TRADE_DATE])[IS_LIMIT_UP]
        .sum()
        .reset_index(name=THEME_LIMIT_UP_COUNT)
    )

    # 2. Base Observations from theme_index_daily
    obs = theme_index_daily.merge(limit_up_counts, on=[COLLECTION_ID, TRADE_DATE], how="left")
    obs[THEME_LIMIT_UP_COUNT] = obs[THEME_LIMIT_UP_COUNT].fillna(0).astype(int)

    # 3. Calculate Rank in Comparison Universe
    ranks = []
    univ_sizes = []

    for trade_date, group in obs.groupby(TRADE_DATE):
        date_boards = comparison_boards[comparison_boards[TRADE_DATE] == trade_date]
        if date_boards.empty:
            raise M4ObservationError(
                "COMPARISON_UNIVERSE_DATE_MISSING",
                f"No comparison boards found for trade_date {trade_date}",
            )

        # Build combined universe for ranking: all comparison boards + this date's theme returns
        board_returns = date_boards[["board_id", "board_return"]].dropna().copy()
        board_returns["board_return"] = pd.to_numeric(board_returns["board_return"], errors="coerce")
        board_returns = board_returns.dropna()

        # Combine all valid returns for ranking
        all_returns = pd.concat(
            [
                board_returns["board_return"],
                group[THEME_DAILY_RETURN].dropna(),
            ]
        ).unique()
        # Sort descending
        sorted_unique_returns = np.sort(all_returns)[::-1]
        univ_size = len(board_returns) + len(group[THEME_DAILY_RETURN].dropna())

        for _, row in group.iterrows():
            ret = row[THEME_DAILY_RETURN]
            if pd.isna(ret) or not math.isfinite(ret):
                ranks.append(None)
            else:
                # 1-based rank (number of returns strictly greater + 1)
                rank = int((sorted_unique_returns > ret).sum()) + 1
                ranks.append(rank)
            univ_sizes.append(univ_size)

    obs[THEME_RETURN_RANK] = ranks
    obs[COMPARISON_UNIVERSE_SIZE] = univ_sizes
    obs[COMPARISON_UNIVERSE_VERSION] = comparison_universe_version
    obs[QUALIFICATION_STATUS] = QUALIFICATION_STATUS_NOT_CONFIGURED

    # Map state columns if present
    if TREND_STATE in obs.columns and CUSTOM_INDEX_TREND_STATE not in obs.columns:
        obs[CUSTOM_INDEX_TREND_STATE] = obs[TREND_STATE]

    return obs
