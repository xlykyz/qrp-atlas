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
    # Fail-closed: theme_limit_up_count = 0 means ALL effective members are proven non-limit-up.
    # If any member's is_limit_up is unproven (missing snapshot row or NULL), theme_limit_up_count = None.
    m_snap = market_snapshot.copy()
    if IS_LIMIT_UP not in m_snap.columns:
        m_snap[IS_LIMIT_UP] = None

    eff_active = effective_members[effective_members[IS_M4_EFFECTIVE_MEMBER].astype(bool)].copy()
    eff_merged = eff_active.merge(
        m_snap[[ASSET_ID, TRADE_DATE, IS_LIMIT_UP]],
        on=[ASSET_ID, TRADE_DATE],
        how="left",
    )

    limit_up_records = []
    if not eff_merged.empty:
        for (coll_id, dt), sub in eff_merged.groupby([COLLECTION_ID, TRADE_DATE]):
            # If any member is missing from snapshot or is_limit_up is NA, count is unprovable -> None
            if sub[IS_LIMIT_UP].isna().any():
                cnt = None
            else:
                cnt = int(sub[IS_LIMIT_UP].astype(bool).sum())
            limit_up_records.append({COLLECTION_ID: coll_id, TRADE_DATE: dt, THEME_LIMIT_UP_COUNT: cnt})

    if limit_up_records:
        limit_up_counts = pd.DataFrame(limit_up_records)
    else:
        limit_up_counts = pd.DataFrame(columns=[COLLECTION_ID, TRADE_DATE, THEME_LIMIT_UP_COUNT])

    # 2. Base Observations from theme_index_daily
    obs = theme_index_daily.merge(limit_up_counts, on=[COLLECTION_ID, TRADE_DATE], how="left")

    if EFFECTIVE_MEMBER_COUNT not in obs.columns:
        eff_counts = (
            eff_active.groupby([COLLECTION_ID, TRADE_DATE])
            .size()
            .reset_index(name=EFFECTIVE_MEMBER_COUNT)
        )
        obs = obs.merge(eff_counts, on=[COLLECTION_ID, TRADE_DATE], how="left")
        obs[EFFECTIVE_MEMBER_COUNT] = obs[EFFECTIVE_MEMBER_COUNT].fillna(0).astype(int)

    if TOTAL_MEMBER_COUNT not in obs.columns:
        total_counts = (
            effective_members.groupby([COLLECTION_ID, TRADE_DATE])
            .size()
            .reset_index(name=TOTAL_MEMBER_COUNT)
        )
        obs = obs.merge(total_counts, on=[COLLECTION_ID, TRADE_DATE], how="left")
        obs[TOTAL_MEMBER_COUNT] = obs[TOTAL_MEMBER_COUNT].fillna(0).astype(int)

    # For dates where effective_member_count == 0, all effective members (0) have confirmed 0 limit up
    zero_member_mask = (obs[EFFECTIVE_MEMBER_COUNT] == 0) & (obs[THEME_LIMIT_UP_COUNT].isna())
    obs.loc[zero_member_mask, THEME_LIMIT_UP_COUNT] = 0

    # 3. Calculate Rank in Comparison Universe
    # Fail-closed: If any board in the formal universe has missing return, theme_return_rank = None.
    # The comparison_universe_size represents the formal universe identity size (boards + themes), not shrunk.
    ranks = []
    univ_sizes = []

    for trade_date, group in obs.groupby(TRADE_DATE):
        date_boards = comparison_boards[comparison_boards[TRADE_DATE] == trade_date]
        if date_boards.empty:
            raise M4ObservationError(
                "COMPARISON_UNIVERSE_DATE_MISSING",
                f"No comparison boards found for trade_date {trade_date}",
            )

        formal_boards_count = len(date_boards["board_id"].unique())
        univ_size = formal_boards_count + len(group)

        # Check if any comparison board has missing return
        board_rets_series = pd.to_numeric(date_boards["board_return"], errors="coerce")
        has_unresolved_board_return = board_rets_series.isna().any()

        if not has_unresolved_board_return:
            valid_board_rets = board_rets_series.to_numpy(dtype=float)
            valid_theme_rets = group[THEME_DAILY_RETURN].dropna().to_numpy(dtype=float)
            all_returns = np.concatenate([valid_board_rets, valid_theme_rets])
        else:
            all_returns = None

        for _, row in group.iterrows():
            ret = row[THEME_DAILY_RETURN]
            if has_unresolved_board_return or pd.isna(ret) or not math.isfinite(ret):
                ranks.append(None)
            else:
                rank = int((all_returns > ret).sum()) + 1
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
