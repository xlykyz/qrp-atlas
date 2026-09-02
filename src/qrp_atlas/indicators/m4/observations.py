"""Pure calculation of M4 Raw Observations."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    CALCULATION_VERSION,
    COLLECTION_ID,
    COMPARISON_UNIVERSE_SIZE,
    COMPARISON_UNIVERSE_VERSION,
    COMPARISON_UNIVERSE_VERSION_V1,
    CUSTOM_INDEX_EPISODE_ID,
    CUSTOM_INDEX_TREND_RUN_DAYS,
    CUSTOM_INDEX_TREND_STATE,
    EFFECTIVE_MEMBER_COUNT,
    IS_LIMIT_UP,
    IS_M4_EFFECTIVE_MEMBER,
    M4_CALCULATION_VERSION,
    QUALIFICATION_STATUS,
    QUALIFICATION_STATUS_NOT_CONFIGURED,
    THEME_DAILY_RETURN,
    THEME_ID,
    THEME_LIMIT_UP_COUNT,
    THEME_RETURN_RANK,
    TOTAL_MEMBER_COUNT,
    TRADE_DATE,
)


class M4ObservationCalculationError(ValueError):
    """Raised when M4 observation calculation inputs are invalid."""


def calculate_m4_raw_observations(
    theme_indices: pd.DataFrame,
    effective_members: pd.DataFrame,
    market_snapshot: pd.DataFrame,
    comparison_boards: pd.DataFrame | None = None,
    comparison_universe_version: str = COMPARISON_UNIVERSE_VERSION_V1,
    calculation_version: str = M4_CALCULATION_VERSION,
    theme_states: pd.DataFrame | None = None,
    theme_episodes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Calculate M4 raw observations for each (trade_date, theme_id).

    Invariants:
    1. theme_daily_return: from theme_indices (arithmetic mean of effective members).
    2. theme_limit_up_count: count of effective members where close was at limit-up (is_limit_up == True).
    3. theme_return_rank: 1-based descending rank within the comparison universe for each trade_date.
    4. qualification_status: fixed to NOT_CONFIGURED (no arbitrary threshold guesses).
    """
    if theme_indices.empty:
        return pd.DataFrame(
            columns=[
                THEME_ID,
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
                CALCULATION_VERSION,
            ]
        )

    # 1. Base structure from theme_indices
    obs_df = theme_indices[[
        THEME_ID,
        COLLECTION_ID,
        TRADE_DATE,
        THEME_DAILY_RETURN,
        EFFECTIVE_MEMBER_COUNT,
        TOTAL_MEMBER_COUNT,
    ]].copy()
    obs_df[TRADE_DATE] = pd.to_datetime(obs_df[TRADE_DATE]).dt.date

    # 2. Compute limit-up counts
    # Prepare market snapshot limit_up facts
    mkt = market_snapshot.copy()
    mkt[TRADE_DATE] = pd.to_datetime(mkt[TRADE_DATE]).dt.date
    asset_col = "ticker" if "ticker" in mkt.columns else ASSET_ID
    mkt[ASSET_ID] = mkt[asset_col]

    if IS_LIMIT_UP not in mkt.columns:
        # Fallback to is_limit_up if not explicitly present (assume false or deduce from pct_change)
        mkt[IS_LIMIT_UP] = False

    mkt[IS_LIMIT_UP] = mkt[IS_LIMIT_UP].fillna(False).astype(bool)

    eff_m = effective_members[effective_members[IS_M4_EFFECTIVE_MEMBER] == True].copy()
    eff_m[TRADE_DATE] = pd.to_datetime(eff_m[TRADE_DATE]).dt.date

    eff_merged = eff_m.merge(
        mkt[[ASSET_ID, TRADE_DATE, IS_LIMIT_UP]],
        on=[ASSET_ID, TRADE_DATE],
        how="left",
    )
    eff_merged[IS_LIMIT_UP] = eff_merged[IS_LIMIT_UP].fillna(False).astype(bool)

    # Aggregate limit-up count by theme and date
    lu_counts = (
        eff_merged.groupby([THEME_ID, TRADE_DATE])[IS_LIMIT_UP]
        .sum()
        .reset_index(name=THEME_LIMIT_UP_COUNT)
    )

    obs_df = obs_df.merge(lu_counts, on=[THEME_ID, TRADE_DATE], how="left")
    obs_df[THEME_LIMIT_UP_COUNT] = obs_df[THEME_LIMIT_UP_COUNT].fillna(0).astype(int)

    # 3. Compute cross-sectional return ranks in comparison universe
    # Comparison universe includes: Theme indices + other comparison boards (e.g. THS boards)
    universe_frames: list[pd.DataFrame] = []

    # Theme custom index returns
    theme_returns = obs_df[[THEME_ID, TRADE_DATE, THEME_DAILY_RETURN]].rename(
        columns={THEME_ID: "board_id", THEME_DAILY_RETURN: "board_return"}
    )
    theme_returns["is_target_theme"] = True
    theme_returns["theme_id"] = obs_df[THEME_ID]
    universe_frames.append(theme_returns)

    # External comparison boards (if provided)
    if comparison_boards is not None and not comparison_boards.empty:
        cb = comparison_boards.copy()
        cb[TRADE_DATE] = pd.to_datetime(cb[TRADE_DATE]).dt.date
        b_id_col = "board_id" if "board_id" in cb.columns else ("ts_code" if "ts_code" in cb.columns else "index_code")
        b_ret_col = "board_return" if "board_return" in cb.columns else ("pct_change" if "pct_change" in cb.columns else "return_ratio")

        cb_norm = pd.DataFrame({
            "board_id": cb[b_id_col],
            TRADE_DATE: cb[TRADE_DATE],
            "board_return": cb[b_ret_col] / 100.0 if (cb[b_ret_col].abs().max() > 1.5) else cb[b_ret_col],
            "is_target_theme": False,
            "theme_id": None,
        })
        universe_frames.append(cb_norm)

    all_universe = pd.concat(universe_frames, ignore_index=True)

    # Calculate rank per trade_date
    ranked_rows: list[dict[str, object]] = []
    for t_date, group in all_universe.groupby(TRADE_DATE, sort=False):
        valid_group = group.dropna(subset=["board_return"]).copy()
        u_size = len(valid_group)
        if not valid_group.empty:
            # Descending rank: highest return gets rank 1
            valid_group["rank"] = valid_group["board_return"].rank(ascending=False, method="min")

        theme_targets = group[group["is_target_theme"] == True]
        for _, row in theme_targets.iterrows():
            t_id = row["theme_id"]
            if pd.notna(row["board_return"]) and not valid_group.empty and t_id in valid_group["theme_id"].values:
                r_val = int(valid_group[valid_group["theme_id"] == t_id]["rank"].iloc[0])
            else:
                r_val = None

            ranked_rows.append({
                THEME_ID: t_id,
                TRADE_DATE: t_date,
                THEME_RETURN_RANK: r_val,
                COMPARISON_UNIVERSE_SIZE: int(u_size),
            })

    if ranked_rows:
        ranked_df = pd.DataFrame(ranked_rows)
        obs_df = obs_df.merge(ranked_df, on=[THEME_ID, TRADE_DATE], how="left")
    else:
        obs_df[THEME_RETURN_RANK] = None
        obs_df[COMPARISON_UNIVERSE_SIZE] = 0

    obs_df[COMPARISON_UNIVERSE_SIZE] = obs_df[COMPARISON_UNIVERSE_SIZE].fillna(0).astype(int)

    obs_df[COMPARISON_UNIVERSE_VERSION] = comparison_universe_version
    obs_df[QUALIFICATION_STATUS] = QUALIFICATION_STATUS_NOT_CONFIGURED
    obs_df[CALCULATION_VERSION] = calculation_version

    # 4. Attach trend state & episode ID if provided
    if theme_states is not None and not theme_states.empty:
        st = theme_states.copy()
        st[TRADE_DATE] = pd.to_datetime(st[TRADE_DATE]).dt.date
        cols = [THEME_ID, TRADE_DATE, "trend_state", "custom_index_trend_run_days"]
        st_sub = st[[c for c in cols if c in st.columns]].rename(
            columns={"trend_state": CUSTOM_INDEX_TREND_STATE}
        )
        obs_df = obs_df.merge(st_sub, on=[THEME_ID, TRADE_DATE], how="left")
    else:
        obs_df[CUSTOM_INDEX_TREND_STATE] = None
        obs_df[CUSTOM_INDEX_TREND_RUN_DAYS] = None

    if theme_episodes is not None and not theme_episodes.empty:
        ep = theme_episodes.copy()
        # Find active episode per date
        obs_df[CUSTOM_INDEX_EPISODE_ID] = None
        for _, ep_row in ep.iterrows():
            t_id = ep_row[THEME_ID]
            e_id = ep_row["episode_id"]
            s_date = pd.to_datetime(ep_row["episode_start_date"]).date()
            e_date = pd.to_datetime(ep_row["episode_end_date"]).date() if pd.notna(ep_row["episode_end_date"]) else None

            mask = (obs_df[THEME_ID] == t_id) & (obs_df[TRADE_DATE] >= s_date)
            if e_date is not None:
                mask = mask & (obs_df[TRADE_DATE] <= e_date)
            obs_df.loc[mask, CUSTOM_INDEX_EPISODE_ID] = e_id
    else:
        obs_df[CUSTOM_INDEX_EPISODE_ID] = None

    canonical_cols = [
        THEME_ID,
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
        CALCULATION_VERSION,
    ]
    for c in canonical_cols:
        if c not in obs_df.columns:
            obs_df[c] = None

    obs_df = obs_df[canonical_cols]
    return obs_df.sort_values([THEME_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)

