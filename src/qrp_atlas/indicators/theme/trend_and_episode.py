"""Pure calculation of custom Theme Index Trend States and Episodes based on System B price-series semantics."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    CLOSE,
    DAYS_SINCE_START,
    EPISODE_CONFIRMED_DATE,
    EPISODE_END_DATE,
    EPISODE_ID,
    EPISODE_NO,
    EPISODE_RETURN,
    EPISODE_START_DATE,
    IS_ABOVE_OR_EQUAL_MA5,
    MA10,
    MA5,
    MA5_REENTRY_COUNT,
    PREVIOUS_TREND_STATE,
    STATE_CHANGED,
    TRADE_DATE,
    TREND_STATE,
)
from qrp_atlas.contracts.m4 import (
    CUSTOM_INDEX_EPISODE_ID,
    CUSTOM_INDEX_TREND_RUN_DAYS,
    CUSTOM_INDEX_TREND_STATE,
    INDEX_LEVEL,
)
from qrp_atlas.contracts.stock_collection import COLLECTION_ID


@dataclass(frozen=True)
class ThemeTrendAndEpisodeResult:
    daily_states: pd.DataFrame
    episodes: pd.DataFrame


def calculate_theme_index_trend_and_episodes(
    index_df: pd.DataFrame,
    theme_id: str | None = None,
    rule_version: str = "theme_index_state@1.0.0",
) -> ThemeTrendAndEpisodeResult:
    """Calculate trend state machine and episode lifecycle over pure continuous price series.

    Semantics strictly aligned with System B price series:
    1. MA5 requires a complete 5-day observation window. If incomplete, trend_state is None/NaN (diagnostic: INCOMPLETE_MA5_WINDOW).
    2. First time MA5 is available (no previous MA5 proof): state is BASE (does not fabricate CANDIDATE).
    3. CANDIDATE: prev_above_ma5 == False, curr_above_ma5 == True (with complete proof).
    4. ACTIVE: prev_above_ma5 == True, curr_above_ma5 == True.
    5. BASE: curr_above_ma5 == False.
    6. Episode Starts on CANDIDATE -> ACTIVE transition (start_date = candidate_date, confirmed_date = active_date).
    7. MA5 re-entry count increments when returning to ACTIVE within the same episode.
    8. Episode terminates when MA10 window is complete and close < MA10.
    """
    if index_df.empty:
        return ThemeTrendAndEpisodeResult(
            daily_states=pd.DataFrame(
                columns=[
                    COLLECTION_ID,
                    TRADE_DATE,
                    CLOSE,
                    MA5,
                    MA10,
                    TREND_STATE,
                    PREVIOUS_TREND_STATE,
                    CUSTOM_INDEX_TREND_RUN_DAYS,
                    IS_ABOVE_OR_EQUAL_MA5,
                    STATE_CHANGED,
                    CUSTOM_INDEX_EPISODE_ID,
                ]
            ),
            episodes=pd.DataFrame(
                columns=[
                    EPISODE_ID,
                    COLLECTION_ID,
                    EPISODE_NO,
                    EPISODE_START_DATE,
                    EPISODE_CONFIRMED_DATE,
                    EPISODE_END_DATE,
                    MA5_REENTRY_COUNT,
                    EPISODE_RETURN,
                ]
            ),
        )

    df = index_df.sort_values([TRADE_DATE], kind="mergesort").reset_index(drop=True).copy()
    coll_id = str(df[COLLECTION_ID].iloc[0]) if COLLECTION_ID in df.columns else (theme_id or "THEME")
    thm_prefix = theme_id or coll_id

    # Use index_level as close price
    price_col = INDEX_LEVEL if INDEX_LEVEL in df.columns else CLOSE
    prices = pd.to_numeric(df[price_col], errors="coerce")

    # Only valid non-null prices participate in contiguous MA calculations
    valid_mask = prices.notna()

    # Calculate rolling MA5 and MA10 only over contiguous valid prices
    ma5_series = pd.Series(np.nan, index=df.index, dtype="float64")
    ma10_series = pd.Series(np.nan, index=df.index, dtype="float64")

    # Contiguous blocks
    current_block = []
    for idx, (is_valid, price) in enumerate(zip(valid_mask, prices)):
        if is_valid:
            current_block.append(price)
            if len(current_block) >= 5:
                ma5_series.iloc[idx] = float(np.mean(current_block[-5:]))
            if len(current_block) >= 10:
                ma10_series.iloc[idx] = float(np.mean(current_block[-10:]))
        else:
            current_block = []

    df[CLOSE] = prices
    df[MA5] = ma5_series
    df[MA10] = ma10_series

    # State Machine derivation
    trend_states = []
    prev_states = []
    state_changed_list = []
    run_days_list = []
    is_above_ma5_list = []

    current_state: str | None = None
    prev_is_above: bool | None = None
    run_days = 0

    for idx, row in df.iterrows():
        close_val = row[CLOSE]
        ma5_val = row[MA5]

        if pd.isna(close_val) or pd.isna(ma5_val):
            # Incomplete MA5 window or gap -> trend state is None
            trend_states.append(None)
            prev_states.append(current_state)
            state_changed_list.append(False)
            run_days_list.append(0)
            is_above_ma5_list.append(None)
            current_state = None
            prev_is_above = None
            run_days = 0
            continue

        is_above = bool(close_val >= ma5_val)
        is_above_ma5_list.append(is_above)

        if prev_is_above is None:
            # First time MA5 is available -> Base state
            new_state = "BASE"
        elif not prev_is_above and is_above:
            new_state = "CANDIDATE"
        elif prev_is_above and is_above:
            new_state = "ACTIVE"
        else:
            new_state = "BASE"

        changed = (new_state != current_state)
        if changed:
            run_days = 1
        else:
            run_days += 1

        prev_states.append(current_state)
        trend_states.append(new_state)
        state_changed_list.append(changed)
        run_days_list.append(run_days)

        current_state = new_state
        prev_is_above = is_above

    df[TREND_STATE] = trend_states
    df[PREVIOUS_TREND_STATE] = prev_states
    df[STATE_CHANGED] = state_changed_list
    df[CUSTOM_INDEX_TREND_RUN_DAYS] = run_days_list
    df[IS_ABOVE_OR_EQUAL_MA5] = is_above_ma5_list

    # Episodes derivation
    episodes: list[dict[str, Any]] = []
    episode_ids = [None] * len(df)
    current_episode: dict[str, Any] | None = None
    episode_no = 0
    start_price = 0.0

    for idx, row in df.iterrows():
        trade_date = row[TRADE_DATE]
        state = row[TREND_STATE]
        prev_state = row[PREVIOUS_TREND_STATE]
        close_val = row[CLOSE]
        ma10_val = row[MA10]

        if prev_state == "CANDIDATE" and state == "ACTIVE" and current_episode is None:
            # Start new episode
            prev_row = df.iloc[idx - 1]
            episode_no += 1
            ep_id = f"{thm_prefix}_EP_{episode_no:04d}"
            start_price = float(prev_row[CLOSE])
            current_episode = {
                EPISODE_ID: ep_id,
                COLLECTION_ID: coll_id,
                EPISODE_NO: episode_no,
                EPISODE_START_DATE: prev_row[TRADE_DATE],
                EPISODE_CONFIRMED_DATE: trade_date,
                EPISODE_END_DATE: None,
                MA5_REENTRY_COUNT: 0,
                EPISODE_RETURN: 0.0,
                "start_index": idx - 1,
            }
            # Assign episode_id to start date and confirmation date
            episode_ids[idx - 1] = ep_id
            episode_ids[idx] = ep_id
            continue

        if current_episode is not None:
            episode_ids[idx] = current_episode[EPISODE_ID]
            # Check re-entry
            if prev_state != "ACTIVE" and state == "ACTIVE":
                current_episode[MA5_REENTRY_COUNT] += 1

            # Check episode termination: MA10 complete and close < MA10
            if pd.notna(ma10_val) and close_val < ma10_val:
                current_episode[EPISODE_END_DATE] = trade_date
                current_episode[EPISODE_RETURN] = float((close_val / start_price) - 1.0)
                episodes.append(dict(current_episode))
                current_episode = None

    if current_episode is not None:
        # Open episode at end of sequence
        last_close = float(df.iloc[-1][CLOSE])
        current_episode[EPISODE_RETURN] = float((last_close / start_price) - 1.0)
        episodes.append(dict(current_episode))

    df[CUSTOM_INDEX_EPISODE_ID] = episode_ids

    episodes_df = pd.DataFrame(episodes)
    if episodes_df.empty:
        episodes_df = pd.DataFrame(
            columns=[
                EPISODE_ID,
                COLLECTION_ID,
                EPISODE_NO,
                EPISODE_START_DATE,
                EPISODE_CONFIRMED_DATE,
                EPISODE_END_DATE,
                MA5_REENTRY_COUNT,
                EPISODE_RETURN,
            ]
        )
    else:
        episodes_df = episodes_df[
            [
                EPISODE_ID,
                COLLECTION_ID,
                EPISODE_NO,
                EPISODE_START_DATE,
                EPISODE_CONFIRMED_DATE,
                EPISODE_END_DATE,
                MA5_REENTRY_COUNT,
                EPISODE_RETURN,
            ]
        ]

    return ThemeTrendAndEpisodeResult(daily_states=df, episodes=episodes_df)
