"""Point-in-time System B market episode segment derivation."""

from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    ACTIVE_SPRINT_NO, ANCHOR_CLOSE, ANCHOR_DATE, ASSET_ID, CLOSE,
    DAYS_SINCE_CONFIRMED, DAYS_SINCE_START, DRAWDOWN_FROM_PEAK,
    END_CLOSE, END_DATE, EPISODE_CONFIRMED_DATE, EPISODE_END_DATE,
    EPISODE_ID, EPISODE_NO, EPISODE_RETURN, EPISODE_START_DATE,
    IS_EPISODE_CONFIRMED, IS_EPISODE_END, IS_OPEN, MAX_DRAWDOWN,
    PEAK_CLOSE, PEAK_DATE, PEAK_RETURN, SEGMENT_ID, SEGMENT_NO,
    SEGMENT_RETURN, SEGMENT_STATE, START_CLOSE, START_DATE,
    TRADE_DATE, TRADING_DAYS, TREND_STATE,
)
from qrp_atlas.contracts.system_b import SystemBSegmentState


class SystemBEpisodeSegmentError(ValueError):
    """Raised when segment derivation inputs or invariants are invalid."""


@dataclass(frozen=True)
class SystemBEpisodeSegmentResult:
    segments: pd.DataFrame


_EPISODE_REQUIRED = (
    EPISODE_ID, ASSET_ID, EPISODE_NO, EPISODE_START_DATE,
    EPISODE_CONFIRMED_DATE, EPISODE_END_DATE,
)

_OBSERVATION_REQUIRED = (
    TRADE_DATE, ASSET_ID, EPISODE_ID, CLOSE, TREND_STATE, EPISODE_RETURN,
)

_SEGMENT_COLUMNS = (
    SEGMENT_ID, EPISODE_ID, ASSET_ID, SEGMENT_NO, SEGMENT_STATE,
    ACTIVE_SPRINT_NO, ANCHOR_DATE, START_DATE, END_DATE, TRADING_DAYS,
    ANCHOR_CLOSE, START_CLOSE, END_CLOSE, SEGMENT_RETURN,
    PEAK_CLOSE, PEAK_DATE, PEAK_RETURN, MAX_DRAWDOWN, IS_OPEN,
)


def calculate_system_b_episode_segments(
    episodes: pd.DataFrame,
    observations: pd.DataFrame,
) -> SystemBEpisodeSegmentResult:
    """Derive deterministic active and non-active segments for each episode.

    Requires ``episodes`` and ``observations`` dataframes with standard columns.
    Maintains strict mathematical return closure:
        prod(1 + segment_return) == 1 + latest_episode_return
    """
    if episodes.empty or observations.empty:
        empty_df = pd.DataFrame(columns=_SEGMENT_COLUMNS)
        return SystemBEpisodeSegmentResult(empty_df)

    missing_ep = [col for col in _EPISODE_REQUIRED if col not in episodes.columns]
    if missing_ep:
        raise SystemBEpisodeSegmentError(f"episodes missing required columns: {missing_ep}")

    missing_obs = [col for col in _OBSERVATION_REQUIRED if col not in observations.columns]
    if missing_obs:
        raise SystemBEpisodeSegmentError(f"observations missing required columns: {missing_obs}")

    obs_df = observations.loc[:, list(_OBSERVATION_REQUIRED)].copy()
    obs_df[TRADE_DATE] = pd.to_datetime(obs_df[TRADE_DATE], errors="coerce").dt.normalize()
    if obs_df[TRADE_DATE].isna().any():
        raise SystemBEpisodeSegmentError("invalid trade_date in observations")

    # Map trend_state to segment_state
    obs_df[SEGMENT_STATE] = np.where(
        obs_df[TREND_STATE].eq("ACTIVE"),
        SystemBSegmentState.ACTIVE.value,
        SystemBSegmentState.NON_ACTIVE.value,
    )

    # Stable sort by episode_id and trade_date
    obs_df = obs_df.sort_values([EPISODE_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)

    # Segment detection within each episode_id
    prev_state = obs_df.groupby(EPISODE_ID, sort=False)[SEGMENT_STATE].shift()
    is_new_seg = prev_state.isna() | obs_df[SEGMENT_STATE].ne(prev_state)
    obs_df[SEGMENT_NO] = is_new_seg.groupby(obs_df[EPISODE_ID], sort=False).cumsum().astype(int)

    # Index episodes metadata by episode_id
    ep_map = episodes.set_index(EPISODE_ID).to_dict(orient="index")

    segments: list[dict[str, object]] = []

    for episode_id, ep_obs in obs_df.groupby(EPISODE_ID, sort=False):
        if episode_id not in ep_map:
            raise SystemBEpisodeSegmentError(f"orphan observations for episode_id: {episode_id}")

        ep_meta = ep_map[episode_id]
        asset_id = ep_meta[ASSET_ID]
        ep_start_date = pd.to_datetime(ep_meta[EPISODE_START_DATE]).normalize()
        ep_end_date = ep_meta[EPISODE_END_DATE]
        is_ep_ended = pd.notna(ep_end_date)

        seg_groups = list(ep_obs.groupby(SEGMENT_NO, sort=True))
        total_segs = len(seg_groups)

        prev_end_date: pd.Timestamp | None = None
        prev_end_close: float | None = None
        active_sprint_counter = 0

        for seg_idx, (seg_no, seg_df) in enumerate(seg_groups):
            seg_state = seg_df[SEGMENT_STATE].iloc[0]
            if seg_state == SystemBSegmentState.ACTIVE.value:
                active_sprint_counter += 1
                active_sprint_no: int | None = active_sprint_counter
            else:
                active_sprint_no = None

            start_date = seg_df[TRADE_DATE].iloc[0]
            end_date = seg_df[TRADE_DATE].iloc[-1]
            trading_days = len(seg_df)
            start_close = float(seg_df[CLOSE].iloc[0])
            end_close = float(seg_df[CLOSE].iloc[-1])

            # Anchor determination
            if seg_no == 1:
                anchor_date = ep_start_date
                first_obs_close = start_close
                first_obs_return = float(seg_df[EPISODE_RETURN].iloc[0])
                anchor_close = first_obs_close / (1.0 + first_obs_return)
            else:
                assert prev_end_date is not None and prev_end_close is not None
                anchor_date = prev_end_date
                anchor_close = prev_end_close

            # Return calculation
            segment_return = end_close / anchor_close - 1.0

            # Price path: [anchor_close] + list(closes)
            # Find peak_close, peak_date, and max_drawdown
            dates_path = [anchor_date] + seg_df[TRADE_DATE].tolist()
            closes_path = np.array([anchor_close] + seg_df[CLOSE].tolist(), dtype=np.float64)

            peak_idx = int(np.argmax(closes_path))
            peak_close = float(closes_path[peak_idx])
            peak_date = dates_path[peak_idx]
            peak_return = peak_close / anchor_close - 1.0

            # Max drawdown along the path
            running_peak = np.maximum.accumulate(closes_path)
            drawdowns = closes_path / running_peak - 1.0
            max_drawdown = float(np.min(drawdowns))

            # Open segment condition: last segment of an open episode
            is_open = (not is_ep_ended) and (seg_idx == total_segs - 1)

            segment_id = f"{episode_id}_SEG_{seg_no:03d}"

            segments.append({
                SEGMENT_ID: segment_id,
                EPISODE_ID: episode_id,
                ASSET_ID: asset_id,
                SEGMENT_NO: int(seg_no),
                SEGMENT_STATE: seg_state,
                ACTIVE_SPRINT_NO: active_sprint_no,
                ANCHOR_DATE: anchor_date,
                START_DATE: start_date,
                END_DATE: end_date,
                TRADING_DAYS: int(trading_days),
                ANCHOR_CLOSE: anchor_close,
                START_CLOSE: start_close,
                END_CLOSE: end_close,
                SEGMENT_RETURN: segment_return,
                PEAK_CLOSE: peak_close,
                PEAK_DATE: peak_date,
                PEAK_RETURN: peak_return,
                MAX_DRAWDOWN: max_drawdown,
                IS_OPEN: bool(is_open),
            })

            prev_end_date = end_date
            prev_end_close = end_close

    seg_frame = pd.DataFrame(segments, columns=_SEGMENT_COLUMNS)
    if not seg_frame.empty:
        seg_frame[ACTIVE_SPRINT_NO] = seg_frame[ACTIVE_SPRINT_NO].astype("Int64")
        seg_frame[IS_OPEN] = seg_frame[IS_OPEN].astype(bool)
        seg_frame[SEGMENT_NO] = seg_frame[SEGMENT_NO].astype(int)
        seg_frame[TRADING_DAYS] = seg_frame[TRADING_DAYS].astype(int)
    return SystemBEpisodeSegmentResult(seg_frame.reset_index(drop=True))
