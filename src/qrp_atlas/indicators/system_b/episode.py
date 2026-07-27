"""Point-in-time System B market episode derivation over actual state observations."""

from __future__ import annotations

from dataclasses import dataclass
import math

import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID, CLOSE, DAYS_SINCE_CONFIRMED, DAYS_SINCE_START,
    DRAWDOWN_FROM_PEAK, EPISODE_CONFIRMED_DATE, EPISODE_END_DATE, EPISODE_ID,
    EPISODE_NO, EPISODE_RETURN, EPISODE_START_DATE, IS_EPISODE_CONFIRMED,
    IS_EPISODE_END, MA10, MA5, MA5_REENTRY_COUNT, PEAK_RETURN,
    PREVIOUS_TREND_STATE, STATE_TRANSITION, TRADE_DATE, TREND_STATE,
)


class SystemBEpisodeError(ValueError):
    pass


@dataclass(frozen=True)
class SystemBEpisodeResult:
    episodes: pd.DataFrame
    observations: pd.DataFrame


_REQUIRED = (ASSET_ID, TRADE_DATE, CLOSE, MA5, MA10, TREND_STATE)
_EPISODE_COLUMNS = (
    EPISODE_ID, ASSET_ID, EPISODE_NO, EPISODE_START_DATE,
    EPISODE_CONFIRMED_DATE, EPISODE_END_DATE, MA5_REENTRY_COUNT,
)
_OBSERVATION_COLUMNS = (
    TRADE_DATE, ASSET_ID, EPISODE_ID, DAYS_SINCE_START, DAYS_SINCE_CONFIRMED,
    CLOSE, MA5, MA10, TREND_STATE, PREVIOUS_TREND_STATE, STATE_TRANSITION,
    EPISODE_RETURN, PEAK_RETURN, DRAWDOWN_FROM_PEAK, MA5_REENTRY_COUNT,
    IS_EPISODE_CONFIRMED, IS_EPISODE_END,
)


def calculate_system_b_episodes(frame: pd.DataFrame) -> SystemBEpisodeResult:
    """Calculate episodes without future data; input rows are actual observation dates."""
    missing = [column for column in _REQUIRED if column not in frame.columns]
    if missing:
        raise SystemBEpisodeError(f"missing required columns: {missing}")
    data = frame.loc[:, _REQUIRED].copy()
    data[TRADE_DATE] = pd.to_datetime(data[TRADE_DATE], errors="coerce").dt.normalize()
    if data[TRADE_DATE].isna().any() or data[[ASSET_ID, TRADE_DATE]].duplicated().any():
        raise SystemBEpisodeError("invalid or duplicate asset_id + trade_date")
    data = data.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    valid_states = {"BASE", "CANDIDATE", "ACTIVE"}
    if not data[TREND_STATE].dropna().isin(valid_states).all():
        raise SystemBEpisodeError("unsupported trend_state")
    numeric = data[[CLOSE, MA5, MA10]].apply(pd.to_numeric, errors="coerce")
    if numeric.isna().any().any() or not numeric.map(math.isfinite).all().all():
        raise SystemBEpisodeError("close, ma5 and ma10 must be finite")
    data[[CLOSE, MA5, MA10]] = numeric

    episodes: list[dict[str, object]] = []
    observations: list[dict[str, object]] = []
    for asset_id, asset in data.groupby(ASSET_ID, sort=False):
        episode_no = 0
        current: dict[str, object] | None = None
        previous_state: str | None = None
        previous_date: pd.Timestamp | None = None
        previous_close: float | None = None
        previous_below_ma10 = False
        observation_number = -1
        confirmed_observation_number = -1
        start_observation_number = -1
        peak_close = 0.0
        previous_episode_end: pd.Timestamp | None = None

        for row in asset.itertuples(index=False):
            observation_number += 1
            trade_date = getattr(row, TRADE_DATE)
            close = float(getattr(row, CLOSE))
            ma5 = float(getattr(row, MA5))
            ma10 = float(getattr(row, MA10))
            trend_state = str(getattr(row, TREND_STATE))
            transition = (
                f"{previous_state}->{trend_state}" if previous_state is not None and previous_state != trend_state else None
            )
            candidate_to_active = previous_state == "CANDIDATE" and trend_state == "ACTIVE"

            if candidate_to_active and current is None:
                if previous_date is None or previous_close is None:
                    raise SystemBEpisodeError("candidate confirmation lacks previous actual observation")
                if previous_episode_end is not None and previous_episode_end > previous_date:
                    raise SystemBEpisodeError("new episode start would cross the previous episode end date")
                episode_no += 1
                episode_id = f"{asset_id}_EP_{episode_no:04d}"
                current = {
                    EPISODE_ID: episode_id,
                    ASSET_ID: asset_id,
                    EPISODE_NO: episode_no,
                    EPISODE_START_DATE: previous_date,
                    EPISODE_CONFIRMED_DATE: trade_date,
                    EPISODE_END_DATE: pd.NaT,
                    MA5_REENTRY_COUNT: 0,
                    "start_close": previous_close,
                    "episode_index": len(episodes),
                }
                start_observation_number = observation_number - 1
                confirmed_observation_number = observation_number
                peak_close = max(previous_close, close)
                episodes.append(current)
            elif candidate_to_active and current is not None:
                current[MA5_REENTRY_COUNT] = int(current[MA5_REENTRY_COUNT]) + 1

            if current is not None:
                peak_close = max(peak_close, close)
                start_close = float(current["start_close"])
                episode_return = close / start_close - 1.0
                peak_return = peak_close / start_close - 1.0
                is_end = trend_state != "ACTIVE" and previous_below_ma10 and close < ma10
                if is_end:
                    current[EPISODE_END_DATE] = trade_date
                observations.append({
                    TRADE_DATE: trade_date,
                    ASSET_ID: asset_id,
                    EPISODE_ID: current[EPISODE_ID],
                    DAYS_SINCE_START: observation_number - start_observation_number,
                    DAYS_SINCE_CONFIRMED: observation_number - confirmed_observation_number,
                    CLOSE: close,
                    MA5: ma5,
                    MA10: ma10,
                    TREND_STATE: trend_state,
                    PREVIOUS_TREND_STATE: previous_state,
                    STATE_TRANSITION: transition,
                    EPISODE_RETURN: episode_return,
                    PEAK_RETURN: peak_return,
                    DRAWDOWN_FROM_PEAK: close / peak_close - 1.0,
                    MA5_REENTRY_COUNT: int(current[MA5_REENTRY_COUNT]),
                    IS_EPISODE_CONFIRMED: trade_date == current[EPISODE_CONFIRMED_DATE],
                    IS_EPISODE_END: is_end,
                })
                if is_end:
                    previous_episode_end = trade_date
                    current = None
                    previous_below_ma10 = False
                else:
                    previous_below_ma10 = close < ma10
            else:
                previous_below_ma10 = False

            previous_state = trend_state
            previous_date = trade_date
            previous_close = close

    episode_frame = pd.DataFrame(episodes)
    if not episode_frame.empty:
        episode_frame = episode_frame.loc[:, _EPISODE_COLUMNS]
    else:
        episode_frame = pd.DataFrame(columns=_EPISODE_COLUMNS)
    observation_frame = pd.DataFrame(observations, columns=_OBSERVATION_COLUMNS)
    return SystemBEpisodeResult(episode_frame.reset_index(drop=True), observation_frame.reset_index(drop=True))
