"""Pure calculation of price trend states and episodes for Theme custom index series."""

from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

from qrp_atlas.contracts import (
    CLOSE,
    COLLECTION_ID,
    CUSTOM_INDEX_TREND_RUN_DAYS,
    EPISODE_CONFIRMED_DATE,
    EPISODE_END_DATE,
    EPISODE_ID,
    EPISODE_NO,
    EPISODE_RETURN,
    EPISODE_START_DATE,
    INDEX_LEVEL,
    IS_ABOVE_OR_EQUAL_MA5,
    MA10,
    MA5,
    MA5_REENTRY_COUNT,
    PREVIOUS_TREND_STATE,
    RULE_VERSION,
    STATE_CHANGED,
    THEME_CUSTOM_INDEX_EPISODE_VERSION,
    THEME_CUSTOM_INDEX_STATE_VERSION,
    THEME_ID,
    TRADE_DATE,
    TREND_STATE,
)


@dataclass(frozen=True)
class ThemeIndexTrendAndEpisodeResult:
    states: pd.DataFrame
    episodes: pd.DataFrame


class ThemeTrendCalculationError(ValueError):
    """Raised when trend inputs are invalid."""


def calculate_theme_index_trend_and_episodes(
    index_daily: pd.DataFrame,
    state_rule_version: str = THEME_CUSTOM_INDEX_STATE_VERSION,
    episode_rule_version: str = THEME_CUSTOM_INDEX_EPISODE_VERSION,
) -> ThemeIndexTrendAndEpisodeResult:
    """Calculate MA5, MA10, trend states (BASE/CANDIDATE/ACTIVE) and episodes on Theme index.

    Note:
    - Theme Index is a continuous price series without new-listing warmup or delisting lifecycles.
    - MA5 / MA10 are rolling simple moving averages on index_level.
    """
    if index_daily.empty:
        return ThemeIndexTrendAndEpisodeResult(
            states=pd.DataFrame(
                columns=[
                    THEME_ID,
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
                    RULE_VERSION,
                ]
            ),
            episodes=pd.DataFrame(
                columns=[
                    EPISODE_ID,
                    THEME_ID,
                    COLLECTION_ID,
                    EPISODE_NO,
                    EPISODE_START_DATE,
                    EPISODE_CONFIRMED_DATE,
                    EPISODE_END_DATE,
                    MA5_REENTRY_COUNT,
                    EPISODE_RETURN,
                    RULE_VERSION,
                ]
            ),
        )

    df = index_daily.copy()
    df[TRADE_DATE] = pd.to_datetime(df[TRADE_DATE]).dt.date
    df = df.sort_values([THEME_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)

    state_rows: list[dict[str, object]] = []
    episode_rows: list[dict[str, object]] = []

    for (theme_id, collection_id), group in df.groupby([THEME_ID, COLLECTION_ID], sort=False):
        group = group.reset_index(drop=True)
        prices = group[INDEX_LEVEL].astype(float)
        ma5_series = prices.rolling(window=5, min_periods=5).mean()
        ma10_series = prices.rolling(window=10, min_periods=10).mean()

        group[CLOSE] = prices
        group[MA5] = ma5_series
        group[MA10] = ma10_series

        # Determine trend state for price series
        # State machine predicates:
        # - MA5 available:
        #   is_above_or_equal_ma5_today = close_t >= ma5_t
        #   is_above_or_equal_ma5_yesterday = close_{t-1} >= ma5_{t-1}
        # - BASE: not above today AND not above yesterday (or MA5 not complete)
        # - CANDIDATE: above today AND not above yesterday
        # - ACTIVE: above today AND above yesterday
        prev_close: float | None = None
        prev_ma5: float | None = None
        prev_state: str | None = None
        run_days = 0
        episode_no = 0

        # Episode tracking variables
        current_ep: dict[str, object] | None = None
        ep_start_price: float = 0.0
        reentry_count = 0
        was_non_active_in_ep = False

        for i, row in group.iterrows():
            t_date = row[TRADE_DATE]
            c = float(row[CLOSE])
            m5 = float(row[MA5]) if pd.notna(row[MA5]) else None
            m10 = float(row[MA10]) if pd.notna(row[MA10]) else None

            if m5 is None:
                state = "BASE"
                above_ma5 = None
            else:
                above_ma5 = c >= m5
                prev_above_ma5 = (prev_close >= prev_ma5) if (prev_close is not None and prev_ma5 is not None) else False

                if above_ma5 and prev_above_ma5:
                    state = "ACTIVE"
                elif above_ma5 and not prev_above_ma5:
                    state = "CANDIDATE"
                else:
                    state = "BASE"

            state_changed = (state != prev_state) if prev_state is not None else False
            if state == prev_state:
                run_days += 1
            else:
                run_days = 1

            # Episode lifecycle
            candidate_to_active = (prev_state == "CANDIDATE" and state == "ACTIVE")

            # Check if episode starts
            if candidate_to_active and current_ep is None:
                episode_no += 1
                ep_id = f"{theme_id}_EP_{episode_no:04d}"
                prev_date = group.loc[i - 1, TRADE_DATE] if i > 0 else t_date
                ep_start_price = float(group.loc[i - 1, CLOSE]) if i > 0 else c
                current_ep = {
                    EPISODE_ID: ep_id,
                    THEME_ID: theme_id,
                    COLLECTION_ID: collection_id,
                    EPISODE_NO: int(episode_no),
                    EPISODE_START_DATE: prev_date,
                    EPISODE_CONFIRMED_DATE: t_date,
                    EPISODE_END_DATE: None,
                    MA5_REENTRY_COUNT: 0,
                    EPISODE_RETURN: None,
                    RULE_VERSION: episode_rule_version,
                }
                reentry_count = 0
                was_non_active_in_ep = False

            elif current_ep is not None:
                # Reentry check
                if state != "ACTIVE":
                    was_non_active_in_ep = True
                elif state == "ACTIVE" and was_non_active_in_ep:
                    reentry_count += 1
                    was_non_active_in_ep = False
                    current_ep[MA5_REENTRY_COUNT] = int(reentry_count)

                # End check: close < ma10
                if m10 is not None and c < m10:
                    current_ep[EPISODE_END_DATE] = t_date
                    current_ep[EPISODE_RETURN] = (c - ep_start_price) / ep_start_price
                    episode_rows.append(current_ep)
                    current_ep = None

            state_rows.append(
                {
                    THEME_ID: theme_id,
                    COLLECTION_ID: collection_id,
                    TRADE_DATE: t_date,
                    CLOSE: c,
                    MA5: m5,
                    MA10: m10,
                    TREND_STATE: state,
                    PREVIOUS_TREND_STATE: prev_state,
                    CUSTOM_INDEX_TREND_RUN_DAYS: int(run_days),
                    IS_ABOVE_OR_EQUAL_MA5: above_ma5,
                    STATE_CHANGED: state_changed,
                    RULE_VERSION: state_rule_version,
                }
            )

            prev_close = c
            prev_ma5 = m5
            prev_state = state

        # If an episode is still open at the end of data
        if current_ep is not None:
            current_ep[EPISODE_RETURN] = (group.iloc[-1][CLOSE] - ep_start_price) / ep_start_price
            episode_rows.append(current_ep)

    states_df = pd.DataFrame(state_rows)
    episodes_df = pd.DataFrame(episode_rows)

    return ThemeIndexTrendAndEpisodeResult(
        states=states_df,
        episodes=episodes_df,
    )
