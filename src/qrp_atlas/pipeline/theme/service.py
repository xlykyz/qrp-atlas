"""Production pipeline service for Theme Custom Index, Trend States, Episodes and M4 Observations."""

from __future__ import annotations

from datetime import date, datetime
from typing import Sequence
import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    CLOSE,
    COLLECTION_ID,
    COMPARISON_UNIVERSE_VERSION_V1,
    IS_LIMIT_UP,
    IS_M4_EFFECTIVE_MEMBER,
    IS_OPEN,
    M4_CALCULATION_VERSION,
    STOCK_INFO,
    SUSPEND_D,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_VERSION,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_CUSTOM_INDEX_STATE_VERSION,
    THEME_CUSTOM_INDEX_VERSION,
    THEME_ID,
    THEME_M4_OBSERVATION_TABLE,
    THEME_MEMBERSHIP_HISTORY_TABLE,
    THS_DAILY,
    TRADE_DATE,
    TRADING_CALENDAR,
)
from qrp_atlas.indicators.m4.observations import calculate_m4_raw_observations
from qrp_atlas.indicators.theme.custom_index import calculate_theme_equal_weight_index
from qrp_atlas.indicators.theme.effective_members import calculate_m4_effective_members
from qrp_atlas.indicators.theme.trend_and_episode import calculate_theme_index_trend_and_episodes


class ThemePipelineProductionError(RuntimeError):
    """Raised when theme pipeline production or audit fails."""


class ThemePipelineService:
    """Service executing batch historical rebuild and daily production runs for M4 facts."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.con = connection

    def rebuild_m4_facts(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        knowledge_date: date | None = None,
        comparison_universe_version: str = COMPARISON_UNIVERSE_VERSION_V1,
    ) -> dict[str, int]:
        """Replay and atomically persist Theme Index, States, Episodes and M4 Observations across target date range."""
        k_date = knowledge_date or date.today()

        # 1. Fetch available trade dates
        date_clauses = ["is_open = true"]
        date_params: list[object] = []
        if start_date is not None:
            date_clauses.append("trade_date >= ?")
            date_params.append(start_date)
        if end_date is not None:
            date_clauses.append("trade_date <= ?")
            date_params.append(end_date)

        where_dates = " AND ".join(date_clauses)
        calendar_rows = self.con.execute(
            f"SELECT trade_date FROM trading_calendar WHERE {where_dates} ORDER BY trade_date ASC",
            date_params,
        ).fetchall()

        if not calendar_rows:
            return {
                "theme_custom_index_daily": 0,
                "theme_custom_index_state": 0,
                "theme_custom_index_episode": 0,
                "theme_m4_observation": 0,
            }

        target_dates = [r[0] if isinstance(r[0], date) else pd.to_datetime(r[0]).date() for r in calendar_rows]
        min_date = target_dates[0]
        max_date = target_dates[-1]

        # 2. Vectorized PIT Membership Resolution (All Themes across all target dates)
        # Fetch visible revisions as of k_date
        mem_sql = f"""
        WITH visible_revs AS (
            SELECT
                membership_id,
                theme_id,
                collection_id,
                asset_id,
                effective_from,
                effective_to,
                available_trade_date,
                ROW_NUMBER() OVER (
                    PARTITION BY membership_id
                    ORDER BY available_trade_date DESC, ingested_at DESC
                ) as rn
            FROM {THEME_MEMBERSHIP_HISTORY_TABLE}
            WHERE available_trade_date <= ?
        ),
        latest_revs AS (
            SELECT * FROM visible_revs WHERE rn = 1
        )
        SELECT
            c.trade_date,
            m.theme_id,
            m.collection_id,
            m.asset_id
        FROM latest_revs m
        JOIN trading_calendar c
          ON c.trade_date >= m.effective_from
         AND (m.effective_to IS NULL OR c.trade_date < m.effective_to)
         AND c.trade_date BETWEEN ? AND ?
         AND c.is_open = true
        ORDER BY c.trade_date, m.theme_id, m.asset_id
        """
        mem_df = self.con.execute(mem_sql, [k_date, min_date, max_date]).df()
        if mem_df.empty:
            return {
                "theme_custom_index_daily": 0,
                "theme_custom_index_state": 0,
                "theme_custom_index_episode": 0,
                "theme_m4_observation": 0,
            }

        mem_df[TRADE_DATE] = pd.to_datetime(mem_df[TRADE_DATE]).dt.date

        # 3. Fetch Market facts (daily_market_snapshot)
        mkt_sql = """
        SELECT
            trade_date,
            ticker AS asset_id,
            open,
            high,
            low,
            close,
            pre_close,
            pct_change,
            COALESCE(is_limit_up, false) AS is_limit_up
        FROM daily_market_snapshot
        WHERE trade_date BETWEEN ? AND ?
        """
        mkt_df = self.con.execute(mkt_sql, [min_date, max_date]).df()
        mkt_df[TRADE_DATE] = pd.to_datetime(mkt_df[TRADE_DATE]).dt.date

        # 4. Fetch Suspension facts
        susp_sql = """
        SELECT
            trade_date,
            ticker AS asset_id,
            true AS is_suspended
        FROM suspend_d
        WHERE trade_date BETWEEN ? AND ?
        """
        susp_df = self.con.execute(susp_sql, [min_date, max_date]).df()
        if not susp_df.empty:
            susp_df[TRADE_DATE] = pd.to_datetime(susp_df[TRADE_DATE]).dt.date

        # 5. Fetch Listing Trading Days facts
        listing_sql = """
        WITH stock_dates AS (
            SELECT
                s.ticker AS asset_id,
                c.trade_date,
                CASE
                    WHEN (c.trade_date - s.list_date) > 30 THEN 999999
                    ELSE COUNT(c2.trade_date)
                END AS confirmed_listing_trading_day_count
            FROM stock_info s
            JOIN trading_calendar c
              ON c.trade_date >= s.list_date
             AND (s.delist_date IS NULL OR c.trade_date <= s.delist_date)
             AND c.trade_date BETWEEN ? AND ?
             AND c.is_open = true
            LEFT JOIN trading_calendar c2
              ON c2.trade_date >= s.list_date
             AND c2.trade_date <= c.trade_date
             AND c2.is_open = true
            GROUP BY s.ticker, s.list_date, c.trade_date
        )
        SELECT * FROM stock_dates
        """
        try:
            listing_df = self.con.execute(listing_sql, [min_date, max_date]).df()
            listing_df[TRADE_DATE] = pd.to_datetime(listing_df[TRADE_DATE]).dt.date
        except Exception:
            listing_df = pd.DataFrame(columns=[ASSET_ID, TRADE_DATE, "confirmed_listing_trading_day_count"])

        # 6. Execute Pure Indicator Calculations
        eff_members_df = calculate_m4_effective_members(mem_df, listing_df, susp_df)
        theme_index_df = calculate_theme_equal_weight_index(eff_members_df, mkt_df)
        trend_res = calculate_theme_index_trend_and_episodes(theme_index_df)

        # 7. Fetch Comparison Boards
        comp_boards_df: pd.DataFrame | None = None
        try:
            comp_sql = """
            SELECT
                trade_date,
                index_code AS board_id,
                pct_change
            FROM ths_daily
            WHERE trade_date BETWEEN ? AND ?
            """
            comp_boards_df = self.con.execute(comp_sql, [min_date, max_date]).df()
            comp_boards_df[TRADE_DATE] = pd.to_datetime(comp_boards_df[TRADE_DATE]).dt.date
        except Exception:
            comp_boards_df = None

        m4_obs_df = calculate_m4_raw_observations(
            theme_indices=theme_index_df,
            effective_members=eff_members_df,
            market_snapshot=mkt_df,
            comparison_boards=comp_boards_df,
            comparison_universe_version=comparison_universe_version,
            theme_states=trend_res.states,
            theme_episodes=trend_res.episodes,
        )

        # 8. Single Transaction Atomic Overwrite
        now = datetime.now()
        with self.con.cursor() as cur:
            cur.execute("BEGIN TRANSACTION")
            try:
                # Delete old target date range
                cur.execute(
                    f"DELETE FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE trade_date BETWEEN ? AND ?",
                    [min_date, max_date],
                )
                cur.execute(
                    f"DELETE FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE trade_date BETWEEN ? AND ?",
                    [min_date, max_date],
                )
                cur.execute(
                    f"DELETE FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date BETWEEN ? AND ?",
                    [min_date, max_date],
                )
                # For episodes, delete those belonging to updated themes starting in range
                if not theme_index_df.empty:
                    theme_ids = list(theme_index_df[THEME_ID].unique())
                    cur.execute(
                        f"DELETE FROM {THEME_CUSTOM_INDEX_EPISODE_TABLE} WHERE theme_id IN (SELECT unnest(?))",
                        [theme_ids],
                    )

                # Insert Index Daily
                if not theme_index_df.empty:
                    theme_index_df["created_at"] = now
                    cur.register("tmp_theme_index", theme_index_df)
                    cols_daily = ", ".join(theme_index_df.columns)
                    cur.execute(f"INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE} ({cols_daily}) SELECT {cols_daily} FROM tmp_theme_index")
                    cur.unregister("tmp_theme_index")

                # Insert States
                if not trend_res.states.empty:
                    trend_res.states["created_at"] = now
                    cur.register("tmp_theme_states", trend_res.states)
                    cols_states = ", ".join(trend_res.states.columns)
                    cur.execute(f"INSERT INTO {THEME_CUSTOM_INDEX_STATE_TABLE} ({cols_states}) SELECT {cols_states} FROM tmp_theme_states")
                    cur.unregister("tmp_theme_states")

                # Insert Episodes
                if not trend_res.episodes.empty:
                    trend_res.episodes["created_at"] = now
                    cur.register("tmp_theme_episodes", trend_res.episodes)
                    cols_episodes = ", ".join(trend_res.episodes.columns)
                    cur.execute(f"INSERT INTO {THEME_CUSTOM_INDEX_EPISODE_TABLE} ({cols_episodes}) SELECT {cols_episodes} FROM tmp_theme_episodes")
                    cur.unregister("tmp_theme_episodes")

                # Insert M4 Observations
                if not m4_obs_df.empty:
                    m4_obs_df["created_at"] = now
                    cur.register("tmp_theme_m4_obs", m4_obs_df)
                    cols_m4 = ", ".join(m4_obs_df.columns)
                    cur.execute(f"INSERT INTO {THEME_M4_OBSERVATION_TABLE} ({cols_m4}) SELECT {cols_m4} FROM tmp_theme_m4_obs")
                    cur.unregister("tmp_theme_m4_obs")

                cur.execute("COMMIT")
            except Exception as exc:
                cur.execute("ROLLBACK")
                raise ThemePipelineProductionError(f"M4 rebuild transaction failed: {exc}") from exc

        return {
            "theme_custom_index_daily": len(theme_index_df),
            "theme_custom_index_state": len(trend_res.states),
            "theme_custom_index_episode": len(trend_res.episodes),
            "theme_m4_observation": len(m4_obs_df),
        }

    def run_m4_daily(
        self,
        trade_date: date,
        knowledge_date: date | None = None,
        comparison_universe_version: str = COMPARISON_UNIVERSE_VERSION_V1,
    ) -> dict[str, int]:
        """Run daily production for a single trade date."""
        return self.rebuild_m4_facts(
            start_date=trade_date,
            end_date=trade_date,
            knowledge_date=knowledge_date,
            comparison_universe_version=comparison_universe_version,
        )
