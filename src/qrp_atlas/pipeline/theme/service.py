"""High-throughput Theme Custom Index and M4 Observation production pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import uuid

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    CLOSE,
    CALCULATION_VERSION,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    DAILY_MARKET_SNAPSHOT,
    IS_LIMIT_UP,
    IS_SUSPENDED,
    IS_TRADING_DAY,
    MA10,
    MA5,
    STOCK_INFO,
    SUSPEND_D,
    THS_DAILY,
    TRADE_DATE,
    TRADING_CALENDAR,
    TREND_STATE,
    PREVIOUS_TREND_STATE,
)
from qrp_atlas.contracts.m4 import (
    BASE_LEVEL,
    COMPARISON_UNIVERSE_SIZE,
    COMPARISON_UNIVERSE_VERSION,
    COMPARISON_UNIVERSE_VERSION_V1,
    CUSTOM_INDEX_EPISODE_ID,
    CUSTOM_INDEX_TREND_RUN_DAYS,
    CUSTOM_INDEX_TREND_STATE,
    DEFAULT_BASE_LEVEL,
    EFFECTIVE_MEMBER_COUNT,
    INDEX_LEVEL,
    IS_M4_EFFECTIVE_MEMBER,
    M4_CALCULATION_VERSION,
    QUALIFICATION_STATUS,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_VERSION,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_CUSTOM_INDEX_STATE_VERSION,
    THEME_CUSTOM_INDEX_VERSION,
    THEME_DAILY_RETURN,
    THEME_LIMIT_UP_COUNT,
    THEME_M4_OBSERVATION_TABLE,
    THEME_M4_OBSERVATION_VERSION,
    THEME_RETURN_RANK,
    TOTAL_MEMBER_COUNT,
)
from qrp_atlas.contracts.schema import PRODUCTION_RUN_ID, INPUT_SNAPSHOT_ID, CREATED_AT, RULE_VERSION
from qrp_atlas.contracts.stock_collection import (
    COLLECTION_ID,
    CollectionType,
    THEME_ID,
    THEME_TABLE,
)
from qrp_atlas.indicators.m4.observations import (
    M4ObservationError,
    calculate_m4_raw_observations,
)
from qrp_atlas.indicators.theme.custom_index import calculate_theme_equal_weight_index
from qrp_atlas.indicators.theme.effective_members import calculate_m4_effective_members
from qrp_atlas.indicators.theme.trend_and_episode import (
    calculate_theme_index_trend_and_episodes,
)
from qrp_atlas.stock_collections.resolver import StockCollectionResolver


class ThemePipelineError(ValueError):
    """Pipeline production error with code and detail."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class ThemeProductionReport:
    production_run_id: str
    input_snapshot_id: str
    theme_count: int
    trade_date_count: int
    start_date: date
    end_date: date
    total_index_rows: int
    total_observation_rows: int
    total_episodes: int
    execution_seconds: float


class ThemePipelineService:
    """Production service for Theme Custom Indices and M4 Observations."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con
        self.resolver = StockCollectionResolver(con)

    def _fetch_trading_calendar_dates(self, start_date: date, end_date: date) -> list[date]:
        rows = self.con.execute(
            """
            SELECT trade_date FROM trading_calendar
            WHERE trade_date BETWEEN ? AND ? AND is_open = true
            ORDER BY trade_date ASC
            """,
            [start_date, end_date],
        ).fetchall()
        return [r[0] for r in rows]

    def _fetch_all_canonical_themes(self, knowledge_date: date) -> list[tuple[str, str]]:
        rows = self.con.execute(
            f"""
            WITH ranked AS (
                SELECT theme_id, collection_id, status,
                       row_number() OVER (PARTITION BY theme_id ORDER BY available_trade_date DESC, ingested_at DESC) as rn
                FROM {THEME_TABLE}
                WHERE available_trade_date <= ?
            )
            SELECT theme_id, collection_id FROM ranked WHERE rn = 1 AND status = 'ACTIVE'
            ORDER BY theme_id ASC
            """,
            [knowledge_date],
        ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def _fetch_confirmed_listing_facts(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Fetch exact confirmed listing actual trading day counts without guessing."""
        sql = """
        WITH stock_dates AS (
            SELECT
                s.ticker AS asset_id,
                c.trade_date,
                COUNT(c2.trade_date) AS confirmed_listing_trading_day_count
            FROM stock_info s
            JOIN trading_calendar c
              ON c.trade_date >= s.list_date
             AND (s.delist_date IS NULL OR c.trade_date <= s.delist_date)
             AND c.trade_date BETWEEN ? AND ?
             AND c.is_open = true
            JOIN trading_calendar c2
              ON c2.trade_date >= s.list_date
             AND c2.trade_date <= c.trade_date
             AND c2.is_open = true
            GROUP BY s.ticker, c.trade_date
        )
        SELECT * FROM stock_dates
        """
        try:
            return self.con.execute(sql, [start_date, end_date]).df()
        except Exception:
            return pd.DataFrame(columns=[ASSET_ID, TRADE_DATE, CONFIRMED_LISTING_TRADING_DAY_COUNT])

    def _fetch_suspension_facts(self, start_date: date, end_date: date) -> pd.DataFrame:
        sql = """
        SELECT ticker AS asset_id, trade_date, true AS is_suspended
        FROM suspend_d
        WHERE trade_date BETWEEN ? AND ?
        """
        try:
            return self.con.execute(sql, [start_date, end_date]).df()
        except Exception:
            return pd.DataFrame(columns=[ASSET_ID, TRADE_DATE, IS_SUSPENDED])

    def _fetch_market_snapshot(self, start_date: date, end_date: date) -> pd.DataFrame:
        sql = """
        SELECT ticker AS asset_id, trade_date, pct_change, close, (pct_change >= 9.8) AS is_limit_up
        FROM daily_market_snapshot
        WHERE trade_date BETWEEN ? AND ?
        """
        return self.con.execute(sql, [start_date, end_date]).df()

    def _fetch_comparison_boards(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Fetch comparison universe (THS Industry & Concept indices: 881xxx, 885xxx, 886xxx)."""
        sql = """
        SELECT
            index_code AS board_id,
            trade_date,
            pct_change / 100.0 AS board_return
        FROM ths_daily
        WHERE trade_date BETWEEN ? AND ?
          AND (index_code LIKE '881%' OR index_code LIKE '885%' OR index_code LIKE '886%')
        ORDER BY trade_date, index_code
        """
        try:
            df = self.con.execute(sql, [start_date, end_date]).df()
            if df.empty:
                # Fallback: if ths_daily has records without TI prefix filtering
                df = self.con.execute(
                    """
                    SELECT index_code AS board_id, trade_date, pct_change / 100.0 AS board_return
                    FROM ths_daily
                    WHERE trade_date BETWEEN ? AND ?
                    """,
                    [start_date, end_date],
                ).df()
            return df
        except Exception as exc:
            raise ThemePipelineError(
                "COMPARISON_UNIVERSE_QUERY_FAILED",
                f"Failed to query THS comparison universe: {exc}",
            ) from exc

    def rebuild_m4_facts(
        self,
        start_date: date,
        end_date: date,
        knowledge_date: date | None = None,
        production_run_id: str | None = None,
    ) -> ThemeProductionReport:
        """Full or range rebuild of Theme Custom Indices and M4 Observations."""
        t0 = datetime.now(timezone.utc)
        kd = knowledge_date or end_date
        run_id = production_run_id or f"RUN:THEME_M4:{uuid.uuid4().hex[:12].upper()}"
        snap_id = f"SNAP:{uuid.uuid4().hex[:12].upper()}"

        trade_dates = self._fetch_trading_calendar_dates(start_date, end_date)
        if not trade_dates:
            raise ThemePipelineError(
                "NO_TRADING_DATES", f"No open trading dates between {start_date} and {end_date}"
            )

        themes = self._fetch_all_canonical_themes(kd)
        if not themes:
            raise ThemePipelineError("NO_ACTIVE_THEMES", f"No active themes as of {kd}")

        theme_map = {coll_id: theme_id for theme_id, coll_id in themes}
        coll_ids = list(theme_map.keys())

        # 1. Fetch set-based memberships via batch Resolver
        raw_memberships = self.resolver.batch_resolve_members(coll_ids, trade_dates, kd)

        # 2. Fetch facts
        listing_df = self._fetch_confirmed_listing_facts(start_date, end_date)
        susp_df = self._fetch_suspension_facts(start_date, end_date)
        market_df = self._fetch_market_snapshot(start_date, end_date)
        comp_boards_df = self._fetch_comparison_boards(start_date, end_date)

        # 3. Calculate Effective Members
        eff_members_df = calculate_m4_effective_members(raw_memberships, listing_df, susp_df)

        # 4. Calculate Custom Index per Theme
        index_daily_list = []
        state_list = []
        episode_list = []

        for theme_id, coll_id in themes:
            coll_eff = eff_members_df[eff_members_df[COLLECTION_ID] == coll_id]
            # Custom Index calculation
            idx_df = calculate_theme_equal_weight_index(coll_eff, market_df)
            if idx_df.empty:
                continue

            # Add theme_id
            idx_df[THEME_ID] = theme_id
            index_daily_list.append(idx_df)

            # Trend & Episode calculation
            trend_res = calculate_theme_index_trend_and_episodes(idx_df, theme_id=theme_id)
            states_df = trend_res.daily_states.copy()
            states_df[THEME_ID] = theme_id
            state_list.append(states_df)

            eps_df = trend_res.episodes.copy()
            if not eps_df.empty:
                eps_df[THEME_ID] = theme_id
                episode_list.append(eps_df)

        if not index_daily_list:
            all_index_daily = pd.DataFrame()
            all_states = pd.DataFrame()
            all_episodes = pd.DataFrame()
        else:
            all_index_daily = pd.concat(index_daily_list, ignore_index=True)
            all_states = pd.concat(state_list, ignore_index=True)
            all_episodes = pd.concat(episode_list, ignore_index=True) if episode_list else pd.DataFrame()

        # 5. Calculate M4 Raw Observations
        m4_obs_df = calculate_m4_raw_observations(
            all_index_daily,
            eff_members_df,
            market_df,
            comp_boards_df,
            comparison_universe_version=COMPARISON_UNIVERSE_VERSION_V1,
        )

        # Merge state and episode info into M4 observations
        if not m4_obs_df.empty and not all_states.empty:
            m4_obs_df = m4_obs_df.merge(
                all_states[[COLLECTION_ID, TRADE_DATE, TREND_STATE, CUSTOM_INDEX_TREND_RUN_DAYS, CUSTOM_INDEX_EPISODE_ID]],
                on=[COLLECTION_ID, TRADE_DATE],
                how="left",
                suffixes=("", "_st"),
            )
            if CUSTOM_INDEX_TREND_STATE not in m4_obs_df.columns:
                m4_obs_df[CUSTOM_INDEX_TREND_STATE] = m4_obs_df[TREND_STATE]
            m4_obs_df[THEME_ID] = m4_obs_df[COLLECTION_ID].map(theme_map)

        # 6. Single ACID Transaction Persistence
        now = datetime.now(timezone.utc)
        try:
            self.con.execute("BEGIN TRANSACTION")
            # Delete target range
            self.con.execute(
                f"DELETE FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE trade_date BETWEEN ? AND ?",
                [start_date, end_date],
            )
            self.con.execute(
                f"DELETE FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE trade_date BETWEEN ? AND ?",
                [start_date, end_date],
            )
            self.con.execute(
                f"DELETE FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date BETWEEN ? AND ?",
                [start_date, end_date],
            )
            # Delete episodes overlapping range
            self.con.execute(
                f"DELETE FROM {THEME_CUSTOM_INDEX_EPISODE_TABLE} WHERE episode_confirmed_date BETWEEN ? AND ?",
                [start_date, end_date],
            )

            # Insert Daily Indices
            if not all_index_daily.empty:
                for r in all_index_daily.itertuples(index=False):
                    self.con.execute(
                        f"""
                        INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE} (
                            theme_id, collection_id, trade_date, theme_daily_return,
                            index_level, base_level, effective_member_count, total_member_count,
                            calculation_version, production_run_id, input_snapshot_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            getattr(r, THEME_ID),
                            getattr(r, COLLECTION_ID),
                            getattr(r, TRADE_DATE),
                            getattr(r, THEME_DAILY_RETURN) if pd.notna(getattr(r, THEME_DAILY_RETURN)) else None,
                            getattr(r, INDEX_LEVEL) if pd.notna(getattr(r, INDEX_LEVEL)) else None,
                            getattr(r, BASE_LEVEL),
                            getattr(r, EFFECTIVE_MEMBER_COUNT),
                            getattr(r, TOTAL_MEMBER_COUNT),
                            THEME_CUSTOM_INDEX_VERSION,
                            run_id,
                            snap_id,
                            now,
                        ],
                    )

            # Insert States
            if not all_states.empty:
                for r in all_states.itertuples(index=False):
                    self.con.execute(
                        f"""
                        INSERT INTO {THEME_CUSTOM_INDEX_STATE_TABLE} (
                            theme_id, collection_id, trade_date, close, ma5, ma10,
                            trend_state, previous_trend_state, custom_index_trend_run_days,
                            is_above_or_equal_ma5, state_changed, rule_version,
                            production_run_id, input_snapshot_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            getattr(r, THEME_ID),
                            getattr(r, COLLECTION_ID),
                            getattr(r, TRADE_DATE),
                            getattr(r, CLOSE) if pd.notna(getattr(r, CLOSE)) else None,
                            getattr(r, MA5) if pd.notna(getattr(r, MA5)) else None,
                            getattr(r, MA10) if pd.notna(getattr(r, MA10)) else None,
                            getattr(r, TREND_STATE) if pd.notna(getattr(r, TREND_STATE)) else None,
                            getattr(r, PREVIOUS_TREND_STATE) if pd.notna(getattr(r, PREVIOUS_TREND_STATE)) else None,
                            getattr(r, CUSTOM_INDEX_TREND_RUN_DAYS),
                            getattr(r, "is_above_or_equal_ma5") if pd.notna(getattr(r, "is_above_or_equal_ma5")) else None,
                            getattr(r, "state_changed") if pd.notna(getattr(r, "state_changed")) else None,
                            THEME_CUSTOM_INDEX_STATE_VERSION,
                            run_id,
                            snap_id,
                            now,
                        ],
                    )

            # Insert Episodes
            if not all_episodes.empty:
                for r in all_episodes.itertuples(index=False):
                    self.con.execute(
                        f"""
                        INSERT INTO {THEME_CUSTOM_INDEX_EPISODE_TABLE} (
                            episode_id, theme_id, collection_id, episode_no,
                            episode_start_date, episode_confirmed_date, episode_end_date,
                            ma5_reentry_count, episode_return, rule_version,
                            production_run_id, input_snapshot_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            getattr(r, "episode_id"),
                            getattr(r, THEME_ID),
                            getattr(r, COLLECTION_ID),
                            getattr(r, "episode_no"),
                            getattr(r, "episode_start_date"),
                            getattr(r, "episode_confirmed_date"),
                            getattr(r, "episode_end_date") if pd.notna(getattr(r, "episode_end_date")) else None,
                            getattr(r, "ma5_reentry_count"),
                            getattr(r, "episode_return") if pd.notna(getattr(r, "episode_return")) else None,
                            THEME_CUSTOM_INDEX_EPISODE_VERSION,
                            run_id,
                            snap_id,
                            now,
                        ],
                    )

            # Insert M4 Observations
            if not m4_obs_df.empty:
                for r in m4_obs_df.itertuples(index=False):
                    self.con.execute(
                        f"""
                        INSERT INTO {THEME_M4_OBSERVATION_TABLE} (
                            theme_id, collection_id, trade_date, theme_daily_return,
                            theme_limit_up_count, theme_return_rank, effective_member_count,
                            total_member_count, comparison_universe_size, comparison_universe_version,
                            custom_index_trend_state, custom_index_trend_run_days, custom_index_episode_id,
                            qualification_status, calculation_version, production_run_id, input_snapshot_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            getattr(r, THEME_ID),
                            getattr(r, COLLECTION_ID),
                            getattr(r, TRADE_DATE),
                            getattr(r, THEME_DAILY_RETURN) if pd.notna(getattr(r, THEME_DAILY_RETURN)) else None,
                            getattr(r, THEME_LIMIT_UP_COUNT),
                            getattr(r, THEME_RETURN_RANK) if pd.notna(getattr(r, THEME_RETURN_RANK)) else None,
                            getattr(r, EFFECTIVE_MEMBER_COUNT),
                            getattr(r, TOTAL_MEMBER_COUNT),
                            getattr(r, COMPARISON_UNIVERSE_SIZE),
                            getattr(r, COMPARISON_UNIVERSE_VERSION),
                            getattr(r, CUSTOM_INDEX_TREND_STATE) if pd.notna(getattr(r, CUSTOM_INDEX_TREND_STATE)) else None,
                            getattr(r, CUSTOM_INDEX_TREND_RUN_DAYS) if pd.notna(getattr(r, CUSTOM_INDEX_TREND_RUN_DAYS)) else None,
                            getattr(r, CUSTOM_INDEX_EPISODE_ID) if pd.notna(getattr(r, CUSTOM_INDEX_EPISODE_ID)) else None,
                            getattr(r, QUALIFICATION_STATUS),
                            THEME_M4_OBSERVATION_VERSION,
                            run_id,
                            snap_id,
                            now,
                        ],
                    )

            self.con.execute("COMMIT")
        except Exception as exc:
            try:
                self.con.execute("ROLLBACK")
            except Exception:
                pass
            raise ThemePipelineError("PRODUCTION_TRANSACTION_FAILED", str(exc)) from exc

        exec_sec = (datetime.now(timezone.utc) - t0).total_seconds()
        return ThemeProductionReport(
            production_run_id=run_id,
            input_snapshot_id=snap_id,
            theme_count=len(themes),
            trade_date_count=len(trade_dates),
            start_date=start_date,
            end_date=end_date,
            total_index_rows=len(all_index_daily),
            total_observation_rows=len(m4_obs_df),
            total_episodes=len(all_episodes),
            execution_seconds=exec_sec,
        )

    def run_m4_daily(
        self,
        trade_date: date,
        start_date: date | None = None,
        knowledge_date: date | None = None,
        production_run_id: str | None = None,
    ) -> ThemeProductionReport:
        """Daily production preserving complete historical sequence and continuous index levels."""
        if start_date is None:
            min_date_row = self.con.execute(
                f"SELECT MIN(effective_from) FROM {THEME_TABLE} WHERE available_trade_date <= ?",
                [knowledge_date or trade_date],
            ).fetchone()
            start_date = min_date_row[0] if min_date_row and min_date_row[0] else trade_date

        return self.rebuild_m4_facts(
            start_date=start_date,
            end_date=trade_date,
            knowledge_date=knowledge_date or trade_date,
            production_run_id=production_run_id,
        )
