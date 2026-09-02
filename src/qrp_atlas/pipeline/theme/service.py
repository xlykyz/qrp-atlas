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
from qrp_atlas.contracts.fields import (
    KNOWLEDGE_DATE,
    THEME_PRODUCTION_RUN_TABLE,
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
from qrp_atlas.pipeline.market_facts import query_confirmed_listing_facts
from qrp_atlas.stock_collections.resolver import StockCollectionResolver


def compute_deterministic_snapshot_id(
    themes: list[tuple[str, str]],
    memberships: pd.DataFrame,
    listing_df: pd.DataFrame,
    susp_df: pd.DataFrame,
    market_df: pd.DataFrame,
    comp_boards_df: pd.DataFrame,
    versions: dict[str, str],
) -> str:
    """Derive deterministic SHA256 digest over logical input facts and versions."""
    hasher = hashlib.sha256()
    for k in sorted(versions.keys()):
        hasher.update(f"{k}={versions[k]}\n".encode("utf-8"))
    for tid, cid in sorted(themes):
        hasher.update(f"theme:{tid}:{cid}\n".encode("utf-8"))
    for name, df in [
        ("mem", memberships),
        ("list", listing_df),
        ("susp", susp_df),
        ("mkt", market_df),
        ("comp", comp_boards_df),
    ]:
        if df.empty:
            hasher.update(f"{name}:empty\n".encode("utf-8"))
        else:
            sorted_cols = sorted(df.columns)
            sdf = df[sorted_cols].sort_values(by=sorted_cols).astype(str)
            hasher.update(f"{name}:\n".encode("utf-8"))
            hasher.update(sdf.to_csv(index=False).encode("utf-8"))
    return f"SNAP:{hasher.hexdigest()[:24].upper()}"


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
        """Fetch exact confirmed listing actual trading day counts strictly reusing System B semantics."""
        return query_confirmed_listing_facts(
            self.con,
            end_date=end_date,
            start_date=start_date,
        )

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
        """Fetch market snapshot strictly using official is_limit_up without heuristics."""
        sql = """
        SELECT ticker AS asset_id, trade_date, pct_change, close, is_limit_up
        FROM daily_market_snapshot
        WHERE trade_date BETWEEN ? AND ?
        """
        return self.con.execute(sql, [start_date, end_date]).df()

    def _fetch_comparison_boards(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Fetch comparison universe (THS Industry & Concept indices: 881xxx, 885xxx, 886xxx). Fail-closed on missing."""
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
            return self.con.execute(sql, [start_date, end_date]).df()
        except Exception as exc:
            raise ThemePipelineError(
                "COMPARISON_UNIVERSE_QUERY_FAILED",
                f"Failed to query THS comparison universe: {exc}",
            ) from exc

    def rebuild_m4_facts(
        self,
        start_date: date,
        end_date: date,
        context_start_date: date | None = None,
        knowledge_date: date | None = None,
        production_run_id: str | None = None,
        run_type: str = "REPLAY",
        execution_control: Any | None = None,
    ) -> ThemeProductionReport:
        """Range rebuild or daily production of Theme Custom Indices and M4 Observations.

        Distinguishes calculation context range [context_start_date, end_date] from requested
        output range [start_date, end_date].
        """
        t0 = datetime.now(timezone.utc)
        kd = knowledge_date or end_date
        run_id = production_run_id or f"RUN:THEME_M4:{uuid.uuid4().hex[:12].upper()}"

        themes = self._fetch_all_canonical_themes(kd)
        if not themes:
            raise ThemePipelineError("NO_ACTIVE_THEMES", f"No active themes as of {kd}")

        theme_map = {coll_id: theme_id for theme_id, coll_id in themes}
        coll_ids = list(theme_map.keys())

        # Determine calculation context start: must start at earliest theme inception to preserve compounding
        if context_start_date is None:
            min_date_row = self.con.execute(
                f"SELECT MIN(effective_from) FROM {THEME_TABLE} WHERE available_trade_date <= ?",
                [kd],
            ).fetchone()
            earliest_theme_date = min_date_row[0] if min_date_row and min_date_row[0] else start_date
            calc_start = min(earliest_theme_date, start_date)
        else:
            calc_start = min(context_start_date, start_date)

        calc_trade_dates = self._fetch_trading_calendar_dates(calc_start, end_date)
        if not calc_trade_dates:
            raise ThemePipelineError(
                "NO_TRADING_DATES", f"No open trading dates between {calc_start} and {end_date}"
            )

        output_trade_dates = [d for d in calc_trade_dates if start_date <= d <= end_date]

        # 1. Fetch set-based memberships via batch Resolver across calculation range
        raw_memberships = self.resolver.batch_resolve_members(coll_ids, calc_trade_dates, kd)

        # 2. Fetch facts across calculation context range
        listing_df = self._fetch_confirmed_listing_facts(calc_start, end_date)
        susp_df = self._fetch_suspension_facts(calc_start, end_date)
        market_df = self._fetch_market_snapshot(calc_start, end_date)
        comp_boards_df = self._fetch_comparison_boards(calc_start, end_date)

        if execution_control is not None and hasattr(execution_control, "checkpoint"):
            execution_control.checkpoint()

        # Compute deterministic snapshot identity over facts and versions
        versions = {
            "calculation_version": THEME_M4_OBSERVATION_VERSION,
            "rule_version": THEME_CUSTOM_INDEX_STATE_VERSION,
            "comparison_universe_version": COMPARISON_UNIVERSE_VERSION_V1,
        }
        snap_id = compute_deterministic_snapshot_id(
            themes=themes,
            memberships=raw_memberships,
            listing_df=listing_df,
            susp_df=susp_df,
            market_df=market_df,
            comp_boards_df=comp_boards_df,
            versions=versions,
        )

        # 3. Calculate Effective Members
        eff_members_df = calculate_m4_effective_members(raw_memberships, listing_df, susp_df)

        # 4. Calculate Custom Index per Theme continuously from calc_start
        index_daily_list = []
        state_list = []
        episode_list = []

        for theme_id, coll_id in themes:
            if execution_control is not None and hasattr(execution_control, "checkpoint"):
                execution_control.checkpoint()

            coll_eff = eff_members_df[eff_members_df[COLLECTION_ID] == coll_id]
            idx_df = calculate_theme_equal_weight_index(coll_eff, market_df)
            if idx_df.empty:
                continue

            idx_df[THEME_ID] = theme_id
            index_daily_list.append(idx_df)

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

        # 6. Slice records strictly to requested output range [start_date, end_date]
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        def _to_date(val: Any) -> date:
            if isinstance(val, date) and not isinstance(val, datetime):
                return val
            if hasattr(val, "date"):
                return val.date()
            return pd.to_datetime(val).date()

        if not all_index_daily.empty:
            dates = pd.to_datetime(all_index_daily[TRADE_DATE])
            written_index_daily = all_index_daily[(dates >= start_dt) & (dates <= end_dt)]
        else:
            written_index_daily = pd.DataFrame()

        if not all_states.empty:
            dates = pd.to_datetime(all_states[TRADE_DATE])
            written_states = all_states[(dates >= start_dt) & (dates <= end_dt)]
        else:
            written_states = pd.DataFrame()

        if not m4_obs_df.empty:
            dates = pd.to_datetime(m4_obs_df[TRADE_DATE])
            written_m4_obs = m4_obs_df[(dates >= start_dt) & (dates <= end_dt)]
        else:
            written_m4_obs = pd.DataFrame()

        # 7. Single ACID Transaction Persistence with Scope Control
        now = datetime.now(timezone.utc)
        try:
            self.con.execute("BEGIN TRANSACTION")

            if run_type == "DAILY":
                # Option A: physically replace target_date only
                self.con.execute(
                    f"DELETE FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE trade_date = ?",
                    [end_date],
                )
                self.con.execute(
                    f"DELETE FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE trade_date = ?",
                    [end_date],
                )
                self.con.execute(
                    f"DELETE FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date = ?",
                    [end_date],
                )
                # For episodes: upsert only episodes touching end_date
                if not all_episodes.empty:
                    for r in all_episodes.itertuples(index=False):
                        st = _to_date(getattr(r, "episode_start_date"))
                        ed_val = getattr(r, "episode_end_date")
                        ed = _to_date(ed_val) if (ed_val is not None and pd.notna(ed_val)) else None
                        if st <= end_date and (ed is None or ed >= end_date):
                            self.con.execute(
                                f"DELETE FROM {THEME_CUSTOM_INDEX_EPISODE_TABLE} WHERE episode_id = ?",
                                [getattr(r, "episode_id")],
                            )
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
            else:
                # Replay mode: replace requested date range
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
                self.con.execute(
                    f"DELETE FROM {THEME_CUSTOM_INDEX_EPISODE_TABLE} WHERE episode_confirmed_date BETWEEN ? AND ? OR episode_start_date BETWEEN ? AND ?",
                    [start_date, end_date, start_date, end_date],
                )
                if not all_episodes.empty:
                    for r in all_episodes.itertuples(index=False):
                        c_date = _to_date(getattr(r, "episode_confirmed_date"))
                        s_date = _to_date(getattr(r, "episode_start_date"))
                        if (start_date <= c_date <= end_date) or (start_date <= s_date <= end_date):
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

            # Insert Daily Indices for sliced output
            if not written_index_daily.empty:
                for r in written_index_daily.itertuples(index=False):
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

            # Insert States for sliced output
            if not written_states.empty:
                for r in written_states.itertuples(index=False):
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

            # Insert M4 Observations for sliced output
            if not written_m4_obs.empty:
                for r in written_m4_obs.itertuples(index=False):
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

            # Persist production run record
            self.con.execute(
                f"""
                INSERT INTO {THEME_PRODUCTION_RUN_TABLE} (
                    production_run_id, run_type, status, target_start_date, target_end_date,
                    knowledge_date, calculation_version, rule_version, comparison_universe_version,
                    input_snapshot_id, theme_count, total_index_rows, total_observation_rows,
                    error_code, error_detail, created_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    run_type,
                    "SUCCEEDED",
                    start_date,
                    end_date,
                    kd,
                    THEME_M4_OBSERVATION_VERSION,
                    THEME_CUSTOM_INDEX_STATE_VERSION,
                    COMPARISON_UNIVERSE_VERSION_V1,
                    snap_id,
                    len(themes),
                    len(written_index_daily),
                    len(written_m4_obs),
                    None,
                    None,
                    t0,
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
            trade_date_count=len(output_trade_dates),
            start_date=start_date,
            end_date=end_date,
            total_index_rows=len(written_index_daily),
            total_observation_rows=len(written_m4_obs),
            total_episodes=len(all_episodes),
            execution_seconds=exec_sec,
        )

    def run_m4_daily(
        self,
        trade_date: date,
        knowledge_date: date | None = None,
        production_run_id: str | None = None,
        execution_control: Any | None = None,
    ) -> ThemeProductionReport:
        """Daily production (Option A: target-date physical replacement with historical calculation context)."""
        return self.rebuild_m4_facts(
            start_date=trade_date,
            end_date=trade_date,
            knowledge_date=knowledge_date or trade_date,
            production_run_id=production_run_id,
            run_type="DAILY",
            execution_control=execution_control,
        )
