"""High-throughput Theme Custom Index and M4 Observation production pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import hashlib
import json
import uuid

import duckdb
import numpy as np
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
    IS_ABOVE_OR_EQUAL_MA5,
    STATE_CHANGED,
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
    QUALIFICATION_STATUS_NOT_CONFIGURED,
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
    THEME_EFFECTIVE_MEMBER_DAILY_TABLE,
    THEME_EFFECTIVE_MEMBER_VERSION,
    EXCLUSION_REASON,
)
from qrp_atlas.contracts.fields import (
    FINALIZED_AT,
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
    total_state_rows: int = 0


@dataclass(frozen=True)
class ThemeM4CalculatedFacts:
    """Calculated Theme Custom Indices, states, episodes, and M4 observations.

    Directly consumable by backtest, historical review, and lineage audit without
    requiring physical database materialization.
    """

    theme_count: int
    start_date: date
    end_date: date
    knowledge_date: date
    calc_start_date: date
    input_snapshot_id: str
    daily_indices: pd.DataFrame
    daily_states: pd.DataFrame
    episodes: pd.DataFrame
    m4_observations: pd.DataFrame
    execution_seconds: float
    all_episodes: pd.DataFrame = field(default_factory=pd.DataFrame)
    effective_members: pd.DataFrame = field(default_factory=pd.DataFrame)


def _to_date(val: Any) -> date:
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if hasattr(val, "date"):
        return val.date()
    return pd.to_datetime(val).date()


class ThemePipelineService:
    """Production service for Theme Custom Indices and M4 Observations."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con
        self.resolver = StockCollectionResolver(con)

    def _is_open_trading_day(self, trade_date: date) -> bool:
        """Check if trade_date is an open trading day strictly from trading_calendar in database."""
        row = self.con.execute(
            "SELECT is_open FROM trading_calendar WHERE trade_date = ?",
            [trade_date],
        ).fetchone()
        return bool(row and row[0])

    def _get_finalized_theme_ids(self, trade_date: date) -> set[str]:
        """Get theme_ids that have already completed finalized production on trade_date.

        A theme is considered finalized on trade_date if and only if both its custom index daily
        record AND its M4 observation record have been successfully persisted.
        Themes with 0 candidate/effective members will have effective_member_count = 0 in both tables,
        strictly distinguishing them from unrun themes.
        """
        rows = self.con.execute(
            f"""
            SELECT idx.theme_id
            FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} idx
            JOIN {THEME_M4_OBSERVATION_TABLE} obs
              ON idx.theme_id = obs.theme_id AND idx.trade_date = obs.trade_date
            WHERE idx.trade_date = ?
            """,
            [trade_date],
        ).fetchall()
        return {r[0] for r in rows}

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

    def _fetch_all_canonical_themes(
        self,
        trade_date: date,
        knowledge_date: date | None = None,
    ) -> list[tuple[str, str]]:
        """Fetch expected canonical themes for trade_date under PIT rules.

        Invariant:
        ExpectedThemes(D) contains only themes that are legally visible and effective
        as of trade_date D (ingested before D 09:00:00 Asia/Shanghai, available <= D,
        and effective on D).
        If trade_date D has already completed a SUCCEEDED production run, its expected
        themes are permanently frozen to the finalized themes of that run.
        """
        # 1. If trade_date D is already finalized in production history, freeze to finalized themes
        is_already_finalized = self.con.execute(
            f"""
            SELECT COUNT(*) FROM {THEME_PRODUCTION_RUN_TABLE}
            WHERE target_start_date = ? AND target_end_date = ? AND status = 'SUCCEEDED'
            """,
            [trade_date, trade_date],
        ).fetchone()[0] > 0

        if is_already_finalized:
            finalized_rows = self.con.execute(
                f"""
                SELECT DISTINCT idx.theme_id, idx.collection_id
                FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} idx
                JOIN {THEME_M4_OBSERVATION_TABLE} obs
                  ON idx.theme_id = obs.theme_id AND idx.trade_date = obs.trade_date
                WHERE idx.trade_date = ?
                ORDER BY idx.theme_id ASC
                """,
                [trade_date],
            ).fetchall()
            if finalized_rows:
                return [(r[0], r[1]) for r in finalized_rows]

        # 2. Otherwise, resolve legally visible and effective themes at D 09:00 cutoff (independent of caller knowledge_date)
        rows = self.con.execute(
            f"""
            WITH ranked AS (
                SELECT theme_id, collection_id, status, effective_from, effective_to,
                       row_number() OVER (PARTITION BY theme_id ORDER BY available_trade_date DESC, ingested_at DESC) as rn
                FROM {THEME_TABLE}
                WHERE ingested_at < (CAST(? AS DATE) + INTERVAL 9 HOUR)::TIMESTAMP AT TIME ZONE 'Asia/Shanghai'
                  AND available_trade_date <= ?
            )
            SELECT theme_id, collection_id FROM ranked
            WHERE rn = 1
              AND status = 'ACTIVE'
              AND effective_from <= ?
              AND (effective_to IS NULL OR effective_to > ?)
            ORDER BY theme_id ASC
            """,
            [trade_date, trade_date, trade_date, trade_date],
        ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def _fetch_replay_canonical_themes(
        self,
        as_of_date: date,
        knowledge_date: date,
    ) -> list[tuple[str, str]]:
        """Fetch canonical themes for research replay as of (as_of_date, knowledge_date).

        Unlike production ExpectedThemes(D), replay is not constrained by D 09:00 cutoff
        or finalized production history freeze, allowing researchers to evaluate themes
        with hindsight knowledge.
        """
        rows = self.con.execute(
            f"""
            WITH ranked AS (
                SELECT theme_id, collection_id, status, effective_from, effective_to,
                       row_number() OVER (PARTITION BY theme_id ORDER BY available_trade_date DESC, ingested_at DESC) as rn
                FROM {THEME_TABLE}
                WHERE available_trade_date <= ?
            )
            SELECT theme_id, collection_id FROM ranked
            WHERE rn = 1
              AND status = 'ACTIVE'
              AND effective_from <= ?
              AND (effective_to IS NULL OR effective_to > ?)
            ORDER BY theme_id ASC
            """,
            [knowledge_date, as_of_date, as_of_date],
        ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def _fetch_replay_canonical_themes_for_range(
        self, start_date: date, end_date: date, knowledge_date: date
    ) -> list[tuple[str, str]]:
        """Fetch active canonical themes whose lifecycle intersects [start_date, end_date] as of knowledge_date."""
        rows = self.con.execute(
            f"""
            WITH ranked AS (
                SELECT theme_id, collection_id, status, effective_from, effective_to,
                       row_number() OVER (PARTITION BY theme_id ORDER BY available_trade_date DESC, ingested_at DESC) as rn
                FROM {THEME_TABLE}
                WHERE available_trade_date <= ?
            )
            SELECT theme_id, collection_id FROM ranked
            WHERE rn = 1
              AND status = 'ACTIVE'
              AND effective_from <= ?
              AND (effective_to IS NULL OR effective_to > ?)
            ORDER BY theme_id ASC
            """,
            [knowledge_date, end_date, start_date],
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

    def calculate_m4_facts(
        self,
        start_date: date,
        end_date: date,
        knowledge_date: date | None = None,
        context_start_date: date | None = None,
        execution_control: Any | None = None,
    ) -> ThemeM4CalculatedFacts:
        """Pure calculation core for Theme Custom Indices and M4 Observations.

        Strictly decoupled from database persistence (100% read-only).
        Used by both materializing production (DAILY, BACKFILL, CORRECTION) and
        read-only historical PIT replay.
        """
        t0 = datetime.now(timezone.utc)
        kd = knowledge_date or end_date

        # Determine calculation context start: continue forward from anchor unless context_start_date is explicitly given
        calc_start = min(context_start_date, start_date) if context_start_date is not None else start_date

        themes = self._fetch_replay_canonical_themes_for_range(calc_start, end_date, kd)
        if not themes:
            raise ThemePipelineError("NO_ACTIVE_THEMES", f"No active themes in range [{calc_start}, {end_date}] as of {kd}")

        theme_map = {coll_id: theme_id for theme_id, coll_id in themes}
        coll_ids = list(theme_map.keys())

        calc_trade_dates = self._fetch_trading_calendar_dates(calc_start, end_date)
        if not calc_trade_dates:
            raise ThemePipelineError(
                "NO_TRADING_DATES", f"No open trading dates between {calc_start} and {end_date}"
            )

        # 1. Fetch set-based memberships via batch Resolver (replay/PIT allows retroactive view as of kd)
        raw_memberships = self.resolver.batch_resolve_members(
            coll_ids, calc_trade_dates, kd, enforce_admission_cutoff=False
        )

        # 2. Fetch facts across calculation context range
        listing_df = self._fetch_confirmed_listing_facts(calc_start, end_date)
        susp_df = self._fetch_suspension_facts(calc_start, end_date)
        market_df = self._fetch_market_snapshot(calc_start, end_date)
        comp_boards_df = self._fetch_comparison_boards(calc_start, end_date)

        if execution_control is not None and hasattr(execution_control, "checkpoint"):
            execution_control.checkpoint()

        # Compute deterministic snapshot identity over target range [start_date, end_date] facts and versions
        target_dates_set = set(self._fetch_trading_calendar_dates(start_date, end_date))
        if isinstance(raw_memberships, pd.DataFrame):
            m_dates = pd.to_datetime(raw_memberships[TRADE_DATE]).dt.date
            snap_memberships = raw_memberships[m_dates.isin(target_dates_set)] if not raw_memberships.empty else raw_memberships
        else:
            snap_memberships = [m for m in raw_memberships if getattr(m, TRADE_DATE) in target_dates_set] if raw_memberships else []

        l_dates = pd.to_datetime(listing_df[TRADE_DATE]).dt.date if not listing_df.empty else pd.Series()
        snap_listing = listing_df[l_dates.isin(target_dates_set)] if not listing_df.empty else listing_df

        s_dates = pd.to_datetime(susp_df[TRADE_DATE]).dt.date if not susp_df.empty else pd.Series()
        snap_susp = susp_df[s_dates.isin(target_dates_set)] if not susp_df.empty else susp_df

        mkt_dates = pd.to_datetime(market_df[TRADE_DATE]).dt.date if not market_df.empty else pd.Series()
        snap_market = market_df[mkt_dates.isin(target_dates_set)] if not market_df.empty else market_df

        c_dates = pd.to_datetime(comp_boards_df[TRADE_DATE]).dt.date if not comp_boards_df.empty else pd.Series()
        snap_comp = comp_boards_df[c_dates.isin(target_dates_set)] if not comp_boards_df.empty else comp_boards_df

        versions = {
            "calculation_version": THEME_M4_OBSERVATION_VERSION,
            "effective_member_version": THEME_EFFECTIVE_MEMBER_VERSION,
            "rule_version": THEME_CUSTOM_INDEX_STATE_VERSION,
            "comparison_universe_version": COMPARISON_UNIVERSE_VERSION_V1,
        }
        snap_id = compute_deterministic_snapshot_id(
            themes=themes,
            memberships=snap_memberships,
            listing_df=snap_listing,
            susp_df=snap_susp,
            market_df=snap_market,
            comp_boards_df=snap_comp,
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
            # Replay forms its own historical path starting from DEFAULT_BASE_LEVEL at calc_start.
            # It NEVER queries canonical theme_custom_index_daily for previous level or prior history.
            idx_df = calculate_theme_equal_weight_index(
                coll_eff, market_df, previous_cumulative_index_level=DEFAULT_BASE_LEVEL
            )
            if idx_df.empty:
                continue

            idx_df[THEME_ID] = theme_id
            index_daily_list.append(idx_df)

            trend_res = calculate_theme_index_trend_and_episodes(idx_df, theme_id=theme_id)
            states_df = trend_res.daily_states.copy()
            states_df[THEME_ID] = theme_id
            target_dates_set = set(self._fetch_trading_calendar_dates(start_date, end_date))
            s_dates = pd.to_datetime(states_df[TRADE_DATE]).dt.date
            states_df = states_df[s_dates.isin(target_dates_set)].reset_index(drop=True)
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
            m4_obs_df[TRADE_DATE] = pd.to_datetime(m4_obs_df[TRADE_DATE]).dt.date
            all_states_copy = all_states.copy()
            all_states_copy[TRADE_DATE] = pd.to_datetime(all_states_copy[TRADE_DATE]).dt.date
            m4_obs_df = m4_obs_df.merge(
                all_states_copy[[COLLECTION_ID, TRADE_DATE, TREND_STATE, CUSTOM_INDEX_TREND_RUN_DAYS, CUSTOM_INDEX_EPISODE_ID]],
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

        if not all_index_daily.empty:
            dates = pd.to_datetime(all_index_daily[TRADE_DATE])
            sliced_index_daily = all_index_daily[(dates >= start_dt) & (dates <= end_dt)].copy()
        else:
            sliced_index_daily = pd.DataFrame()

        if not all_states.empty:
            dates = pd.to_datetime(all_states[TRADE_DATE])
            sliced_states = all_states[(dates >= start_dt) & (dates <= end_dt)].copy()
        else:
            sliced_states = pd.DataFrame()

        if not m4_obs_df.empty:
            dates = pd.to_datetime(m4_obs_df[TRADE_DATE])
            sliced_m4_obs = m4_obs_df[(dates >= start_dt) & (dates <= end_dt)].copy()
        else:
            sliced_m4_obs = pd.DataFrame()

        # Episodes touching or active within [start_date, end_date]
        if not all_episodes.empty:
            ep_rows = []
            for r in all_episodes.itertuples(index=False):
                st = _to_date(getattr(r, "episode_start_date"))
                ed_val = getattr(r, "episode_end_date")
                ed = _to_date(ed_val) if (ed_val is not None and pd.notna(ed_val)) else None
                if st <= end_date and (ed is None or ed >= start_date):
                    ep_rows.append(r)
            sliced_episodes = pd.DataFrame(ep_rows) if ep_rows else pd.DataFrame(columns=all_episodes.columns)
        else:
            sliced_episodes = pd.DataFrame()

        # Stamp deterministic snapshot id on sliced outputs
        if not sliced_index_daily.empty:
            sliced_index_daily[INPUT_SNAPSHOT_ID] = snap_id
        if not sliced_states.empty:
            sliced_states[INPUT_SNAPSHOT_ID] = snap_id
        if not sliced_m4_obs.empty:
            sliced_m4_obs[INPUT_SNAPSHOT_ID] = snap_id
        if not sliced_episodes.empty:
            sliced_episodes[INPUT_SNAPSHOT_ID] = snap_id

        exec_sec = (datetime.now(timezone.utc) - t0).total_seconds()
        return ThemeM4CalculatedFacts(
            theme_count=len(themes),
            start_date=start_date,
            end_date=end_date,
            knowledge_date=kd,
            calc_start_date=calc_start,
            input_snapshot_id=snap_id,
            daily_indices=sliced_index_daily,
            daily_states=sliced_states,
            episodes=sliced_episodes,
            m4_observations=sliced_m4_obs,
            execution_seconds=exec_sec,
            all_episodes=all_episodes,
            effective_members=eff_members_df,
        )

    def replay_m4_facts(
        self,
        start_date: date,
        end_date: date,
        knowledge_date: date,
        context_start_date: date | None = None,
        execution_control: Any | None = None,
    ) -> ThemeM4CalculatedFacts:
        """Historical Point-in-Time replay.

        Strictly READ-ONLY and NON-MATERIALIZING:
        - Re-executes the exact same calculation core as canonical production
        - Does NOT open database write transactions
        - Does NOT delete or overwrite canonical tables
        - Does NOT insert production run records
        - Returns full calculated facts directly consumable by backtest/audit
        """
        return self.calculate_m4_facts(
            start_date=start_date,
            end_date=end_date,
            knowledge_date=knowledge_date,
            context_start_date=context_start_date,
            execution_control=execution_control,
        )

    def _produce_single_day(
        self,
        trade_date: date,
        knowledge_date: date | None = None,
        production_run_id: str | None = None,
        run_type: str = "DAILY",
        execution_control: Any | None = None,
        write_run_record: bool = True,
    ) -> ThemeProductionReport:
        """Produce canonical facts for a single legal trading date forward.

        Enforces:
        1. Legal trading day check via trading_calendar in database (is_open = true).
        2. Finalized history is immutable ledger: if already finalized, do not mutate.
        3. Production sequence:
           Theme Membership -> 09:00 Admission -> M4 Eligibility
           -> theme_effective_member_daily (materialized with finalized_at)
           -> Custom Index (only reading is_m4_effective_member = true from persisted table,
              compounding forward from last finite finalized anchor)
           -> State & Episode (continuing forward without restating closed episodes)
           -> M4 Observation
           -> theme_production_run
        """
        t0 = datetime.now(timezone.utc)
        kd = knowledge_date or trade_date

        # 1. Enforce legal trading day from trading_calendar
        if not self._is_open_trading_day(trade_date):
            raise ThemePipelineError(
                "NOT_LEGAL_TRADING_DAY",
                f"Date {trade_date} is not an open trading day in trading_calendar",
            )

        themes = self._fetch_all_canonical_themes(trade_date)
        if not themes:
            raise ThemePipelineError("NO_ACTIVE_THEMES", f"No active themes as of {trade_date}")

        all_theme_ids = {t[0] for t in themes}
        finalized_theme_ids = self._get_finalized_theme_ids(trade_date)

        # 2. Check if Day D is already finalized in production history
        is_already_finalized = self.con.execute(
            f"""
            SELECT COUNT(*) FROM {THEME_PRODUCTION_RUN_TABLE}
            WHERE target_start_date = ? AND target_end_date = ? AND status = 'SUCCEEDED'
            """,
            [trade_date, trade_date],
        ).fetchone()[0] > 0

        if is_already_finalized and all_theme_ids and all_theme_ids.issubset(finalized_theme_ids):
            # Ledger is immutable. Return existing report without mutation.
            obs_info = self.con.execute(
                f"""
                SELECT production_run_id, input_snapshot_id
                FROM {THEME_M4_OBSERVATION_TABLE}
                WHERE trade_date = ?
                LIMIT 1
                """,
                [trade_date],
            ).fetchone()
            run_id_final = obs_info[0] if obs_info else (production_run_id or f"RUN:THEME_M4:{uuid.uuid4().hex[:12].upper()}")
            snap_id_final = obs_info[1] if obs_info and obs_info[1] else ""
            idx_cnt_val = self.con.execute(
                f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE trade_date = ?",
                [trade_date],
            ).fetchone()[0]
            obs_cnt_val = self.con.execute(
                f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date = ?",
                [trade_date],
            ).fetchone()[0]
            st_cnt_val = self.con.execute(
                f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE trade_date = ?",
                [trade_date],
            ).fetchone()[0]
            return ThemeProductionReport(
                production_run_id=run_id_final,
                input_snapshot_id=snap_id_final,
                theme_count=len(themes),
                trade_date_count=1,
                start_date=trade_date,
                end_date=trade_date,
                total_index_rows=idx_cnt_val,
                total_observation_rows=obs_cnt_val,
                total_episodes=0,
                execution_seconds=0.0,
                total_state_rows=st_cnt_val,
            )

        # 3. BEGIN TRADING-DAY D ATOMIC TRANSACTION
        self.con.execute("BEGIN TRANSACTION")
        try:
            # Clean slate for Day D to guarantee atomicity and idempotence
            self.con.execute(
                f"DELETE FROM {THEME_EFFECTIVE_MEMBER_DAILY_TABLE} WHERE trade_date = ?",
                [trade_date],
            )
            self.con.execute(
                f"DELETE FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE trade_date = ?",
                [trade_date],
            )
            self.con.execute(
                f"DELETE FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE trade_date = ?",
                [trade_date],
            )
            self.con.execute(
                f"DELETE FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date = ?",
                [trade_date],
            )
            self.con.execute(
                f"DELETE FROM {THEME_PRODUCTION_RUN_TABLE} WHERE target_start_date = ? AND target_end_date = ?",
                [trade_date, trade_date],
            )

            now = datetime.now(timezone.utc)
            run_id = production_run_id or f"RUN:THEME_M4_{run_type}:{trade_date.strftime('%Y%m%d')}:{uuid.uuid4().hex[:8].upper()}"
            theme_map = {coll_id: theme_id for theme_id, coll_id in themes}
            coll_ids = list(theme_map.keys())

            if execution_control is not None and hasattr(execution_control, "checkpoint"):
                execution_control.checkpoint()

            # Step 1: Materialize all theme_effective_member_daily for day D
            raw_memberships = self.resolver.batch_resolve_members(
                coll_ids, [trade_date], None, enforce_admission_cutoff=True
            )
            listing_df = self._fetch_confirmed_listing_facts(trade_date, trade_date)
            susp_df = self._fetch_suspension_facts(trade_date, trade_date)
            eff_members_df = calculate_m4_effective_members(raw_memberships, listing_df, susp_df)
            if THEME_ID not in eff_members_df.columns:
                eff_members_df[THEME_ID] = eff_members_df[COLLECTION_ID].map(theme_map)

            market_df = self._fetch_market_snapshot(trade_date, trade_date)
            comp_boards_df = self._fetch_comparison_boards(trade_date, trade_date)

            versions = {
                "calculation_version": THEME_M4_OBSERVATION_VERSION,
                "effective_member_version": THEME_EFFECTIVE_MEMBER_VERSION,
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

            for r in eff_members_df.itertuples(index=False):
                self.con.execute(
                    f"""
                    INSERT INTO {THEME_EFFECTIVE_MEMBER_DAILY_TABLE} (
                        collection_id, theme_id, asset_id, trade_date,
                        is_theme_member, confirmed_listing_trading_day_count,
                        is_suspended, is_m4_effective_member, exclusion_reason,
                        calculation_version, input_snapshot_id, production_run_id,
                        created_at, finalized_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        getattr(r, COLLECTION_ID),
                        getattr(r, THEME_ID),
                        getattr(r, ASSET_ID),
                        trade_date,
                        bool(getattr(r, "is_theme_member")),
                        int(getattr(r, CONFIRMED_LISTING_TRADING_DAY_COUNT)) if pd.notna(getattr(r, CONFIRMED_LISTING_TRADING_DAY_COUNT)) else None,
                        bool(getattr(r, IS_SUSPENDED)),
                        bool(getattr(r, IS_M4_EFFECTIVE_MEMBER)),
                        getattr(r, EXCLUSION_REASON) if pd.notna(getattr(r, EXCLUSION_REASON)) else None,
                        THEME_EFFECTIVE_MEMBER_VERSION,
                        snap_id,
                        run_id,
                        now,
                        now,
                    ],
                )

            # Step 2: Query formal persisted effective-member facts from database
            # Custom Index strictly consumes formal persisted facts!
            persisted_eff_df = self.con.execute(
                f"""
                SELECT collection_id, theme_id, asset_id, trade_date,
                       is_theme_member, is_m4_effective_member, exclusion_reason
                FROM {THEME_EFFECTIVE_MEMBER_DAILY_TABLE}
                WHERE trade_date = ?
                """,
                [trade_date],
            ).df()

            # Step 3: Compute Custom Index for ALL themes from formal persisted facts
            indices_list = []
            for theme_id, coll_id in themes:
                anchor_row = self.con.execute(
                    f"""
                    SELECT index_level FROM {THEME_CUSTOM_INDEX_DAILY_TABLE}
                    WHERE theme_id = ? AND trade_date < ? AND index_level IS NOT NULL
                    ORDER BY trade_date DESC LIMIT 1
                    """,
                    [theme_id, trade_date],
                ).fetchone()
                prev_level = anchor_row[0] if (anchor_row and anchor_row[0] is not None) else DEFAULT_BASE_LEVEL

                theme_persisted_eff = persisted_eff_df[
                    (persisted_eff_df[COLLECTION_ID] == coll_id) & (persisted_eff_df[IS_M4_EFFECTIVE_MEMBER] == True)
                ]
                total_theme_members = len(persisted_eff_df[persisted_eff_df[COLLECTION_ID] == coll_id])

                if theme_persisted_eff.empty:
                    idx_df = pd.DataFrame([
                        {
                            COLLECTION_ID: coll_id,
                            THEME_ID: theme_id,
                            TRADE_DATE: trade_date,
                            THEME_DAILY_RETURN: np.nan,
                            INDEX_LEVEL: np.nan,
                            BASE_LEVEL: DEFAULT_BASE_LEVEL,
                            EFFECTIVE_MEMBER_COUNT: 0,
                            TOTAL_MEMBER_COUNT: total_theme_members,
                        }
                    ])
                else:
                    idx_df = calculate_theme_equal_weight_index(
                        theme_persisted_eff, market_df, previous_cumulative_index_level=prev_level
                    )
                    idx_df[THEME_ID] = theme_id
                    idx_df[TOTAL_MEMBER_COUNT] = total_theme_members

                indices_list.append(idx_df)

                row_idx = idx_df.iloc[0]
                today_close = (
                    float(row_idx[INDEX_LEVEL])
                    if (pd.notna(row_idx[INDEX_LEVEL]) and np.isfinite(row_idx[INDEX_LEVEL]))
                    else None
                )
                self.con.execute(
                    f"""
                    INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE} (
                        theme_id, collection_id, trade_date, theme_daily_return,
                        index_level, base_level, effective_member_count, total_member_count,
                        calculation_version, production_run_id, input_snapshot_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        theme_id,
                        coll_id,
                        trade_date,
                        row_idx[THEME_DAILY_RETURN] if pd.notna(row_idx[THEME_DAILY_RETURN]) else None,
                        today_close,
                        row_idx[BASE_LEVEL],
                        int(row_idx[EFFECTIVE_MEMBER_COUNT]),
                        int(row_idx[TOTAL_MEMBER_COUNT]),
                        THEME_CUSTOM_INDEX_VERSION,
                        run_id,
                        snap_id,
                        now,
                    ],
                )

            day_indices_df = pd.concat(indices_list, ignore_index=True) if indices_list else pd.DataFrame()

            # Step 4: Advance State & Open Episodes for ALL themes
            written_episodes = []
            theme_states = {}
            for theme_id, coll_id in themes:
                matching_idx = day_indices_df[day_indices_df[THEME_ID] == theme_id]
                row_idx = matching_idx.iloc[0]
                today_close = (
                    float(row_idx[INDEX_LEVEL])
                    if (pd.notna(row_idx[INDEX_LEVEL]) and np.isfinite(row_idx[INDEX_LEVEL]))
                    else None
                )

                prev_state_row = self.con.execute(
                    f"""
                    SELECT
                        theme_id, collection_id, trade_date, close, ma5, ma10,
                        trend_state, previous_trend_state, custom_index_trend_run_days,
                        is_above_or_equal_ma5, state_changed
                    FROM {THEME_CUSTOM_INDEX_STATE_TABLE}
                    WHERE theme_id = ? AND trade_date < ?
                    ORDER BY trade_date DESC LIMIT 1
                    """,
                    [theme_id, trade_date],
                ).fetchone()

                open_ep_row = self.con.execute(
                    f"""
                    SELECT
                        episode_id, theme_id, collection_id, episode_no,
                        episode_start_date, episode_confirmed_date, episode_end_date,
                        ma5_reentry_count, episode_return
                    FROM {THEME_CUSTOM_INDEX_EPISODE_TABLE}
                    WHERE theme_id = ? AND episode_end_date IS NULL
                    LIMIT 1
                    """,
                    [theme_id],
                ).fetchone()

                # Contiguous price suffix: preserve NULL rows and break on first NULL/non-finite going backward
                prior_close_rows = self.con.execute(
                    f"""
                    SELECT index_level FROM {THEME_CUSTOM_INDEX_DAILY_TABLE}
                    WHERE theme_id = ? AND trade_date < ?
                    ORDER BY trade_date DESC LIMIT 9
                    """,
                    [theme_id, trade_date],
                ).fetchall()
                contiguous_prior_closes = []
                for r_lvl in prior_close_rows:
                    val = r_lvl[0]
                    if val is None or (isinstance(val, float) and (np.isnan(val) or not np.isfinite(val))):
                        break
                    contiguous_prior_closes.append(float(val))
                prior_closes = list(reversed(contiguous_prior_closes))

                if today_close is not None and np.isfinite(today_close):
                    window = prior_closes + [today_close]
                    ma5_val = float(np.mean(window[-5:])) if len(window) >= 5 else None
                    ma10_val = float(np.mean(window[-10:])) if len(window) >= 10 else None
                else:
                    ma5_val = None
                    ma10_val = None

                if today_close is None or ma5_val is None:
                    curr_state = None
                    is_above = None
                    state_changed = False
                    run_days = 0
                else:
                    is_above = bool(today_close >= ma5_val)
                    prev_ma5_complete = bool(
                        prev_state_row and prev_state_row[4] is not None and not np.isnan(prev_state_row[4])
                    )
                    prev_is_above = (
                        bool(prev_state_row[9]) if (prev_state_row and prev_state_row[9] is not None) else False
                    )

                    if not is_above:
                        curr_state = "BASE"
                    elif prev_ma5_complete:
                        curr_state = "ACTIVE" if prev_is_above else "CANDIDATE"
                    else:
                        curr_state = None

                    prev_state_name = prev_state_row[6] if prev_state_row else None
                    prev_run_days = int(prev_state_row[8]) if prev_state_row else 0

                    if curr_state is None:
                        state_changed = False
                        run_days = 0
                    elif prev_state_name is None or curr_state != prev_state_name:
                        state_changed = True
                        run_days = 1
                    else:
                        state_changed = False
                        run_days = prev_run_days + 1

                candidate_to_active = bool(
                    prev_state_row and prev_state_row[6] == "CANDIDATE" and curr_state == "ACTIVE"
                )
                assigned_ep_id = None

                if open_ep_row is None:
                    if candidate_to_active:
                        max_ep_no_row = self.con.execute(
                            f"SELECT COALESCE(MAX(episode_no), 0) FROM {THEME_CUSTOM_INDEX_EPISODE_TABLE} WHERE theme_id = ?",
                            [theme_id],
                        ).fetchone()
                        ep_no = (max_ep_no_row[0] if max_ep_no_row else 0) + 1
                        assigned_ep_id = f"{theme_id}_EP_{ep_no:04d}"
                        start_date_ep = prev_state_row[2]

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
                                assigned_ep_id, theme_id, coll_id, ep_no,
                                start_date_ep, trade_date, None,
                                0, 0.0, THEME_CUSTOM_INDEX_EPISODE_VERSION,
                                run_id, snap_id, now,
                            ],
                        )
                        # NOTE: ITEM 2 COMPLIANCE - DO NOT back-assign episode_id to start_date_ep observation!
                        # D-1 observation remains 100% immutable and bit-stable!
                        written_episodes.append(assigned_ep_id)
                else:
                    assigned_ep_id = open_ep_row[0]
                    reentries = int(open_ep_row[7])
                    if candidate_to_active:
                        reentries += 1
                        self.con.execute(
                            f"UPDATE {THEME_CUSTOM_INDEX_EPISODE_TABLE} SET ma5_reentry_count = ? WHERE episode_id = ?",
                            [reentries, assigned_ep_id],
                        )

                    prev_below_ma10 = bool(
                        prev_state_row and prev_state_row[5] is not None
                        and not np.isnan(prev_state_row[5])
                        and prev_state_row[3] is not None
                        and prev_state_row[3] < prev_state_row[5]
                    )
                    is_end = bool(
                        curr_state != "ACTIVE"
                        and prev_below_ma10
                        and ma10_val is not None
                        and today_close is not None
                        and today_close < ma10_val
                    )
                    if is_end:
                        start_dt = open_ep_row[4]
                        sp_row = self.con.execute(
                            f"SELECT index_level FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE theme_id = ? AND trade_date = ?",
                            [theme_id, start_dt],
                        ).fetchone()
                        sp = float(sp_row[0]) if (sp_row and sp_row[0] is not None) else float(today_close)
                        ep_ret = float((today_close / sp) - 1.0) if sp > 0 else 0.0
                        self.con.execute(
                            f"""
                            UPDATE {THEME_CUSTOM_INDEX_EPISODE_TABLE}
                            SET episode_end_date = ?, episode_return = ?
                            WHERE episode_id = ?
                            """,
                            [trade_date, ep_ret, assigned_ep_id],
                        )

                # Persist State for trade_date
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
                        theme_id, coll_id, trade_date,
                        today_close, ma5_val, ma10_val,
                        curr_state, prev_state_row[6] if prev_state_row else None, run_days,
                        is_above, state_changed,
                        THEME_CUSTOM_INDEX_STATE_VERSION, run_id, snap_id, now,
                    ],
                )
                theme_states[theme_id] = (curr_state, run_days, assigned_ep_id)

            # Step 5: Uniform M4 observations / ranks across the complete D-day Theme universe
            persisted_indices_df = self.con.execute(
                f"""
                SELECT theme_id, collection_id, trade_date, theme_daily_return, index_level,
                       base_level, effective_member_count, total_member_count
                FROM {THEME_CUSTOM_INDEX_DAILY_TABLE}
                WHERE trade_date = ?
                """,
                [trade_date],
            ).df()

            m4_obs_df = calculate_m4_raw_observations(
                persisted_indices_df,
                persisted_eff_df,
                market_df,
                comp_boards_df,
                comparison_universe_version=COMPARISON_UNIVERSE_VERSION_V1,
            )

            for r_obs in m4_obs_df.itertuples(index=False):
                th_id = getattr(r_obs, THEME_ID)
                cl_id = getattr(r_obs, COLLECTION_ID)
                st_info = theme_states.get(th_id, (None, 0, None))
                curr_state_val, run_days_val, assigned_ep_val = st_info

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
                        th_id,
                        cl_id,
                        trade_date,
                        getattr(r_obs, THEME_DAILY_RETURN, None) if pd.notna(getattr(r_obs, THEME_DAILY_RETURN, None)) else None,
                        int(getattr(r_obs, THEME_LIMIT_UP_COUNT, None)) if pd.notna(getattr(r_obs, THEME_LIMIT_UP_COUNT, None)) else None,
                        int(getattr(r_obs, THEME_RETURN_RANK, None)) if pd.notna(getattr(r_obs, THEME_RETURN_RANK, None)) else None,
                        int(getattr(r_obs, EFFECTIVE_MEMBER_COUNT, 0)),
                        int(getattr(r_obs, TOTAL_MEMBER_COUNT, 0)),
                        int(getattr(r_obs, COMPARISON_UNIVERSE_SIZE, 0)),
                        getattr(r_obs, COMPARISON_UNIVERSE_VERSION, COMPARISON_UNIVERSE_VERSION_V1),
                        curr_state_val,
                        int(run_days_val) if run_days_val is not None else None,
                        assigned_ep_val,
                        getattr(r_obs, QUALIFICATION_STATUS, QUALIFICATION_STATUS_NOT_CONFIGURED),
                        THEME_M4_OBSERVATION_VERSION,
                        run_id,
                        snap_id,
                        now,
                    ],
                )

            # Step 6: Write theme_production_run for day D (status = 'SUCCEEDED')
            total_idx_rows = self.con.execute(
                f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE trade_date = ?",
                [trade_date],
            ).fetchone()[0]
            total_obs_rows = self.con.execute(
                f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date = ?",
                [trade_date],
            ).fetchone()[0]
            total_st_rows = self.con.execute(
                f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE trade_date = ?",
                [trade_date],
            ).fetchone()[0]

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
                    trade_date,
                    trade_date,
                    kd,
                    THEME_M4_OBSERVATION_VERSION,
                    THEME_CUSTOM_INDEX_STATE_VERSION,
                    COMPARISON_UNIVERSE_VERSION_V1,
                    snap_id,
                    len(themes),
                    total_idx_rows,
                    total_obs_rows,
                    None,
                    None,
                    t0,
                    now,
                ],
            )

            # Step 7: COMMIT D
            self.con.execute("COMMIT")
        except Exception as exc:
            try:
                self.con.execute("ROLLBACK")
            except Exception:
                pass
            raise ThemePipelineError(
                "THEME_PRODUCTION_TRANSACTION_FAILED",
                f"Trading-Day D transaction failed for {trade_date}: {exc}",
            ) from exc

        exec_sec = (datetime.now(timezone.utc) - t0).total_seconds()
        return ThemeProductionReport(
            production_run_id=run_id,
            input_snapshot_id=snap_id,
            theme_count=len(themes),
            trade_date_count=1,
            start_date=trade_date,
            end_date=trade_date,
            total_index_rows=total_idx_rows,
            total_observation_rows=total_obs_rows,
            total_episodes=len(written_episodes),
            execution_seconds=exec_sec,
            total_state_rows=total_st_rows,
        )

    def rebuild_m4_facts(
        self,
        start_date: date,
        end_date: date,
        context_start_date: date | None = None,
        knowledge_date: date | None = None,
        production_run_id: str | None = None,
        run_type: str = "REBUILD",
        execution_control: Any | None = None,
    ) -> ThemeProductionReport:
        """Sequential production of Theme Custom Indices and M4 Observations across a date range.

        Advances one open trading day at a time, strictly following the ledger invariant.
        Past finalized trading days are NEVER overwritten or restated.
        """
        t0 = datetime.now(timezone.utc)
        open_dates = self._fetch_trading_calendar_dates(start_date, end_date)
        if not open_dates:
            raise ThemePipelineError(
                "NO_TRADING_DATES",
                f"No open trading dates between {start_date} and {end_date}",
            )

        reports = []
        for d in open_dates:
            if execution_control is not None and hasattr(execution_control, "checkpoint"):
                execution_control.checkpoint()
            rep = self._produce_single_day(
                trade_date=d,
                knowledge_date=knowledge_date,
                production_run_id=production_run_id if len(open_dates) == 1 else None,
                run_type=run_type,
                execution_control=execution_control,
            )
            reports.append(rep)

        last_rep = reports[-1]
        exec_sec = (datetime.now(timezone.utc) - t0).total_seconds()
        return ThemeProductionReport(
            production_run_id=last_rep.production_run_id,
            input_snapshot_id=last_rep.input_snapshot_id,
            theme_count=last_rep.theme_count,
            trade_date_count=len(open_dates),
            start_date=start_date,
            end_date=end_date,
            total_index_rows=sum(r.total_index_rows for r in reports),
            total_observation_rows=sum(r.total_observation_rows for r in reports),
            total_episodes=sum(r.total_episodes for r in reports),
            execution_seconds=exec_sec,
            total_state_rows=sum(r.total_state_rows for r in reports),
        )

    def run_m4_daily(
        self,
        trade_date: date,
        knowledge_date: date | None = None,
        production_run_id: str | None = None,
        execution_control: Any | None = None,
    ) -> ThemeProductionReport:
        """Daily production for a single legal trading date."""
        return self._produce_single_day(
            trade_date=trade_date,
            knowledge_date=knowledge_date,
            production_run_id=production_run_id,
            run_type="DAILY",
            execution_control=execution_control,
        )

    def calculate_m5_facts(
        self,
        trade_date: date,
        *,
        execution_control: Any | None = None,
    ):
        """Compatibility entry point for the independent M5 calculation service."""
        from .m5_service import ThemeM5PipelineService

        return ThemeM5PipelineService(self.con).calculate_m5_facts(
            trade_date,
            execution_control=execution_control,
        )

    def run_m5_daily(
        self,
        trade_date: date,
        *,
        production_run_id: str | None = None,
        execution_control: Any | None = None,
    ):
        """Compatibility entry point for the independent M5 daily pipeline."""
        from .m5_service import ThemeM5PipelineService

        return ThemeM5PipelineService(self.con).run_m5_daily(
            trade_date,
            production_run_id=production_run_id,
            execution_control=execution_control,
        )
