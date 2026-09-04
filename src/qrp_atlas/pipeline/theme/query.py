"""Internal Query and Lineage Audit Service for Theme Custom Index and M4 Observations."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    DAILY_MARKET_SNAPSHOT,
    IS_LIMIT_UP,
    IS_SUSPENDED,
    STOCK_INFO,
    SUSPEND_D,
    THS_DAILY,
    TRADE_DATE,
    TRADING_CALENDAR,
)
from qrp_atlas.contracts.m4 import (
    COMPARISON_UNIVERSE_SIZE,
    COMPARISON_UNIVERSE_VERSION,
    CUSTOM_INDEX_EPISODE_ID,
    CUSTOM_INDEX_TREND_RUN_DAYS,
    CUSTOM_INDEX_TREND_STATE,
    EFFECTIVE_MEMBER_COUNT,
    EXCLUSION_REASON,
    IS_M4_EFFECTIVE_MEMBER,
    QUALIFICATION_STATUS,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_DAILY_RETURN,
    THEME_LIMIT_UP_COUNT,
    THEME_M4_OBSERVATION_TABLE,
    THEME_RETURN_RANK,
    TOTAL_MEMBER_COUNT,
    THEME_EFFECTIVE_MEMBER_DAILY_TABLE,
    THEME_EFFECTIVE_MEMBER_VERSION,
)
from qrp_atlas.contracts.m5 import (
    THEME_HOT_LIST_APPEARANCE_COUNT,
    THEME_HOT_SOURCE_COUNT,
    THEME_HOT_STOCK_COUNT,
    THEME_HOT_STOCK_RATIO,
    THEME_MEMBER_COUNT,
    THEME_M5_OBSERVATION_TABLE,
)
from qrp_atlas.contracts.fields import THEME_PRODUCTION_RUN_TABLE
from qrp_atlas.contracts.stock_collection import (
    COLLECTION_ID,
    CollectionScope,
    CollectionType,
    THEME_ID,
    THEME_MEMBERSHIP_HISTORY_TABLE,
    THEME_TABLE,
)
from qrp_atlas.pipeline.market_facts import query_confirmed_listing_facts
from qrp_atlas.pipeline.theme.service import compute_deterministic_snapshot_id
from qrp_atlas.pipeline.theme.m5_service import ThemeM5PipelineError, ThemeM5PipelineService
from qrp_atlas.stock_collections.models import StockCollectionQueryContext
from qrp_atlas.stock_collections.resolver import StockCollectionResolver


@dataclass(frozen=True)
class M4ObservationAuditReport:
    theme_id: str
    collection_id: str
    trade_date: date
    theme_daily_return: float | None
    theme_limit_up_count: int | None
    theme_return_rank: int | None
    effective_members: int
    total_members: int
    comparison_universe_size: int
    comparison_universe_version: str
    custom_index_trend_state: str | None
    custom_index_trend_run_days: int | None
    custom_index_episode_id: str | None
    qualification_status: str
    production_run_id: str | None
    input_snapshot_id: str | None
    effective_member_assets: tuple[str, ...]
    excluded_members: tuple[dict[str, Any], ...]
    limit_up_assets: tuple[str, ...]
    comparison_boards: tuple[dict[str, Any], ...]
    is_reproducible: bool = True
    discrepancy_reason: str | None = None
    production_knowledge_date: date | None = None
    audit_knowledge_date: date | None = None


@dataclass(frozen=True)
class M5ObservationAuditReport:
    theme_id: str
    collection_id: str
    trade_date: date
    theme_member_count: int
    theme_hot_stock_count: int
    theme_hot_stock_ratio: float | None
    theme_hot_list_appearance_count: int
    theme_hot_source_count: int
    production_run_id: str | None
    input_snapshot_id: str | None
    reconstructed_input_snapshot_id: str | None
    is_reproducible: bool
    discrepancy_reason: str | None = None


class ThemeQueryService:
    """Query service for Theme Custom Indices, States, Episodes, and M4 Lineage Audit."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con
        self.resolver = StockCollectionResolver(con)

    def get_theme_index_history(
        self,
        theme_id: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Fetch historical daily custom index levels and returns for a theme."""
        sql = f"""
        SELECT
            theme_id, collection_id, trade_date, theme_daily_return,
            index_level, base_level, effective_member_count, total_member_count,
            calculation_version, production_run_id, input_snapshot_id, created_at
        FROM {THEME_CUSTOM_INDEX_DAILY_TABLE}
        WHERE theme_id = ? AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date ASC
        """
        return self.con.execute(sql, [theme_id, start_date, end_date]).df()

    def get_theme_episodes(self, theme_id: str) -> pd.DataFrame:
        """Fetch all confirmed episodes for a theme."""
        sql = f"""
        SELECT
            episode_id, theme_id, collection_id, episode_no,
            episode_start_date, episode_confirmed_date, episode_end_date,
            ma5_reentry_count, episode_return, rule_version,
            production_run_id, input_snapshot_id, created_at
        FROM {THEME_CUSTOM_INDEX_EPISODE_TABLE}
        WHERE theme_id = ?
        ORDER BY episode_no ASC
        """
        return self.con.execute(sql, [theme_id]).df()

    def get_m4_observations(
        self,
        trade_date: date,
        theme_id: str | None = None,
    ) -> pd.DataFrame:
        """Fetch M4 observations for all themes or a specific theme on a trade_date."""
        params: list[Any] = [trade_date]
        thm_filter = ""
        if theme_id is not None:
            thm_filter = "AND theme_id = ?"
            params.append(theme_id)

        sql = f"""
        SELECT
            theme_id, collection_id, trade_date, theme_daily_return,
            theme_limit_up_count, theme_return_rank, effective_member_count,
            total_member_count, comparison_universe_size, comparison_universe_version,
            custom_index_trend_state, custom_index_trend_run_days, custom_index_episode_id,
            qualification_status, calculation_version, production_run_id, input_snapshot_id, created_at
        FROM {THEME_M4_OBSERVATION_TABLE}
        WHERE trade_date = ? {thm_filter}
        ORDER BY theme_return_rank ASC NULLS LAST, theme_id ASC
        """
        return self.con.execute(sql, params).df()

    def get_m5_observations(
        self,
        trade_date: date,
        theme_id: str | None = None,
    ) -> pd.DataFrame:
        """Fetch persisted M5 facts for all Themes or one Theme on a date."""
        params: list[Any] = [trade_date]
        theme_filter = ""
        if theme_id is not None:
            theme_filter = "AND theme_id = ?"
            params.append(theme_id)
        sql = f"""
            SELECT
                theme_id, collection_id, trade_date,
                {THEME_MEMBER_COUNT}, {THEME_HOT_STOCK_COUNT}, {THEME_HOT_STOCK_RATIO},
                {THEME_HOT_LIST_APPEARANCE_COUNT}, {THEME_HOT_SOURCE_COUNT},
                calculation_version, production_run_id, input_snapshot_id, created_at
            FROM {THEME_M5_OBSERVATION_TABLE}
            WHERE trade_date = ? {theme_filter}
            ORDER BY theme_id ASC
        """
        return self.con.execute(sql, params).df()

    def audit_m5_observation(
        self,
        theme_id: str,
        trade_date: date,
    ) -> M5ObservationAuditReport:
        """Recompute the complete M5 input set and compare it with one row.

        Auditing is read-only.  If a current source is unavailable, the stored
        row is returned as non-reproducible with the stable source error rather
        than being overwritten or treated as a zero-valued source.
        """
        row = self.con.execute(
            f"""
            SELECT theme_id, collection_id, trade_date,
                   {THEME_MEMBER_COUNT}, {THEME_HOT_STOCK_COUNT}, {THEME_HOT_STOCK_RATIO},
                   {THEME_HOT_LIST_APPEARANCE_COUNT}, {THEME_HOT_SOURCE_COUNT},
                   production_run_id, input_snapshot_id
            FROM {THEME_M5_OBSERVATION_TABLE}
            WHERE theme_id = ? AND trade_date = ?
            """,
            [theme_id, trade_date],
        ).fetchone()
        if not row:
            raise ValueError(f"No M5 observation found for theme {theme_id} on {trade_date}")

        try:
            facts = ThemeM5PipelineService(self.con).calculate_m5_facts(trade_date)
            current = facts.observations[facts.observations[THEME_ID] == theme_id]
            if current.empty:
                return M5ObservationAuditReport(
                    theme_id=row[0],
                    collection_id=row[1],
                    trade_date=row[2],
                    theme_member_count=int(row[3]),
                    theme_hot_stock_count=int(row[4]),
                    theme_hot_stock_ratio=float(row[5]) if row[5] is not None else None,
                    theme_hot_list_appearance_count=int(row[6]),
                    theme_hot_source_count=int(row[7]),
                    production_run_id=row[8],
                    input_snapshot_id=row[9],
                    reconstructed_input_snapshot_id=facts.input_snapshot_id,
                    is_reproducible=False,
                    discrepancy_reason="THEME_NOT_IN_CURRENT_PIT_UNIVERSE",
                )
            calculated = current.iloc[0]
            fields = (
                THEME_MEMBER_COUNT,
                THEME_HOT_STOCK_COUNT,
                THEME_HOT_STOCK_RATIO,
                THEME_HOT_LIST_APPEARANCE_COUNT,
                THEME_HOT_SOURCE_COUNT,
            )
            stored_values = (row[3], row[4], row[5], row[6], row[7])
            calculated_values = tuple(calculated[field] for field in fields)
            values_match = all(
                (stored is None or pd.isna(stored)) and (current_value is None or pd.isna(current_value))
                or stored == current_value
                for stored, current_value in zip(stored_values, calculated_values)
            )
            snapshot_match = row[9] == facts.input_snapshot_id
            reason = None
            if not values_match:
                reason = "CURRENT_INPUTS_RECALCULATE_DIFFER"
            elif not snapshot_match:
                reason = "CURRENT_SOURCE_DIFFERS_FROM_PRODUCTION_SNAPSHOT"
            return M5ObservationAuditReport(
                theme_id=row[0],
                collection_id=row[1],
                trade_date=row[2],
                theme_member_count=int(row[3]),
                theme_hot_stock_count=int(row[4]),
                theme_hot_stock_ratio=float(row[5]) if row[5] is not None else None,
                theme_hot_list_appearance_count=int(row[6]),
                theme_hot_source_count=int(row[7]),
                production_run_id=row[8],
                input_snapshot_id=row[9],
                reconstructed_input_snapshot_id=facts.input_snapshot_id,
                is_reproducible=values_match and snapshot_match,
                discrepancy_reason=reason,
            )
        except ThemeM5PipelineError as exc:
            return M5ObservationAuditReport(
                theme_id=row[0],
                collection_id=row[1],
                trade_date=row[2],
                theme_member_count=int(row[3]),
                theme_hot_stock_count=int(row[4]),
                theme_hot_stock_ratio=float(row[5]) if row[5] is not None else None,
                theme_hot_list_appearance_count=int(row[6]),
                theme_hot_source_count=int(row[7]),
                production_run_id=row[8],
                input_snapshot_id=row[9],
                reconstructed_input_snapshot_id=None,
                is_reproducible=False,
                discrepancy_reason=exc.code,
            )

    def get_theme_effective_members(
        self,
        collection_id: str,
        trade_date: date,
    ) -> pd.DataFrame:
        """Fetch effective member daily facts for a theme collection on a trade_date."""
        sql = f"""
        SELECT
            collection_id, theme_id, asset_id, trade_date,
            is_theme_member, confirmed_listing_trading_day_count,
            is_suspended, is_m4_effective_member, exclusion_reason,
            calculation_version, input_snapshot_id, production_run_id,
            created_at, finalized_at
        FROM {THEME_EFFECTIVE_MEMBER_DAILY_TABLE}
        WHERE collection_id = ? AND trade_date = ?
        ORDER BY asset_id ASC
        """
        return self.con.execute(sql, [collection_id, trade_date]).df()

    def audit_m4_observation(
        self,
        theme_id: str,
        trade_date: date,
        knowledge_date: date | None = None,
    ) -> M4ObservationAuditReport:
        """Set-based, non-N+1 lineage audit for a specific Theme M4 Observation."""
        # 1. Fetch persisted observation
        obs_row = self.con.execute(
            f"""
            SELECT
                theme_id, collection_id, trade_date, theme_daily_return,
                theme_limit_up_count, theme_return_rank, effective_member_count,
                total_member_count, comparison_universe_size, comparison_universe_version,
                custom_index_trend_state, custom_index_trend_run_days, custom_index_episode_id,
                qualification_status, production_run_id, input_snapshot_id
            FROM {THEME_M4_OBSERVATION_TABLE}
            WHERE theme_id = ? AND trade_date = ?
            """,
            [theme_id, trade_date],
        ).fetchone()

        if not obs_row:
            raise ValueError(f"No M4 observation found for theme {theme_id} on {trade_date}")

        collection_id = obs_row[1]
        prod_run_id = obs_row[14]
        snap_id = obs_row[15]

        # Resolve persisted production knowledge_date if not explicitly supplied
        persisted_kd = None
        if prod_run_id:
            run_kd_row = self.con.execute(
                f"SELECT knowledge_date FROM {THEME_PRODUCTION_RUN_TABLE} WHERE production_run_id = ?",
                [prod_run_id],
            ).fetchone()
            if run_kd_row and run_kd_row[0]:
                persisted_kd = run_kd_row[0]
                if hasattr(persisted_kd, "date"):
                    persisted_kd = persisted_kd.date()
                elif isinstance(persisted_kd, str):
                    persisted_kd = pd.to_datetime(persisted_kd).date()

        kd = knowledge_date if knowledge_date is not None else (persisted_kd or trade_date)

        # 2. Check persisted theme_effective_member_daily fact table first
        persisted_eff_df = self.get_theme_effective_members(collection_id, trade_date)
        excluded = []
        effective_assets = []
        limit_up_assets = []

        if not persisted_eff_df.empty:
            effective_assets = persisted_eff_df[persisted_eff_df["is_m4_effective_member"] == True]["asset_id"].tolist()
            excluded = [
                {
                    "asset_id": r.asset_id,
                    "reason": r.exclusion_reason,
                    "listing_trading_days": r.confirmed_listing_trading_day_count,
                }
                for r in persisted_eff_df[persisted_eff_df["is_m4_effective_member"] == False].itertuples()
            ]
            if effective_assets:
                snap_sql = f"""
                SELECT ticker, is_limit_up
                FROM daily_market_snapshot
                WHERE trade_date = ? AND ticker IN ({','.join(['?']*len(effective_assets))})
                """
                snap_rows = self.con.execute(snap_sql, [trade_date, *effective_assets]).fetchall()
                limit_up_assets = [r[0] for r in snap_rows if r[1]]
        else:
            ctx = StockCollectionQueryContext(as_of_date=trade_date, knowledge_date=kd)
            resolved_members = self.resolver.resolve_members(collection_id, ctx)
            asset_ids = [m.asset_id for m in resolved_members]

            if asset_ids:
                # Query confirmed listing facts using System B semantics
                listing_df = query_confirmed_listing_facts(
                    self.con,
                    end_date=trade_date,
                    start_date=trade_date,
                    asset_ids=asset_ids,
                )
                list_map = {}
                if not listing_df.empty:
                    for _, row in listing_df.iterrows():
                        list_map[row[ASSET_ID]] = (
                            int(row[CONFIRMED_LISTING_TRADING_DAY_COUNT])
                            if pd.notna(row[CONFIRMED_LISTING_TRADING_DAY_COUNT])
                            else None,
                            row.get("market_fact_status"),
                        )

                # Suspension batch
                susp_sql = f"""
                SELECT ticker FROM suspend_d
                WHERE trade_date = ? AND ticker IN ({','.join(['?']*len(asset_ids))})
                """
                susp_set = {r[0] for r in self.con.execute(susp_sql, [trade_date, *asset_ids]).fetchall()}

                # Market snapshot batch: strictly use official is_limit_up
                snap_sql = f"""
                SELECT ticker, pct_change, is_limit_up
                FROM daily_market_snapshot
                WHERE trade_date = ? AND ticker IN ({','.join(['?']*len(asset_ids))})
                """
                snap_rows = {r[0]: (r[1], bool(r[2])) for r in self.con.execute(snap_sql, [trade_date, *asset_ids]).fetchall()}

                for m in resolved_members:
                    list_info = list_map.get(m.asset_id)
                    list_days = list_info[0] if list_info else None
                    m_status = list_info[1] if list_info else None
                    is_susp = (m.asset_id in susp_set) or (m_status == "EXPLICIT_NON_TRADING")

                    if list_days is None or m_status == "UNRESOLVED_MISSING":
                        excluded.append({
                            "asset_id": m.asset_id,
                            "reason": "UNCONFIRMED_LISTING_DAYS",
                            "listing_trading_days": None,
                        })
                    elif list_days <= 5:
                        excluded.append({
                            "asset_id": m.asset_id,
                            "reason": "NEW_LISTING_LE_5",
                            "listing_trading_days": list_days,
                        })
                    elif is_susp:
                        excluded.append({
                            "asset_id": m.asset_id,
                            "reason": "SUSPENDED",
                            "listing_trading_days": list_days,
                        })
                    else:
                        effective_assets.append(m.asset_id)
                        snap_data = snap_rows.get(m.asset_id)
                        if snap_data and snap_data[1]:
                            limit_up_assets.append(m.asset_id)

        total_member_count = len(persisted_eff_df) if not persisted_eff_df.empty else (len(resolved_members) if 'resolved_members' in locals() else 0)

        # 4. Comparison universe batch
        try:
            comp_rows = self.con.execute(
                """
                SELECT index_code, pct_change / 100.0
                FROM ths_daily
                WHERE trade_date = ?
                  AND (index_code LIKE '881%' OR index_code LIKE '885%' OR index_code LIKE '886%')
                ORDER BY pct_change DESC
                """,
                [trade_date],
            ).fetchall()
        except Exception:
            comp_rows = []
        comparison_boards = [{"board_id": r[0], "return": r[1]} for r in comp_rows]

        # 5. Detect source drift against persisted theme_production_run
        is_reproducible = True
        drift_status = None

        if prod_run_id:
            try:
                run_row = self.con.execute(
                    f"""
                    SELECT target_start_date, target_end_date, knowledge_date,
                           calculation_version, rule_version, comparison_universe_version, input_snapshot_id
                    FROM {THEME_PRODUCTION_RUN_TABLE}
                    WHERE production_run_id = ?
                    """,
                    [prod_run_id],
                ).fetchone()

                if not run_row:
                    is_reproducible = False
                    drift_status = "AUDIT_RECONSTRUCTION_FAILED"
                else:
                    r_start, r_end, r_kd, r_calc_v, r_rule_v, r_comp_v, r_snap_id = run_row
                    audit_kd = kd or r_kd

                    recon_trade_dates = [trade_date]

                    recon_themes = [
                        (r[0], r[1])
                        for r in self.con.execute(
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
                    ]
                    recon_colls = [t[1] for t in recon_themes]
                    recon_mem = self.resolver.batch_resolve_members(
                        recon_colls, recon_trade_dates, trade_date, enforce_admission_cutoff=True
                    )
                    recon_list = query_confirmed_listing_facts(self.con, end_date=trade_date, start_date=trade_date)
                    recon_susp = self.con.execute(
                        "SELECT ticker AS asset_id, trade_date, true AS is_suspended FROM suspend_d WHERE trade_date = ?",
                        [trade_date],
                    ).df()
                    recon_mkt = self.con.execute(
                        "SELECT ticker AS asset_id, trade_date, pct_change, close, is_limit_up FROM daily_market_snapshot WHERE trade_date = ?",
                        [trade_date],
                    ).df()
                    recon_comp = self.con.execute(
                        """
                        SELECT index_code AS board_id, trade_date, pct_change / 100.0 AS board_return
                        FROM ths_daily
                        WHERE trade_date = ?
                          AND (index_code LIKE '881%' OR index_code LIKE '885%' OR index_code LIKE '886%')
                        ORDER BY trade_date, index_code
                        """,
                        [trade_date],
                    ).df()

                    recon_versions = {
                        "calculation_version": r_calc_v,
                        "effective_member_version": THEME_EFFECTIVE_MEMBER_VERSION,
                        "rule_version": r_rule_v,
                        "comparison_universe_version": r_comp_v,
                    }

                    reconstructed_digest = compute_deterministic_snapshot_id(
                        themes=recon_themes,
                        memberships=recon_mem,
                        listing_df=recon_list,
                        susp_df=recon_susp,
                        market_df=recon_mkt,
                        comp_boards_df=recon_comp,
                        versions=recon_versions,
                    )

                    target_snap_id = snap_id or r_snap_id
                    if reconstructed_digest != target_snap_id:
                        is_reproducible = False
                        drift_status = "CURRENT_SOURCE_DIFFERS_FROM_PRODUCTION_SNAPSHOT"
                    else:
                        is_reproducible = True
                        drift_status = None
            except Exception:
                is_reproducible = False
                drift_status = "AUDIT_RECONSTRUCTION_FAILED"
        else:
            is_reproducible = False
            drift_status = "AUDIT_RECONSTRUCTION_FAILED"

        return M4ObservationAuditReport(
            theme_id=obs_row[0],
            collection_id=obs_row[1],
            trade_date=obs_row[2],
            theme_daily_return=obs_row[3],
            theme_limit_up_count=obs_row[4],
            theme_return_rank=obs_row[5],
            effective_members=len(effective_assets),
            total_members=total_member_count,
            comparison_universe_size=obs_row[8],
            comparison_universe_version=obs_row[9],
            custom_index_trend_state=obs_row[10],
            custom_index_trend_run_days=obs_row[11],
            custom_index_episode_id=obs_row[12],
            qualification_status=obs_row[13],
            production_run_id=prod_run_id,
            input_snapshot_id=snap_id,
            effective_member_assets=tuple(effective_assets),
            excluded_members=tuple(excluded),
            limit_up_assets=tuple(limit_up_assets),
            comparison_boards=tuple(comparison_boards),
            is_reproducible=is_reproducible,
            discrepancy_reason=drift_status,
            production_knowledge_date=persisted_kd,
            audit_knowledge_date=kd,
        )
