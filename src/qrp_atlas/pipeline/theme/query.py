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
)
from qrp_atlas.contracts.stock_collection import (
    COLLECTION_ID,
    CollectionScope,
    CollectionType,
    THEME_ID,
    THEME_MEMBERSHIP_HISTORY_TABLE,
    THEME_TABLE,
)
from qrp_atlas.stock_collections.models import StockCollectionQueryContext
from qrp_atlas.stock_collections.resolver import StockCollectionResolver


@dataclass(frozen=True)
class M4ObservationAuditReport:
    theme_id: str
    collection_id: str
    trade_date: date
    theme_daily_return: float | None
    theme_limit_up_count: int
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

    def audit_m4_observation(
        self,
        theme_id: str,
        trade_date: date,
        knowledge_date: date | None = None,
    ) -> M4ObservationAuditReport:
        """Set-based, non-N+1 lineage audit for a specific Theme M4 Observation."""
        kd = knowledge_date or trade_date

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

        # 2. Batch resolve members via StockCollectionResolver
        ctx = StockCollectionQueryContext(as_of_date=trade_date, knowledge_date=kd)
        resolved_members = self.resolver.resolve_members(collection_id, ctx)
        asset_ids = [m.asset_id for m in resolved_members]

        # 3. Batch query listing facts, suspension facts, and returns
        excluded = []
        effective_assets = []
        limit_up_assets = []

        if asset_ids:
            # Listing days batch
            list_sql = f"""
            WITH stock_dates AS (
                SELECT
                    s.ticker AS asset_id,
                    COUNT(c2.trade_date) AS confirmed_listing_trading_day_count
                FROM stock_info s
                JOIN trading_calendar c2
                  ON c2.trade_date >= s.list_date
                 AND c2.trade_date <= ?
                 AND c2.is_open = true
                WHERE s.ticker IN ({','.join(['?']*len(asset_ids))})
                GROUP BY s.ticker
            )
            SELECT asset_id, confirmed_listing_trading_day_count FROM stock_dates
            """
            list_rows = dict(self.con.execute(list_sql, [trade_date, *asset_ids]).fetchall())

            # Suspension batch
            susp_sql = f"""
            SELECT ticker FROM suspend_d
            WHERE trade_date = ? AND ticker IN ({','.join(['?']*len(asset_ids))})
            """
            susp_set = {r[0] for r in self.con.execute(susp_sql, [trade_date, *asset_ids]).fetchall()}

            # Market snapshot batch
            snap_sql = f"""
            SELECT ticker, pct_change, (pct_change >= 9.8) as is_lu
            FROM daily_market_snapshot
            WHERE trade_date = ? AND ticker IN ({','.join(['?']*len(asset_ids))})
            """
            snap_rows = {r[0]: (r[1], r[2]) for r in self.con.execute(snap_sql, [trade_date, *asset_ids]).fetchall()}

            for m in resolved_members:
                list_days = list_rows.get(m.asset_id)
                is_susp = m.asset_id in susp_set

                if list_days is None:
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

        # 4. Comparison universe batch
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
        comparison_boards = [{"board_id": r[0], "return": r[1]} for r in comp_rows]

        return M4ObservationAuditReport(
            theme_id=obs_row[0],
            collection_id=obs_row[1],
            trade_date=obs_row[2],
            theme_daily_return=obs_row[3],
            theme_limit_up_count=obs_row[4],
            theme_return_rank=obs_row[5],
            effective_members=len(effective_assets),
            total_members=len(resolved_members),
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
        )
