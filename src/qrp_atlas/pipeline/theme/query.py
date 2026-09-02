"""Query service and explainability audit for Theme custom indices and M4 observations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Sequence
import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    STOCK_COLLECTION_TABLE,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_M4_OBSERVATION_TABLE,
    THEME_TABLE,
)
from qrp_atlas.stock_collections.models import (
    StockCollectionQueryContext,
    StockCollectionRecord,
)
from qrp_atlas.stock_collections.resolver import StockCollectionResolver


@dataclass(frozen=True)
class M4ObservationAuditReport:
    """Detailed audit trace for a specific (theme_id, trade_date) M4 observation."""
    theme_id: str
    collection_id: str
    trade_date: date
    knowledge_date: date
    total_members: int
    effective_members: int
    excluded_members: list[dict[str, object]]
    effective_member_assets: list[str]
    theme_daily_return: float | None
    theme_limit_up_count: int
    theme_return_rank: int | None
    comparison_universe_size: int
    custom_index_trend_state: str | None
    custom_index_episode_id: str | None
    qualification_status: str


class ThemeQueryService:
    """Read service providing PIT queries and auditability for theme indices and M4 observations."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.con = connection
        self.resolver = StockCollectionResolver(connection)

    def get_theme_index_history(
        self,
        theme_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Query theme custom index history."""
        clauses = ["theme_id = ?"]
        params: list[object] = [theme_id]
        if start_date is not None:
            clauses.append("trade_date >= ?")
            params.append(start_date)
        if end_date is not None:
            clauses.append("trade_date <= ?")
            params.append(end_date)

        where_sql = " AND ".join(clauses)
        sql = f"""
        SELECT *
        FROM {THEME_CUSTOM_INDEX_DAILY_TABLE}
        WHERE {where_sql}
        ORDER BY trade_date ASC
        """
        return self.con.execute(sql, params).df()

    def get_theme_index_current_state(
        self,
        theme_id: str,
        trade_date: date,
    ) -> dict[str, object] | None:
        """Query latest trend state of a theme on a specific trade_date."""
        sql = f"""
        SELECT *
        FROM {THEME_CUSTOM_INDEX_STATE_TABLE}
        WHERE theme_id = ? AND trade_date = ?
        """
        row = self.con.execute(sql, [theme_id, trade_date]).fetchone()
        if not row:
            return None
        cols = [col[0] for col in self.con.description]
        return dict(zip(cols, row))

    def get_theme_episodes(
        self,
        theme_id: str,
    ) -> pd.DataFrame:
        """Query all trend episodes for a theme."""
        sql = f"""
        SELECT *
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
        """Query M4 raw observations on a trade_date."""
        clauses = ["trade_date = ?"]
        params: list[object] = [trade_date]
        if theme_id is not None:
            clauses.append("theme_id = ?")
            params.append(theme_id)

        where_sql = " AND ".join(clauses)
        sql = f"""
        SELECT *
        FROM {THEME_M4_OBSERVATION_TABLE}
        WHERE {where_sql}
        ORDER BY theme_return_rank ASC NULLS LAST, theme_id ASC
        """
        return self.con.execute(sql, params).df()

    def audit_m4_observation(
        self,
        theme_id: str,
        trade_date: date,
        knowledge_date: date | None = None,
    ) -> M4ObservationAuditReport:
        """Generate comprehensive audit trail explaining how M4 observation was derived."""
        k_date = knowledge_date or trade_date

        # 1. Fetch observation row
        obs_df = self.get_m4_observations(trade_date, theme_id=theme_id)
        if obs_df.empty:
            raise ValueError(f"No M4 observation found for theme '{theme_id}' on {trade_date}")
        obs = obs_df.iloc[0]
        cid = obs["collection_id"]

        # 2. Resolve PIT members
        resolved_members = self.resolver.resolve_members(
            cid,
            as_of_date=trade_date,
            knowledge_date=k_date,
        )
        total_assets = [m.asset_id for m in resolved_members]

        # 3. Check listing days & suspensions
        excluded: list[dict[str, object]] = []
        effective_assets: list[str] = []

        for m in resolved_members:
            # Check listing trading days
            row = self.con.execute(
                """
                SELECT s.list_date, COUNT(c.trade_date)
                FROM stock_info s
                LEFT JOIN trading_calendar c
                  ON c.trade_date >= s.list_date
                 AND c.trade_date <= ?
                 AND c.is_open = true
                WHERE s.ticker = ?
                GROUP BY s.list_date
                """,
                [trade_date, m.asset_id],
            ).fetchone()

            if not row:
                list_days = 999999
            else:
                list_date, count_days = row
                if (trade_date - list_date).days > 30:
                    list_days = 999999
                else:
                    list_days = count_days

            is_susp = bool(
                self.con.execute(
                    "SELECT COUNT(*) FROM suspend_d WHERE ticker = ? AND trade_date = ?",
                    [m.asset_id, trade_date],
                ).fetchone()[0]
            )

            if list_days <= 5:
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

        return M4ObservationAuditReport(
            theme_id=theme_id,
            collection_id=cid,
            trade_date=trade_date,
            knowledge_date=k_date,
            total_members=len(total_assets),
            effective_members=len(effective_assets),
            excluded_members=excluded,
            effective_member_assets=effective_assets,
            theme_daily_return=obs["theme_daily_return"] if pd.notna(obs["theme_daily_return"]) else None,
            theme_limit_up_count=int(obs["theme_limit_up_count"]) if pd.notna(obs["theme_limit_up_count"]) else 0,
            theme_return_rank=int(obs["theme_return_rank"]) if pd.notna(obs["theme_return_rank"]) else None,
            comparison_universe_size=int(obs["comparison_universe_size"]) if pd.notna(obs["comparison_universe_size"]) else 0,
            custom_index_trend_state=obs["custom_index_trend_state"] if pd.notna(obs["custom_index_trend_state"]) else None,
            custom_index_episode_id=obs["custom_index_episode_id"] if pd.notna(obs["custom_index_episode_id"]) else None,
            qualification_status=str(obs["qualification_status"]),
        )
