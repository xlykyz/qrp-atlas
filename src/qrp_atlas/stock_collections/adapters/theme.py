"""Theme StockCollection Adapter implementing PIT membership resolution and explainability."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any

import duckdb
import pandas as pd

from qrp_atlas.contracts.stock_collection import (
    STOCK_COLLECTION_TABLE,
    THEME_MEMBERSHIP_HISTORY_TABLE,
)

from ..models import (
    MembershipExplanation,
    ResolvedMember,
    StockCollectionError,
    StockCollectionQueryContext,
)


class ThemeAdapter:
    """Adapter for THEME StockCollections resolving PIT memberships and lineage."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def resolve_members(
        self,
        collection_id: str,
        context: StockCollectionQueryContext,
    ) -> list[ResolvedMember]:
        """Resolve theme members strictly under dual-time PIT semantics."""
        sql = f"""
        WITH latest_visible_revisions AS (
            SELECT
                membership_id,
                theme_id,
                collection_id,
                asset_id,
                effective_from,
                effective_to,
                available_trade_date,
                source_record_id,
                revision_id,
                row_number() OVER (
                    PARTITION BY membership_id
                    ORDER BY available_trade_date DESC, ingested_at DESC
                ) as rn
            FROM {THEME_MEMBERSHIP_HISTORY_TABLE}
            WHERE collection_id = ?
              AND available_trade_date <= ?
        )
        SELECT
            collection_id,
            asset_id,
            membership_id,
            revision_id,
            effective_from,
            effective_to,
            available_trade_date,
            source_record_id
        FROM latest_visible_revisions
        WHERE rn = 1
          AND effective_from <= ?
          AND (effective_to IS NULL OR ? < effective_to)
        ORDER BY asset_id ASC
        """
        rows = self.con.execute(
            sql,
            [
                collection_id,
                context.knowledge_date,
                context.as_of_date,
                context.as_of_date,
            ],
        ).fetchall()

        return [
            ResolvedMember(
                collection_id=r[0],
                asset_id=r[1],
                as_of_date=context.as_of_date,
                weight=None,
                membership_id=r[2],
                revision_id=r[3],
                effective_from=r[4],
                effective_to=r[5],
                available_trade_date=r[6],
                source_table=THEME_MEMBERSHIP_HISTORY_TABLE,
                source_record_id=r[7],
            )
            for r in rows
        ]

    def batch_resolve_members(
        self,
        collection_ids: Sequence[str],
        trade_dates: Sequence[date],
        knowledge_date: date,
    ) -> pd.DataFrame:
        """Set-based vectorized batch resolution across multiple collections and dates."""
        if not collection_ids or not trade_dates:
            return pd.DataFrame(
                columns=[
                    "collection_id",
                    "asset_id",
                    "trade_date",
                    "membership_id",
                    "revision_id",
                    "effective_from",
                    "effective_to",
                    "available_trade_date",
                ]
            )

        coll_filter = "AND collection_id IN (" + ",".join(["?"] * len(collection_ids)) + ")"

        sql = f"""
        WITH visible_collections AS (
            SELECT
                collection_id,
                effective_from AS coll_effective_from,
                effective_to AS coll_effective_to,
                status AS coll_status,
                row_number() OVER (
                    PARTITION BY collection_id
                    ORDER BY available_trade_date DESC, ingested_at DESC
                ) as rn
            FROM {STOCK_COLLECTION_TABLE}
            WHERE available_trade_date <= ?
              {coll_filter}
        ),
        latest_collections AS (
            SELECT * FROM visible_collections WHERE rn = 1 AND coll_status = 'ACTIVE'
        ),
        visible_revisions AS (
            SELECT
                membership_id,
                theme_id,
                collection_id,
                asset_id,
                effective_from,
                effective_to,
                available_trade_date,
                revision_id,
                row_number() OVER (
                    PARTITION BY membership_id
                    ORDER BY available_trade_date DESC, ingested_at DESC
                ) as rn
            FROM {THEME_MEMBERSHIP_HISTORY_TABLE}
            WHERE available_trade_date <= ?
              {coll_filter}
        ),
        latest_revisions AS (
            SELECT * FROM visible_revisions WHERE rn = 1
        ),
        dates AS (
            SELECT unnest(?::DATE[]) as trade_date
        )
        SELECT
            r.collection_id,
            r.asset_id,
            d.trade_date,
            r.membership_id,
            r.revision_id,
            r.effective_from,
            r.effective_to,
            r.available_trade_date
        FROM latest_revisions r
        JOIN latest_collections c
          ON r.collection_id = c.collection_id
        JOIN dates d
          ON r.effective_from <= d.trade_date
         AND (r.effective_to IS NULL OR d.trade_date < r.effective_to)
         AND c.coll_effective_from <= d.trade_date
         AND (c.coll_effective_to IS NULL OR d.trade_date < c.coll_effective_to)
        ORDER BY r.collection_id, d.trade_date, r.asset_id
        """
        params: list[Any] = [
            knowledge_date,
            *collection_ids,
            knowledge_date,
            *collection_ids,
            list(trade_dates),
        ]
        return self.con.execute(sql, params).df()

    def reverse_lookup(
        self,
        asset_id: str,
        context: StockCollectionQueryContext,
    ) -> list[ResolvedMember]:
        """Find all THEME collections containing asset_id as of (as_of_date, knowledge_date)."""
        sql = f"""
        WITH latest_visible_revisions AS (
            SELECT
                membership_id,
                theme_id,
                collection_id,
                asset_id,
                effective_from,
                effective_to,
                available_trade_date,
                source_record_id,
                revision_id,
                row_number() OVER (
                    PARTITION BY membership_id
                    ORDER BY available_trade_date DESC, ingested_at DESC
                ) as rn
            FROM {THEME_MEMBERSHIP_HISTORY_TABLE}
            WHERE asset_id = ?
              AND available_trade_date <= ?
        )
        SELECT
            collection_id,
            asset_id,
            membership_id,
            revision_id,
            effective_from,
            effective_to,
            available_trade_date,
            source_record_id
        FROM latest_visible_revisions
        WHERE rn = 1
          AND effective_from <= ?
          AND (effective_to IS NULL OR ? < effective_to)
        ORDER BY collection_id ASC
        """
        rows = self.con.execute(
            sql,
            [
                asset_id,
                context.knowledge_date,
                context.as_of_date,
                context.as_of_date,
            ],
        ).fetchall()

        return [
            ResolvedMember(
                collection_id=r[0],
                asset_id=r[1],
                as_of_date=context.as_of_date,
                weight=None,
                membership_id=r[2],
                revision_id=r[3],
                effective_from=r[4],
                effective_to=r[5],
                available_trade_date=r[6],
                source_table=THEME_MEMBERSHIP_HISTORY_TABLE,
                source_record_id=r[7],
            )
            for r in rows
        ]

    def explain_membership(
        self,
        collection_id: str,
        asset_id: str,
        context: StockCollectionQueryContext,
    ) -> MembershipExplanation:
        """Explain membership lifecycle and PIT visibility with 100% equivalence to resolve_members."""
        # Query all visible revisions for this asset up to knowledge_date
        sql = f"""
        SELECT
            membership_id,
            revision_id,
            effective_from,
            effective_to,
            available_trade_date,
            source,
            ingested_at
        FROM {THEME_MEMBERSHIP_HISTORY_TABLE}
        WHERE collection_id = ?
          AND asset_id = ?
          AND available_trade_date <= ?
        ORDER BY available_trade_date ASC, ingested_at ASC
        """
        rows = self.con.execute(
            sql, [collection_id, asset_id, context.knowledge_date]
        ).fetchall()

        history = [
            {
                "membership_id": r[0],
                "revision_id": r[1],
                "effective_from": r[2].isoformat() if hasattr(r[2], "isoformat") else str(r[2]),
                "effective_to": r[3].isoformat() if r[3] and hasattr(r[3], "isoformat") else (str(r[3]) if r[3] else None),
                "available_trade_date": r[4].isoformat() if hasattr(r[4], "isoformat") else str(r[4]),
                "source": r[5],
                "ingested_at": r[6].isoformat() if hasattr(r[6], "isoformat") else str(r[6]),
            }
            for r in rows
        ]

        # Use exact same resolution semantics as resolve_members:
        # Group by membership_id, take latest visible revision, and check interval
        resolved = self.resolve_members(collection_id, context)
        matching = [m for m in resolved if m.asset_id == asset_id]

        if matching:
            m = matching[0]
            return MembershipExplanation(
                collection_id=collection_id,
                asset_id=asset_id,
                as_of_date=context.as_of_date,
                knowledge_date=context.knowledge_date,
                is_member=True,
                membership_id=m.membership_id,
                revision_id=m.revision_id,
                effective_from=m.effective_from,
                effective_to=m.effective_to,
                available_trade_date=m.available_trade_date,
                reason="ACTIVE_INTERVAL",
                lifecycle_history=tuple(history),
            )

        if not history:
            reason = "NO_RECORDS_VISIBLE"
        else:
            reason = "OUTSIDE_EFFECTIVE_INTERVAL"

        return MembershipExplanation(
            collection_id=collection_id,
            asset_id=asset_id,
            as_of_date=context.as_of_date,
            knowledge_date=context.knowledge_date,
            is_member=False,
            membership_id=None,
            revision_id=None,
            effective_from=None,
            effective_to=None,
            available_trade_date=None,
            reason=reason,
            lifecycle_history=tuple(history),
        )
