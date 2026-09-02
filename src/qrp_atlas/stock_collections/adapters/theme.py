"""Theme membership adapter implementing StockCollection resolution contracts."""

from __future__ import annotations

from typing import Sequence

import duckdb

from qrp_atlas.contracts.stock_collection import (
    CollectionType,
    THEME_MEMBERSHIP_HISTORY_TABLE,
)
from qrp_atlas.stock_collections.models import (
    MembershipExplanation,
    ResolvedMember,
    StockCollectionQueryContext,
)


class ThemeAdapter:
    """Adapter for resolving THEME stock collections from theme_membership_history."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.con = connection

    def resolve_members(
        self,
        collection_id: str,
        context: StockCollectionQueryContext,
    ) -> Sequence[ResolvedMember]:
        """Resolve valid PIT members of a THEME collection for as_of_date @ knowledge_date.

        Algorithm:
        1. Filter records where available_trade_date <= knowledge_date.
        2. Group by membership_id, select the latest revision by (available_trade_date DESC, ingested_at DESC).
        3. Check effective interval: effective_from <= as_of_date AND (effective_to IS NULL OR as_of_date < effective_to).
        4. Return normalized ResolvedMember (weight=None).
        """
        sql = f"""
        WITH visible_revisions AS (
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
                ROW_NUMBER() OVER (
                    PARTITION BY membership_id
                    ORDER BY available_trade_date DESC, ingested_at DESC
                ) as rn
            FROM {THEME_MEMBERSHIP_HISTORY_TABLE}
            WHERE collection_id = ?
              AND available_trade_date <= ?
        ),
        latest_revisions AS (
            SELECT *
            FROM visible_revisions
            WHERE rn = 1
        )
        SELECT
            collection_id,
            asset_id,
            source_record_id,
            revision_id
        FROM latest_revisions
        WHERE effective_from <= ?
          AND (effective_to IS NULL OR ? < effective_to)
        ORDER BY asset_id ASC
        """
        rows = self.con.execute(
            sql,
            [collection_id, context.knowledge_date, context.as_of_date, context.as_of_date],
        ).fetchall()

        return [
            ResolvedMember(
                collection_id=r[0],
                collection_type=CollectionType.THEME,
                asset_id=r[1],
                as_of_date=context.as_of_date,
                weight=None,
                source_table=THEME_MEMBERSHIP_HISTORY_TABLE,
                source_record_id=r[2],
                source_revision_id=r[3],
                source_rule_version=None,
            )
            for r in rows
        ]

    def resolve_asset_collections(
        self,
        asset_id: str,
        context: StockCollectionQueryContext,
    ) -> Sequence[str]:
        """Reverse-lookup: find all active THEME collections the asset belongs to as of as_of_date."""
        sql = f"""
        WITH visible_revisions AS (
            SELECT
                membership_id,
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
            WHERE asset_id = ?
              AND available_trade_date <= ?
        ),
        latest_revisions AS (
            SELECT *
            FROM visible_revisions
            WHERE rn = 1
        )
        SELECT DISTINCT collection_id
        FROM latest_revisions
        WHERE effective_from <= ?
          AND (effective_to IS NULL OR ? < effective_to)
        ORDER BY collection_id ASC
        """
        rows = self.con.execute(
            sql,
            [asset_id, context.knowledge_date, context.as_of_date, context.as_of_date],
        ).fetchall()
        return [r[0] for r in rows]

    def explain_membership(
        self,
        collection_id: str,
        asset_id: str,
        context: StockCollectionQueryContext,
    ) -> MembershipExplanation:
        """Explain the membership status and PIT reasoning of an asset for a collection."""
        sql = f"""
        SELECT
            membership_id,
            revision_id,
            effective_from,
            effective_to,
            available_trade_date,
            source,
            source_record_id
        FROM {THEME_MEMBERSHIP_HISTORY_TABLE}
        WHERE collection_id = ?
          AND asset_id = ?
          AND available_trade_date <= ?
        ORDER BY available_trade_date DESC, ingested_at DESC
        LIMIT 1
        """
        row = self.con.execute(
            sql,
            [collection_id, asset_id, context.knowledge_date],
        ).fetchone()

        if not row:
            return MembershipExplanation(
                collection_id=collection_id,
                asset_id=asset_id,
                is_member=False,
                as_of_date=context.as_of_date,
                knowledge_date=context.knowledge_date,
                membership_id=None,
                revision_id=None,
                effective_from=None,
                effective_to=None,
                available_trade_date=None,
                source=None,
                source_record_id=None,
                reasons=("NO_VISIBLE_MEMBERSHIP_REVISION",),
            )

        mid, rev_id, eff_from, eff_to, avail_date, src, src_rec = row
        reasons: list[str] = []
        is_member = True

        if eff_from > context.as_of_date:
            is_member = False
            reasons.append(f"EFFECTIVE_FROM_IN_FUTURE: {eff_from} > {context.as_of_date}")

        if eff_to is not None and context.as_of_date >= eff_to:
            is_member = False
            reasons.append(f"EFFECTIVE_TO_EXPIRED: {context.as_of_date} >= {eff_to}")

        if is_member:
            reasons.append("VALID_POINT_IN_TIME_MEMBER")

        return MembershipExplanation(
            collection_id=collection_id,
            asset_id=asset_id,
            is_member=is_member,
            as_of_date=context.as_of_date,
            knowledge_date=context.knowledge_date,
            membership_id=mid,
            revision_id=rev_id,
            effective_from=eff_from,
            effective_to=eff_to,
            available_trade_date=avail_date,
            source=src,
            source_record_id=src_rec,
            reasons=tuple(reasons),
        )
