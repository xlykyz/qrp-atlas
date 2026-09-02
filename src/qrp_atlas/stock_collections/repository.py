"""DuckDB persistence repository for StockCollection and Theme domain."""

from __future__ import annotations

from datetime import date, datetime
from typing import Sequence

import duckdb

from qrp_atlas.contracts.stock_collection import (
    STOCK_COLLECTION_TABLE,
    THEME_MEMBERSHIP_HISTORY_TABLE,
    THEME_TABLE,
)
from qrp_atlas.stock_collections.models import (
    StockCollectionError,
    StockCollectionErrorCode,
    StockCollectionRecord,
    ThemeMembershipRecord,
    ThemeRecord,
)


class StockCollectionRepository:
    """Repository handling atomic write and PIT read for StockCollection & Theme tables."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.con = connection

    def create_theme_collection_atomic(
        self,
        theme: ThemeRecord,
        collection: StockCollectionRecord,
    ) -> None:
        """Atomically create Theme and its corresponding THEME StockCollection (1:1)."""
        if theme.collection_id != collection.collection_id:
            raise StockCollectionError(
                StockCollectionErrorCode.COLLECTION_SOURCE_INCONSISTENT,
                f"theme.collection_id ({theme.collection_id}) != collection.collection_id ({collection.collection_id})",
            )

        # Check existing collection_id with different source_key or type (collision)
        existing = self.con.execute(
            f"SELECT collection_type, namespace, source_key FROM {STOCK_COLLECTION_TABLE} WHERE collection_id = ?",
            [collection.collection_id],
        ).fetchone()
        if existing:
            ctype, ns, sk = existing
            if (
                str(ctype) != str(collection.collection_type)
                or str(ns) != str(collection.namespace)
                or str(sk) != str(collection.source_key)
            ):
                raise StockCollectionError(
                    StockCollectionErrorCode.COLLECTION_IDENTITY_COLLISION,
                    f"Collection ID collision on {collection.collection_id}",
                )

        self.con.execute(
            f"""
            INSERT INTO {STOCK_COLLECTION_TABLE} (
                collection_id, collection_type, collection_scope, namespace, source_key,
                canonical_name, membership_model, status, effective_from, effective_to,
                available_trade_date, source, source_record_id, revision_id, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                collection.collection_id,
                str(collection.collection_type),
                str(collection.collection_scope),
                collection.namespace,
                collection.source_key,
                collection.canonical_name,
                str(collection.membership_model),
                str(collection.status),
                collection.effective_from,
                collection.effective_to,
                collection.available_trade_date,
                collection.source,
                collection.source_record_id,
                collection.revision_id,
                collection.ingested_at,
            ],
        )

        self.con.execute(
            f"""
            INSERT INTO {THEME_TABLE} (
                theme_id, collection_id, canonical_name, status, effective_from,
                effective_to, available_trade_date, source, source_record_id,
                revision_id, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                theme.theme_id,
                theme.collection_id,
                theme.canonical_name,
                str(theme.status),
                theme.effective_from,
                theme.effective_to,
                theme.available_trade_date,
                theme.source,
                theme.source_record_id,
                theme.revision_id,
                theme.ingested_at,
            ],
        )

    def append_theme_membership_revisions(
        self,
        records: Sequence[ThemeMembershipRecord],
    ) -> None:
        """Append one or more theme membership revisions (append-only)."""
        if not records:
            return

        for r in records:
            self.con.execute(
                f"""
                INSERT INTO {THEME_MEMBERSHIP_HISTORY_TABLE} (
                    membership_id, theme_id, collection_id, asset_id, effective_from,
                    effective_to, available_trade_date, source, source_record_id,
                    revision_id, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r.membership_id,
                    r.theme_id,
                    r.collection_id,
                    r.asset_id,
                    r.effective_from,
                    r.effective_to,
                    r.available_trade_date,
                    r.source,
                    r.source_record_id,
                    r.revision_id,
                    r.ingested_at,
                ],
            )

    def get_collection_record(
        self,
        collection_id: str,
        knowledge_date: date,
    ) -> StockCollectionRecord | None:
        """Get the latest revision of a collection visible as of knowledge_date."""
        row = self.con.execute(
            f"""
            SELECT
                collection_id, collection_type, collection_scope, namespace, source_key,
                canonical_name, membership_model, status, effective_from, effective_to,
                available_trade_date, source, source_record_id, revision_id, ingested_at
            FROM {STOCK_COLLECTION_TABLE}
            WHERE collection_id = ?
              AND available_trade_date <= ?
            ORDER BY available_trade_date DESC, ingested_at DESC
            LIMIT 1
            """,
            [collection_id, knowledge_date],
        ).fetchone()
        if not row:
            return None
        return StockCollectionRecord(
            collection_id=row[0],
            collection_type=row[1],
            collection_scope=row[2],
            namespace=row[3],
            source_key=row[4],
            canonical_name=row[5],
            membership_model=row[6],
            status=row[7],
            effective_from=row[8],
            effective_to=row[9],
            available_trade_date=row[10],
            source=row[11],
            source_record_id=row[12],
            revision_id=row[13],
            ingested_at=row[14],
        )

    def get_theme_record(
        self,
        theme_id: str,
        knowledge_date: date,
    ) -> ThemeRecord | None:
        """Get the latest revision of a theme visible as of knowledge_date."""
        row = self.con.execute(
            f"""
            SELECT
                theme_id, collection_id, canonical_name, status, effective_from,
                effective_to, available_trade_date, source, source_record_id,
                revision_id, ingested_at
            FROM {THEME_TABLE}
            WHERE theme_id = ?
              AND available_trade_date <= ?
            ORDER BY available_trade_date DESC, ingested_at DESC
            LIMIT 1
            """,
            [theme_id, knowledge_date],
        ).fetchone()
        if not row:
            return None
        return ThemeRecord(
            theme_id=row[0],
            collection_id=row[1],
            canonical_name=row[2],
            status=row[3],
            effective_from=row[4],
            effective_to=row[5],
            available_trade_date=row[6],
            source=row[7],
            source_record_id=row[8],
            revision_id=row[9],
            ingested_at=row[10],
        )

    def get_pit_theme_membership_revisions(
        self,
        *,
        collection_id: str | None = None,
        theme_id: str | None = None,
        asset_id: str | None = None,
        knowledge_date: date,
    ) -> Sequence[ThemeMembershipRecord]:
        """Fetch all visible revisions for theme memberships up to knowledge_date."""
        clauses = ["available_trade_date <= ?"]
        params: list[object] = [knowledge_date]

        if collection_id is not None:
            clauses.append("collection_id = ?")
            params.append(collection_id)
        if theme_id is not None:
            clauses.append("theme_id = ?")
            params.append(theme_id)
        if asset_id is not None:
            clauses.append("asset_id = ?")
            params.append(asset_id)

        where_sql = " AND ".join(clauses)
        sql = f"""
            SELECT
                membership_id, theme_id, collection_id, asset_id, effective_from,
                effective_to, available_trade_date, source, source_record_id,
                revision_id, ingested_at
            FROM {THEME_MEMBERSHIP_HISTORY_TABLE}
            WHERE {where_sql}
            ORDER BY membership_id, available_trade_date ASC, ingested_at ASC
        """
        rows = self.con.execute(sql, params).fetchall()
        return [
            ThemeMembershipRecord(
                membership_id=r[0],
                theme_id=r[1],
                collection_id=r[2],
                asset_id=r[3],
                effective_from=r[4],
                effective_to=r[5],
                available_trade_date=r[6],
                source=r[7],
                source_record_id=r[8],
                revision_id=r[9],
                ingested_at=r[10],
            )
            for r in rows
        ]
