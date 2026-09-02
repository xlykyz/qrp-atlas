"""DuckDB repository for StockCollection, Theme, and PIT Membership."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Any

import duckdb

from qrp_atlas.contracts.schema import (
    STOCK_COLLECTION,
    THEME,
    THEME_MEMBERSHIP_HISTORY,
)
from qrp_atlas.contracts.stock_collection import (
    STOCK_COLLECTION_TABLE,
    THEME_MEMBERSHIP_HISTORY_TABLE,
    THEME_TABLE,
)

from .models import (
    StockCollectionError,
    StockCollectionRecord,
    ThemeMembershipRecord,
    ThemeRecord,
)


class StockCollectionRepository:
    """Repository handling ACID persistence and queries for StockCollection domain."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def create_theme_collection_atomic(
        self,
        theme: ThemeRecord,
        collection: StockCollectionRecord,
    ) -> None:
        """Atomically insert Theme and StockCollection in a single transaction."""
        try:
            self.con.execute("BEGIN TRANSACTION")
            # 1. Insert Collection
            self.con.execute(
                f"""
                INSERT INTO {STOCK_COLLECTION_TABLE} (
                    collection_id, collection_type, collection_scope, namespace,
                    source_key, canonical_name, membership_model, status,
                    effective_from, effective_to, available_trade_date, source,
                    source_record_id, revision_id, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    collection.collection_id,
                    collection.collection_type,
                    collection.collection_scope,
                    collection.namespace,
                    collection.source_key,
                    collection.canonical_name,
                    collection.membership_model,
                    collection.status,
                    collection.effective_from,
                    collection.effective_to,
                    collection.available_trade_date,
                    collection.source,
                    collection.source_record_id,
                    collection.revision_id,
                    collection.ingested_at,
                ],
            )
            # 2. Insert Theme
            self.con.execute(
                f"""
                INSERT INTO {THEME_TABLE} (
                    theme_id, collection_id, canonical_name,
                    status, effective_from, effective_to, available_trade_date,
                    source, source_record_id, revision_id, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    theme.theme_id,
                    theme.collection_id,
                    theme.canonical_name,
                    theme.status,
                    theme.effective_from,
                    theme.effective_to,
                    theme.available_trade_date,
                    theme.source,
                    theme.source_record_id,
                    theme.revision_id,
                    theme.ingested_at,
                ],
            )
            self.con.execute("COMMIT")
        except Exception as exc:
            try:
                self.con.execute("ROLLBACK")
            except Exception:
                pass
            raise StockCollectionError("ATOMIC_CREATION_FAILED", str(exc)) from exc

    def get_collection_revisions(self, collection_id: str) -> list[StockCollectionRecord]:
        """Fetch all revisions for a collection."""
        rows = self.con.execute(
            f"""
            SELECT
                collection_id, collection_type, collection_scope, namespace,
                source_key, canonical_name, membership_model, status,
                effective_from, effective_to, available_trade_date, source,
                source_record_id, revision_id, ingested_at
            FROM {STOCK_COLLECTION_TABLE}
            WHERE collection_id = ?
            ORDER BY available_trade_date ASC, ingested_at ASC
            """,
            [collection_id],
        ).fetchall()
        return [
            StockCollectionRecord(
                collection_id=r[0],
                collection_type=r[1],
                collection_scope=r[2],
                namespace=r[3],
                source_key=r[4],
                canonical_name=r[5],
                membership_model=r[6],
                status=r[7],
                effective_from=r[8],
                effective_to=r[9],
                available_trade_date=r[10],
                source=r[11],
                source_record_id=r[12],
                revision_id=r[13],
                ingested_at=r[14],
            )
            for r in rows
        ]

    def get_theme_revisions(self, theme_id: str) -> list[ThemeRecord]:
        """Fetch all revisions for a theme."""
        rows = self.con.execute(
            f"""
            SELECT
                theme_id, collection_id, canonical_name,
                status, effective_from, effective_to, available_trade_date,
                source, source_record_id, revision_id, ingested_at
            FROM {THEME_TABLE}
            WHERE theme_id = ?
            ORDER BY available_trade_date ASC, ingested_at ASC
            """,
            [theme_id],
        ).fetchall()
        return [
            ThemeRecord(
                theme_id=r[0],
                collection_id=r[1],
                canonical_name=r[2],
                status=r[3],
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

    def check_is_equity(self, asset_id: str) -> bool:
        """Verify asset is a valid EQUITY in stock_info (if stock_info exists)."""
        # If stock_info exists in database, check it; otherwise treat as valid if properly formatted
        tables = [
            t[0]
            for t in self.con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        ]
        if "stock_info" in tables:
            row = self.con.execute(
                "SELECT COUNT(*) FROM stock_info WHERE ticker = ?", [asset_id]
            ).fetchone()
            return bool(row and row[0] > 0)
        # Fallback check on asset_id format (e.g. 600519.SH, 000001.SZ, 300750.SZ, 688981.SH, 830000.BJ)
        return isinstance(asset_id, str) and len(asset_id) >= 6 and (
            asset_id.endswith(".SH") or asset_id.endswith(".SZ") or asset_id.endswith(".BJ")
        )

    def append_membership_revisions(
        self,
        records: Sequence[ThemeMembershipRecord],
    ) -> None:
        """Atomically append a batch of membership revisions."""
        if not records:
            return
        try:
            self.con.execute("BEGIN TRANSACTION")
            for r in records:
                self.con.execute(
                    f"""
                    INSERT INTO {THEME_MEMBERSHIP_HISTORY_TABLE} (
                        membership_id, theme_id, collection_id, asset_id,
                        effective_from, effective_to, available_trade_date,
                        source, source_record_id, revision_id, ingested_at
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
            self.con.execute("COMMIT")
        except Exception as exc:
            try:
                self.con.execute("ROLLBACK")
            except Exception:
                pass
            raise StockCollectionError("MEMBERSHIP_APPEND_FAILED", str(exc)) from exc

    def get_membership_revisions(
        self,
        membership_id: str,
    ) -> list[ThemeMembershipRecord]:
        """Fetch all revisions for a single membership_id."""
        rows = self.con.execute(
            f"""
            SELECT
                membership_id, theme_id, collection_id, asset_id,
                effective_from, effective_to, available_trade_date, source,
                source_record_id, revision_id, ingested_at
            FROM {THEME_MEMBERSHIP_HISTORY_TABLE}
            WHERE membership_id = ?
            ORDER BY available_trade_date ASC, ingested_at ASC
            """,
            [membership_id],
        ).fetchall()
        return [
            ThemeMembershipRecord(
                membership_id=r[0],
                theme_id=r[1],
                collection_id=r[2],
                asset_id=r[3],
                weight=None,
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

    def get_asset_memberships(
        self,
        collection_id: str,
        asset_id: str | None = None,
        knowledge_date: date | None = None,
    ) -> list[ThemeMembershipRecord]:
        """Fetch latest visible revisions of all membership lifecycles for an asset (or all assets if asset_id is None)."""
        asset_filter = "AND asset_id = ?" if asset_id is not None else ""
        kd_filter = "AND available_trade_date <= ?" if knowledge_date is not None else ""
        params: list[Any] = [collection_id]
        if asset_id is not None:
            params.append(asset_id)
        if knowledge_date is not None:
            params.append(knowledge_date)

        sql = f"""
        WITH ranked AS (
            SELECT
                membership_id, theme_id, collection_id, asset_id,
                effective_from, effective_to, available_trade_date, source,
                source_record_id, revision_id, ingested_at,
                row_number() OVER (
                    PARTITION BY membership_id
                    ORDER BY available_trade_date DESC, ingested_at DESC
                ) as rn
            FROM {THEME_MEMBERSHIP_HISTORY_TABLE}
            WHERE collection_id = ? {asset_filter} {kd_filter}
        )
        SELECT
            membership_id, theme_id, collection_id, asset_id,
            effective_from, effective_to, available_trade_date, source,
            source_record_id, revision_id, ingested_at
        FROM ranked
        WHERE rn = 1
        ORDER BY effective_from ASC
        """
        rows = self.con.execute(sql, params).fetchall()
        return [
            ThemeMembershipRecord(
                membership_id=r[0],
                theme_id=r[1],
                collection_id=r[2],
                asset_id=r[3],
                weight=None,
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
