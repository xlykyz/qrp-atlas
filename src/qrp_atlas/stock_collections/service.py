"""Application service managing StockCollection and Theme domain lifecycles."""

from __future__ import annotations

from datetime import date, datetime
from typing import Sequence
import uuid

import duckdb

from qrp_atlas.contracts.stock_collection import (
    CollectionScope,
    CollectionStatus,
    CollectionType,
    MembershipModel,
)
from qrp_atlas.stock_collections.identity import make_collection_id
from qrp_atlas.stock_collections.models import (
    StockCollectionError,
    StockCollectionErrorCode,
    StockCollectionRecord,
    ThemeMembershipRecord,
    ThemeRecord,
)
from qrp_atlas.stock_collections.repository import StockCollectionRepository


class StockCollectionService:
    """Service orchestrating lifecycle mutations for themes and collections."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.con = connection
        self.repo = StockCollectionRepository(connection)

    def create_canonical_theme(
        self,
        *,
        theme_id: str,
        canonical_name: str,
        source_key: str,
        effective_from: date,
        available_trade_date: date,
        namespace: str = "QRP",
        source: str = "CANONICAL",
        source_record_id: str | None = None,
        effective_to: date | None = None,
        ingested_at: datetime | None = None,
    ) -> tuple[ThemeRecord, StockCollectionRecord]:
        """Atomically create a Theme and its corresponding StockCollection (1:1)."""
        collection_id = make_collection_id(CollectionType.THEME, namespace, source_key)
        now = ingested_at or datetime.now()
        rev_id = f"REV_{uuid.uuid4().hex[:12]}"

        collection_record = StockCollectionRecord(
            collection_id=collection_id,
            collection_type=CollectionType.THEME,
            collection_scope=CollectionScope.CANONICAL,
            namespace=namespace.upper(),
            source_key=source_key.upper(),
            canonical_name=canonical_name,
            membership_model=MembershipModel.INTERVAL,
            status=CollectionStatus.ACTIVE,
            effective_from=effective_from,
            effective_to=effective_to,
            available_trade_date=available_trade_date,
            source=source,
            source_record_id=source_record_id,
            revision_id=rev_id,
            ingested_at=now,
        )

        theme_record = ThemeRecord(
            theme_id=theme_id,
            collection_id=collection_id,
            canonical_name=canonical_name,
            status=CollectionStatus.ACTIVE,
            effective_from=effective_from,
            effective_to=effective_to,
            available_trade_date=available_trade_date,
            source=source,
            source_record_id=source_record_id,
            revision_id=rev_id,
            ingested_at=now,
        )

        self.repo.create_theme_collection_atomic(theme_record, collection_record)
        return theme_record, collection_record

    def add_member(
        self,
        *,
        theme_id: str,
        collection_id: str,
        asset_id: str,
        effective_from: date,
        available_trade_date: date,
        membership_id: str | None = None,
        effective_to: date | None = None,
        source: str = "CANONICAL",
        source_record_id: str | None = None,
        ingested_at: datetime | None = None,
    ) -> ThemeMembershipRecord:
        """Add a new logical member to a theme with initial revision."""
        mid = membership_id or f"MEM_{theme_id}_{asset_id}_{uuid.uuid4().hex[:8]}"
        now = ingested_at or datetime.now()
        rev_id = f"{mid}_R1"

        record = ThemeMembershipRecord(
            membership_id=mid,
            theme_id=theme_id,
            collection_id=collection_id,
            asset_id=asset_id,
            effective_from=effective_from,
            effective_to=effective_to,
            available_trade_date=available_trade_date,
            source=source,
            source_record_id=source_record_id,
            revision_id=rev_id,
            ingested_at=now,
        )
        self.repo.append_theme_membership_revisions([record])
        return record

    def remove_member(
        self,
        *,
        membership_id: str,
        theme_id: str,
        collection_id: str,
        asset_id: str,
        effective_from: date,
        removal_date: date,
        available_trade_date: date,
        source: str = "CANONICAL",
        source_record_id: str | None = None,
        revision_sequence: int = 2,
        ingested_at: datetime | None = None,
    ) -> ThemeMembershipRecord:
        """Remove a member by creating a new revision setting effective_to = removal_date (left-closed, right-open)."""
        if removal_date <= effective_from:
            raise StockCollectionError(
                StockCollectionErrorCode.COLLECTION_PIT_INVARIANT_VIOLATION,
                f"removal_date ({removal_date}) must be > effective_from ({effective_from})",
            )
        now = ingested_at or datetime.now()
        rev_id = f"{membership_id}_R{revision_sequence}"

        record = ThemeMembershipRecord(
            membership_id=membership_id,
            theme_id=theme_id,
            collection_id=collection_id,
            asset_id=asset_id,
            effective_from=effective_from,
            effective_to=removal_date,
            available_trade_date=available_trade_date,
            source=source,
            source_record_id=source_record_id,
            revision_id=rev_id,
            ingested_at=now,
        )
        self.repo.append_theme_membership_revisions([record])
        return record

    def revise_member_late(
        self,
        *,
        membership_id: str,
        theme_id: str,
        collection_id: str,
        asset_id: str,
        effective_from: date,
        effective_to: date | None,
        knowledge_date: date,
        source: str = "CANONICAL",
        source_record_id: str | None = None,
        revision_sequence: int = 3,
        ingested_at: datetime | None = None,
    ) -> ThemeMembershipRecord:
        """Apply a late revision adjusting effective interval as known on knowledge_date."""
        now = ingested_at or datetime.now()
        rev_id = f"{membership_id}_R{revision_sequence}"

        record = ThemeMembershipRecord(
            membership_id=membership_id,
            theme_id=theme_id,
            collection_id=collection_id,
            asset_id=asset_id,
            effective_from=effective_from,
            effective_to=effective_to,
            available_trade_date=knowledge_date,
            source=source,
            source_record_id=source_record_id,
            revision_id=rev_id,
            ingested_at=now,
        )
        self.repo.append_theme_membership_revisions([record])
        return record

    def reenter_member(
        self,
        *,
        theme_id: str,
        collection_id: str,
        asset_id: str,
        effective_from: date,
        available_trade_date: date,
        effective_to: date | None = None,
        source: str = "CANONICAL",
        source_record_id: str | None = None,
        ingested_at: datetime | None = None,
    ) -> ThemeMembershipRecord:
        """Re-enter an asset into a theme after prior departure, creating a fresh logical membership_id."""
        return self.add_member(
            theme_id=theme_id,
            collection_id=collection_id,
            asset_id=asset_id,
            effective_from=effective_from,
            available_trade_date=available_trade_date,
            effective_to=effective_to,
            source=source,
            source_record_id=source_record_id,
            ingested_at=ingested_at,
        )
