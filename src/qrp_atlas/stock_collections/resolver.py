"""StockCollection Resolver orchestrating adapters and enforcing PIT access controls."""

from __future__ import annotations

from datetime import date
from typing import Sequence

import duckdb

from qrp_atlas.contracts.stock_collection import (
    CollectionScope,
    CollectionType,
)
from qrp_atlas.stock_collections.adapters.theme import ThemeAdapter
from qrp_atlas.stock_collections.models import (
    CollectionVersionContext,
    MembershipExplanation,
    ResolvedMember,
    StockCollectionError,
    StockCollectionErrorCode,
    StockCollectionQueryContext,
    StockCollectionRecord,
)
from qrp_atlas.stock_collections.repository import StockCollectionRepository


class StockCollectionResolver:
    """Unified resolver for resolving stock collection identities and PIT memberships."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.con = connection
        self.repo = StockCollectionRepository(connection)
        self.theme_adapter = ThemeAdapter(connection)

    def resolve_collection(
        self,
        collection_id: str,
        context: StockCollectionQueryContext,
    ) -> StockCollectionRecord:
        """Resolve and validate the StockCollection record as of context.knowledge_date."""
        record = self.repo.get_collection_record(collection_id, context.knowledge_date)
        if record is None:
            raise StockCollectionError(
                StockCollectionErrorCode.COLLECTION_NOT_FOUND,
                f"Collection '{collection_id}' not found or not visible as of knowledge_date {context.knowledge_date}",
            )

        # Check scope permissions
        if record.collection_scope not in context.allowed_scopes:
            raise StockCollectionError(
                StockCollectionErrorCode.COLLECTION_SCOPE_NOT_ALLOWED,
                f"Collection scope '{record.collection_scope}' is not in allowed scopes {context.allowed_scopes}",
            )

        # Check collection availability vs as_of_date
        if record.available_trade_date > context.as_of_date:
            raise StockCollectionError(
                StockCollectionErrorCode.COLLECTION_NOT_AVAILABLE_AS_OF,
                f"Collection '{collection_id}' is only available starting {record.available_trade_date}, requested as_of_date is {context.as_of_date}",
            )

        return record

    def resolve_members(
        self,
        collection_id: str,
        as_of_date: date,
        knowledge_date: date,
        version_context: CollectionVersionContext | None = None,
        allowed_scopes: tuple[CollectionScope | str, ...] = (CollectionScope.CANONICAL,),
    ) -> Sequence[ResolvedMember]:
        """Resolve valid PIT members of a collection."""
        ctx = StockCollectionQueryContext(
            as_of_date=as_of_date,
            knowledge_date=knowledge_date,
            version_context=version_context or CollectionVersionContext(),
            allowed_scopes=allowed_scopes,
        )

        collection_record = self.resolve_collection(collection_id, ctx)

        if collection_record.collection_type == CollectionType.THEME:
            return self.theme_adapter.resolve_members(collection_id, ctx)
        else:
            raise StockCollectionError(
                StockCollectionErrorCode.COLLECTION_ADAPTER_NOT_FOUND,
                f"Collection type '{collection_record.collection_type}' is not supported in v1.1 Task 04-A",
            )

    def resolve_asset_collections(
        self,
        asset_id: str,
        context: StockCollectionQueryContext,
        collection_types: tuple[CollectionType | str, ...] | None = None,
    ) -> Sequence[str]:
        """Reverse-lookup: find all collections an asset belongs to as of as_of_date @ knowledge_date."""
        types = collection_types or (CollectionType.THEME,)
        results: list[str] = []

        if CollectionType.THEME in types:
            theme_collections = self.theme_adapter.resolve_asset_collections(asset_id, context)
            for cid in theme_collections:
                try:
                    # Filter through scope and availability verification
                    self.resolve_collection(cid, context)
                    results.append(cid)
                except StockCollectionError:
                    pass

        return tuple(results)

    def explain_membership(
        self,
        collection_id: str,
        asset_id: str,
        context: StockCollectionQueryContext,
    ) -> MembershipExplanation:
        """Explain why an asset is or is not an active PIT member."""
        collection_record = self.resolve_collection(collection_id, context)
        if collection_record.collection_type == CollectionType.THEME:
            return self.theme_adapter.explain_membership(collection_id, asset_id, context)
        else:
            raise StockCollectionError(
                StockCollectionErrorCode.COLLECTION_ADAPTER_NOT_FOUND,
                f"Collection type '{collection_record.collection_type}' is not supported in v1.1 Task 04-A",
            )
