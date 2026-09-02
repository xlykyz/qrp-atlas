"""Unified StockCollection resolver executing PIT resolution, reverse lookups, and audit."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any

import duckdb
import pandas as pd

from qrp_atlas.contracts.stock_collection import CollectionScope, CollectionType

from .adapters.theme import ThemeAdapter
from .models import (
    MembershipExplanation,
    ResolvedMember,
    StockCollectionError,
    StockCollectionQueryContext,
    StockCollectionRecord,
)
from .repository import StockCollectionRepository


class StockCollectionResolver:
    """Entry point for StockCollection resolution, PIT verification, and explainability."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con
        self.repo = StockCollectionRepository(con)
        self.theme_adapter = ThemeAdapter(con)

    def resolve_collection(
        self,
        collection_id: str,
        context: StockCollectionQueryContext,
    ) -> StockCollectionRecord:
        """Resolve collection metadata as of (as_of_date, knowledge_date)."""
        revisions = self.repo.get_collection_revisions(collection_id)
        if not revisions:
            raise StockCollectionError(
                "COLLECTION_NOT_FOUND", f"Collection {collection_id} does not exist"
            )

        visible = [r for r in revisions if r.available_trade_date <= context.knowledge_date]
        if not visible:
            raise StockCollectionError(
                "COLLECTION_NOT_KNOWN_AT_KNOWLEDGE_DATE",
                f"Collection {collection_id} is not known as of knowledge_date {context.knowledge_date}",
            )

        latest = visible[-1]
        is_effective = latest.effective_from <= context.as_of_date and (
            latest.effective_to is None or context.as_of_date < latest.effective_to
        )
        if not is_effective:
            raise StockCollectionError(
                "COLLECTION_NOT_EFFECTIVE_AT_AS_OF_DATE",
                f"Collection {collection_id} is not effective as of {context.as_of_date} "
                f"(effective interval [{latest.effective_from}, {latest.effective_to}))",
            )

        if latest.collection_scope not in context.allowed_scopes:
            raise StockCollectionError(
                "SCOPE_NOT_ALLOWED",
                f"Collection scope {latest.collection_scope} not in allowed scopes {context.allowed_scopes}",
            )

        if latest.collection_type != CollectionType.THEME:
            raise StockCollectionError(
                "UNSUPPORTED_COLLECTION_TYPE",
                f"Collection type {latest.collection_type} is not supported in v1.1 Task 04-A",
            )

        return latest

    def resolve_members(
        self,
        collection_id: str,
        context: StockCollectionQueryContext,
    ) -> list[ResolvedMember]:
        """Resolve point-in-time members for a collection."""
        coll = self.resolve_collection(collection_id, context)
        if coll.collection_type == CollectionType.THEME:
            return self.theme_adapter.resolve_members(collection_id, context)
        raise StockCollectionError(
            "UNSUPPORTED_COLLECTION_TYPE", f"Unsupported type {coll.collection_type}"
        )

    def batch_resolve_members(
        self,
        collection_ids: Sequence[str],
        trade_dates: Sequence[date],
        knowledge_date: date,
        allowed_scopes: tuple[str, ...] = ("CANONICAL",),
    ) -> pd.DataFrame:
        """Batch set-based vectorized resolution across multiple collections and dates."""
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

        # Validate collections visibility at knowledge_date
        for coll_id in collection_ids:
            revisions = self.repo.get_collection_revisions(coll_id)
            if not revisions:
                raise StockCollectionError(
                    "COLLECTION_NOT_FOUND", f"Collection {coll_id} does not exist"
                )
            visible = [r for r in revisions if r.available_trade_date <= knowledge_date]
            if not visible:
                raise StockCollectionError(
                    "COLLECTION_NOT_KNOWN_AT_KNOWLEDGE_DATE",
                    f"Collection {coll_id} is not known as of knowledge_date {knowledge_date}",
                )
            latest = visible[-1]
            if latest.collection_scope not in allowed_scopes:
                raise StockCollectionError(
                    "SCOPE_NOT_ALLOWED",
                    f"Collection scope {latest.collection_scope} not in allowed scopes {allowed_scopes}",
                )
            if latest.collection_type != CollectionType.THEME:
                raise StockCollectionError(
                    "UNSUPPORTED_COLLECTION_TYPE",
                    f"Collection type {latest.collection_type} is not supported in v1.1 Task 04-A",
                )

        return self.theme_adapter.batch_resolve_members(
            collection_ids, trade_dates, knowledge_date
        )

    def resolve_asset_collections(
        self,
        asset_id: str,
        context: StockCollectionQueryContext,
        collection_types: tuple[str, ...] | None = None,
    ) -> list[ResolvedMember]:
        """Reverse lookup: find all collections containing asset_id as of PIT context."""
        types = collection_types or (CollectionType.THEME,)
        results: list[ResolvedMember] = []
        if CollectionType.THEME in types:
            theme_members = self.theme_adapter.reverse_lookup(asset_id, context)
            for m in theme_members:
                try:
                    self.resolve_collection(m.collection_id, context)
                    results.append(m)
                except StockCollectionError:
                    continue
        return results

    def explain_membership(
        self,
        collection_id: str,
        asset_id: str,
        context: StockCollectionQueryContext,
    ) -> MembershipExplanation:
        """Explain PIT membership lifecycle and validity."""
        self.resolve_collection(collection_id, context)
        return self.theme_adapter.explain_membership(collection_id, asset_id, context)
