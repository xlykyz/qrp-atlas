"""Tests for StockCollection Resolver, reverse lookup, and explainability."""

from __future__ import annotations

from datetime import date
import duckdb
import pytest

from qrp_atlas.contracts import init_stock_collections_database
from qrp_atlas.contracts.stock_collection import CollectionScope, CollectionType
from qrp_atlas.stock_collections.models import (
    StockCollectionError,
    StockCollectionErrorCode,
    StockCollectionQueryContext,
)
from qrp_atlas.stock_collections.resolver import StockCollectionResolver
from qrp_atlas.stock_collections.service import StockCollectionService


@pytest.fixture
def memory_db():
    con = duckdb.connect(":memory:")
    init_stock_collections_database(con)
    yield con
    con.close()


def test_resolver_collection_not_found_and_availability(memory_db):
    resolver = StockCollectionResolver(memory_db)
    ctx = StockCollectionQueryContext(
        as_of_date=date(2026, 8, 10),
        knowledge_date=date(2026, 8, 10),
    )

    with pytest.raises(StockCollectionError) as exc_info:
        resolver.resolve_collection("COLL:THEME:QRP:NON_EXISTENT", ctx)
    assert exc_info.value.code == StockCollectionErrorCode.COLLECTION_NOT_FOUND

    # Create collection available starting 2026-08-15
    service = StockCollectionService(memory_db)
    _, coll = service.create_canonical_theme(
        theme_id="TH_FUTURE",
        canonical_name="未来题材",
        source_key="FUTURE",
        effective_from=date(2026, 8, 15),
        available_trade_date=date(2026, 8, 15),
    )

    # Querying as of 2026-08-10 with knowledge_date 2026-08-15 -> not available as of 2026-08-10
    ctx_early = StockCollectionQueryContext(
        as_of_date=date(2026, 8, 10),
        knowledge_date=date(2026, 8, 15),
    )
    with pytest.raises(StockCollectionError) as exc_early:
        resolver.resolve_collection(coll.collection_id, ctx_early)
    assert exc_early.value.code == StockCollectionErrorCode.COLLECTION_NOT_AVAILABLE_AS_OF


def test_resolver_reverse_lookup_and_explainability(memory_db):
    service = StockCollectionService(memory_db)
    resolver = StockCollectionResolver(memory_db)

    # Create two themes
    _, coll1 = service.create_canonical_theme(
        theme_id="TH_ROBOT",
        canonical_name="人形机器人",
        source_key="ROBOT",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )
    _, coll2 = service.create_canonical_theme(
        theme_id="TH_MOTOR",
        canonical_name="微特电机",
        source_key="MOTOR",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )

    # Add Stock '600001.SH' to both themes
    service.add_member(
        theme_id="TH_ROBOT",
        collection_id=coll1.collection_id,
        asset_id="600001.SH",
        effective_from=date(2026, 5, 1),
        available_trade_date=date(2026, 5, 1),
    )
    service.add_member(
        theme_id="TH_MOTOR",
        collection_id=coll2.collection_id,
        asset_id="600001.SH",
        effective_from=date(2026, 6, 1),
        available_trade_date=date(2026, 6, 1),
    )

    # Reverse lookup on 2026-05-15 (only belongs to ROBOT)
    ctx_may = StockCollectionQueryContext(
        as_of_date=date(2026, 5, 15),
        knowledge_date=date(2026, 6, 15),
    )
    cids_may = resolver.resolve_asset_collections("600001.SH", ctx_may)
    assert list(cids_may) == [coll1.collection_id]

    # Reverse lookup on 2026-06-15 (belongs to both)
    ctx_jun = StockCollectionQueryContext(
        as_of_date=date(2026, 6, 15),
        knowledge_date=date(2026, 6, 15),
    )
    cids_jun = resolver.resolve_asset_collections("600001.SH", ctx_jun)
    assert sorted(cids_jun) == sorted([coll1.collection_id, coll2.collection_id])

    # Explainability test
    exp = resolver.explain_membership(coll1.collection_id, "600001.SH", ctx_jun)
    assert exp.is_member is True
    assert "VALID_POINT_IN_TIME_MEMBER" in exp.reasons

    exp_non = resolver.explain_membership(coll1.collection_id, "999999.SH", ctx_jun)
    assert exp_non.is_member is False
    assert "NO_VISIBLE_MEMBERSHIP_REVISION" in exp_non.reasons
