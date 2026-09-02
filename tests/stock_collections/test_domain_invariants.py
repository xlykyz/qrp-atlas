"""Tests for StockCollection domain invariants, transaction atomicity, and lifecycles."""

from datetime import date
import duckdb
import pytest

from qrp_atlas.contracts.schema import init_stock_collections_database
from qrp_atlas.contracts.stock_collection import CollectionType
from qrp_atlas.stock_collections.models import StockCollectionError
from qrp_atlas.stock_collections.service import StockCollectionService


@pytest.fixture
def db():
    con = duckdb.connect(":memory:")
    init_stock_collections_database(con)
    # Create mock stock_info for equity checking
    con.execute(
        """
        CREATE TABLE stock_info (
            ticker VARCHAR PRIMARY KEY,
            name VARCHAR,
            list_date DATE,
            delist_date DATE
        )
        """
    )
    con.execute(
        """
        INSERT INTO stock_info VALUES
        ('000001.SZ', 'Ping An Bank', '2020-01-01', NULL),
        ('600519.SH', 'Kweichow Moutai', '2020-01-01', NULL),
        ('300750.SZ', 'CATL', '2020-01-01', NULL)
        """
    )
    yield con
    con.close()


def test_theme_collection_1_to_1_atomic_creation_and_rollback(db):
    service = StockCollectionService(db)

    # 1. Successful atomic creation
    thm, coll = service.create_canonical_theme(
        theme_name="AI算力",
        source_key="AI_COMPUTE",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )
    assert coll.collection_id == "COLL:THEME:QRP:AI_COMPUTE"
    assert thm.theme_id == "THM:QRP:AI_COMPUTE"

    # 2. Collision rejection
    with pytest.raises(StockCollectionError, match="COLLECTION_COLLISION"):
        service.create_canonical_theme(
            theme_name="AI算力重名",
            source_key="AI_COMPUTE",
            effective_from=date(2026, 1, 1),
            available_trade_date=date(2026, 1, 1),
        )


def test_membership_identity_immutability_and_lifecycle_invariants(db):
    service = StockCollectionService(db)
    thm, coll = service.create_canonical_theme(
        theme_name="新能源",
        source_key="NEW_ENERGY",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )

    # 1. Non-equity asset rejected
    with pytest.raises(StockCollectionError, match="NON_EQUITY_ASSET"):
        service.add_member(
            theme_id=thm.theme_id,
            collection_id=coll.collection_id,
            asset_id="INVALID_BOND_001",
            effective_from=date(2026, 1, 1),
            available_trade_date=date(2026, 1, 1),
        )

    # 2. Invalid interval rejected (effective_to <= effective_from)
    with pytest.raises(StockCollectionError, match="INVALID_EFFECTIVE_INTERVAL"):
        service.add_member(
            theme_id=thm.theme_id,
            collection_id=coll.collection_id,
            asset_id="000001.SZ",
            effective_from=date(2026, 6, 1),
            effective_to=date(2026, 5, 1),
            available_trade_date=date(2026, 6, 1),
        )

    # 3. Add member 1
    m1 = service.add_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="000001.SZ",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )
    assert m1.membership_id.startswith("MEM:THM:QRP:NEW_ENERGY:000001.SZ:")

    # 4. Overlapping lifecycle rejected
    with pytest.raises(StockCollectionError, match="OVERLAPPING_MEMBERSHIP_LIFECYCLE"):
        service.add_member(
            theme_id=thm.theme_id,
            collection_id=coll.collection_id,
            asset_id="000001.SZ",
            effective_from=date(2026, 3, 1),
            available_trade_date=date(2026, 3, 1),
        )

    # 5. Remove member (closes lifecycle cleanly)
    m1_rev2 = service.remove_member(
        membership_id=m1.membership_id,
        removal_date=date(2026, 4, 1),
        available_trade_date=date(2026, 4, 1),
    )
    # Immutable identity check
    assert m1_rev2.membership_id == m1.membership_id
    assert m1_rev2.theme_id == m1.theme_id
    assert m1_rev2.collection_id == m1.collection_id
    assert m1_rev2.asset_id == m1.asset_id
    assert m1_rev2.effective_from == m1.effective_from
    assert m1_rev2.effective_to == date(2026, 4, 1)

    # 6. Re-entry after old lifecycle ended creates NEW membership_id
    m2 = service.reenter_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="000001.SZ",
        effective_from=date(2026, 5, 1),
        available_trade_date=date(2026, 5, 1),
    )
    assert m2.membership_id != m1.membership_id
    assert m2.asset_id == "000001.SZ"
    assert m2.effective_from == date(2026, 5, 1)
