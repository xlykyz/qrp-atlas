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


def test_orphan_and_mismatch_forbidden(db):
    service = StockCollectionService(db)
    thm_a, coll_a = service.create_canonical_theme(
        theme_name="题材A",
        source_key="THEME_A",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )
    thm_b, coll_b = service.create_canonical_theme(
        theme_name="题材B",
        source_key="THEME_B",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )

    # 1. Orphan membership: non-existent collection
    with pytest.raises(StockCollectionError, match="COLLECTION_NOT_FOUND"):
        service.add_member(
            theme_id=thm_a.theme_id,
            collection_id="COLL:THEME:QRP:NON_EXISTENT",
            asset_id="000001.SZ",
            effective_from=date(2026, 1, 1),
            available_trade_date=date(2026, 1, 1),
        )

    # 2. Orphan membership: non-existent theme
    with pytest.raises(StockCollectionError, match="THEME_NOT_FOUND"):
        service.add_member(
            theme_id="THM:QRP:NON_EXISTENT",
            collection_id=coll_a.collection_id,
            asset_id="000001.SZ",
            effective_from=date(2026, 1, 1),
            available_trade_date=date(2026, 1, 1),
        )

    # 3. Theme/Collection mismatch forbidden
    with pytest.raises(StockCollectionError, match="THEME_COLLECTION_MISMATCH"):
        service.add_member(
            theme_id=thm_a.theme_id,
            collection_id=coll_b.collection_id,
            asset_id="000001.SZ",
            effective_from=date(2026, 1, 1),
            available_trade_date=date(2026, 1, 1),
        )


def test_membership_effective_from_revision(db):
    service = StockCollectionService(db)
    thm, coll = service.create_canonical_theme(
        theme_name="光伏",
        source_key="SOLAR",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )
    m = service.add_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="300750.SZ",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )

    # Attempting to revise effective_from outside collection bounds must raise error
    with pytest.raises(StockCollectionError, match="MEMBERSHIP_OUTSIDE_COLLECTION_LIFECYCLE"):
        service.revise_member_late(
            membership_id=m.membership_id,
            effective_from=date(2025, 12, 1),
            effective_to=None,
            available_trade_date=date(2026, 2, 1),
        )

    # Valid revision of effective_from succeeds and preserves membership_id identity
    rev = service.revise_member_late(
        membership_id=m.membership_id,
        effective_from=date(2026, 1, 15),
        effective_to=date(2026, 6, 1),
        available_trade_date=date(2026, 2, 1),
    )
    assert rev.membership_id == m.membership_id
    assert rev.revision_id != m.revision_id
    assert rev.effective_from == date(2026, 1, 15)
    assert rev.effective_to == date(2026, 6, 1)


def test_reentry_requires_previous_lifecycle_closed(db):
    service = StockCollectionService(db)
    thm, coll = service.create_canonical_theme(
        theme_name="储能",
        source_key="ENERGY_STORAGE",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )
    # Open member (effective_to is None)
    service.add_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="300750.SZ",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )

    # Re-entry must fail because previous lifecycle is open
    with pytest.raises(StockCollectionError, match="PREVIOUS_LIFECYCLE_NOT_CLOSED"):
        service.reenter_member(
            theme_id=thm.theme_id,
            collection_id=coll.collection_id,
            asset_id="300750.SZ",
            effective_from=date(2026, 3, 1),
            available_trade_date=date(2026, 3, 1),
        )


def test_batch_append_atomic_and_rollback(db):
    service = StockCollectionService(db)
    thm, coll = service.create_canonical_theme(
        theme_name="低空经济",
        source_key="LOW_ALTITUDE",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )

    # Batch with 1 invalid equity -> entire batch fails
    entries = [
        {"asset_id": "000001.SZ", "effective_from": date(2026, 1, 1)},
        {"asset_id": "INVALID_ASSET_X", "effective_from": date(2026, 1, 1)},
        {"asset_id": "300750.SZ", "effective_from": date(2026, 1, 1)},
    ]
    with pytest.raises(StockCollectionError, match="NON_EQUITY_ASSET"):
        service.add_members_batch(
            theme_id=thm.theme_id,
            collection_id=coll.collection_id,
            member_entries=entries,
            available_trade_date=date(2026, 1, 1),
        )

    # Verify 0 records were inserted (atomic rollback)
    members = service.repo.get_asset_memberships(coll.collection_id)
    assert len(members) == 0

    # Valid batch -> all inserted atomically
    valid_entries = [
        {"asset_id": "000001.SZ", "effective_from": date(2026, 1, 1)},
        {"asset_id": "600519.SH", "effective_from": date(2026, 1, 1)},
        {"asset_id": "300750.SZ", "effective_from": date(2026, 1, 1)},
    ]
    records = service.add_members_batch(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        member_entries=valid_entries,
        available_trade_date=date(2026, 1, 1),
    )
    assert len(records) == 3
    all_inserted = service.repo.get_asset_memberships(coll.collection_id)
    assert len(all_inserted) == 3


def test_theme_collection_membership_effective_interval_invariants(db):
    """验证 Theme / Collection 1:1 有效区间一致性及 Membership lifecycle 必须落入集合区间：
    1. member.effective_from < collection.effective_from -> MEMBERSHIP_OUTSIDE_COLLECTION_LIFECYCLE
    2. collection 已闭合但 member open-ended -> MEMBERSHIP_OUTSIDE_COLLECTION_LIFECYCLE
    3. member.effective_to > collection.effective_to -> MEMBERSHIP_OUTSIDE_COLLECTION_LIFECYCLE
    4. Theme 与 Collection 区间不匹配 -> PIT_INVARIANT_VIOLATION
    """
    service = StockCollectionService(db)
    thm, coll = service.create_canonical_theme(
        theme_name="人形机器人",
        source_key="HUMANOID_ROBOT",
        effective_from=date(2026, 2, 1),
        available_trade_date=date(2026, 1, 1),
    )

    # 1. 尝试添加早于 collection effective_from 的成员 -> fail
    with pytest.raises(StockCollectionError, match="MEMBERSHIP_OUTSIDE_COLLECTION_LIFECYCLE"):
        service.add_member(
            theme_id=thm.theme_id,
            collection_id=coll.collection_id,
            asset_id="000001.SZ",
            effective_from=date(2026, 1, 15),  # 01-15 < 02-01
            available_trade_date=date(2026, 1, 1),
        )

    # 2. 为集合和主题设定闭合时间 effective_to = 2026-06-01
    db.execute(
        "UPDATE stock_collection SET effective_to = ? WHERE collection_id = ?",
        [date(2026, 6, 1), coll.collection_id],
    )
    db.execute(
        "UPDATE theme SET effective_to = ? WHERE theme_id = ?",
        [date(2026, 6, 1), thm.theme_id],
    )

    # 3. 闭合集合不允许添加 open-ended 成员
    with pytest.raises(StockCollectionError, match="MEMBERSHIP_OUTSIDE_COLLECTION_LIFECYCLE"):
        service.add_member(
            theme_id=thm.theme_id,
            collection_id=coll.collection_id,
            asset_id="000001.SZ",
            effective_from=date(2026, 3, 1),
            effective_to=None,  # open-ended in closed collection!
            available_trade_date=date(2026, 1, 1),
        )

    # 4. member effective_to 超过 collection effective_to -> fail
    with pytest.raises(StockCollectionError, match="MEMBERSHIP_OUTSIDE_COLLECTION_LIFECYCLE"):
        service.add_member(
            theme_id=thm.theme_id,
            collection_id=coll.collection_id,
            asset_id="000001.SZ",
            effective_from=date(2026, 3, 1),
            effective_to=date(2026, 7, 1),  # 07-01 > 06-01
            available_trade_date=date(2026, 1, 1),
        )

    # 5. Theme 与 Collection 区间不匹配 -> PIT_INVARIANT_VIOLATION
    db.execute(
        "UPDATE theme SET effective_to = ? WHERE theme_id = ?",
        [date(2026, 8, 1), thm.theme_id],
    )
    with pytest.raises(StockCollectionError, match="PIT_INVARIANT_VIOLATION"):
        service.add_member(
            theme_id=thm.theme_id,
            collection_id=coll.collection_id,
            asset_id="000001.SZ",
            effective_from=date(2026, 3, 1),
            effective_to=date(2026, 5, 1),
            available_trade_date=date(2026, 1, 1),
        )
