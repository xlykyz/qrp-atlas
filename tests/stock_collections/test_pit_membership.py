"""Tests for Theme Membership PIT lifecycle, late revisions, and dual-time query isolation."""

from __future__ import annotations

from datetime import date, datetime
import duckdb
import pytest

from qrp_atlas.contracts import init_stock_collections_database
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


def test_theme_membership_complete_pit_lifecycle_and_late_revision(memory_db):
    """Test full PIT lifecycle: Add -> Remove -> Late Revision -> Re-entry and dual-time isolation.

    Scenario:
    - 2026-08-01: Create Theme 'AI_CHIP' (effective 2026-08-01, available 2026-08-01).
    - 2026-08-10 (know on 2026-08-10): Add Stock '000001.SZ' effective 2026-08-10 -> NULL (MID_1, R1).
    - 2026-08-25 (know on 2026-08-25): Remove '000001.SZ' effective_to = 2026-08-25 (MID_1, R2).
    - 2026-09-01 (know on 2026-09-01): Late Revision discovered! Actually stopped belonging earlier on 2026-08-15 (MID_1, R3).
    - 2026-09-15 (know on 2026-09-15): Re-entry! '000001.SZ' rejoins theme effective 2026-09-15 -> NULL (MID_2, R1).
    """
    service = StockCollectionService(memory_db)
    resolver = StockCollectionResolver(memory_db)

    # 1. Create Theme & Collection (1:1 atomic)
    theme, coll = service.create_canonical_theme(
        theme_id="TH_AI_CHIP",
        canonical_name="AI芯片与算力",
        source_key="AI_CHIP",
        effective_from=date(2026, 8, 1),
        available_trade_date=date(2026, 8, 1),
    )
    cid = coll.collection_id
    assert cid == "COLL:THEME:QRP:AI_CHIP"

    # 2. Add member (on 2026-08-10)
    m1_r1 = service.add_member(
        theme_id="TH_AI_CHIP",
        collection_id=cid,
        asset_id="000001.SZ",
        effective_from=date(2026, 8, 10),
        available_trade_date=date(2026, 8, 10),
        membership_id="MEM_AI_000001",
    )
    assert m1_r1.revision_id == "MEM_AI_000001_R1"

    # Check query on 2026-08-12 as of knowledge_date 2026-08-10
    members = resolver.resolve_members(
        cid,
        as_of_date=date(2026, 8, 12),
        knowledge_date=date(2026, 8, 10),
    )
    assert len(members) == 1
    assert members[0].asset_id == "000001.SZ"

    # Check query before effective_from (2026-08-05) -> Not a member
    members_before = resolver.resolve_members(
        cid,
        as_of_date=date(2026, 8, 5),
        knowledge_date=date(2026, 8, 10),
    )
    assert len(members_before) == 0

    # 3. Remove member on 2026-08-25
    m1_r2 = service.remove_member(
        membership_id="MEM_AI_000001",
        theme_id="TH_AI_CHIP",
        collection_id=cid,
        asset_id="000001.SZ",
        effective_from=date(2026, 8, 10),
        removal_date=date(2026, 8, 25),
        available_trade_date=date(2026, 8, 25),
        revision_sequence=2,
    )
    assert m1_r2.revision_id == "MEM_AI_000001_R2"

    # On as_of_date 2026-08-20, as of knowledge_date 2026-08-25 -> Still a member
    members_20th = resolver.resolve_members(
        cid,
        as_of_date=date(2026, 8, 20),
        knowledge_date=date(2026, 8, 25),
    )
    assert len(members_20th) == 1

    # On as_of_date 2026-08-25 (right-open boundary), as of knowledge_date 2026-08-25 -> Excluded
    members_25th = resolver.resolve_members(
        cid,
        as_of_date=date(2026, 8, 25),
        knowledge_date=date(2026, 8, 25),
    )
    assert len(members_25th) == 0

    # 4. Late revision on 2026-09-01: actually ended earlier on 2026-08-15
    m1_r3 = service.revise_member_late(
        membership_id="MEM_AI_000001",
        theme_id="TH_AI_CHIP",
        collection_id=cid,
        asset_id="000001.SZ",
        effective_from=date(2026, 8, 10),
        effective_to=date(2026, 8, 15),
        knowledge_date=date(2026, 9, 1),
        revision_sequence=3,
    )
    assert m1_r3.revision_id == "MEM_AI_000001_R3"

    # CRUCIAL PIT DUAL-TIME TEST:
    # Query as_of_date = 2026-08-20:
    # (a) When knowledge_date = 2026-08-25 (at that time, we thought it ended on 2026-08-25):
    #     -> Was considered a member!
    hist_known_aug = resolver.resolve_members(
        cid,
        as_of_date=date(2026, 8, 20),
        knowledge_date=date(2026, 8, 25),
    )
    assert len(hist_known_aug) == 1

    # (b) When knowledge_date = 2026-09-02 (after late revision):
    #     -> Now recognized as NOT a member on 2026-08-20!
    hist_known_sep = resolver.resolve_members(
        cid,
        as_of_date=date(2026, 8, 20),
        knowledge_date=date(2026, 9, 2),
    )
    assert len(hist_known_sep) == 0

    # 5. Re-entry on 2026-09-15 with NEW membership_id
    m2_r1 = service.reenter_member(
        theme_id="TH_AI_CHIP",
        collection_id=cid,
        asset_id="000001.SZ",
        effective_from=date(2026, 9, 15),
        available_trade_date=date(2026, 9, 15),
    )
    assert m2_r1.membership_id != "MEM_AI_000001"

    # Query on 2026-09-16 @ knowledge_date 2026-09-16
    reentered = resolver.resolve_members(
        cid,
        as_of_date=date(2026, 9, 16),
        knowledge_date=date(2026, 9, 16),
    )
    assert len(reentered) == 1
    assert reentered[0].asset_id == "000001.SZ"


def test_theme_collection_identity_collision_defense(memory_db):
    service = StockCollectionService(memory_db)
    service.create_canonical_theme(
        theme_id="TH_1",
        canonical_name="测试题材1",
        source_key="SAMENAME",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )

    # Attempt to create collision with different type or namespace for same collection_id
    from qrp_atlas.stock_collections.models import StockCollectionRecord, ThemeRecord
    from qrp_atlas.contracts.stock_collection import CollectionType, CollectionScope, MembershipModel, CollectionStatus

    # Manual collision attempt
    coll_collide = StockCollectionRecord(
        collection_id="COLL:THEME:QRP:SAMENAME",
        collection_type=CollectionType.INDUSTRY,  # Mismatched type
        collection_scope=CollectionScope.CANONICAL,
        namespace="QRP",
        source_key="SAMENAME",
        canonical_name="冲突测试",
        membership_model=MembershipModel.INTERVAL,
        status=CollectionStatus.ACTIVE,
        effective_from=date(2026, 1, 1),
        effective_to=None,
        available_trade_date=date(2026, 1, 1),
        source="CANONICAL",
        source_record_id=None,
        revision_id="REV_COLLIDE",
        ingested_at=datetime.now(),
    )
    theme_collide = ThemeRecord(
        theme_id="TH_2",
        collection_id="COLL:THEME:QRP:SAMENAME",
        canonical_name="冲突测试",
        status=CollectionStatus.ACTIVE,
        effective_from=date(2026, 1, 1),
        effective_to=None,
        available_trade_date=date(2026, 1, 1),
        source="CANONICAL",
        source_record_id=None,
        revision_id="REV_COLLIDE",
        ingested_at=datetime.now(),
    )

    repo = service.repo
    with pytest.raises(StockCollectionError) as exc_info:
        repo.create_theme_collection_atomic(theme_collide, coll_collide)
    assert exc_info.value.code == StockCollectionErrorCode.COLLECTION_IDENTITY_COLLISION
