"""Tests for Theme Membership PIT lifecycle, late revisions, and dual-time query isolation."""

from __future__ import annotations

from datetime import date
import duckdb
import pytest

from qrp_atlas.contracts import init_stock_collections_database
from qrp_atlas.stock_collections.models import (
    StockCollectionError,
    StockCollectionQueryContext,
)
from qrp_atlas.stock_collections.resolver import StockCollectionResolver
from qrp_atlas.stock_collections.service import StockCollectionService


@pytest.fixture
def memory_db():
    con = duckdb.connect(":memory:")
    init_stock_collections_database(con)
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
    con.execute("INSERT INTO stock_info VALUES ('000001.SZ', 'Ping An', '2020-01-01', NULL)")
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
        theme_name="AI芯片与算力",
        source_key="AI_CHIP",
        effective_from=date(2026, 8, 1),
        available_trade_date=date(2026, 8, 1),
    )
    cid = coll.collection_id
    assert cid == "COLL:THEME:QRP:AI_CHIP"

    # 2. Add member (on 2026-08-10)
    m1_r1 = service.add_member(
        theme_id=theme.theme_id,
        collection_id=cid,
        asset_id="000001.SZ",
        effective_from=date(2026, 8, 10),
        available_trade_date=date(2026, 8, 10),
    )
    mid_1 = m1_r1.membership_id

    # 3. Remove member (on 2026-08-25)
    m1_r2 = service.remove_member(
        membership_id=mid_1,
        removal_date=date(2026, 8, 25),
        available_trade_date=date(2026, 8, 25),
    )
    assert m1_r2.effective_to == date(2026, 8, 25)

    # 4. Late Revision (on 2026-09-01): discovers removal was actually 2026-08-15
    m1_r3 = service.revise_member_late(
        membership_id=mid_1,
        effective_from=date(2026, 8, 10),
        effective_to=date(2026, 8, 15),
        available_trade_date=date(2026, 9, 1),
    )
    assert m1_r3.effective_to == date(2026, 8, 15)

    # 5. Re-entry (on 2026-09-15)
    m2 = service.reenter_member(
        theme_id=theme.theme_id,
        collection_id=cid,
        asset_id="000001.SZ",
        effective_from=date(2026, 9, 15),
        available_trade_date=date(2026, 9, 15),
    )
    assert m2.membership_id != mid_1

    # ── Point-in-Time Dual Time Query Tests ──

    # Query 1: As of 2026-08-20, Knowledge as of 2026-08-20 (Before removal)
    # -> Should be a member (effective_to was NULL in R1)
    res_1 = resolver.resolve_members(
        cid, StockCollectionQueryContext(as_of_date=date(2026, 8, 20), knowledge_date=date(2026, 8, 20))
    )
    assert len(res_1) == 1

    # Query 2: As of 2026-08-20, Knowledge as of 2026-08-26 (After removal on 25th)
    # -> Still a member on 20th because effective_to was 25th in R2!
    res_2 = resolver.resolve_members(
        cid, StockCollectionQueryContext(as_of_date=date(2026, 8, 20), knowledge_date=date(2026, 8, 26))
    )
    assert len(res_2) == 1

    # Query 3: As of 2026-08-20, Knowledge as of 2026-09-02 (After Late Revision discovered removal was 15th)
    # -> NOT a member on 20th because effective_to is now 15th!
    res_3 = resolver.resolve_members(
        cid, StockCollectionQueryContext(as_of_date=date(2026, 8, 20), knowledge_date=date(2026, 9, 2))
    )
    assert len(res_3) == 0

    # Query 4: As of 2026-09-16, Knowledge as of 2026-09-16 (After re-entry)
    # -> Member of new lifecycle!
    res_4 = resolver.resolve_members(
        cid, StockCollectionQueryContext(as_of_date=date(2026, 9, 16), knowledge_date=date(2026, 9, 16))
    )
    assert len(res_4) == 1
    assert res_4[0].membership_id == m2.membership_id
