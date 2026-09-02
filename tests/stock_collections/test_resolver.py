from datetime import date
import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts.schema import init_stock_collections_database
from qrp_atlas.stock_collections.models import (
    StockCollectionError,
    StockCollectionQueryContext,
)
from qrp_atlas.stock_collections.resolver import StockCollectionResolver
from qrp_atlas.stock_collections.service import StockCollectionService


@pytest.fixture
def db():
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


def test_resolver_dual_time_and_collection_availability_states(db):
    service = StockCollectionService(db)
    resolver = StockCollectionResolver(db)

    # Create collection available as of 2026-03-01
    thm, coll = service.create_canonical_theme(
        theme_name="半导体",
        source_key="SEMICONDUCTOR",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 3, 1),
    )

    # 1. Unknown collection -> COLLECTION_NOT_FOUND
    with pytest.raises(StockCollectionError, match="COLLECTION_NOT_FOUND"):
        resolver.resolve_collection(
            "COLL:THEME:QRP:NON_EXISTENT",
            StockCollectionQueryContext(as_of_date=date(2026, 3, 1), knowledge_date=date(2026, 3, 1)),
        )

    # 2. Knowledge Date before available_trade_date -> COLLECTION_NOT_KNOWN_AT_KNOWLEDGE_DATE
    with pytest.raises(StockCollectionError, match="COLLECTION_NOT_KNOWN_AT_KNOWLEDGE_DATE"):
        resolver.resolve_collection(
            coll.collection_id,
            StockCollectionQueryContext(as_of_date=date(2026, 1, 15), knowledge_date=date(2026, 2, 28)),
        )

    # 3. As-of Date before effective_from -> COLLECTION_NOT_EFFECTIVE_AT_AS_OF_DATE
    with pytest.raises(StockCollectionError, match="COLLECTION_NOT_EFFECTIVE_AT_AS_OF_DATE"):
        resolver.resolve_collection(
            coll.collection_id,
            StockCollectionQueryContext(as_of_date=date(2025, 12, 31), knowledge_date=date(2026, 3, 1)),
        )

    # 4. Late-known historical collection:
    # Knowledge date is 2026-03-01 (visible), querying historical as_of_date 2026-01-15 (business valid)
    coll_resolved = resolver.resolve_collection(
        coll.collection_id,
        StockCollectionQueryContext(as_of_date=date(2026, 1, 15), knowledge_date=date(2026, 3, 1)),
    )
    assert coll_resolved.collection_id == coll.collection_id

    # 5. Collection exists but has empty membership
    members = resolver.resolve_members(
        coll.collection_id,
        StockCollectionQueryContext(as_of_date=date(2026, 1, 15), knowledge_date=date(2026, 3, 1)),
    )
    assert members == []


def test_resolver_explain_reentry_and_equivalence(db):
    service = StockCollectionService(db)
    resolver = StockCollectionResolver(db)

    thm, coll = service.create_canonical_theme(
        theme_name="机器人",
        source_key="ROBOTICS",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )

    # Lifecycle 1: [2026-01-01, 2026-03-01), available on 2026-01-01
    m1 = service.add_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="000001.SZ",
        effective_from=date(2026, 1, 1),
        available_trade_date=date(2026, 1, 1),
    )
    service.remove_member(
        membership_id=m1.membership_id,
        removal_date=date(2026, 3, 1),
        available_trade_date=date(2026, 3, 1),
    )

    # Lifecycle 2 (Re-entry): [2026-05-01, None), available on 2026-05-01
    m2 = service.reenter_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="000001.SZ",
        effective_from=date(2026, 5, 1),
        available_trade_date=date(2026, 5, 1),
    )

    # Now knowledge_date is 2026-06-01 (both lifecycles known)
    kd = date(2026, 6, 1)

    # Test Case A: as_of_date = 2026-02-01 (inside Lifecycle 1)
    ctx_a = StockCollectionQueryContext(as_of_date=date(2026, 2, 1), knowledge_date=kd)
    members_a = resolver.resolve_members(coll.collection_id, ctx_a)
    exp_a = resolver.explain_membership(coll.collection_id, "000001.SZ", ctx_a)

    assert "000001.SZ" in [m.asset_id for m in members_a]
    assert exp_a.is_member is True
    assert exp_a.membership_id == m1.membership_id
    assert len(exp_a.lifecycle_history) >= 2

    # Test Case B: as_of_date = 2026-04-01 (in between lifecycles)
    ctx_b = StockCollectionQueryContext(as_of_date=date(2026, 4, 1), knowledge_date=kd)
    members_b = resolver.resolve_members(coll.collection_id, ctx_b)
    exp_b = resolver.explain_membership(coll.collection_id, "000001.SZ", ctx_b)

    assert "000001.SZ" not in [m.asset_id for m in members_b]
    assert exp_b.is_member is False
    assert exp_b.reason == "OUTSIDE_EFFECTIVE_INTERVAL"

    # Test Case C: as_of_date = 2026-05-15 (inside Lifecycle 2)
    ctx_c = StockCollectionQueryContext(as_of_date=date(2026, 5, 15), knowledge_date=kd)
    members_c = resolver.resolve_members(coll.collection_id, ctx_c)
    exp_c = resolver.explain_membership(coll.collection_id, "000001.SZ", ctx_c)

    assert "000001.SZ" in [m.asset_id for m in members_c]
    assert exp_c.is_member is True
    assert exp_c.membership_id == m2.membership_id

    # Test Case D: batch resolution across all dates
    batch_df = resolver.batch_resolve_members(
        [coll.collection_id],
        [date(2026, 2, 1), date(2026, 4, 1), date(2026, 5, 15)],
        knowledge_date=kd,
    )
    assert len(batch_df) == 2  # dates 2026-02-01 and 2026-05-15
    assert set(pd.to_datetime(batch_df["trade_date"]).dt.date.tolist()) == {date(2026, 2, 1), date(2026, 5, 15)}


def test_batch_resolver_collection_effective_interval(db):
    """验证 Batch Resolver 严格执行 Collection Effective-Time ∩ Membership Effective-Time：
    集合闭合后，即使成员原本有效，也不会解析出来；集合未生效前同样不解析。
    """
    service = StockCollectionService(db)
    resolver = StockCollectionResolver(db)

    thm, coll = service.create_canonical_theme(
        theme_name="周期主题",
        source_key="CYCLE_THEME",
        effective_from=date(2026, 2, 1),
        available_trade_date=date(2026, 1, 1),
    )
    # 给集合设置 effective_to = 2026-04-01
    db.execute(
        "UPDATE stock_collection SET effective_to = ? WHERE collection_id = ?",
        [date(2026, 4, 1), coll.collection_id],
    )
    db.execute(
        "UPDATE theme SET effective_to = ? WHERE theme_id = ?",
        [date(2026, 4, 1), thm.theme_id],
    )

    # 添加成员，effective_from 2026-02-01, effective_to 2026-04-01
    service.add_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="000001.SZ",
        effective_from=date(2026, 2, 1),
        effective_to=date(2026, 4, 1),
        available_trade_date=date(2026, 1, 1),
    )

    # 批量解析日期跨越：未生效前 (01-15)、生效中 (02-15)、失效后 (04-15)
    trade_dates = [date(2026, 1, 15), date(2026, 2, 15), date(2026, 4, 15)]
    kd = date(2026, 5, 1)

    batch_df = resolver.batch_resolve_members([coll.collection_id], trade_dates, knowledge_date=kd)
    # 只有 2026-02-15 满足 Collection Effective ∩ Membership Effective
    assert len(batch_df) == 1
    assert pd.to_datetime(batch_df.iloc[0]["trade_date"]).date() == date(2026, 2, 15)
    assert batch_df.iloc[0]["asset_id"] == "000001.SZ"
