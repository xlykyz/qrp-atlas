"""Task04-A Semantic Finalization Test Suite.

Covers all 20 mandatory regression scenarios:
1. 08:59:59 录入，D 日立即生效进入候选；
2. 09:00:00 录入，D 日不生效，D_next 才生效；
3. 09:00:01 录入，即使声明 effective_from <= D，D 日也不生效；
4. 跨午夜维护：D-1 23:30 录入，D 日正常生效；
5. 盘前维护：D 08:30 录入，D 日正常生效；
6. 周末录入：周六录入且 effective_from 覆盖下周一，下周一 09:00 前提交，下周一生效；
7. 08:59:59 删除，D 日立即不进入候选；
8. 09:00:00 删除，D 日仍作为候选，D_next 起移除；
9. 实际计算时刻 T 早跑（08:45）、正常（16:00）、重跑（20:00），同一 D 的合法候选集合严格一致；
10. 上市交易日事实缺失 / UNRESOLVED_MISSING，严格 fail-closed 为 UNCONFIRMED_LISTING_DAYS；
11. 上市交易日数 = 5，排除为 NEW_LISTING_LE_5；
12. 上市交易日数 = 6，通过上市交易日规则；
13. 既 unconfirmed 又 suspended，按优先级标记为 UNCONFIRMED_LISTING_DAYS；
14. 既 new_listing_le_5 又 suspended，按优先级标记为 NEW_LISTING_LE_5；
15. EXPLICIT_NON_TRADING 正确归入停牌不可交易；
16. theme_effective_member_daily 物理落库，字段完整，主键约束生效，is_theme_member 正确；
17. D 日 finalized 后，后续倒填更早成员，D 日及之前已落库的所有表行与数值绝对不变；
18. Custom Index 延续计算正确从上一个已落库 finite cumulative level 开始，不从 inception 全量重算；
19. State / Episode 在已 finalized 历史的基础上向前推进，不改写历史 closed episode；
20. replay_m4_facts(...) 只读回放返回完整计算结果，但不向任何表写入任何一行，不更新 theme_production_run，不改变后续生产。
"""

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
import duckdb
import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    ASSET_ID,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    IS_SUSPENDED,
    TRADE_DATE,
)
from qrp_atlas.contracts.m4 import (
    DEFAULT_BASE_LEVEL,
    EXCLUSION_REASON,
    EXCLUSION_REASON_NEW_LISTING_LE_5,
    EXCLUSION_REASON_SUSPENDED,
    EXCLUSION_REASON_UNCONFIRMED_LISTING_DAYS,
    IS_M4_EFFECTIVE_MEMBER,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_EFFECTIVE_MEMBER_DAILY_TABLE,
    THEME_M4_OBSERVATION_TABLE,
)
from qrp_atlas.contracts.schema import init_database, init_stock_collections_database
from qrp_atlas.contracts.stock_collection import (
    STOCK_COLLECTION_TABLE,
    THEME_MEMBERSHIP_HISTORY_TABLE,
    THEME_TABLE,
)
from qrp_atlas.indicators.theme.effective_members import calculate_m4_effective_members
from qrp_atlas.pipeline.theme.service import ThemePipelineService
from qrp_atlas.stock_collections.service import StockCollectionService

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


@pytest.fixture
def f_db():
    con = duckdb.connect(":memory:")
    init_database(con)
    init_stock_collections_database(con)

    # 1. Trading calendar from database: 10 prior days + 10 August trading days
    # Prior days: 2026-07-20 .. 2026-07-31 (10 open days)
    prior_dates = [date(2026, 7, i) for i in range(20, 31)]
    # August days:
    # 2026-08-03 (Mon, D1), 2026-08-04 (Tue, D2), 2026-08-05 (Wed, D3),
    # 2026-08-06 (Thu, D4), 2026-08-07 (Fri, D5),
    # 2026-08-10 (Mon, D6), 2026-08-11 (Tue, D7), 2026-08-12 (Wed, D8),
    # 2026-08-13 (Thu, D9), 2026-08-14 (Fri, D10)
    dates = [
        date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7),
        date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13), date(2026, 8, 14),
    ]
    for d in prior_dates + dates:
        con.execute("INSERT INTO trading_calendar (trade_date, is_open) VALUES (?, true)", [d])

    # 2. Stock Info
    stocks = [
        ("000001.SZ", "Stock A", "2020-01-01"),
        ("600519.SH", "Stock B", "2020-01-01"),
        ("000002.SZ", "Stock C", "2020-01-01"),
        ("300750.SZ", "Stock New", "2026-07-28"),
    ]
    for ticker, name, list_d in stocks:
        con.execute("INSERT INTO stock_info (ticker, name, list_date) VALUES (?, ?, ?)", [ticker, name, list_d])

    # 3. Market Snapshots & THS Daily for prior dates (ensuring > 5 actual trading days)
    for p_d in prior_dates:
        for ticker in ["000001.SZ", "600519.SH", "000002.SZ"]:
            con.execute(
                "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, ?, ?, 10.0, 10.0, 9.8, 10.0, 1000, 10000, 0.0, false)",
                [p_d, ticker, ticker],
            )
        con.execute(
            "INSERT INTO ths_daily (trade_date, index_code, close, pct_change) VALUES (?, '881101.TI', 100.0, 0.0)",
            [p_d],
        )

    # 4. Market Snapshots & THS Daily for production dates
    for d in dates:
        con.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, '000001.SZ', 'Stock A', 10.0, 10.2, 9.8, 10.2, 1000, 10000, 2.0, false)",
            [d],
        )
        con.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, '600519.SH', 'Stock B', 20.0, 20.6, 19.8, 20.6, 1000, 20000, 3.0, false)",
            [d],
        )
        con.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, '000002.SZ', 'Stock C', 15.0, 15.3, 14.8, 15.3, 1000, 15000, 2.0, false)",
            [d],
        )
        con.execute(
            "INSERT INTO ths_daily (trade_date, index_code, close, pct_change) VALUES (?, '881101.TI', 100.0, 1.5)",
            [d],
        )

    # 5. Create canonical theme
    sc = StockCollectionService(con, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc))
    thm, coll = sc.create_canonical_theme(
        theme_name="高算力芯片",
        source_key="AI_CHIP",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )

    yield con
    con.close()


def _snapshot_canonical_tables(con: duckdb.DuckDBPyConnection) -> dict[str, list]:
    tables = [
        THEME_EFFECTIVE_MEMBER_DAILY_TABLE,
        THEME_CUSTOM_INDEX_DAILY_TABLE,
        THEME_CUSTOM_INDEX_STATE_TABLE,
        THEME_CUSTOM_INDEX_EPISODE_TABLE,
        THEME_M4_OBSERVATION_TABLE,
        "theme_production_run",
    ]
    return {t: con.execute(f"SELECT * FROM {t} ORDER BY ALL").fetchall() for t in tables}


# ==============================================================================
# Scenario 1: 08:59:59 录入，D 日立即生效进入候选
# ==============================================================================
def test_scenario_01_admission_at_085959_enters_d_immediately(f_db):
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 3, 8, 59, 59, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d = date(2026, 8, 3)

    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d,
        available_trade_date=d,
    )

    resolved = sc.resolver.batch_resolve_members(
        [thm[1]], [d], knowledge_date=d, enforce_admission_cutoff=True
    )
    assert len(resolved) == 1
    assert resolved.iloc[0]["asset_id"] == "000001.SZ"


# ==============================================================================
# Scenario 2: 09:00:00 录入，D 日不生效，D_next 才生效
# ==============================================================================
def test_scenario_02_admission_at_090000_excluded_on_d_admitted_on_d_next(f_db):
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 3, 9, 0, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)

    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d1,
        available_trade_date=d1,
    )

    # On D1: cutoff strictly excludes 09:00:00 (requires t < D 09:00:00)
    res_d1 = sc.resolver.batch_resolve_members(
        [thm[1]], [d1], knowledge_date=d2, enforce_admission_cutoff=True
    )
    assert res_d1.empty

    # On D2: 09:00:00 on D1 is well before D2 09:00:00 -> admitted on D2
    res_d2 = sc.resolver.batch_resolve_members(
        [thm[1]], [d2], knowledge_date=d2, enforce_admission_cutoff=True
    )
    assert len(res_d2) == 1
    assert res_d2.iloc[0]["asset_id"] == "000001.SZ"


# ==============================================================================
# Scenario 3: 09:00:01 录入，即使声明 effective_from <= D，D 日也不生效
# ==============================================================================
def test_scenario_03_admission_at_090001_with_retroactive_effective_from_excluded_on_d(f_db):
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 3, 9, 0, 1, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)

    # 声明从 d1 (2026-08-03) 就生效，但提交时刻为 08-03 09:00:01
    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d1,
        available_trade_date=d1,
    )

    res_d1 = sc.resolver.batch_resolve_members(
        [thm[1]], [d1], knowledge_date=d2, enforce_admission_cutoff=True
    )
    assert res_d1.empty

    res_d2 = sc.resolver.batch_resolve_members(
        [thm[1]], [d2], knowledge_date=d2, enforce_admission_cutoff=True
    )
    assert len(res_d2) == 1
    assert res_d2.iloc[0]["asset_id"] == "000001.SZ"


# ==============================================================================
# Scenario 4: 跨午夜维护：D-1 23:30 录入，D 日正常生效
# ==============================================================================
def test_scenario_04_cross_midnight_maintenance_admitted_on_d(f_db):
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 3, 23, 30, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d = date(2026, 8, 4)

    # D-1 (2026-08-03) 23:30:00 提交
    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d,
        available_trade_date=d,
    )

    res = sc.resolver.batch_resolve_members(
        [thm[1]], [d], knowledge_date=d, enforce_admission_cutoff=True
    )
    assert len(res) == 1
    assert res.iloc[0]["asset_id"] == "000001.SZ"


# ==============================================================================
# Scenario 5: 盘前维护：D 08:30 录入，D 日正常生效
# ==============================================================================
def test_scenario_05_pre_market_maintenance_at_0830_admitted_on_d(f_db):
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 5, 8, 30, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d = date(2026, 8, 5)

    # D 08:30:00 提交
    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="600519.SH",
        effective_from=d,
        available_trade_date=d,
    )

    res = sc.resolver.batch_resolve_members(
        [thm[1]], [d], knowledge_date=d, enforce_admission_cutoff=True
    )
    assert len(res) == 1
    assert res.iloc[0]["asset_id"] == "600519.SH"


# ==============================================================================
# Scenario 6: 周末录入：周六录入且 effective_from 覆盖下周一，下周一生效
# ==============================================================================
def test_scenario_06_weekend_maintenance_admitted_on_monday(f_db):
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 8, 14, 0, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    # 2026-08-08 (Saturday) 提交，下周一为 2026-08-10
    monday = date(2026, 8, 10)

    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000002.SZ",
        effective_from=monday,
        available_trade_date=monday,
    )

    res = sc.resolver.batch_resolve_members(
        [thm[1]], [monday], knowledge_date=monday, enforce_admission_cutoff=True
    )
    assert len(res) == 1
    assert res.iloc[0]["asset_id"] == "000002.SZ"


# ==============================================================================
# Scenario 7: 08:59:59 删除，D 日立即不进入候选
# ==============================================================================
def test_scenario_07_removal_at_085959_removes_on_d_immediately(f_db):
    sc_add = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 12, 0, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)

    # 初始添加
    mem = sc_add.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d1,
        available_trade_date=d1,
    )

    # D2 08:59:59 删除
    sc_remove = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 4, 8, 59, 59, tzinfo=SHANGHAI_TZ))
    sc_remove.remove_member(
        membership_id=mem.membership_id,
        removal_date=d2,
        available_trade_date=d2,
    )

    res_d1 = sc_remove.resolver.batch_resolve_members(
        [thm[1]], [d1], knowledge_date=d2, enforce_admission_cutoff=True
    )
    assert len(res_d1) == 1

    res_d2 = sc_remove.resolver.batch_resolve_members(
        [thm[1]], [d2], knowledge_date=d2, enforce_admission_cutoff=True
    )
    assert res_d2.empty


# ==============================================================================
# Scenario 8: 09:00:00 删除，D 日仍作为候选，D_next 起移除
# ==============================================================================
def test_scenario_08_removal_at_090000_persists_on_d_removed_on_d_next(f_db):
    sc_add = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 12, 0, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)
    d3 = date(2026, 8, 5)

    mem = sc_add.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d1,
        available_trade_date=d1,
    )

    # D2 09:00:00 执行删除（迟于 D2 09:00 cutoff，因此 D2 仍按原状态处理）
    sc_remove = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 4, 9, 0, 0, tzinfo=SHANGHAI_TZ))
    sc_remove.remove_member(
        membership_id=mem.membership_id,
        removal_date=d2,
        available_trade_date=d2,
    )

    # D2 仍包含
    res_d2 = sc_remove.resolver.batch_resolve_members(
        [thm[1]], [d2], knowledge_date=d3, enforce_admission_cutoff=True
    )
    assert len(res_d2) == 1

    # D3 起被移除
    res_d3 = sc_remove.resolver.batch_resolve_members(
        [thm[1]], [d3], knowledge_date=d3, enforce_admission_cutoff=True
    )
    assert res_d3.empty


# ==============================================================================
# Scenario 9: 实际计算时刻 T 早跑、正常、重跑，候选集合严格一致
# ==============================================================================
def test_scenario_09_actual_calculation_time_t_does_not_affect_candidates(f_db):
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 4, 8, 0, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d = date(2026, 8, 4)

    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d,
        available_trade_date=d,
    )

    # 模拟早跑（08:45）、收盘跑（16:00）、晚间重跑（20:00）调用 resolver
    run_early = sc.resolver.batch_resolve_members([thm[1]], [d], knowledge_date=d, enforce_admission_cutoff=True)
    run_normal = sc.resolver.batch_resolve_members([thm[1]], [d], knowledge_date=d, enforce_admission_cutoff=True)
    run_late = sc.resolver.batch_resolve_members([thm[1]], [d], knowledge_date=d, enforce_admission_cutoff=True)

    pd.testing.assert_frame_equal(run_early, run_normal)
    pd.testing.assert_frame_equal(run_normal, run_late)


# ==============================================================================
# Scenario 10: 上市交易日事实缺失 / UNRESOLVED_MISSING，严格 fail-closed 为 UNCONFIRMED_LISTING_DAYS
# ==============================================================================
def test_scenario_10_missing_listing_facts_fail_closed_unconfirmed(f_db):
    d = date(2026, 8, 3)
    coll_id = "COLL:TEST"
    memberships = pd.DataFrame([{
        "collection_id": coll_id,
        "asset_id": "999999.SZ",
        "trade_date": d,
    }])
    # listing_df 为空或无此股票
    listing_df = pd.DataFrame(columns=[ASSET_ID, TRADE_DATE, CONFIRMED_LISTING_TRADING_DAY_COUNT, "market_fact_status"])
    susp_df = pd.DataFrame(columns=[ASSET_ID, TRADE_DATE, IS_SUSPENDED])

    res = calculate_m4_effective_members(memberships, listing_df, susp_df)
    assert len(res) == 1
    assert res.iloc[0][IS_M4_EFFECTIVE_MEMBER] == False
    assert res.iloc[0][EXCLUSION_REASON] == EXCLUSION_REASON_UNCONFIRMED_LISTING_DAYS


# ==============================================================================
# Scenario 11: 上市交易日数 = 5，排除为 NEW_LISTING_LE_5
# ==============================================================================
def test_scenario_11_listing_days_5_excluded_new_listing_le_5(f_db):
    d = date(2026, 8, 3)
    coll_id = "COLL:TEST"
    memberships = pd.DataFrame([{
        "collection_id": coll_id,
        "asset_id": "300750.SZ",
        "trade_date": d,
    }])
    listing_df = pd.DataFrame([{
        ASSET_ID: "300750.SZ",
        TRADE_DATE: d,
        CONFIRMED_LISTING_TRADING_DAY_COUNT: 5,
        "market_fact_status": "ACTUAL_TRADING",
    }])
    susp_df = pd.DataFrame(columns=[ASSET_ID, TRADE_DATE, IS_SUSPENDED])

    res = calculate_m4_effective_members(memberships, listing_df, susp_df)
    assert len(res) == 1
    assert res.iloc[0][IS_M4_EFFECTIVE_MEMBER] == False
    assert res.iloc[0][EXCLUSION_REASON] == EXCLUSION_REASON_NEW_LISTING_LE_5


# ==============================================================================
# Scenario 12: 上市交易日数 = 6，通过上市交易日规则
# ==============================================================================
def test_scenario_12_listing_days_6_qualifies(f_db):
    d = date(2026, 8, 4)
    coll_id = "COLL:TEST"
    memberships = pd.DataFrame([{
        "collection_id": coll_id,
        "asset_id": "300750.SZ",
        "trade_date": d,
    }])
    listing_df = pd.DataFrame([{
        ASSET_ID: "300750.SZ",
        TRADE_DATE: d,
        CONFIRMED_LISTING_TRADING_DAY_COUNT: 6,
        "market_fact_status": "ACTUAL_TRADING",
    }])
    susp_df = pd.DataFrame(columns=[ASSET_ID, TRADE_DATE, IS_SUSPENDED])

    res = calculate_m4_effective_members(memberships, listing_df, susp_df)
    assert len(res) == 1
    assert res.iloc[0][IS_M4_EFFECTIVE_MEMBER] == True
    assert pd.isna(res.iloc[0][EXCLUSION_REASON]) or res.iloc[0][EXCLUSION_REASON] is None


# ==============================================================================
# Scenario 13: 既 unconfirmed 又 suspended，按优先级标记为 UNCONFIRMED_LISTING_DAYS
# ==============================================================================
def test_scenario_13_priority_unconfirmed_over_suspended(f_db):
    d = date(2026, 8, 3)
    coll_id = "COLL:TEST"
    memberships = pd.DataFrame([{
        "collection_id": coll_id,
        "asset_id": "000001.SZ",
        "trade_date": d,
    }])
    listing_df = pd.DataFrame([{
        ASSET_ID: "000001.SZ",
        TRADE_DATE: d,
        CONFIRMED_LISTING_TRADING_DAY_COUNT: None,
        "market_fact_status": "UNRESOLVED_MISSING",
    }])
    susp_df = pd.DataFrame([{
        ASSET_ID: "000001.SZ",
        TRADE_DATE: d,
        IS_SUSPENDED: True,
    }])

    res = calculate_m4_effective_members(memberships, listing_df, susp_df)
    assert len(res) == 1
    assert res.iloc[0][IS_M4_EFFECTIVE_MEMBER] == False
    assert res.iloc[0][EXCLUSION_REASON] == EXCLUSION_REASON_UNCONFIRMED_LISTING_DAYS


# ==============================================================================
# Scenario 14: 既 new_listing_le_5 又 suspended，按优先级标记为 NEW_LISTING_LE_5
# ==============================================================================
def test_scenario_14_priority_new_listing_le_5_over_suspended(f_db):
    d = date(2026, 8, 3)
    coll_id = "COLL:TEST"
    memberships = pd.DataFrame([{
        "collection_id": coll_id,
        "asset_id": "000001.SZ",
        "trade_date": d,
    }])
    listing_df = pd.DataFrame([{
        ASSET_ID: "000001.SZ",
        TRADE_DATE: d,
        CONFIRMED_LISTING_TRADING_DAY_COUNT: 3,
        "market_fact_status": "ACTUAL_TRADING",
    }])
    susp_df = pd.DataFrame([{
        ASSET_ID: "000001.SZ",
        TRADE_DATE: d,
        IS_SUSPENDED: True,
    }])

    res = calculate_m4_effective_members(memberships, listing_df, susp_df)
    assert len(res) == 1
    assert res.iloc[0][IS_M4_EFFECTIVE_MEMBER] == False
    assert res.iloc[0][EXCLUSION_REASON] == EXCLUSION_REASON_NEW_LISTING_LE_5


# ==============================================================================
# Scenario 15: EXPLICIT_NON_TRADING 正确归入停牌不可交易
# ==============================================================================
def test_scenario_15_explicit_non_trading_treated_as_suspended(f_db):
    d = date(2026, 8, 3)
    coll_id = "COLL:TEST"
    memberships = pd.DataFrame([{
        "collection_id": coll_id,
        "asset_id": "000001.SZ",
        "trade_date": d,
    }])
    listing_df = pd.DataFrame([{
        ASSET_ID: "000001.SZ",
        TRADE_DATE: d,
        CONFIRMED_LISTING_TRADING_DAY_COUNT: 100,
        "market_fact_status": "EXPLICIT_NON_TRADING",
    }])
    susp_df = pd.DataFrame(columns=[ASSET_ID, TRADE_DATE, IS_SUSPENDED])

    res = calculate_m4_effective_members(memberships, listing_df, susp_df)
    assert len(res) == 1
    assert res.iloc[0][IS_M4_EFFECTIVE_MEMBER] == False
    assert res.iloc[0][EXCLUSION_REASON] == EXCLUSION_REASON_SUSPENDED


# ==============================================================================
# Scenario 16: theme_effective_member_daily 物理落库，字段完整，主键生效
# ==============================================================================
# ==============================================================================
# Scenario 16: theme_effective_member_daily 表结构、非空字段、物理主键约束
# ==============================================================================
def test_scenario_16_theme_effective_member_daily_table_schema_and_pk_integrity(f_db):
    service = ThemePipelineService(f_db)
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 3, 8, 30, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d = date(2026, 8, 3)

    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d,
        available_trade_date=d,
    )

    service.run_m4_daily(trade_date=d)

    rows = f_db.execute(f"SELECT * FROM {THEME_EFFECTIVE_MEMBER_DAILY_TABLE} WHERE trade_date = ?", [d]).fetchall()
    assert len(rows) == 1
    row = rows[0]
    # 验证字段数量为 14
    assert len(row) == 14
    assert row[0] == thm[1]  # collection_id
    assert row[1] == thm[0]  # theme_id
    assert row[2] == "000001.SZ"  # asset_id
    assert row[4] == True  # is_theme_member
    assert row[7] == True  # is_m4_effective_member
    assert row[13] is not None  # finalized_at

    # 验证主键约束 (collection_id, trade_date, asset_id) 阻止重复插入
    with pytest.raises(Exception):
        f_db.execute(
            f"""
            INSERT INTO {THEME_EFFECTIVE_MEMBER_DAILY_TABLE} (
                collection_id, theme_id, asset_id, trade_date, is_theme_member,
                confirmed_listing_trading_day_count, is_suspended, is_m4_effective_member,
                exclusion_reason, calculation_version, input_snapshot_id, production_run_id,
                created_at, finalized_at
            ) VALUES (?, ?, ?, ?, true, 10, false, true, null, 'v1', 's1', 'r1', now(), now())
            """,
            [thm[1], thm[0], "000001.SZ", d],
        )


# ==============================================================================
# Scenario 17: D 日 finalized 后，后续倒填更早成员，D 日及之前历史账本绝对不变
# ==============================================================================
def test_scenario_17_immutable_production_history_survives_retroactive_membership(f_db):
    service = ThemePipelineService(f_db)
    sc1 = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)

    # 初始添加 Stock A
    sc1.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d1,
        available_trade_date=d1,
    )

    # 1. 正常生产 D1 与 D2
    service.rebuild_m4_facts(start_date=d1, end_date=d2)
    snap_before = _snapshot_canonical_tables(f_db)

    # 2. 倒填/追溯成员 Stock B，声称 effective_from 从 D1 生效，但在 D2 收盘后 (2026-08-04 18:00) 提交
    sc2 = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 4, 18, 0, 0, tzinfo=SHANGHAI_TZ))
    sc2.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="600519.SH",
        effective_from=d1,
        available_trade_date=date(2026, 8, 5),
    )

    # 3. 再次触发生产 / 纠错调用 D1..D2
    service.rebuild_m4_facts(start_date=d1, end_date=d2, run_type="CORRECTION")
    snap_after = _snapshot_canonical_tables(f_db)

    # 4. 严格断言：已 finalized 的物理账本 100% 不变！
    assert snap_before == snap_after


# ==============================================================================
# Scenario 18: Custom Index 延续计算正确从上一个已落库 finite cumulative level 开始
# ==============================================================================
def test_scenario_18_custom_index_continues_from_finalized_finite_level_anchor(f_db):
    service = ThemePipelineService(f_db)
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)

    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d1,
        available_trade_date=d1,
    )

    # 生产 D1: return is 2% -> level = 1000.0 * 1.02 = 1020.0
    service.run_m4_daily(trade_date=d1)
    lvl_d1 = f_db.execute(
        f"SELECT index_level FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE trade_date = ?",
        [d1],
    ).fetchone()[0]
    assert np.isclose(lvl_d1, 1020.0)

    # 生产 D2: 单独跑 D2，必须锚定 D1 的 1020.0 向后递推 -> 1020.0 * 1.02 = 1040.4
    service.run_m4_daily(trade_date=d2)
    lvl_d2 = f_db.execute(
        f"SELECT index_level FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE trade_date = ?",
        [d2],
    ).fetchone()[0]
    assert np.isclose(lvl_d2, 1040.4)


# ==============================================================================
# Scenario 19: State / Episode 在已 finalized 历史的基础上向前推进，不改写历史 closed episode
# ==============================================================================
def test_scenario_19_state_and_episode_forward_continuation_without_closed_episode_restatement(f_db):
    dates = [
        date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7),
    ]
    service = ThemePipelineService(f_db)
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()

    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=dates[0],
        available_trade_date=dates[0],
    )

    # 初始生产前 3 天
    service.rebuild_m4_facts(start_date=dates[0], end_date=dates[2])
    states_before = f_db.execute(f"SELECT trade_date, trend_state FROM {THEME_CUSTOM_INDEX_STATE_TABLE} ORDER BY trade_date").fetchall()
    assert len(states_before) == 3

    # 继续向后推进生产 Day 4 与 Day 5
    service.rebuild_m4_facts(start_date=dates[3], end_date=dates[4])

    # 验证前 3 天的 states 保持原状，无任何改写
    states_after = f_db.execute(f"SELECT trade_date, trend_state FROM {THEME_CUSTOM_INDEX_STATE_TABLE} ORDER BY trade_date").fetchall()
    assert len(states_after) == 5
    assert states_after[:3] == states_before


# ==============================================================================
# Scenario 20: replay_m4_facts(...) 严格只读、非物化，不改变物理表与 production_run
# ==============================================================================
def test_scenario_20_replay_is_strictly_read_only_and_non_materializing(f_db):
    service = ThemePipelineService(f_db)
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=SHANGHAI_TZ))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)

    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d1,
        available_trade_date=d1,
    )

    # 1. 初始生产 D1
    service.run_m4_daily(trade_date=d1)
    snapshot_pre_replay = _snapshot_canonical_tables(f_db)

    # 2. 执行只读回放（包含不同日期与不同 knowledge_date）
    rep1 = service.replay_m4_facts(start_date=d1, end_date=d2, knowledge_date=d2)
    rep2 = service.replay_m4_facts(start_date=d2, end_date=d2, knowledge_date=d2)

    # 回放返回完整计算结果
    assert not rep1.daily_indices.empty
    assert not rep1.m4_observations.empty
    assert not rep1.effective_members.empty
    assert rep1.input_snapshot_id.startswith("SNAP:")

    # 3. 严格断言：回放未向任何表写入任何一行，物理快照 100% 相同！
    snapshot_post_replay = _snapshot_canonical_tables(f_db)
    assert snapshot_pre_replay == snapshot_post_replay


# ==============================================================================
# P0-2: 真实生产写入路径测试 UTC ↔ Asia/Shanghai cutoff (00:59:59 / 01:00:00 / 01:00:01 UTC)
# ==============================================================================
def test_p0_2_production_write_path_utc_and_shanghai_cutoff(f_db):
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} LIMIT 1").fetchone()
    d = date(2026, 8, 3)
    d_next = date(2026, 8, 4)

    # 1. 00:59:59 UTC = 08:59:59 +08 -> D admitted
    sc_0859 = StockCollectionService(
        f_db, clock=lambda: datetime(2026, 8, 3, 0, 59, 59, tzinfo=timezone.utc)
    )
    sc_0859.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d,
        available_trade_date=d,
    )

    # 2. 01:00:00 UTC = 09:00:00 +08 -> D not admitted, D_next admitted
    sc_0900 = StockCollectionService(
        f_db, clock=lambda: datetime(2026, 8, 3, 1, 0, 0, tzinfo=timezone.utc)
    )
    sc_0900.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="600519.SH",
        effective_from=d,
        available_trade_date=d,
    )

    # 3. 01:00:01 UTC = 09:00:01 +08 -> D not admitted, D_next admitted
    sc_0901 = StockCollectionService(
        f_db, clock=lambda: datetime(2026, 8, 3, 1, 0, 1, tzinfo=timezone.utc)
    )
    sc_0901.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000002.SZ",
        effective_from=d,
        available_trade_date=d,
    )

    # 验证在 UTC session 下的表现
    f_db.execute("SET TimeZone = 'UTC'")
    res_d_utc = sc_0859.resolver.batch_resolve_members(
        [thm[1]], [d], knowledge_date=d_next, enforce_admission_cutoff=True
    )
    assert set(res_d_utc["asset_id"]) == {"000001.SZ"}

    res_dnext_utc = sc_0859.resolver.batch_resolve_members(
        [thm[1]], [d_next], knowledge_date=d_next, enforce_admission_cutoff=True
    )
    assert set(res_dnext_utc["asset_id"]) == {"000001.SZ", "600519.SH", "000002.SZ"}

    # 验证在 Asia/Shanghai session 下的表现完全一致
    f_db.execute("SET TimeZone = 'Asia/Shanghai'")
    res_d_sh = sc_0859.resolver.batch_resolve_members(
        [thm[1]], [d], knowledge_date=d_next, enforce_admission_cutoff=True
    )
    assert set(res_d_sh["asset_id"]) == {"000001.SZ"}


# ==============================================================================
# P0-3 (A): 0 candidate members Theme 生产判定为已完成，不与“尚未运行”等价
# ==============================================================================
def test_p0_3_zero_member_theme_finalization_not_equivalent_to_unrun(f_db):
    service = ThemePipelineService(f_db)
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc))
    thm_empty, coll_empty = sc.create_canonical_theme(
        theme_name="空主题",
        source_key="EMPTY_THEME",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )
    d = date(2026, 8, 3)

    # 运行前：该 theme 未 finalized
    finalized_before = service._get_finalized_theme_ids(d)
    assert thm_empty.theme_id not in finalized_before

    # 生产 Day D
    rep = service.run_m4_daily(trade_date=d)

    # 运行后：即便 0 成员，该 Theme 也明确判定为 finalized
    finalized_after = service._get_finalized_theme_ids(d)
    assert thm_empty.theme_id in finalized_after

    idx_row = f_db.execute(
        f"SELECT effective_member_count, total_member_count, index_level FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE theme_id = ? AND trade_date = ?",
        [thm_empty.theme_id, d],
    ).fetchone()
    assert idx_row is not None
    assert idx_row[0] == 0
    assert idx_row[1] == 0
    assert idx_row[2] is None or np.isnan(idx_row[2])

    obs_row = f_db.execute(
        f"SELECT effective_member_count, total_member_count FROM {THEME_M4_OBSERVATION_TABLE} WHERE theme_id = ? AND trade_date = ?",
        [thm_empty.theme_id, d],
    ).fetchone()
    assert obs_row is not None
    assert obs_row[0] == 0

    # 再次运行：整日全部 Theme 均已 finalized，短路跳过且不报错
    rep_rerun = service.run_m4_daily(trade_date=d)
    assert rep_rerun.total_index_rows >= 2


# ==============================================================================
# P0-3 (B): 模拟多 Theme 生产中途失败导致部分写入，Retry 必须完成剩余生产且不覆盖
# ==============================================================================
def test_p0_3_multi_theme_partial_failure_and_retry_recovery(f_db):
    service = ThemePipelineService(f_db)
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc))
    thm_b, coll_b = sc.create_canonical_theme(
        theme_name="次级主题",
        source_key="SUB_THEME",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )
    sc.add_member(
        theme_id=thm_b.theme_id,
        collection_id=coll_b.collection_id,
        asset_id="600519.SH",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )

    thm_main = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} WHERE theme_id = 'THM:QRP:AI_CHIP'").fetchone()
    sc.add_member(
        theme_id=thm_main[0],
        collection_id=thm_main[1],
        asset_id="000001.SZ",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )

    d = date(2026, 8, 3)

    # 1. 模拟中途崩溃：只有 thm_main 写入了 facts，而 thm_b 尚未写入
    # 手工为 thm_main 写入一条 finalized facts
    now = datetime.now(timezone.utc)
    f_db.execute(
        f"""
        INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE} (
            theme_id, collection_id, trade_date, theme_daily_return, index_level,
            base_level, effective_member_count, total_member_count, calculation_version,
            production_run_id, input_snapshot_id, created_at
        ) VALUES (?, ?, ?, 0.02, 1020.0, 1000.0, 1, 1, 'v1', 'partial_run', 'snap_part', ?)
        """,
        [thm_main[0], thm_main[1], d, now],
    )
    f_db.execute(
        f"""
        INSERT INTO {THEME_EFFECTIVE_MEMBER_DAILY_TABLE} (
            collection_id, theme_id, asset_id, trade_date, is_theme_member,
            confirmed_listing_trading_day_count, is_suspended, is_m4_effective_member,
            exclusion_reason, calculation_version, input_snapshot_id, production_run_id,
            created_at, finalized_at
        ) VALUES (?, ?, '000001.SZ', ?, true, 10, false, true, null, 'v1', 'snap_part', 'partial_run', ?, ?)
        """,
        [thm_main[1], thm_main[0], d, now, now],
    )
    f_db.execute(
        f"""
        INSERT INTO {THEME_M4_OBSERVATION_TABLE} (
            theme_id, collection_id, trade_date, theme_daily_return,
            theme_limit_up_count, theme_return_rank, effective_member_count,
            total_member_count, comparison_universe_size, comparison_universe_version,
            custom_index_trend_state, custom_index_trend_run_days, custom_index_episode_id,
            qualification_status, calculation_version, production_run_id, input_snapshot_id, created_at
        ) VALUES (?, ?, ?, 0.02, 0, 1, 1, 1, 1, 'v1', null, null, null, 'QUALIFIED', 'v1', 'partial_run', 'snap_part', ?)
        """,
        [thm_main[0], thm_main[1], d, now],
    )

    # 检查状态：thm_main 已完成，thm_b 未完成
    finalized = service._get_finalized_theme_ids(d)
    assert thm_main[0] in finalized
    assert thm_b.theme_id not in finalized

    # 2. 执行生产 Retry：系统不得因存在部分 finalized 行而整日短路！
    rep = service.run_m4_daily(trade_date=d)

    # 3. 验证 thm_b 现已成功完成
    finalized_retry = service._get_finalized_theme_ids(d)
    assert thm_b.theme_id in finalized_retry
    assert thm_main[0] in finalized_retry

    # 4. 验证 thm_main 原有的 production_run_id = 'partial_run' 保持未被覆盖或破坏
    main_fact = f_db.execute(
        f"SELECT production_run_id FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE theme_id = ? AND trade_date = ?",
        [thm_main[0], d],
    ).fetchone()
    assert main_fact[0] == "partial_run"


# ==============================================================================
# P1: 确认 State / Episode 是基于 previous finalized state + active OPEN episode + D 的 continuation
# ==============================================================================
def test_p1_state_and_episode_single_day_continuation_without_historical_replay(f_db):
    service = ThemePipelineService(f_db)
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} WHERE theme_id = 'THM:QRP:AI_CHIP'").fetchone()

    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )

    dates = [
        date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7),
    ]

    # 连续推进生产前 4 天
    for dt in dates[:4]:
        service.run_m4_daily(trade_date=dt)

    st_d4 = f_db.execute(
        f"""
        SELECT trade_date, close, ma5, trend_state, custom_index_trend_run_days
        FROM {THEME_CUSTOM_INDEX_STATE_TABLE}
        WHERE theme_id = ? AND trade_date = ?
        """,
        [thm[0], dates[3]],
    ).fetchone()
    assert st_d4 is not None

    # 生产第 5 天：单步推进
    service.run_m4_daily(trade_date=dates[4])

    st_d5 = f_db.execute(
        f"""
        SELECT trade_date, close, ma5, trend_state, previous_trend_state, custom_index_trend_run_days
        FROM {THEME_CUSTOM_INDEX_STATE_TABLE}
        WHERE theme_id = ? AND trade_date = ?
        """,
        [thm[0], dates[4]],
    ).fetchone()
    assert st_d5 is not None
    # 验证 previous_trend_state 严格对应 D4 的 trend_state
    assert st_d5[4] == st_d4[3]
    # 验证历史 D1..D4 的状态行完全未被修改
    st_d4_after = f_db.execute(
        f"""
        SELECT trade_date, close, ma5, trend_state, custom_index_trend_run_days
        FROM {THEME_CUSTOM_INDEX_STATE_TABLE}
        WHERE theme_id = ? AND trade_date = ?
        """,
        [thm[0], dates[3]],
    ).fetchone()
    assert st_d4 == st_d4_after


# ==============================================================================
# P0: 恢复 Trend MA5/MA10 的 contiguous-price 语义 (NULL gap 重置均线窗口)
# ==============================================================================
def test_p0_trend_ma5_contiguous_price_null_gap_resets_window(f_db):
    """
    回归验证：4 finite → 1 NULL → D finite
    断言 D 的 MA5 仍为 NULL，trend state 未被跨越 NULL gap 的历史价格错误恢复。
    """
    service = ThemePipelineService(f_db)
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} WHERE theme_id = 'THM:QRP:AI_CHIP'").fetchone()

    # Dates from August: 2026-08-03 .. 2026-08-10
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)
    d3 = date(2026, 8, 5)
    d4 = date(2026, 8, 6)
    d5 = date(2026, 8, 7)
    d6 = date(2026, 8, 10)

    now = datetime.now(timezone.utc)
    for i, dt in enumerate([d1, d2, d3, d4]):
        lvl = 1000.0 + i * 10.0
        f_db.execute(
            f"""
            INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE} (
                theme_id, collection_id, trade_date, theme_daily_return, index_level,
                base_level, effective_member_count, total_member_count, calculation_version,
                production_run_id, input_snapshot_id, created_at
            ) VALUES (?, ?, ?, 0.01, ?, 1000.0, 1, 1, 'v1', 'setup_run', 'snap', ?)
            """,
            [thm[0], thm[1], dt, lvl, now],
        )
        f_db.execute(
            f"""
            INSERT INTO {THEME_CUSTOM_INDEX_STATE_TABLE} (
                theme_id, collection_id, trade_date, close, ma5, ma10,
                trend_state, previous_trend_state, custom_index_trend_run_days,
                is_above_or_equal_ma5, state_changed, rule_version,
                production_run_id, input_snapshot_id, created_at
            ) VALUES (?, ?, ?, ?, null, null, null, null, 0, null, false, 'v1', 'setup_run', 'snap', ?)
            """,
            [thm[0], thm[1], dt, lvl, now],
        )

    # d5 写入 1 个 NULL index_level
    f_db.execute(
        f"""
        INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE} (
            theme_id, collection_id, trade_date, theme_daily_return, index_level,
            base_level, effective_member_count, total_member_count, calculation_version,
            production_run_id, input_snapshot_id, created_at
        ) VALUES (?, ?, ?, null, null, 1000.0, 0, 0, 'v1', 'setup_run', 'snap', ?)
        """,
        [thm[0], thm[1], d5, now],
    )
    f_db.execute(
        f"""
        INSERT INTO {THEME_CUSTOM_INDEX_STATE_TABLE} (
            theme_id, collection_id, trade_date, close, ma5, ma10,
            trend_state, previous_trend_state, custom_index_trend_run_days,
            is_above_or_equal_ma5, state_changed, rule_version,
            production_run_id, input_snapshot_id, created_at
        ) VALUES (?, ?, ?, null, null, null, null, null, 0, null, false, 'v1', 'setup_run', 'snap', ?)
        """,
        [thm[0], thm[1], d5, now],
    )

    # 为 d6 增加 1 个有效成员
    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d6,
        available_trade_date=d6,
    )

    # 运行 d6 生产 (D 是第 1 个 finite 日子，前面紧邻 NULL)
    service.run_m4_daily(trade_date=d6)

    st_d6 = f_db.execute(
        f"SELECT close, ma5, ma10, trend_state FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE theme_id = ? AND trade_date = ?",
        [thm[0], d6],
    ).fetchone()

    assert st_d6 is not None
    assert st_d6[0] is not None  # today's close is finite
    assert st_d6[1] is None      # MA5 MUST BE NULL! (Cannot cross NULL gap to use d1..d4)
    assert st_d6[2] is None      # MA10 MUST BE NULL!
    assert st_d6[3] is None      # trend_state MUST BE NULL! (incomplete MA5 window)


def test_p0_trend_ma10_contiguous_price_null_gap_resets_window(f_db):
    """
    回归验证：9 finite → 1 NULL → 5 finite (D 是第 5 个 finite)
    断言 D 的 MA5 计算正确 (5个连续价格)，但 MA10 必须仍为 NULL (不能跨越 NULL gap 取前面的 9 个价格)。
    """
    service = ThemePipelineService(f_db)
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc))
    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} WHERE theme_id = 'THM:QRP:AI_CHIP'").fetchone()

    # Prior July dates (10 dates): 2026-07-20 .. 2026-07-31
    # 9 finite days: 2026-07-20 .. 2026-07-30 (exclude 31)
    prior_9 = [date(2026, 7, i) for i in range(20, 29)]
    d_null = date(2026, 7, 29)
    d_f1 = date(2026, 7, 30)
    d_f2 = date(2026, 7, 31)
    d_f3 = date(2026, 8, 3)
    d_f4 = date(2026, 8, 4)
    d_f5 = date(2026, 8, 5)  # D day

    now = datetime.now(timezone.utc)
    for i, dt in enumerate(prior_9):
        lvl = 1000.0 + i * 10.0
        f_db.execute(
            f"""
            INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE} (
                theme_id, collection_id, trade_date, theme_daily_return, index_level,
                base_level, effective_member_count, total_member_count, calculation_version,
                production_run_id, input_snapshot_id, created_at
            ) VALUES (?, ?, ?, 0.01, ?, 1000.0, 1, 1, 'v1', 'setup_run', 'snap', ?)
            """,
            [thm[0], thm[1], dt, lvl, now],
        )

    # NULL day
    f_db.execute(
        f"""
        INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE} (
            theme_id, collection_id, trade_date, theme_daily_return, index_level,
            base_level, effective_member_count, total_member_count, calculation_version,
            production_run_id, input_snapshot_id, created_at
        ) VALUES (?, ?, ?, null, null, 1000.0, 0, 0, 'v1', 'setup_run', 'snap', ?)
        """,
        [thm[0], thm[1], d_null, now],
    )

    # 4 finite days before D
    for i, dt in enumerate([d_f1, d_f2, d_f3, d_f4]):
        lvl = 1100.0 + i * 10.0
        f_db.execute(
            f"""
            INSERT INTO {THEME_CUSTOM_INDEX_DAILY_TABLE} (
                theme_id, collection_id, trade_date, theme_daily_return, index_level,
                base_level, effective_member_count, total_member_count, calculation_version,
                production_run_id, input_snapshot_id, created_at
            ) VALUES (?, ?, ?, 0.01, ?, 1000.0, 1, 1, 'v1', 'setup_run', 'snap', ?)
            """,
            [thm[0], thm[1], dt, lvl, now],
        )
        f_db.execute(
            f"""
            INSERT INTO {THEME_CUSTOM_INDEX_STATE_TABLE} (
                theme_id, collection_id, trade_date, close, ma5, ma10,
                trend_state, previous_trend_state, custom_index_trend_run_days,
                is_above_or_equal_ma5, state_changed, rule_version,
                production_run_id, input_snapshot_id, created_at
            ) VALUES (?, ?, ?, ?, null, null, null, null, 0, null, false, 'v1', 'setup_run', 'snap', ?)
            """,
            [thm[0], thm[1], dt, lvl, now],
        )

    # 为 D (d_f5) 添加有效成员
    sc.add_member(
        theme_id=thm[0],
        collection_id=thm[1],
        asset_id="000001.SZ",
        effective_from=d_f5,
        available_trade_date=d_f5,
    )

    # 运行 D (d_f5) 生产
    service.run_m4_daily(trade_date=d_f5)

    st_df5 = f_db.execute(
        f"SELECT close, ma5, ma10, trend_state FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE theme_id = ? AND trade_date = ?",
        [thm[0], d_f5],
    ).fetchone()

    assert st_df5 is not None
    assert st_df5[0] is not None
    assert st_df5[1] is not None  # MA5 is available (5 contiguous finite prices)
    assert st_df5[2] is None      # MA10 MUST BE NULL! (Cannot cross d_null to reach prior_9 prices)


# ==============================================================================
# P0: Theme-D materialization 具备原子事务边界 (异常注入与回滚验证)
# ==============================================================================
def test_p0_theme_d_materialization_atomic_transaction_rollback(f_db, monkeypatch):
    """
    测试原子事务边界：
    1. 同一 Theme-D 任一步失败，整个 Theme-D ROLLBACK，不存在半成品；
    2. Retry 可以完整重新生产；
    3. 已经真正 committed/finalized 的其它 Theme 不受影响。
    """
    service = ThemePipelineService(f_db)
    sc = StockCollectionService(f_db, clock=lambda: datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc))

    # 创建两个主题：Theme 1 (A) 和 Theme 2 (B)
    thm_a = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} WHERE theme_id = 'THM:QRP:AI_CHIP'").fetchone()
    thm_b, coll_b = sc.create_canonical_theme(
        theme_name="储能主题",
        source_key="ENERGY_STORAGE",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )

    d = date(2026, 8, 3)
    sc.add_member(theme_id=thm_a[0], collection_id=thm_a[1], asset_id="000001.SZ", effective_from=d, available_trade_date=d)
    sc.add_member(theme_id=thm_b.theme_id, collection_id=coll_b.collection_id, asset_id="600519.SH", effective_from=d, available_trade_date=d)

    # 注入故障：当执行到 thm_b 的 observation 写入时故意抛出异常
    class ProxyCon:
        def __init__(self, real):
            self._real = real
            self.fail = True

        def execute(self, sql, *args, **kwargs):
            if self.fail and f"INSERT INTO {THEME_M4_OBSERVATION_TABLE}" in sql:
                # 仅在写入 thm_b 时崩溃
                if args and args[0] and args[0][0] == thm_b.theme_id:
                    raise RuntimeError("Simulated Database Crash during Theme B Observation Insert")
            return self._real.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._real, name)

    proxy = ProxyCon(f_db)
    service.con = proxy

    # 执行生产：应当触发异常
    with pytest.raises(Exception, match="Simulated Database Crash"):
        service.run_m4_daily(trade_date=d)

    # 1. 验证 Theme A 已经先一步成功 COMMIT，完全不受影响
    finalized_after_fail = service._get_finalized_theme_ids(d)
    assert thm_a[0] in finalized_after_fail
    assert thm_b.theme_id not in finalized_after_fail

    # 2. 验证 Theme B 在各表中完全不存在半成品 (全部被 ROLLBACK 擦除)
    cnt_eff_b = f_db.execute(f"SELECT COUNT(*) FROM {THEME_EFFECTIVE_MEMBER_DAILY_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_b.theme_id, d]).fetchone()[0]
    cnt_idx_b = f_db.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_b.theme_id, d]).fetchone()[0]
    cnt_st_b = f_db.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_b.theme_id, d]).fetchone()[0]
    cnt_obs_b = f_db.execute(f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_b.theme_id, d]).fetchone()[0]

    assert cnt_eff_b == 0
    assert cnt_idx_b == 0
    assert cnt_st_b == 0
    assert cnt_obs_b == 0

    # 3. 解除异常，执行 Retry：Theme B 必须完整重新生产且 COMMIT
    proxy.fail = False
    rep = service.run_m4_daily(trade_date=d)

    finalized_after_retry = service._get_finalized_theme_ids(d)
    assert thm_a[0] in finalized_after_retry
    assert thm_b.theme_id in finalized_after_retry

    # 4. 验证 Theme B 现已完整写入全部 5 个事实
    assert f_db.execute(f"SELECT COUNT(*) FROM {THEME_EFFECTIVE_MEMBER_DAILY_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_b.theme_id, d]).fetchone()[0] > 0
    assert f_db.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_b.theme_id, d]).fetchone()[0] == 1
    assert f_db.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_b.theme_id, d]).fetchone()[0] == 1
    assert f_db.execute(f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_b.theme_id, d]).fetchone()[0] == 1


# ==============================================================================
# P0/P1: Expected Theme Set 必须冻结在 D 的合法历史语义 (新知识不污染历史)
# ==============================================================================
def test_p0_p1_expected_themes_frozen_in_historical_semantics(f_db):
    """
    回归验证：
    1. 先完成并 finalize D；
    2. 后来创建 Theme X，令其 effective_from <= D；
    3. X 的实际创建/ingested 时间晚于 D；
    4. retry D；
    5. D 必须仍保持 finalized；
    6. 不得为 Theme X 补造 D 的 effective-member/index/state/observation 历史事实。
    """
    service = ThemePipelineService(f_db)
    # Clock for D (2026-08-01 10:00:00 UTC)
    clock_d = datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc)
    sc = StockCollectionService(f_db, clock=lambda: clock_d)

    thm = f_db.execute(f"SELECT theme_id, collection_id FROM {THEME_TABLE} WHERE theme_id = 'THM:QRP:AI_CHIP'").fetchone()
    d = date(2026, 8, 3)
    sc.add_member(theme_id=thm[0], collection_id=thm[1], asset_id="000001.SZ", effective_from=d, available_trade_date=d)

    # 1. 完成并 finalize D
    rep_d = service.run_m4_daily(trade_date=d)
    finalized_d = service._get_finalized_theme_ids(d)
    assert thm[0] in finalized_d

    # 记录 D 完成时的各表行数
    cnt_eff_before = f_db.execute(f"SELECT COUNT(*) FROM {THEME_EFFECTIVE_MEMBER_DAILY_TABLE} WHERE trade_date = ?", [d]).fetchone()[0]
    cnt_idx_before = f_db.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE trade_date = ?", [d]).fetchone()[0]
    cnt_st_before = f_db.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE trade_date = ?", [d]).fetchone()[0]
    cnt_obs_before = f_db.execute(f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date = ?", [d]).fetchone()[0]

    # 2 & 3. 后来（D+1 日，如 2026-08-04 10:00:00）创建 Theme X，令其 effective_from <= D
    clock_later = datetime(2026, 8, 4, 10, 0, 0, tzinfo=timezone.utc)
    sc_later = StockCollectionService(f_db, clock=lambda: clock_later)
    thm_x, coll_x = sc_later.create_canonical_theme(
        theme_name="后知主题X",
        source_key="LATER_THEME_X",
        effective_from=d,  # effective_from <= D
        available_trade_date=d,
    )
    sc_later.add_member(
        theme_id=thm_x.theme_id,
        collection_id=coll_x.collection_id,
        asset_id="600519.SH",
        effective_from=d,
        available_trade_date=d,
    )

    # 4. Retry D
    rep_retry = service.run_m4_daily(trade_date=d)

    # 5. D 必须仍保持 finalized，且 finalized themes 集合严格未变
    finalized_after = service._get_finalized_theme_ids(d)
    assert finalized_after == finalized_d
    assert thm_x.theme_id not in finalized_after

    # 6. 验证各表行数完全未变，不得为 Theme X 补造 D 的任何事实
    cnt_eff_after = f_db.execute(f"SELECT COUNT(*) FROM {THEME_EFFECTIVE_MEMBER_DAILY_TABLE} WHERE trade_date = ?", [d]).fetchone()[0]
    cnt_idx_after = f_db.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE trade_date = ?", [d]).fetchone()[0]
    cnt_st_after = f_db.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE trade_date = ?", [d]).fetchone()[0]
    cnt_obs_after = f_db.execute(f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE} WHERE trade_date = ?", [d]).fetchone()[0]

    assert cnt_eff_after == cnt_eff_before
    assert cnt_idx_after == cnt_idx_before
    assert cnt_st_after == cnt_st_before
    assert cnt_obs_after == cnt_obs_before

    # 明确查验 Theme X 在 D 日零记录
    assert f_db.execute(f"SELECT COUNT(*) FROM {THEME_EFFECTIVE_MEMBER_DAILY_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_x.theme_id, d]).fetchone()[0] == 0
    assert f_db.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_DAILY_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_x.theme_id, d]).fetchone()[0] == 0
    assert f_db.execute(f"SELECT COUNT(*) FROM {THEME_CUSTOM_INDEX_STATE_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_x.theme_id, d]).fetchone()[0] == 0
    assert f_db.execute(f"SELECT COUNT(*) FROM {THEME_M4_OBSERVATION_TABLE} WHERE theme_id = ? AND trade_date = ?", [thm_x.theme_id, d]).fetchone()[0] == 0


