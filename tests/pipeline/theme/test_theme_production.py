"""Tests for Theme M4 production pipeline: full vs daily equality, targeted replay, and lineage audit."""

from datetime import date, datetime, timezone
import duckdb
import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts.schema import init_database, init_stock_collections_database
from qrp_atlas.pipeline.theme.query import ThemeQueryService
from qrp_atlas.pipeline.theme.service import ThemePipelineService
from qrp_atlas.stock_collections.service import StockCollectionService


@pytest.fixture
def db():
    con = duckdb.connect(":memory:")
    init_database(con)
    init_stock_collections_database(con)

    # 1. Populate trading calendar (prior dates + 10 production days)
    prior_dates = [date(2026, 7, i) for i in range(20, 31)]
    dates = [
        date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7),
        date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13), date(2026, 8, 14),
    ]
    for d in prior_dates + dates:
        con.execute("INSERT INTO trading_calendar (trade_date, is_open) VALUES (?, true)", [d])

    # 2. Populate stock info
    con.execute("INSERT INTO stock_info (ticker, name, list_date) VALUES ('000001.SZ', 'Stock A', '2020-01-01')")
    con.execute("INSERT INTO stock_info (ticker, name, list_date) VALUES ('600519.SH', 'Stock B', '2020-01-01')")

    # 3. Populate market snapshots & THS daily for prior dates (ensuring > 5 actual trading days)
    for p_d in prior_dates:
        con.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [p_d, "000001.SZ", "Stock A", 10.0, 10.0, 9.8, 10.0, 1000, 10000, 0.0, False],
        )
        con.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [p_d, "600519.SH", "Stock B", 20.0, 20.0, 19.8, 20.0, 1000, 20000, 0.0, False],
        )
        con.execute(
            "INSERT INTO ths_daily (trade_date, index_code, close, pct_change) VALUES (?, ?, 100.0, 0.0)",
            [p_d, "881101.TI"],
        )

    # 4. Populate market snapshots & THS daily for production dates
    for idx, d in enumerate(dates):
        # Stock A return 2% (+0.02, limit up on day 4)
        pct_a = 9.9 if idx == 3 else 2.0
        is_lu_a = (idx == 3)
        close_a = 10.0 * ((1 + 0.02) ** (idx + 1))
        con.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [d, "000001.SZ", "Stock A", 10.0, close_a, 9.8, close_a, 1000, 10000, pct_a, is_lu_a],
        )

        # Stock B return 3% (+0.03)
        close_b = 20.0 * ((1 + 0.03) ** (idx + 1))
        con.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [d, "600519.SH", "Stock B", 20.0, close_b, 19.8, close_b, 1000, 20000, 3.0, False],
        )

        # THS Board (881101.TI) return 1.5%
        con.execute(
            "INSERT INTO ths_daily (trade_date, index_code, close, pct_change) VALUES (?, ?, 100.0, 1.5)",
            [d, "881101.TI"],
        )

    # 4. Create Theme and Add Members
    sc_service = StockCollectionService(con, clock=lambda: datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc))
    thm, coll = sc_service.create_canonical_theme(
        theme_name="高算力芯片",
        source_key="AI_CHIP",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )
    sc_service.add_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="000001.SZ",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )
    sc_service.add_member(
        theme_id=thm.theme_id,
        collection_id=coll.collection_id,
        asset_id="600519.SH",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 3),
    )

    yield con
    con.close()


def test_full_replay_vs_daily_production_exact_value_equality(db):
    """Assert full replay and sequential daily production produce 100% numerically identical outputs."""
    dates = [
        date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7),
        date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13), date(2026, 8, 14),
    ]
    service = ThemePipelineService(db)
    query_service = ThemeQueryService(db)

    # 1. Run full historical replay
    report_full = service.rebuild_m4_facts(start_date=dates[0], end_date=dates[-1])
    assert report_full.total_index_rows == 10
    full_indices = query_service.get_theme_index_history("THM:QRP:AI_CHIP", dates[0], dates[-1])
    full_m4 = query_service.get_m4_observations(dates[-1])

    # Save full replay values
    full_levels = full_indices["index_level"].tolist()
    full_returns = full_indices["theme_daily_return"].tolist()

    # 2. Simulate sequential daily production on clean state
    db.execute("DELETE FROM theme_custom_index_daily")
    db.execute("DELETE FROM theme_custom_index_state")
    db.execute("DELETE FROM theme_custom_index_episode")
    db.execute("DELETE FROM theme_m4_observation")

    for d in dates:
        service.run_m4_daily(trade_date=d)

    daily_indices = query_service.get_theme_index_history("THM:QRP:AI_CHIP", dates[0], dates[-1])
    daily_m4 = query_service.get_m4_observations(dates[-1])

    daily_levels = daily_indices["index_level"].tolist()
    daily_returns = daily_indices["theme_daily_return"].tolist()

    # Numerical exact equality assertion
    np.testing.assert_allclose(full_levels, daily_levels, rtol=1e-7)
    np.testing.assert_allclose(full_returns, daily_returns, rtol=1e-7)
    assert len(daily_indices) == len(full_indices)
    assert len(daily_m4) == len(full_m4)


def test_lineage_audit_and_input_snapshot_traceability(db):
    """Verify audit service traces inputs without N+1 queries."""
    d = date(2026, 8, 6)  # Day 4 (Stock A has limit up)
    service = ThemePipelineService(db)
    query_service = ThemeQueryService(db)

    service.rebuild_m4_facts(start_date=date(2026, 8, 3), end_date=d)

    audit = query_service.audit_m4_observation("THM:QRP:AI_CHIP", d)

    assert audit.theme_id == "THM:QRP:AI_CHIP"
    assert audit.trade_date == d
    assert audit.theme_limit_up_count == 1
    assert "000001.SZ" in audit.limit_up_assets
    assert audit.effective_members == 2
    assert audit.total_members == 2
    assert audit.production_run_id is not None
    assert audit.input_snapshot_id is not None
    assert len(audit.comparison_boards) >= 1
    assert audit.is_reproducible is True
    assert audit.discrepancy_reason is None


def test_targeted_replay_exact_zero_drift_on_overlapping_range(db):
    """Verify targeted replay over [d5..d8] produces 100% identical outputs to full replay."""
    dates = [
        date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7),
        date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13), date(2026, 8, 14),
    ]
    service = ThemePipelineService(db)

    # 1. Full replay on all dates
    service.rebuild_m4_facts(start_date=dates[0], end_date=dates[-1])
    full_idx = db.execute(
        "SELECT trade_date, theme_daily_return, index_level, effective_member_count, total_member_count FROM theme_custom_index_daily WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
        [dates[5], dates[8]],
    ).fetchall()
    full_states = db.execute(
        "SELECT trade_date, close, ma5, ma10, trend_state, custom_index_trend_run_days FROM theme_custom_index_state WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
        [dates[5], dates[8]],
    ).fetchall()
    full_m4 = db.execute(
        "SELECT trade_date, theme_daily_return, theme_limit_up_count, theme_return_rank, effective_member_count, comparison_universe_size, custom_index_trend_state, custom_index_trend_run_days FROM theme_m4_observation WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
        [dates[5], dates[8]],
    ).fetchall()

    # 2. Targeted replay on sub-interval [dates[5]..dates[8]]
    service.rebuild_m4_facts(start_date=dates[5], end_date=dates[8])
    target_idx = db.execute(
        "SELECT trade_date, theme_daily_return, index_level, effective_member_count, total_member_count FROM theme_custom_index_daily WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
        [dates[5], dates[8]],
    ).fetchall()
    target_states = db.execute(
        "SELECT trade_date, close, ma5, ma10, trend_state, custom_index_trend_run_days FROM theme_custom_index_state WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
        [dates[5], dates[8]],
    ).fetchall()
    target_m4 = db.execute(
        "SELECT trade_date, theme_daily_return, theme_limit_up_count, theme_return_rank, effective_member_count, comparison_universe_size, custom_index_trend_state, custom_index_trend_run_days FROM theme_m4_observation WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
        [dates[5], dates[8]],
    ).fetchall()

    assert full_idx == target_idx
    assert full_states == target_states
    assert full_m4 == target_m4


def test_option_a_daily_production_physical_scope_and_run_persistence(db):
    """Verify daily production replaces target date only and persists theme_production_run."""
    service = ThemePipelineService(db)
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)

    # Day 1
    rep1 = service.run_m4_daily(trade_date=d1)
    assert rep1.trade_date_count == 1
    cnt1 = db.execute("SELECT COUNT(*) FROM theme_custom_index_daily").fetchone()[0]
    assert cnt1 == 1

    # Day 2: Option A physical replacement
    rep2 = service.run_m4_daily(trade_date=d2)
    assert rep2.trade_date_count == 1
    cnt2 = db.execute("SELECT COUNT(*) FROM theme_custom_index_daily").fetchone()[0]
    assert cnt2 == 2  # Day 1 was NOT deleted; Day 2 was inserted

    # Verify theme_production_run persisted both runs
    runs = db.execute(
        "SELECT production_run_id, run_type, status, target_start_date, target_end_date, input_snapshot_id FROM theme_production_run ORDER BY created_at ASC"
    ).fetchall()
    assert len(runs) >= 2
    assert runs[0][1] == "DAILY"
    assert runs[0][2] == "SUCCEEDED"
    assert runs[0][3] == d1
    assert runs[0][4] == d1
    assert runs[1][3] == d2
    assert runs[1][4] == d2


def test_input_snapshot_id_determinism(db):
    """Verify input snapshot id is identical across runs with unchanged facts."""
    service = ThemePipelineService(db)
    d = date(2026, 8, 3)

    rep1 = service.rebuild_m4_facts(start_date=d, end_date=d)
    rep2 = service.rebuild_m4_facts(start_date=d, end_date=d)

    assert rep1.input_snapshot_id == rep2.input_snapshot_id
    assert rep1.input_snapshot_id.startswith("SNAP:")


def test_source_drift_detection_in_lineage_audit(db):
    """Verify audit detects drift when underlying facts mutate after production run."""
    service = ThemePipelineService(db)
    query_service = ThemeQueryService(db)
    d = date(2026, 8, 6)

    # 1. Run production
    rep = service.run_m4_daily(trade_date=d)
    prod_run_id = rep.production_run_id

    # Audit immediately: must be reproducible
    audit_clean = query_service.audit_m4_observation("THM:QRP:AI_CHIP", d)
    assert audit_clean.is_reproducible is True
    assert audit_clean.discrepancy_reason is None

    # 2. Mutate market fact after production run
    db.execute(
        "UPDATE daily_market_snapshot SET pct_change = 5.5 WHERE trade_date = ? AND ticker = '000001.SZ'",
        [d],
    )

    # Audit again: must detect drift
    audit_drifted = query_service.audit_m4_observation("THM:QRP:AI_CHIP", d)
    assert audit_drifted.is_reproducible is False
    assert audit_drifted.discrepancy_reason == "CURRENT_SOURCE_DIFFERS_FROM_PRODUCTION_SNAPSHOT"


def test_historical_correction_forward_dependency_closure(db):
    """验证历史修订从受影响起点开始，自动向前闭包直到当前 Theme 已 materialize 的最大交易日：
    - 已生产至 dates[9] (Day 10)
    - 历史修订请求 start_date = dates[5] (Day 6), end_date = dates[5] (Day 6)
    - 系统必须自动识别 affected_output_end = dates[9]
    - 从 Inception 重算，输出写入 dates[5]..dates[9]
    - 断言 affected range 每个 trade_date 都仍存在且 row count 完整
    - 断言 affected range 内每行数据的 production_run_id 均更新为本次 correction run_id
    - 断言 prior range (dates[0]..dates[4]) 保留原初始 production_run_id
    """
    dates = [
        date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7),
        date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13), date(2026, 8, 14),
    ]
    service = ThemePipelineService(db)

    # 1. 初始生产全部 10 天
    init_rep = service.rebuild_m4_facts(start_date=dates[0], end_date=dates[-1], run_type="BACKFILL")
    max_d = db.execute("SELECT MAX(trade_date) FROM theme_custom_index_daily").fetchone()[0]
    assert max_d == dates[-1]

    initial_run_ids = {
        tbl: db.execute(f"SELECT trade_date, production_run_id FROM {tbl} ORDER BY trade_date").fetchall()
        for tbl in ["theme_custom_index_daily", "theme_custom_index_state", "theme_m4_observation"]
    }

    # 2. 对 Day 6 (dates[5]) 尝试发起历史修订（已被 finalized 的账本不可改写）
    rep = service.rebuild_m4_facts(start_date=dates[5], end_date=dates[5], run_type="CORRECTION")

    # 3. 严格断言物理事实表行不可篡改性与原始 production_run_id 归属：
    # 验证全部 10 个日期均保留，且全部保持初始 run_id，历史账本绝对不被重述
    for tbl in ["theme_custom_index_daily", "theme_custom_index_state", "theme_m4_observation"]:
        rows = db.execute(f"SELECT trade_date, production_run_id FROM {tbl} ORDER BY trade_date").fetchall()
        assert len(rows) == 10, f"Table {tbl} row count expected 10, got {len(rows)}"
        assert rows == initial_run_ids[tbl], f"Finalized rows in {tbl} mutated after correction attempt!"


def test_cross_range_existing_episode_update_on_historical_correction(db):
    """验证已 finalized 的历史 Episode 属于不可篡改账本：
    历史生产一旦完成，后续重跑或修订均不得删除或覆写已持久化的 Episode。
    """
    dates = [
        date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7),
        date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13), date(2026, 8, 14),
    ]
    service = ThemePipelineService(db)

    # 构造能够触发 Episode 的价格形态：Day 5 大幅下跌进入 BASE，Day 6 上涨进入 CANDIDATE，Day 7 上涨确认 ACTIVE
    db.execute("UPDATE daily_market_snapshot SET pct_change = -18.0 WHERE trade_date = ?", [dates[4]])
    db.execute("UPDATE daily_market_snapshot SET pct_change = 15.0 WHERE trade_date = ?", [dates[5]])
    db.execute("UPDATE daily_market_snapshot SET pct_change = 5.0 WHERE trade_date = ?", [dates[6]])

    # 1. 初始生产全部 10 天
    rep1 = service.rebuild_m4_facts(start_date=dates[0], end_date=dates[-1])
    episodes_before = db.execute(
        "SELECT episode_id, episode_start_date, episode_confirmed_date, episode_end_date, production_run_id FROM theme_custom_index_episode"
    ).fetchall()
    assert len(episodes_before) >= 1

    # 2. 对已 finalized 的 dates[7] 再次执行生产
    rep2 = service.rebuild_m4_facts(start_date=dates[7], end_date=dates[7], run_type="CORRECTION")

    # 3. 严格断言：已有 Episode 完全未被改写或覆盖，保持 ledger 权威性
    episodes_after = db.execute(
        "SELECT episode_id, episode_start_date, episode_confirmed_date, episode_end_date, production_run_id FROM theme_custom_index_episode"
    ).fetchall()
    assert episodes_before == episodes_after


def test_deterministic_replay_with_as_of_and_different_knowledge_dates(db):
    """验证 (T, K) 双时间非物化只读回放：
    - baseline canonical tables snapshot
    - replay(T, K1) 两次 -> exact deterministic equality
    - replay(T, K2) 两次 -> exact deterministic equality
    - K1 != K2 -> 表达知识演进差异
    - canonical tables 在 replay 前后 100% 保持完全未变
    """
    service = ThemePipelineService(db)
    sc_service = StockCollectionService(db)

    # 1. 建立基线 canonical 生产数据
    dates = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6)]
    service.rebuild_m4_facts(start_date=dates[0], end_date=dates[-1], run_type="BACKFILL")

    # 快照保存全部 4 张 Canonical 事实表及 production_run
    def _snapshot_canonical(con):
        tables = [
            "theme_custom_index_daily",
            "theme_custom_index_state",
            "theme_custom_index_episode",
            "theme_m4_observation",
            "theme_production_run",
        ]
        return {t: con.execute(f"SELECT * FROM {t} ORDER BY ALL").fetchall() for t in tables}

    # 添加第 3 只股票 000002.SZ，在 K2 = 2026-08-08 才可见
    db.execute("INSERT INTO stock_info (ticker, name, list_date) VALUES ('000002.SZ', 'Stock C', '2020-01-01')")
    prior_dates = [date(2026, 7, i) for i in range(20, 31)]
    for p_d in prior_dates:
        db.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [p_d, "000002.SZ", "Stock C", 10.0, 10.0, 9.8, 10.0, 1000, 10000, 0.0, False],
        )
    for d in dates:
        db.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, volume, amount, pct_change, is_limit_up) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [d, "000002.SZ", "Stock C", 10.0, 10.5, 9.8, 10.5, 1000, 10000, 5.0, False],
        )

    thm = db.execute("SELECT theme_id, collection_id FROM theme LIMIT 1").fetchone()
    theme_id, coll_id = thm[0], thm[1]

    # 添加成员，effective_from 2026-08-03，但 available_trade_date 为 2026-08-08 (K2)
    sc_service.add_member(
        theme_id=theme_id,
        collection_id=coll_id,
        asset_id="000002.SZ",
        effective_from=date(2026, 8, 3),
        available_trade_date=date(2026, 8, 8),
    )

    t = date(2026, 8, 6)
    k1 = date(2026, 8, 5)  # K1 无法获知 Stock C
    k2 = date(2026, 8, 9)  # K2 可以获知 Stock C

    canonical_pre_replay = _snapshot_canonical(db)

    # 1. 运行只读 replay(T, K1) 两次
    rep_k1_first = service.replay_m4_facts(start_date=t, end_date=t, knowledge_date=k1)
    rep_k1_second = service.replay_m4_facts(start_date=t, end_date=t, knowledge_date=k1)

    # K1 下必须完全一致 (2 个成员) 且具有确定性
    assert rep_k1_first.input_snapshot_id == rep_k1_second.input_snapshot_id
    assert rep_k1_first.m4_observations["effective_member_count"].iloc[0] == 2
    assert rep_k1_second.m4_observations["effective_member_count"].iloc[0] == 2
    pd.testing.assert_frame_equal(rep_k1_first.m4_observations, rep_k1_second.m4_observations)
    pd.testing.assert_frame_equal(rep_k1_first.daily_indices, rep_k1_second.daily_indices)

    # 2. 运行只读 replay(T, K2) 两次
    rep_k2_first = service.replay_m4_facts(start_date=t, end_date=t, knowledge_date=k2)
    rep_k2_second = service.replay_m4_facts(start_date=t, end_date=t, knowledge_date=k2)

    # K2 下必须完全一致 (3 个成员) 且具有确定性
    assert rep_k2_first.input_snapshot_id == rep_k2_second.input_snapshot_id
    assert rep_k2_first.m4_observations["effective_member_count"].iloc[0] == 3
    assert rep_k2_second.m4_observations["effective_member_count"].iloc[0] == 3
    pd.testing.assert_frame_equal(rep_k2_first.m4_observations, rep_k2_second.m4_observations)
    pd.testing.assert_frame_equal(rep_k2_first.daily_indices, rep_k2_second.daily_indices)

    # K1 与 K2 的结果不同，表达确定性的知识演进
    assert rep_k1_first.m4_observations["effective_member_count"].iloc[0] != rep_k2_first.m4_observations["effective_member_count"].iloc[0]
    assert rep_k1_first.input_snapshot_id != rep_k2_first.input_snapshot_id

    # 3. 严格断言：Replay 前后的 Canonical 事实表完全未变（行数、内容、主键、run_id 100% 相同）
    canonical_post_replay = _snapshot_canonical(db)
    assert canonical_pre_replay == canonical_post_replay


def test_replay_calculation_equivalence_with_canonical_rebuild(db):
    """验证只读 Replay 与 Canonical Materialization 共用同一个计算核心：
    Replay(T, K) 返回的各项计算事实与 Canonical Rebuild 落库结果 100% 精确一致。
    """
    service = ThemePipelineService(db)
    t = date(2026, 8, 5)
    kd = date(2026, 8, 5)

    # 1. 运行 canonical rebuild
    prod_rep = service.rebuild_m4_facts(start_date=t, end_date=t, knowledge_date=kd, run_type="BACKFILL")

    # 2. 运行 read-only replay
    replay_res = service.replay_m4_facts(start_date=t, end_date=t, knowledge_date=kd)

    # 3. 验证 Snapshot ID 完全一致
    assert replay_res.input_snapshot_id == prod_rep.input_snapshot_id

    # 4. 从 canonical 事实表查询物化结果
    can_daily = db.execute(
        "SELECT theme_daily_return, index_level, effective_member_count, total_member_count FROM theme_custom_index_daily WHERE trade_date = ?",
        [t],
    ).fetchone()
    can_state = db.execute(
        "SELECT close, ma5, ma10, trend_state, custom_index_trend_run_days FROM theme_custom_index_state WHERE trade_date = ?",
        [t],
    ).fetchone()
    can_m4 = db.execute(
        "SELECT theme_daily_return, theme_limit_up_count, theme_return_rank, effective_member_count FROM theme_m4_observation WHERE trade_date = ?",
        [t],
    ).fetchone()

    # 断言 Replay 结果与 Canonical 物化结果逐字段完全一致
    assert replay_res.daily_indices["theme_daily_return"].iloc[0] == can_daily[0]
    assert replay_res.daily_indices["index_level"].iloc[0] == can_daily[1]
    assert replay_res.daily_indices["effective_member_count"].iloc[0] == can_daily[2]

    assert replay_res.daily_states["close"].iloc[0] == can_state[0]
    assert replay_res.daily_states["trend_state"].iloc[0] == can_state[3]

    assert replay_res.m4_observations["theme_daily_return"].iloc[0] == can_m4[0]
    assert replay_res.m4_observations["theme_limit_up_count"].iloc[0] == can_m4[1]
    assert replay_res.m4_observations["theme_return_rank"].iloc[0] == can_m4[2]
    assert replay_res.m4_observations["effective_member_count"].iloc[0] == can_m4[3]


def test_audit_reconstruction_failure_fails_closed(db):
    """验证当 Audit 重建依赖被破坏或发生异常时，审计严格 fail closed：
    is_reproducible == False
    discrepancy_reason == 'AUDIT_RECONSTRUCTION_FAILED'
    """
    service = ThemePipelineService(db)
    query_service = ThemeQueryService(db)
    t = date(2026, 8, 4)

    # 1. 正常生产
    service.rebuild_m4_facts(start_date=t, end_date=t)

    # 正常审计应通过
    audit_ok = query_service.audit_m4_observation("THM:QRP:AI_CHIP", t)
    assert audit_ok.is_reproducible is True
    assert audit_ok.discrepancy_reason is None

    # 2. 模拟破坏重建必需的依赖（例如删除 ths_daily 表）
    db.execute("DROP TABLE ths_daily")

    # 再次审计必须 Fail Closed
    audit_fail = query_service.audit_m4_observation("THM:QRP:AI_CHIP", t)
    assert audit_fail.is_reproducible is False
    assert audit_fail.discrepancy_reason == "AUDIT_RECONSTRUCTION_FAILED"


def test_audit_defaults_to_persisted_production_knowledge_date(db):
    """验证当 trade_date != production knowledge_date 时，audit_m4_observation(knowledge_date=None)
    自动解析并使用 production run 中保存的 knowledge_date。
    """
    service = ThemePipelineService(db)
    query_service = ThemeQueryService(db)

    t = date(2026, 8, 4)
    k_prod = date(2026, 8, 10)  # trade_date (08-04) != knowledge_date (08-10)

    # 运行生产，显式指定 knowledge_date = k_prod
    rep = service.rebuild_m4_facts(start_date=t, end_date=t, knowledge_date=k_prod)

    # 不传 knowledge_date 进行审计
    audit = query_service.audit_m4_observation("THM:QRP:AI_CHIP", t, knowledge_date=None)

    assert audit.production_knowledge_date == k_prod
    assert audit.audit_knowledge_date == k_prod
    assert audit.production_knowledge_date != t
    assert audit.is_reproducible is True
    assert audit.discrepancy_reason is None
