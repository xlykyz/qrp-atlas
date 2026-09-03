"""Tests for M4 raw observation calculations, ranking, and universe fail-closed semantics."""

from datetime import date
import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import ASSET_ID, IS_LIMIT_UP, TRADE_DATE
from qrp_atlas.contracts.m4 import (
    COMPARISON_UNIVERSE_SIZE,
    COMPARISON_UNIVERSE_VERSION,
    EFFECTIVE_MEMBER_COUNT,
    IS_M4_EFFECTIVE_MEMBER,
    QUALIFICATION_STATUS,
    THEME_DAILY_RETURN,
    THEME_LIMIT_UP_COUNT,
    THEME_RETURN_RANK,
)
from qrp_atlas.contracts.stock_collection import COLLECTION_ID
from qrp_atlas.indicators.m4.observations import (
    M4ObservationError,
    calculate_m4_raw_observations,
)


def test_m4_observations_ranking_and_missing_universe_fail_closed():
    d1 = date(2026, 8, 3)

    theme_index_daily = pd.DataFrame(
        [
            {COLLECTION_ID: "COLL_A", TRADE_DATE: d1, THEME_DAILY_RETURN: 0.05, "effective_member_count": 2, "total_member_count": 2},
            {COLLECTION_ID: "COLL_B", TRADE_DATE: d1, THEME_DAILY_RETURN: 0.02, "effective_member_count": 2, "total_member_count": 2},
        ]
    )

    effective_members = pd.DataFrame(
        [
            {COLLECTION_ID: "COLL_A", ASSET_ID: "S1", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
            {COLLECTION_ID: "COLL_A", ASSET_ID: "S2", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
            {COLLECTION_ID: "COLL_B", ASSET_ID: "S3", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
            {COLLECTION_ID: "COLL_B", ASSET_ID: "S4", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
        ]
    )

    market_snapshot = pd.DataFrame(
        [
            {ASSET_ID: "S1", TRADE_DATE: d1, IS_LIMIT_UP: True},
            {ASSET_ID: "S2", TRADE_DATE: d1, IS_LIMIT_UP: False},
            {ASSET_ID: "S3", TRADE_DATE: d1, IS_LIMIT_UP: True},
            {ASSET_ID: "S4", TRADE_DATE: d1, IS_LIMIT_UP: True},
        ]
    )

    # 1. Missing comparison boards fails closed
    with pytest.raises(M4ObservationError, match="MISSING_COMPARISON_UNIVERSE"):
        calculate_m4_raw_observations(
            theme_index_daily, effective_members, market_snapshot, pd.DataFrame()
        )

    # 2. Valid comparison boards: e.g. THS boards with returns 0.08, 0.03, 0.01
    comparison_boards = pd.DataFrame(
        [
            {"board_id": "881101.TI", TRADE_DATE: d1, "board_return": 0.08},
            {"board_id": "885750.TI", TRADE_DATE: d1, "board_return": 0.03},
            {"board_id": "886001.TI", TRADE_DATE: d1, "board_return": 0.01},
        ]
    )

    obs = calculate_m4_raw_observations(
        theme_index_daily, effective_members, market_snapshot, comparison_boards
    )

    # Returns across universe: 0.08, 0.05 (COLL_A), 0.03, 0.02 (COLL_B), 0.01
    # Total size = 3 boards + 2 themes = 5
    r_a = obs[obs[COLLECTION_ID] == "COLL_A"].iloc[0]
    r_b = obs[obs[COLLECTION_ID] == "COLL_B"].iloc[0]

    assert r_a[THEME_LIMIT_UP_COUNT] == 1
    assert r_a[THEME_RETURN_RANK] == 2  # Behind 0.08
    assert r_a[COMPARISON_UNIVERSE_SIZE] == 5
    assert r_a[QUALIFICATION_STATUS] == "NOT_CONFIGURED"

    assert r_b[THEME_LIMIT_UP_COUNT] == 2
    assert r_b[THEME_RETURN_RANK] == 4  # Behind 0.08, 0.05, 0.03


def test_m4_limit_up_uses_official_field_avoiding_heuristics():
    """验证 M4 涨停数统计严格直接使用 is_limit_up，规避 pct_change 阈值误判：
    - 10% 涨停 (is_limit_up=True)
    - 20% 涨停 (is_limit_up=True)
    - ST 5% 涨停 (is_limit_up=True)
    - 9.8% 冲高但未封板 (is_limit_up=False) -> 绝不能被误判为涨停！
    """
    d1 = date(2026, 8, 3)
    theme_index_daily = pd.DataFrame([
        {COLLECTION_ID: "COLL_T", TRADE_DATE: d1, THEME_DAILY_RETURN: 0.06},
    ])
    effective_members = pd.DataFrame([
        {COLLECTION_ID: "COLL_T", ASSET_ID: "MAIN_10", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
        {COLLECTION_ID: "COLL_T", ASSET_ID: "CHINEXT_20", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
        {COLLECTION_ID: "COLL_T", ASSET_ID: "ST_5", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
        {COLLECTION_ID: "COLL_T", ASSET_ID: "NOT_LIMIT_98", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
    ])
    # 模拟真实市场快照：NOT_LIMIT_98 虽然日收益高，但并未封涨停
    market_snapshot = pd.DataFrame([
        {ASSET_ID: "MAIN_10", TRADE_DATE: d1, IS_LIMIT_UP: True, "pct_change": 10.0},
        {ASSET_ID: "CHINEXT_20", TRADE_DATE: d1, IS_LIMIT_UP: True, "pct_change": 20.0},
        {ASSET_ID: "ST_5", TRADE_DATE: d1, IS_LIMIT_UP: True, "pct_change": 5.0},
        {ASSET_ID: "NOT_LIMIT_98", TRADE_DATE: d1, IS_LIMIT_UP: False, "pct_change": 9.85},
    ])
    comparison_boards = pd.DataFrame([
        {"board_id": "881101.TI", TRADE_DATE: d1, "board_return": 0.02},
    ])

    obs = calculate_m4_raw_observations(
        theme_index_daily, effective_members, market_snapshot, comparison_boards
    )
    row = obs.iloc[0]
    # 只有前3个封板股票计入涨停数，9.85%未封板的绝不计入
    assert row[THEME_LIMIT_UP_COUNT] == 3


def test_m4_comparison_universe_missing_date_fails_closed():
    """验证当某日对比板块数据缺失时，必须 fail-closed 报错，不得隐式忽略或伪造 rank。"""
    d1 = date(2026, 8, 3)
    d2 = date(2026, 8, 4)
    theme_index_daily = pd.DataFrame([
        {COLLECTION_ID: "COLL_T", TRADE_DATE: d1, THEME_DAILY_RETURN: 0.02},
        {COLLECTION_ID: "COLL_T", TRADE_DATE: d2, THEME_DAILY_RETURN: 0.03},
    ])
    effective_members = pd.DataFrame([
        {COLLECTION_ID: "COLL_T", ASSET_ID: "S1", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
        {COLLECTION_ID: "COLL_T", ASSET_ID: "S1", TRADE_DATE: d2, IS_M4_EFFECTIVE_MEMBER: True},
    ])
    market_snapshot = pd.DataFrame([
        {ASSET_ID: "S1", TRADE_DATE: d1, IS_LIMIT_UP: False},
        {ASSET_ID: "S1", TRADE_DATE: d2, IS_LIMIT_UP: False},
    ])
    # 对比板块只有 d1，没有 d2
    comparison_boards = pd.DataFrame([
        {"board_id": "881101.TI", TRADE_DATE: d1, "board_return": 0.01},
    ])

    with pytest.raises(M4ObservationError, match="COMPARISON_UNIVERSE_DATE_MISSING"):
        calculate_m4_raw_observations(
            theme_index_daily, effective_members, market_snapshot, comparison_boards
        )


def test_m4_comparison_universe_standard_competition_ranking():
    """验证标准竞争排序 (Standard Competition Ranking, 1224)：
    两个板块同为 0.08，theme 为 0.05 -> theme 的排名应为 3 (而非 dense rank 的 2)。
    """
    d1 = date(2026, 8, 3)
    theme_index_daily = pd.DataFrame([
        {COLLECTION_ID: "COLL_T", TRADE_DATE: d1, THEME_DAILY_RETURN: 0.05},
    ])
    effective_members = pd.DataFrame([
        {COLLECTION_ID: "COLL_T", ASSET_ID: "S1", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
    ])
    market_snapshot = pd.DataFrame([
        {ASSET_ID: "S1", TRADE_DATE: d1, IS_LIMIT_UP: False},
    ])
    # 两个对比板块收益并列 0.08
    comparison_boards = pd.DataFrame([
        {"board_id": "881101.TI", TRADE_DATE: d1, "board_return": 0.08},
        {"board_id": "885750.TI", TRADE_DATE: d1, "board_return": 0.08},
        {"board_id": "886001.TI", TRADE_DATE: d1, "board_return": 0.01},
    ])

    obs = calculate_m4_raw_observations(
        theme_index_daily, effective_members, market_snapshot, comparison_boards
    )
    row = obs.iloc[0]
    # 0.08 (2个) > 0.05 (COLL_T) > 0.01 (1个)
    # 标准竞争排名：大于 0.05 的有 2 个，因此 rank = 3
    assert row[THEME_RETURN_RANK] == 3
    assert row[COMPARISON_UNIVERSE_SIZE] == 4


def test_m4_missing_is_limit_up_fails_closed_to_none():
    """验证当有效成员的 is_limit_up 无法证明 (缺失或为 None) 时，theme_limit_up_count 为 None 而不是假定 0。"""
    d1 = date(2026, 8, 3)
    theme_index_daily = pd.DataFrame([
        {COLLECTION_ID: "COLL_T", TRADE_DATE: d1, THEME_DAILY_RETURN: 0.05, EFFECTIVE_MEMBER_COUNT: 2},
    ])
    effective_members = pd.DataFrame([
        {COLLECTION_ID: "COLL_T", ASSET_ID: "S1", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
        {COLLECTION_ID: "COLL_T", ASSET_ID: "S2", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
    ])
    # S1 确认未涨停，S2 缺少 market snapshot 事实
    market_snapshot = pd.DataFrame([
        {ASSET_ID: "S1", TRADE_DATE: d1, IS_LIMIT_UP: False},
    ])
    comparison_boards = pd.DataFrame([
        {"board_id": "881101.TI", TRADE_DATE: d1, "board_return": 0.01},
    ])

    obs = calculate_m4_raw_observations(
        theme_index_daily, effective_members, market_snapshot, comparison_boards
    )
    row = obs.iloc[0]
    assert pd.isna(row[THEME_LIMIT_UP_COUNT])


def test_m4_missing_board_return_fails_closed_to_none_rank_and_preserves_universe_size():
    """验证当基准 Universe 中某板块 return 缺失时：
    - theme_return_rank = None (fail-closed，不得静默 dropna 缩小 universe)
    - comparison_universe_size 保持正式 Universe 大小 (4)
    """
    d1 = date(2026, 8, 3)
    theme_index_daily = pd.DataFrame([
        {COLLECTION_ID: "COLL_T", TRADE_DATE: d1, THEME_DAILY_RETURN: 0.05, EFFECTIVE_MEMBER_COUNT: 1},
    ])
    effective_members = pd.DataFrame([
        {COLLECTION_ID: "COLL_T", ASSET_ID: "S1", TRADE_DATE: d1, IS_M4_EFFECTIVE_MEMBER: True},
    ])
    market_snapshot = pd.DataFrame([
        {ASSET_ID: "S1", TRADE_DATE: d1, IS_LIMIT_UP: False},
    ])
    # 3 个板块，其中一个 board_return 为 None
    comparison_boards = pd.DataFrame([
        {"board_id": "881101.TI", TRADE_DATE: d1, "board_return": 0.08},
        {"board_id": "885750.TI", TRADE_DATE: d1, "board_return": None},
        {"board_id": "886001.TI", TRADE_DATE: d1, "board_return": 0.01},
    ])

    obs = calculate_m4_raw_observations(
        theme_index_daily, effective_members, market_snapshot, comparison_boards
    )
    row = obs.iloc[0]
    assert pd.isna(row[THEME_RETURN_RANK])
    assert row[COMPARISON_UNIVERSE_SIZE] == 4
