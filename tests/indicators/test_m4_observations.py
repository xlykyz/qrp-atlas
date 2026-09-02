"""Tests for M4 raw observation calculations, ranking, and universe fail-closed semantics."""

from datetime import date
import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import ASSET_ID, IS_LIMIT_UP, TRADE_DATE
from qrp_atlas.contracts.m4 import (
    COMPARISON_UNIVERSE_SIZE,
    COMPARISON_UNIVERSE_VERSION,
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
