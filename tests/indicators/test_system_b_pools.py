from __future__ import annotations

import json

import pandas as pd
import pytest

from qrp_atlas.indicators.system_b.pools import (
    AVG5_AMOUNT_OK,
    AVG5_AMOUNT_RANK,
    CAPACITY,
    CAPACITY_OK,
    DAILY_AMOUNT_OK,
    DAILY_AMOUNT_RANK,
    FLOAT_CAPACITY_OK,
    HEIGHT,
    IN_POOL,
    RECOGNITION,
    RETURN5_RANK,
    build_common_features,
    calculate_stock_pools,
    evaluate_capacity,
    evaluate_height,
    evaluate_recognition,
)


def _frame(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "open": 10.0,
        "high": 10.0,
        "low": 10.0,
        "close": 10.0,
        "amount": 1_000_000.0,
        "float_cap": 30_000_000_000.0,
        "trend_state": "BASE",
        "previous_trend_state": "BASE",
        "episode_id": None,
        "episode_end_date": pd.NaT,
        "episode_return": None,
        "is_limit_up": False,
        "is_trading_day": True,
        "market_fact_status": "ACTUAL_TRADING",
    }
    values = []
    for row in rows:
        value = defaults | row
        values.append(value)
    return pd.DataFrame(values)


def test_height_exits_on_break_day_five_with_active_state():
    dates = pd.bdate_range("2026-01-01", periods=8)
    flags = [True, True, True] + [False] * 5
    frame = _frame([
        {
            "asset_id": "A",
            "trade_date": day,
            "is_limit_up": flag,
            "trend_state": "ACTIVE",
            "previous_trend_state": "ACTIVE",
        }
        for day, flag in zip(dates, flags, strict=True)
    ])
    result = evaluate_height(build_common_features(frame)).membership
    break_day_four = result.loc[result["trade_date"] == dates[6]].iloc[0]
    assert break_day_four["membership_state"] == IN_POOL
    assert result.iloc[-1]["membership_state"] == "EXITED"
    assert result.iloc[-1]["exit_reason"] == "BREAK_DAY_5"


@pytest.mark.parametrize("limit_count", [2, 3, 5])
def test_height_break_day_five_is_independent_of_n(limit_count: int):
    dates = pd.bdate_range("2026-01-01", periods=limit_count + 5)
    flags = [True] * limit_count + [False] * 5
    frame = _frame([
        {
            "asset_id": "A",
            "trade_date": day,
            "is_limit_up": flag,
            "trend_state": "ACTIVE",
            "previous_trend_state": "ACTIVE",
        }
        for day, flag in zip(dates, flags, strict=True)
    ])
    result = evaluate_height(build_common_features(frame)).membership
    assert result.iloc[-1]["trade_date"] == dates[-1]
    assert result.iloc[-1]["membership_state"] == "EXITED"
    assert result.iloc[-1]["exit_reason"] == "BREAK_DAY_5"


def test_height_active_to_base_ends_before_break_day_five():
    dates = pd.bdate_range("2026-01-01", periods=5)
    states = ["ACTIVE", "ACTIVE", "ACTIVE", "ACTIVE", "BASE"]
    frame = _frame([
        {
            "asset_id": "A",
            "trade_date": day,
            "is_limit_up": index < 3,
            "trend_state": state,
            "previous_trend_state": states[index - 1] if index else "ACTIVE",
        }
        for index, (day, state) in enumerate(zip(dates, states, strict=True))
    ])
    result = evaluate_height(build_common_features(frame)).membership
    assert result.iloc[-1]["trade_date"] == dates[-1]
    assert result.iloc[-1]["membership_state"] == "EXITED"
    assert result.iloc[-1]["exit_reason"] == "ACTIVE_TO_BASE"


def test_height_after_break_day_five_requires_a_new_cycle():
    dates = pd.bdate_range("2026-01-01", periods=9)
    flags = [True, True] + [False] * 5 + [True, True]
    frame = _frame([
        {
            "asset_id": "A",
            "trade_date": day,
            "is_limit_up": flag,
            "trend_state": "ACTIVE",
            "previous_trend_state": "ACTIVE",
        }
        for day, flag in zip(dates, flags, strict=True)
    ])
    result = evaluate_height(build_common_features(frame)).membership
    first_exit = result.loc[result["trade_date"] == dates[6]].iloc[0]
    second_cycle = result.loc[result["trade_date"] == dates[8]].iloc[0]
    metrics = json.loads(second_cycle["metrics_json"])
    assert first_exit["membership_state"] == "EXITED"
    assert first_exit["pool_cycle_no"] == 1
    assert result.loc[result["trade_date"] == dates[7]].empty
    assert second_cycle["membership_state"] == IN_POOL
    assert second_cycle["pool_cycle_no"] == 2
    assert second_cycle["entry_date"] == dates[8]
    assert metrics["n"] == 2
    assert metrics["m"] is None
    assert metrics["i"] == 0


def test_height_reconstructs_rebound_structure_on_confirmation():
    dates = pd.bdate_range("2026-01-01", periods=5)
    flags = [True, False, True, False, True]
    frame = _frame([
        {"asset_id": "A", "trade_date": day, "is_limit_up": flag}
        for day, flag in zip(dates, flags, strict=True)
    ])
    result = evaluate_height(build_common_features(frame)).membership
    assert result["trade_date"].tolist() == [dates[-1]]
    metrics = json.loads(result.iloc[0]["metrics_json"])
    assert metrics["n"] == 3
    assert metrics["m"] == 1
    assert metrics["i"] == 2
    assert metrics["height_start_date"].startswith(str(dates[0].date()))
    assert metrics["height_admitted_date"].startswith(str(dates[-1].date()))


def test_height_rebound_m_four_is_retained_and_counts_reentry():
    dates = pd.bdate_range("2026-01-01", periods=8)
    flags = [True, True, False, False, False, False, True, False]
    frame = _frame([
        {"asset_id": "A", "trade_date": day, "is_limit_up": flag}
        for day, flag in zip(dates, flags, strict=True)
    ])
    result = evaluate_height(build_common_features(frame)).membership
    rebound = result.loc[result["trade_date"] == dates[6]].iloc[0]
    metrics = json.loads(rebound["metrics_json"])
    assert rebound["membership_state"] == IN_POOL
    assert metrics["m"] == 4
    assert metrics["i"] == 1


def test_capacity_requires_active_and_uses_inclusive_market_cap_boundary():
    frame = _frame([
        {"asset_id": "A", "trade_date": pd.Timestamp("2026-01-01"), "trend_state": "BASE", "amount": 9e12},
        {"asset_id": "B", "trade_date": pd.Timestamp("2026-01-01"), "trend_state": "ACTIVE", "float_cap": 30e9},
    ])
    features = build_common_features(frame)
    result = evaluate_capacity(features).membership
    assert result.loc[result["asset_id"] == "A"].empty
    assert result.loc[result["asset_id"] == "B", "membership_state"].tolist() == [IN_POOL]


def _capacity_feature_frame(second_day: dict) -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-01", periods=2)
    first = {
        "asset_id": "A",
        "trade_date": dates[0],
        "trend_state": "ACTIVE",
        "episode_id": None,
        "is_one_word_limit_up": False,
        DAILY_AMOUNT_OK: True,
        DAILY_AMOUNT_RANK: 1,
        AVG5_AMOUNT_OK: True,
        AVG5_AMOUNT_RANK: 1,
        FLOAT_CAPACITY_OK: False,
        "float_cap": 1.0,
        CAPACITY_OK: True,
    }
    second = first | {"trade_date": dates[1]} | second_day
    return pd.DataFrame([first, second])


def test_capacity_exits_for_shrink_volume_when_both_amount_conditions_fail():
    frame = _capacity_feature_frame({
        DAILY_AMOUNT_OK: False,
        DAILY_AMOUNT_RANK: 101,
        AVG5_AMOUNT_OK: False,
        AVG5_AMOUNT_RANK: 101,
        FLOAT_CAPACITY_OK: False,
        CAPACITY_OK: False,
    })
    result = evaluate_capacity(frame).membership
    assert result.iloc[-1]["membership_state"] == "EXITED"
    assert result.iloc[-1]["exit_reason"] == "SHRINK_VOLUME"
    metrics = json.loads(result.iloc[-1]["metrics_json"])
    assert metrics["daily_amount_rank"] == 101
    assert metrics["avg5_amount_rank"] == 101


def test_capacity_keeps_when_avg5_amount_remains_top100():
    frame = _capacity_feature_frame({
        DAILY_AMOUNT_OK: False,
        DAILY_AMOUNT_RANK: 101,
        AVG5_AMOUNT_OK: True,
        AVG5_AMOUNT_RANK: 100,
        CAPACITY_OK: True,
    })
    result = evaluate_capacity(frame).membership
    assert result["membership_state"].tolist() == [IN_POOL, IN_POOL]


def test_capacity_keeps_when_daily_amount_remains_top100():
    frame = _capacity_feature_frame({
        DAILY_AMOUNT_OK: True,
        DAILY_AMOUNT_RANK: 100,
        AVG5_AMOUNT_OK: False,
        AVG5_AMOUNT_RANK: 101,
        CAPACITY_OK: True,
    })
    result = evaluate_capacity(frame).membership
    assert result["membership_state"].tolist() == [IN_POOL, IN_POOL]


def test_capacity_keeps_large_float_cap_when_amount_conditions_fail():
    frame = _capacity_feature_frame({
        DAILY_AMOUNT_OK: False,
        DAILY_AMOUNT_RANK: 101,
        AVG5_AMOUNT_OK: False,
        AVG5_AMOUNT_RANK: 101,
        FLOAT_CAPACITY_OK: True,
        "float_cap": 30_000_000_000.0,
        CAPACITY_OK: True,
    })
    result = evaluate_capacity(frame).membership
    assert result["membership_state"].tolist() == [IN_POOL, IN_POOL]


def test_recognition_enters_without_height_or_capacity_and_stays_on_episode():
    dates = pd.bdate_range("2026-01-01", periods=3)
    frame = _frame([
        {"asset_id": "A", "trade_date": dates[0], "close": 10.0, "episode_id": "A_EP_0001", "episode_return": 0.30},
        {"asset_id": "A", "trade_date": dates[1], "close": 10.1, "episode_id": "A_EP_0001", "episode_return": 0.20},
        {"asset_id": "A", "trade_date": dates[2], "close": 10.0, "episode_id": None, "episode_return": None},
    ])
    features = build_common_features(frame)
    result = evaluate_recognition(features).membership
    assert result.iloc[0]["membership_state"] == IN_POOL
    assert result.iloc[1]["membership_state"] == IN_POOL
    assert result.iloc[2]["membership_state"] == "EXITED"


def test_calculate_stock_pools_recognition_needs_no_other_pool_authorization():
    dates = pd.bdate_range("2026-01-01", periods=5)
    frame = _frame([
        {
            "asset_id": "A",
            "trade_date": day,
            "close": 10.0 + index,
            "high": 11.0 + index,
            "low": 9.0 + index,
            "float_cap": 1.0,
            "trend_state": "BASE",
        }
        for index, day in enumerate(dates)
    ])
    results = calculate_stock_pools(frame)
    assert results[HEIGHT].membership.empty
    assert results[CAPACITY].membership.empty
    recognition = results[RECOGNITION].membership
    assert recognition.iloc[0]["trade_date"] == dates[-1]
    assert recognition.iloc[0]["membership_state"] == IN_POOL


def test_common_features_are_deterministic_for_ties_and_actual_sessions():
    dates = pd.bdate_range("2026-01-01", periods=5)
    rows = []
    for asset in ["B", "A"]:
        for day in dates:
            rows.append({"asset_id": asset, "trade_date": day, "amount": 10.0})
    shuffled = _frame(rows).sample(frac=1.0, random_state=42)
    features = build_common_features(shuffled)
    ranks = features.loc[features["trade_date"] == dates[-1], ["asset_id", "daily_amount_rank"]]
    assert ranks.sort_values("asset_id")["daily_amount_rank"].tolist() == [1, 2]
    assert features.loc[features["trade_date"] == dates[-1], RETURN5_RANK].notna().all()
