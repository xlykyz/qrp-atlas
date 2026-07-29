"""Pure, point-in-time System B stock-pool calculations."""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    AMOUNT,
    ASSET_ID,
    CLOSE,
    EPISODE_END_DATE,
    EPISODE_ID,
    EPISODE_RETURN,
    FLOAT_CAP,
    CAPACITY_AVG_AMOUNT_RANK_MAX,
    CAPACITY_AVG_AMOUNT_WINDOW_DAYS,
    CAPACITY_DAILY_AMOUNT_RANK_MAX,
    CAPACITY_FLOAT_CAP_MIN_CNY,
    HEIGHT_LIMIT_MIN_COUNT,
    HEIGHT_LIMIT_WINDOW_DAYS,
    HEIGHT_MAX_BREAK_DAYS,
    HEIGHT_NATURAL_MIN,
    HIGH,
    IS_LIMIT_UP,
    LOW,
    OPEN,
    RECOGNITION_EPISODE_RETURN_MIN,
    RECOGNITION_LONG_WINDOW_DAYS,
    RECOGNITION_RANK_MAX,
    RECOGNITION_SHORT_WINDOW_DAYS,
    TRADE_DATE,
    TREND_STATE,
)

HEIGHT = "HEIGHT"
CAPACITY = "CAPACITY"
RECOGNITION = "RECOGNITION"
IN_POOL = "IN_POOL"
EXITED = "EXITED"

DAILY_AMOUNT_RANK = "daily_amount_rank"
AVG5_AMOUNT_RANK = "avg5_amount_rank"
RETURN5 = "return5"
RETURN5_RANK = "return5_rank"
RETURN10 = "return10"
RETURN10_RANK = "return10_rank"
EPISODE_RANK = "recognition_episode_rank"
DAILY_AMOUNT_OK = "daily_amount_ok"
AVG5_AMOUNT_OK = "avg5_amount_ok"
FLOAT_CAPACITY_OK = "float_capacity_ok"
RECOGNITION_EPISODE_OK = "recognition_episode_ok"
RECOGNITION_5D_OK = "recognition_5d_ok"
RECOGNITION_10D_OK = "recognition_10d_ok"
CAPACITY_OK = "capacity_ok"


@dataclass(frozen=True)
class PoolCalculationResult:
    membership: pd.DataFrame
    features: pd.DataFrame


_REQUIRED = (
    ASSET_ID,
    TRADE_DATE,
    CLOSE,
    HIGH,
    LOW,
    AMOUNT,
    FLOAT_CAP,
    TREND_STATE,
    EPISODE_ID,
    EPISODE_END_DATE,
    EPISODE_RETURN,
    IS_LIMIT_UP,
)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _rank_by_date(frame: pd.DataFrame, value: str) -> pd.Series:
    """Stable descending rank; asset id is the deterministic tie breaker."""
    result = pd.Series(pd.NA, index=frame.index, dtype="Int64")
    for _, group in frame.groupby(TRADE_DATE, sort=False):
        valid = group[value].notna() & np.isfinite(pd.to_numeric(group[value], errors="coerce"))
        ordered = group.loc[valid, [value, ASSET_ID]].sort_values(
            [value, ASSET_ID], ascending=[False, True], kind="mergesort"
        )
        result.loc[ordered.index] = pd.Series(
            range(1, len(ordered) + 1), index=ordered.index, dtype="Int64"
        )
    return result


def _actual_observations(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    if "is_trading_day" in data.columns:
        data = data.loc[data["is_trading_day"].fillna(False).astype(bool)]
    if "market_fact_status" in data.columns:
        data = data.loc[data["market_fact_status"].eq("ACTUAL_TRADING")]
    return data


def build_common_features(frame: pd.DataFrame, pool_type: str | None = None) -> pd.DataFrame:
    """Build one deterministic feature panel for all three evaluators.

    The caller supplies official adjusted prices, limit flags, state facts and
    episode observations. Rolling windows therefore operate on the supplied
    actual observation sequence rather than calendar days.
    """
    if pool_type not in (None, HEIGHT, CAPACITY, RECOGNITION):
        raise ValueError(f"unsupported pool_type: {pool_type}")
    missing = [column for column in _REQUIRED if column not in frame.columns]
    if missing:
        raise ValueError(f"missing required pool fields: {missing}")
    selected = list(dict.fromkeys((*_REQUIRED, "previous_trend_state", "is_trading_day", "market_fact_status", OPEN)))
    available = [column for column in selected if column in frame.columns]
    data = _actual_observations(frame.loc[:, available]).copy()
    data[TRADE_DATE] = pd.to_datetime(data[TRADE_DATE], errors="coerce").dt.normalize()
    if data[TRADE_DATE].isna().any() or data[[ASSET_ID, TRADE_DATE]].duplicated().any():
        raise ValueError("invalid or duplicate asset_id + trade_date")
    data = data.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    for column in (CLOSE, HIGH, LOW, AMOUNT, FLOAT_CAP, EPISODE_RETURN):
        data[column] = pd.to_numeric(data[column], errors="coerce")
    price_values = data[[CLOSE, HIGH, LOW]].to_numpy(dtype=float)
    if not np.isfinite(price_values).all():
        raise ValueError("close, high and low must be finite")
    data[IS_LIMIT_UP] = data[IS_LIMIT_UP].fillna(False).astype(bool)
    if OPEN in data.columns:
        data[OPEN] = pd.to_numeric(data[OPEN], errors="coerce")
        data["is_one_word_limit_up"] = (
            data[IS_LIMIT_UP]
            & data[OPEN].eq(data[HIGH])
            & data[HIGH].eq(data[LOW])
            & data[LOW].eq(data[CLOSE])
        )
    else:
        data["is_one_word_limit_up"] = (
            data[IS_LIMIT_UP] & data[CLOSE].eq(data[HIGH]) & data[HIGH].eq(data[LOW])
        )
    grouped = data.groupby(ASSET_ID, sort=False)
    if pool_type in (None, CAPACITY):
        data["avg5_amount"] = grouped[AMOUNT].transform(
            lambda values: values.rolling(
                CAPACITY_AVG_AMOUNT_WINDOW_DAYS,
                min_periods=CAPACITY_AVG_AMOUNT_WINDOW_DAYS,
            ).mean()
        )
        data[DAILY_AMOUNT_RANK] = _rank_by_date(data, AMOUNT)
        data[AVG5_AMOUNT_RANK] = _rank_by_date(data, "avg5_amount")
        data[DAILY_AMOUNT_OK] = data[DAILY_AMOUNT_RANK].le(CAPACITY_DAILY_AMOUNT_RANK_MAX).fillna(False)
        data[AVG5_AMOUNT_OK] = data[AVG5_AMOUNT_RANK].le(CAPACITY_AVG_AMOUNT_RANK_MAX).fillna(False)
        data[FLOAT_CAPACITY_OK] = data[FLOAT_CAP].ge(CAPACITY_FLOAT_CAP_MIN_CNY).fillna(False)
        data[CAPACITY_OK] = (
            data[TREND_STATE].eq("ACTIVE")
            & ~data["is_one_word_limit_up"]
            & (data[DAILY_AMOUNT_OK] | data[AVG5_AMOUNT_OK] | data[FLOAT_CAPACITY_OK])
        )
    if pool_type in (None, RECOGNITION):
        data[RETURN5] = grouped[CLOSE].transform(
            lambda values: values.div(values.shift(RECOGNITION_SHORT_WINDOW_DAYS - 1)).sub(1.0)
        )
        data[RETURN10] = grouped[CLOSE].transform(
            lambda values: values.div(values.shift(RECOGNITION_LONG_WINDOW_DAYS - 1)).sub(1.0)
        )
        data[RETURN5_RANK] = _rank_by_date(data, RETURN5)
        data[RETURN10_RANK] = _rank_by_date(data, RETURN10)
        data[EPISODE_RANK] = _rank_by_date(data, EPISODE_RETURN)
        data[RECOGNITION_EPISODE_OK] = (
            data[EPISODE_RETURN].ge(RECOGNITION_EPISODE_RETURN_MIN)
            & data[EPISODE_RANK].le(RECOGNITION_RANK_MAX)
        ).fillna(False)
        data[RECOGNITION_5D_OK] = data[RETURN5_RANK].le(RECOGNITION_RANK_MAX).fillna(False)
        data[RECOGNITION_10D_OK] = data[RETURN10_RANK].le(RECOGNITION_RANK_MAX).fillna(False)
    return data


def _membership_row(
    trade_date: Any,
    asset_id: str,
    pool_type: str,
    state: str,
    snapshot: dict[str, Any],
    exit_reason: str | None,
    episode_id: Any,
) -> dict[str, Any]:
    metrics = dict(snapshot.get("metrics", {}))
    metrics.update({"episode_id": episode_id})
    return {
        TRADE_DATE: trade_date,
        ASSET_ID: asset_id,
        "pool_type": pool_type,
        "membership_state": state,
        "pool_cycle_no": snapshot["cycle_no"],
        "entry_date": snapshot["entry_date"],
        "exit_date": trade_date if state == EXITED else None,
        "entry_reason": snapshot["entry_reason"],
        "exit_reason": exit_reason,
        EPISODE_ID: episode_id,
        "metrics_json": _json(metrics),
    }


def _height_structure_snapshot(
    group: pd.DataFrame,
    start_position: int,
    end_position: int,
) -> dict[str, int | None]:
    flags = group.iloc[start_position:end_position + 1][IS_LIMIT_UP].astype(bool).tolist()
    limit_count = int(sum(flags))
    maximum_completed_break: int | None = None
    rebound_count = 0
    current_break_days = 0
    observed_first_limit = False
    for is_limit_up in flags:
        if is_limit_up:
            if observed_first_limit and current_break_days > 0:
                rebound_count += 1
                maximum_completed_break = max(maximum_completed_break or 0, current_break_days)
            observed_first_limit = True
            current_break_days = 0
        elif observed_first_limit:
            current_break_days += 1
    return {
        "n": limit_count,
        "m": maximum_completed_break,
        "i": rebound_count,
        "break_days": current_break_days,
    }


def evaluate_height(frame: pd.DataFrame) -> PoolCalculationResult:
    """Evaluate natural boards and seven-session rebound structures."""
    data = frame.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    output: list[dict[str, Any]] = []
    for asset_id, group in data.groupby(ASSET_ID, sort=False):
        group = group.reset_index(drop=True)
        trade_dates = group[TRADE_DATE].to_numpy()
        limit_flags = group[IS_LIMIT_UP].to_numpy(dtype=bool)
        trend_states = group[TREND_STATE].to_numpy(dtype=object)
        previous_trend_states = group["previous_trend_state"].to_numpy(dtype=object)
        episode_ids = group[EPISODE_ID].to_numpy(dtype=object)
        active: dict[str, Any] | None = None
        cycle_no = 0
        current_streak = 0
        recent_limit_positions: list[int] = []
        for position, is_limit_up in enumerate(limit_flags):
            current_streak = current_streak + 1 if is_limit_up else 0
            window_start = position - (HEIGHT_LIMIT_WINDOW_DAYS - 1)
            while recent_limit_positions and recent_limit_positions[0] < window_start:
                recent_limit_positions.pop(0)
            if is_limit_up:
                recent_limit_positions.append(position)

            if active is None:
                if current_streak >= HEIGHT_NATURAL_MIN or len(recent_limit_positions) >= HEIGHT_LIMIT_MIN_COUNT:
                    start_position = (
                        position - current_streak + 1
                        if current_streak >= HEIGHT_NATURAL_MIN
                        else recent_limit_positions[0]
                    )
                    structure = _height_structure_snapshot(group, start_position, position)
                    cycle_no += 1
                    active = {
                        "cycle_no": cycle_no,
                        "entry_date": trade_dates[position],
                        "entry_reason": "NATURAL_CONSECUTIVE_LIMIT_UP" if current_streak >= 2 else "SEVEN_SESSION_THREE_LIMIT_UP",
                        "start": trade_dates[start_position],
                        **structure,
                        "metrics": {},
                    }
            elif is_limit_up:
                if active["break_days"] > 0:
                    if active["break_days"] <= HEIGHT_MAX_BREAK_DAYS:
                        active["i"] += 1
                        active["m"] = max(active["m"] or 0, active["break_days"])
                active["n"] += 1
                active["break_days"] = 0
            else:
                active["break_days"] += 1

            if active is None:
                continue
            active["metrics"] = {
                "height_type": "H(n,m,i)" if active["i"] else "H(n)",
                "n": active["n"],
                "m": active["m"],
                "i": active["i"],
                "current_break_days": active["break_days"],
                "height_start_date": active["start"],
                "height_admitted_date": active["entry_date"],
            }
            exit_reason = None
            active_to_base = previous_trend_states[position] == "ACTIVE" and trend_states[position] == "BASE"
            if active_to_base:
                exit_reason = "ACTIVE_TO_BASE"
            elif active["break_days"] > HEIGHT_MAX_BREAK_DAYS:
                exit_reason = "BREAK_DAY_5"
            if exit_reason is not None:
                output.append(_membership_row(
                    trade_dates[position], asset_id, HEIGHT, EXITED, active, exit_reason, episode_ids[position]
                ))
                active = None
            else:
                output.append(_membership_row(
                    trade_dates[position], asset_id, HEIGHT, IN_POOL, active, None, episode_ids[position]
                ))
    return PoolCalculationResult(pd.DataFrame(output), data)


def evaluate_capacity(frame: pd.DataFrame) -> PoolCalculationResult:
    """Evaluate ACTIVE capacity membership and immediate capacity loss."""
    data = frame.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    output: list[dict[str, Any]] = []
    for asset_id, group in data.groupby(ASSET_ID, sort=False):
        trade_dates = group[TRADE_DATE].to_numpy()
        conditions = group[CAPACITY_OK].to_numpy(dtype=bool)
        previous_conditions = np.concatenate(([False], conditions[:-1]))
        emitted_positions = np.flatnonzero(conditions | previous_conditions)
        daily_amount_ok = group[DAILY_AMOUNT_OK].to_numpy(dtype=bool)
        daily_amount_ranks = group[DAILY_AMOUNT_RANK].to_numpy(dtype=object)
        avg5_amount_ok = group[AVG5_AMOUNT_OK].to_numpy(dtype=bool)
        avg5_amount_ranks = group[AVG5_AMOUNT_RANK].to_numpy(dtype=object)
        float_capacity_ok = group[FLOAT_CAPACITY_OK].to_numpy(dtype=bool)
        float_caps = group[FLOAT_CAP].to_numpy(dtype=float)
        one_word_flags = group["is_one_word_limit_up"].to_numpy(dtype=bool)
        trend_states = group[TREND_STATE].to_numpy(dtype=object)
        episode_ids = group[EPISODE_ID].to_numpy(dtype=object)
        active: dict[str, Any] | None = None
        cycle_no = 0
        for position in emitted_positions:
            day = trade_dates[position]
            reasons = {
                "daily_amount_top100": bool(daily_amount_ok[position]),
                "daily_amount_rank": None if pd.isna(daily_amount_ranks[position]) else int(daily_amount_ranks[position]),
                "avg5_amount_top100": bool(avg5_amount_ok[position]),
                "avg5_amount_rank": None if pd.isna(avg5_amount_ranks[position]) else int(avg5_amount_ranks[position]),
                "float_cap_ge_300b": bool(float_capacity_ok[position]),
                "float_cap": None if pd.isna(float_caps[position]) else float(float_caps[position]),
            }
            condition = bool(conditions[position])
            if condition:
                if not previous_conditions[position]:
                    cycle_no += 1
                    active = {
                        "cycle_no": cycle_no,
                        "entry_date": day,
                        "entry_reason": _json(reasons),
                        "metrics": reasons,
                    }
                else:
                    active["metrics"] = reasons
                output.append(_membership_row(day, asset_id, CAPACITY, IN_POOL, active, None, episode_ids[position]))
            elif active is not None:
                active["metrics"] = reasons
                if one_word_flags[position]:
                    reason = "ONE_WORD_LIMIT_UP"
                elif trend_states[position] != "ACTIVE":
                    reason = "NOT_ACTIVE"
                else:
                    reason = "SHRINK_VOLUME"
                output.append(_membership_row(day, asset_id, CAPACITY, EXITED, active, reason, episode_ids[position]))
                active = None
    return PoolCalculationResult(pd.DataFrame(output), data)


def evaluate_recognition(frame: pd.DataFrame) -> PoolCalculationResult:
    """Evaluate recognition with sticky retention while the current episode runs."""
    data = frame.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    output: list[dict[str, Any]] = []
    for asset_id, group in data.groupby(ASSET_ID, sort=False):
        trade_dates = group[TRADE_DATE].to_numpy()
        episode_ids = group[EPISODE_ID].to_numpy(dtype=object)
        episode_ok = group[RECOGNITION_EPISODE_OK].to_numpy(dtype=bool)
        return5_ok = group[RECOGNITION_5D_OK].to_numpy(dtype=bool)
        return10_ok = group[RECOGNITION_10D_OK].to_numpy(dtype=bool)
        conditions = episode_ok | return5_ok | return10_ok
        retains = conditions | pd.notna(episode_ids)
        cycle_no = 0
        position = 0
        while position < len(group):
            starts = np.flatnonzero(conditions[position:])
            if not len(starts):
                break
            start_position = position + int(starts[0])
            reasons = {
                "episode_return_top30": bool(episode_ok[start_position]),
                "return5_top30": bool(return5_ok[start_position]),
                "return10_top30": bool(return10_ok[start_position]),
            }
            cycle_no += 1
            active = {
                "cycle_no": cycle_no,
                "entry_date": trade_dates[start_position],
                "entry_reason": _json(reasons),
                "metrics": reasons,
            }
            stops = np.flatnonzero(~retains[start_position:])
            stop_position = len(group) if not len(stops) else start_position + int(stops[0])
            for member_position in range(start_position, stop_position):
                output.append(_membership_row(
                    trade_dates[member_position], asset_id, RECOGNITION, IN_POOL,
                    active, None, episode_ids[member_position],
                ))
            if stop_position < len(group):
                output.append(_membership_row(
                    trade_dates[stop_position], asset_id, RECOGNITION, EXITED,
                    active, "NO_CONDITION_AND_NO_ACTIVE_EPISODE", episode_ids[stop_position],
                ))
                position = stop_position + 1
            else:
                break
    return PoolCalculationResult(pd.DataFrame(output), data)


def calculate_stock_pools(frame: pd.DataFrame) -> dict[str, PoolCalculationResult]:
    """Run the shared feature pass and three independent evaluators."""
    features = build_common_features(frame)
    height = evaluate_height(features)
    capacity = evaluate_capacity(features)
    recognition = evaluate_recognition(features)
    return {HEIGHT: height, CAPACITY: capacity, RECOGNITION: recognition}


def calculate_stock_pool(frame: pd.DataFrame, pool_type: str) -> PoolCalculationResult:
    """Run one pool evaluator through the canonical shared feature pass."""
    evaluators = {
        HEIGHT: evaluate_height,
        CAPACITY: evaluate_capacity,
        RECOGNITION: evaluate_recognition,
    }
    try:
        evaluator = evaluators[pool_type]
    except KeyError as exc:
        raise ValueError(f"unsupported pool_type: {pool_type}") from exc
    return evaluator(build_common_features(frame, pool_type=pool_type))
