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
    HEIGHT_TIMEOUT_BREAK_DAYS,
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


def build_common_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Build one deterministic feature panel for all three evaluators.

    The caller supplies official adjusted prices, limit flags, state facts and
    episode observations. Rolling windows therefore operate on the supplied
    actual observation sequence rather than calendar days.
    """
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
    data[RETURN5] = grouped[CLOSE].transform(
        lambda values: values.div(values.shift(RECOGNITION_SHORT_WINDOW_DAYS - 1)).sub(1.0)
    )
    data[RETURN10] = grouped[CLOSE].transform(
        lambda values: values.div(values.shift(RECOGNITION_LONG_WINDOW_DAYS - 1)).sub(1.0)
    )
    data["avg5_amount"] = grouped[AMOUNT].transform(
        lambda values: values.rolling(
            CAPACITY_AVG_AMOUNT_WINDOW_DAYS,
            min_periods=CAPACITY_AVG_AMOUNT_WINDOW_DAYS,
        ).mean()
    )
    data[DAILY_AMOUNT_RANK] = _rank_by_date(data, AMOUNT)
    data[AVG5_AMOUNT_RANK] = _rank_by_date(data, "avg5_amount")
    data[RETURN5_RANK] = _rank_by_date(data, RETURN5)
    data[RETURN10_RANK] = _rank_by_date(data, RETURN10)
    data[EPISODE_RANK] = _rank_by_date(data, EPISODE_RETURN)
    data[DAILY_AMOUNT_OK] = data[DAILY_AMOUNT_RANK].le(CAPACITY_DAILY_AMOUNT_RANK_MAX).fillna(False)
    data[AVG5_AMOUNT_OK] = data[AVG5_AMOUNT_RANK].le(CAPACITY_AVG_AMOUNT_RANK_MAX).fillna(False)
    data[FLOAT_CAPACITY_OK] = data[FLOAT_CAP].ge(CAPACITY_FLOAT_CAP_MIN_CNY).fillna(False)
    data[RECOGNITION_EPISODE_OK] = (
        data[EPISODE_RETURN].ge(RECOGNITION_EPISODE_RETURN_MIN)
        & data[EPISODE_RANK].le(RECOGNITION_RANK_MAX)
    ).fillna(False)
    data[RECOGNITION_5D_OK] = data[RETURN5_RANK].le(RECOGNITION_RANK_MAX).fillna(False)
    data[RECOGNITION_10D_OK] = data[RETURN10_RANK].le(RECOGNITION_RANK_MAX).fillna(False)
    data[CAPACITY_OK] = (
        data[TREND_STATE].eq("ACTIVE")
        & ~data["is_one_word_limit_up"]
        & (data[DAILY_AMOUNT_OK] | data[AVG5_AMOUNT_OK] | data[FLOAT_CAPACITY_OK])
    )
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


def evaluate_height(frame: pd.DataFrame) -> PoolCalculationResult:
    """Evaluate natural boards and seven-session rebound structures."""
    data = frame.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    output: list[dict[str, Any]] = []
    for asset_id, group in data.groupby(ASSET_ID, sort=False):
        group = group.reset_index(drop=True)
        active: dict[str, Any] | None = None
        cycle_no = 0
        for position, row in group.iterrows():
            is_limit_up = bool(row[IS_LIMIT_UP])
            if active is None:
                current_streak = 0
                for prior_position in range(position, -1, -1):
                    if bool(group.iloc[prior_position][IS_LIMIT_UP]):
                        current_streak += 1
                    else:
                        break
                window_start = max(0, position - (HEIGHT_LIMIT_WINDOW_DAYS - 1))
                limit_positions = [
                    index for index in range(window_start, position + 1)
                    if bool(group.iloc[index][IS_LIMIT_UP])
                ]
                if current_streak >= HEIGHT_NATURAL_MIN or len(limit_positions) >= HEIGHT_LIMIT_MIN_COUNT:
                    start_position = position - current_streak + 1 if current_streak >= 2 else limit_positions[0]
                    cycle_no += 1
                    active = {
                        "cycle_no": cycle_no,
                        "entry_date": row[TRADE_DATE],
                        "entry_reason": "NATURAL_CONSECUTIVE_LIMIT_UP" if current_streak >= 2 else "SEVEN_SESSION_THREE_LIMIT_UP",
                        "start": group.iloc[start_position][TRADE_DATE],
                        "n": int(group.iloc[start_position:position + 1][IS_LIMIT_UP].sum()),
                        "m": None,
                        "i": 0,
                        "break_days": 0,
                        "metrics": {},
                    }
            elif is_limit_up:
                if active["break_days"] > 0:
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
            if active["break_days"] >= HEIGHT_TIMEOUT_BREAK_DAYS:
                exit_reason = "BREAK_DAY_5"
            if exit_reason is not None:
                output.append(_membership_row(row[TRADE_DATE], asset_id, HEIGHT, EXITED, active, exit_reason, row[EPISODE_ID]))
                active = None
            else:
                output.append(_membership_row(row[TRADE_DATE], asset_id, HEIGHT, IN_POOL, active, None, row[EPISODE_ID]))
    return PoolCalculationResult(pd.DataFrame(output), data)


def evaluate_capacity(frame: pd.DataFrame) -> PoolCalculationResult:
    """Evaluate ACTIVE capacity membership. Shrink-volume exclusion is fail-closed."""
    data = frame.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    output: list[dict[str, Any]] = []
    for asset_id, group in data.groupby(ASSET_ID, sort=False):
        active: dict[str, Any] | None = None
        cycle_no = 0
        for row in group.itertuples(index=False):
            day = getattr(row, TRADE_DATE)
            reasons = {
                "daily_amount_top100": bool(getattr(row, DAILY_AMOUNT_OK)),
                "avg5_amount_top100": bool(getattr(row, AVG5_AMOUNT_OK)),
                "float_cap_ge_300b": bool(getattr(row, FLOAT_CAPACITY_OK)),
                "shrink_volume": "NOT_CONFIGURED",
            }
            condition = bool(getattr(row, CAPACITY_OK))
            if condition:
                if active is None:
                    cycle_no += 1
                    active = {
                        "cycle_no": cycle_no,
                        "entry_date": day,
                        "entry_reason": _json(reasons),
                        "metrics": reasons,
                    }
                output.append(_membership_row(day, asset_id, CAPACITY, IN_POOL, active, None, getattr(row, EPISODE_ID)))
            elif active is not None:
                if bool(getattr(row, "is_one_word_limit_up")):
                    reason = "ONE_WORD_LIMIT_UP"
                elif getattr(row, TREND_STATE) != "ACTIVE":
                    reason = "NOT_ACTIVE"
                else:
                    reason = "CAPACITY_CONDITION_LOST"
                output.append(_membership_row(day, asset_id, CAPACITY, EXITED, active, reason, getattr(row, EPISODE_ID)))
                active = None
    return PoolCalculationResult(pd.DataFrame(output), data)


def evaluate_recognition(
    frame: pd.DataFrame,
    admission_keys: set[tuple[Any, Any]] | None = None,
) -> PoolCalculationResult:
    """Evaluate recognition with sticky retention while the current episode runs."""
    data = frame.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    output: list[dict[str, Any]] = []
    for asset_id, group in data.groupby(ASSET_ID, sort=False):
        active: dict[str, Any] | None = None
        cycle_no = 0
        for row in group.itertuples(index=False):
            day = getattr(row, TRADE_DATE)
            reasons = {
                "episode_return_top30": bool(getattr(row, RECOGNITION_EPISODE_OK)),
                "return5_top30": bool(getattr(row, RECOGNITION_5D_OK)),
                "return10_top30": bool(getattr(row, RECOGNITION_10D_OK)),
            }
            condition = any(reasons.values())
            episode_id = getattr(row, EPISODE_ID)
            episode_is_observable = pd.notna(episode_id)
            # Episode observations are the point-in-time source of truth.  Do
            # not consult the final episode master end date here: doing so
            # would leak a later fact into an earlier daily snapshot.
            episode_continues = episode_is_observable
            admitted_from_pool = admission_keys is None or (day, asset_id) in admission_keys
            if active is None and condition and admitted_from_pool:
                cycle_no += 1
                active = {
                    "cycle_no": cycle_no,
                    "entry_date": day,
                    "entry_reason": _json(reasons),
                    "metrics": reasons,
                }
            if active is not None and (condition or episode_continues):
                output.append(_membership_row(day, asset_id, RECOGNITION, IN_POOL, active, None, episode_id))
            elif active is not None:
                output.append(_membership_row(day, asset_id, RECOGNITION, EXITED, active, "NO_CONDITION_AND_NO_ACTIVE_EPISODE", episode_id))
                active = None
    return PoolCalculationResult(pd.DataFrame(output), data)


def calculate_stock_pools(frame: pd.DataFrame) -> dict[str, PoolCalculationResult]:
    """Run the shared feature pass and three independent evaluators."""
    features = build_common_features(frame)
    height = evaluate_height(features)
    capacity = evaluate_capacity(features)
    admitted_keys = set(
        zip(
            height.membership.loc[height.membership["membership_state"] == IN_POOL, TRADE_DATE],
            height.membership.loc[height.membership["membership_state"] == IN_POOL, ASSET_ID],
        )
    ) | set(
        zip(
            capacity.membership.loc[capacity.membership["membership_state"] == IN_POOL, TRADE_DATE],
            capacity.membership.loc[capacity.membership["membership_state"] == IN_POOL, ASSET_ID],
        )
    )
    recognition = evaluate_recognition(features, admission_keys=admitted_keys)
    return {HEIGHT: height, CAPACITY: capacity, RECOGNITION: recognition}
