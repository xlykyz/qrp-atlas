"""Deterministic System B 2.0 base trend state machine.

This module implements only SB20.DATA.001, SB20.DATA.002, SB20.STATE.001,
and SB20.STATE.002. It consumes caller-provided, after-close observations and
never accesses clocks, databases, market-data services, strategies, or execution.
The legacy system_b_basic@1.0.0 detector remains in detector.py.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts.fields import ASSET_ID, CLOSE, TRADE_DATE
from qrp_atlas.contracts.system_b import (
    CONSECUTIVE_ABOVE_MA5_DAYS,
    CONSECUTIVE_BELOW_MA5_DAYS,
    DIAGNOSTICS,
    IS_ABOVE_OR_EQUAL_MA5,
    IS_TRADING_DAY,
    LISTING_TRADING_DAY_NUMBER,
    MA5,
    PARAMETER_SET_ID,
    PREVIOUS_TREND_STATE,
    PRICE_ADJUSTMENT,
    RULE_VERSION_SET_ID,
    SOURCE_RULE_IDS,
    STATE_CHANGED,
    SYSTEM_B_2_0_PARAMETER_SET_ID,
    SYSTEM_B_2_0_PARAMETERS,
    SYSTEM_B_2_0_RULE_VERSION_SET_ID,
    SYSTEM_B_2_0_SOURCE_RULE_IDS,
    SYSTEM_B_STATE_INPUT_COLUMNS,
    SYSTEM_B_STATE_OUTPUT_COLUMNS,
    TREND_STATE,
    UNDERLYING_TREND_STATE,
    PriceAdjustment,
    SystemBStateCheckpoint,
    SystemBStateMachineParameters,
    SystemBStateMachineRequest,
    SystemBStateMachineResult,
    SystemBTrendState,
)
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    normalize_asset_id,
    normalize_trade_date,
    normalize_trade_date_series,
)

DIAGNOSTIC_INPUT_SORTED = "INPUT_SORTED_BY_ASSET_AND_TRADE_DATE"
DIAGNOSTIC_WARMUP = "NEW_LISTING_WARMUP"
DIAGNOSTIC_NON_TRADING_DAY = "NON_TRADING_DAY_STATE_HELD"


class SystemBStateMachineError(ValueError):
    """Raised with a stable code when the state machine cannot calculate safely."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _raise(code: str, message: str) -> None:
    raise SystemBStateMachineError(code, message)


def _require_int(name: str, value: Any, *, minimum: int) -> int:
    if isinstance(value, (bool, np.bool_)):
        _raise("INVALID_INTEGER", f"{name} must be an integer >= {minimum}")
    try:
        number = float(value)
    except (TypeError, ValueError):
        _raise("INVALID_INTEGER", f"{name} must be an integer >= {minimum}")
    if not math.isfinite(number) or not number.is_integer() or number < minimum:
        _raise("INVALID_INTEGER", f"{name} must be an integer >= {minimum}")
    return int(number)


def _require_finite_number(name: str, value: Any, *, asset_id: str, trade_date: pd.Timestamp) -> float:
    if isinstance(value, (bool, np.bool_)):
        _raise(
            "INVALID_NUMERIC_INPUT",
            f"{name} must be finite for trading observation {asset_id} {trade_date.date()}",
        )
    try:
        number = float(value)
    except (TypeError, ValueError):
        _raise(
            "MISSING_NUMERIC_INPUT",
            f"{name} is required for trading observation {asset_id} {trade_date.date()}",
        )
    if not math.isfinite(number):
        _raise(
            "MISSING_NUMERIC_INPUT",
            f"{name} must be finite for trading observation {asset_id} {trade_date.date()}",
        )
    return number


def _optional_finite_number(
    name: str,
    value: Any,
    *,
    asset_id: str,
    trade_date: pd.Timestamp,
) -> float | None:
    """Return None for a missing optional value, otherwise require a finite number."""

    try:
        is_missing = pd.isna(value)
    except (TypeError, ValueError):
        is_missing = False
    if isinstance(is_missing, (bool, np.bool_)) and bool(is_missing):
        return None
    return _require_finite_number(
        name,
        value,
        asset_id=asset_id,
        trade_date=trade_date,
    )


def _validate_definition(request: SystemBStateMachineRequest) -> None:
    parameters = request.parameters
    if not isinstance(parameters, SystemBStateMachineParameters):
        _raise("INVALID_PARAMETERS", "parameters must be SystemBStateMachineParameters")
    if parameters != SYSTEM_B_2_0_PARAMETERS:
        _raise(
            "UNSUPPORTED_PARAMETER_SET",
            "System B 2.0 base state machine requires the frozen parameter values "
            f"{SYSTEM_B_2_0_PARAMETERS.to_dict()}",
        )
    if request.rule_version_set_id != SYSTEM_B_2_0_RULE_VERSION_SET_ID:
        _raise(
            "RULE_VERSION_SET_MISMATCH",
            f"expected {SYSTEM_B_2_0_RULE_VERSION_SET_ID!r}",
        )
    if request.parameter_set_id != SYSTEM_B_2_0_PARAMETER_SET_ID:
        _raise(
            "PARAMETER_SET_MISMATCH",
            f"expected {SYSTEM_B_2_0_PARAMETER_SET_ID!r}",
        )
    if not isinstance(request.input_price_adjustment, PriceAdjustment):
        _raise("INVALID_PRICE_ADJUSTMENT", "input_price_adjustment must be PriceAdjustment")
    if request.input_price_adjustment is not parameters.price_adjustment:
        _raise(
            "PRICE_ADJUSTMENT_MISMATCH",
            "input prices must be declared FORWARD_ADJUSTED",
        )


def _normalize_checkpoint(
    checkpoint: SystemBStateCheckpoint,
    *,
    parameters: SystemBStateMachineParameters,
) -> SystemBStateCheckpoint:
    if not isinstance(checkpoint, SystemBStateCheckpoint):
        _raise("INVALID_CHECKPOINT", "initial_states must contain SystemBStateCheckpoint values")
    try:
        asset_id = normalize_asset_id(checkpoint.asset_id)
        last_date = normalize_trade_date(checkpoint.last_observation_date)
    except CrossSectionFrameError as exc:
        _raise("INVALID_CHECKPOINT", str(exc))
    if not isinstance(checkpoint.trend_state, SystemBTrendState):
        _raise("INVALID_CHECKPOINT", f"checkpoint trend_state is invalid for {asset_id}")
    if not isinstance(checkpoint.underlying_trend_state, SystemBTrendState):
        _raise("INVALID_CHECKPOINT", f"checkpoint underlying_trend_state is invalid for {asset_id}")

    listing_day = _require_int(
        f"checkpoint {asset_id} listing_trading_day_number",
        checkpoint.listing_trading_day_number,
        minimum=1,
    )
    above_days = _require_int(
        f"checkpoint {asset_id} consecutive_above_ma5_days",
        checkpoint.consecutive_above_ma5_days,
        minimum=0,
    )
    below_days = _require_int(
        f"checkpoint {asset_id} consecutive_below_ma5_days",
        checkpoint.consecutive_below_ma5_days,
        minimum=0,
    )
    if above_days and below_days:
        _raise("INVALID_CHECKPOINT", f"checkpoint streaks conflict for {asset_id}")

    state = checkpoint.trend_state
    underlying = checkpoint.underlying_trend_state
    if state is SystemBTrendState.NEW_LISTING_WARMUP:
        if listing_day > parameters.warmup_trading_days:
            _raise("INVALID_CHECKPOINT", f"warmup checkpoint exceeds day 10 for {asset_id}")
        if underlying is not SystemBTrendState.BASE or above_days or below_days:
            _raise(
                "INVALID_CHECKPOINT",
                f"warmup checkpoint must carry BASE with zero streaks for {asset_id}",
            )
    else:
        if listing_day <= parameters.warmup_trading_days:
            _raise("INVALID_CHECKPOINT", f"normal checkpoint is still inside warmup for {asset_id}")
        if underlying is not state:
            _raise("INVALID_CHECKPOINT", f"normal checkpoint states disagree for {asset_id}")
        if state is SystemBTrendState.BASE and above_days:
            _raise("INVALID_CHECKPOINT", f"BASE checkpoint cannot have an above-MA5 streak for {asset_id}")
        if state is SystemBTrendState.CANDIDATE:
            if above_days != parameters.active_confirm_days - 1 or below_days:
                _raise("INVALID_CHECKPOINT", f"CANDIDATE checkpoint streaks are invalid for {asset_id}")
        if state is SystemBTrendState.ACTIVE and below_days >= parameters.exit_confirm_days:
            _raise("INVALID_CHECKPOINT", f"ACTIVE checkpoint already meets exit threshold for {asset_id}")

    return SystemBStateCheckpoint(
        asset_id=asset_id,
        last_observation_date=last_date,
        trend_state=state,
        underlying_trend_state=underlying,
        listing_trading_day_number=listing_day,
        consecutive_above_ma5_days=above_days,
        consecutive_below_ma5_days=below_days,
    )


def _normalize_initial_states(
    checkpoints: Iterable[SystemBStateCheckpoint],
    *,
    parameters: SystemBStateMachineParameters,
) -> dict[str, SystemBStateCheckpoint]:
    normalized: dict[str, SystemBStateCheckpoint] = {}
    for checkpoint in checkpoints:
        item = _normalize_checkpoint(checkpoint, parameters=parameters)
        if item.asset_id in normalized:
            _raise("DUPLICATE_CHECKPOINT", f"duplicate initial state for {item.asset_id}")
        normalized[item.asset_id] = item
    return normalized


def _normalize_observations(observations: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    if not isinstance(observations, pd.DataFrame):
        _raise("INVALID_OBSERVATIONS", "observations must be a pandas DataFrame")
    missing = [column for column in SYSTEM_B_STATE_INPUT_COLUMNS if column not in observations.columns]
    if missing:
        _raise("MISSING_REQUIRED_COLUMNS", f"missing required columns: {missing}")
    if observations.empty:
        return observations.loc[:, SYSTEM_B_STATE_INPUT_COLUMNS].copy(), False

    frame = observations.loc[:, SYSTEM_B_STATE_INPUT_COLUMNS].copy()
    try:
        frame[ASSET_ID] = [normalize_asset_id(value) for value in frame[ASSET_ID].tolist()]
        frame[TRADE_DATE] = normalize_trade_date_series(frame[TRADE_DATE])
    except CrossSectionFrameError as exc:
        _raise("INVALID_PRIMARY_KEY", str(exc))

    normalized_trading_flags: list[bool] = []
    for value in frame[IS_TRADING_DAY].tolist():
        if not isinstance(value, (bool, np.bool_)):
            _raise("MISSING_TRADING_DAY_STATUS", "is_trading_day must contain explicit booleans")
        normalized_trading_flags.append(bool(value))
    frame[IS_TRADING_DAY] = normalized_trading_flags

    frame[LISTING_TRADING_DAY_NUMBER] = [
        _require_int(LISTING_TRADING_DAY_NUMBER, value, minimum=1)
        for value in frame[LISTING_TRADING_DAY_NUMBER].tolist()
    ]

    if frame.duplicated([ASSET_ID, TRADE_DATE], keep=False).any():
        duplicates = (
            frame.loc[frame.duplicated([ASSET_ID, TRADE_DATE], keep=False), [ASSET_ID, TRADE_DATE]]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        _raise("DUPLICATE_OBSERVATION", f"duplicate asset_id + trade_date: {duplicates}")

    original_keys = list(zip(frame[ASSET_ID].tolist(), frame[TRADE_DATE].tolist()))
    frame = frame.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    sorted_keys = list(zip(frame[ASSET_ID].tolist(), frame[TRADE_DATE].tolist()))
    return frame, original_keys != sorted_keys


def _empty_output_frame() -> pd.DataFrame:
    frame = pd.DataFrame(columns=SYSTEM_B_STATE_OUTPUT_COLUMNS)
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    frame[STATE_CHANGED] = pd.Series(dtype=bool)
    frame[IS_TRADING_DAY] = pd.Series(dtype=bool)
    frame[LISTING_TRADING_DAY_NUMBER] = pd.Series(dtype="int64")
    frame[CONSECUTIVE_ABOVE_MA5_DAYS] = pd.Series(dtype="int64")
    frame[CONSECUTIVE_BELOW_MA5_DAYS] = pd.Series(dtype="int64")
    return frame


def _next_normal_state(
    previous_state: SystemBTrendState,
    *,
    is_above: bool,
    above_days: int,
    below_days: int,
    parameters: SystemBStateMachineParameters,
) -> SystemBTrendState:
    if previous_state is SystemBTrendState.BASE:
        return SystemBTrendState.CANDIDATE if is_above else SystemBTrendState.BASE
    if previous_state is SystemBTrendState.CANDIDATE:
        if not is_above:
            return SystemBTrendState.BASE
        if above_days >= parameters.active_confirm_days:
            return SystemBTrendState.ACTIVE
        return SystemBTrendState.CANDIDATE
    if previous_state is SystemBTrendState.ACTIVE:
        if not is_above and below_days >= parameters.exit_confirm_days:
            return SystemBTrendState.BASE
        return SystemBTrendState.ACTIVE
    _raise("INVALID_PREVIOUS_STATE", f"normal transition cannot start from {previous_state.value}")


def _metadata(request: SystemBStateMachineRequest) -> dict[str, Any]:
    return {
        RULE_VERSION_SET_ID: request.rule_version_set_id,
        PARAMETER_SET_ID: request.parameter_set_id,
        SOURCE_RULE_IDS: SYSTEM_B_2_0_SOURCE_RULE_IDS,
        "parameters": request.parameters.to_dict(),
        PRICE_ADJUSTMENT: request.input_price_adjustment.value,
    }


def calculate_system_b_2_0_states(
    request: SystemBStateMachineRequest,
) -> SystemBStateMachineResult:
    """Calculate full-history or incremental System B 2.0 state observations.

    Full-history calculation starts each asset at listing trading day 1. An
    incremental request supplies one frozen checkpoint per continuing asset.
    Rows are normalized and deterministically sorted by asset_id and trade_date;
    duplicate normalized keys are rejected.
    """

    if not isinstance(request, SystemBStateMachineRequest):
        _raise("INVALID_REQUEST", "request must be SystemBStateMachineRequest")
    _validate_definition(request)
    checkpoints = _normalize_initial_states(
        request.initial_states,
        parameters=request.parameters,
    )
    observations, was_sorted = _normalize_observations(request.observations)
    batch_diagnostics = (DIAGNOSTIC_INPUT_SORTED,) if was_sorted else ()

    if observations.empty:
        return SystemBStateMachineResult(
            frame=_empty_output_frame(),
            final_states=tuple(checkpoints[key] for key in sorted(checkpoints)),
            diagnostics=batch_diagnostics,
            metadata=_metadata(request),
        )

    rows: list[dict[str, Any]] = []
    states = dict(checkpoints)
    seen_assets: set[str] = set()

    for record in observations.to_dict(orient="records"):
        asset_id = record[ASSET_ID]
        trade_date = record[TRADE_DATE]
        is_trading_day = record[IS_TRADING_DAY]
        listing_day = record[LISTING_TRADING_DAY_NUMBER]
        previous = states.get(asset_id)

        if asset_id not in seen_assets:
            seen_assets.add(asset_id)
            if previous is None:
                if not is_trading_day or listing_day != 1:
                    _raise(
                        "INITIAL_STATE_REQUIRED",
                        f"{asset_id} must start at listing trading day 1 or provide a checkpoint",
                    )
            elif trade_date <= previous.last_observation_date:
                _raise(
                    "NON_FORWARD_INCREMENT",
                    f"{asset_id} observation {trade_date.date()} is not after checkpoint "
                    f"{previous.last_observation_date.date()}",
                )

        if previous is not None:
            expected_listing_day = previous.listing_trading_day_number + int(is_trading_day)
            if listing_day != expected_listing_day:
                _raise(
                    "INVALID_LISTING_TRADING_DAY_SEQUENCE",
                    f"{asset_id} {trade_date.date()} expected listing trading day "
                    f"{expected_listing_day}, got {listing_day}",
                )

        previous_state = previous.trend_state if previous is not None else None
        previous_underlying = (
            previous.underlying_trend_state if previous is not None else SystemBTrendState.BASE
        )
        previous_above_days = previous.consecutive_above_ma5_days if previous is not None else 0
        previous_below_days = previous.consecutive_below_ma5_days if previous is not None else 0

        close_value: float | None
        ma5_value: float | None
        is_above: bool | None
        row_diagnostics: tuple[str, ...]

        if not is_trading_day:
            if previous is None:
                _raise("INITIAL_STATE_REQUIRED", f"{asset_id} cannot begin with a non-trading day")
            close_value = None
            ma5_value = None
            is_above = None
            state = previous_state
            underlying_state = previous_underlying
            above_days = previous_above_days
            below_days = previous_below_days
            row_diagnostics = (DIAGNOSTIC_NON_TRADING_DAY,)
        else:
            close_value = _require_finite_number(
                CLOSE,
                record[CLOSE],
                asset_id=asset_id,
                trade_date=trade_date,
            )
            if listing_day <= request.parameters.warmup_trading_days:
                ma5_value = _optional_finite_number(
                    MA5,
                    record[MA5],
                    asset_id=asset_id,
                    trade_date=trade_date,
                )
                is_above = close_value >= ma5_value if ma5_value is not None else None
                state = SystemBTrendState.NEW_LISTING_WARMUP
                underlying_state = SystemBTrendState.BASE
                above_days = 0
                below_days = 0
                row_diagnostics = (DIAGNOSTIC_WARMUP,)
            else:
                ma5_value = _require_finite_number(
                    MA5,
                    record[MA5],
                    asset_id=asset_id,
                    trade_date=trade_date,
                )
                is_above = close_value >= ma5_value
                if previous_state is SystemBTrendState.NEW_LISTING_WARMUP:
                    previous_underlying = SystemBTrendState.BASE
                    previous_above_days = 0
                    previous_below_days = 0
                if is_above:
                    above_days = previous_above_days + 1
                    below_days = 0
                else:
                    above_days = 0
                    below_days = previous_below_days + 1
                state = _next_normal_state(
                    previous_underlying,
                    is_above=is_above,
                    above_days=above_days,
                    below_days=below_days,
                    parameters=request.parameters,
                )
                underlying_state = state
                row_diagnostics = ()

        state_changed = previous_state is not None and state is not previous_state
        rows.append(
            {
                ASSET_ID: asset_id,
                TRADE_DATE: trade_date,
                TREND_STATE: state.value,
                UNDERLYING_TREND_STATE: underlying_state.value,
                PREVIOUS_TREND_STATE: previous_state.value if previous_state is not None else None,
                STATE_CHANGED: state_changed,
                IS_TRADING_DAY: is_trading_day,
                LISTING_TRADING_DAY_NUMBER: listing_day,
                CLOSE: close_value,
                MA5: ma5_value,
                IS_ABOVE_OR_EQUAL_MA5: is_above,
                CONSECUTIVE_ABOVE_MA5_DAYS: above_days,
                CONSECUTIVE_BELOW_MA5_DAYS: below_days,
                PRICE_ADJUSTMENT: request.input_price_adjustment.value,
                RULE_VERSION_SET_ID: request.rule_version_set_id,
                PARAMETER_SET_ID: request.parameter_set_id,
                SOURCE_RULE_IDS: SYSTEM_B_2_0_SOURCE_RULE_IDS,
                DIAGNOSTICS: row_diagnostics,
            }
        )
        states[asset_id] = SystemBStateCheckpoint(
            asset_id=asset_id,
            last_observation_date=trade_date,
            trend_state=state,
            underlying_trend_state=underlying_state,
            listing_trading_day_number=listing_day,
            consecutive_above_ma5_days=above_days,
            consecutive_below_ma5_days=below_days,
        )

    frame = pd.DataFrame.from_records(rows, columns=SYSTEM_B_STATE_OUTPUT_COLUMNS)
    frame[STATE_CHANGED] = frame[STATE_CHANGED].astype(bool)
    frame[IS_TRADING_DAY] = frame[IS_TRADING_DAY].astype(bool)
    frame[LISTING_TRADING_DAY_NUMBER] = frame[LISTING_TRADING_DAY_NUMBER].astype(int)
    frame[CONSECUTIVE_ABOVE_MA5_DAYS] = frame[CONSECUTIVE_ABOVE_MA5_DAYS].astype(int)
    frame[CONSECUTIVE_BELOW_MA5_DAYS] = frame[CONSECUTIVE_BELOW_MA5_DAYS].astype(int)

    return SystemBStateMachineResult(
        frame=frame,
        final_states=tuple(states[key] for key in sorted(states)),
        diagnostics=batch_diagnostics,
        metadata=_metadata(request),
    )
