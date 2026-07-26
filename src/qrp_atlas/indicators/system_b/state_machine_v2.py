"""Pure fact-derived System B 2.0 trend-state calculation."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    ACTUAL_PAIR_CONTIGUOUS,
    ASSET_ID,
    CLOSE,
    DIAGNOSTICS,
    IS_ABOVE_OR_EQUAL_MA5,
    IS_TRADING_DAY,
    LATEST_ACTUAL_CLOSE,
    LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    LATEST_ACTUAL_MA5,
    LATEST_ACTUAL_TRADE_DATE,
    LIFECYCLE_STATE,
    LISTING_TRADING_DAY_NUMBER,
    MA5,
    MARKET_FACT_STATUS,
    PARAMETER_SET_ID,
    PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    PREVIOUS_ACTUAL_TRADE_DATE,
    PREVIOUS_TREND_STATE,
    PRICE_ADJUSTMENT,
    RULE_VERSION_SET_ID,
    SOURCE_RULE_IDS,
    STATE_CHANGED,
    STATE_BASIS_SEQUENCE_INTACT,
    SYSTEM_B_2_0_PARAMETER_SET_ID,
    SYSTEM_B_2_0_PARAMETERS,
    SYSTEM_B_2_0_RULE_VERSION_SET_ID,
    SYSTEM_B_2_0_SOURCE_RULE_IDS,
    SYSTEM_B_STATE_INPUT_COLUMNS,
    SYSTEM_B_STATE_OUTPUT_COLUMNS,
    TRADE_DATE,
    TREND_STATE,
    PriceAdjustment,
    SystemBLifecycleState,
    SystemBMarketFactStatus,
    SystemBStateMachineRequest,
    SystemBStateMachineResult,
    SystemBTrendState,
)
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    normalize_asset_id,
    normalize_trade_date_series,
)

DIAGNOSTIC_INPUT_SORTED = "INPUT_SORTED_BY_ASSET_AND_DATE"
DIAGNOSTIC_WARMUP = "NEW_LISTING_WARMUP"
DIAGNOSTIC_INSUFFICIENT = "INSUFFICIENT_STATE_FACTS"
DIAGNOSTIC_BROKEN_SEQUENCE = "BROKEN_TRADING_SEQUENCE"
DIAGNOSTIC_MISSING_PREVIOUS_ACTUAL = "MISSING_PREVIOUS_ACTUAL_TRADING_FACT"
DIAGNOSTIC_NO_UNIQUE_MATCH = "NO_UNIQUE_STATE_MATCH"
DIAGNOSTIC_NON_TRADING_DERIVATION = "NON_TRADING_DAY_FACT_DERIVED"


class SystemBStateMachineError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


def _raise(code: str, detail: str) -> None:
    raise SystemBStateMachineError(code, detail)


def _validate_definition(request: SystemBStateMachineRequest) -> None:
    if request.parameters != SYSTEM_B_2_0_PARAMETERS:
        _raise("UNSUPPORTED_PARAMETER_SET", "parameters do not match the frozen fact-derived set")
    if request.input_price_adjustment is not PriceAdjustment.FORWARD_ADJUSTED:
        _raise("INVALID_PRICE_ADJUSTMENT", "System B requires FORWARD_ADJUSTED inputs")
    if request.rule_version_set_id != SYSTEM_B_2_0_RULE_VERSION_SET_ID:
        _raise("UNSUPPORTED_RULE_VERSION_SET", request.rule_version_set_id)
    if request.parameter_set_id != SYSTEM_B_2_0_PARAMETER_SET_ID:
        _raise("UNSUPPORTED_PARAMETER_SET_ID", request.parameter_set_id)


def _normalize_observations(observations: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    if not isinstance(observations, pd.DataFrame):
        _raise("INVALID_OBSERVATIONS", "observations must be a pandas DataFrame")
    missing = [column for column in SYSTEM_B_STATE_INPUT_COLUMNS if column not in observations.columns]
    if missing:
        _raise("MISSING_REQUIRED_COLUMNS", f"missing required columns: {missing}")
    frame = observations.loc[:, SYSTEM_B_STATE_INPUT_COLUMNS].copy()
    if frame.empty:
        return frame, False
    try:
        frame[ASSET_ID] = [normalize_asset_id(value) for value in frame[ASSET_ID]]
        frame[TRADE_DATE] = normalize_trade_date_series(frame[TRADE_DATE])
    except CrossSectionFrameError as exc:
        _raise("INVALID_PRIMARY_KEY", str(exc))
    if frame.duplicated([ASSET_ID, TRADE_DATE], keep=False).any():
        _raise("DUPLICATE_OBSERVATION", "duplicate normalized asset_id + trade_date")
    original = list(zip(frame[ASSET_ID], frame[TRADE_DATE]))
    frame = frame.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)
    return frame, original != list(zip(frame[ASSET_ID], frame[TRADE_DATE]))


def _nullable_bool_series(frame: pd.DataFrame, field: str) -> pd.Series:
    values = frame[field]
    invalid = values.notna() & ~values.map(lambda value: isinstance(value, (bool, np.bool_)))
    if invalid.any():
        _raise("INVALID_FACT_TYPE", f"{field} must be boolean or NULL")
    return values.astype("boolean")


def _numeric_series(frame: pd.DataFrame, field: str) -> pd.Series:
    values = pd.to_numeric(frame[field], errors="coerce")
    invalid = frame[field].notna() & values.isna()
    if invalid.any() or np.isinf(values.dropna().to_numpy(dtype=float)).any():
        _raise("NON_FINITE_MARKET_FACT", field)
    return values.astype(float)


def _date_series(frame: pd.DataFrame, field: str) -> pd.Series:
    return pd.to_datetime(frame[field], errors="coerce").dt.normalize()


def _metadata(request: SystemBStateMachineRequest) -> dict[str, Any]:
    return {
        RULE_VERSION_SET_ID: request.rule_version_set_id,
        PARAMETER_SET_ID: request.parameter_set_id,
        SOURCE_RULE_IDS: SYSTEM_B_2_0_SOURCE_RULE_IDS,
        "parameters": request.parameters.to_dict(),
        PRICE_ADJUSTMENT: request.input_price_adjustment.value,
    }


def calculate_system_b_2_0_states(request: SystemBStateMachineRequest) -> SystemBStateMachineResult:
    """Derive each trend state solely from market and historical statistical facts."""
    if not isinstance(request, SystemBStateMachineRequest):
        _raise("INVALID_REQUEST", "request must be SystemBStateMachineRequest")
    _validate_definition(request)
    observations, was_sorted = _normalize_observations(request.observations)
    batch_diagnostics = (DIAGNOSTIC_INPUT_SORTED,) if was_sorted else ()
    if observations.empty:
        return SystemBStateMachineResult(
            frame=pd.DataFrame(columns=SYSTEM_B_STATE_OUTPUT_COLUMNS),
            diagnostics=batch_diagnostics,
            metadata=_metadata(request),
        )

    status = observations[MARKET_FACT_STATUS].astype("string")
    valid_statuses = {item.value for item in SystemBMarketFactStatus}
    if not status.isin(valid_statuses).all():
        _raise("INVALID_MARKET_FACT_STATUS", "market_fact_status contains unsupported values")
    trading = _nullable_bool_series(observations, IS_TRADING_DAY)
    if trading.isna().any() or not trading.eq(status.eq(SystemBMarketFactStatus.ACTUAL_TRADING.value)).all():
        _raise("INCONSISTENT_MARKET_FACT_STATUS", "is_trading_day disagrees with market_fact_status")

    listing_day = pd.to_numeric(observations[LISTING_TRADING_DAY_NUMBER], errors="coerce")
    if listing_day.isna().any() or (listing_day < 0).any() or (listing_day % 1 != 0).any():
        _raise("INVALID_LISTING_TRADING_DAY_NUMBER", "listing day must be a non-negative integer")
    listing_day = listing_day.astype(int)
    latest_relation = _nullable_bool_series(observations, LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5)
    previous_relation = _nullable_bool_series(observations, PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5)
    basis_intact = _nullable_bool_series(observations, STATE_BASIS_SEQUENCE_INTACT)
    pair_contiguous = _nullable_bool_series(observations, ACTUAL_PAIR_CONTIGUOUS)
    if basis_intact.isna().any() or pair_contiguous.isna().any():
        _raise("MISSING_SEQUENCE_FACT", "sequence proof fields cannot be NULL")

    lifecycle = pd.Series(
        np.where(
            listing_day <= request.parameters.warmup_trading_days,
            SystemBLifecycleState.NEW_LISTING_WARMUP.value,
            SystemBLifecycleState.NORMAL.value,
        ),
        index=observations.index,
        dtype="string",
    )
    normal = lifecycle.eq(SystemBLifecycleState.NORMAL.value)
    unresolved = status.eq(SystemBMarketFactStatus.UNRESOLVED_MISSING.value)
    eligible = normal & ~unresolved
    base = eligible & latest_relation.eq(False).fillna(False) & basis_intact.fillna(False)
    candidate = (
        eligible
        & latest_relation.eq(True).fillna(False)
        & previous_relation.eq(False).fillna(False)
        & basis_intact.fillna(False)
        & pair_contiguous.fillna(False)
    )
    active = (
        eligible
        & latest_relation.eq(True).fillna(False)
        & previous_relation.eq(True).fillna(False)
        & basis_intact.fillna(False)
        & pair_contiguous.fillna(False)
    )
    match_count = base.astype(int) + candidate.astype(int) + active.astype(int)
    if match_count.gt(1).any():
        _raise("CONFLICTING_STATE_FACTS", "multiple state predicates matched")

    trend = pd.Series(pd.NA, index=observations.index, dtype="string")
    trend.loc[base] = SystemBTrendState.BASE.value
    trend.loc[candidate] = SystemBTrendState.CANDIDATE.value
    trend.loc[active] = SystemBTrendState.ACTIVE.value

    diagnostics = pd.Series([()] * len(observations), index=observations.index, dtype=object)
    warmup = ~normal
    diagnostics.loc[warmup] = [(DIAGNOSTIC_WARMUP,)] * int(warmup.sum())
    warmup_unresolved = warmup & unresolved
    diagnostics.loc[warmup_unresolved] = [
        (DIAGNOSTIC_WARMUP, DIAGNOSTIC_BROKEN_SEQUENCE)
    ] * int(warmup_unresolved.sum())
    diagnostics.loc[normal & unresolved] = [(DIAGNOSTIC_BROKEN_SEQUENCE,)] * int((normal & unresolved).sum())
    derived_non_trading = trend.notna() & ~trading.fillna(False)
    diagnostics.loc[derived_non_trading] = [(DIAGNOSTIC_NON_TRADING_DERIVATION,)] * int(derived_non_trading.sum())
    unmatched = normal & ~unresolved & trend.isna()
    broken_basis = unmatched & ~basis_intact.fillna(False)
    insufficient = unmatched & ~broken_basis & latest_relation.isna()
    missing_previous = (
        unmatched & ~broken_basis
        & latest_relation.eq(True).fillna(False) & previous_relation.isna()
    )
    broken = unmatched & ~insufficient & ~missing_previous & (
        broken_basis | ~pair_contiguous.fillna(False)
    )
    no_unique = unmatched & ~insufficient & ~missing_previous & ~broken
    diagnostics.loc[insufficient] = [(DIAGNOSTIC_INSUFFICIENT,)] * int(insufficient.sum())
    diagnostics.loc[missing_previous] = [(DIAGNOSTIC_MISSING_PREVIOUS_ACTUAL,)] * int(missing_previous.sum())
    diagnostics.loc[broken] = [(DIAGNOSTIC_BROKEN_SEQUENCE,)] * int(broken.sum())
    diagnostics.loc[no_unique] = [(DIAGNOSTIC_NO_UNIQUE_MATCH,)] * int(no_unique.sum())

    close = _numeric_series(observations, CLOSE)
    ma5 = _numeric_series(observations, MA5)
    current_relation = (close >= ma5).astype("boolean")
    current_relation.loc[~trading.fillna(False) | close.isna() | ma5.isna()] = pd.NA

    frame = pd.DataFrame(
        {
            ASSET_ID: observations[ASSET_ID],
            TRADE_DATE: observations[TRADE_DATE],
            LIFECYCLE_STATE: lifecycle,
            TREND_STATE: trend,
            PREVIOUS_TREND_STATE: trend.groupby(observations[ASSET_ID], sort=False).shift(1),
            STATE_CHANGED: pd.Series(pd.NA, index=observations.index, dtype="boolean"),
            MARKET_FACT_STATUS: status,
            IS_TRADING_DAY: trading.astype(bool),
            LISTING_TRADING_DAY_NUMBER: listing_day,
            CLOSE: close,
            MA5: ma5,
            IS_ABOVE_OR_EQUAL_MA5: current_relation,
            LATEST_ACTUAL_TRADE_DATE: _date_series(observations, LATEST_ACTUAL_TRADE_DATE),
            LATEST_ACTUAL_CLOSE: _numeric_series(observations, LATEST_ACTUAL_CLOSE),
            LATEST_ACTUAL_MA5: _numeric_series(observations, LATEST_ACTUAL_MA5),
            LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5: latest_relation,
            PREVIOUS_ACTUAL_TRADE_DATE: _date_series(observations, PREVIOUS_ACTUAL_TRADE_DATE),
            PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5: previous_relation,
            STATE_BASIS_SEQUENCE_INTACT: basis_intact.astype(bool),
            ACTUAL_PAIR_CONTIGUOUS: pair_contiguous.astype(bool),
            PRICE_ADJUSTMENT: request.input_price_adjustment.value,
            RULE_VERSION_SET_ID: request.rule_version_set_id,
            PARAMETER_SET_ID: request.parameter_set_id,
            SOURCE_RULE_IDS: [SYSTEM_B_2_0_SOURCE_RULE_IDS] * len(observations),
            DIAGNOSTICS: diagnostics,
        },
        columns=SYSTEM_B_STATE_OUTPUT_COLUMNS,
    )
    comparable = frame[TREND_STATE].notna() & frame[PREVIOUS_TREND_STATE].notna()
    frame.loc[comparable, STATE_CHANGED] = (
        frame.loc[comparable, TREND_STATE] != frame.loc[comparable, PREVIOUS_TREND_STATE]
    ).astype(bool)
    return SystemBStateMachineResult(
        frame=frame,
        diagnostics=batch_diagnostics,
        metadata=_metadata(request),
    )
