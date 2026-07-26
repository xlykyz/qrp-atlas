"""Versioned contracts for the System B 2.0 base trend state machine.

The contracts in this module describe deterministic indicator inputs and outputs.
They deliberately contain no database, market-data download, strategy, portfolio,
or execution behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

import pandas as pd

from qrp_atlas.contracts.fields import ASSET_ID, CLOSE, TRADE_DATE

MA5 = "ma5"
IS_TRADING_DAY = "is_trading_day"
LISTING_TRADING_DAY_NUMBER = "listing_trading_day_number"
TREND_STATE = "trend_state"
UNDERLYING_TREND_STATE = "underlying_trend_state"
PREVIOUS_TREND_STATE = "previous_trend_state"
STATE_CHANGED = "state_changed"
IS_ABOVE_OR_EQUAL_MA5 = "is_above_or_equal_ma5"
CONSECUTIVE_ABOVE_MA5_DAYS = "consecutive_above_ma5_days"
CONSECUTIVE_BELOW_MA5_DAYS = "consecutive_below_ma5_days"
RULE_VERSION_SET_ID = "rule_version_set_id"
PARAMETER_SET_ID = "parameter_set_id"
SOURCE_RULE_IDS = "source_rule_ids"
DIAGNOSTICS = "diagnostics"
PRICE_ADJUSTMENT = "price_adjustment"
PRODUCTION_RUN_ID = "production_run_id"
INPUT_SNAPSHOT_ID = "input_snapshot_id"
CALCULATION_VERSION = "calculation_version"
COMPLETED_AT = "completed_at"

SYSTEM_B_STATE_OBSERVATION_TABLE = "system_b_state_observation"
SYSTEM_B_LATEST_STATE_VIEW = "system_b_latest_state"
SYSTEM_B_PRODUCTION_RUN_TABLE = "system_b_production_run"
SYSTEM_B_CALCULATION_VERSION = "system_b_state_monitoring@1.0.1"

SYSTEM_B_2_0_RULE_VERSION_SET_ID = "system_b_2_0_draft_1__mts_8236965"
SYSTEM_B_2_0_PARAMETER_SET_ID = "system_b_2_0_draft_1_params_1"
SYSTEM_B_2_0_SOURCE_RULE_IDS: tuple[str, ...] = (
    "SB20.DATA.001",
    "SB20.DATA.002",
    "SB20.STATE.001",
    "SB20.STATE.002",
)

SYSTEM_B_STATE_INPUT_COLUMNS: tuple[str, ...] = (
    ASSET_ID,
    TRADE_DATE,
    IS_TRADING_DAY,
    LISTING_TRADING_DAY_NUMBER,
    CLOSE,
    MA5,
)

SYSTEM_B_STATE_OUTPUT_COLUMNS: tuple[str, ...] = (
    ASSET_ID,
    TRADE_DATE,
    TREND_STATE,
    UNDERLYING_TREND_STATE,
    PREVIOUS_TREND_STATE,
    STATE_CHANGED,
    IS_TRADING_DAY,
    LISTING_TRADING_DAY_NUMBER,
    CLOSE,
    MA5,
    IS_ABOVE_OR_EQUAL_MA5,
    CONSECUTIVE_ABOVE_MA5_DAYS,
    CONSECUTIVE_BELOW_MA5_DAYS,
    PRICE_ADJUSTMENT,
    RULE_VERSION_SET_ID,
    PARAMETER_SET_ID,
    SOURCE_RULE_IDS,
    DIAGNOSTICS,
)


class PriceAdjustment(str, Enum):
    """Declared price adjustment carried by the state-machine request."""

    FORWARD_ADJUSTED = "FORWARD_ADJUSTED"


class SystemBTrendState(str, Enum):
    """Formal System B 2.0 base trend states."""

    NEW_LISTING_WARMUP = "NEW_LISTING_WARMUP"
    BASE = "BASE"
    CANDIDATE = "CANDIDATE"
    ACTIVE = "ACTIVE"


@dataclass(frozen=True)
class SystemBStateMachineParameters:
    """Explicit parameter set; every field must be supplied by the caller."""

    price_adjustment: PriceAdjustment
    warmup_trading_days: int
    ma_period: int
    active_confirm_days: int
    exit_confirm_days: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "price.adjustment": self.price_adjustment.value,
            "new_listing.warmup_trading_days": self.warmup_trading_days,
            "trend.ma_period": self.ma_period,
            "trend.active_confirm_days": self.active_confirm_days,
            "trend.exit_confirm_days": self.exit_confirm_days,
        }


SYSTEM_B_2_0_PARAMETERS = SystemBStateMachineParameters(
    price_adjustment=PriceAdjustment.FORWARD_ADJUSTED,
    warmup_trading_days=10,
    ma_period=5,
    active_confirm_days=2,
    exit_confirm_days=2,
)


@dataclass(frozen=True)
class SystemBStateCheckpoint:
    """Frozen per-asset state sufficient for deterministic incremental continuation."""

    asset_id: str
    last_observation_date: pd.Timestamp
    trend_state: SystemBTrendState
    underlying_trend_state: SystemBTrendState
    listing_trading_day_number: int
    consecutive_above_ma5_days: int
    consecutive_below_ma5_days: int

    def to_dict(self) -> dict[str, Any]:
        return {
            ASSET_ID: self.asset_id,
            "last_observation_date": self.last_observation_date.isoformat(),
            TREND_STATE: self.trend_state.value,
            UNDERLYING_TREND_STATE: self.underlying_trend_state.value,
            LISTING_TRADING_DAY_NUMBER: self.listing_trading_day_number,
            CONSECUTIVE_ABOVE_MA5_DAYS: self.consecutive_above_ma5_days,
            CONSECUTIVE_BELOW_MA5_DAYS: self.consecutive_below_ma5_days,
        }


@dataclass(frozen=True)
class SystemBStateMachineRequest:
    """Versioned request for full-history or incremental batch calculation."""

    observations: pd.DataFrame
    parameters: SystemBStateMachineParameters
    input_price_adjustment: PriceAdjustment
    rule_version_set_id: str
    parameter_set_id: str
    initial_states: tuple[SystemBStateCheckpoint, ...]


@dataclass(frozen=True)
class SystemBStateMachineResult:
    """Calculated observations, resumable final states, and batch diagnostics."""

    frame: pd.DataFrame
    final_states: tuple[SystemBStateCheckpoint, ...]
    diagnostics: tuple[str, ...]
    metadata: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "frame": self.frame.to_dict(orient="records"),
            "final_states": [state.to_dict() for state in self.final_states],
            "diagnostics": list(self.diagnostics),
            "metadata": dict(self.metadata),
        }
