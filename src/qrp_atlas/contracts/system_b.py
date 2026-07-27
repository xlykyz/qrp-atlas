"""Versioned contracts for fact-derived System B 2.0 trend observations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

import pandas as pd

from qrp_atlas.contracts.fields import ASSET_ID, CLOSE, TRADE_DATE

MA5 = "ma5"
MA5_WINDOW_COMPLETE = "ma5_window_complete"
IS_TRADING_DAY = "is_trading_day"
MARKET_FACT_STATUS = "market_fact_status"
LISTING_TRADING_DAY_NUMBER = "listing_trading_day_number"
CONFIRMED_LISTING_TRADING_DAY_COUNT = "confirmed_listing_trading_day_count"
LISTING_TRADING_DAY_NUMBER_IS_EXACT = "listing_trading_day_number_is_exact"
LIFECYCLE_STATE = "lifecycle_state"
TREND_STATE = "trend_state"
PREVIOUS_TREND_STATE = "previous_trend_state"
STATE_CHANGED = "state_changed"
IS_ABOVE_OR_EQUAL_MA5 = "is_above_or_equal_ma5"
LATEST_ACTUAL_TRADE_DATE = "latest_actual_trade_date"
LATEST_ACTUAL_CLOSE = "latest_actual_close"
LATEST_ACTUAL_MA5 = "latest_actual_ma5"
LATEST_ACTUAL_MA5_WINDOW_COMPLETE = "latest_actual_ma5_window_complete"
LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5 = "latest_actual_is_above_or_equal_ma5"
PREVIOUS_ACTUAL_TRADE_DATE = "previous_actual_trade_date"
PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5 = "previous_actual_is_above_or_equal_ma5"
PREVIOUS_ACTUAL_MA5_WINDOW_COMPLETE = "previous_actual_ma5_window_complete"
STATE_BASIS_SEQUENCE_INTACT = "state_basis_sequence_intact"
ACTUAL_PAIR_CONTIGUOUS = "actual_pair_contiguous"
RULE_VERSION_SET_ID = "rule_version_set_id"
PARAMETER_SET_ID = "parameter_set_id"
SOURCE_RULE_IDS = "source_rule_ids"
DIAGNOSTICS = "diagnostics"
PRICE_ADJUSTMENT = "price_adjustment"
PRODUCTION_RUN_ID = "production_run_id"
INPUT_SNAPSHOT_ID = "input_snapshot_id"
CALCULATION_VERSION = "calculation_version"
COMPLETED_AT = "completed_at"
EPISODE_ID = "episode_id"
EPISODE_NO = "episode_no"
EPISODE_START_DATE = "episode_start_date"
EPISODE_CONFIRMED_DATE = "episode_confirmed_date"
EPISODE_END_DATE = "episode_end_date"
MA5_REENTRY_COUNT = "ma5_reentry_count"
CREATED_RUN_ID = "created_run_id"
RULE_VERSION = "rule_version"
DAYS_SINCE_START = "days_since_start"
DAYS_SINCE_CONFIRMED = "days_since_confirmed"
EPISODE_RETURN = "episode_return"
PEAK_RETURN = "peak_return"
DRAWDOWN_FROM_PEAK = "drawdown_from_peak"
IS_EPISODE_CONFIRMED = "is_episode_confirmed"
IS_EPISODE_END = "is_episode_end"
STATE_TRANSITION = "state_transition"
MA10 = "ma10"

SYSTEM_B_STATE_OBSERVATION_TABLE = "system_b_state_observation"
SYSTEM_B_LATEST_STATE_VIEW = "system_b_latest_state"
SYSTEM_B_PRODUCTION_RUN_TABLE = "system_b_production_run"
SYSTEM_B_CALCULATION_VERSION = "system_b_fact_derived_state@2.1.0"
SYSTEM_B_EPISODE_TABLE = "system_b_episode"
SYSTEM_B_EPISODE_OBSERVATION_TABLE = "system_b_episode_observation"
SYSTEM_B_EPISODE_RULE_VERSION = "system_b_episode@1.0.0__user_20260727"

SYSTEM_B_2_0_RULE_VERSION_SET_ID = "system_b_2_0_fact_derived_ma5_complete_1__user_20260726"
SYSTEM_B_2_0_PARAMETER_SET_ID = "system_b_2_0_fact_derived_ma5_complete_1_params_1"
SYSTEM_B_2_0_SOURCE_RULE_IDS: tuple[str, ...] = (
    "SB20.DATA.001",
    "SB20.DATA.002",
    "SB20.STATE.001",
    "SB20.STATE.002",
)

SYSTEM_B_STATE_INPUT_COLUMNS: tuple[str, ...] = (
    ASSET_ID,
    TRADE_DATE,
    MARKET_FACT_STATUS,
    IS_TRADING_DAY,
    LISTING_TRADING_DAY_NUMBER,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    LISTING_TRADING_DAY_NUMBER_IS_EXACT,
    CLOSE,
    MA5,
    MA5_WINDOW_COMPLETE,
    LATEST_ACTUAL_TRADE_DATE,
    LATEST_ACTUAL_CLOSE,
    LATEST_ACTUAL_MA5,
    LATEST_ACTUAL_MA5_WINDOW_COMPLETE,
    LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    PREVIOUS_ACTUAL_TRADE_DATE,
    PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    PREVIOUS_ACTUAL_MA5_WINDOW_COMPLETE,
    STATE_BASIS_SEQUENCE_INTACT,
    ACTUAL_PAIR_CONTIGUOUS,
)

SYSTEM_B_STATE_OUTPUT_COLUMNS: tuple[str, ...] = (
    ASSET_ID,
    TRADE_DATE,
    LIFECYCLE_STATE,
    TREND_STATE,
    PREVIOUS_TREND_STATE,
    STATE_CHANGED,
    MARKET_FACT_STATUS,
    IS_TRADING_DAY,
    LISTING_TRADING_DAY_NUMBER,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    LISTING_TRADING_DAY_NUMBER_IS_EXACT,
    CLOSE,
    MA5,
    MA5_WINDOW_COMPLETE,
    IS_ABOVE_OR_EQUAL_MA5,
    LATEST_ACTUAL_TRADE_DATE,
    LATEST_ACTUAL_CLOSE,
    LATEST_ACTUAL_MA5,
    LATEST_ACTUAL_MA5_WINDOW_COMPLETE,
    LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    PREVIOUS_ACTUAL_TRADE_DATE,
    PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    PREVIOUS_ACTUAL_MA5_WINDOW_COMPLETE,
    STATE_BASIS_SEQUENCE_INTACT,
    ACTUAL_PAIR_CONTIGUOUS,
    PRICE_ADJUSTMENT,
    RULE_VERSION_SET_ID,
    PARAMETER_SET_ID,
    SOURCE_RULE_IDS,
    DIAGNOSTICS,
)


class PriceAdjustment(str, Enum):
    FORWARD_ADJUSTED = "FORWARD_ADJUSTED"


class SystemBTrendState(str, Enum):
    BASE = "BASE"
    CANDIDATE = "CANDIDATE"
    ACTIVE = "ACTIVE"


class SystemBLifecycleState(str, Enum):
    NEW_LISTING_WARMUP = "NEW_LISTING_WARMUP"
    NORMAL = "NORMAL"


class SystemBMarketFactStatus(str, Enum):
    ACTUAL_TRADING = "ACTUAL_TRADING"
    EXPLICIT_NON_TRADING = "EXPLICIT_NON_TRADING"
    UNRESOLVED_MISSING = "UNRESOLVED_MISSING"


@dataclass(frozen=True)
class SystemBStateMachineParameters:
    price_adjustment: PriceAdjustment
    warmup_trading_days: int
    ma_period: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "price.adjustment": self.price_adjustment.value,
            "new_listing.warmup_trading_days": self.warmup_trading_days,
            "trend.ma_period": self.ma_period,
        }


SYSTEM_B_2_0_PARAMETERS = SystemBStateMachineParameters(
    price_adjustment=PriceAdjustment.FORWARD_ADJUSTED,
    warmup_trading_days=10,
    ma_period=5,
)


@dataclass(frozen=True)
class SystemBStateMachineRequest:
    observations: pd.DataFrame
    parameters: SystemBStateMachineParameters
    input_price_adjustment: PriceAdjustment
    rule_version_set_id: str
    parameter_set_id: str


@dataclass(frozen=True)
class SystemBStateMachineResult:
    frame: pd.DataFrame
    diagnostics: tuple[str, ...]
    metadata: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "frame": self.frame.to_dict(orient="records"),
            "diagnostics": list(self.diagnostics),
            "metadata": dict(self.metadata),
        }
