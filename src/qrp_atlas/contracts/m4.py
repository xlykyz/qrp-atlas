"""Contracts and field constants for M4 facts and custom Theme index."""

from __future__ import annotations

from typing import Final


# ── M4 & Theme Index Field Constants ──
THEME_DAILY_RETURN: Final[str] = "theme_daily_return"
THEME_LIMIT_UP_COUNT: Final[str] = "theme_limit_up_count"
THEME_RETURN_RANK: Final[str] = "theme_return_rank"
EFFECTIVE_MEMBER_COUNT: Final[str] = "effective_member_count"
TOTAL_MEMBER_COUNT: Final[str] = "total_member_count"
COMPARISON_UNIVERSE_SIZE: Final[str] = "comparison_universe_size"
COMPARISON_UNIVERSE_VERSION: Final[str] = "comparison_universe_version"
QUALIFICATION_STATUS: Final[str] = "qualification_status"

INDEX_LEVEL: Final[str] = "index_level"
BASE_LEVEL: Final[str] = "base_level"
IS_THEME_MEMBER: Final[str] = "is_theme_member"
IS_M4_EFFECTIVE_MEMBER: Final[str] = "is_m4_effective_member"
EXCLUSION_REASON: Final[str] = "exclusion_reason"

CUSTOM_INDEX_TREND_STATE: Final[str] = "custom_index_trend_state"
CUSTOM_INDEX_TREND_RUN_DAYS: Final[str] = "custom_index_trend_run_days"
CUSTOM_INDEX_EPISODE_ID: Final[str] = "custom_index_episode_id"
KNOWLEDGE_DATE: Final[str] = "knowledge_date"

# ── Value Constants ──
QUALIFICATION_STATUS_NOT_CONFIGURED: Final[str] = "NOT_CONFIGURED"
COMPARISON_UNIVERSE_VERSION_V1: Final[str] = "m4_board_universe_v1"
DEFAULT_BASE_LEVEL: Final[float] = 1000.0

EXCLUSION_REASON_NEW_LISTING_LE_5: Final[str] = "NEW_LISTING_LE_5"
EXCLUSION_REASON_SUSPENDED: Final[str] = "SUSPENDED"

# ── Table Names & Versions ──
THEME_CUSTOM_INDEX_DAILY_TABLE: Final[str] = "theme_custom_index_daily"
THEME_CUSTOM_INDEX_STATE_TABLE: Final[str] = "theme_custom_index_state"
THEME_CUSTOM_INDEX_EPISODE_TABLE: Final[str] = "theme_custom_index_episode"
THEME_M4_OBSERVATION_TABLE: Final[str] = "theme_m4_observation"
THEME_PRODUCTION_RUN_TABLE: Final[str] = "theme_production_run"

THEME_CUSTOM_INDEX_VERSION: Final[str] = "theme_custom_index@1.0.0"
THEME_CUSTOM_INDEX_STATE_VERSION: Final[str] = "theme_custom_index_state@1.0.0"
THEME_CUSTOM_INDEX_EPISODE_VERSION: Final[str] = "theme_custom_index_episode@1.0.0"
THEME_M4_OBSERVATION_VERSION: Final[str] = "theme_m4_observation@1.0.0"
M4_CALCULATION_VERSION: Final[str] = THEME_M4_OBSERVATION_VERSION
