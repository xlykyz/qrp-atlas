"""Contracts and field constants for M5 Theme popularity facts."""

from __future__ import annotations

from typing import Final


# ── M5 business facts ──
THEME_MEMBER_COUNT: Final[str] = "theme_member_count"
THEME_HOT_STOCK_COUNT: Final[str] = "theme_hot_stock_count"
THEME_HOT_STOCK_RATIO: Final[str] = "theme_hot_stock_ratio"
THEME_HOT_LIST_APPEARANCE_COUNT: Final[str] = "theme_hot_list_appearance_count"
THEME_HOT_SOURCE_COUNT: Final[str] = "theme_hot_source_count"

# ── Table and calculation versions ──
THEME_M5_OBSERVATION_TABLE: Final[str] = "theme_m5_observation"
THEME_M5_OBSERVATION_VERSION: Final[str] = "theme_m5_observation@0.1.0"
M5_OBSERVATION_VERSION: Final[str] = THEME_M5_OBSERVATION_VERSION
M5_CALCULATION_VERSION: Final[str] = THEME_M5_OBSERVATION_VERSION
