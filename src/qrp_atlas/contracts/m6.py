"""Contracts and field constants for M6 Market Sentiment complete facts."""

from __future__ import annotations

from typing import Final


# ── M6 Field Constants ──
MARKET_SCOPE: Final[str] = "market_scope"
LIMIT_UP_COUNT: Final[str] = "limit_up_count"
LIMIT_DOWN_COUNT: Final[str] = "limit_down_count"
CONSECUTIVE_LIMIT_UP_COUNT: Final[str] = "consecutive_limit_up_count"
MAX_CONSECUTIVE_LIMIT_UP_HEIGHT: Final[str] = "max_consecutive_limit_up_height"
PRE_LIMIT_UP_PREMIUM: Final[str] = "pre_limit_up_premium"

# ── Market Scope Values ──
MARKET_SCOPE_ALL_MARKET: Final[str] = "ALL_MARKET"
MARKET_SCOPE_MAIN_BOARD: Final[str] = "MAIN_BOARD"
MARKET_SCOPE_CHINEXT: Final[str] = "CHINEXT"
MARKET_SCOPE_STAR_MARKET: Final[str] = "STAR_MARKET"
MARKET_SCOPE_BSE: Final[str] = "BSE"

MARKET_SCOPES: Final[tuple[str, ...]] = (
    MARKET_SCOPE_ALL_MARKET,
    MARKET_SCOPE_MAIN_BOARD,
    MARKET_SCOPE_CHINEXT,
    MARKET_SCOPE_STAR_MARKET,
    MARKET_SCOPE_BSE,
)

# ── Table and Calculation Versions ──
MARKET_M6_OBSERVATION_TABLE: Final[str] = "market_m6_observation"
MARKET_M6_OBSERVATION_VERSION: Final[str] = "market_m6_observation@0.1.0"
M6_OBSERVATION_VERSION: Final[str] = MARKET_M6_OBSERVATION_VERSION
M6_CALCULATION_VERSION: Final[str] = MARKET_M6_OBSERVATION_VERSION
