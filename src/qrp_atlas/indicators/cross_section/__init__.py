"""Cross-sectional research foundation: conventions, operators, universes, factors.

This package sits under ``qrp_atlas.indicators`` and provides reusable
cross-sectional transforms, historical universe helpers, and formal raw-factor
generation (task 04-B). It deliberately stops short of neutralization, Top-N
selection, multifactor combination and strategy logic (tasks 04-C through 04-E).
"""

from __future__ import annotations

from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    REQUIRED_CROSS_SECTION_COLUMNS,
    enforce_cross_section_primary_key,
    ensure_cross_section_frame,
    normalize_asset_id,
    normalize_feature_columns,
    normalize_trade_date,
    normalize_trade_dates,
    sort_cross_section_frame,
)
from qrp_atlas.indicators.cross_section.operators import (
    apply_cross_section_operators,
    cross_section_percentile_rank,
    cross_section_rank,
    cross_section_winsorize,
    cross_section_zscore,
)
from qrp_atlas.indicators.cross_section.pipeline import process_cross_section
from qrp_atlas.indicators.cross_section.universe import (
    HistoricalUniverseRequest,
    HistoricalUniverseSource,
    build_historical_universe,
    resolve_historical_universe,
)
from qrp_atlas.indicators.cross_section.factors import (
    FACTOR_DEFINITIONS,
    FactorDefinition,
    FactorError,
    FactorParameterSpec,
    FactorRequest,
    FactorRequestError,
    ResolvedFactorRequest,
    UnknownFactorError,
    compute_book_to_price_factor,
    compute_log_market_cap_factor,
    compute_momentum_factor,
    compute_roe_factor,
    generate_factor_frame,
    get_factor_definition,
    list_factors,
    resolve_factor_requests,
)

__all__ = [
    "CrossSectionFrameError",
    "FACTOR_DEFINITIONS",
    "FactorDefinition",
    "FactorError",
    "FactorParameterSpec",
    "FactorRequest",
    "FactorRequestError",
    "HistoricalUniverseRequest",
    "HistoricalUniverseSource",
    "REQUIRED_CROSS_SECTION_COLUMNS",
    "ResolvedFactorRequest",
    "UnknownFactorError",
    "apply_cross_section_operators",
    "build_historical_universe",
    "compute_book_to_price_factor",
    "compute_log_market_cap_factor",
    "compute_momentum_factor",
    "compute_roe_factor",
    "cross_section_percentile_rank",
    "cross_section_rank",
    "cross_section_winsorize",
    "cross_section_zscore",
    "enforce_cross_section_primary_key",
    "ensure_cross_section_frame",
    "generate_factor_frame",
    "get_factor_definition",
    "list_factors",
    "normalize_asset_id",
    "normalize_feature_columns",
    "normalize_trade_date",
    "normalize_trade_dates",
    "process_cross_section",
    "resolve_factor_requests",
    "resolve_historical_universe",
    "sort_cross_section_frame",
]
