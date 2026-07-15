"""Cross-sectional research foundation: conventions, operators, universes, factors.

This package sits under ``qrp_atlas.indicators`` and provides reusable
cross-sectional transforms, historical universe helpers, formal raw-factor
generation (task 04-B) and industry/size neutralization (task 04-C). Top-N
selection, target weights and long-only strategy logic live in
``qrp_atlas.strategies`` (task 04-D); factor research analytics remain task 04-E.
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
from qrp_atlas.indicators.cross_section.neutralize import (
    NeutralizationError,
    neutralize_cross_section,
    neutralize_factor_frame,
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
    compute_daily_basic_factor,
    compute_log_market_cap_factor,
    compute_momentum_factor,
    compute_roe_factor,
    generate_factor_frame,
    get_factor_definition,
    list_factors,
    resolve_factor_requests,
)
from qrp_atlas.indicators.cross_section.market_factors import (
    compute_amihud_illiquidity_factor,
    compute_average_traded_amount_factor,
    compute_average_turnover_factor,
    compute_distance_to_high_factor,
    compute_downside_volatility_factor,
    compute_high_low_range_volatility_factor,
    compute_intermediate_momentum_factor,
    compute_price_efficiency_factor,
    compute_price_volume_correlation_factor,
    compute_realized_volatility_factor,
    compute_relative_volume_factor,
    compute_rolling_max_drawdown_factor,
    compute_short_term_reversal_factor,
    compute_trend_r_squared_factor,
    compute_trend_slope_factor,
    compute_turnover_change_factor,
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
    "compute_amihud_illiquidity_factor",
    "compute_average_traded_amount_factor",
    "compute_average_turnover_factor",
    "compute_distance_to_high_factor",
    "compute_downside_volatility_factor",
    "compute_high_low_range_volatility_factor",
    "compute_intermediate_momentum_factor",
    "compute_price_efficiency_factor",
    "compute_price_volume_correlation_factor",
    "compute_realized_volatility_factor",
    "compute_relative_volume_factor",
    "compute_rolling_max_drawdown_factor",
    "compute_short_term_reversal_factor",
    "compute_trend_r_squared_factor",
    "compute_trend_slope_factor",
    "compute_turnover_change_factor",
    "compute_book_to_price_factor",
    "compute_daily_basic_factor",
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
    "NeutralizationError",
    "neutralize_cross_section",
    "neutralize_factor_frame",
    "normalize_asset_id",
    "normalize_feature_columns",
    "normalize_trade_date",
    "normalize_trade_dates",
    "process_cross_section",
    "resolve_factor_requests",
    "resolve_historical_universe",
    "sort_cross_section_frame",
]
