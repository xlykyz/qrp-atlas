"""Cross-sectional research foundation: conventions, operators, universes.

This package sits under ``qrp_atlas.indicators`` and provides reusable
cross-sectional transforms plus a minimal historical universe interface.
It deliberately stops short of strategies, neutralization, Top-N selection,
and multifactor combination (tasks 04-B through 04-E).
"""

from __future__ import annotations

from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    REQUIRED_CROSS_SECTION_COLUMNS,
    ensure_cross_section_frame,
    normalize_feature_columns,
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

__all__ = [
    "CrossSectionFrameError",
    "HistoricalUniverseRequest",
    "HistoricalUniverseSource",
    "REQUIRED_CROSS_SECTION_COLUMNS",
    "apply_cross_section_operators",
    "build_historical_universe",
    "cross_section_percentile_rank",
    "cross_section_rank",
    "cross_section_winsorize",
    "cross_section_zscore",
    "ensure_cross_section_frame",
    "normalize_feature_columns",
    "process_cross_section",
    "resolve_historical_universe",
    "sort_cross_section_frame",
]
