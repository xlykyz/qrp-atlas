"""Task06-A System B asset-relative ranking production service."""

from .service import (
    SystemBAssetRankProductionError,
    build_canonical_a_share_universe,
    ensure_schema,
    get_asset_rank_component_audit,
    get_asset_rank_snapshot,
    run_asset_rank_daily,
)

__all__ = [
    "SystemBAssetRankProductionError",
    "build_canonical_a_share_universe",
    "ensure_schema",
    "get_asset_rank_component_audit",
    "get_asset_rank_snapshot",
    "run_asset_rank_daily",
]
