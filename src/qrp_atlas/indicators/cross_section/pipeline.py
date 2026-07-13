"""Minimal composition entry for cross-sectional research.

``process_cross_section`` connects an optional historical universe with a
caller-supplied feature frame and reusable cross-sectional operators.

It is intentionally open for later tasks to plug in:

- momentum / fundamental / size factors
- industry neutralization
- Top-N selection
- multifactor combination

without requiring changes to this core path.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    ensure_cross_section_frame,
    normalize_feature_columns,
    sort_cross_section_frame,
)
from qrp_atlas.indicators.cross_section.operators import (
    RankMethod,
    apply_cross_section_operators,
)
from qrp_atlas.indicators.cross_section.universe import (
    HistoricalUniverseRequest,
    HistoricalUniverseSource,
    build_historical_universe,
)


def _align_features_to_universe(
    features: pd.DataFrame,
    universe: pd.DataFrame,
    *,
    feature_columns: Sequence[str],
    how: str = "left",
) -> pd.DataFrame:
    """Align feature rows to a universe on (trade_date, asset_id)."""
    if universe is None or universe.empty:
        cols = list(universe.columns) if universe is not None else [TRADE_DATE, ASSET_ID]
        for col in feature_columns:
            if col not in cols:
                cols.append(col)
        return pd.DataFrame(columns=cols)

    # Both sides must already use the shared trade_date/asset_id contract.
    left = ensure_cross_section_frame(universe, enforce_primary_key=True)
    right_cols = [TRADE_DATE, ASSET_ID] + [
        c for c in feature_columns if c in features.columns and c not in (TRADE_DATE, ASSET_ID)
    ]
    extra = [
        c
        for c in features.columns
        if c not in right_cols and c not in (TRADE_DATE, ASSET_ID)
    ]
    right = ensure_cross_section_frame(
        features[right_cols + extra],
        feature_columns=[c for c in feature_columns if c in features.columns],
        enforce_primary_key=True,
    )

    merged = left.merge(right, on=[TRADE_DATE, ASSET_ID], how=how, suffixes=("", "_feature"))
    # left/right keys were unique; result must remain unique on the join key
    return sort_cross_section_frame(merged)


def process_cross_section(
    features: pd.DataFrame | None = None,
    *,
    feature_columns: str | Sequence[str] | None = None,
    trade_dates: Sequence[Any] | Any | None = None,
    asset_ids: Sequence[str] | None = None,
    index_code: str | None = None,
    universe_source: HistoricalUniverseSource | None = None,
    universe: pd.DataFrame | None = None,
    universe_request: HistoricalUniverseRequest | None = None,
    operators: Sequence[str] = ("rank", "pct_rank", "winsorize", "zscore"),
    ascending: bool = True,
    rank_method: RankMethod = "average",
    winsor_limits: tuple[float, float] = (0.01, 0.99),
    zscore_ddof: int = 0,
    zscore_min_count: int = 2,
    prefix: str | None = None,
    align: str = "left",
    db_path: Any = None,
    con: Any = None,
    index_query: Any = None,
    operator_kwargs: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Compose universe resolution, feature alignment and cross-section ops.

    Typical usage patterns:

    1. Caller already has a feature frame for a historical stock pool::

        process_cross_section(features_df, feature_columns=["momentum"])

    2. Build an index universe, then join caller features and transform::

        process_cross_section(
            features_df,
            feature_columns=["momentum"],
            trade_dates=dates,
            index_code="000300.SH",
        )

    3. Explicit asset list without an external feature table::

        process_cross_section(
            features_df,
            feature_columns=["value"],
            trade_dates=dates,
            asset_ids=["A", "B"],
        )

    Notes:
        - Does not mutate caller inputs.
        - Cross-section operators still run per trade_date only.
        - ``trade_date`` values are normalized before merge/operators.
        - Caller feature frames and explicit universes must have unique
          ``(trade_date, asset_id)`` keys; duplicates raise
          ``CrossSectionFrameError``.
        - Does not implement Top-N, neutralization or strategy selection.
    """
    if features is None and universe is None and universe_request is None:
        if trade_dates is None and asset_ids is None and index_code is None:
            raise CrossSectionFrameError(
                "process_cross_section requires features and/or a universe specification"
            )

    feature_frame: pd.DataFrame | None = None
    resolved_features: list[str] = []
    if features is not None:
        if feature_columns is None:
            candidate = [c for c in features.columns if c not in (TRADE_DATE, ASSET_ID)]
            resolved_features = normalize_feature_columns(candidate)
        else:
            resolved_features = normalize_feature_columns(feature_columns)
        feature_frame = ensure_cross_section_frame(
            features,
            feature_columns=resolved_features if resolved_features else None,
            require_features=bool(resolved_features),
            enforce_primary_key=True,
        )
    elif feature_columns is not None:
        resolved_features = normalize_feature_columns(feature_columns)

    resolved_universe = universe
    if resolved_universe is None and universe_request is not None:
        resolved_universe = build_historical_universe(
            universe_request.normalized_dates(),
            asset_ids=universe_request.asset_ids,
            index_code=universe_request.index_code,
            source=universe_request.source,
            db_path=db_path,
            con=con,
            index_query=index_query,
        )
    elif resolved_universe is None and (
        trade_dates is not None or asset_ids is not None or index_code is not None
    ):
        dates = trade_dates
        if dates is None and feature_frame is not None and not feature_frame.empty:
            dates = sorted(feature_frame[TRADE_DATE].unique().tolist())
        resolved_universe = build_historical_universe(
            dates if dates is not None else [],
            asset_ids=asset_ids,
            index_code=index_code,
            source=universe_source,
            db_path=db_path,
            con=con,
            index_query=index_query,
        )
    elif resolved_universe is not None:
        # Caller-supplied universes must satisfy the same primary-key contract.
        resolved_universe = ensure_cross_section_frame(
            resolved_universe,
            enforce_primary_key=True,
        )

    if resolved_universe is not None and feature_frame is not None:
        working = _align_features_to_universe(
            feature_frame,
            resolved_universe,
            feature_columns=resolved_features,
            how=align,
        )
    elif resolved_universe is not None:
        working = ensure_cross_section_frame(resolved_universe, enforce_primary_key=True)
    elif feature_frame is not None:
        working = feature_frame
    else:
        working = ensure_cross_section_frame(None, feature_columns=resolved_features)

    if not resolved_features or working.empty:
        return sort_cross_section_frame(working)

    present_features = [c for c in resolved_features if c in working.columns]
    if not present_features:
        return sort_cross_section_frame(working)

    kwargs = dict(operator_kwargs or {})
    return apply_cross_section_operators(
        working,
        present_features,
        operators=operators,
        ascending=kwargs.get("ascending", ascending),
        method=kwargs.get("method", kwargs.get("rank_method", rank_method)),
        winsor_limits=kwargs.get("winsor_limits", winsor_limits),
        zscore_ddof=kwargs.get("zscore_ddof", zscore_ddof),
        zscore_min_count=kwargs.get("zscore_min_count", zscore_min_count),
        prefix=kwargs.get("prefix", prefix),
    )
