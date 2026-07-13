"""Minimal historical universe helpers for cross-sectional research.

Universe construction is intentionally narrow for task 04-A:

- explicit asset lists provided by the caller;
- index membership as of each historical trade date via
  ``query_index_components_as_of``;
- multi-date expansion with stable (trade_date, asset_id) ordering.

It does NOT implement ST / suspension / liquidity filters or full-market
listing-status universes (deferred to later 04-x work).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, INDEX_CODE, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    empty_cross_section_frame,
    normalize_asset_id,
    normalize_trade_date,
    normalize_trade_dates,
    sort_cross_section_frame,
)

HistoricalUniverseSource = Literal["explicit", "index"]

# Optional query hook for tests; production default resolves lazily.
IndexComponentsQuery = Callable[..., pd.DataFrame]


@dataclass(frozen=True)
class HistoricalUniverseRequest:
    """Declarative request for a historical research universe.

    ``trade_dates`` accepts the same shapes as ``build_historical_universe``:
    a single date-like scalar, a sequence of dates, or an empty sequence.
    Strings are treated as scalars and never expanded character-by-character.
    """

    trade_dates: Sequence[Any] | Any
    source: HistoricalUniverseSource = "explicit"
    asset_ids: Sequence[str] | None = None
    index_code: str | None = None

    def normalized_dates(self) -> list[pd.Timestamp]:
        return normalize_trade_dates(self.trade_dates)


def _normalize_assets(asset_ids: Sequence[str] | None) -> list[str]:
    if asset_ids is None:
        return []
    if isinstance(asset_ids, str):
        items = [asset_ids]
    else:
        items = list(asset_ids)
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        asset = normalize_asset_id(item)
        if asset in seen:
            continue
        seen.add(asset)
        out.append(asset)
    return out


def _empty_universe(*, extra_columns: Sequence[str] | None = None) -> pd.DataFrame:
    return empty_cross_section_frame(extra_columns=extra_columns)


def build_historical_universe(
    trade_dates: Sequence[Any] | Any,
    *,
    asset_ids: Sequence[str] | None = None,
    index_code: str | None = None,
    source: HistoricalUniverseSource | None = None,
    db_path: Any = None,
    con: Any = None,
    index_query: IndexComponentsQuery | None = None,
) -> pd.DataFrame:
    """Build a historical universe frame keyed by trade_date and asset_id.

    Args:
        trade_dates: one date-like value or a sequence of dates.
        asset_ids: explicit assets. Empty sequence yields an empty universe and
            never expands into a full-market set.
        index_code: required when source is ``index``.
        source: ``explicit`` or ``index``. Inferred when omitted:
            index_code present -> index; otherwise explicit.
        db_path / con: DuckDB location used by the index query service.
        index_query: injectable replacement for ``query_index_components_as_of``
            (primarily for tests).

    Returns:
        DataFrame with columns at least ``trade_date`` and ``asset_id``, sorted
        stably by both. ``trade_date`` is timezone-naive midnight Timestamp.
        Index universes also retain ``index_code`` and ``snapshot_date`` when
        available.

    Notes:
        Deterministic de-duplication is applied for assets/components generated
        by this helper. Caller-supplied feature/universe frames that already
        contain duplicate keys are rejected by ``ensure_cross_section_frame``.
    """
    dates = normalize_trade_dates(trade_dates)
    resolved_source: HistoricalUniverseSource
    if source is None:
        resolved_source = "index" if index_code else "explicit"
    else:
        resolved_source = source

    if resolved_source == "explicit":
        assets = _normalize_assets(asset_ids)
        if not dates or not assets:
            return _empty_universe()
        rows = [
            {TRADE_DATE: trade_date, ASSET_ID: asset}
            for trade_date in dates
            for asset in assets
        ]
        return sort_cross_section_frame(pd.DataFrame(rows))

    if resolved_source != "index":
        raise CrossSectionFrameError(f"unsupported universe source: {resolved_source!r}")

    if not index_code or not str(index_code).strip():
        raise CrossSectionFrameError("index_code is required for index universe")
    code = str(index_code).strip()

    if not dates:
        return _empty_universe(extra_columns=[INDEX_CODE, "snapshot_date"])

    query = index_query
    if query is None:
        from qrp_atlas.backtest.pit_queries import query_index_components_as_of

        query = query_index_components_as_of

    frames: list[pd.DataFrame] = []
    for trade_date in dates:
        # query service accepts date-like values; pass normalized Timestamp.
        components = query(
            as_of_date=trade_date,
            index_code=code,
            db_path=db_path,
            con=con,
        )
        if components is None or components.empty:
            continue
        piece = pd.DataFrame(
            {
                TRADE_DATE: trade_date,
                ASSET_ID: [normalize_asset_id(v) for v in components[ASSET_ID].tolist()],
            }
        )
        if INDEX_CODE in components.columns:
            piece[INDEX_CODE] = components[INDEX_CODE].astype(str).to_numpy()
        else:
            piece[INDEX_CODE] = code
        if "snapshot_date" in components.columns:
            piece["snapshot_date"] = components["snapshot_date"].to_numpy()
        if "weight" in components.columns:
            piece["weight"] = pd.to_numeric(components["weight"], errors="coerce").to_numpy()
        frames.append(piece)

    if not frames:
        return _empty_universe(extra_columns=[INDEX_CODE, "snapshot_date", "weight"])

    out = pd.concat(frames, ignore_index=True)
    # Deterministic de-duplication for helper-generated universes only.
    out = out.drop_duplicates(subset=[TRADE_DATE, ASSET_ID], keep="first")
    out[TRADE_DATE] = [normalize_trade_date(v) for v in out[TRADE_DATE].tolist()]
    return sort_cross_section_frame(out)


def resolve_historical_universe(
    request: HistoricalUniverseRequest,
    *,
    db_path: Any = None,
    con: Any = None,
    index_query: IndexComponentsQuery | None = None,
) -> pd.DataFrame:
    """Resolve a :class:`HistoricalUniverseRequest` into a universe frame."""
    return build_historical_universe(
        request.normalized_dates(),
        asset_ids=request.asset_ids,
        index_code=request.index_code,
        source=request.source,
        db_path=db_path,
        con=con,
        index_query=index_query,
    )
