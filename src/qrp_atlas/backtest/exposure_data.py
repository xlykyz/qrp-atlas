"""Prepare industry and size exposure panels for cross-sectional neutralization.

Architecture boundary:

```text
contracts / DuckDB / PIT industry query
  -> prepare_cross_section_exposure_panel(...)
  -> indicators.neutralize_factor_frame(exposure_panel=...)
```

Indicators must not import this module or open DuckDB. Industry membership is
resolved with the existing task 03-C as-of semantics; market-cap uses only the
same-day available value (no forward fill).
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from typing import Any

import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    CIRC_MV,
    CLASSIFICATION_SYSTEM,
    FLOAT_CAP,
    INDUSTRY_CODE,
    INDUSTRY_LEVEL,
    MARKET_CAP,
    TICKER,
    TOTAL_MV,
    TRADE_DATE,
)
from qrp_atlas.indicators.cross_section.conventions import (
    empty_cross_section_frame,
    ensure_cross_section_frame,
    normalize_asset_id,
    normalize_trade_date,
    sort_cross_section_frame,
)

IndustryQuery = Callable[..., pd.DataFrame]

DEFAULT_CLASSIFICATION_SYSTEM = "sw2021"
DEFAULT_INDUSTRY_LEVEL = 1
LOG_MARKET_CAP = "log_market_cap"
_SIZE_FIELDS: tuple[str, ...] = (MARKET_CAP, FLOAT_CAP, TOTAL_MV, CIRC_MV)
_EXPOSURE_COLUMNS: tuple[str, ...] = (
    TRADE_DATE,
    ASSET_ID,
    INDUSTRY_CODE,
    LOG_MARKET_CAP,
)


class ExposurePanelError(ValueError):
    """Raised when an exposure panel cannot be prepared or validated."""


def _as_finite_series(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")

    def _finite(x: Any) -> bool:
        return bool(pd.notna(x) and math.isfinite(float(x)))

    return values.where(values.map(_finite))


def _resolve_id_column(df: pd.DataFrame, *, label: str) -> str:
    if ASSET_ID in df.columns:
        return ASSET_ID
    if TICKER in df.columns:
        return TICKER
    raise ExposurePanelError(
        f"{label} requires an asset identifier column ({ASSET_ID} or {TICKER})"
    )


def _reject_duplicate_keys(df: pd.DataFrame, *, label: str) -> None:
    if df is None or df.empty:
        return
    if TRADE_DATE not in df.columns or ASSET_ID not in df.columns:
        return
    duplicated = df.duplicated(subset=[TRADE_DATE, ASSET_ID], keep=False)
    if bool(duplicated.any()):
        sample = (
            df.loc[duplicated, [TRADE_DATE, ASSET_ID]]
            .drop_duplicates()
            .head(5)
            .to_dict(orient="records")
        )
        raise ExposurePanelError(
            f"{label} contains duplicate (trade_date, asset_id) keys: {sample}"
        )


def _normalize_size_panel(
    size_panel: pd.DataFrame | None,
    *,
    size_field: str,
) -> pd.DataFrame:
    if size_panel is None:
        return pd.DataFrame(columns=[TRADE_DATE, ASSET_ID, size_field])
    if not isinstance(size_panel, pd.DataFrame):
        raise ExposurePanelError("size_panel must be a pandas DataFrame")
    if size_field not in _SIZE_FIELDS:
        raise ExposurePanelError(
            f"size_field must be one of {list(_SIZE_FIELDS)}; got {size_field!r}"
        )
    if size_panel.empty:
        out = size_panel.copy()
        if ASSET_ID not in out.columns and TICKER in out.columns:
            out[ASSET_ID] = out[TICKER].astype(str)
        return out
    if TRADE_DATE not in size_panel.columns:
        raise ExposurePanelError("size_panel missing required column: 'trade_date'")
    if size_field not in size_panel.columns:
        raise ExposurePanelError(f"size_panel missing required column: {size_field!r}")
    id_col = _resolve_id_column(size_panel, label="size_panel")
    out = size_panel.copy()
    out[TRADE_DATE] = [normalize_trade_date(v) for v in out[TRADE_DATE].tolist()]
    out[ASSET_ID] = [normalize_asset_id(v) for v in out[id_col].tolist()]
    _reject_duplicate_keys(out, label="size_panel")
    return out


def _normalize_prepared_industry_panel(industry_panel: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(industry_panel, pd.DataFrame):
        raise ExposurePanelError("industry_panel must be a pandas DataFrame")
    if industry_panel.empty:
        out = industry_panel.copy()
        if ASSET_ID not in out.columns and TICKER in out.columns:
            out[ASSET_ID] = out[TICKER].astype(str)
        return out
    if TRADE_DATE not in industry_panel.columns:
        raise ExposurePanelError("industry_panel missing required column: 'trade_date'")
    if INDUSTRY_CODE not in industry_panel.columns:
        raise ExposurePanelError("industry_panel missing required column: 'industry_code'")
    id_col = _resolve_id_column(industry_panel, label="industry_panel")
    out = industry_panel.copy()
    out[TRADE_DATE] = [normalize_trade_date(v) for v in out[TRADE_DATE].tolist()]
    out[ASSET_ID] = [normalize_asset_id(v) for v in out[id_col].tolist()]
    out[INDUSTRY_CODE] = out[INDUSTRY_CODE].map(
        lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip()
    )
    out.loc[out[INDUSTRY_CODE] == "", INDUSTRY_CODE] = None
    _reject_duplicate_keys(out, label="industry_panel")
    return out


def _empty_exposure_frame() -> pd.DataFrame:
    out = empty_cross_section_frame(extra_columns=[INDUSTRY_CODE, LOG_MARKET_CAP])
    # ensure stable dtype for optional columns
    out[INDUSTRY_CODE] = pd.Series(dtype=object)
    out[LOG_MARKET_CAP] = pd.Series(dtype="float64")
    return out[list(_EXPOSURE_COLUMNS)]


def _log_market_cap_series(raw: pd.Series) -> pd.Series:
    values = _as_finite_series(raw)
    return values.where(values > 0).map(
        lambda x: math.log(float(x)) if pd.notna(x) else math.nan
    )


def _load_industry_for_date(
    *,
    as_of_date: Any,
    asset_ids: Sequence[str],
    industry_query: IndustryQuery | None,
    classification_system: str,
    industry_level: int,
    db_path: Any,
    con: Any,
) -> pd.DataFrame:
    query = industry_query
    if query is None:
        from qrp_atlas.backtest.pit_queries import query_industry_as_of

        query = query_industry_as_of
    if not asset_ids:
        return pd.DataFrame(columns=[ASSET_ID, INDUSTRY_CODE])
    frame = query(
        as_of_date=as_of_date,
        asset_ids=list(asset_ids),
        classification_system=classification_system,
        industry_level=industry_level,
        db_path=db_path,
        con=con,
    )
    if frame is None or frame.empty:
        return pd.DataFrame(columns=[ASSET_ID, INDUSTRY_CODE])
    out = frame.copy()
    if ASSET_ID not in out.columns:
        raise ExposurePanelError("industry query result missing asset_id")
    if INDUSTRY_CODE not in out.columns:
        raise ExposurePanelError("industry query result missing industry_code")
    out[ASSET_ID] = [normalize_asset_id(v) for v in out[ASSET_ID].tolist()]
    out[INDUSTRY_CODE] = out[INDUSTRY_CODE].map(
        lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip()
    )
    out.loc[out[INDUSTRY_CODE] == "", INDUSTRY_CODE] = None
    # One membership per asset at the requested level (query already conflict-checks).
    out = out.drop_duplicates(subset=[ASSET_ID], keep="last")
    return out[[ASSET_ID, INDUSTRY_CODE]]


def prepare_cross_section_exposure_panel(
    universe: pd.DataFrame,
    *,
    size_panel: pd.DataFrame | None = None,
    industry_panel: pd.DataFrame | None = None,
    industry_query: IndustryQuery | None = None,
    classification_system: str = DEFAULT_CLASSIFICATION_SYSTEM,
    industry_level: int = DEFAULT_INDUSTRY_LEVEL,
    size_field: str = MARKET_CAP,
    db_path: Any = None,
    con: Any = None,
) -> pd.DataFrame:
    """Build a prepared exposure panel for neutralization.

    Returns columns:

    - trade_date
    - asset_id
    - industry_code
    - log_market_cap

    Industry membership is point-in-time for each target trade_date via
    ``query_industry_as_of`` (or an injected ``industry_query`` /
    pre-aligned ``industry_panel``). Market cap uses only the same calendar
    trade_date from ``size_panel`` and is transformed to natural log; non-positive
    or non-finite values become NaN without forward fill.

    Args:
        universe: historical stock pool with trade_date / asset_id.
        size_panel: same-day market-cap source (market_cap / float_cap / total_mv / circ_mv).
        industry_panel: optional pre-aligned industry codes keyed by trade_date + asset_id.
        industry_query: injectable replacement for ``query_industry_as_of``.
        classification_system: default ``sw2021``.
        industry_level: default ``1`` (primary industry).
        size_field: market-cap field name in ``size_panel``.
        db_path / con: DuckDB source for industry query when panel is omitted.
    """
    uni = ensure_cross_section_frame(universe, enforce_primary_key=True)
    if uni.empty:
        return _empty_exposure_frame()

    if not isinstance(classification_system, str) or not classification_system.strip():
        raise ExposurePanelError("classification_system must be a non-empty string")
    if not isinstance(industry_level, int) or isinstance(industry_level, bool) or industry_level < 1:
        raise ExposurePanelError(
            f"industry_level must be a positive integer; got {industry_level!r}"
        )

    sizes = _normalize_size_panel(size_panel, size_field=size_field)
    prepared_industry = None
    if industry_panel is not None:
        prepared_industry = _normalize_prepared_industry_panel(industry_panel)

    pieces: list[pd.DataFrame] = []
    for trade_date, day_uni in uni.groupby(TRADE_DATE, sort=False):
        assets = day_uni[ASSET_ID].tolist()
        piece = day_uni[[TRADE_DATE, ASSET_ID]].copy()

        # Industry for this target date only.
        if prepared_industry is not None:
            day_ind = prepared_industry.loc[
                prepared_industry[TRADE_DATE] == normalize_trade_date(trade_date),
                [ASSET_ID, INDUSTRY_CODE],
            ]
        else:
            day_ind = _load_industry_for_date(
                as_of_date=trade_date,
                asset_ids=assets,
                industry_query=industry_query,
                classification_system=str(classification_system).strip(),
                industry_level=int(industry_level),
                db_path=db_path,
                con=con,
            )
        piece = piece.merge(day_ind, on=ASSET_ID, how="left")
        if INDUSTRY_CODE not in piece.columns:
            piece[INDUSTRY_CODE] = None

        # Same-day market cap only (no cross-date fill).
        if sizes.empty:
            piece[LOG_MARKET_CAP] = math.nan
        else:
            day_size = sizes.loc[
                (sizes[TRADE_DATE] == normalize_trade_date(trade_date))
                & (sizes[ASSET_ID].isin(assets)),
                [ASSET_ID, size_field],
            ]
            piece = piece.merge(day_size, on=ASSET_ID, how="left")
            piece[LOG_MARKET_CAP] = _log_market_cap_series(piece[size_field])
            if size_field in piece.columns and size_field != LOG_MARKET_CAP:
                piece = piece.drop(columns=[size_field])

        pieces.append(piece[list(_EXPOSURE_COLUMNS)])

    out = pd.concat(pieces, ignore_index=True) if pieces else _empty_exposure_frame()
    out = ensure_cross_section_frame(
        out,
        feature_columns=[INDUSTRY_CODE, LOG_MARKET_CAP],
        enforce_primary_key=True,
    )
    # Keep industry_code as object with missing as None/NA, log_market_cap finite-or-NaN.
    out[LOG_MARKET_CAP] = _as_finite_series(out[LOG_MARKET_CAP])
    return sort_cross_section_frame(out)[list(_EXPOSURE_COLUMNS)]


__all__ = [
    "DEFAULT_CLASSIFICATION_SYSTEM",
    "DEFAULT_INDUSTRY_LEVEL",
    "LOG_MARKET_CAP",
    "ExposurePanelError",
    "prepare_cross_section_exposure_panel",
]
