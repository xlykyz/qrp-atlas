"""Prepare financial panels for formal cross-sectional factors.

This module sits on the backtest data-preparation boundary:

```text
contracts / DuckDB / PIT query services
  -> prepare_financial_factor_panel(...)
  -> indicators.generate_factor_frame(financial_panel=...)
```

Indicators must not import this module, query DuckDB, or resolve multi-version
financial records themselves.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import math

import pandas as pd

from qrp_atlas.backtest.point_in_time import select_latest_available_records
from qrp_atlas.contracts import (
    ASSET_ID,
    AVAILABLE_TRADE_DATE,
    BPS,
    FINANCIAL_INDICATOR,
    REPORT_PERIOD,
    ROE,
    TICKER,
    TRADE_DATE,
)
from qrp_atlas.indicators.cross_section.conventions import (
    ensure_cross_section_frame,
    normalize_asset_id,
    normalize_trade_date,
    sort_cross_section_frame,
)

FinancialQuery = Callable[..., pd.DataFrame]

FINANCIAL_FACTOR_VALUE_COLUMNS: tuple[str, str] = (ROE, BPS)


def _resolve_id_column(df: pd.DataFrame) -> str:
    if ASSET_ID in df.columns:
        return ASSET_ID
    if TICKER in df.columns:
        return TICKER
    raise ValueError(
        f"financial records require an identifier column ({ASSET_ID} or {TICKER})"
    )


def _normalize_versioned_financials(financials: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(financials, pd.DataFrame):
        raise ValueError("financials must be a pandas DataFrame")
    if financials.empty:
        out = financials.copy()
        if ASSET_ID not in out.columns:
            out[ASSET_ID] = pd.Series(dtype=object)
        return out

    id_col = _resolve_id_column(financials)
    required = [AVAILABLE_TRADE_DATE, REPORT_PERIOD]
    missing = [c for c in required if c not in financials.columns]
    if missing:
        raise ValueError(f"financials missing required columns: {missing}")

    out = financials.copy()
    out[ASSET_ID] = [normalize_asset_id(v) for v in out[id_col].tolist()]
    if TICKER not in out.columns:
        out[TICKER] = out[ASSET_ID]
    out[AVAILABLE_TRADE_DATE] = pd.to_datetime(out[AVAILABLE_TRADE_DATE], errors="coerce")
    out[REPORT_PERIOD] = pd.to_datetime(out[REPORT_PERIOD], errors="coerce")
    return out


def _as_finite_series(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")

    def _finite(x: Any) -> bool:
        return bool(pd.notna(x) and math.isfinite(float(x)))

    return values.where(values.map(_finite))


def _latest_rows_as_of(
    financials: pd.DataFrame,
    *,
    as_of_date: Any,
    asset_ids: Sequence[str],
) -> pd.DataFrame:
    """One PIT-valid financial row per asset as of a trade date.

    1. eligible only when available_trade_date <= as_of_date;
    2. within (asset_id, report_period) keep latest version via
       select_latest_available_records;
    3. among remaining reports for an asset, keep the latest report_period.
    """
    as_of = normalize_trade_date(as_of_date)
    assets = [normalize_asset_id(a) for a in asset_ids]
    empty_cols = [ASSET_ID, REPORT_PERIOD, AVAILABLE_TRADE_DATE, ROE, BPS]
    if not assets:
        return pd.DataFrame(columns=empty_cols)

    panel = _normalize_versioned_financials(financials)
    if panel.empty:
        return pd.DataFrame(columns=empty_cols)

    for col in (ROE, BPS):
        if col not in panel.columns:
            panel[col] = math.nan

    panel = panel.loc[panel[ASSET_ID].isin(set(assets))].copy()
    if panel.empty:
        return pd.DataFrame(columns=empty_cols)

    selected = select_latest_available_records(
        panel,
        as_of_date=as_of,
        entity_keys=[ASSET_ID, REPORT_PERIOD],
        available_date_col=AVAILABLE_TRADE_DATE,
        published_at_col="published_at" if "published_at" in panel.columns else None,
        ingested_at_col="ingested_at" if "ingested_at" in panel.columns else None,
        revision_col="revision_id" if "revision_id" in panel.columns else None,
    )
    if selected.empty:
        return pd.DataFrame(columns=empty_cols)

    selected = selected.sort_values(
        [ASSET_ID, REPORT_PERIOD, AVAILABLE_TRADE_DATE],
        kind="mergesort",
    )
    latest = selected.drop_duplicates(subset=[ASSET_ID], keep="last")
    cols = [ASSET_ID, REPORT_PERIOD, AVAILABLE_TRADE_DATE]
    for col in (ROE, BPS):
        if col in latest.columns:
            cols.append(col)
    return latest[cols].reset_index(drop=True)


def _load_versioned_financials(
    *,
    financial_query: FinancialQuery | None,
    as_of_date: Any,
    asset_ids: Sequence[str],
    db_path: Any,
    con: Any,
) -> pd.DataFrame:
    query = financial_query
    if query is None:
        from qrp_atlas.backtest.pit_queries import query_financial_as_of

        query = query_financial_as_of
    if not asset_ids:
        return pd.DataFrame()
    frame = query(
        as_of_date=as_of_date,
        table=FINANCIAL_INDICATOR.name,
        tickers=list(asset_ids),
        db_path=db_path,
        con=con,
    )
    if frame is None or frame.empty:
        return pd.DataFrame()
    return frame


def prepare_financial_factor_panel(
    universe: pd.DataFrame,
    *,
    financials: pd.DataFrame | None = None,
    financial_query: FinancialQuery | None = None,
    db_path: Any = None,
    con: Any = None,
) -> pd.DataFrame:
    """Build a shared PIT financial panel for ROE and book-to-price.

    Returns a frame with columns:

    - trade_date
    - asset_id
    - roe
    - bps

    One query / selection pass is performed per target trade_date so that both
    financial factors reuse the same as-of snapshot. Later revisions that only
    become available after a target date never affect earlier dates.

    Args:
        universe: historical stock pool with trade_date / asset_id.
        financials: multi-version financial_indicator-like records (for tests
            or offline preparation). When provided, no database is queried.
        financial_query / db_path / con: optional DuckDB PIT sources used only
            when ``financials`` is omitted.
    """
    uni = ensure_cross_section_frame(universe, enforce_primary_key=True)
    columns = [TRADE_DATE, ASSET_ID, ROE, BPS]
    if uni.empty:
        out = pd.DataFrame(columns=columns)
        out[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
        out[ASSET_ID] = pd.Series(dtype=object)
        out[ROE] = pd.Series(dtype="float64")
        out[BPS] = pd.Series(dtype="float64")
        return out

    pieces: list[pd.DataFrame] = []
    for trade_date, day_uni in uni.groupby(TRADE_DATE, sort=False):
        assets = day_uni[ASSET_ID].tolist()
        if financials is not None:
            day_fin = financials
        else:
            day_fin = _load_versioned_financials(
                financial_query=financial_query,
                as_of_date=trade_date,
                asset_ids=assets,
                db_path=db_path,
                con=con,
            )
        latest = _latest_rows_as_of(
            day_fin if day_fin is not None else pd.DataFrame(),
            as_of_date=trade_date,
            asset_ids=assets,
        )
        piece = day_uni[[TRADE_DATE, ASSET_ID]].copy()
        if latest.empty:
            piece[ROE] = math.nan
            piece[BPS] = math.nan
        else:
            keep = [ASSET_ID]
            if ROE in latest.columns:
                keep.append(ROE)
            if BPS in latest.columns:
                keep.append(BPS)
            piece = piece.merge(latest[keep], on=ASSET_ID, how="left")
            if ROE not in piece.columns:
                piece[ROE] = math.nan
            if BPS not in piece.columns:
                piece[BPS] = math.nan
            piece[ROE] = _as_finite_series(piece[ROE])
            piece[BPS] = _as_finite_series(piece[BPS])
        pieces.append(piece[columns])

    out = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame(columns=columns)
    out = ensure_cross_section_frame(
        out, feature_columns=[ROE, BPS], enforce_primary_key=True
    )
    return sort_cross_section_frame(out)


__all__ = [
    "FINANCIAL_FACTOR_VALUE_COLUMNS",
    "prepare_financial_factor_panel",
]
