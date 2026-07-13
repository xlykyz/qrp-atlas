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
    FLOAT_CAP,
    INDUSTRY_CODE,
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
_MISSING_INDUSTRY_LABELS = frozenset({"", "nan", "none", "<na>", "nat", "null"})


class ExposurePanelError(ValueError):
    """Raised when an exposure panel cannot be prepared or validated."""


def _as_finite_series(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")

    def _finite(x: Any) -> bool:
        return bool(pd.notna(x) and math.isfinite(float(x)))

    return values.where(values.map(_finite))


def _is_missing_industry(value: object) -> bool:
    if value is None:
        return True
    try:
        if value is pd.NA or value is pd.NaT:
            return True
    except Exception:
        pass
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip().lower() in _MISSING_INDUSTRY_LABELS
    text = str(value).strip().lower()
    return text in _MISSING_INDUSTRY_LABELS


def _normalize_industry_code(value: object) -> object:
    """Map missing tokens to None; never stringify pd.NA as ``<NA>``."""
    if _is_missing_industry(value):
        return None
    label = str(value).strip()
    if not label or label.lower() in _MISSING_INDUSTRY_LABELS:
        return None
    return label


def _resolve_id_column(df: pd.DataFrame, *, label: str) -> str:
    if ASSET_ID in df.columns:
        return ASSET_ID
    if TICKER in df.columns:
        return TICKER
    raise ExposurePanelError(
        f"{label} requires an asset identifier column ({ASSET_ID} or {TICKER})"
    )


def _reject_duplicate_keys(
    df: pd.DataFrame,
    *,
    label: str,
    subset: Sequence[str] | None = None,
) -> None:
    if df is None or df.empty:
        return
    keys = list(subset) if subset is not None else [TRADE_DATE, ASSET_ID]
    missing = [c for c in keys if c not in df.columns]
    if missing:
        return
    duplicated = df.duplicated(subset=list(keys), keep=False)
    if bool(duplicated.any()):
        sample = (
            df.loc[duplicated, list(keys)]
            .drop_duplicates()
            .head(5)
            .to_dict(orient="records")
        )
        raise ExposurePanelError(
            f"{label} contains duplicate keys {list(keys)}: {sample}"
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
        # Empty panel is valid: all sizes become missing later.
        return pd.DataFrame(columns=[TRADE_DATE, ASSET_ID, size_field])
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
        # Empty industry panel is valid: every asset gets missing industry_code.
        return pd.DataFrame(columns=[TRADE_DATE, ASSET_ID, INDUSTRY_CODE])
    if TRADE_DATE not in industry_panel.columns:
        raise ExposurePanelError("industry_panel missing required column: 'trade_date'")
    if INDUSTRY_CODE not in industry_panel.columns:
        raise ExposurePanelError(
            "industry_panel missing required column: 'industry_code'"
        )
    id_col = _resolve_id_column(industry_panel, label="industry_panel")
    out = industry_panel.copy()
    out[TRADE_DATE] = [normalize_trade_date(v) for v in out[TRADE_DATE].tolist()]
    out[ASSET_ID] = [normalize_asset_id(v) for v in out[id_col].tolist()]
    out[INDUSTRY_CODE] = [
        _normalize_industry_code(v) for v in out[INDUSTRY_CODE].tolist()
    ]
    _reject_duplicate_keys(out, label="industry_panel")
    return out[[TRADE_DATE, ASSET_ID, INDUSTRY_CODE]]


def _empty_exposure_frame() -> pd.DataFrame:
    out = empty_cross_section_frame(extra_columns=[INDUSTRY_CODE, LOG_MARKET_CAP])
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
    out[INDUSTRY_CODE] = [
        _normalize_industry_code(v) for v in out[INDUSTRY_CODE].tolist()
    ]
    # Explicit failure: same asset must not map to multiple industry rows.
    _reject_duplicate_keys(
        out,
        label="industry query result",
        subset=[ASSET_ID],
    )
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

    Empty ``industry_panel`` (including ``pd.DataFrame()``) is valid and yields
    missing ``industry_code`` for the full universe rather than raising.
    Injected industry queries that return duplicate asset rows for one as-of
    date raise :class:`ExposurePanelError` instead of silently ``keep="last"``.
    """
    uni = ensure_cross_section_frame(universe, enforce_primary_key=True)
    if uni.empty:
        return _empty_exposure_frame()

    if not isinstance(classification_system, str) or not classification_system.strip():
        raise ExposurePanelError("classification_system must be a non-empty string")
    if (
        not isinstance(industry_level, int)
        or isinstance(industry_level, bool)
        or industry_level < 1
    ):
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

        if prepared_industry is not None:
            if prepared_industry.empty:
                day_ind = pd.DataFrame(columns=[ASSET_ID, INDUSTRY_CODE])
            else:
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
        piece[INDUSTRY_CODE] = [
            _normalize_industry_code(v) for v in piece[INDUSTRY_CODE].tolist()
        ]

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
    out[INDUSTRY_CODE] = [
        _normalize_industry_code(v) for v in out[INDUSTRY_CODE].tolist()
    ]
    out[LOG_MARKET_CAP] = _as_finite_series(out[LOG_MARKET_CAP])
    return sort_cross_section_frame(out)[list(_EXPOSURE_COLUMNS)]


__all__ = [
    "DEFAULT_CLASSIFICATION_SYSTEM",
    "DEFAULT_INDUSTRY_LEVEL",
    "LOG_MARKET_CAP",
    "ExposurePanelError",
    "prepare_cross_section_exposure_panel",
]
