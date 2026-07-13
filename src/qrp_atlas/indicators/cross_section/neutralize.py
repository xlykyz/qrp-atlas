"""Cross-sectional industry and size neutralization (task 04-C).

Consumes already-prepared factor frames and exposure panels. Does not query
DuckDB, import backtest, or reimplement rank / winsorize / z-score.

Model (per trade_date, independently):

```text
factor = intercept + industry fixed effects + numeric exposures + residual
neutralized_factor = residual
```

Recommended processing order:

```text
raw factor
  -> optional winsorize
  -> neutralize (this module)
  -> z-score / rank
```
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import ASSET_ID, INDUSTRY_CODE, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    empty_cross_section_frame,
    ensure_cross_section_frame,
    normalize_feature_columns,
    normalize_trade_date,
    sort_cross_section_frame,
)

LOG_MARKET_CAP = "log_market_cap"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_COLUMNS = frozenset({TRADE_DATE, ASSET_ID})


class NeutralizationError(CrossSectionFrameError):
    """Raised when neutralization cannot be performed or outputs conflict."""


def _validate_identifier(value: str, label: str) -> None:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise NeutralizationError(f"{label} must be a stable identifier: {value!r}")


def _as_finite_array(series: pd.Series) -> np.ndarray:
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float, copy=True)
    values[~np.isfinite(values)] = np.nan
    return values


def _default_output_name(factor: str) -> str:
    return f"{factor}_neutral"


def _resolve_output_map(
    factor_columns: Sequence[str],
    output_columns: Mapping[str, str] | None,
    *,
    existing_columns: Sequence[str],
) -> dict[str, str]:
    existing = set(existing_columns)
    mapping: dict[str, str] = {}
    seen: set[str] = set()
    outputs = dict(output_columns or {})
    unknown = sorted(set(outputs) - set(factor_columns))
    if unknown:
        raise NeutralizationError(f"output_columns has unknown factors: {unknown}")
    for factor in factor_columns:
        name = outputs.get(factor, _default_output_name(factor))
        _validate_identifier(name, "neutralized output column")
        if name in _RESERVED_COLUMNS:
            raise NeutralizationError(
                f"neutralized output column {name!r} is reserved; "
                f"cannot use {sorted(_RESERVED_COLUMNS)}"
            )
        if name in existing or name in factor_columns:
            raise NeutralizationError(
                f"neutralized output column {name!r} collides with an existing column"
            )
        if name in seen:
            raise NeutralizationError(f"duplicate neutralized output column: {name}")
        seen.add(name)
        mapping[factor] = name
    return mapping


def _normalize_exposure_panel(
    exposure_panel: pd.DataFrame,
    *,
    categorical_exposures: Sequence[str],
    numeric_exposures: Sequence[str],
) -> pd.DataFrame:
    if exposure_panel is None:
        raise NeutralizationError("exposure_panel is required")
    if not isinstance(exposure_panel, pd.DataFrame):
        raise NeutralizationError("exposure_panel must be a pandas DataFrame")
    needed = list(categorical_exposures) + list(numeric_exposures)
    frame = ensure_cross_section_frame(
        exposure_panel,
        feature_columns=needed if needed else None,
        enforce_primary_key=True,
    )
    return frame


def _build_design_matrix(
    frame: pd.DataFrame,
    *,
    categorical_exposures: Sequence[str],
    numeric_exposures: Sequence[str],
) -> tuple[np.ndarray | None, np.ndarray]:
    """Build intercept + categorical dummies + numeric exposures.

    Categorical encoding:
    - categories sorted lexicographically for stability;
    - drop the first category (baseline) to avoid the dummy trap with intercept;
    - if only one category is present, no categorical columns are added.

    Returns:
        (design matrix or None when unusable, row mask of usable exposure rows)
    """
    n = len(frame)
    usable = np.ones(n, dtype=bool)

    blocks: list[np.ndarray] = [np.ones((n, 1), dtype=float)]

    for col in categorical_exposures:
        series = frame[col]
        # missing category marks unusable
        present = series.notna() & series.astype(str).map(lambda x: str(x).strip() != "")
        usable &= present.to_numpy()
        # categories determined on present values only, stable sorted order
        cats = sorted({str(v).strip() for v in series.loc[present].tolist() if str(v).strip()})
        if len(cats) <= 1:
            continue
        baseline = cats[0]
        kept = cats[1:]
        cat_values = series.map(lambda v: str(v).strip() if pd.notna(v) else "")
        for cat in kept:
            blocks.append((cat_values == cat).to_numpy(dtype=float).reshape(n, 1))

    for col in numeric_exposures:
        values = _as_finite_array(frame[col])
        usable &= np.isfinite(values)
        blocks.append(values.reshape(n, 1))

    if not np.any(usable):
        return None, usable

    x = np.concatenate(blocks, axis=1)
    return x, usable


def _neutralize_one_section(
    y: np.ndarray,
    x: np.ndarray | None,
    usable_exposure: np.ndarray,
) -> np.ndarray:
    """OLS residualization for one trade_date section.

    Rows with non-finite y or unusable exposures get NaN residuals.
    When usable observations are fewer than design columns, or the design is
    rank-deficient / singular, return NaN for residual slots rather than
    inventing numbers via pseudo-inverse. Exactly identified full-rank systems
    (including constant factors residualizing to zero) are allowed.
    """
    residual = np.full(y.shape[0], np.nan, dtype=float)
    finite_y = np.isfinite(y)
    row_ok = finite_y & usable_exposure
    if x is None or int(row_ok.sum()) == 0:
        return residual

    x_ok = x[row_ok]
    y_ok = y[row_ok]
    n_obs, n_params = x_ok.shape
    # Standard OLS needs at least n_params observations and full column rank.
    # Exactly determined systems are allowed (e.g. constant factor residualizes to 0).
    # Rank deficiency / singularity is rejected separately below (no silent pinv).
    if n_obs < max(n_params, 2):
        return residual

    # Constant y with intercept-only design: residual is zero; still valid OLS.
    # Rank-deficient / singular design -> NaN (no pseudo-inverse silent fix).
    try:
        rank = np.linalg.matrix_rank(x_ok, tol=1e-10)
    except Exception:
        return residual
    if rank < n_params:
        return residual

    try:
        beta, _, rank_lstsq, _ = np.linalg.lstsq(x_ok, y_ok, rcond=None)
    except np.linalg.LinAlgError:
        return residual
    if int(rank_lstsq) < n_params:
        return residual

    fitted = x_ok @ beta
    resid = y_ok - fitted
    # Reject non-finite residual vectors.
    if not np.all(np.isfinite(resid)):
        return residual
    residual[row_ok] = resid
    return residual


def neutralize_cross_section(
    factors: pd.DataFrame,
    exposure_panel: pd.DataFrame,
    *,
    factor_columns: str | Sequence[str],
    categorical_exposures: Sequence[str] = (INDUSTRY_CODE,),
    numeric_exposures: Sequence[str] = (LOG_MARKET_CAP,),
    output_columns: Mapping[str, str] | None = None,
    suffix: str = "neutral",
) -> pd.DataFrame:
    """Neutralize one or more factors on each trade_date independently.

    Args:
        factors: raw factor frame with trade_date / asset_id / factor columns.
        exposure_panel: prepared panel with trade_date / asset_id and exposures.
        factor_columns: factor columns to residualize.
        categorical_exposures: category columns (default industry_code).
        numeric_exposures: numeric columns (default log_market_cap). Empty tuple
            allowed for industry-only neutralization.
        output_columns: optional mapping factor -> output name.
        suffix: default suffix when output_columns omits a factor (unused when
            output_columns provided for that factor); kept for API clarity.

    Returns:
        Copy of the factor frame plus neutralized residual columns. Original
        factor columns are preserved. Assets with missing factor/exposure stay
        in the frame with NaN residuals.
    """
    del suffix  # naming goes through _default_output_name / output_columns
    factor_cols = normalize_feature_columns(factor_columns)
    if not factor_cols:
        raise NeutralizationError("at least one factor column is required")

    cat_cols = list(dict.fromkeys(str(c) for c in categorical_exposures))
    num_cols = list(dict.fromkeys(str(c) for c in numeric_exposures))
    if not cat_cols and not num_cols:
        raise NeutralizationError(
            "at least one of categorical_exposures or numeric_exposures is required"
        )

    factor_frame = ensure_cross_section_frame(
        factors,
        feature_columns=factor_cols,
        require_features=True,
        enforce_primary_key=True,
    )
    exposure = _normalize_exposure_panel(
        exposure_panel,
        categorical_exposures=cat_cols,
        numeric_exposures=num_cols,
    )

    out_map = _resolve_output_map(
        factor_cols,
        output_columns,
        existing_columns=list(factor_frame.columns),
    )

    if factor_frame.empty:
        empty = empty_cross_section_frame(
            list(factor_cols) + list(out_map.values())
        )
        return empty

    # Left-join exposures onto factor universe; preserve all factor rows.
    working = factor_frame.merge(
        exposure[[TRADE_DATE, ASSET_ID] + cat_cols + num_cols],
        on=[TRADE_DATE, ASSET_ID],
        how="left",
        validate="one_to_one",
    )

    for out_col in out_map.values():
        working[out_col] = math.nan

    pieces: list[pd.DataFrame] = []
    for _, section in working.groupby(TRADE_DATE, sort=False):
        section = section.copy()
        x, usable = _build_design_matrix(
            section,
            categorical_exposures=cat_cols,
            numeric_exposures=num_cols,
        )
        for factor in factor_cols:
            y = _as_finite_array(section[factor])
            resid = _neutralize_one_section(y, x, usable)
            section[out_map[factor]] = resid
        pieces.append(section)

    out = pd.concat(pieces, ignore_index=True) if pieces else working
    # Drop exposure helper columns that are not original factor columns.
    drop_cols = [
        c
        for c in cat_cols + num_cols
        if c in out.columns and c not in factor_frame.columns
    ]
    if drop_cols:
        out = out.drop(columns=drop_cols)

    # Preserve original columns + neutralized columns in stable order.
    ordered = list(factor_frame.columns) + [out_map[f] for f in factor_cols]
    out = out[ordered]
    out = ensure_cross_section_frame(
        out,
        feature_columns=list(factor_cols) + list(out_map.values()),
        enforce_primary_key=True,
    )
    return sort_cross_section_frame(out)


def neutralize_factor_frame(
    factors: pd.DataFrame,
    *,
    exposure_panel: pd.DataFrame,
    factor_columns: str | Sequence[str],
    categorical_exposures: Sequence[str] = (INDUSTRY_CODE,),
    numeric_exposures: Sequence[str] = (LOG_MARKET_CAP,),
    output_columns: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Stable public entry for neutralizing a raw factor frame.

    Thin wrapper around :func:`neutralize_cross_section` with keyword-only
    exposure arguments matching the 04-C composition pattern.
    """
    return neutralize_cross_section(
        factors,
        exposure_panel,
        factor_columns=factor_columns,
        categorical_exposures=categorical_exposures,
        numeric_exposures=numeric_exposures,
        output_columns=output_columns,
    )


__all__ = [
    "LOG_MARKET_CAP",
    "NeutralizationError",
    "neutralize_cross_section",
    "neutralize_factor_frame",
]
