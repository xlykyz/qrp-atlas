"""Cross-sectional industry and size neutralization (task 04-C).

Consumes already-prepared factor frames and exposure panels. Does not query
DuckDB, import backtest, or reimplement rank / winsorize / z-score.

Model (per trade_date, independently, and per factor on its own sample):

```text
finite factor + valid required exposures
  -> actual regression sample
  -> design matrix from that sample only
  -> OLS residual
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

import numpy as np
import pandas as pd

from qrp_atlas.contracts import ASSET_ID, INDUSTRY_CODE, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    empty_cross_section_frame,
    ensure_cross_section_frame,
    normalize_feature_columns,
    sort_cross_section_frame,
)

LOG_MARKET_CAP = "log_market_cap"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_COLUMNS = frozenset({TRADE_DATE, ASSET_ID})
_MISSING_CATEGORY_LABELS = frozenset({"", "nan", "none", "<na>", "nat", "null"})


class NeutralizationError(CrossSectionFrameError):
    """Raised when neutralization cannot be performed or outputs conflict."""


def _validate_identifier(value: str, label: str) -> None:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise NeutralizationError(f"{label} must be a stable identifier: {value!r}")


def _as_finite_array(series: pd.Series) -> np.ndarray:
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float, copy=True)
    values[~np.isfinite(values)] = np.nan
    return values


def _is_missing_category(value: object) -> bool:
    """Treat None / NaN / NA / NaT / blank / stringified NA as missing."""
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
        return value.strip().lower() in _MISSING_CATEGORY_LABELS
    # Avoid str(pd.NA) -> "<NA>" becoming a real category: already handled above.
    text = str(value).strip().lower()
    return text in _MISSING_CATEGORY_LABELS


def _normalize_category_label(value: object) -> str | None:
    if _is_missing_category(value):
        return None
    label = str(value).strip()
    if not label or label.lower() in _MISSING_CATEGORY_LABELS:
        return None
    return label


def _default_output_name(factor: str, *, suffix: str) -> str:
    return f"{factor}_{suffix}"


def _resolve_output_map(
    factor_columns: Sequence[str],
    output_columns: Mapping[str, str] | None,
    *,
    forbidden_columns: Sequence[str],
    suffix: str,
) -> dict[str, str]:
    _validate_identifier(suffix, "suffix")
    forbidden = set(forbidden_columns)
    mapping: dict[str, str] = {}
    seen: set[str] = set()
    outputs = dict(output_columns or {})
    unknown = sorted(set(outputs) - set(factor_columns))
    if unknown:
        raise NeutralizationError(f"output_columns has unknown factors: {unknown}")
    for factor in factor_columns:
        name = outputs.get(factor, _default_output_name(factor, suffix=suffix))
        _validate_identifier(name, "neutralized output column")
        if name in _RESERVED_COLUMNS:
            raise NeutralizationError(
                f"neutralized output column {name!r} is reserved; "
                f"cannot use {sorted(_RESERVED_COLUMNS)}"
            )
        if name in forbidden:
            raise NeutralizationError(
                f"neutralized output column {name!r} collides with an existing "
                f"or exposure column"
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
    # Normalize categorical missing tokens so pd.NA never becomes "<NA>".
    for col in categorical_exposures:
        if col in frame.columns:
            frame = frame.copy()
            frame[col] = [
                _normalize_category_label(v) for v in frame[col].tolist()
            ]
    return frame


def _exposure_usable_mask(
    frame: pd.DataFrame,
    *,
    categorical_exposures: Sequence[str],
    numeric_exposures: Sequence[str],
) -> np.ndarray:
    n = len(frame)
    usable = np.ones(n, dtype=bool)
    for col in categorical_exposures:
        labels = [_normalize_category_label(v) for v in frame[col].tolist()]
        usable &= np.array([lab is not None for lab in labels], dtype=bool)
    for col in numeric_exposures:
        values = _as_finite_array(frame[col])
        usable &= np.isfinite(values)
    return usable


def _build_design_matrix_for_sample(
    frame: pd.DataFrame,
    row_mask: np.ndarray,
    *,
    categorical_exposures: Sequence[str],
    numeric_exposures: Sequence[str],
) -> np.ndarray | None:
    """Build intercept + FE + numeric exposures on the actual regression sample.

    Categories and numeric columns are taken only from rows selected by
    ``row_mask`` (finite factor + valid required exposures). One factor's
    missing pattern therefore cannot invent zero dummy columns for another.
    """
    idx = np.flatnonzero(row_mask)
    n = int(idx.size)
    if n == 0:
        return None

    blocks: list[np.ndarray] = [np.ones((n, 1), dtype=float)]

    for col in categorical_exposures:
        labels = [
            _normalize_category_label(v)
            for v in frame[col].iloc[idx].tolist()
        ]
        cats = sorted({lab for lab in labels if lab is not None})
        if len(cats) <= 1:
            # Single category in this factor's sample: intercept-only demean.
            continue
        kept = cats[1:]  # drop first sorted baseline
        for cat in kept:
            dummy = np.array(
                [1.0 if lab == cat else 0.0 for lab in labels],
                dtype=float,
            ).reshape(n, 1)
            blocks.append(dummy)

    for col in numeric_exposures:
        values = _as_finite_array(frame[col])[idx]
        blocks.append(values.reshape(n, 1))

    return np.concatenate(blocks, axis=1)


def _neutralize_one_factor(
    y: np.ndarray,
    frame: pd.DataFrame,
    *,
    categorical_exposures: Sequence[str],
    numeric_exposures: Sequence[str],
    exposure_usable: np.ndarray,
) -> np.ndarray:
    """OLS residualization for one factor on one trade_date.

    Sample selection is factor-specific:

    ```text
    finite(y) & exposure_usable
      -> design matrix from that sample only
      -> full-rank check
      -> residual
    ```
    """
    residual = np.full(y.shape[0], np.nan, dtype=float)
    row_ok = np.isfinite(y) & exposure_usable
    n_obs = int(row_ok.sum())
    if n_obs == 0:
        return residual

    x_ok = _build_design_matrix_for_sample(
        frame,
        row_ok,
        categorical_exposures=categorical_exposures,
        numeric_exposures=numeric_exposures,
    )
    if x_ok is None:
        return residual

    y_ok = y[row_ok]
    n_obs, n_params = x_ok.shape
    # Standard OLS: need at least n_params observations; rank checked next.
    if n_obs < max(n_params, 2):
        return residual

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

    resid = y_ok - (x_ok @ beta)
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

    Each factor builds its own design matrix from its own usable sample so that
    one factor's missing pattern cannot force rank deficiency on another.

    Args:
        factors: raw factor frame with trade_date / asset_id / factor columns.
        exposure_panel: prepared panel with trade_date / asset_id and exposures.
        factor_columns: factor columns to residualize.
        categorical_exposures: category columns (default industry_code).
        numeric_exposures: numeric columns (default log_market_cap). Empty tuple
            allowed for industry-only neutralization.
        output_columns: optional mapping factor -> output name.
        suffix: default output suffix when output_columns omits a factor
            (default ``neutral`` → ``{factor}_neutral``).

    Returns:
        Copy of the factor frame plus neutralized residual columns. Original
        factor columns are preserved. Assets with missing factor/exposure stay
        in the frame with NaN residuals.
    """
    factor_cols = normalize_feature_columns(factor_columns)
    if not factor_cols:
        raise NeutralizationError("at least one factor column is required")

    cat_cols = list(dict.fromkeys(str(c) for c in categorical_exposures))
    num_cols = list(dict.fromkeys(str(c) for c in numeric_exposures))
    if not cat_cols and not num_cols:
        raise NeutralizationError(
            "at least one of categorical_exposures or numeric_exposures is required"
        )
    overlap = sorted(set(cat_cols) & set(num_cols))
    if overlap:
        raise NeutralizationError(
            "exposure columns cannot be both categorical and numeric: "
            f"{overlap}"
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

    forbidden = (
        list(factor_frame.columns)
        + list(exposure.columns)
        + cat_cols
        + num_cols
    )
    out_map = _resolve_output_map(
        factor_cols,
        output_columns,
        forbidden_columns=forbidden,
        suffix=suffix,
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
    # Re-normalize categoricals after merge (left-missing become NaN).
    for col in cat_cols:
        working[col] = [
            _normalize_category_label(v) for v in working[col].tolist()
        ]

    for out_col in out_map.values():
        working[out_col] = math.nan

    pieces: list[pd.DataFrame] = []
    for _, section in working.groupby(TRADE_DATE, sort=False):
        section = section.copy()
        exposure_usable = _exposure_usable_mask(
            section,
            categorical_exposures=cat_cols,
            numeric_exposures=num_cols,
        )
        for factor in factor_cols:
            y = _as_finite_array(section[factor])
            resid = _neutralize_one_factor(
                y,
                section,
                categorical_exposures=cat_cols,
                numeric_exposures=num_cols,
                exposure_usable=exposure_usable,
            )
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
    suffix: str = "neutral",
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
        suffix=suffix,
    )


__all__ = [
    "LOG_MARKET_CAP",
    "NeutralizationError",
    "neutralize_cross_section",
    "neutralize_factor_frame",
]
