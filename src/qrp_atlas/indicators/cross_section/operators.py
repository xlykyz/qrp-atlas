"""Reusable cross-sectional operators.

All operators:

- group exclusively by trade_date;
- never mix samples across dates;
- leave the caller's DataFrame untouched;
- emit NaN for non-finite inputs and for sections with insufficient valid sample;
- produce deterministic ordering by (trade_date, asset_id).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

import numpy as np
import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    ensure_cross_section_frame,
    normalize_feature_columns,
    sort_cross_section_frame,
)

RankMethod = Literal["average", "min", "max", "first", "dense"]


def _finite_mask(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return pd.Series(np.isfinite(values.to_numpy(dtype=float, copy=False)), index=series.index)


def _prepare(
    df: pd.DataFrame,
    feature_columns: str | Sequence[str],
) -> tuple[pd.DataFrame, list[str]]:
    features = normalize_feature_columns(feature_columns)
    if not features:
        raise CrossSectionFrameError("at least one feature column is required")
    frame = ensure_cross_section_frame(df, feature_columns=features, require_features=True)
    return frame, features


def _output_name(feature: str, suffix: str, prefix: str | None = None) -> str:
    base = f"{feature}_{suffix}"
    return f"{prefix}_{base}" if prefix else base


def _rank_1d(
    values: pd.Series,
    *,
    ascending: bool,
    method: RankMethod,
    pct: bool,
) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    finite = _finite_mask(numeric)
    out = pd.Series(np.nan, index=values.index, dtype=float)
    if int(finite.sum()) == 0:
        return out
    ranked = numeric.where(finite).rank(method=method, ascending=ascending, pct=pct)
    out.loc[finite] = ranked.loc[finite].astype(float)
    return out


def _winsorize_1d(
    values: pd.Series,
    *,
    lower: float,
    upper: float,
) -> pd.Series:
    if not (0.0 <= lower < upper <= 1.0):
        raise CrossSectionFrameError(
            f"winsorize bounds must satisfy 0 <= lower < upper <= 1; got lower={lower}, upper={upper}"
        )
    numeric = pd.to_numeric(values, errors="coerce")
    finite = _finite_mask(numeric)
    out = pd.Series(np.nan, index=values.index, dtype=float)
    n = int(finite.sum())
    if n == 0:
        return out
    sample = numeric.loc[finite].astype(float)
    if n == 1:
        out.loc[finite] = sample
        return out
    lo = float(sample.quantile(lower, interpolation="linear"))
    hi = float(sample.quantile(upper, interpolation="linear"))
    clipped = sample.clip(lower=lo, upper=hi)
    out.loc[finite] = clipped
    return out


def _zscore_1d(
    values: pd.Series,
    *,
    ddof: int,
    min_count: int,
) -> pd.Series:
    if ddof < 0:
        raise CrossSectionFrameError("ddof must be >= 0")
    if min_count < 1:
        raise CrossSectionFrameError("min_count must be >= 1")
    numeric = pd.to_numeric(values, errors="coerce")
    finite = _finite_mask(numeric)
    out = pd.Series(np.nan, index=values.index, dtype=float)
    n = int(finite.sum())
    if n < min_count or n <= ddof:
        return out
    sample = numeric.loc[finite].astype(float)
    mean = float(sample.mean())
    std = float(sample.std(ddof=ddof))
    if not np.isfinite(std) or std == 0.0:
        # zero-variance or non-finite scale: leave NaN for all members
        return out
    out.loc[finite] = (sample - mean) / std
    return out


def _apply_per_date(
    frame: pd.DataFrame,
    feature_columns: Sequence[str],
    transform,
    *,
    suffix: str,
    prefix: str | None,
    **kwargs: Any,
) -> pd.DataFrame:
    out = frame.copy()
    for feature in feature_columns:
        target = _output_name(feature, suffix, prefix)
        pieces: list[pd.Series] = []
        for _, group in out.groupby(TRADE_DATE, sort=False):
            pieces.append(transform(group[feature], **kwargs))
        if pieces:
            out[target] = pd.concat(pieces).sort_index()
        else:
            out[target] = np.nan
    return sort_cross_section_frame(out)


def cross_section_rank(
    df: pd.DataFrame,
    feature_columns: str | Sequence[str],
    *,
    ascending: bool = True,
    method: RankMethod = "average",
    prefix: str | None = None,
) -> pd.DataFrame:
    """Cross-sectional rank within each trade_date.

    Semantics:
    - Non-finite values (NaN / +/-inf) are excluded from ranking and remain NaN.
    - ``method`` follows pandas rank methods (default average for ties).
    - ``ascending=True`` ranks low values first; ``False`` ranks high values first.
    - Single-asset sections produce rank 1.0 for finite values.
    """
    frame, features = _prepare(df, feature_columns)
    return _apply_per_date(
        frame,
        features,
        _rank_1d,
        suffix="rank",
        prefix=prefix,
        ascending=ascending,
        method=method,
        pct=False,
    )


def cross_section_percentile_rank(
    df: pd.DataFrame,
    feature_columns: str | Sequence[str],
    *,
    ascending: bool = True,
    method: RankMethod = "average",
    prefix: str | None = None,
) -> pd.DataFrame:
    """Percentile rank within each trade_date (pandas rank with pct=True).

    Values are on (0, 1]. Non-finite inputs remain NaN.
    """
    frame, features = _prepare(df, feature_columns)
    return _apply_per_date(
        frame,
        features,
        _rank_1d,
        suffix="pct_rank",
        prefix=prefix,
        ascending=ascending,
        method=method,
        pct=True,
    )


def cross_section_winsorize(
    df: pd.DataFrame,
    feature_columns: str | Sequence[str],
    *,
    limits: tuple[float, float] = (0.01, 0.99),
    prefix: str | None = None,
) -> pd.DataFrame:
    """Winsorize each feature within each trade_date.

    Semantics:
    - ``limits=(lower, upper)`` are quantiles with 0 <= lower < upper <= 1.
    - Non-finite values are ignored when computing quantiles and remain NaN.
    - Single-asset sections return the original finite value unchanged.
    - Output keeps original columns and adds ``{feature}_winsorized``.
    """
    if not isinstance(limits, tuple | list) or len(limits) != 2:
        raise CrossSectionFrameError("limits must be a (lower, upper) pair")
    lower, upper = float(limits[0]), float(limits[1])
    frame, features = _prepare(df, feature_columns)
    return _apply_per_date(
        frame,
        features,
        _winsorize_1d,
        suffix="winsorized",
        prefix=prefix,
        lower=lower,
        upper=upper,
    )


def cross_section_zscore(
    df: pd.DataFrame,
    feature_columns: str | Sequence[str],
    *,
    ddof: int = 0,
    min_count: int = 2,
    prefix: str | None = None,
) -> pd.DataFrame:
    """Cross-sectional z-score within each trade_date.

    Semantics:
    - Uses population std by default (``ddof=0``) so a two-point section is usable.
    - Non-finite values are excluded from mean/std and remain NaN.
    - Sections with fewer than ``min_count`` finite samples return all NaN.
    - Zero-variance sections return all NaN (no division by zero).
    """
    frame, features = _prepare(df, feature_columns)
    return _apply_per_date(
        frame,
        features,
        _zscore_1d,
        suffix="zscore",
        prefix=prefix,
        ddof=ddof,
        min_count=min_count,
    )


def apply_cross_section_operators(
    df: pd.DataFrame,
    feature_columns: str | Sequence[str],
    *,
    operators: Sequence[str] = ("rank", "pct_rank", "winsorize", "zscore"),
    ascending: bool = True,
    method: RankMethod = "average",
    winsor_limits: tuple[float, float] = (0.01, 0.99),
    zscore_ddof: int = 0,
    zscore_min_count: int = 2,
    prefix: str | None = None,
) -> pd.DataFrame:
    """Apply a sequence of cross-sectional operators to one frame.

    Supported operator names: ``rank``, ``pct_rank`` / ``percentile_rank``,
    ``winsorize``, ``zscore``.
    """
    frame, features = _prepare(df, feature_columns)
    result = frame
    for raw_name in operators:
        name = str(raw_name).strip().lower()
        if name == "rank":
            result = cross_section_rank(
                result,
                features,
                ascending=ascending,
                method=method,
                prefix=prefix,
            )
        elif name in {"pct_rank", "percentile_rank", "percentile"}:
            result = cross_section_percentile_rank(
                result,
                features,
                ascending=ascending,
                method=method,
                prefix=prefix,
            )
        elif name == "winsorize":
            result = cross_section_winsorize(
                result,
                features,
                limits=winsor_limits,
                prefix=prefix,
            )
        elif name in {"zscore", "z_score"}:
            result = cross_section_zscore(
                result,
                features,
                ddof=zscore_ddof,
                min_count=zscore_min_count,
                prefix=prefix,
            )
        else:
            raise CrossSectionFrameError(f"unknown cross-section operator: {raw_name!r}")
    return sort_cross_section_frame(result)
