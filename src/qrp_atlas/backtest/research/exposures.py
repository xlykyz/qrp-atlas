"""Target portfolio exposure analysis for cross-sectional research."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    ensure_cross_section_frame,
    normalize_feature_columns,
    normalize_trade_date,
)

NUMERIC_EXPOSURE_COLUMNS = (
    "signal_date",
    "trade_date",
    "exposure",
    "weighted_mean",
    "covered_weight",
    "missing_weight",
)
CATEGORICAL_EXPOSURE_COLUMNS = (
    "signal_date",
    "trade_date",
    "exposure",
    "category",
    "target_weight",
)


class ExposureAnalysisError(ValueError):
    """Raised when target exposure analysis cannot proceed."""


@dataclass(frozen=True)
class TargetExposureResult:
    """Numeric and categorical target exposures aligned to signal dates."""

    numeric: pd.DataFrame
    categorical: pd.DataFrame


def analyze_target_exposures(
    target_weights: pd.DataFrame,
    *,
    schedule: pd.DataFrame | Mapping[Any, Any],
    factor_frame: pd.DataFrame | None = None,
    exposure_panel: pd.DataFrame | None = None,
    numeric_exposures: Sequence[str] | None = None,
    categorical_exposures: Sequence[str] | None = None,
) -> TargetExposureResult:
    """Analyze positive target weights using signal-date exposures.

    Exposures are always read on the rebalance ``signal_date`` that produced
    the execution ``trade_date`` snapshot. Future revisions must not enter.
    Only positive target weights contribute.

    Mapping schedules must be ``signal_date -> trade_date``.
    """
    if target_weights is None or not isinstance(target_weights, pd.DataFrame):
        raise ExposureAnalysisError("target_weights must be a pandas DataFrame")
    required = {"trade_date", "asset_id", "target_weight"}
    missing = sorted(required - set(target_weights.columns))
    if missing:
        raise ExposureAnalysisError(
            f"target_weights missing required columns: {missing}"
        )

    working = _validate_target_weights(target_weights)
    trade_to_signal = _schedule_to_trade_signal_map(schedule)
    numeric_cols = normalize_feature_columns(numeric_exposures)
    categorical_cols = normalize_feature_columns(categorical_exposures)

    if not numeric_cols and not categorical_cols:
        if exposure_panel is not None:
            if "log_market_cap" in exposure_panel.columns:
                numeric_cols.append("log_market_cap")
            if "industry_code" in exposure_panel.columns:
                categorical_cols.append("industry_code")
        if factor_frame is not None:
            skip = {TRADE_DATE, ASSET_ID, "ticker"}
            for column in factor_frame.columns:
                if column in skip or column in numeric_cols or column in categorical_cols:
                    continue
                if pd.api.types.is_numeric_dtype(factor_frame[column]):
                    numeric_cols.append(column)

    if working.empty:
        return TargetExposureResult(
            numeric=_empty_numeric(),
            categorical=_empty_categorical(),
        )

    signal_lookup = _build_signal_lookup(
        factor_frame=factor_frame,
        exposure_panel=exposure_panel,
        numeric_cols=numeric_cols,
        categorical_cols=categorical_cols,
    )

    numeric_rows: list[dict[str, Any]] = []
    categorical_rows: list[dict[str, Any]] = []

    for trade_date, day in working.groupby("trade_date", sort=True):
        signal_date = trade_to_signal.get(normalize_trade_date(trade_date))
        if signal_date is None:
            raise ExposureAnalysisError(
                f"missing signal_date mapping for trade_date={trade_date}"
            )
        positive = day[day["target_weight"] > 0]
        total_positive = float(positive["target_weight"].sum()) if not positive.empty else 0.0

        if positive.empty:
            for exposure in numeric_cols:
                numeric_rows.append(
                    {
                        "signal_date": signal_date.strftime("%Y-%m-%d"),
                        "trade_date": trade_date,
                        "exposure": exposure,
                        "weighted_mean": math.nan,
                        "covered_weight": 0.0,
                        "missing_weight": 0.0,
                    }
                )
            continue

        weights = {
            str(asset_id): float(weight)
            for asset_id, weight in zip(
                positive["asset_id"].tolist(),
                positive["target_weight"].tolist(),
                strict=True,
            )
        }

        for exposure in numeric_cols:
            covered = 0.0
            weighted_sum = 0.0
            for asset_id, weight in weights.items():
                value = signal_lookup.get((signal_date, asset_id, exposure))
                if value is None or not _is_finite(value):
                    continue
                covered += weight
                weighted_sum += weight * float(value)
            missing_weight = max(total_positive - covered, 0.0)
            mean = weighted_sum / covered if covered > 0 else math.nan
            numeric_rows.append(
                {
                    "signal_date": signal_date.strftime("%Y-%m-%d"),
                    "trade_date": trade_date,
                    "exposure": exposure,
                    "weighted_mean": mean,
                    "covered_weight": covered,
                    "missing_weight": missing_weight,
                }
            )

        for exposure in categorical_cols:
            category_weights: dict[str, float] = {}
            for asset_id, weight in weights.items():
                category = signal_lookup.get((signal_date, asset_id, exposure))
                if category is None or _is_missing_category(category):
                    continue
                key = str(category)
                category_weights[key] = category_weights.get(key, 0.0) + weight
            category_total = sum(category_weights.values())
            if category_total > total_positive + 1e-9:
                raise ExposureAnalysisError(
                    "categorical exposure weights exceed positive target gross "
                    f"on trade_date={trade_date} exposure={exposure!r}: "
                    f"{category_total} > {total_positive}"
                )
            for category in sorted(category_weights):
                categorical_rows.append(
                    {
                        "signal_date": signal_date.strftime("%Y-%m-%d"),
                        "trade_date": trade_date,
                        "exposure": exposure,
                        "category": category,
                        "target_weight": category_weights[category],
                    }
                )

    numeric = (
        pd.DataFrame(numeric_rows, columns=list(NUMERIC_EXPOSURE_COLUMNS))
        if numeric_rows
        else _empty_numeric()
    )
    categorical = (
        pd.DataFrame(categorical_rows, columns=list(CATEGORICAL_EXPOSURE_COLUMNS))
        if categorical_rows
        else _empty_categorical()
    )
    if not numeric.empty:
        numeric = numeric.sort_values(
            ["trade_date", "exposure"], kind="mergesort"
        ).reset_index(drop=True)
    if not categorical.empty:
        categorical = categorical.sort_values(
            ["trade_date", "exposure", "category"],
            kind="mergesort",
        ).reset_index(drop=True)
    return TargetExposureResult(numeric=numeric, categorical=categorical)


def _validate_target_weights(target_weights: pd.DataFrame) -> pd.DataFrame:
    working = target_weights.copy()
    if working.empty:
        return working
    try:
        working["trade_date"] = working["trade_date"].map(
            lambda value: normalize_trade_date(value).strftime("%Y-%m-%d")
        )
    except Exception as exc:
        raise ExposureAnalysisError("target_weights contains invalid trade_date") from exc
    working["asset_id"] = working["asset_id"].astype(str)
    if working["asset_id"].eq("").any() or working["asset_id"].isin({"nan", "None"}).any():
        raise ExposureAnalysisError("target_weights contains missing asset_id values")
    if working.duplicated(["trade_date", "asset_id"], keep=False).any():
        raise ExposureAnalysisError(
            "target_weights has duplicate (trade_date, asset_id) pairs"
        )
    weights = pd.to_numeric(working["target_weight"], errors="coerce")
    if weights.isna().any() or not weights.map(_is_finite).all():
        raise ExposureAnalysisError("target_weight values must be finite numbers")
    if (weights < 0).any():
        raise ExposureAnalysisError("target_weight values must be >= 0")
    working["target_weight"] = weights.astype(float)
    sums = working.groupby("trade_date")["target_weight"].sum()
    if (sums > 1.0 + 1e-9).any():
        raise ExposureAnalysisError(
            "target weights must sum to <= 1 on each trade_date"
        )
    return working


def _schedule_to_trade_signal_map(
    schedule: pd.DataFrame | Mapping[Any, Any],
) -> dict[pd.Timestamp, pd.Timestamp]:
    """Normalize schedule to trade_date -> signal_date.

    Mapping input is strictly ``signal_date -> trade_date``.
    """
    if isinstance(schedule, pd.DataFrame):
        required = {"signal_date", "trade_date"}
        missing = required - set(schedule.columns)
        if missing:
            raise ExposureAnalysisError(
                f"schedule missing required columns: {sorted(missing)}"
            )
        if schedule.empty:
            return {}
        signals = [normalize_trade_date(value) for value in schedule["signal_date"]]
        trades = [normalize_trade_date(value) for value in schedule["trade_date"]]
        if len(signals) != len(set(signals)):
            raise ExposureAnalysisError("schedule signal_date values must be unique")
        if len(trades) != len(set(trades)):
            raise ExposureAnalysisError("schedule trade_date values must be unique")
        mapping: dict[pd.Timestamp, pd.Timestamp] = {}
        for signal, trade in zip(signals, trades, strict=True):
            if not trade > signal:
                raise ExposureAnalysisError(
                    "schedule trade_date must be strictly after signal_date: "
                    f"{signal.strftime('%Y-%m-%d')} -> {trade.strftime('%Y-%m-%d')}"
                )
            if trade in mapping:
                raise ExposureAnalysisError(
                    f"duplicate schedule trade_date: {trade.strftime('%Y-%m-%d')}"
                )
            mapping[trade] = signal
        return mapping

    if isinstance(schedule, Mapping):
        mapping = {}
        for signal_key, trade_value in schedule.items():
            signal = normalize_trade_date(signal_key)
            trade = normalize_trade_date(trade_value)
            if not trade > signal:
                raise ExposureAnalysisError(
                    "schedule trade_date must be strictly after signal_date: "
                    f"{signal.strftime('%Y-%m-%d')} -> {trade.strftime('%Y-%m-%d')}"
                )
            if trade in mapping:
                raise ExposureAnalysisError(
                    f"duplicate schedule trade_date: {trade.strftime('%Y-%m-%d')}"
                )
            # Mapping contract is signal_date -> trade_date only.
            mapping[trade] = signal
        # Also enforce unique signal dates when provided via mapping.
        if len(mapping) != len({signal for signal in mapping.values()}):
            raise ExposureAnalysisError("schedule signal_date values must be unique")
        return mapping

    raise ExposureAnalysisError(
        "schedule must be a DataFrame or signal_date->trade_date mapping"
    )


def _build_signal_lookup(
    *,
    factor_frame: pd.DataFrame | None,
    exposure_panel: pd.DataFrame | None,
    numeric_cols: Sequence[str],
    categorical_cols: Sequence[str],
) -> dict[tuple[pd.Timestamp, str, str], Any]:
    lookup: dict[tuple[pd.Timestamp, str, str], Any] = {}
    frames: list[tuple[pd.DataFrame, Sequence[str]]] = []
    if factor_frame is not None:
        cols = [
            c
            for c in list(numeric_cols) + list(categorical_cols)
            if c in factor_frame.columns
        ]
        if cols:
            frames.append((factor_frame, cols))
    if exposure_panel is not None:
        cols = [
            c
            for c in list(numeric_cols) + list(categorical_cols)
            if c in exposure_panel.columns
        ]
        if cols:
            frames.append((exposure_panel, cols))
    for frame, cols in frames:
        try:
            normalized = ensure_cross_section_frame(
                frame,
                feature_columns=cols,
                copy=True,
                enforce_primary_key=True,
            )
        except CrossSectionFrameError as exc:
            raise ExposureAnalysisError(str(exc)) from exc
        for _, row in normalized.iterrows():
            signal = normalize_trade_date(row[TRADE_DATE])
            asset_id = str(row[ASSET_ID])
            for column in cols:
                lookup[(signal, asset_id, column)] = row[column]
    return lookup


def _is_finite(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def _is_missing_category(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "<na>", "nat", "null"}


def _empty_numeric() -> pd.DataFrame:
    frame = pd.DataFrame(columns=list(NUMERIC_EXPOSURE_COLUMNS))
    for column in ("signal_date", "trade_date", "exposure"):
        frame[column] = pd.Series(dtype=object)
    for column in ("weighted_mean", "covered_weight", "missing_weight"):
        frame[column] = pd.Series(dtype=float)
    return frame


def _empty_categorical() -> pd.DataFrame:
    frame = pd.DataFrame(columns=list(CATEGORICAL_EXPOSURE_COLUMNS))
    for column in ("signal_date", "trade_date", "exposure", "category"):
        frame[column] = pd.Series(dtype=object)
    frame["target_weight"] = pd.Series(dtype=float)
    return frame


def empty_numeric_exposures() -> pd.DataFrame:
    """Public empty numeric exposure schema."""
    return _empty_numeric()


def empty_categorical_exposures() -> pd.DataFrame:
    """Public empty categorical exposure schema."""
    return _empty_categorical()
