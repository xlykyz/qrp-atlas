"""Prepare market residual research panels for indicators and strategies.

This module lives on the backtest data-preparation boundary. It converts asset
and benchmark prices into aligned simple returns without querying DuckDB inside
indicator or strategy layers.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from qrp_atlas.backtest.exposure_data import (
    DEFAULT_CLASSIFICATION_SYSTEM,
    DEFAULT_INDUSTRY_LEVEL,
    ExposurePanelError,
    prepare_cross_section_exposure_panel,
)
from qrp_atlas.contracts import ASSET_ID, CLOSE, INDUSTRY_CODE, TICKER, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    normalize_asset_id,
    normalize_trade_date,
)
from qrp_atlas.indicators.stock.residual import (
    ASSET_RETURN,
    BENCHMARK_ID,
    BENCHMARK_RETURN,
    RESIDUAL_OUTPUT_COLUMNS,
    ResidualIndicatorError,
    calculate_market_residuals,
)

PREP_MISSING_INDUSTRY = "MISSING_INDUSTRY"
PREP_MISSING_INDUSTRY_BENCHMARK = "MISSING_INDUSTRY_BENCHMARK"
PREPARATION_DIAGNOSTIC_CODE = "preparation_diagnostic_code"
INDICATOR_DIAGNOSTIC_CODE = "indicator_diagnostic_code"


class ResidualDataError(ValueError):
    """Raised when residual market data cannot be prepared."""


@dataclass(frozen=True)
class ResidualPanelPreparation:
    """Aligned residual-ready panel and preparation diagnostics."""

    panel: pd.DataFrame
    diagnostics: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "panel": self.panel.to_dict(orient="list"),
            "diagnostics": list(self.diagnostics),
            "metadata": dict(self.metadata),
        }


def _empty_panel() -> pd.DataFrame:
    columns = [
        TRADE_DATE,
        ASSET_ID,
        TICKER,
        BENCHMARK_ID,
        CLOSE,
        ASSET_RETURN,
        BENCHMARK_RETURN,
        "open",
        "high",
        "low",
    ]
    frame = pd.DataFrame(columns=columns)
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    for column in columns:
        if column == TRADE_DATE:
            continue
        if column in {ASSET_ID, TICKER, BENCHMARK_ID}:
            frame[column] = pd.Series(dtype=object)
        else:
            frame[column] = pd.Series(dtype="float64")
    return frame


def _empty_industry_panel() -> pd.DataFrame:
    columns = [
        TRADE_DATE,
        ASSET_ID,
        TICKER,
        BENCHMARK_ID,
        INDUSTRY_CODE,
        CLOSE,
        ASSET_RETURN,
        BENCHMARK_RETURN,
        PREPARATION_DIAGNOSTIC_CODE,
        "open",
        "high",
        "low",
    ]
    frame = pd.DataFrame(columns=columns)
    frame[TRADE_DATE] = pd.Series(dtype="datetime64[ns]")
    for column in columns:
        if column == TRADE_DATE:
            continue
        if column in {
            ASSET_ID,
            TICKER,
            BENCHMARK_ID,
            INDUSTRY_CODE,
            PREPARATION_DIAGNOSTIC_CODE,
        }:
            frame[column] = pd.Series(dtype=object)
        else:
            frame[column] = pd.Series(dtype="float64")
    return frame


def _require_price_frame(df: pd.DataFrame, *, label: str) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise ResidualDataError(f"{label} must be a pandas DataFrame")
    work = df.copy()
    if ASSET_ID not in work.columns and TICKER in work.columns:
        work[ASSET_ID] = work[TICKER]
    if TICKER not in work.columns and ASSET_ID in work.columns:
        work[TICKER] = work[ASSET_ID]
    required = [TRADE_DATE, ASSET_ID, CLOSE]
    missing = [column for column in required if column not in work.columns]
    if missing:
        raise ResidualDataError(f"{label} missing required columns: {missing}")
    if work.empty:
        return work

    try:
        work[TRADE_DATE] = [normalize_trade_date(value) for value in work[TRADE_DATE].tolist()]
        work[ASSET_ID] = [normalize_asset_id(value) for value in work[ASSET_ID].tolist()]
    except CrossSectionFrameError as exc:
        raise ResidualDataError(str(exc)) from exc

    work[TICKER] = work[TICKER].map(lambda value: str(value))
    work[CLOSE] = pd.to_numeric(work[CLOSE], errors="coerce")
    for column in ("open", "high", "low"):
        if column in work.columns:
            work[column] = pd.to_numeric(work[column], errors="coerce")

    if work.duplicated(subset=[ASSET_ID, TRADE_DATE], keep=False).any():
        raise ResidualDataError(
            f"{label} has duplicate (asset_id, trade_date) rows"
        )
    return work.sort_values([ASSET_ID, TRADE_DATE], kind="mergesort").reset_index(drop=True)


def _simple_returns(closes: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(closes, errors="coerce").astype("float64")
    prev = numeric.shift(1)
    returns = numeric / prev - 1.0
    current_ok = numeric.map(
        lambda value: bool(pd.notna(value) and math.isfinite(float(value)) and float(value) > 0.0)
    )
    previous_ok = prev.map(
        lambda value: bool(pd.notna(value) and math.isfinite(float(value)) and float(value) > 0.0)
    )
    return returns.where(current_ok & previous_ok).astype("float64")


def prepare_market_residual_panel(
    asset_prices: pd.DataFrame,
    benchmark_prices: pd.DataFrame,
    *,
    benchmark_id: str | None = None,
    window: int = 60,
    min_periods: int | None = None,
    z_window: int = 60,
    fit_intercept: bool = True,
    compute_residuals: bool = True,
) -> ResidualPanelPreparation:
    """Build an exact-date residual panel from asset and benchmark prices.

    Rules:
    - simple close-to-close returns by asset and by benchmark independently;
    - exact trade_date join only (no forward/backward fill of benchmark);
    - duplicate dates are rejected;
    - missing same-day benchmark return yields NaN residual inputs and diagnostics.
    """

    assets = _require_price_frame(asset_prices, label="asset_prices")
    benchmark = _require_price_frame(benchmark_prices, label="benchmark_prices")

    if assets.empty:
        return ResidualPanelPreparation(
            panel=_empty_panel(),
            diagnostics=(),
            metadata={
                "benchmark_id": benchmark_id,
                "asset_rows": 0,
                "benchmark_rows": int(len(benchmark)),
                "compute_residuals": compute_residuals,
            },
        )

    if benchmark.empty:
        raise ResidualDataError("benchmark_prices is empty")

    benchmark_ids = sorted(set(benchmark[ASSET_ID].astype(str)))
    if benchmark_id is None:
        if len(benchmark_ids) != 1:
            raise ResidualDataError(
                "benchmark_id is required when benchmark_prices contains multiple assets"
            )
        resolved_benchmark_id = benchmark_ids[0]
    else:
        resolved_benchmark_id = str(benchmark_id).strip()
        if not resolved_benchmark_id:
            raise ResidualDataError("benchmark_id must be a non-empty string")
        if resolved_benchmark_id not in benchmark_ids:
            # Allow a caller-provided identity label that still maps through one series.
            if len(benchmark_ids) != 1:
                raise ResidualDataError(
                    f"benchmark_id {resolved_benchmark_id!r} not found in benchmark_prices"
                )
            # Keep the single series, but label with caller identity.
            pass

    if len(benchmark_ids) == 1:
        bench = benchmark[benchmark[ASSET_ID] == benchmark_ids[0]].copy()
    else:
        bench = benchmark[benchmark[ASSET_ID] == resolved_benchmark_id].copy()
    if bench.empty:
        raise ResidualDataError("no benchmark rows available after identity resolution")
    if bench.duplicated(subset=[TRADE_DATE], keep=False).any():
        raise ResidualDataError("benchmark_prices has duplicate trade_date rows")

    bench = bench.sort_values(TRADE_DATE, kind="mergesort").reset_index(drop=True)
    bench[BENCHMARK_RETURN] = _simple_returns(bench[CLOSE])
    bench_map = {
        pd.Timestamp(date): (
            float(value) if pd.notna(value) and math.isfinite(float(value)) else math.nan
        )
        for date, value in zip(bench[TRADE_DATE].tolist(), bench[BENCHMARK_RETURN].tolist(), strict=True)
    }
    available_dates = set(bench_map)

    diagnostics: list[str] = []
    pieces: list[pd.DataFrame] = []
    for asset, group in assets.groupby(ASSET_ID, sort=False):
        piece = group.copy()
        piece[ASSET_RETURN] = _simple_returns(piece[CLOSE])
        piece[BENCHMARK_ID] = resolved_benchmark_id
        piece[BENCHMARK_RETURN] = [
            bench_map.get(pd.Timestamp(date), math.nan) for date in piece[TRADE_DATE].tolist()
        ]
        missing_mask = ~piece[TRADE_DATE].isin(available_dates)
        for date in piece.loc[missing_mask, TRADE_DATE].tolist():
            diagnostics.append(
                f"{asset}|{pd.Timestamp(date).strftime('%Y-%m-%d')}|MISSING_BENCHMARK"
            )
        # Same-day exact alignment only; leave NaN when benchmark return absent.
        pieces.append(piece)

    panel = pd.concat(pieces, ignore_index=True) if pieces else _empty_panel()
    panel = panel.sort_values([TRADE_DATE, ASSET_ID], kind="mergesort").reset_index(drop=True)

    keep_cols = [
        TRADE_DATE,
        ASSET_ID,
        TICKER,
        BENCHMARK_ID,
        CLOSE,
        ASSET_RETURN,
        BENCHMARK_RETURN,
    ]
    for column in ("open", "high", "low"):
        if column in panel.columns:
            keep_cols.append(column)
    panel = panel.loc[:, keep_cols].copy()

    metadata: dict[str, Any] = {
        "benchmark_id": resolved_benchmark_id,
        "asset_count": int(panel[ASSET_ID].nunique()) if not panel.empty else 0,
        "row_count": int(len(panel)),
        "benchmark_date_count": int(len(bench)),
        "missing_benchmark_count": int(sum(1 for item in diagnostics if item.endswith("MISSING_BENCHMARK"))),
        "date_range": {
            "start": (
                None
                if panel.empty
                else pd.Timestamp(panel[TRADE_DATE].min()).strftime("%Y-%m-%d")
            ),
            "end": (
                None
                if panel.empty
                else pd.Timestamp(panel[TRADE_DATE].max()).strftime("%Y-%m-%d")
            ),
        },
        "window": window,
        "min_periods": min_periods if min_periods is not None else window,
        "z_window": z_window,
        "fit_intercept": fit_intercept,
        "compute_residuals": compute_residuals,
    }

    if compute_residuals:
        try:
            residual_result = calculate_market_residuals(
                panel,
                window=window,
                min_periods=min_periods,
                z_window=z_window,
                fit_intercept=fit_intercept,
            )
        except ResidualIndicatorError as exc:
            raise ResidualDataError(str(exc)) from exc
        residual_frame = residual_result.frame
        merge_cols = [
            TRADE_DATE,
            ASSET_ID,
            *RESIDUAL_OUTPUT_COLUMNS,
            "diagnostic_code",
        ]
        panel = panel.merge(
            residual_frame[merge_cols],
            on=[TRADE_DATE, ASSET_ID],
            how="left",
            sort=False,
        )
        diagnostics.extend(residual_result.diagnostics)
        metadata["residual_calculation"] = dict(residual_result.metadata)
        metadata["usable_residual_count"] = int(
            residual_frame["residual_return"].notna().sum()
        ) if not residual_frame.empty else 0

    return ResidualPanelPreparation(
        panel=panel.sort_values([TRADE_DATE, ASSET_ID], kind="mergesort").reset_index(drop=True),
        diagnostics=tuple(diagnostics),
        metadata=metadata,
    )


def _require_industry_benchmark_prices(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise ResidualDataError("industry_benchmark_prices must be a pandas DataFrame")
    work = df.copy()
    required = [TRADE_DATE, INDUSTRY_CODE, CLOSE]
    missing = [column for column in required if column not in work.columns]
    if missing:
        raise ResidualDataError(
            f"industry_benchmark_prices missing required columns: {missing}"
        )
    if work.empty:
        return work
    try:
        work[TRADE_DATE] = [
            normalize_trade_date(value) for value in work[TRADE_DATE].tolist()
        ]
    except CrossSectionFrameError as exc:
        raise ResidualDataError(str(exc)) from exc
    work[INDUSTRY_CODE] = [
        None
        if value is None or (isinstance(value, float) and math.isnan(value))
        else str(value).strip()
        for value in work[INDUSTRY_CODE].tolist()
    ]
    if any(code is None or code == "" for code in work[INDUSTRY_CODE].tolist()):
        raise ResidualDataError(
            "industry_benchmark_prices contains empty industry_code values"
        )
    work[CLOSE] = pd.to_numeric(work[CLOSE], errors="coerce")
    if work.duplicated(subset=[INDUSTRY_CODE, TRADE_DATE], keep=False).any():
        raise ResidualDataError(
            "industry_benchmark_prices has duplicate (industry_code, trade_date) rows"
        )
    return work.sort_values([INDUSTRY_CODE, TRADE_DATE], kind="mergesort").reset_index(
        drop=True
    )


def _require_industry_benchmark_returns(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise ResidualDataError("industry_benchmark_returns must be a pandas DataFrame")
    work = df.copy()
    required = [TRADE_DATE, INDUSTRY_CODE, BENCHMARK_RETURN]
    missing = [column for column in required if column not in work.columns]
    if missing:
        raise ResidualDataError(
            f"industry_benchmark_returns missing required columns: {missing}"
        )
    if work.empty:
        return work
    try:
        work[TRADE_DATE] = [
            normalize_trade_date(value) for value in work[TRADE_DATE].tolist()
        ]
    except CrossSectionFrameError as exc:
        raise ResidualDataError(str(exc)) from exc
    work[INDUSTRY_CODE] = [
        None
        if value is None or (isinstance(value, float) and math.isnan(value))
        else str(value).strip()
        for value in work[INDUSTRY_CODE].tolist()
    ]
    if any(code is None or code == "" for code in work[INDUSTRY_CODE].tolist()):
        raise ResidualDataError(
            "industry_benchmark_returns contains empty industry_code values"
        )
    work[BENCHMARK_RETURN] = pd.to_numeric(work[BENCHMARK_RETURN], errors="coerce")
    if work.duplicated(subset=[INDUSTRY_CODE, TRADE_DATE], keep=False).any():
        raise ResidualDataError(
            "industry_benchmark_returns has duplicate (industry_code, trade_date) rows"
        )
    return work.sort_values([INDUSTRY_CODE, TRADE_DATE], kind="mergesort").reset_index(
        drop=True
    )


def _build_industry_benchmark_return_map(
    *,
    industry_benchmark_prices: pd.DataFrame | None,
    industry_benchmark_returns: pd.DataFrame | None,
    trading_calendar: Sequence[Any] | None = None,
) -> dict[tuple[pd.Timestamp, str], float]:
    """Map exact (trade_date, industry_code) -> single-day benchmark return.

    For price inputs, returns are formed only when both T and the previous
    calendar trading day have valid positive closes. Sparse industry records
    are reindexed onto the provided calendar so a missing day never creates a
    multi-day return on the next available observation.
    """

    if industry_benchmark_prices is not None and industry_benchmark_returns is not None:
        raise ResidualDataError(
            "provide either industry_benchmark_prices or industry_benchmark_returns, not both"
        )
    if industry_benchmark_prices is None and industry_benchmark_returns is None:
        raise ResidualDataError(
            "industry_benchmark_prices or industry_benchmark_returns is required"
        )

    result: dict[tuple[pd.Timestamp, str], float] = {}
    if industry_benchmark_returns is not None:
        frame = _require_industry_benchmark_returns(industry_benchmark_returns)
        for date, code, value in zip(
            frame[TRADE_DATE].tolist(),
            frame[INDUSTRY_CODE].tolist(),
            frame[BENCHMARK_RETURN].tolist(),
            strict=True,
        ):
            if pd.isna(value) or not math.isfinite(float(value)):
                continue
            result[(pd.Timestamp(date), str(code))] = float(value)
        return result

    frame = _require_industry_benchmark_prices(industry_benchmark_prices)
    if frame.empty:
        return result

    calendar_dates: list[pd.Timestamp]
    if trading_calendar is None:
        calendar_dates = sorted({pd.Timestamp(value) for value in frame[TRADE_DATE].tolist()})
    else:
        calendar_dates = []
        seen: set[pd.Timestamp] = set()
        for value in trading_calendar:
            try:
                date = normalize_trade_date(value)
            except CrossSectionFrameError as exc:
                raise ResidualDataError(str(exc)) from exc
            if date in seen:
                continue
            seen.add(date)
            calendar_dates.append(date)
        calendar_dates = sorted(calendar_dates)
    if not calendar_dates:
        return result

    calendar_index = pd.DatetimeIndex(calendar_dates)
    for code, group in frame.groupby(INDUSTRY_CODE, sort=False):
        piece = group.sort_values(TRADE_DATE, kind="mergesort")
        closes = (
            pd.Series(
                pd.to_numeric(piece[CLOSE], errors="coerce").to_numpy(),
                index=pd.DatetimeIndex(piece[TRADE_DATE].tolist()),
                dtype="float64",
            )
            .groupby(level=0, sort=False)
            .last()
            .reindex(calendar_index)
        )
        # Exact adjacent-calendar-day returns only; missing intermediate days
        # yield NaN rather than bridging across the gap.
        returns = _simple_returns(closes)
        for date, value in zip(closes.index.tolist(), returns.tolist(), strict=True):
            if pd.isna(value) or not math.isfinite(float(value)):
                continue
            result[(pd.Timestamp(date), str(code))] = float(value)
    return result


def prepare_industry_residual_panel(
    asset_prices: pd.DataFrame,
    *,
    industry_benchmark_prices: pd.DataFrame | None = None,
    industry_benchmark_returns: pd.DataFrame | None = None,
    industry_panel: pd.DataFrame | None = None,
    industry_query: Any | None = None,
    classification_system: str = DEFAULT_CLASSIFICATION_SYSTEM,
    industry_level: int = DEFAULT_INDUSTRY_LEVEL,
    db_path: Any = None,
    con: Any = None,
    window: int = 60,
    min_periods: int | None = None,
    z_window: int = 60,
    fit_intercept: bool = True,
    compute_residuals: bool = True,
) -> ResidualPanelPreparation:
    """Build industry residual panel with PIT industry membership and exact join.

    Industry membership for date T uses ``prepare_cross_section_exposure_panel``
    / ``query_industry_as_of(as_of_date=T)`` semantics. Benchmark returns join on
    exact ``(trade_date, industry_code)`` only — no fill, no market fallback.

    Output is compatible with :func:`calculate_market_residuals` where
    ``benchmark_id = industry_code``.
    """

    assets = _require_price_frame(asset_prices, label="asset_prices")
    trading_calendar = (
        sorted({pd.Timestamp(value) for value in assets[TRADE_DATE].tolist()})
        if not assets.empty
        else []
    )
    bench_map = _build_industry_benchmark_return_map(
        industry_benchmark_prices=industry_benchmark_prices,
        industry_benchmark_returns=industry_benchmark_returns,
        trading_calendar=trading_calendar,
    )

    if assets.empty:
        return ResidualPanelPreparation(
            panel=_empty_industry_panel(),
            diagnostics=(),
            metadata={
                "benchmark_kind": "industry",
                "classification_system": classification_system,
                "industry_level": industry_level,
                "asset_count": 0,
                "row_count": 0,
                "compute_residuals": compute_residuals,
            },
        )

    universe = assets[[TRADE_DATE, ASSET_ID]].drop_duplicates().copy()
    try:
        exposure = prepare_cross_section_exposure_panel(
            universe,
            industry_panel=industry_panel,
            industry_query=industry_query,
            classification_system=classification_system,
            industry_level=industry_level,
            db_path=db_path,
            con=con,
            size_panel=pd.DataFrame(),
        )
    except ExposurePanelError as exc:
        raise ResidualDataError(str(exc)) from exc

    industry_lookup = {
        (pd.Timestamp(date), str(asset)): (
            None
            if code is None or (isinstance(code, float) and math.isnan(code))
            else str(code)
        )
        for date, asset, code in zip(
            exposure[TRADE_DATE].tolist(),
            exposure[ASSET_ID].tolist(),
            exposure[INDUSTRY_CODE].tolist(),
            strict=True,
        )
    }

    diagnostics: list[str] = []
    pieces: list[pd.DataFrame] = []
    missing_industry = 0
    missing_industry_benchmark = 0

    for asset, group in assets.groupby(ASSET_ID, sort=False):
        piece = group.copy()
        piece[ASSET_RETURN] = _simple_returns(piece[CLOSE])
        prep_codes: list[str | None] = []
        industry_codes: list[str | None] = []
        bench_ids: list[str | None] = []
        bench_returns: list[float] = []
        for date in piece[TRADE_DATE].tolist():
            ts = pd.Timestamp(date)
            code = industry_lookup.get((ts, str(asset)))
            if code is None:
                industry_codes.append(None)
                bench_ids.append(None)
                bench_returns.append(math.nan)
                prep_codes.append(PREP_MISSING_INDUSTRY)
                missing_industry += 1
                diagnostics.append(
                    f"{asset}|{ts.strftime('%Y-%m-%d')}|{PREP_MISSING_INDUSTRY}"
                )
                continue
            industry_codes.append(code)
            bench_ids.append(code)
            value = bench_map.get((ts, code))
            if value is None:
                bench_returns.append(math.nan)
                prep_codes.append(PREP_MISSING_INDUSTRY_BENCHMARK)
                missing_industry_benchmark += 1
                diagnostics.append(
                    f"{asset}|{ts.strftime('%Y-%m-%d')}|{PREP_MISSING_INDUSTRY_BENCHMARK}"
                )
            else:
                bench_returns.append(float(value))
                prep_codes.append(None)
        piece[INDUSTRY_CODE] = industry_codes
        piece[BENCHMARK_ID] = bench_ids
        piece[BENCHMARK_RETURN] = bench_returns
        piece[PREPARATION_DIAGNOSTIC_CODE] = prep_codes
        pieces.append(piece)

    panel = pd.concat(pieces, ignore_index=True) if pieces else _empty_industry_panel()
    panel = panel.sort_values([TRADE_DATE, ASSET_ID], kind="mergesort").reset_index(
        drop=True
    )

    keep_cols = [
        TRADE_DATE,
        ASSET_ID,
        TICKER,
        BENCHMARK_ID,
        INDUSTRY_CODE,
        CLOSE,
        ASSET_RETURN,
        BENCHMARK_RETURN,
        PREPARATION_DIAGNOSTIC_CODE,
    ]
    for column in ("open", "high", "low"):
        if column in panel.columns:
            keep_cols.append(column)
    panel = panel.loc[:, keep_cols].copy()

    industry_codes_present = sorted(
        {
            str(code)
            for code in panel[INDUSTRY_CODE].tolist()
            if code is not None and str(code).strip() != "" and str(code).lower() != "nan"
        }
    )
    industry_sample_counts = {
        code: int((panel[INDUSTRY_CODE].astype(str) == code).sum())
        for code in industry_codes_present
    }

    metadata: dict[str, Any] = {
        "benchmark_kind": "industry",
        "classification_system": str(classification_system).strip(),
        "industry_level": int(industry_level),
        "asset_count": int(panel[ASSET_ID].nunique()) if not panel.empty else 0,
        "row_count": int(len(panel)),
        "industry_count": int(len(industry_codes_present)),
        "industry_codes": industry_codes_present,
        "industry_sample_counts": industry_sample_counts,
        "missing_industry_count": int(missing_industry),
        "missing_industry_benchmark_count": int(missing_industry_benchmark),
        "date_range": {
            "start": (
                None
                if panel.empty
                else pd.Timestamp(panel[TRADE_DATE].min()).strftime("%Y-%m-%d")
            ),
            "end": (
                None
                if panel.empty
                else pd.Timestamp(panel[TRADE_DATE].max()).strftime("%Y-%m-%d")
            ),
        },
        "window": window,
        "min_periods": min_periods if min_periods is not None else window,
        "z_window": z_window,
        "fit_intercept": fit_intercept,
        "compute_residuals": compute_residuals,
        "benchmark_input": (
            "returns" if industry_benchmark_returns is not None else "prices"
        ),
    }

    if compute_residuals:
        residual_input = panel.copy()
        residual_input[BENCHMARK_ID] = residual_input[BENCHMARK_ID].where(
            residual_input[BENCHMARK_ID].notna(), other="MISSING"
        )
        try:
            residual_result = calculate_market_residuals(
                residual_input,
                window=window,
                min_periods=min_periods,
                z_window=z_window,
                fit_intercept=fit_intercept,
            )
        except ResidualIndicatorError as exc:
            raise ResidualDataError(str(exc)) from exc
        residual_frame = residual_result.frame.rename(
            columns={"diagnostic_code": INDICATOR_DIAGNOSTIC_CODE}
        )
        merge_cols = [
            TRADE_DATE,
            ASSET_ID,
            *RESIDUAL_OUTPUT_COLUMNS,
            INDICATOR_DIAGNOSTIC_CODE,
        ]
        panel = panel.merge(
            residual_frame[merge_cols],
            on=[TRADE_DATE, ASSET_ID],
            how="left",
            sort=False,
        )
        panel["diagnostic_code"] = panel[INDICATOR_DIAGNOSTIC_CODE]
        diagnostics.extend(residual_result.diagnostics)
        metadata["residual_calculation"] = dict(residual_result.metadata)
        metadata["usable_residual_count"] = (
            int(residual_frame["residual_return"].notna().sum())
            if not residual_frame.empty
            else 0
        )

    return ResidualPanelPreparation(
        panel=panel.sort_values([TRADE_DATE, ASSET_ID], kind="mergesort").reset_index(
            drop=True
        ),
        diagnostics=tuple(diagnostics),
        metadata=metadata,
    )


__all__ = [
    "INDICATOR_DIAGNOSTIC_CODE",
    "PREPARATION_DIAGNOSTIC_CODE",
    "PREP_MISSING_INDUSTRY",
    "PREP_MISSING_INDUSTRY_BENCHMARK",
    "ResidualDataError",
    "ResidualPanelPreparation",
    "prepare_industry_residual_panel",
    "prepare_market_residual_panel",
]
