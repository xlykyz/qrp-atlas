"""Clean / normalize Tushare earnings forecast into contracts."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Sequence

import pandas as pd

from qrp_atlas.contracts import (
    ANNOUNCEMENT_DATE,
    AVAILABLE_TRADE_DATE,
    CHANGE_REASON,
    EVENT_SERIES_ID,
    EVENT_TYPE,
    FIRST_ANNOUNCEMENT_DATE,
    FORECAST_TYPE,
    INGESTED_AT,
    LAST_PARENT_NET,
    NET_PROFIT_MAX,
    NET_PROFIT_MIN,
    PROFIT_CHANGE_MAX,
    PROFIT_CHANGE_MIN,
    PUBLISHED_AT,
    REPORT_PERIOD,
    REVISION_ID,
    SOURCE,
    SOURCE_RECORD_ID,
    SUMMARY,
    TICKER,
    TIME_PRECISION,
    align_to_schema,
    apply_mapping,
    canonicalize,
    quick_validate,
)
from qrp_atlas.pipeline.earnings_forecast.fetch import SOURCE_BUSINESS
from qrp_atlas.pipeline.pit_utils import (
    NextTradeDateResolver,
    content_signature,
    empty_to_none,
    normalize_date_series,
    stable_hash,
    to_date,
)
from qrp_atlas.orchestration.execution_control import ExecutionControl

EVENT_TYPE_EARNINGS_FORECAST = "earnings_forecast"
TIME_PRECISION_DATE = "date"
MAPPING_SOURCE = "tushare_forecast"

# Units (SSOT for this dataset):
# - net_profit_min / net_profit_max: 万元 (Tushare docs)
# - profit_change_min / profit_change_max: percentage points (e.g. 10.5 means 10.5%)
# - last_parent_net: unit not independently confirmed in this task; store raw value
#   and do not convert. 05-B must not compute surprise from this field alone.
NET_PROFIT_UNIT = "万元"
PROFIT_CHANGE_UNIT = "percent"
LAST_PARENT_NET_UNIT = "unconfirmed_raw"

CONTENT_COLS = (
    SOURCE_RECORD_ID,
    FORECAST_TYPE,
    PROFIT_CHANGE_MIN,
    PROFIT_CHANGE_MAX,
    NET_PROFIT_MIN,
    NET_PROFIT_MAX,
    LAST_PARENT_NET,
    FIRST_ANNOUNCEMENT_DATE,
    SUMMARY,
    CHANGE_REASON,
)


class EarningsForecastDataQualityError(ValueError):
    """Raised when earnings forecast rows fail quality rules."""


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _to_float(value):
    if _is_missing(value):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError) as exc:
        raise EarningsForecastDataQualityError(f"non-numeric value: {value!r}") from exc
    if not math.isfinite(num):
        raise EarningsForecastDataQualityError(f"non-finite numeric value: {value!r}")
    return num


def _normalize_text(value) -> str | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    return text or None


def event_series_id(ticker: str, report_period) -> str:
    period = to_date(report_period)
    if period is None:
        raise EarningsForecastDataQualityError(f"invalid report_period for series id: {report_period!r}")
    return stable_hash(
        [str(ticker).strip(), EVENT_TYPE_EARNINGS_FORECAST, period.isoformat()],
        length=20,
    )


def source_record_id(
    *,
    ticker: str,
    report_period,
    announcement_date,
    source: str = SOURCE_BUSINESS,
) -> str:
    period = to_date(report_period)
    ann = to_date(announcement_date)
    if period is None or ann is None:
        raise EarningsForecastDataQualityError(
            f"invalid dates for source_record_id: period={report_period!r} ann={announcement_date!r}"
        )
    return stable_hash(
        [
            source,
            str(ticker).strip(),
            EVENT_TYPE_EARNINGS_FORECAST,
            period.isoformat(),
            ann.isoformat(),
        ],
        length=20,
    )


def revision_id_for_row(row: pd.Series | dict) -> str:
    payload = {c: empty_to_none(row.get(c) if hasattr(row, "get") else row[c]) for c in CONTENT_COLS}
    return content_signature(payload, CONTENT_COLS)


def clean_earnings_forecast(
    df: pd.DataFrame,
    *,
    trade_date_resolver: NextTradeDateResolver | None = None,
    open_dates: Sequence | None = None,
    ingested_at: datetime | None = None,
    source: str = SOURCE_BUSINESS,
    execution_control: ExecutionControl | None = None,
) -> pd.DataFrame:
    """Map raw Tushare forecast rows to earnings_forecast_event schema."""
    if execution_control is not None:
        execution_control.check()
    if df is None or df.empty:
        return pd.DataFrame(columns=[])

    out = apply_mapping(df.copy(), MAPPING_SOURCE)

    # Core mapped columns must exist for non-empty batches.
    core_mapped = [TICKER, REPORT_PERIOD, ANNOUNCEMENT_DATE, FORECAST_TYPE]
    missing_core = [c for c in core_mapped if c not in out.columns]
    if missing_core:
        raise EarningsForecastDataQualityError(
            f"mapped frame missing core columns: {missing_core}; present={list(out.columns)}"
        )

    for col in [REPORT_PERIOD, ANNOUNCEMENT_DATE, FIRST_ANNOUNCEMENT_DATE]:
        if col in out.columns:
            out[col] = normalize_date_series(out[col])
        else:
            out[col] = None

    out[TICKER] = out[TICKER].map(lambda x: None if _is_missing(x) else str(x).strip())
    out[FORECAST_TYPE] = out[FORECAST_TYPE].map(_normalize_text) if FORECAST_TYPE in out.columns else None

    invalid_mask = (
        out[TICKER].isna()
        | out[REPORT_PERIOD].isna()
        | out[ANNOUNCEMENT_DATE].isna()
        | out[FORECAST_TYPE].isna()
    )
    invalid_rows = int(invalid_mask.sum())
    if invalid_rows:
        sample = out.loc[
            invalid_mask,
            [c for c in [TICKER, REPORT_PERIOD, ANNOUNCEMENT_DATE, FORECAST_TYPE] if c in out.columns],
        ].head(5)
        raise EarningsForecastDataQualityError(
            f"core field null/empty in {invalid_rows} rows; sample={sample.to_dict('records')}"
        )

    out.attrs["invalid_rows"] = 0
    out[EVENT_TYPE] = EVENT_TYPE_EARNINGS_FORECAST
    out[TIME_PRECISION] = TIME_PRECISION_DATE
    out[PUBLISHED_AT] = None
    out[SOURCE] = source
    now = ingested_at or datetime.now(timezone.utc).replace(tzinfo=None)
    out[INGESTED_AT] = now

    # Structured optional numeric/text fields
    for col in (
        PROFIT_CHANGE_MIN,
        PROFIT_CHANGE_MAX,
        NET_PROFIT_MIN,
        NET_PROFIT_MAX,
        LAST_PARENT_NET,
    ):
        if col not in out.columns:
            out[col] = None
        out[col] = out[col].map(_to_float)

    if SUMMARY not in out.columns:
        out[SUMMARY] = None
    if CHANGE_REASON not in out.columns:
        out[CHANGE_REASON] = None
    out[SUMMARY] = out[SUMMARY].map(_normalize_text)
    out[CHANGE_REASON] = out[CHANGE_REASON].map(_normalize_text)

    # Range consistency
    for idx, row in out.iterrows():
        if execution_control is not None:
            execution_control.check()
        pmin, pmax = row[PROFIT_CHANGE_MIN], row[PROFIT_CHANGE_MAX]
        if pmin is not None and pmax is not None and pmin > pmax:
            raise EarningsForecastDataQualityError(
                f"profit_change_min > profit_change_max for {row[TICKER]} {row[REPORT_PERIOD]}: {pmin} > {pmax}"
            )
        nmin, nmax = row[NET_PROFIT_MIN], row[NET_PROFIT_MAX]
        if nmin is not None and nmax is not None and nmin > nmax:
            raise EarningsForecastDataQualityError(
                f"net_profit_min > net_profit_max for {row[TICKER]} {row[REPORT_PERIOD]}: {nmin} > {nmax}"
            )

    resolver = trade_date_resolver or NextTradeDateResolver(open_dates)
    out[AVAILABLE_TRADE_DATE] = out[ANNOUNCEMENT_DATE].map(resolver.next_trade_date)
    if out[AVAILABLE_TRADE_DATE].isna().any():
        raise EarningsForecastDataQualityError("available_trade_date could not be resolved for some rows")

    series_ids = []
    source_ids = []
    revision_ids = []
    for _, row in out.iterrows():
        if execution_control is not None:
            execution_control.check()
        sid = event_series_id(row[TICKER], row[REPORT_PERIOD])
        srid = source_record_id(
            ticker=row[TICKER],
            report_period=row[REPORT_PERIOD],
            announcement_date=row[ANNOUNCEMENT_DATE],
            source=source,
        )
        series_ids.append(sid)
        source_ids.append(srid)
        payload = {
            SOURCE_RECORD_ID: srid,
            FORECAST_TYPE: row[FORECAST_TYPE],
            PROFIT_CHANGE_MIN: row[PROFIT_CHANGE_MIN],
            PROFIT_CHANGE_MAX: row[PROFIT_CHANGE_MAX],
            NET_PROFIT_MIN: row[NET_PROFIT_MIN],
            NET_PROFIT_MAX: row[NET_PROFIT_MAX],
            LAST_PARENT_NET: row[LAST_PARENT_NET],
            FIRST_ANNOUNCEMENT_DATE: row[FIRST_ANNOUNCEMENT_DATE],
            SUMMARY: row[SUMMARY],
            CHANGE_REASON: row[CHANGE_REASON],
        }
        revision_ids.append(revision_id_for_row(payload))

    out[EVENT_SERIES_ID] = series_ids
    out[SOURCE_RECORD_ID] = source_ids
    out[REVISION_ID] = revision_ids

    # Identical normalized content collapses safely via revision_id.
    # Distinct contents under the same source_record_id are technical revisions
    # and must all be retained (append-only); never silent keep-first/last.
    out = out.drop_duplicates(subset=[REVISION_ID], keep="last")

    out = align_to_schema(out, "earnings_forecast_event", fill_missing_optional=True, drop_extra=True)
    out = canonicalize(out, "earnings_forecast_event")
    out = quick_validate(out, "earnings_forecast_event", allow_extra=False)
    if execution_control is not None:
        execution_control.check()
    return out.reset_index(drop=True)
