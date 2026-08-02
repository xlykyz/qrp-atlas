"""Fetch earnings forecast rows from Tushare forecast / forecast_vip."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence

import pandas as pd

from qrp_atlas.config import get_tushare_pro
from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError

# Business source identity is endpoint-agnostic.
# Endpoint name is retained only in fetch diagnostics / raw metadata.
SOURCE_BUSINESS = "tushare.earnings_forecast"
ENDPOINT_FORECAST = "forecast"
ENDPOINT_FORECAST_VIP = "forecast_vip"

# Core identity fields: non-empty responses must include these columns and values.
CORE_RAW_FIELDS = (
    "ts_code",
    "ann_date",
    "end_date",
    "type",
)

# Optional structured / text fields may be filled with None when omitted by API.
OPTIONAL_RAW_FIELDS = (
    "p_change_min",
    "p_change_max",
    "net_profit_min",
    "net_profit_max",
    "last_parent_net",
    "first_ann_date",
    "summary",
    "change_reason",
)

REQUIRED_RAW_FIELDS = CORE_RAW_FIELDS + OPTIONAL_RAW_FIELDS


@dataclass(slots=True)
class ForecastFetchReport:
    """Provider work counters for one explicit earnings-forecast invocation."""

    api_requests: int = 0
    batches: int = 0
    retries: int = 0
    rows_read: int = 0


class ForecastPermissionError(PermissionError):
    """Raised when Tushare returns a permission / points / auth failure."""


class ForecastApiError(RuntimeError):
    """Raised for non-permission forecast API failures."""


_PERMISSION_MARKERS = (
    "积分",
    "权限",
    "没有接口访问权限",
    "没有访问权限",
    "permission",
    "not enough",
    "点数不足",
    "token",
)


def _check(execution_control: ExecutionControl | None) -> None:
    if execution_control is not None:
        execution_control.check()


def _wait(execution_control: ExecutionControl | None, seconds: float) -> None:
    if execution_control is None:
        time.sleep(seconds)
    else:
        execution_control.wait(threading.Event(), timeout=seconds)


def _is_permission_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return any(marker.lower() in text for marker in _PERMISSION_MARKERS)


def _normalize_yyyymmdd(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"{field_name} must be YYYYMMDD, got {value!r}")
    return text


def _call_with_retry(
    func: Callable[..., Any],
    *,
    retries: int = 5,
    base_sleep: float = 1.2,
    endpoint: str,
    execution_control: ExecutionControl | None = None,
    report: ForecastFetchReport | None = None,
    **kwargs,
) -> pd.DataFrame:
    last_err: Exception | None = None
    for i in range(retries):
        _check(execution_control)
        if report is not None:
            report.api_requests += 1
        try:
            df = func(**kwargs)
            _check(execution_control)
            if df is None:
                return pd.DataFrame()
            if not isinstance(df, pd.DataFrame):
                raise ForecastApiError(f"{endpoint} returned non-DataFrame: {type(df)!r}")
            return df
        except ExecutionControlError:
            raise
        except Exception as exc:  # network / gateway / permission
            if _is_permission_error(exc):
                raise ForecastPermissionError(
                    f"{endpoint} permission/points failure: {exc}"
                ) from exc
            last_err = exc
            if i + 1 >= retries:
                break
            if report is not None:
                report.retries += 1
            _wait(execution_control, base_sleep * (i + 1))
    if last_err is not None:
        raise ForecastApiError(f"{endpoint} failed after retries: {last_err}") from last_err
    return pd.DataFrame()


def _ensure_raw_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Validate core columns and fill only optional omitted columns with None.

    Non-empty responses missing core columns fail closed. Empty responses keep a
    stable column layout for downstream empty handling.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=list(REQUIRED_RAW_FIELDS))
    out = df.copy()
    missing_core = [c for c in CORE_RAW_FIELDS if c not in out.columns]
    if missing_core:
        raise ForecastApiError(
            f"non-empty forecast response missing core columns: {missing_core}; "
            f"present={list(out.columns)}"
        )
    for col in OPTIONAL_RAW_FIELDS:
        if col not in out.columns:
            out[col] = None
    return out


def fetch_forecast_vip(
    period: str,
    *,
    client=None,
    tickers: Sequence[str] | None = None,
    execution_control: ExecutionControl | None = None,
    report: ForecastFetchReport | None = None,
    settings=None,
) -> pd.DataFrame:
    """Market-wide historical pull by report period via forecast_vip."""
    period = _normalize_yyyymmdd(period, field_name="period")
    _check(execution_control)
    pro = client or get_tushare_pro(settings=settings, execution_control=execution_control)
    method = getattr(pro, ENDPOINT_FORECAST_VIP, None)
    if method is None:
        raise ForecastApiError(f"client missing endpoint {ENDPOINT_FORECAST_VIP}")
    df = _call_with_retry(
        method,
        endpoint=ENDPOINT_FORECAST_VIP,
        period=period,
        execution_control=execution_control,
        report=report,
    )
    df = _ensure_raw_columns(df)
    if tickers:
        ticker_set = set(tickers)
        df = df[df["ts_code"].isin(ticker_set)].copy()
    if not df.empty:
        df = df.copy()
        df.attrs["fetch_endpoint"] = ENDPOINT_FORECAST_VIP
        df.attrs["business_source"] = SOURCE_BUSINESS
    if report is not None:
        report.batches += 1
        report.rows_read += len(df)
    _check(execution_control)
    return df.reset_index(drop=True)


def fetch_forecast(
    *,
    ts_code: str | None = None,
    period: str | None = None,
    ann_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
    execution_control: ExecutionControl | None = None,
    report: ForecastFetchReport | None = None,
    settings=None,
) -> pd.DataFrame:
    """Single-stock / ann_date / targeted forecast pull."""
    _check(execution_control)
    pro = client or get_tushare_pro(settings=settings, execution_control=execution_control)
    method = getattr(pro, ENDPOINT_FORECAST, None)
    if method is None:
        raise ForecastApiError(f"client missing endpoint {ENDPOINT_FORECAST}")
    kwargs: dict[str, Any] = {}
    if ts_code is not None:
        kwargs["ts_code"] = str(ts_code).strip()
    if period is not None:
        kwargs["period"] = _normalize_yyyymmdd(period, field_name="period")
    if ann_date is not None:
        kwargs["ann_date"] = _normalize_yyyymmdd(ann_date, field_name="ann_date")
    if start_date is not None:
        kwargs["start_date"] = _normalize_yyyymmdd(start_date, field_name="start_date")
    if end_date is not None:
        kwargs["end_date"] = _normalize_yyyymmdd(end_date, field_name="end_date")
    if not kwargs:
        raise ValueError("at least one of ts_code/period/ann_date/start_date/end_date is required")
    df = _call_with_retry(
        method,
        endpoint=ENDPOINT_FORECAST,
        execution_control=execution_control,
        report=report,
        **kwargs,
    )
    df = _ensure_raw_columns(df)
    if not df.empty:
        df = df.copy()
        df.attrs["fetch_endpoint"] = ENDPOINT_FORECAST
        df.attrs["business_source"] = SOURCE_BUSINESS
    if report is not None:
        report.batches += 1
        report.rows_read += len(df)
    _check(execution_control)
    return df.reset_index(drop=True)


def fetch_forecast_by_tickers(
    tickers: Iterable[str],
    *,
    periods: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
    execution_control: ExecutionControl | None = None,
    report: ForecastFetchReport | None = None,
    settings=None,
) -> pd.DataFrame:
    """Loop forecast(ts_code=...) for targeted debug / gap-fill."""
    frames: list[pd.DataFrame] = []
    period_set = None
    if periods is not None:
        period_set = {_normalize_yyyymmdd(p, field_name="period") for p in periods}
    for ts_code in tickers:
        _check(execution_control)
        df = fetch_forecast(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            client=client,
            execution_control=execution_control,
            report=report,
            settings=settings,
        )
        if df is None or df.empty:
            continue
        if period_set is not None and "end_date" in df.columns:
            mask = df["end_date"].astype(str).str.replace("-", "", regex=False).isin(period_set)
            df = df.loc[mask].copy()
        if not df.empty:
            frames.append(df)
        _check(execution_control)
    if not frames:
        return pd.DataFrame(columns=list(REQUIRED_RAW_FIELDS))
    out = pd.concat(frames, ignore_index=True)
    out.attrs["fetch_endpoint"] = ENDPOINT_FORECAST
    out.attrs["business_source"] = SOURCE_BUSINESS
    return out


def fetch_forecast_by_ann_date(
    ann_date: str,
    *,
    client=None,
    tickers: Sequence[str] | None = None,
    execution_control: ExecutionControl | None = None,
    report: ForecastFetchReport | None = None,
    settings=None,
) -> pd.DataFrame:
    """Daily incremental candidate: forecast(ann_date=YYYYMMDD)."""
    df = fetch_forecast(
        ann_date=ann_date,
        client=client,
        execution_control=execution_control,
        report=report,
        settings=settings,
    )
    if tickers:
        ticker_set = set(tickers)
        df = df[df["ts_code"].isin(ticker_set)].copy()
    return df.reset_index(drop=True)


def fetch_earnings_forecast(
    *,
    mode: str = "period",
    periods: Sequence[str] | None = None,
    tickers: Sequence[str] | None = None,
    ann_dates: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
    execution_control: ExecutionControl | None = None,
    report: ForecastFetchReport | None = None,
    settings=None,
) -> pd.DataFrame:
    """Unified fetch entry for earnings forecast.

    mode:
      - period: forecast_vip by report period (bulk historical)
      - ticker: forecast by ticker list (debug / gap-fill)
      - ann_date: forecast by announcement date (incremental candidate)
    """
    if mode == "period":
        if not periods:
            raise ValueError("periods is required when mode='period'")
        frames = [
            fetch_forecast_vip(
                p,
                client=client,
                tickers=tickers,
                execution_control=execution_control,
                report=report,
                settings=settings,
            )
            for p in periods
        ]
        frames = [f for f in frames if f is not None and not f.empty]
        if not frames:
            return pd.DataFrame(columns=list(REQUIRED_RAW_FIELDS))
        out = pd.concat(frames, ignore_index=True)
        out.attrs["fetch_endpoint"] = ENDPOINT_FORECAST_VIP
        out.attrs["business_source"] = SOURCE_BUSINESS
        return out
    if mode == "ticker":
        if not tickers:
            raise ValueError("tickers is required when mode='ticker'")
        return fetch_forecast_by_tickers(
            tickers,
            periods=periods,
            start_date=start_date,
            end_date=end_date,
            client=client,
            execution_control=execution_control,
            report=report,
            settings=settings,
        )
    if mode == "ann_date":
        if not ann_dates:
            raise ValueError("ann_dates is required when mode='ann_date'")
        frames = [
            fetch_forecast_by_ann_date(
                d,
                client=client,
                tickers=tickers,
                execution_control=execution_control,
                report=report,
                settings=settings,
            )
            for d in ann_dates
        ]
        frames = [f for f in frames if f is not None and not f.empty]
        if not frames:
            return pd.DataFrame(columns=list(REQUIRED_RAW_FIELDS))
        out = pd.concat(frames, ignore_index=True)
        out.attrs["fetch_endpoint"] = ENDPOINT_FORECAST
        out.attrs["business_source"] = SOURCE_BUSINESS
        return out
    raise ValueError(f"unsupported mode: {mode}")
