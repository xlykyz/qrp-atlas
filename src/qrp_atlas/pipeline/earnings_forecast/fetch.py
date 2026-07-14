"""Fetch earnings forecast rows from Tushare forecast / forecast_vip."""

from __future__ import annotations

import time
from typing import Any, Callable, Iterable, Sequence

import pandas as pd

from qrp_atlas.config import get_tushare_pro

# Business source identity is endpoint-agnostic.
# Endpoint name is retained only in fetch diagnostics / raw metadata.
SOURCE_BUSINESS = "tushare.earnings_forecast"
ENDPOINT_FORECAST = "forecast"
ENDPOINT_FORECAST_VIP = "forecast_vip"

REQUIRED_RAW_FIELDS = (
    "ts_code",
    "ann_date",
    "end_date",
    "type",
    "p_change_min",
    "p_change_max",
    "net_profit_min",
    "net_profit_max",
    "last_parent_net",
    "first_ann_date",
    "summary",
    "change_reason",
)


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
    **kwargs,
) -> pd.DataFrame:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            df = func(**kwargs)
            if df is None:
                return pd.DataFrame()
            if not isinstance(df, pd.DataFrame):
                raise ForecastApiError(f"{endpoint} returned non-DataFrame: {type(df)!r}")
            return df
        except Exception as exc:  # network / gateway / permission
            if _is_permission_error(exc):
                raise ForecastPermissionError(
                    f"{endpoint} permission/points failure: {exc}"
                ) from exc
            last_err = exc
            if i + 1 >= retries:
                break
            time.sleep(base_sleep * (i + 1))
    if last_err is not None:
        raise ForecastApiError(f"{endpoint} failed after retries: {last_err}") from last_err
    return pd.DataFrame()


def _ensure_raw_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Keep all required raw fields present even when the API omits empty columns."""
    if df is None or df.empty:
        return pd.DataFrame(columns=list(REQUIRED_RAW_FIELDS))
    out = df.copy()
    for col in REQUIRED_RAW_FIELDS:
        if col not in out.columns:
            out[col] = None
    # Preserve any extra diagnostic columns but ensure required ones exist.
    return out


def fetch_forecast_vip(
    period: str,
    *,
    client=None,
    tickers: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Market-wide historical pull by report period via forecast_vip."""
    period = _normalize_yyyymmdd(period, field_name="period")
    pro = client or get_tushare_pro()
    method = getattr(pro, ENDPOINT_FORECAST_VIP, None)
    if method is None:
        raise ForecastApiError(f"client missing endpoint {ENDPOINT_FORECAST_VIP}")
    df = _call_with_retry(method, endpoint=ENDPOINT_FORECAST_VIP, period=period)
    df = _ensure_raw_columns(df)
    if tickers:
        ticker_set = set(tickers)
        df = df[df["ts_code"].isin(ticker_set)].copy()
    if not df.empty:
        df = df.copy()
        df.attrs["fetch_endpoint"] = ENDPOINT_FORECAST_VIP
        df.attrs["business_source"] = SOURCE_BUSINESS
    return df.reset_index(drop=True)


def fetch_forecast(
    *,
    ts_code: str | None = None,
    period: str | None = None,
    ann_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
) -> pd.DataFrame:
    """Single-stock / ann_date / targeted forecast pull."""
    pro = client or get_tushare_pro()
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
    df = _call_with_retry(method, endpoint=ENDPOINT_FORECAST, **kwargs)
    df = _ensure_raw_columns(df)
    if not df.empty:
        df = df.copy()
        df.attrs["fetch_endpoint"] = ENDPOINT_FORECAST
        df.attrs["business_source"] = SOURCE_BUSINESS
    return df.reset_index(drop=True)


def fetch_forecast_by_tickers(
    tickers: Iterable[str],
    *,
    periods: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
) -> pd.DataFrame:
    """Loop forecast(ts_code=...) for targeted debug / gap-fill."""
    frames: list[pd.DataFrame] = []
    period_set = None
    if periods is not None:
        period_set = {_normalize_yyyymmdd(p, field_name="period") for p in periods}
    for ts_code in tickers:
        df = fetch_forecast(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            client=client,
        )
        if df is None or df.empty:
            continue
        if period_set is not None and "end_date" in df.columns:
            mask = df["end_date"].astype(str).str.replace("-", "", regex=False).isin(period_set)
            df = df.loc[mask].copy()
        if not df.empty:
            frames.append(df)
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
) -> pd.DataFrame:
    """Daily incremental candidate: forecast(ann_date=YYYYMMDD)."""
    df = fetch_forecast(ann_date=ann_date, client=client)
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
            fetch_forecast_vip(p, client=client, tickers=tickers)
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
        )
    if mode == "ann_date":
        if not ann_dates:
            raise ValueError("ann_dates is required when mode='ann_date'")
        frames = [
            fetch_forecast_by_ann_date(d, client=client, tickers=tickers)
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
