"""Fetch financial statements / indicators from Tushare VIP APIs."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

import pandas as pd

from qrp_atlas.config import get_tushare_pro
from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError

# VIP period endpoints return market-wide rows for one report period.
API_BY_TABLE = {
    "income_statement": "income_vip",
    "balance_sheet": "balancesheet_vip",
    "cashflow_statement": "cashflow_vip",
    "financial_indicator": "fina_indicator_vip",
}

# Non-VIP per-ticker fallback / targeted pull.
API_BY_TICKER = {
    "income_statement": "income",
    "balance_sheet": "balancesheet",
    "cashflow_statement": "cashflow",
    "financial_indicator": "fina_indicator",
}

# Tushare's per-ticker fina_indicator endpoint returns at most 100 rows.  A
# response at the limit is ambiguous: it may be a truncated window, so the
# formal contract must not silently ingest it as a complete scope.
FINANCIAL_TICKER_ROW_LIMIT = 100


class FinancialFetchError(RuntimeError):
    """A provider response cannot be accepted as a complete financial scope."""

    def __init__(self, code: str, detail: str):
        self.code = code
        self.detail = detail
        super().__init__(detail)


@dataclass(slots=True)
class FinancialFetchReport:
    """Provider work counters for one explicit fundamentals invocation."""

    api_requests: int = 0
    batches: int = 0
    retries: int = 0
    rows_read: int = 0


def _check(execution_control: ExecutionControl | None) -> None:
    if execution_control is not None:
        execution_control.check()


def _wait(execution_control: ExecutionControl | None, seconds: float) -> None:
    if execution_control is None:
        time.sleep(seconds)
    else:
        execution_control.wait(threading.Event(), timeout=seconds)


def _call_with_retry(
    func: Callable,
    *,
    retries: int = 5,
    base_sleep: float = 1.2,
    execution_control: ExecutionControl | None = None,
    report: FinancialFetchReport | None = None,
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
            return df
        except ExecutionControlError:
            raise
        except Exception as exc:  # network / gateway blips
            last_err = exc
            if i + 1 >= retries:
                break
            if report is not None:
                report.retries += 1
            _wait(execution_control, base_sleep * (i + 1))
    if last_err is not None:
        raise last_err
    return pd.DataFrame()


def _normalize_period(period: str | None) -> str | None:
    if period is None:
        return None
    text = str(period).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"period must be YYYYMMDD, got {period!r}")
    return text


def fetch_financial_by_period(
    table: str,
    period: str,
    *,
    client=None,
    tickers: Sequence[str] | None = None,
    execution_control: ExecutionControl | None = None,
    report: FinancialFetchReport | None = None,
    settings=None,
) -> pd.DataFrame:
    """Fetch one report period via VIP interface.

    Args:
        table: contracts table name
        period: YYYYMMDD report period
        client: optional tushare pro client / fake client
        tickers: optional filter after fetch
    """
    _check(execution_control)
    api_name = API_BY_TABLE[table]
    period = _normalize_period(period)
    pro = client or get_tushare_pro(settings=settings, execution_control=execution_control)
    method = getattr(pro, api_name)
    df = _call_with_retry(
        method,
        period=period,
        execution_control=execution_control,
        report=report,
    )
    _check(execution_control)
    if df is None or df.empty:
        if report is not None:
            report.batches += 1
        return pd.DataFrame()
    if tickers:
        ticker_set = set(tickers)
        df = df[df["ts_code"].isin(ticker_set)].copy()
    if report is not None:
        report.batches += 1
        report.rows_read += len(df)
    _check(execution_control)
    return df.reset_index(drop=True)


def fetch_financial_by_tickers(
    table: str,
    tickers: Iterable[str],
    *,
    periods: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
    execution_control: ExecutionControl | None = None,
    report: FinancialFetchReport | None = None,
    settings=None,
) -> pd.DataFrame:
    """Fetch by ticker using non-VIP APIs (useful for targeted backfill)."""
    _check(execution_control)
    api_name = API_BY_TICKER[table]
    pro = client or get_tushare_pro(settings=settings, execution_control=execution_control)
    method = getattr(pro, api_name)
    period_set = {_normalize_period(p) for p in periods} if periods else None
    frames: list[pd.DataFrame] = []
    for ts_code in tickers:
        _check(execution_control)
        kwargs = {"ts_code": ts_code}
        if start_date:
            kwargs["start_date"] = _normalize_period(start_date)
        if end_date:
            kwargs["end_date"] = _normalize_period(end_date)
        df = _call_with_retry(
            method,
            execution_control=execution_control,
            report=report,
            **kwargs,
        )
        if report is not None:
            report.batches += 1
        if df is None or df.empty:
            _check(execution_control)
            continue
        if table == "financial_indicator" and len(df) >= FINANCIAL_TICKER_ROW_LIMIT:
            raise FinancialFetchError(
                "FUNDAMENTALS_PAGE_LIMIT_REACHED",
                f"{table}:{ts_code} returned {len(df)} rows; the 100-row provider limit may truncate the requested scope",
            )
        if period_set is not None and "end_date" in df.columns:
            periods_normalized = df["end_date"].astype(str).str.replace("-", "", regex=False)
            df = df[periods_normalized.isin(period_set)]
        if not df.empty:
            frames.append(df)
            if report is not None:
                report.rows_read += len(df)
        _check(execution_control)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_financial(
    table: str,
    *,
    periods: Sequence[str] | None = None,
    tickers: Sequence[str] | None = None,
    mode: str = "period",
    start_date: str | None = None,
    end_date: str | None = None,
    client=None,
    execution_control: ExecutionControl | None = None,
    report: FinancialFetchReport | None = None,
    settings=None,
) -> pd.DataFrame:
    """Unified fetch entry.

    mode:
      - period: VIP by report period (default, preferred for bulk)
      - ticker: non-VIP by ticker list
    """
    if mode == "period":
        if not periods:
            raise ValueError("periods is required when mode='period'")
        frames = [
            fetch_financial_by_period(
                table,
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
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if mode == "ticker":
        if not tickers:
            raise ValueError("tickers is required when mode='ticker'")
        return fetch_financial_by_tickers(
            table,
            tickers,
            periods=periods,
            start_date=start_date,
            end_date=end_date,
            client=client,
            execution_control=execution_control,
            report=report,
            settings=settings,
        )
    raise ValueError(f"unsupported mode: {mode}")
