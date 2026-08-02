"""Fetch index component weights from Tushare index_weight."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Sequence

import pandas as pd

from qrp_atlas.config import get_tushare_pro
from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError


@dataclass(slots=True)
class IndexComponentFetchReport:
    """Observable provider work for one explicit index/date range."""

    api_requests: int = 0
    batches: int = 0
    rows_read: int = 0
    retries: int = 0
    completeness_boundary: str = (
        "index_weight returns one response per explicit index code and date range; "
        "the endpoint exposes no total or pagination evidence"
    )


def _check(execution_control: ExecutionControl | None) -> None:
    if execution_control is not None:
        execution_control.check()


def _wait(execution_control: ExecutionControl | None, seconds: float) -> None:
    if execution_control is None:
        time.sleep(seconds)
        return
    execution_control.wait(threading.Event(), timeout=seconds)


def _call_with_retry(
    func: Callable,
    *,
    retries: int = 5,
    base_sleep: float = 1.2,
    execution_control: ExecutionControl | None = None,
    report: IndexComponentFetchReport | None = None,
    **kwargs,
) -> pd.DataFrame:
    last_err: Exception | None = None
    for i in range(retries):
        _check(execution_control)
        if report is not None:
            report.api_requests += 1
        try:
            df = func(**kwargs)
            if df is None:
                return pd.DataFrame()
            return df
        except ExecutionControlError:
            raise
        except Exception as exc:
            last_err = exc
            if i < retries - 1 or (execution_control is None and report is None):
                if report is not None:
                    report.retries += 1
                _wait(execution_control, base_sleep * (i + 1))
    if last_err is not None:
        raise last_err
    return pd.DataFrame()


def _ymd(value: str) -> str:
    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"date must be YYYYMMDD, got {value!r}")
    return text


def fetch_index_weight(
    index_code: str,
    *,
    start_date: str,
    end_date: str,
    client=None,
) -> pd.DataFrame:
    pro = client or get_tushare_pro()
    method = getattr(pro, "index_weight")
    df = _call_with_retry(
        method,
        index_code=index_code,
        start_date=_ymd(start_date),
        end_date=_ymd(end_date),
    )
    if df is None or df.empty:
        return pd.DataFrame()
    return df.reset_index(drop=True)


def fetch_index_weights(
    index_codes: Sequence[str],
    *,
    start_date: str,
    end_date: str,
    client=None,
) -> pd.DataFrame:
    frames = [
        fetch_index_weight(code, start_date=start_date, end_date=end_date, client=client)
        for code in index_codes
    ]
    frames = [f for f in frames if f is not None and not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_index_weights_with_report(
    index_codes: Sequence[str],
    *,
    start_date: str,
    end_date: str,
    client=None,
    execution_control: ExecutionControl | None = None,
) -> tuple[pd.DataFrame, IndexComponentFetchReport]:
    """Fetch each explicit index/date scope with cooperative control checks."""

    _check(execution_control)
    pro = client or get_tushare_pro()
    method = getattr(pro, "index_weight")
    normalized_start = _ymd(start_date)
    normalized_end = _ymd(end_date)
    report = IndexComponentFetchReport()
    frames: list[pd.DataFrame] = []
    for index_code in index_codes:
        _check(execution_control)
        df = _call_with_retry(
            method,
            execution_control=execution_control,
            report=report,
            index_code=index_code,
            start_date=normalized_start,
            end_date=normalized_end,
        )
        report.batches += 1
        report.rows_read += 0 if df is None else len(df)
        if df is not None and not df.empty:
            frames.append(df.reset_index(drop=True))
        _check(execution_control)
    if not frames:
        return pd.DataFrame(), report
    return pd.concat(frames, ignore_index=True), report
