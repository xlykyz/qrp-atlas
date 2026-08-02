"""Fetch Shenwan industry membership history from Tushare index_member_all."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Sequence

import pandas as pd

from qrp_atlas.config import get_tushare_pro
from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError


@dataclass(slots=True)
class IndustryMembershipFetchReport:
    """Observable provider work for one explicit membership scope."""

    api_requests: int = 0
    batches: int = 0
    rows_read: int = 0
    retries: int = 0
    completeness_boundary: str = (
        "index_member_all returns one response per explicit scope; the endpoint exposes "
        "no total or pagination evidence, so global-universe completeness is not claimed"
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
    report: IndustryMembershipFetchReport | None = None,
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


def fetch_industry_membership(
    *,
    tickers: Sequence[str] | None = None,
    l1_code: str | None = None,
    l2_code: str | None = None,
    l3_code: str | None = None,
    is_new: str | None = None,
    client=None,
) -> pd.DataFrame:
    """Fetch membership rows.

    Prefer ticker list for small verification. Industry code filters support later backfill.
    """
    pro = client or get_tushare_pro()
    method = getattr(pro, "index_member_all")
    frames: list[pd.DataFrame] = []

    if tickers:
        for ts_code in tickers:
            kwargs = {"ts_code": ts_code}
            if is_new is not None:
                kwargs["is_new"] = is_new
            df = _call_with_retry(method, **kwargs)
            if df is not None and not df.empty:
                frames.append(df)
    else:
        kwargs = {}
        if l1_code:
            kwargs["l1_code"] = l1_code
        if l2_code:
            kwargs["l2_code"] = l2_code
        if l3_code:
            kwargs["l3_code"] = l3_code
        if is_new is not None:
            kwargs["is_new"] = is_new
        if not kwargs:
            raise ValueError("provide tickers or an industry code filter; refuse full-universe pull")
        df = _call_with_retry(method, **kwargs)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_industry_membership_with_report(
    *,
    tickers: Sequence[str] | None = None,
    l1_code: str | None = None,
    l2_code: str | None = None,
    l3_code: str | None = None,
    is_new: str | None = None,
    client=None,
    execution_control: ExecutionControl | None = None,
) -> tuple[pd.DataFrame, IndustryMembershipFetchReport]:
    """Fetch an explicit scope while reporting requests and controlled retries.

    This endpoint has no page or total fields. Every requested ticker is one
    atomic scope unit; code-filter mode is one provider request. The caller
    must therefore treat a successful response as scoped evidence only, not a
    proof of an unfiltered full-universe pull.
    """

    _check(execution_control)
    pro = client or get_tushare_pro()
    method = getattr(pro, "index_member_all")
    report = IndustryMembershipFetchReport()
    frames: list[pd.DataFrame] = []

    if tickers:
        for ts_code in tickers:
            _check(execution_control)
            kwargs = {"ts_code": ts_code}
            if is_new is not None:
                kwargs["is_new"] = is_new
            df = _call_with_retry(
                method,
                execution_control=execution_control,
                report=report,
                **kwargs,
            )
            report.batches += 1
            report.rows_read += 0 if df is None else len(df)
            if df is not None and not df.empty:
                frames.append(df)
            _check(execution_control)
    else:
        kwargs = {}
        if l1_code:
            kwargs["l1_code"] = l1_code
        if l2_code:
            kwargs["l2_code"] = l2_code
        if l3_code:
            kwargs["l3_code"] = l3_code
        if is_new is not None:
            kwargs["is_new"] = is_new
        if not kwargs:
            raise ValueError("provide tickers or an industry code filter; refuse full-universe pull")
        _check(execution_control)
        df = _call_with_retry(
            method,
            execution_control=execution_control,
            report=report,
            **kwargs,
        )
        report.batches = 1
        report.rows_read = 0 if df is None else len(df)
        if df is not None and not df.empty:
            frames.append(df)
        _check(execution_control)

    if not frames:
        return pd.DataFrame(), report
    return pd.concat(frames, ignore_index=True), report
