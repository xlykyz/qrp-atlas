"""Eastmoney industry research-report list fetching.

The legacy ``fetch_report_list`` function keeps its list-returning API. Formal
Pipeline contracts use ``fetch_report_list_with_report`` so a failed page is
never mistaken for a complete target range.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import threading
import urllib.parse
import urllib.request
from collections.abc import Mapping
from typing import Any

from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError

from .config import (
    REPORT_API_URL,
    REPORT_HEADERS,
    REPORT_PAGE_SIZE,
    REPORT_QTYPE,
    sleep_interval,
)

logger = logging.getLogger(__name__)

REPORT_TIMEOUT_SECONDS = 30.0
MAX_RETRIES = 2
MAX_PAGES = 10_000


@dataclass(frozen=True, slots=True)
class IndustryReportFetchReport:
    """Auditable result for one inclusive Eastmoney date-range scan."""

    records: tuple[dict[str, Any], ...]
    pages_fetched: int
    api_requests: int
    retries: int
    failed_pages: tuple[int, ...]
    complete: bool
    stop_reason: str
    reported_total: int | None = None


def _check(execution_control: ExecutionControl | None) -> None:
    if execution_control is not None:
        execution_control.check()


def _wait(execution_control: ExecutionControl | None, seconds: float) -> None:
    if seconds <= 0:
        return
    if execution_control is None:
        import time

        time.sleep(seconds)
        return
    execution_control.wait(threading.Event(), seconds)


def _request_page(
    begin_date: str,
    end_date: str,
    page: int,
    *,
    execution_control: ExecutionControl | None,
) -> Mapping[str, Any]:
    params = {
        "qType": REPORT_QTYPE,
        "beginTime": begin_date,
        "endTime": end_date,
        "industryCode": "*",
        "rating": "*",
        "ratingChange": "*",
        "pageSize": REPORT_PAGE_SIZE,
        "pageNo": page,
    }
    query_string = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{REPORT_API_URL}?{query_string}",
        headers=REPORT_HEADERS,
        method="GET",
    )
    timeout = (
        execution_control.bounded_timeout(REPORT_TIMEOUT_SECONDS)
        if execution_control is not None
        else REPORT_TIMEOUT_SECONDS
    )
    if timeout is not None and timeout <= 0:
        _check(execution_control)
        raise TimeoutError("industry research report request deadline elapsed")
    _check(execution_control)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        status = getattr(response, "status", None)
        if status is not None and status != 200:
            raise RuntimeError(f"Eastmoney industry report returned HTTP {status}")
        payload = json.loads(response.read().decode("utf-8"))
    _check(execution_control)
    if not isinstance(payload, Mapping):
        raise ValueError("industry research report response must be an object")
    return payload


def _reported_total(payload: Mapping[str, Any]) -> int | None:
    value = payload.get("total")
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("industry research report total must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("industry research report total must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError("industry research report total must be a non-negative integer")
    return parsed


def fetch_report_list_with_report(
    begin_date: str,
    end_date: str,
    *,
    execution_control: ExecutionControl | None = None,
) -> IndustryReportFetchReport:
    """Fetch all pages and report whether the result is complete."""

    records: list[dict[str, Any]] = []
    failed_pages: list[int] = []
    pages_fetched = 0
    api_requests = 0
    retries = 0
    reported_total: int | None = None
    stop_reason = "max_pages_reached"
    complete = False

    for page in range(1, MAX_PAGES + 1):
        _check(execution_control)
        page_records: list[dict[str, Any]] | None = None
        last_error: Exception | None = None
        for retry_index in range(MAX_RETRIES + 1):
            _check(execution_control)
            api_requests += 1
            try:
                payload = _request_page(
                    begin_date,
                    end_date,
                    page,
                    execution_control=execution_control,
                )
                if "data" not in payload:
                    raise ValueError("industry research report response is missing data")
                raw_records = payload["data"]
                if not isinstance(raw_records, list) or any(
                    not isinstance(record, Mapping) for record in raw_records
                ):
                    raise ValueError("industry research report data must be a list of objects")
                page_records = [dict(record) for record in raw_records]
                page_total = _reported_total(payload)
                if reported_total is not None and page_total is not None and page_total != reported_total:
                    raise ValueError("industry research report total changed between pages")
                if page_total is not None:
                    reported_total = page_total
                break
            except ExecutionControlError:
                raise
            except Exception as exc:  # noqa: BLE001 - provider errors become incomplete.
                last_error = exc
                if retry_index < MAX_RETRIES:
                    retries += 1
                    _wait(execution_control, float(3**retry_index))
        if page_records is None:
            failed_pages.append(page)
            stop_reason = f"page_{page}_failed:{type(last_error).__name__}"
            break

        pages_fetched += 1
        records.extend(page_records)
        if reported_total is not None and len(records) > reported_total:
            stop_reason = "reported_total_mismatch"
            break
        if len(page_records) < REPORT_PAGE_SIZE:
            if reported_total is None or len(records) == reported_total:
                complete = True
                stop_reason = "short_page"
            else:
                stop_reason = "reported_total_mismatch"
            break
        if reported_total is not None and len(records) == reported_total:
            complete = True
            stop_reason = "reported_total"
            break
        _wait(execution_control, sleep_interval())

    if reported_total is not None and len(records) != reported_total:
        complete = False
        stop_reason = "reported_total_mismatch"
    if not complete and not failed_pages and pages_fetched >= MAX_PAGES:
        stop_reason = "max_pages_reached"

    return IndustryReportFetchReport(
        records=tuple(records),
        pages_fetched=pages_fetched,
        api_requests=api_requests,
        retries=retries,
        failed_pages=tuple(failed_pages),
        complete=complete,
        stop_reason=stop_reason,
        reported_total=reported_total,
    )


def fetch_report_list(begin_date: str, end_date: str) -> list[dict]:
    """
    从东方财富行业研报列表 API 抓取指定日期区间的所有记录。

    Args:
        begin_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)

    Returns:
        包含所有记录的扁平列表
    """
    return list(fetch_report_list_with_report(begin_date, end_date).records)


__all__ = ["IndustryReportFetchReport", "fetch_report_list", "fetch_report_list_with_report"]
