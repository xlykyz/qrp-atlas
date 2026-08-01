"""Eastmoney CNINFO research-visit pagination.

The legacy ``fetch_from_eastmoney`` entry point keeps its list-returning
interface. Formal Pipeline contracts use ``fetch_from_eastmoney_report`` so
they can reject incomplete pagination before opening a database write.
"""

from dataclasses import dataclass
import json
import threading
import time
import urllib.parse
import urllib.request
from typing import Any

from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError

from .config import (
    EASTMONEY_CLIENT,
    EASTMONEY_HEADERS,
    EASTMONEY_PAGE_SIZE,
    EASTMONEY_REPORT,
    EASTMONEY_SORT_COLUMNS,
    EASTMONEY_SOURCE,
    EASTMONEY_URL,
    build_eastmoney_filter,
    eastmoney_sleep_interval,
)


EASTMONEY_TIMEOUT_SECONDS = 15.0
MAX_RETRIES = 3
MAX_CONSECUTIVE_FAILURES = 5
MAX_PAGES = 10_000


@dataclass(frozen=True, slots=True)
class EastmoneyFetchReport:
    """Auditable outcome of one date-bound paginated provider request."""

    date_str: str
    records: tuple[dict[str, Any], ...]
    pages_fetched: int
    requests: int
    retries: int
    failed_pages: tuple[int, ...]
    complete: bool
    last_error: str | None = None


def _controlled_wait(seconds: float, execution_control: ExecutionControl | None) -> None:
    if seconds <= 0:
        return
    if execution_control is None:
        time.sleep(seconds)
        return
    execution_control.wait(threading.Event(), timeout=seconds)


def fetch_from_eastmoney_report(
    date_str: str,
    *,
    execution_control: ExecutionControl | None = None,
) -> EastmoneyFetchReport:
    """Fetch one notice date and report whether pagination was complete.

    A failed page remains visible in ``failed_pages`` even when a later page
    succeeds. This lets the formal executor reject a partial provider snapshot
    instead of treating the returned subset as a complete day.
    """

    page = 1
    all_records: list[dict[str, Any]] = []
    pages_fetched = 0
    requests = 0
    retries = 0
    failed_pages: list[int] = []
    consecutive_failures = 0
    last_error: str | None = None
    stopped_after_short_page = False

    while page <= MAX_PAGES:
        if execution_control is not None:
            execution_control.check()
        success = False
        page_records: list[dict[str, Any]] = []

        for attempt in range(MAX_RETRIES):
            if execution_control is not None:
                execution_control.check()
            try:
                params = {
                    "reportName": EASTMONEY_REPORT,
                    "columns": (
                        "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,"
                        "NOTICE_DATE,RECEIVE_START_DATE,RECEIVE_OBJECT,"
                        "RECEIVE_PLACE,RECEIVE_WAY_EXPLAIN,RECEPTIONIST,"
                        "ORG_TYPE,CONTENT,URL"
                    ),
                    "pageNumber": page,
                    "pageSize": EASTMONEY_PAGE_SIZE,
                    "sortTypes": -1,
                    "sortColumns": EASTMONEY_SORT_COLUMNS,
                    "source": EASTMONEY_SOURCE,
                    "client": EASTMONEY_CLIENT,
                    "filter": build_eastmoney_filter(date_str),
                }
                url = f"{EASTMONEY_URL}?{urllib.parse.urlencode(params)}"
                req = urllib.request.Request(url, headers=EASTMONEY_HEADERS)
                timeout = (
                    execution_control.bounded_timeout(EASTMONEY_TIMEOUT_SECONDS)
                    if execution_control is not None
                    else EASTMONEY_TIMEOUT_SECONDS
                )
                if execution_control is not None:
                    execution_control.check()
                requests += 1
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data: dict[str, Any] = json.loads(resp.read().decode("utf-8"))
                if execution_control is not None:
                    execution_control.check()

                if data.get("success") is not True:
                    raise RuntimeError(f"API error: {data.get('message', 'unknown error')}")
                result = data.get("result")
                if not isinstance(result, dict):
                    raise RuntimeError("API response missing result object")
                raw_records = result.get("data", []) or []
                if not isinstance(raw_records, list) or any(
                    not isinstance(record, dict) for record in raw_records
                ):
                    raise RuntimeError("API response data must be a list of objects")
                page_records = raw_records
                success = True
                break
            except ExecutionControlError:
                raise
            except Exception as exc:  # noqa: BLE001 - provider failures are reported below.
                last_error = type(exc).__name__
                if attempt < MAX_RETRIES - 1:
                    retries += 1
                    _controlled_wait(float(3**attempt), execution_control)

        if not success:
            failed_pages.append(page)
            consecutive_failures += 1
            print(
                f"[WARN] Page {page} failed after {MAX_RETRIES} retries; "
                f"skipping (consecutive failures: {consecutive_failures})",
                file=__import__("sys").stderr,
            )
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(
                    f"[WARN] {MAX_CONSECUTIVE_FAILURES} consecutive failures; "
                    f"stopping fetch for {date_str}",
                    file=__import__("sys").stderr,
                )
                break
            page += 1
            _controlled_wait(eastmoney_sleep_interval(), execution_control)
            continue

        pages_fetched += 1
        all_records.extend(page_records)
        consecutive_failures = 0
        print(
            f"Fetched page {page}, {len(page_records)} records",
            file=__import__("sys").stderr,
        )
        if len(page_records) < EASTMONEY_PAGE_SIZE:
            stopped_after_short_page = True
            break
        page += 1
        _controlled_wait(eastmoney_sleep_interval(), execution_control)

    return EastmoneyFetchReport(
        date_str=date_str,
        records=tuple(all_records),
        pages_fetched=pages_fetched,
        requests=requests,
        retries=retries,
        failed_pages=tuple(failed_pages),
        complete=stopped_after_short_page and not failed_pages,
        last_error=last_error,
    )


def fetch_from_eastmoney(date_str: str) -> list[dict]:
    """
    分页抓取指定日期的东财机构调研数据。

    单页失败时跳过该页（非致命），确保海量数据天不因限流丢失整批。

    Args:
        date_str: 日期字符串，格式 "2026-05-28"

    Returns:
        所有成功页面的原始记录列表
    """
    return list(fetch_from_eastmoney_report(date_str).records)
