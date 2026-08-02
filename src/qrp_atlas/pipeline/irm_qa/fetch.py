"""Strict P5W latest-reply feed fetching with pagination evidence."""

from __future__ import annotations

import json
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError
from qrp_atlas.pipeline.contracts import ContractError

from .config import (
    P5W_HEADERS,
    P5W_MAX_PAGES,
    P5W_PAGE_SIZE,
    P5W_PROVIDER_MAX_RETRIES,
    P5W_REQUEST_TIMEOUT,
    P5W_RETRY_BACKOFF_BASE_SECONDS,
    P5W_URL,
    p5w_sleep_interval,
)


P5W_REQUIRED_PROVIDER_FIELDS: tuple[str, ...] = (
    "companyShortname",
    "companyCode",
    "nickname",
    "content",
    "replyContent",
    "replyerTimeStr",
    "questionerTimeStr",
    "pid",
)
P5W_REQUIRED_NON_EMPTY_FIELDS: tuple[str, ...] = (
    "companyCode",
    "replyerTimeStr",
    "pid",
)


@dataclass(slots=True)
class InteractionQAFetchReport:
    """Observable provider work for one latest-feed scan."""

    api_requests: int = 0
    pages_fetched: int = 0
    rows_read: int = 0
    unique_rows: int = 0
    retries: int = 0
    stop_reason: str | None = None


def _check(execution_control: ExecutionControl | None) -> None:
    if execution_control is not None:
        execution_control.check()


def _wait(
    execution_control: ExecutionControl | None,
    seconds: float,
) -> None:
    if execution_control is None:
        time.sleep(seconds)
        return
    execution_control.wait(execution_control.cancel_event, seconds)


def _post_page(
    page: int,
    *,
    company_code: str = "",
    keywords: str = "",
    timeout: float = P5W_REQUEST_TIMEOUT,
    execution_control: ExecutionControl | None = None,
) -> dict[str, Any]:
    """Request one P5W page while honoring the invocation deadline."""

    _check(execution_control)
    payload = {
        "page": str(page),
        "rows": str(P5W_PAGE_SIZE),
        "isPagination": "1",
        "keyWords": keywords or "",
        "companyCode": company_code or "",
        "companyBaseinfoId": "",
    }
    body = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(
        P5W_URL,
        data=body,
        headers=P5W_HEADERS,
        method="POST",
    )
    bounded_timeout = (
        execution_control.bounded_timeout(timeout)
        if execution_control is not None
        else timeout
    )
    if bounded_timeout is not None and bounded_timeout <= 0:
        _check(execution_control)
        raise TimeoutError("P5W request deadline elapsed")
    with urllib.request.urlopen(req, timeout=bounded_timeout) as resp:
        status = getattr(resp, "status", None)
        if status is not None and status != 200:
            raise RuntimeError(f"P5W returned HTTP {status}")
        body_bytes = resp.read()
    _check(execution_control)
    return json.loads(body_bytes.decode("utf-8"))


def _validate_page_response(data: object, *, page: int) -> list[dict[str, Any]]:
    if not isinstance(data, Mapping):
        raise ContractError("IRM_PROVIDER_RESPONSE_INVALID", f"page {page} is not an object")
    if data.get("success") is not True:
        message = str(data.get("message") or "provider returned success=false")
        raise ContractError("IRM_PROVIDER_RESPONSE_FAILED", f"page {page}: {message}")
    rows = data.get("rows")
    if not isinstance(rows, list):
        raise ContractError("IRM_PROVIDER_RESPONSE_INVALID", f"page {page} rows is not a list")

    validated: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ContractError(
                "IRM_PROVIDER_SCHEMA_MISSING",
                f"page {page} row {index} is not an object",
            )
        missing = [field for field in P5W_REQUIRED_PROVIDER_FIELDS if field not in row]
        if missing:
            raise ContractError(
                "IRM_PROVIDER_SCHEMA_MISSING",
                f"page {page} row {index} missing {','.join(missing)}",
            )
        for field in P5W_REQUIRED_NON_EMPTY_FIELDS:
            if not str(row.get(field) or "").strip():
                raise ContractError(
                    "IRM_PROVIDER_SCHEMA_MISSING",
                    f"page {page} row {index} has empty {field}",
                )
        validated.append(dict(row))
    return validated


def _fetch_page_with_retries(
    page: int,
    *,
    company_code: str,
    keywords: str,
    timeout: float,
    max_retries: int,
    execution_control: ExecutionControl | None,
    report: InteractionQAFetchReport,
) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for retry_index in range(max_retries + 1):
        _check(execution_control)
        report.api_requests += 1
        try:
            data = _post_page(
                page,
                company_code=company_code,
                keywords=keywords,
                timeout=timeout,
                execution_control=execution_control,
            )
            rows = _validate_page_response(data, page=page)
            report.pages_fetched += 1
            return rows
        except ExecutionControlError:
            raise
        except ContractError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if retry_index >= max_retries:
                break
            report.retries += 1
            _wait(
                execution_control,
                P5W_RETRY_BACKOFF_BASE_SECONDS * (3**retry_index),
            )

    detail = type(last_error).__name__ if last_error is not None else "unknown error"
    raise ContractError("IRM_PROVIDER_REQUEST_FAILED", f"page {page}: {detail}") from last_error


def fetch_interaction_qa_with_report(
    *,
    since_date: str | None = None,
    company_code: str = "",
    keywords: str = "",
    max_pages: int = P5W_MAX_PAGES,
    max_retries: int = P5W_PROVIDER_MAX_RETRIES,
    timeout: float = P5W_REQUEST_TIMEOUT,
    execution_control: ExecutionControl | None = None,
) -> tuple[list[dict[str, Any]], InteractionQAFetchReport]:
    """Scan the provider's latest feed and return records plus work metrics.

    The provider's ``total`` field is intentionally ignored because the local
    investigation found it is not a reliable total. A full repeated page is a
    known provider wrap-around terminator. A partial overlap, malformed page,
    failed page, or exhausted page limit is not sufficient evidence of a
    complete scan and fails closed.
    """

    if max_pages <= 0 or max_retries < 0 or timeout <= 0:
        raise ContractError("IRM_PROVIDER_CONFIGURATION_INVALID")

    page = 1
    all_records: list[dict[str, Any]] = []
    seen_pids: set[str] = set()
    report = InteractionQAFetchReport()

    while page <= max_pages:
        _check(execution_control)
        rows = _fetch_page_with_retries(
            page,
            company_code=company_code,
            keywords=keywords,
            timeout=timeout,
            max_retries=max_retries,
            execution_control=execution_control,
            report=report,
        )
        _check(execution_control)
        report.rows_read += len(rows)

        if not rows:
            report.stop_reason = "empty_page"
            break

        page_pids = [str(row["pid"]).strip() for row in rows]
        if len(page_pids) != len(set(page_pids)):
            raise ContractError("IRM_PROVIDER_DUPLICATE_PAGE", f"page {page} contains duplicate pid values")
        overlap = set(page_pids) & seen_pids
        if overlap:
            if len(overlap) == len(page_pids):
                report.stop_reason = "full_page_overlap"
                break
            raise ContractError(
                "IRM_PROVIDER_PARTIAL_PAGE_OVERLAP",
                f"page {page} overlaps {len(overlap)} prior pid values",
            )

        stop_by_date = False
        for row in rows:
            reply_time = str(row.get("replyerTimeStr") or "").strip()
            if since_date and reply_time and reply_time[:10] < since_date:
                stop_by_date = True
                continue
            seen_pids.add(str(row["pid"]).strip())
            all_records.append(row)

        if stop_by_date:
            report.stop_reason = "since_date"
            break
        if len(rows) < P5W_PAGE_SIZE:
            report.stop_reason = "short_page"
            break
        if page == max_pages:
            raise ContractError(
                "IRM_PROVIDER_PAGE_LIMIT",
                f"page limit {max_pages} reached without a complete-page boundary",
            )

        page += 1
        _wait(execution_control, p5w_sleep_interval())

    if report.stop_reason is None:
        raise ContractError("IRM_PROVIDER_PAGE_LIMIT")
    report.unique_rows = len(all_records)
    print(
        f"Fetched {report.pages_fetched} pages, {report.rows_read} rows "
        f"({report.unique_rows} unique), {report.api_requests} requests",
        file=sys.stderr,
    )
    return all_records, report


def fetch_interaction_qa(
    *,
    since_date: str | None = None,
    company_code: str = "",
    keywords: str = "",
    max_pages: int = P5W_MAX_PAGES,
) -> list[dict[str, Any]]:
    """Backward-compatible list-only wrapper for the legacy CLI/tests."""

    records, _report = fetch_interaction_qa_with_report(
        since_date=since_date,
        company_code=company_code,
        keywords=keywords,
        max_pages=max_pages,
    )
    return records
