"""Eastmoney stock research-report detail fetching.

The strict report-returning function is used by formal Pipeline contracts;
the legacy list-returning wrapper remains available for existing callers.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import re
import threading
import urllib.request
from collections.abc import Mapping, Sequence
from typing import Any

from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError

from .config import (
    DETAIL_HEADERS,
    DETAIL_URL_TEMPLATE,
    sleep_interval,
)

logger = logging.getLogger(__name__)

DETAIL_TIMEOUT_SECONDS = 30.0
MAX_RETRIES = 2


@dataclass(frozen=True, slots=True)
class ResearchReportDetailReport:
    """Auditable outcome of detail-page enrichment."""

    records: tuple[dict[str, Any], ...]
    requests: int
    retries: int
    failed_indices: tuple[int, ...]
    complete: bool


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


def _parse_detail(html: str, encode_url: str) -> tuple[str, str]:
    match = re.search(r"var zwinfo = ({.*?});", html, re.DOTALL)
    if not match:
        raise ValueError(f"detail page has no zwinfo for {encode_url}")
    try:
        zwinfo = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise ValueError(f"detail page zwinfo is invalid for {encode_url}") from exc
    if not isinstance(zwinfo, Mapping):
        raise ValueError(f"detail page zwinfo is not an object for {encode_url}")
    return str(zwinfo.get("notice_content") or ""), str(zwinfo.get("attach_url") or "")


def fetch_report_detail_with_report(
    records: Sequence[Mapping[str, Any]],
    *,
    execution_control: ExecutionControl | None = None,
) -> ResearchReportDetailReport:
    """Enrich every record and fail closed when any detail page is incomplete."""

    enriched: list[dict[str, Any]] = []
    failed_indices: list[int] = []
    requests = 0
    retries = 0

    for index, source_record in enumerate(records):
        _check(execution_control)
        if not isinstance(source_record, Mapping):
            failed_indices.append(index)
            continue
        record = dict(source_record)
        encode_url = str(record.get("encodeUrl") or "").strip()
        if not encode_url:
            failed_indices.append(index)
            continue
        last_error: Exception | None = None
        enriched_record: tuple[str, str] | None = None
        for retry_index in range(MAX_RETRIES + 1):
            _check(execution_control)
            try:
                url = DETAIL_URL_TEMPLATE.format(encode_url=encode_url)
                req = urllib.request.Request(url, headers=DETAIL_HEADERS, method="GET")
                timeout = (
                    execution_control.bounded_timeout(DETAIL_TIMEOUT_SECONDS)
                    if execution_control is not None
                    else DETAIL_TIMEOUT_SECONDS
                )
                if timeout is not None and timeout <= 0:
                    _check(execution_control)
                    raise TimeoutError("research report detail deadline elapsed")
                requests += 1
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    status = getattr(response, "status", None)
                    if status is not None and status != 200:
                        raise RuntimeError(f"Eastmoney detail returned HTTP {status}")
                    html = response.read().decode("utf-8", errors="replace")
                _check(execution_control)
                enriched_record = _parse_detail(html, encode_url)
                break
            except ExecutionControlError:
                raise
            except Exception as exc:  # noqa: BLE001 - recorded as incomplete detail.
                last_error = exc
                if retry_index < MAX_RETRIES:
                    retries += 1
                    _wait(execution_control, float(3**retry_index))
        if enriched_record is None:
            logger.warning(
                "Failed to fetch detail for record %d: %s",
                index,
                type(last_error).__name__ if last_error is not None else "unknown",
            )
            failed_indices.append(index)
            continue
        notice_content, attach_url = enriched_record
        record["noticeContent"] = notice_content
        record["attachUrl"] = attach_url
        enriched.append(record)
        if index + 1 < len(records):
            _wait(execution_control, sleep_interval())

    return ResearchReportDetailReport(
        records=tuple(enriched),
        requests=requests,
        retries=retries,
        failed_indices=tuple(failed_indices),
        complete=not failed_indices,
    )


def fetch_report_detail(records: list[dict]) -> list[dict]:
    """
    为每条记录抓取详情页，提取 noticeContent 和 attachUrl。

    Args:
        records: 来自 fetch_report_list() 的原始记录列表

    Returns:
        相同列表，每条记录补充了 noticeContent 和 attachUrl 字段
    """
    report = fetch_report_detail_with_report(records)
    failed_indices = set(report.failed_indices)
    enriched_records = iter(report.records)
    result: list[dict] = []
    for index, record in enumerate(records):
        result.append(dict(record) if index in failed_indices else next(enriched_records))
    records[:] = result
    return records


__all__ = ["ResearchReportDetailReport", "fetch_report_detail", "fetch_report_detail_with_report"]
