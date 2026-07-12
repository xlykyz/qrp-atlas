"""
fetch.py - 全景网互动问答数据抓取模块

接口限制（见 docs/全景网互动问答接口调研报告.md）：
- 活跃深度有限，默认全量抓取当前可见数据
- rows 硬截断为 10，必须分页
- page 越界会循环返回第一页数据，需用 pid 去重判断结束
- total 字段固定无效
- since_date 可选，一般不用
"""

from __future__ import annotations

import json
import sys
import time
import urllib.parse
import urllib.request
from typing import Any

from .config import (
    P5W_HEADERS,
    P5W_MAX_PAGES,
    P5W_PAGE_SIZE,
    P5W_URL,
    p5w_sleep_interval,
)


def _post_page(
    page: int,
    *,
    company_code: str = "",
    keywords: str = "",
    timeout: float = 15.0,
) -> dict[str, Any]:
    """请求单页互动问答数据。"""
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
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_interaction_qa(
    *,
    since_date: str | None = None,
    company_code: str = "",
    keywords: str = "",
    max_pages: int = P5W_MAX_PAGES,
) -> list[dict]:
    """
    分页抓取全景网最新互动问答回复。

    Args:
        since_date: 可选回复日期下限（含），格式 YYYY-MM-DD；
            默认 None，表示全量抓取直到页重叠或到顶。
        company_code: 6 位证券代码，空串表示全市场。
        keywords: 关键词过滤。
        max_pages: 最大翻页数，防止服务端越界循环。

    Returns:
        原始 API 记录列表（保留 camelCase 字段）。
    """
    page = 1
    all_records: list[dict] = []
    seen_pids: set[str] = set()
    max_retries = 3
    consecutive_failures = 0
    max_consecutive_failures = 5

    while page <= max_pages:
        success = False
        rows: list[dict] = []
        last_error: Exception | None = None

        for attempt in range(max_retries):
            try:
                data = _post_page(
                    page,
                    company_code=company_code,
                    keywords=keywords,
                )
                if data.get("success") is not True:
                    err_msg = data.get("message", "unknown error")
                    raise RuntimeError(f"API error: {err_msg}")
                rows = data.get("rows", []) or []
                success = True
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt < max_retries - 1:
                    time.sleep((3 ** attempt) * 1.0)

        if not success:
            consecutive_failures += 1
            print(
                f"[WARN] Page {page} failed after {max_retries} retries: "
                f"{last_error}. Skipping "
                f"(consecutive failures: {consecutive_failures}).",
                file=sys.stderr,
            )
            if consecutive_failures >= max_consecutive_failures:
                print(
                    f"[WARN] {max_consecutive_failures} consecutive failures, "
                    "stopping fetch.",
                    file=sys.stderr,
                )
                break
            page += 1
            time.sleep(p5w_sleep_interval())
            continue

        consecutive_failures = 0

        if not rows:
            print(f"Fetched page {page}, 0 records – stop", file=sys.stderr)
            break

        page_pids = [str(item.get("pid", "")).strip() for item in rows]
        page_pids = [pid for pid in page_pids if pid]
        overlap = [pid for pid in page_pids if pid in seen_pids]
        # 服务端越界会循环回第一页：本页全部已见则停止
        if page_pids and len(overlap) == len(page_pids):
            print(
                f"Fetched page {page}, full overlap with previous pages – stop",
                file=sys.stderr,
            )
            break

        stop_by_date = False
        new_count = 0
        for item in rows:
            pid = str(item.get("pid", "")).strip()
            if not pid or pid in seen_pids:
                continue

            reply_time = str(item.get("replyerTimeStr", "") or "").strip()
            if since_date and reply_time and reply_time[:10] < since_date:
                stop_by_date = True
                continue

            seen_pids.add(pid)
            all_records.append(item)
            new_count += 1

        print(
            f"Fetched page {page}, {len(rows)} records "
            f"({new_count} new, total {len(all_records)})",
            file=sys.stderr,
        )

        if stop_by_date:
            print(
                f"Reached records earlier than {since_date}, stop paging",
                file=sys.stderr,
            )
            break

        if len(rows) < P5W_PAGE_SIZE:
            break

        page += 1
        time.sleep(p5w_sleep_interval())

    return all_records
