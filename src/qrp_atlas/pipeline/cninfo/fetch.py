"""
fetch.py - 东方财富机构调研数据抓取模块
"""

import json
import time
import urllib.parse
import urllib.request
from typing import Any

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


def fetch_from_eastmoney(date_str: str) -> list[dict]:
    """
    分页抓取指定日期的东财机构调研数据。

    Args:
        date_str: 日期字符串，格式 "2026-05-28"

    Returns:
        所有页面的原始记录列表
    """
    page = 1
    all_records: list[dict] = []
    max_retries = 3

    while True:
        success = False
        last_error: Exception | None = None

        for attempt in range(max_retries):
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
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data: dict[str, Any] = json.loads(resp.read().decode("utf-8"))

                if data.get("success") is True:
                    result = data.get("result", {})
                    records = result.get("data", []) or []
                    all_records.extend(records)
                    success = True
                    break
                else:
                    err_msg = data.get("message", "unknown error")
                    raise RuntimeError(f"API error: {err_msg}")

            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt < max_retries - 1:
                    sleep_time = (3 ** attempt) * 1.0  # 1s, 3s, 9s
                    time.sleep(sleep_time)

        if not success:
            raise RuntimeError(
                f"Failed after {max_retries} retries: {last_error}"
            )

        count = len(records)
        print(
            f"Fetched page {page}, {count} records",
            file=__import__("sys").stderr,
        )

        if count < EASTMONEY_PAGE_SIZE:
            break

        page += 1
        time.sleep(eastmoney_sleep_interval())

    return all_records
