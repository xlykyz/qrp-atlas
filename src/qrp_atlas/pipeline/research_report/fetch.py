"""fetch.py - 研报列表 API 抓取模块"""

import json
import logging
import time
import urllib.request
from typing import Any

from .config import (
    REPORT_API_URL,
    REPORT_HEADERS,
    REPORT_PAGE_SIZE,
    sleep_interval,
)

logger = logging.getLogger(__name__)


def fetch_report_list(begin_date: str, end_date: str) -> list[dict]:
    """
    从东方财富研报列表 API 抓取指定日期区间的所有记录。

    Args:
        begin_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)

    Returns:
        包含所有记录的扁平列表
    """
    all_records: list[dict] = []
    page = 1

    while True:
        body: dict[str, Any] = {
            "beginTime": begin_date,
            "endTime": end_date,
            "industryCode": "*",
            "ratingChange": "*",
            "rating": "*",
            "orgCode": "*",
            "code": "*",
            "rcode": "",
            "pageSize": REPORT_PAGE_SIZE,
            "pageNo": page,
        }

        try:
            req = urllib.request.Request(
                REPORT_API_URL,
                data=json.dumps(body).encode("utf-8"),
                headers=REPORT_HEADERS,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status != 200:
                    logger.warning(
                        "Page %d returned HTTP %s, skipping", page, resp.status
                    )
                    break
                payload = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            logger.warning("Page %d failed: %s, continuing", page, exc)
            break

        data: list[dict] = payload.get("data", [])
        if not data:
            logger.info("Page %d empty – stopping", page)
            break

        all_records.extend(data)
        logger.info("Page %d: fetched %d records (total: %d)", page, len(data), len(all_records))

        if len(data) < REPORT_PAGE_SIZE:
            break

        page += 1
        time.sleep(sleep_interval())

    return all_records
