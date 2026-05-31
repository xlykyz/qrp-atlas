"""fetch_detail.py - 研报详情页抓取模块"""

import json
import logging
import re
import time
import urllib.request

from .config import (
    DETAIL_HEADERS,
    DETAIL_URL_TEMPLATE,
    sleep_interval,
)

logger = logging.getLogger(__name__)


def fetch_report_detail(records: list[dict]) -> list[dict]:
    """
    为每条记录抓取详情页，提取 noticeContent 和 attachUrl。

    Args:
        records: 来自 fetch_report_list() 的原始记录列表

    Returns:
        相同列表，每条记录补充了 noticeContent 和 attachUrl 字段
    """
    total = len(records)
    for idx, record in enumerate(records):
        encode_url = record.get("encodeUrl")
        if not encode_url:
            logger.warning("Record at index %d has no encodeUrl – skipping", idx)
            continue

        url = DETAIL_URL_TEMPLATE.format(encode_url=encode_url)

        try:
            req = urllib.request.Request(
                url,
                headers=DETAIL_HEADERS,
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status != 200:
                    logger.warning(
                        "Detail page for %s returned HTTP %s – skipping",
                        encode_url,
                        resp.status,
                    )
                    continue
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            logger.warning("Failed to fetch detail page for %s: %s – skipping", encode_url, exc)
            continue

        # 提取 var zwinfo = {...};
        match = re.search(r"var zwinfo = ({.*?});", html, re.DOTALL)
        if not match:
            logger.warning("Could not find zwinfo in detail page for %s – skipping", encode_url)
            continue

        try:
            zwinfo = json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            logger.warning("Failed to parse zwinfo JSON for %s: %s – skipping", encode_url, exc)
            continue

        record["noticeContent"] = zwinfo.get("notice_content") or ""
        record["attachUrl"] = zwinfo.get("attach_url") or ""

        # 进度打印，每 10 条输出一次
        if (idx + 1) % 10 == 0:
            logger.info("[fetch_detail] Processed %d/%d records...", idx + 1, total)

        # 速率限制：首次请求不延迟
        if idx > 0:
            time.sleep(sleep_interval())

    return records
