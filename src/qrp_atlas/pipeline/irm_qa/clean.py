"""
clean.py - 全景网互动问答数据清洗模块

将原始 API 记录映射为 contracts 标准字段。
"""

from __future__ import annotations

import re
import sys
from typing import Any

from qrp_atlas.contracts import (
    COMPANY_CODE,
    COMPANY_SHORTNAME,
    INTERACTION_PID,
    KEYWORDS,
    NICKNAME,
    QUESTION_CONTENT,
    QUESTION_TIME,
    REPLY_CONTENT,
    REPLY_DATE,
    REPLY_TIME,
    SOURCE,
    TICKER,
    get_mapping,
    normalize_ticker,
)

FIELD_MAP = get_mapping("p5w_interaction_qa")

# 屏蔽手机号等潜在个人信息（如 186****5202 / 13812345678）
_PHONE_PATTERN = re.compile(r"1\d{2}\*{0,4}\d{4}|\d{3}\*{2,4}\d{4}")


def _strip_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _sanitize_nickname(nickname: str) -> str:
    """统一匿名化提问者昵称，过滤手机号样式。"""
    name = _strip_text(nickname)
    if not name or _PHONE_PATTERN.search(name):
        return "投资者"
    return name


def clean_record(raw: dict, *, keywords: str = "") -> dict | None:
    """清洗单条原始记录。

    Returns:
        标准化记录；缺少 pid / companyCode / reply 时间时返回 None。
    """
    mapped = {FIELD_MAP[k]: raw.get(k) for k in FIELD_MAP}
    pid = _strip_text(mapped.get(INTERACTION_PID))
    company_code = _strip_text(mapped.get(COMPANY_CODE))
    reply_time = _strip_text(mapped.get(REPLY_TIME))

    if not pid or not company_code or not reply_time:
        return None

    # companyCode 为 6 位数字代码，标准化为 ticker（含交易所后缀）
    ticker = normalize_ticker(company_code)
    reply_date = reply_time[:10]

    return {
        INTERACTION_PID: pid,
        TICKER: ticker,
        COMPANY_CODE: company_code.zfill(6) if company_code.isdigit() else company_code,
        COMPANY_SHORTNAME: _strip_text(mapped.get(COMPANY_SHORTNAME)),
        QUESTION_CONTENT: _strip_text(mapped.get(QUESTION_CONTENT)),
        REPLY_CONTENT: _strip_text(mapped.get(REPLY_CONTENT)),
        QUESTION_TIME: _strip_text(mapped.get(QUESTION_TIME)) or None,
        REPLY_TIME: reply_time,
        REPLY_DATE: reply_date,
        NICKNAME: _sanitize_nickname(mapped.get(NICKNAME)),
        KEYWORDS: keywords or None,
        SOURCE: "p5w",
    }


def clean_interaction_qa(
    records: list[dict],
    *,
    keywords: str = "",
    since_date: str | None = None,
) -> list[dict]:
    """清洗原始互动问答记录并按 pid 去重。"""
    input_count = len(records)
    cleaned: list[dict] = []
    seen: set[str] = set()

    for raw in records:
        item = clean_record(raw, keywords=keywords)
        if item is None:
            continue
        if since_date and item[REPLY_DATE] < since_date:
            continue
        pid = item[INTERACTION_PID]
        if pid in seen:
            continue
        seen.add(pid)
        cleaned.append(item)

    print(
        f"Cleaned: {input_count} raw -> {len(cleaned)} unique QA",
        file=sys.stderr,
    )
    return cleaned
