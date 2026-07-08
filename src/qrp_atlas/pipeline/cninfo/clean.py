"""
clean.py - 东方财富机构调研数据清洗模块

将原始东财记录转换为标准化格式。
"""

from collections import defaultdict
from typing import Any

from qrp_atlas.contracts import (
    ORG_COUNT,
    SOURCE,
    get_mapping,
)

FIELD_MAP = get_mapping("eastmoney_research_visits")


def clean_eastmoney(records: list[dict]) -> list[dict]:
    """
    清洗东方财富机构调研原始数据。

    Args:
        records: fetch_from_eastmoney 返回的原始记录列表

    Returns:
        清洗后的标准化记录列表
    """
    input_count = len(records)

    # 按 (SECUCODE, RECEIVE_START_DATE) 分组，保留第一条记录
    groups: dict[tuple[str, str], dict[str, Any]] = defaultdict(dict)
    group_counts: dict[tuple[str, str], int] = defaultdict(int)

    for record in records:
        secu_code = record.get("SECUCODE", "")
        receive_start_date = record.get("RECEIVE_START_DATE", "")
        key = (secu_code, receive_start_date)
        group_counts[key] += 1
        # 只保留第一条
        if key not in groups:
            groups[key] = record

    # 构建清洗后的记录
    cleaned: list[dict] = []
    for (secu_code, receive_start_date), record in groups.items():
        notice_date = record.get("NOTICE_DATE", "")
        # 保留原始格式 "2026-05-28 00:00:00"
        receive_date = receive_start_date[:10] if receive_start_date else ""

        cleaned_record = {
            FIELD_MAP["SECUCODE"]: secu_code,
            FIELD_MAP["SECURITY_NAME_ABBR"]: record.get("SECURITY_NAME_ABBR", ""),
            FIELD_MAP["NOTICE_DATE"]: notice_date,
            FIELD_MAP["RECEIVE_START_DATE"]: receive_date,
            FIELD_MAP["RECEIVE_WAY_EXPLAIN"]: record.get("RECEIVE_WAY_EXPLAIN", ""),
            FIELD_MAP["RECEIVE_PLACE"]: record.get("RECEIVE_PLACE", ""),
            FIELD_MAP["RECEPTIONIST"]: record.get("RECEPTIONIST", ""),
            FIELD_MAP["CONTENT"]: record.get("CONTENT", ""),
            ORG_COUNT: group_counts[(secu_code, receive_start_date)],
            SOURCE: "eastmoney",
        }
        cleaned.append(cleaned_record)

    output_count = len(cleaned)
    print(
        f"Cleaned: {input_count} raw -> {output_count} unique surveys",
        file=__import__("sys").stderr,
    )

    return cleaned
