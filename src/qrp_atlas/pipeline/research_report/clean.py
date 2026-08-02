"""clean.py - 研报数据清洗模块

将 API 返回的 camelCase 字段映射为 snake_case 数据库字段，
保存原始 CSV 和规范化 CSV。
"""

import csv
import json
import logging
from pathlib import Path

from qrp_atlas.config.paths import DATA_DIR
from qrp_atlas.contracts import get_mapping

logger = logging.getLogger(__name__)

FIELD_MAP = get_mapping("eastmoney_research_report")

# Fields that arrive as Python lists and need JSON string serialization
LIST_FIELDS = {"author", "authorID"}


def clean_record(raw: dict) -> dict:
    """Clean a single raw record.

    - Remap camelCase keys → snake_case per FIELD_MAP
    - JSON-serialize list fields
    - Missing fields are set to None
    """
    cleaned = {}
    for api_key, db_key in FIELD_MAP.items():
        val = raw.get(api_key)
        if api_key in LIST_FIELDS and isinstance(val, list):
            val = json.dumps(val, ensure_ascii=False)
        cleaned[db_key] = val if api_key in raw else None
    return cleaned


def clean_report(records: list[dict]) -> list[dict]:
    """Clean a batch of raw API records.

    Args:
        records: List of raw API response dicts (with camelCase keys,
                 possibly enriched by fetch_report_detail with
                 noticeContent/attachUrl).

    Returns:
        List of cleaned dicts using snake_case DB field names.
    """
    return [clean_record(r) for r in records]


def save_raw_csv(records: list[dict], date_tag: str) -> str:
    """Save raw API records to a CSV file.

    Args:
        records: List of raw API response dicts.
        date_tag: Date tag for the filename (e.g. "2026-05-30_2026-05-31").

    Returns:
        Path to the saved raw CSV file.
    """
    path = DATA_DIR / "raw" / "research_report" / f"{date_tag}.csv"
    write_raw_csv(records, path)
    return str(path)


def save_canonical_csv(records: list[dict], date_tag: str) -> str:
    """Save cleaned (canonical) records to a CSV file.

    Args:
        records: List of cleaned dicts (snake_case keys).
        date_tag: Date tag for the filename.

    Returns:
        Path to the saved canonical CSV file.
    """
    path = DATA_DIR / "canonical" / "research_report" / f"{date_tag}.csv"
    write_canonical_csv(records, path)
    return str(path)


def write_raw_csv(records: list[dict], path: Path) -> Path:
    """Write raw records to an explicitly supplied path."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        logger.warning("write_raw_csv: no records to save to %s", path)
        return path
    headers = list(records[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    logger.info("Saved raw CSV: %s (%d rows)", path, len(records))
    return path


def write_canonical_csv(records: list[dict], path: Path) -> Path:
    """Write canonical records to an explicitly supplied path."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        logger.warning("write_canonical_csv: no records to save to %s", path)
        return path
    ordered_keys = list(FIELD_MAP.values())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=ordered_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    logger.info("Saved canonical CSV: %s (%d rows)", path, len(records))
    return path
