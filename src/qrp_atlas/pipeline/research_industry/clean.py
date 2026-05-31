"""clean.py - 研报数据清洗模块

将 API 返回的 camelCase 字段映射为 snake_case 数据库字段，
保存原始 CSV 和规范化 CSV。
"""

import csv
import json
import logging
from pathlib import Path

from qrp_atlas.config.paths import DATA_DIR

logger = logging.getLogger(__name__)

# camelCase API key → snake_case DB key
FIELD_MAP = {
    "infoCode": "info_code",
    "title": "title",
    "stockCode": "stock_code",
    "stockName": "stock_name",
    "publishDate": "publish_date",
    "market": "market",
    "column": "report_column",  # renamed to avoid DuckDB reserved word
    "reportType": "report_type",
    "encodeUrl": "encode_url",
    "emRatingCode": "em_rating_code",
    "emRatingValue": "em_rating_value",
    "emRatingName": "em_rating_name",
    "lastEmRatingCode": "last_em_rating_code",
    "lastEmRatingValue": "last_em_rating_value",
    "lastEmRatingName": "last_em_rating_name",
    "sRatingCode": "s_rating_code",
    "sRatingName": "s_rating_name",
    "ratingChange": "rating_change",
    "indvAimPriceT": "indv_aim_price_t",
    "indvAimPriceL": "indv_aim_price_l",
    "predictThisYearEps": "predict_this_year_eps",
    "predictThisYearPe": "predict_this_year_pe",
    "predictNextYearEps": "predict_next_year_eps",
    "predictNextYearPe": "predict_next_year_pe",
    "predictNextTwoYearEps": "predict_next_two_year_eps",
    "predictNextTwoYearPe": "predict_next_two_year_pe",
    "predictLastYearEps": "predict_last_year_eps",
    "predictLastYearPe": "predict_last_year_pe",
    "actualLastYearEps": "actual_last_year_eps",
    "actualLastTwoYearEps": "actual_last_two_year_eps",
    "orgCode": "org_code",
    "orgName": "org_name",
    "orgSName": "org_sname",
    "orgType": "org_type",
    "author": "author",
    "authorID": "author_id",
    "researcher": "researcher",
    "count": "count",
    "indvInduCode": "indv_indu_code",
    "indvInduName": "indv_indu_name",
    "indvIsNew": "indv_is_new",
    "newListingDate": "new_listing_date",
    "newPurchaseDate": "new_purchase_date",
    "newIssuePrice": "new_issue_price",
    "newPeIssueA": "new_pe_issue_a",
    "attachPages": "attach_pages",
    "attachSize": "attach_size",
    "attachType": "attach_type",
    "industryCode": "industry_code",
    "industryName": "industry_name",
    "emIndustryCode": "em_industry_code",
    # From detail page (already snake_case in zwinfo)
    "noticeContent": "notice_content",
    "attachUrl": "attach_url",
}

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
    out_dir = DATA_DIR / "raw" / "research_industry"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{date_tag}.csv"

    if not records:
        logger.warning("save_raw_csv: no records to save to %s", path)
        return str(path)

    # Use original keys from the first record as column headers
    headers = list(records[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    logger.info("Saved raw CSV: %s (%d rows)", path, len(records))
    return str(path)


def save_canonical_csv(records: list[dict], date_tag: str) -> str:
    """Save cleaned (canonical) records to a CSV file.

    Args:
        records: List of cleaned dicts (snake_case keys).
        date_tag: Date tag for the filename.

    Returns:
        Path to the saved canonical CSV file.
    """
    out_dir = DATA_DIR / "canonical" / "research_industry"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{date_tag}.csv"

    if not records:
        logger.warning("save_canonical_csv: no records to save to %s", path)
        return str(path)

    # Use DB column names from FIELD_MAP values as column order
    ordered_keys = list(FIELD_MAP.values())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ordered_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    logger.info("Saved canonical CSV: %s (%d rows)", path, len(records))
    return str(path)
