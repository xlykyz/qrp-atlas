"""load.py - 研报数据加载模块

将清洗后的研报记录 UPSERT 入 DuckDB research_report_stock 表。
"""

import logging
from typing import Any

import pandas as pd

from qrp_atlas.contracts import CREATED_AT, align_to_schema, quick_validate
from qrp_atlas.contracts.schema import RESEARCH_REPORT_STOCK

logger = logging.getLogger(__name__)


def load_report(con: Any, records: list[dict], incremental: bool = False) -> int:
    """Upsert cleaned research report records into DuckDB.

    Uses INSERT OR IGNORE when incremental=True (skip existing rows by primary key),
    or INSERT OR REPLACE when incremental=False (overwrite existing rows).

    Args:
        con: DuckDB connection object.
        records: List of cleaned dicts with snake_case DB field names.
        incremental: If True, use INSERT OR IGNORE; if False, use INSERT OR REPLACE.

    Returns:
        Number of rows actually inserted.
    """
    if not records:
        logger.warning("load_report: no records to load")
        return 0

    df = pd.DataFrame(records)
    df = align_to_schema(
        df,
        RESEARCH_REPORT_STOCK.name,
        fill_missing_optional=True,
        drop_extra=True,
    )
    df = quick_validate(df, RESEARCH_REPORT_STOCK.name, allow_extra=False)

    # Column names from schema (exclude created_at, which has DEFAULT)
    columns = [c for c in RESEARCH_REPORT_STOCK.column_names() if c != CREATED_AT]
    col_list = ", ".join(columns)
    insert_sql_keyword = "INSERT OR IGNORE" if incremental else "INSERT OR REPLACE"
    insert_sql = (
        f"{insert_sql_keyword} INTO {RESEARCH_REPORT_STOCK.name} ({col_list}) "
        f"SELECT {col_list} FROM tmp_df"
    )

    # Collect info_codes present *before* this load run
    info_codes = df["info_code"].dropna().astype(str).tolist()
    codes_before: set = set()
    if info_codes:
        placeholders_in = ", ".join(["?" for _ in info_codes])
        codes_before = {
            row[0]
            for row in con.execute(
                f"SELECT info_code FROM {RESEARCH_REPORT_STOCK.name} WHERE info_code IN ({placeholders_in})",
                info_codes,
            ).fetchall()
        }

    con.register("tmp_df", df)
    con.execute(insert_sql)

    # Count rows with info_codes that were NOT present before
    codes_after: set = set()
    if info_codes:
        placeholders_in = ", ".join(["?" for _ in info_codes])
        codes_after = {
            row[0]
            for row in con.execute(
                f"SELECT info_code FROM {RESEARCH_REPORT_STOCK.name} WHERE info_code IN ({placeholders_in})",
                info_codes,
            ).fetchall()
        }

    rows_inserted = len(codes_after - codes_before)
    logger.info("load_report: inserted %d new rows", rows_inserted)
    return rows_inserted
