"""load.py - 研报数据加载模块

将清洗后的研报记录 UPSERT 入 DuckDB research_report_stock 表。
"""

import logging
from typing import Any

from qrp_atlas.contracts.schema import RESEARCH_REPORT_STOCK

logger = logging.getLogger(__name__)

# Columns whose DuckDB dtype is numeric but may arrive as empty string
_NUMERIC_DUCKDB_TYPES = {"DOUBLE", "INTEGER", "BIGINT", "DECIMAL", "FLOAT"}
_COL_DTYPE = {col.name: col.dtype for col in RESEARCH_REPORT_STOCK.columns}


def _coerce_params(params: list, columns: list[str]) -> list:
    """Convert '' → None for numeric DuckDB columns."""
    return [
        None if (val == "" and _COL_DTYPE.get(col) in _NUMERIC_DUCKDB_TYPES) else val
        for col, val in zip(columns, params)
    ]


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

    # Column names from schema (exclude created_at, which has DEFAULT)
    columns = [c for c in RESEARCH_REPORT_STOCK.column_names() if c != "created_at"]
    col_list = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql_keyword = "INSERT OR IGNORE" if incremental else "INSERT OR REPLACE"
    insert_sql = f"{insert_sql_keyword} INTO {RESEARCH_REPORT_STOCK.name} ({col_list}) VALUES ({placeholders})"

    # Collect info_codes present *before* this load run
    info_codes = [r.get("info_code") for r in records if r.get("info_code")]
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

    # Build param rows with empty-string coercion for numeric columns
    params_list = [
        _coerce_params([record.get(col) for col in columns], columns)
        for record in records
    ]
    con.executemany(insert_sql, params_list)

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
