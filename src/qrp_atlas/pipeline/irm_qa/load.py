"""
load.py - 互动问答数据入库模块

将清洗后的记录 upsert 到 DuckDB irm_interaction_qa 表。
"""

from __future__ import annotations

import sys
from typing import Any

import pandas as pd

from qrp_atlas.contracts import (
    CREATED_AT,
    IRM_INTERACTION_QA,
    align_to_schema,
    quick_validate,
)


def upsert_interaction_qa(
    con: Any,
    records: list[dict],
    *,
    incremental: bool = False,
) -> int:
    """将清洗后的互动问答记录 upsert 到 irm_interaction_qa 表。

    Args:
        con: DuckDB 连接
        records: 清洗后的记录列表
        incremental: True=INSERT OR IGNORE；False=INSERT OR REPLACE

    Returns:
        处理的行数
    """
    if not records:
        return 0

    table_name = IRM_INTERACTION_QA.name
    df = pd.DataFrame(records)
    df = align_to_schema(
        df,
        table_name,
        fill_missing_optional=True,
        drop_extra=True,
    )
    df = quick_validate(df, table_name, allow_extra=False)

    columns = [c for c in IRM_INTERACTION_QA.column_names() if c != CREATED_AT]
    col_names = ", ".join(columns)
    action = "INSERT OR IGNORE" if incremental else "INSERT OR REPLACE"
    sql = f"""
    {action} INTO {table_name} ({col_names})
    SELECT {col_names} FROM tmp_irm_qa_df
    """

    con.register("tmp_irm_qa_df", df)
    con.execute(sql)
    count = len(df)

    print(f"Loaded {count} records into {table_name}", file=sys.stderr)
    return count
