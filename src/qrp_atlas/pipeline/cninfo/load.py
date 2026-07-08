"""
load.py - 调研公告数据入库模块

将清洗后的调研数据 upsert 到 DuckDB 数据库。
"""

from typing import Any

import pandas as pd

from qrp_atlas.contracts import (
    CREATED_AT,
    CNINFO_RESEARCH_VISITS,
    align_to_schema,
    quick_validate,
)


def upsert_research_visits(con: Any, records: list[dict], incremental: bool = False) -> int:
    """
    将清洗后的调研记录 upsert 到 cninfo_research_visits 表。

    Args:
        con: DuckDB 连接对象
        records: 清洗后的记录列表
        incremental: True=INSERT OR IGNORE(跳过已有), False=INSERT OR REPLACE(覆盖)

    Returns:
        处理的行数
    """
    if not records:
        return 0

    table_name = CNINFO_RESEARCH_VISITS.name
    df = pd.DataFrame(records)
    df = align_to_schema(
        df,
        table_name,
        fill_missing_optional=True,
        drop_extra=True,
    )
    df = quick_validate(df, table_name, allow_extra=False)

    columns = [c for c in CNINFO_RESEARCH_VISITS.column_names() if c != CREATED_AT]
    col_names = ", ".join(columns)

    # 主更新 = INSERT OR REPLACE, 增量 = INSERT OR IGNORE
    action = "INSERT OR IGNORE" if incremental else "INSERT OR REPLACE"
    sql = f"""
    {action} INTO {table_name} ({col_names})
    SELECT {col_names} FROM tmp_df
    """

    con.register("tmp_df", df)
    con.execute(sql)
    count = len(df)

    print(
        f"Loaded {count} records into {table_name}",
        file=__import__("sys").stderr,
    )

    return count
