"""
load.py - 调研公告数据入库模块

将清洗后的调研数据 upsert 到 DuckDB 数据库。
"""

from typing import Any

from qrp_atlas.contracts import (
    SECU_CODE,
    SEC_NAME,
    NOTICE_DATE,
    RECEIVE_DATE,
    RECEIVE_WAY,
    RECEIVE_PLACE,
    RECEPTIONIST,
    CONTENT,
    ORG_COUNT,
    SOURCE,
    ANNOUNCEMENT_TITLE,
    ADJUNCT_URL,
    ADJUNCT_SIZE,
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

    table_name = "cninfo_research_visits"
    columns = [
        SECU_CODE,
        SEC_NAME,
        NOTICE_DATE,
        RECEIVE_DATE,
        RECEIVE_WAY,
        RECEIVE_PLACE,
        RECEPTIONIST,
        CONTENT,
        ORG_COUNT,
        SOURCE,
        # 东财数据不包含这些字段，设为 NULL
        ANNOUNCEMENT_TITLE,
        ADJUNCT_URL,
        ADJUNCT_SIZE,
    ]

    col_names = ", ".join(columns)
    placeholders = ", ".join(["?" for _ in columns])

    # 主更新 = INSERT OR REPLACE, 增量 = INSERT OR IGNORE
    action = "INSERT OR IGNORE" if incremental else "INSERT OR REPLACE"
    sql = f"""
    {action} INTO {table_name} ({col_names})
    VALUES ({placeholders})
    """

    count = 0
    for record in records:
        values = [record.get(col) for col in columns]
        con.execute(sql, values)
        count += 1

    print(
        f"Loaded {count} records into {table_name}",
        file=__import__("sys").stderr,
    )

    return count
