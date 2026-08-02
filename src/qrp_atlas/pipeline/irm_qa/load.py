"""
load.py - 互动问答数据入库模块

将清洗后的记录 upsert 到 DuckDB irm_interaction_qa 表。
"""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.contracts import (
    CREATED_AT,
    INTERACTION_PID,
    IRM_INTERACTION_QA,
    align_to_schema,
    quick_validate,
)


def prepare_interaction_qa_frame(
    records: Sequence[Mapping[str, Any]] | pd.DataFrame,
) -> pd.DataFrame:
    """Align and validate cleaned records against the table contract."""

    frame = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records)
    if frame.empty and not len(frame.columns):
        return pd.DataFrame(columns=IRM_INTERACTION_QA.column_names())
    frame = align_to_schema(
        frame,
        IRM_INTERACTION_QA.name,
        fill_missing_optional=True,
        drop_extra=True,
    )
    return quick_validate(frame, IRM_INTERACTION_QA.name, allow_extra=False)


def append_interaction_qa(
    con: Any,
    records: Sequence[Mapping[str, Any]] | pd.DataFrame,
    *,
    execution_control: ExecutionControl | None = None,
) -> int:
    """Append cleaned records and return the actual number of inserted rows.

    The caller owns the transaction. Existing ``pid`` rows are preserved by
    ``INSERT OR IGNORE``; this function does not implement revision updates.
    """

    if execution_control is not None:
        execution_control.check()
    prepared = prepare_interaction_qa_frame(records)
    if prepared.empty:
        return 0
    prepared = prepared.drop_duplicates(
        subset=list(IRM_INTERACTION_QA.primary_key),
        keep="first",
    ).reset_index(drop=True)

    table_name = IRM_INTERACTION_QA.name
    view_name = "irm_contract_rows"
    con.register(view_name, prepared)
    try:
        if execution_control is not None:
            execution_control.check()
        existing = int(
            con.execute(
                f"""
                SELECT COUNT(DISTINCT incoming.{INTERACTION_PID})
                FROM {view_name} AS incoming
                JOIN {table_name} AS target
                  ON target.{INTERACTION_PID} = incoming.{INTERACTION_PID}
                """
            ).fetchone()[0]
        )
        columns = [column for column in IRM_INTERACTION_QA.column_names() if column != CREATED_AT]
        column_sql = ", ".join(columns)
        con.execute(
            f"""
            INSERT OR IGNORE INTO {table_name} ({column_sql})
            SELECT {column_sql} FROM {view_name}
            """
        )
        if execution_control is not None:
            execution_control.check()
        return max(0, len(prepared) - existing)
    finally:
        con.unregister(view_name)


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
    df = prepare_interaction_qa_frame(records)

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
