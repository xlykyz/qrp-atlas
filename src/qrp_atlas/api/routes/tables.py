"""通用表浏览路由"""

from typing import Any, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from qrp_atlas.api.db import (
    detach_database_if_attached,
    get_db,
    require_irm_qa_db,
)
from qrp_atlas.contracts import IRM_INTERACTION_QA

router = APIRouter(prefix="/api", tags=["通用表浏览"])
IRM_TABLE = IRM_INTERACTION_QA.name


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


class TableRow(BaseModel):
    """动态行，所有字段用 str 兜底以便前端统一展示"""
    data: dict[str, Any]


@router.get("/tables")
def list_tables():
    """返回所有表名"""
    con = get_db()
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        return [t[0] for t in tables]
    finally:
        con.close()


@router.get("/tables/{table_name}/schema")
def table_schema(table_name: str):
    """返回表的列名和类型"""
    con = get_db()
    attached_alias: str | None = None
    try:
        if table_name == IRM_TABLE:
            attached_alias = require_irm_qa_db(con)
            cols = con.execute(
                f"SELECT column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_catalog = ? AND table_name = ? "
                "ORDER BY ordinal_position",
                [attached_alias, table_name],
            ).fetchall()
        else:
            cols = con.execute(
                "SELECT column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_name = ? "
                "ORDER BY ordinal_position",
                [table_name],
            ).fetchall()
        return [{"name": c[0], "type": c[1]} for c in cols]
    finally:
        if attached_alias is not None:
            detach_database_if_attached(con, attached_alias)
        con.close()


@router.get("/tables/{table_name}")
def query_table(
    table_name: str,
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    order_by: Optional[str] = Query(
        None, description="排序字段名，必须在表 schema 内；默认按第一列"
    ),
    order: str = Query("desc", description="排序方向：asc / desc"),
):
    """查询任意表的数据"""
    con = get_db()
    attached_alias: str | None = None
    try:
        table_ref = _quote_identifier(table_name)
        if table_name == IRM_TABLE:
            attached_alias = require_irm_qa_db(con)
            table_ref = f"{attached_alias}.{table_ref}"
            table_exists_sql = (
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_catalog = ? AND table_name = ?"
            )
            schema_sql = (
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_catalog = ? AND table_name = ? "
                "ORDER BY ordinal_position"
            )
            table_params = [attached_alias, table_name]
        else:
            table_exists_sql = (
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = ?"
            )
            schema_sql = (
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = ? ORDER BY ordinal_position"
            )
            table_params = [table_name]

        # 先检查表是否存在
        row = con.execute(table_exists_sql, table_params).fetchone()
        exists = row[0] if row else 0
        if not exists:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail=f"表 '{table_name}' 不存在")

        # 获取列信息
        cols = con.execute(schema_sql, table_params).fetchall()
        col_names = [c[0] for c in cols]

        # 校验 order_by 白名单，防注入
        if order_by:
            if order_by not in col_names:
                from fastapi import HTTPException
                raise HTTPException(
                    status_code=400,
                    detail=f"非法 order_by: {order_by}，可选: {col_names}",
                )
            sort_col = _quote_identifier(order_by)
        else:
            sort_col = "1"  # 默认按第一列

        direction = "DESC" if order.lower() == "desc" else "ASC"

        total_row = con.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()
        total = total_row[0] if total_row else 0

        rows = con.execute(
            f"SELECT * FROM {table_ref} ORDER BY {sort_col} {direction} LIMIT ? OFFSET ?",
            [limit, offset],
        ).fetchall()

        result = []
        for row in rows:
            row_dict = {}
            for i, val in enumerate(row):
                row_dict[col_names[i]] = _serialize(val)
            result.append(row_dict)

        return {
            "columns": [{"name": c[0], "type": c[1]} for c in cols],
            "rows": result,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    finally:
        if attached_alias is not None:
            detach_database_if_attached(con, attached_alias)
        con.close()


def _serialize(val: Any) -> Any:
    """DuckDB 值转 JSON 可序列化类型"""
    if val is None:
        return None
    if isinstance(val, (int, float, str, bool)):
        return val
    # DuckDB types that might not serialize directly
    try:
        return str(val)
    except Exception:
        return None
