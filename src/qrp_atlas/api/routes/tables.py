"""通用表浏览路由"""

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

from qrp_atlas.api.db import get_db

router = APIRouter(prefix="/api", tags=["通用表浏览"])


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
    try:
        cols = con.execute(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = ? "
            "ORDER BY ordinal_position",
            [table_name],
        ).fetchall()
        return [{"name": c[0], "type": c[1]} for c in cols]
    finally:
        con.close()


@router.get("/tables/{table_name}")
def query_table(
    table_name: str,
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """查询任意表的数据"""
    con = get_db()
    try:
        # 先检查表是否存在
        row = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = ?",
            [table_name],
        ).fetchone()
        exists = row[0] if row else 0
        if not exists:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail=f"表 '{table_name}' 不存在")

        # 获取列信息
        cols = con.execute(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = ? "
            "ORDER BY ordinal_position",
            [table_name],
        ).fetchall()
        col_names = [c[0] for c in cols]

        total_row = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
        total = total_row[0] if total_row else 0

        rows = con.execute(
            f'SELECT * FROM "{table_name}" ORDER BY 1 DESC LIMIT ? OFFSET ?',
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
