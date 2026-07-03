"""开发测试专用 SQL 查询路由"""

import re
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from qrp_atlas.api.db import get_db

router = APIRouter(prefix="/api/dev", tags=["开发工具"])

# 允许的 SQL 语句前缀
_ALLOWED_PREFIXES = (
    "SELECT",
    "WITH",
    "SHOW",
    "DESCRIBE",
    "EXPLAIN",
)

# 禁止的关键词（大小写不敏感），匹配完整单词
_BLOCKED_KEYWORDS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "COPY",
    "ATTACH",
    "DETACH",
    "INSTALL",
    "LOAD",
    "PRAGMA",
    "CALL",
    "EXPORT",
    "IMPORT",
)

MAX_ROWS = 5000


class SqlQuery(BaseModel):
    sql: str


def _serialize(val: Any) -> Any:
    """DuckDB 值转 JSON 可序列化类型"""
    if val is None:
        return None
    if isinstance(val, (int, float, str, bool)):
        return val
    try:
        return str(val)
    except Exception:
        return None


def _validate_sql(sql: str) -> str:
    """校验 SQL 合法性，返回清理后的 SQL"""
    stripped = sql.strip().rstrip(";")

    if not stripped:
        raise HTTPException(status_code=400, detail="SQL 不能为空")

    # 提取语句首词判断类型
    first_word_match = re.match(r"\s*(\w+)", stripped)
    if not first_word_match:
        raise HTTPException(status_code=400, detail="无法解析 SQL 语句")

    first_word = first_word_match.group(1).upper()

    # 检查是否以允许的前缀开头
    if not any(first_word.startswith(p) for p in _ALLOWED_PREFIXES):
        raise HTTPException(
            status_code=403,
            detail=f"不允许的操作类型: {first_word}。仅支持 SELECT / WITH / SHOW / DESCRIBE / EXPLAIN",
        )

    # 检查是否包含被禁止的关键词
    # 排除 SHOW 和 DESCRIBE 场景（它们可能跟表名有关）
    upper_sql = stripped.upper()
    for kw in _BLOCKED_KEYWORDS:
        # 用正则匹配完整单词，避免 SHOW 里误伤
        if re.search(rf"\b{kw}\b", upper_sql):
            raise HTTPException(
                status_code=403,
                detail=f"SQL 中包含被禁止的操作: {kw}。仅允许只读查询。",
            )

    return stripped


@router.post("/sql")
def execute_sql(query: SqlQuery):
    """执行只读 SQL 查询（开发调试用）

    支持: SELECT, WITH, SHOW, DESCRIBE, EXPLAIN
    禁止: INSERT, UPDATE, DELETE, DROP, ALTER, CREATE 等写操作
    """
    sql = _validate_sql(query.sql)

    con = get_db(read_only=True)
    try:
        result = con.execute(sql).fetchall()

        if not result:
            return {"columns": [], "rows": [], "total": 0}

        # 取列描述
        col_desc = con.description
        col_names = [c[0] for c in col_desc]

        # 限制返回行数
        limited = result[:MAX_ROWS]
        total = len(result)

        rows = []
        for row in limited:
            row_dict = {}
            for i, val in enumerate(row):
                row_dict[col_names[i]] = _serialize(val)
            rows.append(row_dict)

        return {
            "columns": [{"name": c[0], "type": str(c[1])} for c in col_desc],
            "rows": rows,
            "total": total,
            "limit": MAX_ROWS,
            "truncated": total > MAX_ROWS,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"SQL 执行错误: {e}",
        )
    finally:
        con.close()