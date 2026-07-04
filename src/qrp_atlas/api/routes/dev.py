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
# 查询超时（毫秒），DuckDB 通过 SET 语句设置
QUERY_TIMEOUT_MS = 30_000


class SqlQuery(BaseModel):
    sql: str
    offset: int = 0
    limit: int = 1000  # 默认 1000，最多 MAX_ROWS


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


def _strip_strings_and_comments(sql: str) -> str:
    """移除字符串字面量与注释，避免禁词检查误伤。

    覆盖：单引号字符串、双引号字符串、-- 行注释、/* */ 块注释。
    """
    # 移除块注释 /* ... */
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    # 移除行注释 --
    sql = re.sub(r"--[^\n]*", " ", sql)
    # 移除单引号字符串（'' 转义视为同一字符串内）
    sql = re.sub(r"'(?:''|[^'])*'", "''", sql)
    # 移除双引号字符串（标识符）
    sql = re.sub(r'"(?:""|[^"])*"', '""', sql)
    return sql


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

    # 检查是否包含被禁止的关键词（在剥离字符串/注释后的文本上检查）
    stripped_for_check = _strip_strings_and_comments(stripped)
    upper_sql = stripped_for_check.upper()
    for kw in _BLOCKED_KEYWORDS:
        # 用正则匹配完整单词
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
    支持 offset/limit 翻页（默认 1000 行，最多 5000 行）。
    """
    sql = _validate_sql(query.sql)

    # 校验分页参数
    if query.offset < 0:
        raise HTTPException(status_code=400, detail="offset 不能为负数")
    if query.limit < 1:
        raise HTTPException(status_code=400, detail="limit 必须大于 0")
    if query.limit > MAX_ROWS:
        raise HTTPException(
            status_code=400, detail=f"limit 超过上限 {MAX_ROWS}"
        )

    con = get_db(read_only=True)
    try:
        # 设置查询超时（DuckDB 0.10+ 支持 SET 设全局会话参数）
        try:
            con.execute(
                f"SET statement_timeout = {int(QUERY_TIMEOUT_MS)}"
            )
        except Exception:
            # 老版本 DuckDB 不支持此参数，忽略
            pass

        result = con.execute(sql).fetchall()

        if not result:
            return {"columns": [], "rows": [], "total": 0, "offset": query.offset}

        # 取列描述
        col_desc = con.description
        col_names = [c[0] for c in col_desc]

        total = len(result)
        # 服务端分页
        paged = result[query.offset : query.offset + query.limit]

        rows = []
        for row in paged:
            row_dict = {}
            for i, val in enumerate(row):
                row_dict[col_names[i]] = _serialize(val)
            rows.append(row_dict)

        return {
            "columns": [{"name": c[0], "type": str(c[1])} for c in col_desc],
            "rows": rows,
            "total": total,
            "limit": query.limit,
            "offset": query.offset,
            "truncated": total > query.offset + len(paged),
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