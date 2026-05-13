"""数据库连接工具"""

from pathlib import Path

import duckdb

from qrp_atlas.config import DB_PATH


def get_db(read_only: bool = True):
    """获取 DuckDB 连接"""
    from qrp_atlas.config import ensure_dirs
    ensure_dirs()
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def get_db_path() -> Path:
    return DB_PATH
