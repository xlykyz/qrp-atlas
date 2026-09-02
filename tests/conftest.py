"""tests 顶层公用 fixtures。

只放跨子目录共用的工具；与回测/contracts/api 强耦合的 fixture 仍放在各自子目录的
conftest.py 或测试文件中。
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import duckdb
import pytest

from qrp_atlas.contracts import init_database


def build_test_db(db_path: Path, include_adj_factor: bool = True) -> None:
    """在 db_path 处创建临时 DuckDB，建好所有 contracts 表并写入小样本数据。

    样本：000001.SZ 3 个交易日的 OHLCV + 复权因子 + 交易日历。
    """

    con = duckdb.connect(str(db_path))
    try:
        init_database(con)
        if not include_adj_factor:
            con.execute("DROP TABLE IF EXISTS adj_factor_changes")

        con.execute(
            """
            INSERT INTO daily_market_snapshot
                (trade_date, ticker, name, open, high, low, close,
                 pre_close, volume, amount)
            VALUES
                ('2024-01-01', '000001.SZ', '平安银行', 10.0, 11.0, 9.0, 10.0, 9.8, 1000, 10000.0),
                ('2024-01-02', '000001.SZ', '平安银行', 11.0, 12.0, 10.0, 11.0, 10.0, 1200, 12000.0),
                ('2024-01-03', '000001.SZ', '平安银行', 12.0, 13.0, 11.0, 12.0, 11.0, 1300, 13000.0)
            """
        )

        if include_adj_factor:
            con.execute(
                """
                INSERT INTO adj_factor_changes (ticker, trade_date, adj_factor)
                VALUES
                    ('000001.SZ', '2024-01-01', 1.0),
                    ('000001.SZ', '2024-01-02', 2.0),
                    ('000001.SZ', '2024-01-03', 4.0)
                """
            )

        con.execute(
            """
            INSERT INTO trading_calendar (trade_date, is_open, year, month, quarter)
            VALUES
                ('2024-01-01', TRUE, 2024, 1, 1),
                ('2024-01-02', TRUE, 2024, 1, 1),
                ('2024-01-03', TRUE, 2024, 1, 1)
            """
        )
    finally:
        con.close()


def make_fake_get_db(db_path: Path) -> Callable:
    """返回一个可替代 qrp_atlas.api.routes.daily.get_db 的桩函数。

    daily 路由会在 finally 中 close()，所以每次调用都返回一个新连接。
    """

    def _fake_get_db(read_only: bool = True):
        return duckdb.connect(str(db_path), read_only=read_only)

    return _fake_get_db


@pytest.fixture
def sample_db_path(tmp_path: Path) -> Path:
    """带复权因子表的样本临时 DuckDB 路径。"""
    db_path = tmp_path / "sample.duckdb"
    build_test_db(db_path, include_adj_factor=True)
    return db_path


@pytest.fixture
def sample_db_path_without_adj(tmp_path: Path) -> Path:
    """没有 adj_factor_changes 表的样本临时 DuckDB 路径，用于退化场景测试。"""
    db_path = tmp_path / "sample_no_adj.duckdb"
    build_test_db(db_path, include_adj_factor=False)
    return db_path
