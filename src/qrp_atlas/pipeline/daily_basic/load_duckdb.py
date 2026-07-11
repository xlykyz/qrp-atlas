"""load_duckdb.py - 将 daily_basic 数据加载到 DuckDB"""

import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH, ensure_dirs
from qrp_atlas.contracts import (
    CREATED_AT,
    DAILY_BASIC,
    align_to_schema,
    init_database,
    quick_validate,
)


def load_daily_basic(df: pd.DataFrame, trade_date: str, *, init: bool = False) -> int:
    """加载清洗后的 daily_basic 数据到 DuckDB

    行为:
    - BEGIN
    - DELETE FROM daily_basic WHERE trade_date = ?
    - INSERT 数据
    - COMMIT

    Args:
        df: 清洗后的 DataFrame
        trade_date: 交易日期字符串 (YYYY-MM-DD)

    Returns:
        插入的行数
    """
    ensure_dirs()

    df = align_to_schema(
        df,
        DAILY_BASIC.name,
        fill_missing_optional=True,
        drop_extra=True,
    )
    df = quick_validate(df, DAILY_BASIC.name, allow_extra=False)

    con = duckdb.connect(str(DB_PATH))
    try:
        if init:
            init_database(con)

        con.execute("BEGIN")
        con.execute(
            f"DELETE FROM {DAILY_BASIC.name} WHERE trade_date = ?",
            [trade_date],
        )

        con.register("tmp_df", df)
        # 按 schema 列名插入，created_at 交给 DEFAULT CURRENT_TIMESTAMP
        cols = [col for col in DAILY_BASIC.column_names() if col != CREATED_AT]
        col_names = ", ".join(cols)
        con.execute(
            f"INSERT INTO {DAILY_BASIC.name} ({col_names}) SELECT {col_names} FROM tmp_df"
        )

        con.execute("COMMIT")

        return len(df)
    except Exception as e:
        con.execute("ROLLBACK")
        raise e
    finally:
        con.close()