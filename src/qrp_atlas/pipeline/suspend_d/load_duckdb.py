"""load_duckdb.py - 将 suspend_d 数据加载到 DuckDB"""

import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH, ensure_dirs
from qrp_atlas.contracts import (
    CREATED_AT,
    SUSPEND_D,
    align_to_schema,
    init_database,
    quick_validate,
)


def load_suspend_d(df: pd.DataFrame, *, init: bool = False) -> int:
    """加载清洗后的 suspend_d 数据到 DuckDB

    行为:
    - BEGIN
    - DELETE FROM suspend_d WHERE trade_date IN (...)
    - INSERT 数据
    - COMMIT

    Args:
        df: 清洗后的 DataFrame
        init: 是否执行 init_database（建表）

    Returns:
        插入的行数
    """
    ensure_dirs()

    df = align_to_schema(
        df,
        SUSPEND_D.name,
        fill_missing_optional=True,
        drop_extra=True,
    )
    df = quick_validate(df, SUSPEND_D.name, allow_extra=False)

    con = duckdb.connect(str(DB_PATH))
    try:
        if init:
            init_database(con)

        # 删除已有日期范围内的数据（避免重复）
        dates = df["trade_date"].unique().tolist()

        con.execute("BEGIN")
        for d in dates:
            con.execute(
                f"DELETE FROM {SUSPEND_D.name} WHERE trade_date = ?",
                [d],
            )

        insert_cols = [col for col in SUSPEND_D.column_names() if col != CREATED_AT]
        cols = ", ".join(insert_cols)
        con.register("tmp_df", df)
        con.execute(
            f"INSERT INTO {SUSPEND_D.name} ({cols}) SELECT {cols} FROM tmp_df"
        )

        con.execute("COMMIT")

        return len(df)
    except Exception as e:
        con.execute("ROLLBACK")
        raise e
    finally:
        con.close()
