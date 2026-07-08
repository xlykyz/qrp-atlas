import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH, ensure_dirs
from qrp_atlas.contracts import (
    CREATED_AT,
    DAILY_MARKET_SNAPSHOT,
    align_to_schema,
    init_database,
    quick_validate,
)


def load_daily_market_snapshot(df: pd.DataFrame, trade_date: str) -> int:
    """
    加载清洗后的数据到 DuckDB
    
    行为:
    - BEGIN
    - DELETE FROM daily_market_snapshot WHERE trade_date = ?
    - INSERT 数据
    - COMMIT
    
    Args:
        df: 清洗后的数据 DataFrame
        trade_date: 交易日期
        
    Returns:
        插入的行数
    """
    ensure_dirs()
    
    df = align_to_schema(
        df,
        "daily_market_snapshot",
        fill_missing_optional=True,
        drop_extra=True,
    )
    df = quick_validate(df, "daily_market_snapshot", allow_extra=False)
    
    con = duckdb.connect(str(DB_PATH))
    try:
        init_database(con)
        
        con.execute("BEGIN")
        con.execute(
            "DELETE FROM daily_market_snapshot WHERE trade_date = ?",
            [trade_date]
        )
        
        con.register("tmp_df", df)
        # 按 schema 列名插入，created_at 交给 DuckDB DEFAULT CURRENT_TIMESTAMP。
        cols = [col for col in DAILY_MARKET_SNAPSHOT.column_names() if col != CREATED_AT]
        col_names = ", ".join(cols)
        con.execute(f"INSERT INTO daily_market_snapshot ({col_names}) SELECT {col_names} FROM tmp_df")
        
        con.execute("COMMIT")
        
        return len(df)
    except Exception as e:
        con.execute("ROLLBACK")
        raise e
    finally:
        con.close()
