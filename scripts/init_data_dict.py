"""
init_data_dict.py - 一次性脚本：填充 stock_info 和 trading_calendar 表

从 tushare pro.stock_basic 拉取全量股票基础信息，
从 akshare tool_trade_date_hist_sina 拉取交易日历，
写入 quant.db。

幂等设计：使用 DELETE + INSERT 模式，可多次安全运行。

使用方式:
    source .venv/bin/activate
    python scripts/init_data_dict.py
"""

import sys
import os

# 确保能找到项目包
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import duckdb
import pandas as pd

from datetime import datetime, date
from qrp_atlas.contracts import STOCK_INFO, TRADING_CALENDAR, init_database
from qrp_atlas.config.tushare_client import get_tushare_pro


def _try_import_akshare():
    """尝试导入 akshare，带友好错误提示"""
    try:
        import akshare as ak
        return ak
    except ImportError:
        print("❌ akshare 未安装，请执行: pip install akshare")
        sys.exit(1)


def fetch_stock_info(pro) -> pd.DataFrame:
    """从 tushare pro.stock_basic 拉取全量股票基础信息

    同时拉取 list_status='L'（上市）和 list_status='D'（退市）的数据。
    ts_code 格式如 000001.SZ，直接作为 ticker 使用。

    Returns:
        DataFrame 包含列: ticker, name, exchange, market, list_date, delist_date, is_active
    """
    print("📡 从 tushare 拉取上市股票数据 (list_status='L')...")
    df_l = pro.stock_basic(list_status="L", fields="ts_code,name,exchange,market,list_date,delist_date")

    print("📡 从 tushare 拉取退市股票数据 (list_status='D')...")
    df_d = pro.stock_basic(list_status="D", fields="ts_code,name,exchange,market,list_date,delist_date")

    df = pd.concat([df_l, df_d], ignore_index=True)
    print(f"   原始数据: {len(df)} 条")

    # 重命名：ts_code -> ticker
    df = df.rename(columns={"ts_code": "ticker"})

    # 处理日期空值
    df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce").where(df["list_date"].notna(), None)
    df["delist_date"] = pd.to_datetime(df["delist_date"], errors="coerce").where(df["delist_date"].notna(), None)

    # 计算 is_active: list_date 不为空 且 delist_date 为空
    df["is_active"] = df["list_date"].notna() & df["delist_date"].isna()

    # 添加 updated_at
    df["updated_at"] = pd.Timestamp.now()

    # 只保留需要的列
    df = df[["ticker", "name", "exchange", "market", "list_date", "delist_date", "is_active", "updated_at"]]

    print(f"   清洗后: {len(df)} 条")
    return df


def fetch_trading_calendar() -> pd.DataFrame:
    """从 akshare tool_trade_date_hist_sina() 拉取交易日历

    Returns:
        DataFrame 包含列: trade_date, is_open, year, month, quarter
    """
    ak = _try_import_akshare()
    print("📡 从 akshare 拉取交易日历...")
    df = ak.tool_trade_date_hist_sina()
    print(f"   原始数据: {len(df)} 条")

    # 重命名 trade_date
    df = df.rename(columns={"trade_date": "trade_date_raw"})

    # 解析日期
    df["trade_date"] = pd.to_datetime(df["trade_date_raw"], errors="coerce").dt.date

    # 所有记录都是交易日 -> is_open=True
    df["is_open"] = True

    # 提取 year, month, quarter
    df["trade_date_dt"] = pd.to_datetime(df["trade_date_raw"], errors="coerce")
    df["year"] = df["trade_date_dt"].dt.year.astype("int64")
    df["month"] = df["trade_date_dt"].dt.month.astype("int64")
    df["quarter"] = df["trade_date_dt"].dt.quarter.astype("int64")

    df = df[["trade_date", "is_open", "year", "month", "quarter"]]
    df = df.dropna(subset=["trade_date"])

    # 去重（akshare 可能返回重复行）
    df = df.drop_duplicates(subset=["trade_date"])

    print(f"   清洗后: {len(df)} 条交易日")
    return df


def write_stock_info(con, df: pd.DataFrame):
    """写入 stock_info 表（幂等：先清空再写入）"""
    print(f"\n📝 写入 stock_info 表...")
    con.execute("DELETE FROM stock_info")
    if len(df) > 0:
        # 转换为 Python 原生类型列表，避免 DuckDB 的 pandas 时间戳问题
        records = []
        for _, row in df.iterrows():
            ld = row["list_date"]
            dd = row["delist_date"]
            records.append((
                row["ticker"],
                row["name"],
                row["exchange"],
                row["market"],
                ld.date() if pd.notna(ld) and ld is not None else None,
                dd.date() if pd.notna(dd) and dd is not None else None,
                bool(row["is_active"]),
                row["updated_at"],
            ))
        con.executemany(
            "INSERT INTO stock_info (ticker, name, exchange, market, list_date, delist_date, is_active, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            records,
        )
    print(f"   ✅ stock_info: {len(df)} 行已写入")


def write_trading_calendar(con, df: pd.DataFrame):
    """写入 trading_calendar 表（幂等：先清空再写入）"""
    print(f"\n📝 写入 trading_calendar 表...")
    con.execute("DELETE FROM trading_calendar")
    if len(df) > 0:
        records = [
            (
                row["trade_date"],
                bool(row["is_open"]),
                int(row["year"]),
                int(row["month"]),
                int(row["quarter"]),
            )
            for _, row in df.iterrows()
        ]
        con.executemany(
            "INSERT INTO trading_calendar (trade_date, is_open, year, month, quarter) VALUES (?, ?, ?, ?, ?)",
            records,
        )
    print(f"   ✅ trading_calendar: {len(df)} 行已写入")


def print_stats(con):
    """打印统计信息"""
    print("\n" + "=" * 50)
    print("📊 数据字典填充统计")
    print("=" * 50)

    for table_name in ["stock_info", "trading_calendar"]:
        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count = result[0] if result else 0
        print(f"  {table_name}: {count:,} 行")

    # 打印 stock_info 样例
    print("\n  stock_info 样例 (前5行):")
    try:
        sample = con.execute("SELECT ticker, name, exchange, is_active FROM stock_info LIMIT 5").fetchdf()
        print(sample.to_string(index=False))
    except Exception:
        pass

    # 打印 trading_calendar 样例
    print("\n  trading_calendar 样例 (首尾各3行):")
    try:
        head = con.execute("SELECT * FROM trading_calendar ORDER BY trade_date LIMIT 3").fetchdf()
        tail = con.execute("SELECT * FROM trading_calendar ORDER BY trade_date DESC LIMIT 3").fetchdf()
        print("    --- 最早 ---")
        print(head.to_string(index=False))
        print("    --- 最晚 ---")
        print(tail.to_string(index=False))
    except Exception:
        pass

    print("\n✅ 数据字典初始化完成")


def main():
    from qrp_atlas.config import DB_PATH
    db_path = str(DB_PATH)

    print(f"📂 数据库: {os.path.abspath(db_path)}")
    print()

    # 获取 tushare 客户端
    print("🔑 获取 tushare 客户端...")
    pro = get_tushare_pro()
    print("   ✅ tushare 客户端就绪")

    # 拉取数据
    df_stock = fetch_stock_info(pro)
    df_calendar = fetch_trading_calendar()

    # 写入数据库
    print(f"\n💾 写入 {os.path.abspath(db_path)} ...")
    con = duckdb.connect(str(db_path))

    # 确保表已创建
    init_database(con)

    write_stock_info(con, df_stock)
    write_trading_calendar(con, df_calendar)

    # 统计
    print_stats(con)

    con.close()
    print("\n🎉 全部完成!")


if __name__ == "__main__":
    main()
