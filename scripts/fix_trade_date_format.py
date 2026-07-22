"""
修复数据库中 trade_date 字段格式
将带时间的日期格式转换为纯日期格式 (YYYY-MM-DD)
"""
from pathlib import Path
import duckdb

from qrp_atlas.config import DB_PATH

def fix_trade_date():
    if not DB_PATH.exists():
        print(f"数据库文件不存在: {DB_PATH}")
        return
    
    con = duckdb.connect(str(DB_PATH))
    
    print("检查 trade_date 字段格式...")
    
    sample = con.execute("""
        SELECT DISTINCT trade_date 
        FROM daily_market_snapshot 
        ORDER BY trade_date DESC
        LIMIT 5
    """).fetchall()
    
    print("当前数据示例:")
    for row in sample:
        print(f"  {row[0]}")
    
    print("\n开始修复...")
    
    con.execute("""
        UPDATE daily_market_snapshot
        SET trade_date = CAST(trade_date AS DATE)
        WHERE trade_date IS NOT NULL
    """)
    
    rows_updated = con.execute("SELECT COUNT(*) FROM daily_market_snapshot").fetchone()[0]
    print(f"已处理 {rows_updated} 行数据")
    
    print("\n验证修复结果...")
    sample = con.execute("""
        SELECT DISTINCT trade_date 
        FROM daily_market_snapshot 
        ORDER BY trade_date DESC
        LIMIT 5
    """).fetchall()
    
    print("修复后数据示例:")
    for row in sample:
        print(f"  {row[0]}")
    
    con.close()
    print("\n修复完成!")

if __name__ == "__main__":
    fix_trade_date()
