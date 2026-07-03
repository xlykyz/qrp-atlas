"""修复数据库中 301/302/689 前缀股票的涨跌停标记"""
import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH

def fix_limit_flags():
    con = duckdb.connect(str(DB_PATH))
    try:
        # 获取所有受影响的股票（前缀 301/302/689）
        rows = con.execute("""
            SELECT DISTINCT ticker FROM daily_market_snapshot
            WHERE ticker LIKE '301%' OR ticker LIKE '302%' OR ticker LIKE '689%'
        """).fetchall()
        tickers = [r[0] for r in rows]
        print(f"需要修复的股票数: {len(tickers)}")

        # 用固定后的逻辑批量更新
        # 创业板(301/302)/科创板(689) 非ST阈值19.9，ST阈值4.9
        con.execute("""
            UPDATE daily_market_snapshot
            SET is_limit_up = CASE
                WHEN ABS(pct_change) >= 100 THEN FALSE
                WHEN is_st THEN pct_change >= 4.9
                ELSE pct_change >= 19.9
            END,
            is_limit_down = CASE
                WHEN ABS(pct_change) >= 100 THEN FALSE
                WHEN is_st THEN pct_change <= -4.9
                ELSE pct_change <= -19.9
            END
            WHERE ticker LIKE '301%'
               OR ticker LIKE '302%'
               OR ticker LIKE '689%'
        """)
        print("UPDATE 完成")

        # 验证：查一下 >10% 但 <19.9% 的 301 股票是否已正确取消标记
        latest = con.execute("SELECT MAX(trade_date) FROM daily_market_snapshot").fetchone()[0]
        wrong = con.execute(f"""
            SELECT COUNT(*) FROM daily_market_snapshot
            WHERE (ticker LIKE '301%' OR ticker LIKE '302%')
              AND trade_date = '{latest}'
              AND pct_change > 10 AND pct_change < 19.9
              AND is_limit_up = TRUE
        """).fetchone()[0]
        print(f"修复后最新交易日仍有误标的: {wrong} 条")
        if wrong == 0:
            print("✅ 全部修复成功")
        else:
            print("❌ 仍有残留，请检查")
    finally:
        con.close()

if __name__ == "__main__":
    fix_limit_flags()