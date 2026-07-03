"""用精确涨停价逻辑修复数据库中所有股票的涨跌停标记"""
import duckdb
from qrp_atlas.config import DB_PATH

def fix_all_limit_flags():
    con = duckdb.connect(str(DB_PATH))
    try:
        # 用精确涨停价计算逻辑更新全部历史数据
        # 
        # 涨停价 = round(pre_close * (1 + limit_pct/100), 2)
        # 涨停判定: close >= 涨停价
        #
        # limit_pct:
        #   - 主板: 10%
        #   - ST: 5%
        #   - 创业板(300/301/302)/科创板(688/689): 20%（含ST）
        #   - 北交所(.BJ): 30%（含ST）
        #   - 主板ST: 5%
        #   - 主板非ST: 10%

        con.execute("""
            UPDATE daily_market_snapshot
            SET is_limit_up = CASE
                WHEN pre_close IS NULL OR pre_close = 0 THEN FALSE
                WHEN RIGHT(ticker, 3) = '.BJ' THEN close >= ROUND(pre_close * 1.30, 2)
                WHEN substr(ticker, 1, 3) IN ('688','689','300','301','302')
                    THEN close >= ROUND(pre_close * 1.20, 2)
                WHEN is_st THEN close >= ROUND(pre_close * 1.05, 2)
                ELSE close >= ROUND(pre_close * 1.10, 2)
            END,
            is_limit_down = CASE
                WHEN pre_close IS NULL OR pre_close = 0 THEN FALSE
                WHEN RIGHT(ticker, 3) = '.BJ' THEN close <= ROUND(pre_close * 0.70, 2)
                WHEN substr(ticker, 1, 3) IN ('688','689','300','301','302')
                    THEN close <= ROUND(pre_close * 0.80, 2)
                WHEN is_st THEN close <= ROUND(pre_close * 0.95, 2)
                ELSE close <= ROUND(pre_close * 0.90, 2)
            END
        """)

        total = con.execute("SELECT COUNT(*) FROM daily_market_snapshot").fetchone()[0]
        print(f"✅ 已修复全部 {total} 条记录的涨跌停标记")

        # 验证：查一个受四舍五入影响的边界案例
        latest = con.execute("SELECT MAX(trade_date) FROM daily_market_snapshot").fetchone()[0]
        rows = con.execute(f"""
            SELECT ticker, name, trade_date, pre_close, close,
                   ROUND(pct_change, 2) as pct,
                   ROUND(pre_close * CASE
                       WHEN is_st THEN 1.05
                       WHEN substr(ticker, 1, 3) IN ('688','689','300','301','302') THEN 1.20
                       ELSE 1.10
                   END, 2) as theoretical_limit_up,
                   is_limit_up
            FROM daily_market_snapshot
            WHERE trade_date = '{latest}'
              AND pct_change BETWEEN 9.8 AND 10.2
              AND substr(ticker, 1, 3) NOT IN ('688','689','300','301','302')
            ORDER BY pct_change
            LIMIT 10
        """).fetchall()
        print(f"\n最新交易日主板 pct 在9.8~10.2之间的股票涨停情况:")
        for r in rows:
            print(f"  {r[0]} {r[1]:<10s} pre={r[3]:>6.2f} close={r[4]:>6.2f} "
                  f"pct={r[5]:>5.2f}% 精确涨停价={r[6]:>6.2f} limit_up={r[7]}")

    finally:
        con.close()

if __name__ == "__main__":
    fix_all_limit_flags()