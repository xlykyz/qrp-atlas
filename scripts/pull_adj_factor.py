"""
pull_adj_factor.py - 全量拉取 A 股复权因子

策略：按股票拉全量历史，仅存储变化点（非每日重复值）
存储：DuckDB 表 adj_factor_changes (ticker, trade_date, adj_factor)
查询：ASOF JOIN

用法：
    python scripts/pull_adj_factor.py              # 全量拉取
    python scripts/pull_adj_factor.py --resume     # 断点续传
"""
import sys
import os
import json
import time
from datetime import datetime, date
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from qrp_atlas.config import DB_PATH, LOG_DIR, STATE_DIR

import duckdb
import pandas as pd

# --- 配置 ---
PROGRESS_FILE = STATE_DIR / ".adj_factor_progress.json"
LOG_FILE = LOG_DIR / "adj_factor_pull.log"
BATCH_INTERVAL = 0.15  # API 调用间隔（秒）
MAX_RETRIES = 3

# Tushare 配置 - 从项目配置读取（仅从 .env 获取，不硬编码）
from qrp_atlas.config.tushare_client import _CUSTOM_API_URL
from qrp_atlas.config import TUSHARE_TOKEN


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def get_tushare_pro_client():
    """获取 tushare pro 客户端（从 .env 读取，不硬编码）"""
    import tushare as ts

    token = TUSHARE_TOKEN
    if not token:
        raise ValueError(
            "TUSHARE_TOKEN 未配置！请复制 .env.example 为 .env 并填入 token。"
        )
    pro = ts.pro_api(token)
    pro._DataApi__http_url = _CUSTOM_API_URL
    return pro


def load_progress() -> set:
    """加载已完成的股票列表"""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return set(json.load(f))
    return set()


def save_progress(ticker: str, done: set):
    """保存进度"""
    done.add(ticker)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(list(done), f)


def get_all_tickers(db: duckdb.DuckDBPyConnection) -> list[str]:
    """从数据库获取所有出现过股票代码，按字母排序"""
    rows = db.execute("""
        SELECT DISTINCT ticker 
        FROM daily_market_snapshot 
        ORDER BY ticker
    """).fetchall()
    return [r[0] for r in rows]


def pull_stock_adj(pro, ticker: str) -> pd.DataFrame | None:
    """拉取一只股票的全量复权因子"""
    for attempt in range(MAX_RETRIES):
        try:
            # 不加日期限制，拿到最早以来的全部数据
            df = pro.adj_factor(ts_code=ticker)
            if df is None or len(df) == 0:
                return None
            # 只保留需要的列，重命名
            df = df[["ts_code", "trade_date", "adj_factor"]].copy()
            df.columns = ["ticker", "trade_date", "adj_factor"]
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            return df
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(1)
                continue
            log(f"  ⚠️ {ticker} 拉取失败({attempt+1}次): {e}")
            return None


def deduplicate_changes(df: pd.DataFrame) -> pd.DataFrame:
    """去重：只保留 adj_factor 发生变化的行（含第一行）"""
    if df is None or len(df) == 0:
        return df
    df = df.sort_values("trade_date").reset_index(drop=True)
    # 标记变化点
    mask = df["adj_factor"] != df["adj_factor"].shift(1)
    mask.iloc[0] = True  # 第一行总是保留
    return df[mask].reset_index(drop=True)


def store_changes(db: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    """写入变化点到 DuckDB"""
    if df is None or len(df) == 0:
        return 0
    
    # 创建表（如果不存在）
    db.execute("""
        CREATE TABLE IF NOT EXISTS adj_factor_changes (
            ticker     VARCHAR,
            trade_date DATE,
            adj_factor DOUBLE,
            PRIMARY KEY (ticker, trade_date)
        )
    """)
    
    # 逐行写入（用 INSERT OR REPLACE 避免主键冲突）
    inserted = 0
    for _, row in df.iterrows():
        try:
            db.execute("""
                INSERT OR REPLACE INTO adj_factor_changes (ticker, trade_date, adj_factor)
                VALUES (?, ?, ?)
            """, [row["ticker"], row["trade_date"], row["adj_factor"]])
            inserted += 1
        except Exception as e:
            log(f"  ⚠️ 写入失败 {row['ticker']} {row['trade_date']}: {e}")
    return inserted


def main(resume: bool = False):
    t0 = time.time()
    log("=" * 50)
    log("开始拉取复权因子")
    log(f"{'断点续传模式' if resume else '全量模式'}")

    # 连接数据库
    db = duckdb.connect(str(DB_PATH))
    pro = get_tushare_pro_client()

    # 获取股票列表
    all_tickers = get_all_tickers(db)
    log(f"共 {len(all_tickers)} 只股票")

    # 加载已完成列表
    done = load_progress() if resume else set()
    if done:
        log(f"已有 {len(done)} 只完成，继续拉取剩余 {len(all_tickers) - len(done)} 只")

    # 逐只拉取
    total_changes = 0
    total_api_calls = 0
    errors = []

    for i, ticker in enumerate(all_tickers):
        if ticker in done:
            continue

        # API 调用
        df = pull_stock_adj(pro, ticker)
        total_api_calls += 1

        if df is None:
            errors.append(ticker)
            save_progress(ticker, done)
            continue

        # 去重 → 仅存变化点
        changes = deduplicate_changes(df)
        n = store_changes(db, changes)
        total_changes += n

        save_progress(ticker, done)

        # 进度日志
        if (i + 1) % 100 == 0 or i == 0 or i == len(all_tickers) - 1:
            pct = (i + 1) / len(all_tickers) * 100
            elapsed = time.time() - t0
            rate = (i + 1 - len(done)) / elapsed if elapsed > 0 else 0
            eta = (len(all_tickers) - i - 1) / rate if rate > 0 else 0
            log(f"[{i+1}/{len(all_tickers)}] {pct:.0f}% | 已存 {total_changes} 条 | "
                f"速率 {rate:.1f}只/秒 | 预计剩余 {eta:.0f}s | "
                f"失败 {len(errors)} 只")

        # 礼貌间隔
        time.sleep(BATCH_INTERVAL)

    # 完成统计
    elapsed = time.time() - t0
    log(f"\n✅ 完成!")
    log(f"  耗时: {elapsed:.0f}s ({elapsed/60:.1f}min)")
    log(f"  API 调用: {total_api_calls} 次")
    log(f"  存入变化点: {total_changes} 行")
    log(f"  失败: {len(errors)} 只")

    if errors:
        log(f"  失败股票: {', '.join(errors[:10])}{'...' if len(errors)>10 else ''}")

    # 验证
    count = db.execute("SELECT COUNT(*) FROM adj_factor_changes").fetchone()[0]
    stocks = db.execute("SELECT COUNT(DISTINCT ticker) FROM adj_factor_changes").fetchone()[0]
    log(f"  数据库验证: {count} 行, {stocks} 只股票")

    db.close()
    # 清理进度文件
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()


if __name__ == "__main__":
    resume = "--resume" in sys.argv
    main(resume=resume)
