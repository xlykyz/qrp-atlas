import os
import time
import random
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd

# ============================================================
# 代理处理：强制禁用 requests 从环境继承代理（不影响你浏览器继续用 Clash）
# ============================================================
os.environ["NO_PROXY"] = "*"
for key in [
    "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
    "ALL_PROXY", "all_proxy",
]:
    os.environ.pop(key, None)

# ============================================================
# 配置区
# ============================================================

START_DATE = pd.Timestamp("2013-01-01")

BASE_DIR = Path(__file__).resolve().parent.parent
SAVE_DIR = BASE_DIR / "data" / "history"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# 你指定的限速：0.5s + (0~0.5s随机)
BASE_SLEEP = 0.5
RANDOM_SLEEP_MIN = 0.0
RANDOM_SLEEP_MAX = 0.5

# 重试次数（东方财富接口偶发波动时很有用）
RETRY = 3


# ============================================================
# 获取股票列表（不做过滤：全部A股/北交所等按源返回）
# ============================================================
def get_stock_list():
    print("获取股票列表...")
    df = ak.stock_zh_a_spot_em()
    tickers = df["代码"].astype(str).tolist()
    print(f"股票数量: {len(tickers)}")
    return tickers


# ============================================================
# 获取单股票历史数据（2013年至今，不复权，东方财富链路）
# ============================================================
def fetch_one_stock(ticker: str):
    last_err = None

    for i in range(RETRY):
        try:
            df = ak.stock_zh_a_hist(
                symbol=ticker,
                period="daily",
                start_date="20260218",
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust="",  # 不复权
            )
            last_err = None
            break
        except Exception as e:
            last_err = e
            # 简单退避
            time.sleep(0.8 + 0.4 * i)

    if last_err is not None:
        print(f"{ticker} 请求失败: {last_err}")
        return None

    if df is None or df.empty:
        print(f"{ticker} 无数据")
        return None

    # 东方财富常见列：日期/开盘/收盘/最高/最低/成交量/成交额/换手率/总市值/流通市值 等
    colmap = {
        "日期": "trade_date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "amount",
        "换手率": "turnover",
        "总市值": "market_cap",
        "流通市值": "float_cap",
    }
    df = df.rename(columns={k: v for k, v in colmap.items() if k in df.columns})

    if "trade_date" not in df.columns:
        print(f"{ticker} 字段异常: {df.columns.tolist()}")
        return None

    # 标准化日期
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df = df.dropna(subset=["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)
    df = df[df["trade_date"] >= START_DATE]

    if df.empty:
        print(f"{ticker} 2013年后无数据")
        return None

    # 加 ticker
    df["ticker"] = ticker

    # 缺失列补齐（东财不保证每只票都有市值/换手率）
    for c in ["amount", "turnover", "market_cap", "float_cap"]:
        if c not in df.columns:
            df[c] = pd.NA

    # pre_close / pct_change
    df["pre_close"] = df["close"].shift(1)
    df = df.dropna(subset=["pre_close"])
    df["pct_change"] = (df["close"] - df["pre_close"]) / df["pre_close"] * 100

    # 调整字段顺序（沿用你原脚本 schema）
    df = df[
        [
            "trade_date",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "pct_change",
            "volume",
            "amount",
            "turnover",
            "market_cap",
            "float_cap",
        ]
    ]

    return df


# ============================================================
# 保存CSV（沿用你原脚本逻辑：单票一个文件，utf-8-sig）
# ============================================================
def save_csv(df: pd.DataFrame, ticker: str):
    file_path = SAVE_DIR / f"{ticker}.csv"
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"保存成功: {ticker}")


# ============================================================
# 主程序（断点续跑：存在即跳过）
# ============================================================
def main():
    print("========================================")
    print("A股历史数据抓取 (2013年至今) - 东方财富链路 -> CSV")
    print("========================================")

    tickers = get_stock_list()
    total = len(tickers)

    success = 0
    skipped = 0
    failed = 0

    start_time = time.time()

    for i, ticker in enumerate(tickers, 1):
        print(f"[{i}/{total}] 处理 {ticker}")

        file_path = SAVE_DIR / f"{ticker}.csv"

        # 断点续跑
        if file_path.exists():
            print("已存在，跳过")
            skipped += 1
            continue

        df = fetch_one_stock(ticker)

        if df is not None:
            save_csv(df, ticker)
            success += 1
        else:
            failed += 1

        # 你指定的限速：0.5s + (0~0.5s随机)
        sleep_time = BASE_SLEEP + random.uniform(RANDOM_SLEEP_MIN, RANDOM_SLEEP_MAX)
        time.sleep(sleep_time)

    elapsed = time.time() - start_time

    print("========================================")
    print("完成")
    print(f"成功: {success}")
    print(f"跳过: {skipped}")
    print(f"失败: {failed}")
    print(f"耗时: {elapsed/60:.1f} 分钟")
    print("保存目录:", str(SAVE_DIR))
    print("========================================")


if __name__ == "__main__":
    main()