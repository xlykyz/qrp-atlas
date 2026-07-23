import akshare as ak
import pandas as pd
import time
import random
from datetime import datetime

# ============================================================
# 配置区
# ============================================================

START_DATE = "2013-01-01"

from qrp_atlas.config import DATA_DIR
from qrp_atlas.config.settings import apply_proxy_environment

apply_proxy_environment(clear_unconfigured=True, default_no_proxy="*")

SAVE_DIR = DATA_DIR / "history"

SAVE_DIR.mkdir(parents=True, exist_ok=True)

# 限速参数（安全）
BASE_SLEEP = 0.9
RANDOM_SLEEP_MIN = 0.0
RANDOM_SLEEP_MAX = 0.4

# ============================================================
# 获取股票列表
# ============================================================

def get_stock_list():

    print("获取股票列表...")

    df = ak.stock_zh_a_spot_em()

    tickers = df["代码"].tolist()

    print(f"股票数量: {len(tickers)}")

    return tickers


# ============================================================
# 获取单股票历史数据（2013年至今，不复权）
# ============================================================

def fetch_one_stock(ticker):

    try:

        df = ak.stock_zh_a_daily(
            symbol=ticker,
            adjust="",
            start_date="20130101",
            end_date=datetime.now().strftime("%Y%m%d")
        )

    except Exception as e:

        print(f"{ticker} 请求失败: {e}")

        return None

    if df is None or df.empty:

        print(f"{ticker} 无数据")

        return None

    df = df.reset_index()

    df.rename(columns={

        "date": "trade_date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "amount": "amount",
        "turnover": "turnover",
        "total_mv": "market_cap",
        "circ_mv": "float_cap"

    }, inplace=True)

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    df = df.sort_values("trade_date").reset_index(drop=True)

    df = df[df["trade_date"] >= START_DATE]

    if df.empty:

        print(f"{ticker} 2013年后无数据")

        return None

    # 添加ticker
    df["ticker"] = ticker

    # pre_close
    df["pre_close"] = df["close"].shift(1)

    # pct_change
    df["pct_change"] = (
        (df["close"] - df["pre_close"])
        / df["pre_close"] * 100
    )

    # 调整字段顺序
    df = df[[
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
        "float_cap"
    ]]

    # 删除第一行（pre_close为空）
    df = df.dropna(subset=["pre_close"])

    return df


# ============================================================
# 保存CSV
# ============================================================

def save_csv(df, ticker):

    file_path = SAVE_DIR / f"{ticker}.csv"

    df.to_csv(

        file_path,
        index=False,
        encoding="utf-8-sig"

    )

    print(f"保存成功: {ticker}")


# ============================================================
# 主程序
# ============================================================

def main():

    print("========================================")
    print("A股历史数据抓取 (2013年至今)")
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

        # 限速控制（防封IP）
        sleep_time = BASE_SLEEP + random.uniform(
            RANDOM_SLEEP_MIN,
            RANDOM_SLEEP_MAX
        )

        time.sleep(sleep_time)

    elapsed = time.time() - start_time

    print("========================================")
    print("完成")
    print(f"成功: {success}")
    print(f"跳过: {skipped}")
    print(f"失败: {failed}")
    print(f"耗时: {elapsed/60:.1f} 分钟")
    print("========================================")


# ============================================================

if __name__ == "__main__":

    main()