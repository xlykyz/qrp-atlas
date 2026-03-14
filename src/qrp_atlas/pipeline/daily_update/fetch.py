"""
fetch.py - 获取 A 股实时行情原始数据

数据源优先级:
1. tushare daily 接口 (首选) - 需要 token，获取指定交易日数据
2. 新浪财经接口 (次选) - akshare.stock_zh_a_spot
3. 东方财富接口 (第三) - akshare.stock_zh_a_spot_em

使用示例:
    from qrp_atlas.pipeline.daily_update.fetch import fetch_current_snapshot

    df, source = fetch_current_snapshot()
    print(df.head())

注意事项:
    - tushare 需要配置 TUSHARE_TOKEN 环境变量
    - 新浪接口有频率限制，重复调用可能被封 IP
    - 数据有 15 分钟延迟
    - 成交量单位: 股
    - 成交额单位: 元
"""

import time
from datetime import date, datetime, time as dt_time
from zoneinfo import ZoneInfo
from typing import Literal

import akshare as ak
import pandas as pd
import tushare as ts

from qrp_atlas.config import DAILY_SNAPSHOT_RAW_DIR, TUSHARE_TOKEN

CHINA_TZ = ZoneInfo("Asia/Shanghai")

DataSource = Literal["tushare", "sina", "em"]


def get_latest_trade_date() -> date:
    """获取最近一个交易日

    如果今天是交易日且已收盘，返回今天；
    如果今天是交易日但未收盘，返回上一个交易日；
    如果今天不是交易日，返回最近的交易日。

    Returns:
        最近的交易日期
    """
    now = datetime.now(CHINA_TZ)
    today = now.date()

    trade_dates_df = ak.tool_trade_date_hist_sina()
    trade_dates_df["trade_date"] = pd.to_datetime(trade_dates_df["trade_date"])
    trade_dates = set(trade_dates_df["trade_date"].dt.date)

    if today not in trade_dates:
        past_dates = [d for d in trade_dates if d < today]
        return max(past_dates)

    market_close_time = dt_time(15, 0)
    if now.time() < market_close_time:
        past_dates = [d for d in trade_dates if d < today]
        return max(past_dates)

    return today


def _fetch_from_tushare(trade_date: date) -> pd.DataFrame:
    """从 tushare 获取指定交易日数据

    Args:
        trade_date: 交易日期

    Returns:
        DataFrame，包含当日所有 A 股行情数据
    """
    if not TUSHARE_TOKEN:
        raise ValueError("TUSHARE_TOKEN 未配置，请设置环境变量 TUSHARE_TOKEN")

    pro = ts.pro_api(TUSHARE_TOKEN)
    date_str = trade_date.strftime("%Y%m%d")
    df = pro.daily(trade_date=date_str)
    return df


def _fetch_from_sina() -> pd.DataFrame:
    """从新浪接口获取原始数据"""
    return ak.stock_zh_a_spot()


def _fetch_from_eastmoney() -> pd.DataFrame:
    """从东方财富接口获取原始数据"""
    return ak.stock_zh_a_spot_em()


def fetch_current_snapshot(
    trade_date: date = None,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> tuple[pd.DataFrame, DataSource]:
    """获取当前 A 股市场快照原始数据

    数据源优先级: tushare > 新浪 > 东方财富

    Args:
        trade_date: 目标交易日，默认自动获取最近交易日
        max_retries: 最大重试次数，默认 3 次
        retry_delay: 重试间隔秒数，默认 2.0 秒

    Returns:
        (DataFrame, source): 原始数据和数据源标识
            - source: "tushare" | "sina" | "em"

    Raises:
        Exception: 所有数据源获取失败时抛出

    Example:
        df, source = fetch_current_snapshot()
        print(f"获取到 {len(df)} 只股票数据，来源: {source}")
    """
    if trade_date is None:
        trade_date = get_latest_trade_date()

    last_error = None

    if TUSHARE_TOKEN:
        for attempt in range(max_retries):
            try:
                print(f"[FETCH] 尝试从 tushare 获取 {trade_date} 数据 (尝试 {attempt + 1}/{max_retries})...")
                df = _fetch_from_tushare(trade_date)
                if df is not None and len(df) > 0:
                    print(f"[FETCH] tushare 成功，获取 {len(df)} 条数据")
                    return df, "tushare"
                else:
                    print(f"[FETCH] tushare 返回空数据")
            except Exception as e:
                last_error = e
                print(f"[FETCH] tushare 失败: {e}")
                if attempt < max_retries - 1:
                    print(f"[FETCH] 等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
    else:
        print("[FETCH] TUSHARE_TOKEN 未配置，跳过 tushare 数据源")

    for attempt in range(max_retries):
        try:
            print(f"[FETCH] 尝试从新浪接口获取数据 (尝试 {attempt + 1}/{max_retries})...")
            df = _fetch_from_sina()
            print(f"[FETCH] 新浪接口成功，获取 {len(df)} 条数据")
            return df, "sina"
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                print(f"[FETCH] 新浪接口失败: {e}")
                print(f"[FETCH] 等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)

    print("[FETCH] 新浪接口失败，尝试东方财富接口...")
    try:
        df = _fetch_from_eastmoney()
        print(f"[FETCH] 东方财富接口成功，获取 {len(df)} 条数据")
        return df, "em"
    except Exception as e:
        raise Exception(f"所有数据源获取失败。tushare/新浪: {last_error}, 东财: {e}")


def save_raw_snapshot(
    df: pd.DataFrame,
    source: DataSource,
    trade_date: date = None,
) -> str:
    """保存原始快照数据到 CSV 文件

    文件路径: data/raw/daily_snapshot/{年份}/{YYYY-MM-DD}_Astock_{source}.csv
    如果文件已存在则覆盖。

    Args:
        df: 原始数据 DataFrame
        source: 数据源标识 ("tushare" | "sina" | "em")
        trade_date: 交易日期，默认自动获取最近交易日

    Returns:
        保存的文件路径

    Example:
        df, source = fetch_current_snapshot()
        path = save_raw_snapshot(df, source)
        print(f"数据已保存到: {path}")
    """
    if trade_date is None:
        trade_date = get_latest_trade_date()

    year_dir = DAILY_SNAPSHOT_RAW_DIR / str(trade_date.year)
    year_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"{trade_date.isoformat()}_Astock_{source}.csv"
    file_path = year_dir / file_name

    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"[FETCH] 原始数据已保存: {file_path}")

    return str(file_path)


def main() -> None:
    """主函数：获取并保存原始数据"""
    trade_date = get_latest_trade_date()
    print(f"[FETCH] 目标交易日: {trade_date}")

    df, source = fetch_current_snapshot(trade_date)
    print(f"获取到 {len(df)} 只股票数据，来源: {source}")
    print(f"字段: {list(df.columns)}")
    print(df.head(10))

    path = save_raw_snapshot(df, source, trade_date)
    print(f"数据已保存到: {path}")


if __name__ == "__main__":
    main()
