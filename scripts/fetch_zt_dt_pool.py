"""抓取东财涨停/跌停股池数据并入库"""

import json
import urllib.request
from datetime import date, datetime

import pandas as pd

from qrp_atlas.contracts.fields import INDUSTRY_NAME
from qrp_atlas.contracts import (
    TRADE_DATE, TICKER, NAME, CLOSE, PCT_CHANGE,
    AMOUNT, FLOAT_CAP, TURNOVER, CREATED_AT,
    FIRST_BLOCK_TIME, CONSECUTIVE_BOARDS, BLOCK_FUND,
    CONSECUTIVE_DAYS, OPEN_COUNT,
)
from qrp_atlas.pipeline.duckdb_store import save_zt_pool, save_dt_pool

# ── API 配置 ──
UT = "7eea3edcaed734bea9cbfc24409ed989"
DPT = "wz.ztzt"
BASE = "http://push2ex.eastmoney.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/ztb/detail",
}


def _fetch_pool(endpoint: str, pagesize: int = 200) -> list:
    """拉取东财股池数据"""
    params = (
        f"ut={UT}&dpt={DPT}&Pageindex=0&pagesize={pagesize}"
        f"&sort=fbt:asc&date={datetime.now().strftime('%Y%m%d')}"
    )
    url = f"{BASE}/{endpoint}?{params}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read().decode("utf-8")
    # 去掉 jQuery 包裹
    if raw.startswith("jQuery"):
        raw = raw[raw.index("{"):raw.rindex("}") + 1]
    d = json.loads(raw)
    if d.get("rc") != 0:
        raise RuntimeError(f"API 返回错误 rc={d.get('rc')}: {d}")
    return d["data"].get("pool", [])


def _normalize_price(val) -> float:
    """东财 price 按千分位转成元"""
    if val is None:
        return None
    return float(val) / 1000


def _normalize_fbt(val) -> str:
    """封板时间转成 HH:MM:SS 格式"""
    if val is None:
        return None
    s = str(int(val)).zfill(6)
    return f"{s[:2]}:{s[2:4]}:{s[4:]}"


def fetch_zt_pool(trade_date: date = None) -> pd.DataFrame:
    """抓取涨停股池并入库

    Args:
        trade_date: 交易日，默认当天
    """
    if trade_date is None:
        trade_date = date.today()

    pool = _fetch_pool("getTopicZTPool")
    records = []
    now = datetime.now()
    for s in pool:
        records.append({
            TRADE_DATE: trade_date,
            TICKER: s.get("c", ""),
            NAME: s.get("n", ""),
            CLOSE: _normalize_price(s.get("p")),
            PCT_CHANGE: s.get("zdp"),
            AMOUNT: s.get("amount"),
            FLOAT_CAP: s.get("ltsz"),
            TURNOVER: s.get("hs"),
            FIRST_BLOCK_TIME: _normalize_fbt(s.get("fbt")),
            CONSECUTIVE_BOARDS: s.get("lbc", 0),
            BLOCK_FUND: s.get("fund"),
            INDUSTRY_NAME: s.get("hybk", ""),
            CREATED_AT: now,
        })

    df = pd.DataFrame(records)
    if not df.empty:
        save_zt_pool(df, replace=True)
        print(f"✅ 涨停股池: {len(df)} 条入库")
    else:
        print("⚠️ 涨停股池为空")
    return df


def fetch_dt_pool(trade_date: date = None) -> pd.DataFrame:
    """抓取跌停股池并入库

    Args:
        trade_date: 交易日，默认当天
    """
    if trade_date is None:
        trade_date = date.today()

    pool = _fetch_pool("getTopicDTPool")
    records = []
    now = datetime.now()
    for s in pool:
        records.append({
            TRADE_DATE: trade_date,
            TICKER: s.get("c", ""),
            NAME: s.get("n", ""),
            CLOSE: _normalize_price(s.get("p")),
            PCT_CHANGE: s.get("zdp"),
            AMOUNT: s.get("amount"),
            FLOAT_CAP: s.get("ltsz"),
            TURNOVER: s.get("hs"),
            BLOCK_FUND: s.get("fund"),
            CONSECUTIVE_DAYS: s.get("dtc", 0),
            OPEN_COUNT: s.get("kbc", 0),
            INDUSTRY_NAME: s.get("hybk", ""),
            CREATED_AT: now,
        })

    df = pd.DataFrame(records)
    if not df.empty:
        save_dt_pool(df, replace=True)
        print(f"✅ 跌停股池: {len(df)} 条入库")
    else:
        print("⚠️ 跌停股池为空")
    return df


def fetch_all(trade_date: date = None):
    """抓取涨停+跌停股池"""
    if trade_date is None:
        trade_date = date.today()
    print(f"📅 {trade_date}")
    zt_df = fetch_zt_pool(trade_date)
    dt_df = fetch_dt_pool(trade_date)
    return zt_df, dt_df


if __name__ == "__main__":
    fetch_all()