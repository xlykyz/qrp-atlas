"""Streamlit 入口(本地可视化)"""
import streamlit as st
import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH
from qrp_atlas.contracts import get_table


st.set_page_config(page_title="QRP Atlas", layout="wide")
st.title("QRP Atlas")


@st.cache_data
def load_latest_trade_date() -> str:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        result = con.execute("SELECT MAX(trade_date) FROM daily_market_snapshot").fetchone()
        return result[0] if result and result[0] else "暂无数据"
    finally:
        con.close()


@st.cache_data
def load_market_snapshot(trade_date: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(
            "SELECT * FROM daily_market_snapshot WHERE trade_date = ? ORDER BY ticker",
            [trade_date]
        ).fetchdf()
    finally:
        con.close()


latest_date = load_latest_trade_date()
st.caption(f"最新交易日: {latest_date}")

if latest_date != "暂无数据":
    df = load_market_snapshot(latest_date)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("股票总数", len(df))
    col2.metric("上涨", len(df[df["pct_change"] > 0]) if "pct_change" in df.columns else "-")
    col3.metric("下跌", len(df[df["pct_change"] < 0]) if "pct_change" in df.columns else "-")
    col4.metric("涨停", len(df[df["is_limit_up"] == True]) if "is_limit_up" in df.columns else "-")

    st.dataframe(df, use_container_width=True, hide_index=True)
