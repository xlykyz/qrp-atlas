"""当日全市场概览"""
import streamlit as st
import duckdb
import pandas as pd

from qrp_atlas.config import DB_PATH


@st.cache_data(ttl="10m")
def load_snapshot(trade_date: str = None) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        if trade_date:
            df = con.execute(
                "SELECT * FROM daily_market_snapshot WHERE trade_date = ? ORDER BY ticker",
                [trade_date]
            ).fetchdf()
        else:
            df = con.execute(
                "SELECT * FROM daily_market_snapshot ORDER BY trade_date DESC, ticker"
            ).fetchdf()
        return df
    finally:
        con.close()


@st.cache_data(ttl="10m")
def load_trade_dates() -> list:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        result = con.execute(
            "SELECT DISTINCT trade_date FROM daily_market_snapshot ORDER BY trade_date DESC"
        ).fetchall()
        return [r[0] for r in result]
    finally:
        con.close()


st.set_page_config(page_title="市场概览 - QRP Atlas", layout="wide")
st.title("市场概览")

trade_dates = load_trade_dates()
if not trade_dates:
    st.info("数据库中暂无数据，请先运行 daily_update 管道获取数据。")
    st.stop()

selected_date = st.selectbox("选择交易日", trade_dates, index=0)
df = load_snapshot(selected_date)

col1, col2, col3, col4, col5 = st.columns(5)
total = len(df)
up_count = len(df[df["pct_change"] > 0])
down_count = len(df[df["pct_change"] < 0])
limit_up = len(df[df["is_limit_up"] == True])
limit_down = len(df[df["is_limit_down"] == True])

col1.metric("股票总数", total)
col2.metric("上涨", up_count, f"{up_count/total*100:.1f}%" if total else "-")
col3.metric("下跌", down_count, f"{down_count/total*100:.1f}%" if total else "-")
col4.metric("涨停", limit_up)
col5.metric("跌停", limit_down)

sort_col = st.selectbox(
    "排序方式",
    ["pct_change", "amount", "turnover", "volume", "market_cap"],
    format_func=lambda x: {"pct_change": "涨跌幅", "amount": "成交额", "turnover": "换手率", "volume": "成交量", "market_cap": "总市值"}.get(x, x)
)
ascending = st.checkbox("升序", value=False)

st.caption(f"共 {total} 只股票")
st.dataframe(
    df.sort_values(sort_col, ascending=ascending),
    column_config={
        "trade_date": "日期",
        "ticker": "代码",
        "name": "名称",
        "open": st.column_config.NumberColumn("开盘", format="%.2f"),
        "close": st.column_config.NumberColumn("收盘", format="%.2f"),
        "high": st.column_config.NumberColumn("最高", format="%.2f"),
        "low": st.column_config.NumberColumn("最低", format="%.2f"),
        "pct_change": st.column_config.NumberColumn("涨跌幅%", format="%.2f"),
        "volume": st.column_config.NumberColumn("成交量", format="%d"),
        "amount": st.column_config.NumberColumn("成交额", format="%.2f"),
        "turnover": st.column_config.NumberColumn("换手率%", format="%.2f"),
        "market_cap": st.column_config.NumberColumn("总市值", format="%.2f"),
        "float_cap": st.column_config.NumberColumn("流通市值", format="%.2f"),
        "is_limit_up": "涨停",
        "is_limit_down": "跌停",
        "is_st": "ST",
    },
    use_container_width=True,
    hide_index=True,
)
