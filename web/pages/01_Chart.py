"""个股K线 + 均线"""
import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from qrp_atlas.config import DB_PATH


@st.cache_data(ttl="10m")
def load_tickers() -> list:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        result = con.execute(
            "SELECT DISTINCT ticker, name FROM daily_market_snapshot ORDER BY ticker"
        ).fetchall()
        return [(r[0], r[1]) for r in result]
    finally:
        con.close()


@st.cache_data(ttl="10m")
def load_stock_data(ticker: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        df = con.execute(
            "SELECT * FROM daily_market_snapshot WHERE ticker = ? ORDER BY trade_date",
            [ticker]
        ).fetchdf()
        return df
    finally:
        con.close()


st.set_page_config(page_title="个股图表 - QRP Atlas", layout="wide")
st.title("个股图表")

tickers = load_tickers()
if not tickers:
    st.info("数据库中暂无数据，请先运行 daily_update 管道获取数据。")
    st.stop()

ticker_options = {f"{t[0]} - {t[1]}" if t[1] else t[0]: t[0] for t in tickers}
selected_label = st.selectbox("选择股票", list(ticker_options.keys()))
ticker = ticker_options[selected_label]

df = load_stock_data(ticker)
if df.empty:
    st.warning(f"未找到 {ticker} 的数据")
    st.stop()

ma_periods = st.multiselect(
    "均线周期",
    [5, 10, 20, 30, 60, 120, 250],
    default=[5, 20, 60]
)

for ma in ma_periods:
    col_name = f"MA{ma}"
    df[col_name] = df["close"].rolling(window=ma).mean()

fig = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.05,
    row_heights=[0.6, 0.2, 0.2],
)

fig.add_trace(
    go.Candlestick(
        x=df["trade_date"],
        open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        name="K线"
    ),
    row=1, col=1
)

colors = {5: "#FF6B6B", 10: "#4ECDC4", 20: "#45B7D1", 30: "#96CEB4",
          60: "#FFEAA7", 120: "#DDA0DD", 250: "#98D8C8"}
for ma in ma_periods:
    col_name = f"MA{ma}"
    fig.add_trace(
        go.Scatter(
            x=df["trade_date"], y=df[col_name],
            mode="lines", name=f"MA{ma}",
            line=dict(color=colors.get(ma, "#CCCCCC"), width=1.5)
        ),
        row=1, col=1
    )

fig.add_trace(
    go.Bar(x=df["trade_date"], y=df["volume"], name="成交量", marker_color="rgba(100,149,237,0.5)"),
    row=2, col=1
)

fig.add_trace(
    go.Scatter(
        x=df["trade_date"], y=df["pct_change"],
        mode="lines+markers", name="涨跌幅",
        line=dict(color="#FF8C00", width=1),
        marker=dict(size=3, color=df["pct_change"].apply(lambda x: "#FF4444" if x > 0 else "#44BB44" if x < 0 else "#888888"))
    ),
    row=3, col=1
)

fig.update_layout(
    height=800,
    xaxis3_title="日期",
    yaxis_title="价格",
    yaxis3_title="涨跌幅%",
    hovermode="x unified",
    showlegend=True,
    margin=dict(l=40, r=40, t=20, b=40),
)
fig.update_xaxes(rangeslider=dict(visible=False))

st.plotly_chart(fig, use_container_width=True)

col1, col2, col3 = st.columns(3)
col1.metric("最新收盘", f"{df['close'].iloc[-1]:.2f}", f"{df['pct_change'].iloc[-1]:.2f}%" if "pct_change" in df.columns else None)
col2.metric("区间最高", f"{df['high'].max():.2f}")
col3.metric("区间最低", f"{df['low'].min():.2f}")

with st.expander("查看数据表"):
    st.dataframe(
        df.drop(columns=[c for c in df.columns if c.startswith("MA")], errors="ignore"),
        column_config={
            "trade_date": "日期",
            "ticker": "代码",
            "open": st.column_config.NumberColumn("开盘", format="%.2f"),
            "close": st.column_config.NumberColumn("收盘", format="%.2f"),
            "high": st.column_config.NumberColumn("最高", format="%.2f"),
            "low": st.column_config.NumberColumn("最低", format="%.2f"),
            "volume": st.column_config.NumberColumn("成交量", format="%d"),
            "pct_change": st.column_config.NumberColumn("涨跌幅%", format="%.2f"),
        },
        use_container_width=True,
        hide_index=True,
    )
