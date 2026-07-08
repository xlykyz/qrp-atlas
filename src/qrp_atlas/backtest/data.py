"""
data.py - 项目数据库适配层

把 qrp-atlas 项目中的 daily_market_snapshot / index_daily 表读取并标准化成
通用 PriceFrame。本模块不属于引擎核心，引擎不依赖它。

调用方式:
- 优先复用项目 DuckDB 连接: load_stock_prices()（无参则用 qrp_atlas.api.db.get_db）
- 外部传入连接: load_stock_prices(con=duckdb_con)
- 外部传入 db_path: load_stock_prices(db_path="path/to/quant.db")

不写数据库，不做策略逻辑。
"""

from datetime import date, datetime
from typing import Any, Iterable, Optional, Tuple

import pandas as pd

PRICE_OPTIONAL_DB_COLUMNS: tuple[str, ...] = (
    "volume",
    "amount",
    "turnover",
    "market_cap",
    "float_cap",
    "is_st",
    "is_limit_up",
    "is_limit_down",
)


def normalize_price_frame(
    df: pd.DataFrame,
    *,
    asset_type: str,
    id_col: str,
    name_col: str,
) -> pd.DataFrame:
    """把外部读取的行情表标准化成 PriceFrame 字段格式。

    只做字段映射、类型转换、排序、必要列检查。不做策略逻辑，不写数据库。

    Args:
        df: 原始行情 DataFrame，必须包含 trade_date / id_col / name_col / OHLC。
        asset_type: 资产类型标签，如 "stock" / "index"。
        id_col: 原 DataFrame 中作为 asset_id 的列名。
        name_col: 原 DataFrame 中作为 asset_name 的列名。

    Returns:
        PriceFrame，包含 trade_date / asset_id / asset_name / asset_type /
        open / high / low / close 及可选字段，按 (asset_id, trade_date) 升序。
    """
    if df is None or len(df) == 0:
        cols = [
            "trade_date",
            "asset_id",
            "asset_name",
            "asset_type",
            "open",
            "high",
            "low",
            "close",
        ]
        return pd.DataFrame(columns=cols)

    required = ["trade_date", id_col, name_col, "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"normalize_price_frame missing required columns: {missing}")

    out = pd.DataFrame()
    out["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce", format="mixed")
    out["asset_id"] = df[id_col].astype(str)
    out["asset_name"] = df[name_col].astype(str)
    out["asset_type"] = asset_type
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            out[col] = pd.NA
    for col in PRICE_OPTIONAL_DB_COLUMNS:
        if col in df.columns:
            out[col] = df[col]

    out = out.dropna(subset=["trade_date", "asset_id"])
    out = out.sort_values(["asset_id", "trade_date"]).reset_index(drop=True)
    return out


def _normalize_date(value: Any) -> Any:
    """把日期输入规整成 DuckDB 可识别的形式（YYYY-MM-DD 字符串或原值）。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        return None
    if "-" in text:
        return text
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _resolve_con(
    con: Any = None,
    db_path: Any = None,
) -> Tuple[Any, bool]:
    """获取 DuckDB 连接。

    优先级: con > db_path > 项目默认 get_db(read_only=True)。

    Returns:
        (con, should_close) - should_close 表示使用完是否需要 close。
    """
    if con is not None:
        return con, False
    if db_path is not None:
        import duckdb

        return duckdb.connect(str(db_path), read_only=True), True
    from qrp_atlas.api.db import get_db

    return get_db(read_only=True), True


def _build_where(
    *,
    column_values: Optional[Iterable[Any]] = None,
    column_name: str,
    start_date: Any,
    end_date: Any,
) -> Tuple[str, list]:
    """构造 WHERE 子句片段和参数。"""
    clauses: list[str] = []
    params: list = []
    if column_values is not None:
        values = list(column_values)
        if values:
            placeholders = ", ".join("?" * len(values))
            clauses.append(f"{column_name} IN ({placeholders})")
            params.extend(str(v) for v in values)
    if start_date is not None:
        clauses.append("trade_date >= ?")
        params.append(_normalize_date(start_date))
    if end_date is not None:
        clauses.append("trade_date <= ?")
        params.append(_normalize_date(end_date))
    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where_sql, params


def load_stock_prices(
    *,
    con: Any = None,
    db_path: Any = None,
    tickers: Optional[Iterable[str]] = None,
    start_date: Any = None,
    end_date: Any = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """从 daily_market_snapshot 读取股票行情，标准化成 PriceFrame。

    映射: ticker→asset_id, name→asset_name, "stock"→asset_type。

    Args:
        con: 可选的 DuckDB 连接。不传则按 db_path 或项目默认连接。
        db_path: 可选的 DuckDB 文件路径。
        tickers: 可选的 ticker 过滤列表。
        start_date: 起始日期（含），支持 "YYYY-MM-DD" / "YYYYMMDD" / date。
        end_date: 截止日期（含）。
        limit: 最大行数。

    Returns:
        PriceFrame。
    """
    own_con, should_close = _resolve_con(con, db_path)
    try:
        sql = (
            "SELECT trade_date, ticker, name, open, high, low, close, "
            "volume, amount, turnover, market_cap, float_cap, "
            "is_st, is_limit_up, is_limit_down "
            "FROM daily_market_snapshot"
        )
        where_sql, params = _build_where(
            column_values=tickers,
            column_name="ticker",
            start_date=start_date,
            end_date=end_date,
        )
        sql = sql + where_sql + " ORDER BY ticker, trade_date"
        if limit is not None:
            sql = sql + f" LIMIT {int(limit)}"
        df = own_con.execute(sql, params).df()
    finally:
        if should_close:
            own_con.close()

    return normalize_price_frame(
        df, asset_type="stock", id_col="ticker", name_col="name"
    )


def load_index_prices(
    *,
    con: Any = None,
    db_path: Any = None,
    codes: Optional[Iterable[str]] = None,
    start_date: Any = None,
    end_date: Any = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """从 index_daily 读取指数行情，标准化成 PriceFrame。

    映射: index_code→asset_id, index_name→asset_name, "index"→asset_type。

    Args:
        con: 可选的 DuckDB 连接。
        db_path: 可选的 DuckDB 文件路径。
        codes: 可选的指数代码过滤列表。
        start_date: 起始日期（含）。
        end_date: 截止日期（含）。
        limit: 最大行数。

    Returns:
        PriceFrame。
    """
    own_con, should_close = _resolve_con(con, db_path)
    try:
        sql = (
            "SELECT trade_date, index_code, index_name, "
            "open, high, low, close, volume "
            "FROM index_daily"
        )
        where_sql, params = _build_where(
            column_values=codes,
            column_name="index_code",
            start_date=start_date,
            end_date=end_date,
        )
        sql = sql + where_sql + " ORDER BY index_code, trade_date"
        if limit is not None:
            sql = sql + f" LIMIT {int(limit)}"
        df = own_con.execute(sql, params).df()
    finally:
        if should_close:
            own_con.close()

    return normalize_price_frame(
        df, asset_type="index", id_col="index_code", name_col="index_name"
    )


__all__ = [
    "normalize_price_frame",
    "load_stock_prices",
    "load_index_prices",
]
