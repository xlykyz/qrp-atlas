"""
conventions.py - 通用约定

定义日期格式、ticker规则、数值列集合等通用约定。
全项目统一使用这些约定。

使用示例:
    from qrp_atlas.contracts import (
        DATE_FORMAT, format_date,
        format_ticker, get_exchange,
        get_limit_pct, calc_limit_up_price
    )

    # 日期格式化
    date_str = format_date("20240101", from_format="%Y%m%d")

    # Ticker 格式化
    ticker = format_ticker("1")  # -> "000001"

    # 获取交易所
    exchange = get_exchange("600000")  # -> "SH"

    # 计算涨停幅度
    limit_pct = get_limit_pct("300750.SZ")  # -> 20.0

约定内容:
    - 日期格式: DATE_FORMAT, DATE_FORMAT_COMPACT, DATETIME_FORMAT
    - Ticker规则: 长度6位, 交易所前缀识别
    - 涨跌停幅度: 主板普通股10%, 主板ST股5%, 上证主板ST自2026-07-07起10%, 科创板/创业板20%, 北交所30%
    - 字段类型集合: NUMERIC_COLUMNS, BOOLEAN_COLUMNS, DATE_COLUMNS
"""

import re
from datetime import date, datetime
from typing import Tuple

import pandas as pd

DATE_FORMAT = "%Y-%m-%d"
DATE_FORMAT_COMPACT = "%Y%m%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

TICKER_PATTERN = r"^\d{6}$"
TICKER_LENGTH = 6

SH_TICKER_PREFIXES = ("60", "68", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59")
SZ_TICKER_PREFIXES = ("00", "30", "12", "15", "16", "17", "18", "19")
BJ_TICKER_PREFIXES = ("43", "83", "87", "88", "92")

LIMIT_UP_PCT = 10.0
LIMIT_UP_ST_PCT = 5.0
LIMIT_DOWN_PCT = -10.0
LIMIT_DOWN_ST_PCT = -5.0
LIMIT_20_PCT = 20.0
LIMIT_BJ_PCT = 30.0
SH_ST_10_PCT_EFFECTIVE_DATE = date(2026, 7, 7)

NUMERIC_DECIMAL_PLACES = 4
VOLUME_UNIT = "股"
AMOUNT_UNIT = "元"
MARKET_CAP_UNIT = "元"

from .fields import (
    OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT,
    PCT_CHANGE, CHANGE, TURNOVER, MARKET_CAP, FLOAT_CAP, PRE_CLOSE,
    ENTRY_PRICE, EXIT_PRICE, HALF_SELL_PRICE,
    HALF_SELL_TRIGGER, POSITION_PCT,
    IS_ST, IS_LIMIT_UP, IS_LIMIT_DOWN,
    M1_CORE, M2_FRONT, M3_IDENTIFIABLE, V_TRIGGERED,
    TRADE_DATE, ENTRY_DATE, EXIT_DATE, HALF_SELL_DATE,
    BASE_DATE, LIST_DATE, EXP_DATE,
    TICKER, TRADE_ID, NAME,
)

NUMERIC_COLUMNS: Tuple[str, ...] = (
    OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT,
    PCT_CHANGE, CHANGE, TURNOVER, MARKET_CAP, FLOAT_CAP, PRE_CLOSE,
    ENTRY_PRICE, EXIT_PRICE, HALF_SELL_PRICE,
    HALF_SELL_TRIGGER, POSITION_PCT,
)

BOOLEAN_COLUMNS: Tuple[str, ...] = (
    IS_ST, IS_LIMIT_UP, IS_LIMIT_DOWN,
    M1_CORE, M2_FRONT, M3_IDENTIFIABLE, V_TRIGGERED,
)

DATE_COLUMNS: Tuple[str, ...] = (
    TRADE_DATE, ENTRY_DATE, EXIT_DATE, HALF_SELL_DATE, BASE_DATE, LIST_DATE, EXP_DATE,
)

STRING_COLUMNS: Tuple[str, ...] = (
    TICKER, TRADE_ID, NAME,
)


def format_ticker(ticker: str) -> str:
    """格式化 Ticker 为标准6位格式

    Args:
        ticker: 原始 ticker 字符串

    Returns:
        6位 ticker 字符串，不足6位前面补0

    Example:
        format_ticker("1")      # -> "000001"
        format_ticker("600000") # -> "600000"
    """
    ticker = str(ticker).strip()
    if len(ticker) < TICKER_LENGTH:
        ticker = ticker.zfill(TICKER_LENGTH)
    return ticker


def format_date(date_str: str, from_format: str = None, to_format: str = DATE_FORMAT) -> str:
    """格式化日期字符串

    Args:
        date_str: 原始日期字符串
        from_format: 原始格式，若为 None 则自动检测
        to_format: 目标格式，默认 "%Y-%m-%d"

    Returns:
        格式化后的日期字符串

    Raises:
        ValueError: 无法解析日期时抛出

    Example:
        format_date("20240101")              # -> "2024-01-01"
        format_date("2024/01/01")            # -> "2024-01-01"
        format_date("20240101", to_format="%Y/%m/%d")  # -> "2024/01/01"
    """
    from datetime import datetime
    if from_format:
        dt = datetime.strptime(date_str, from_format)
    else:
        for fmt in (DATE_FORMAT, DATE_FORMAT_COMPACT, "%Y/%m/%d"):
            try:
                dt = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Cannot parse date: {date_str}")
    return dt.strftime(to_format)


def get_exchange(ticker: str) -> str:
    """根据 Ticker 判断交易所

    Args:
        ticker: 6位股票代码

    Returns:
        交易所代码: "SH"(上海), "SZ"(深圳), "BJ"(北京), "UNKNOWN"(未知)

    Example:
        get_exchange("600000")  # -> "SH"
        get_exchange("000001")  # -> "SZ"
        get_exchange("430001")  # -> "BJ"
    """
    ticker = format_ticker(ticker)
    if ticker.startswith(SH_TICKER_PREFIXES):
        return "SH"
    elif ticker.startswith(SZ_TICKER_PREFIXES):
        return "SZ"
    elif ticker.startswith(BJ_TICKER_PREFIXES):
        return "BJ"
    return "UNKNOWN"


def normalize_ticker(raw: str) -> str:
    """将 ticker 统一为标准格式 6位代码.交易所

    输入: '000001' '000001.SZ' 'bj920000' '600000.SH'
    输出: '000001.SZ' '000001.SZ' '920000.BJ' '600000.SH'

    Args:
        raw: 原始 ticker（可能含 .SZ/.SH/.BJ 后缀，或不带后缀）

    Returns:
        标准化后的 ticker
    """
    raw = str(raw).strip().upper()
    # 去掉后缀
    if "." in raw:
        code = raw.split(".")[0]
    else:
        code = raw
    # 去掉 bj/sh/sz 前缀（新浪北交所数据如 bj920000）
    code = code.lstrip("BJSHSZ")
    code = format_ticker(code)
    exchange = get_exchange(code)
    if exchange == "UNKNOWN":
        return code  # 无法识别时原样返回
    return f"{code}.{exchange}"


def normalize_index_code(raw: str) -> str:
    """将指数代码统一为 Tushare ``ts_code`` 格式。"""

    value = str(raw).strip().upper()
    if re.fullmatch(r"\d{6}\.[A-Z]{2,4}", value):
        return value
    if re.fullmatch(r"[A-Z]{2}\d{6}", value):
        exchange = value[:2]
        if exchange in {"SH", "SZ", "BJ"}:
            return f"{value[2:]}.{exchange}"
    if re.fullmatch(r"\d{6}", value):
        exchange = "SZ" if value.startswith("399") else "SH"
        return f"{value}.{exchange}"
    raise ValueError(f"invalid index code: {raw}")


def is_sh_ticker(ticker: str) -> bool:
    """判断是否为上海交易所股票

    Args:
        ticker: 6位股票代码

    Returns:
        是否为上交所股票
    """
    return get_exchange(ticker) == "SH"


def is_sz_ticker(ticker: str) -> bool:
    """判断是否为深圳交易所股票

    Args:
        ticker: 6位股票代码

    Returns:
        是否为深交所股票
    """
    return get_exchange(ticker) == "SZ"


def is_bj_ticker(ticker: str) -> bool:
    """判断是否为北京交易所股票

    Args:
        ticker: 6位股票代码

    Returns:
        是否为北交所股票
    """
    return get_exchange(ticker) == "BJ"


# 板块分类（项目内 SSOT，API/管道统一调用此函数）
BOARD_KCB = "科创板"
BOARD_SH_MAIN = "上证主板"
BOARD_SZ_MAIN = "深证主板"
BOARD_CYB = "创业板"
BOARD_BJ = "北交所"
BOARD_OTHER = "其他"


def _ticker_code(ticker: str) -> str:
    """提取 ticker 的 6 位代码部分。"""
    raw = str(ticker).strip().upper()
    code = raw.split(".", 1)[0] if "." in raw else raw
    for prefix in ("BJ", "SH", "SZ"):
        if code.startswith(prefix):
            code = code[len(prefix):]
            break
    return format_ticker(code)


def get_board(ticker: str) -> str:
    """根据 Ticker 判断所属板块。"""
    code = _ticker_code(ticker)
    exchange = get_exchange(code)

    if code.startswith(("688", "689")):
        return BOARD_KCB
    if code.startswith("60"):
        return BOARD_SH_MAIN
    if code.startswith("00"):
        return BOARD_SZ_MAIN
    if code.startswith("30"):
        return BOARD_CYB
    if exchange == "BJ":
        return BOARD_BJ
    return BOARD_OTHER


def _board_series(tickers: pd.Series) -> pd.Series:
    """批量判断 ticker 所属板块。"""
    ticker_text = tickers.astype(str).str.strip().str.upper()
    code = ticker_text.str.split(".", n=1).str[0]
    code = code.str.replace(r"^(BJ|SH|SZ)", "", regex=True).str.zfill(TICKER_LENGTH)

    board = pd.Series(BOARD_OTHER, index=tickers.index)
    board = board.mask(code.str.startswith(("688", "689")), BOARD_KCB)
    board = board.mask(code.str.startswith("60"), BOARD_SH_MAIN)
    board = board.mask(code.str.startswith("00"), BOARD_SZ_MAIN)
    board = board.mask(code.str.startswith("30"), BOARD_CYB)
    board = board.mask(code.str.startswith(BJ_TICKER_PREFIXES), BOARD_BJ)
    return board


def _parse_trade_date(value: object) -> date | None:
    """将交易日输入解析为 date，无法解析时返回 None。"""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text:
        return None
    for fmt in (DATE_FORMAT, DATE_FORMAT_COMPACT, "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _is_after_sh_st_10_effective_date(trade_date: object = None) -> bool:
    """判断是否适用 2026-07-07 起上证主板 ST 10% 规则。"""
    parsed = _parse_trade_date(trade_date)
    if parsed is None:
        return True
    return parsed >= SH_ST_10_PCT_EFFECTIVE_DATE


def _sh_st_10_rule_mask(trade_dates: pd.Series | object, index: pd.Index) -> pd.Series:
    """批量判断是否适用上证主板 ST 10% 规则。"""
    if isinstance(trade_dates, pd.Series):
        parsed = pd.to_datetime(trade_dates.reindex(index), errors="coerce")
        return parsed.isna() | (parsed >= pd.Timestamp(SH_ST_10_PCT_EFFECTIVE_DATE))
    return pd.Series(_is_after_sh_st_10_effective_date(trade_dates), index=index)


def get_limit_pct(ticker: str, is_st: bool = False, trade_date: object = None) -> float:
    """根据 ticker 和 ST 状态返回涨跌停幅度百分比。

    规则:
    - 主板普通股: 10%
    - 主板 ST: 5%
    - 上证主板 ST 自 2026-07-07 起: 10%
    - 科创板/创业板: 20%
    - 北交所: 30%
    """
    board = get_board(ticker)
    if board in (BOARD_KCB, BOARD_CYB):
        return LIMIT_20_PCT
    if board == BOARD_BJ:
        return LIMIT_BJ_PCT
    if (
        is_st
        and board == BOARD_SH_MAIN
        and _is_after_sh_st_10_effective_date(trade_date)
    ):
        return LIMIT_UP_PCT
    return LIMIT_UP_ST_PCT if is_st else LIMIT_UP_PCT


def calc_limit_up_pct(
    is_st: bool = False,
    *,
    ticker: str = "",
    trade_date: object = None,
) -> float:
    """计算涨停幅度

    Args:
        is_st: 是否为 ST 股票
        ticker: 股票代码，可选；传入后按板块规则计算
        trade_date: 交易日，可选；用于判断日期生效的市场规则

    Returns:
        涨停幅度百分比

    Example:
        calc_limit_up_pct()        # -> 10.0
        calc_limit_up_pct(True)    # -> 5.0
        calc_limit_up_pct(ticker="300750.SZ")  # -> 20.0
    """
    return get_limit_pct(ticker, is_st=is_st, trade_date=trade_date)


def calc_limit_down_pct(
    is_st: bool = False,
    *,
    ticker: str = "",
    trade_date: object = None,
) -> float:
    """计算跌停幅度

    Args:
        is_st: 是否为 ST 股票
        ticker: 股票代码，可选；传入后按板块规则计算
        trade_date: 交易日，可选；用于判断日期生效的市场规则

    Returns:
        跌停幅度百分比

    Example:
        calc_limit_down_pct()      # -> -10.0
        calc_limit_down_pct(True)  # -> -5.0
        calc_limit_down_pct(ticker="300750.SZ")  # -> -20.0
    """
    return -get_limit_pct(ticker, is_st=is_st, trade_date=trade_date)


def _to_float(value: object) -> float | None:
    """将单个数值转成 float，无法转换或缺失时返回 None。"""
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(result):
        return None
    return result


def calc_limit_up_price(
    pre_close: float,
    ticker: str,
    is_st: bool = False,
    trade_date: object = None,
) -> float | None:
    """根据昨收价计算涨停价，无有效昨收时返回 None。"""
    pre_close_value = _to_float(pre_close)
    if pre_close_value is None or pre_close_value == 0:
        return None
    limit_pct = get_limit_pct(ticker, is_st=is_st, trade_date=trade_date)
    return round(pre_close_value * (1 + limit_pct / 100), 2)


def calc_limit_down_price(
    pre_close: float,
    ticker: str,
    is_st: bool = False,
    trade_date: object = None,
) -> float | None:
    """根据昨收价计算跌停价，无有效昨收时返回 None。"""
    pre_close_value = _to_float(pre_close)
    if pre_close_value is None or pre_close_value == 0:
        return None
    limit_pct = get_limit_pct(ticker, is_st=is_st, trade_date=trade_date)
    return round(pre_close_value * (1 - limit_pct / 100), 2)


def is_limit_up_price(
    close: float,
    pre_close: float,
    ticker: str,
    is_st: bool = False,
    trade_date: object = None,
) -> bool:
    """判断收盘价是否触及涨停，无有效昨收时返回 False。"""
    close_value = _to_float(close)
    limit_up_price = calc_limit_up_price(
        pre_close,
        ticker,
        is_st=is_st,
        trade_date=trade_date,
    )
    if close_value is None or limit_up_price is None:
        return False
    return round(close_value, 2) >= limit_up_price


def is_limit_down_price(
    close: float,
    pre_close: float,
    ticker: str,
    is_st: bool = False,
    trade_date: object = None,
) -> bool:
    """判断收盘价是否触及跌停，无有效昨收时返回 False。"""
    close_value = _to_float(close)
    limit_down_price = calc_limit_down_price(
        pre_close,
        ticker,
        is_st=is_st,
        trade_date=trade_date,
    )
    if close_value is None or limit_down_price is None:
        return False
    return round(close_value, 2) <= limit_down_price


def _limit_pct_series(
    tickers: pd.Series,
    is_st: pd.Series | bool,
    trade_dates: pd.Series | object = None,
) -> pd.Series:
    """按正式日更管线口径批量计算涨跌停幅度。"""
    board = _board_series(tickers)
    is_20_pct_board = board.isin((BOARD_KCB, BOARD_CYB))
    is_bj = board == BOARD_BJ
    is_sh_main = board == BOARD_SH_MAIN
    if isinstance(is_st, pd.Series):
        is_st_mask = is_st.reindex(tickers.index).fillna(False).astype(bool)
    else:
        is_st_mask = pd.Series(bool(is_st), index=tickers.index)
    sh_st_10_mask = _sh_st_10_rule_mask(trade_dates, tickers.index)

    limit_pct = pd.Series(LIMIT_UP_PCT, index=tickers.index)
    limit_pct = limit_pct.mask(is_20_pct_board, LIMIT_20_PCT)
    limit_pct = limit_pct.mask(is_bj, LIMIT_BJ_PCT)
    limit_pct = limit_pct.mask(
        is_st_mask & ~is_20_pct_board & ~is_bj,
        LIMIT_UP_ST_PCT,
    )
    limit_pct = limit_pct.mask(
        is_st_mask & is_sh_main & sh_st_10_mask,
        LIMIT_UP_PCT,
    )
    return limit_pct


def derive_limit_flags(df: pd.DataFrame, trade_date: object = None) -> pd.DataFrame:
    """按统一涨跌停规则派生 is_limit_up 和 is_limit_down 字段。"""
    df = df.copy()

    if CLOSE not in df.columns or PRE_CLOSE not in df.columns:
        df[IS_LIMIT_UP] = False
        df[IS_LIMIT_DOWN] = False
        return df

    is_st = df.get(IS_ST, False)
    trade_dates = df[TRADE_DATE] if TRADE_DATE in df.columns else trade_date
    limit_pct = _limit_pct_series(df[TICKER], is_st, trade_dates=trade_dates)

    pre_close = df[PRE_CLOSE]
    close = df[CLOSE].round(2)
    limit_up_price = (pre_close * (1 + limit_pct / 100)).round(2)
    limit_down_price = (pre_close * (1 - limit_pct / 100)).round(2)

    df[IS_LIMIT_UP] = close >= limit_up_price
    df[IS_LIMIT_DOWN] = close <= limit_down_price

    mask_no_pre = pre_close.isna() | (pre_close == 0)
    df.loc[mask_no_pre, IS_LIMIT_UP] = False
    df.loc[mask_no_pre, IS_LIMIT_DOWN] = False

    return df
