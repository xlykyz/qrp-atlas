"""
conventions.py - 通用约定

定义日期格式、ticker规则、数值列集合等通用约定。
全项目统一使用这些约定。

使用示例:
    from qrp_atlas.contracts import (
        DATE_FORMAT, format_date,
        format_ticker, get_exchange,
        calc_limit_up_pct
    )

    # 日期格式化
    date_str = format_date("20240101", from_format="%Y%m%d")

    # Ticker 格式化
    ticker = format_ticker("1")  # -> "000001"

    # 获取交易所
    exchange = get_exchange("600000")  # -> "SH"

    # 计算涨停幅度
    limit_pct = calc_limit_up_pct(is_st=True)  # -> 5.0

约定内容:
    - 日期格式: DATE_FORMAT, DATE_FORMAT_COMPACT, DATETIME_FORMAT
    - Ticker规则: 长度6位, 交易所前缀识别
    - 涨跌停幅度: 普通股10%, ST股5%
    - 字段类型集合: NUMERIC_COLUMNS, BOOLEAN_COLUMNS, DATE_COLUMNS
"""

from typing import Tuple

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

NUMERIC_DECIMAL_PLACES = 4
VOLUME_UNIT = "股"
AMOUNT_UNIT = "元"
MARKET_CAP_UNIT = "元"

from .fields import (
    OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT,
    PCT_CHANGE, TURNOVER, MARKET_CAP, FLOAT_CAP, PRE_CLOSE,
    ENTRY_PRICE, EXIT_PRICE, HALF_SELL_PRICE,
    HALF_SELL_TRIGGER, POSITION_PCT,
    IS_ST, IS_LIMIT_UP, IS_LIMIT_DOWN,
    M1_CORE, M2_FRONT, M3_IDENTIFIABLE, V_TRIGGERED,
    TRADE_DATE, ENTRY_DATE, EXIT_DATE, HALF_SELL_DATE,
    TICKER, TRADE_ID, NAME,
)

NUMERIC_COLUMNS: Tuple[str, ...] = (
    OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT,
    PCT_CHANGE, TURNOVER, MARKET_CAP, FLOAT_CAP, PRE_CLOSE,
    ENTRY_PRICE, EXIT_PRICE, HALF_SELL_PRICE,
    HALF_SELL_TRIGGER, POSITION_PCT,
)

BOOLEAN_COLUMNS: Tuple[str, ...] = (
    IS_ST, IS_LIMIT_UP, IS_LIMIT_DOWN,
    M1_CORE, M2_FRONT, M3_IDENTIFIABLE, V_TRIGGERED,
)

DATE_COLUMNS: Tuple[str, ...] = (
    TRADE_DATE, ENTRY_DATE, EXIT_DATE, HALF_SELL_DATE,
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


def calc_limit_up_pct(is_st: bool = False) -> float:
    """计算涨停幅度

    Args:
        is_st: 是否为 ST 股票

    Returns:
        涨停幅度百分比(普通股10%, ST股5%)

    Example:
        calc_limit_up_pct()        # -> 10.0
        calc_limit_up_pct(True)    # -> 5.0
    """
    return LIMIT_UP_ST_PCT if is_st else LIMIT_UP_PCT


def calc_limit_down_pct(is_st: bool = False) -> float:
    """计算跌停幅度

    Args:
        is_st: 是否为 ST 股票

    Returns:
        跌停幅度百分比(普通股-10%, ST股-5%)

    Example:
        calc_limit_down_pct()      # -> -10.0
        calc_limit_down_pct(True)  # -> -5.0
    """
    return LIMIT_DOWN_ST_PCT if is_st else LIMIT_DOWN_PCT


# 板块分类（项目内 SSOT，API/管道统一调用此函数）
BOARD_KCB = "科创板"
BOARD_SH_MAIN = "上证主板"
BOARD_SZ_MAIN = "深证主板"
BOARD_CYB = "创业板"
BOARD_BJ = "北交所"
BOARD_OTHER = "其他"


def get_board(ticker: str) -> str:
    """根据 Ticker 判断所属板块

    Args:
        ticker: 6位股票代码（可带 .SH/.SZ/.BJ 后缀）

    Returns:
        板块名称: 科创板/上证主板/深证主板/创业板/北交所/其他

    Example:
        get_board("688001.SH")  # -> "科创板"
        get_board("600000.SH")   # -> "上证主板"
        get_board("000001.SZ")   # -> "深证主板"
        get_board("300750.SZ")   # -> "创业板"
        get_board("430047.BJ")   # -> "北交所"
    """
    raw = str(ticker).strip().upper()
    code = raw.split(".", 1)[0] if "." in raw else raw
    code = format_ticker(code)
    exchange = get_exchange(code)

    if code.startswith("688") or code.startswith("689"):
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
