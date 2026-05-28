"""
fields.py - 全项目字段名常量(SSOT)

所有字段名在此统一定义，其他模块从这里引用。
修改字段名只需改这里，全项目自动生效。

使用示例:
    from qrp_atlas.contracts import TICKER, TRADE_DATE, CLOSE

    # 在 DataFrame 操作中使用
    df = df[[TICKER, TRADE_DATE, CLOSE]]

    # 在 SQL 查询中使用
    sql = f"SELECT {TICKER}, {CLOSE} FROM table"

字段分类:
    - 通用字段: TICKER, TRADE_DATE, NAME, CREATED_AT
    - OHLCV: OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT
    - 涨跌: PCT_CHANGE
    - 换手/市值: TURNOVER, MARKET_CAP, FLOAT_CAP
    - 状态标记: IS_ST, IS_LIMIT_UP, IS_LIMIT_DOWN
    - 市场阶段: PHASE, M1_CORE, M2_FRONT, M3_IDENTIFIABLE, V_TRIGGERED
    - 交易执行: TRADE_ID, ENTRY_DATE, ENTRY_PRICE, ...
"""

TICKER = "ticker"
TRADE_DATE = "trade_date"
NAME = "name"
CREATED_AT = "created_at"

OPEN = "open"
HIGH = "high"
LOW = "low"
CLOSE = "close"
VOLUME = "volume"
AMOUNT = "amount"

PCT_CHANGE = "pct_change"
PRE_CLOSE = "pre_close"

TURNOVER = "turnover"
MARKET_CAP = "market_cap"
FLOAT_CAP = "float_cap"

IS_ST = "is_st"
IS_LIMIT_UP = "is_limit_up"
IS_LIMIT_DOWN = "is_limit_down"

PHASE = "phase"
M1_CORE = "M1_core"
M2_FRONT = "M2_front"
M3_IDENTIFIABLE = "M3_identifiable"
V_TRIGGERED = "V_triggered"
NOTES = "notes"

TRADE_ID = "trade_id"
ENTRY_DATE = "entry_date"
ENTRY_PRICE = "entry_price"
PATH_TYPE = "path_type"
HALF_SELL_TRIGGER = "half_sell_trigger"
HALF_SELL_DATE = "half_sell_date"
HALF_SELL_PRICE = "half_sell_price"
EXIT_DATE = "exit_date"
EXIT_PRICE = "exit_price"
POSITION_PCT = "position_pct"

OHLCV_FIELDS = (OPEN, HIGH, LOW, CLOSE, VOLUME)

PRICE_FIELDS = (OPEN, HIGH, LOW, CLOSE, PRE_CLOSE, ENTRY_PRICE, EXIT_PRICE, HALF_SELL_PRICE)

NUMERIC_FIELDS = (
    OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT,
    PCT_CHANGE, TURNOVER, MARKET_CAP, FLOAT_CAP, PRE_CLOSE,
    ENTRY_PRICE, EXIT_PRICE, HALF_SELL_PRICE,
    HALF_SELL_TRIGGER, POSITION_PCT
)

BOOLEAN_FIELDS = (IS_ST, IS_LIMIT_UP, IS_LIMIT_DOWN, M1_CORE, M2_FRONT, M3_IDENTIFIABLE, V_TRIGGERED)

EXCHANGE = "exchange"
MARKET = "market"
LIST_DATE = "list_date"
DELIST_DATE = "delist_date"
IS_ACTIVE = "is_active"
UPDATED_AT = "updated_at"

IS_OPEN = "is_open"
ADJ_FACTOR = "adj_factor"
YEAR_FIELD = "year"
MONTH_FIELD = "month"
QUARTER = "quarter"

DATE_FIELDS = (TRADE_DATE, ENTRY_DATE, EXIT_DATE, HALF_SELL_DATE)

IDENTIFIER_FIELDS = (TICKER, TRADE_ID)

# ── 调研公告字段 ──
SECU_CODE = "secu_code"
SEC_NAME = "sec_name"
NOTICE_DATE = "notice_date"
RECEIVE_DATE = "receive_date"
RECEIVE_WAY = "receive_way"
RECEIVE_PLACE = "receive_place"
RECEPTIONIST = "receptionist"
ORG_COUNT = "org_count"
CONTENT = "content"
ANNOUNCEMENT_TITLE = "announcement_title"
ADJUNCT_URL = "adjunct_url"
ADJUNCT_SIZE = "adjunct_size"
SOURCE = "source"
