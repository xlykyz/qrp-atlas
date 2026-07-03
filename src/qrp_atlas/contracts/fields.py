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

# ── 指数字段 ──
INDEX_CODE = "index_code"
INDEX_NAME = "index_name"

# ── 涨跌停股池字段 ──
FIRST_BLOCK_TIME = "first_block_time"
LAST_BLOCK_TIME = "last_block_time"
CONSECUTIVE_BOARDS = "consecutive_boards"
BLOCK_FUND = "block_fund"
CONSECUTIVE_DAYS = "consecutive_days"
OPEN_COUNT = "open_count"
BLAST_COUNT = "blast_count"
BLOCK_STATS = "block_stats"
TOTAL_SHARES = "total_shares"
BOARD_AMOUNT = "board_amount"
PE_RATIO = "pe_ratio"

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

# ── 研报字段 ──
INFO_CODE = "info_code"
TITLE = "title"
STOCK_CODE = "stock_code"
STOCK_NAME = "stock_name"
PUBLISH_DATE = "publish_date"
MARKET = "market"
COLUMN = "report_column"
REPORT_COLUMN = COLUMN  # alias
REPORT_TYPE = "report_type"
ENCODE_URL = "encode_url"
EM_RATING_CODE = "em_rating_code"
EM_RATING_VALUE = "em_rating_value"
EM_RATING_NAME = "em_rating_name"
LAST_EM_RATING_CODE = "last_em_rating_code"
LAST_EM_RATING_VALUE = "last_em_rating_value"
LAST_EM_RATING_NAME = "last_em_rating_name"
S_RATING_CODE = "s_rating_code"
S_RATING_NAME = "s_rating_name"
RATING_CHANGE = "rating_change"
INDV_AIM_PRICE_T = "indv_aim_price_t"
INDV_AIM_PRICE_L = "indv_aim_price_l"
PREDICT_THIS_YEAR_EPS = "predict_this_year_eps"
PREDICT_THIS_YEAR_PE = "predict_this_year_pe"
PREDICT_NEXT_YEAR_EPS = "predict_next_year_eps"
PREDICT_NEXT_YEAR_PE = "predict_next_year_pe"
PREDICT_NEXT_TWO_YEAR_EPS = "predict_next_two_year_eps"
PREDICT_NEXT_TWO_YEAR_PE = "predict_next_two_year_pe"
PREDICT_LAST_YEAR_EPS = "predict_last_year_eps"
PREDICT_LAST_YEAR_PE = "predict_last_year_pe"
ACTUAL_LAST_YEAR_EPS = "actual_last_year_eps"
ACTUAL_LAST_TWO_YEAR_EPS = "actual_last_two_year_eps"
ORG_CODE = "org_code"
ORG_NAME = "org_name"
ORG_SNAME = "org_sname"
ORG_TYPE = "org_type"
AUTHOR = "author"
AUTHOR_ID = "author_id"
RESEARCHER = "researcher"
COUNT = "count"
INDV_INDU_CODE = "indv_indu_code"
INDV_INDU_NAME = "indv_indu_name"
INDV_IS_NEW = "indv_is_new"
NEW_LISTING_DATE = "new_listing_date"
NEW_PURCHASE_DATE = "new_purchase_date"
NEW_ISSUE_PRICE = "new_issue_price"
NEW_PE_ISSUE_A = "new_pe_issue_a"
ATTACH_PAGES = "attach_pages"
ATTACH_SIZE = "attach_size"
ATTACH_TYPE = "attach_type"
INDUSTRY_CODE = "industry_code"
INDUSTRY_NAME = "industry_name"
EM_INDUSTRY_CODE = "em_industry_code"
NOTICE_CONTENT = "notice_content"
ATTACH_URL = "attach_url"
