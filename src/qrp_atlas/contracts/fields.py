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
CHANGE = "change"

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
    PCT_CHANGE, CHANGE, TURNOVER, MARKET_CAP, FLOAT_CAP, PRE_CLOSE,
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
FULL_NAME = "full_name"
PUBLISHER = "publisher"
INDEX_TYPE = "index_type"
CATEGORY = "category"
BASE_DATE = "base_date"
BASE_POINT = "base_point"
WEIGHT_RULE = "weight_rule"
DESCRIPTION = "description"
EXP_DATE = "exp_date"

# ── 每日基本面指标字段（daily_basic） ──
TURNOVER_RATE = "turnover_rate"
TURNOVER_RATE_F = "turnover_rate_f"
VOLUME_RATIO = "volume_ratio"
PE_TTM = "pe_ttm"
PB = "pb"
PS = "ps"
PS_TTM = "ps_ttm"
DV_RATIO = "dv_ratio"
DV_TTM = "dv_ttm"
FLOAT_SHARE = "float_share"
FREE_SHARE = "free_share"
TOTAL_MV = "total_mv"
CIRC_MV = "circ_mv"
LIMIT_STATUS = "limit_status"

# ── 停复牌字段 ──
SUSPEND_TIMING = "suspend_timing"
SUSPEND_TYPE = "suspend_type"

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

# ── 互动问答字段（全景网 / 互动易） ──
INTERACTION_PID = "pid"
COMPANY_CODE = "company_code"
COMPANY_SHORTNAME = "company_shortname"
QUESTION_CONTENT = "question_content"
REPLY_CONTENT = "reply_content"
QUESTION_TIME = "question_time"
REPLY_TIME = "reply_time"
REPLY_DATE = "reply_date"
NICKNAME = "nickname"
KEYWORDS = "keywords"

# ── 财务 / 行业 / 指数成分 point-in-time 字段 ──
REPORT_PERIOD = "report_period"
ANNOUNCEMENT_DATE = "announcement_date"
F_ANN_DATE = "f_ann_date"
PUBLISHED_AT = "published_at"
AVAILABLE_TRADE_DATE = "available_trade_date"
UPDATE_FLAG = "update_flag"
COMP_TYPE = "comp_type"
END_TYPE = "end_type"
SOURCE_RECORD_ID = "source_record_id"
REVISION_ID = "revision_id"
INGESTED_AT = "ingested_at"
ASSET_ID = "asset_id"
CLASSIFICATION_SYSTEM = "classification_system"
INDUSTRY_LEVEL = "industry_level"
EFFECTIVE_FROM = "effective_from"
EFFECTIVE_TO = "effective_to"
SNAPSHOT_DATE = "snapshot_date"
WEIGHT = "weight"

# 利润表核心科目
BASIC_EPS = "basic_eps"
DILUTED_EPS = "diluted_eps"
TOTAL_REVENUE = "total_revenue"
REVENUE = "revenue"
OPERATE_PROFIT = "operate_profit"
TOTAL_PROFIT = "total_profit"
N_INCOME = "n_income"
N_INCOME_ATTR_P = "n_income_attr_p"
EBIT = "ebit"
EBITDA = "ebitda"

# 资产负债表核心科目
TOTAL_ASSETS = "total_assets"
TOTAL_LIAB = "total_liab"
TOTAL_CUR_ASSETS = "total_cur_assets"
TOTAL_NCA = "total_nca"
TOTAL_CUR_LIAB = "total_cur_liab"
TOTAL_NCL = "total_ncl"
TOTAL_HLDR_EQY_EXC_MIN_INT = "total_hldr_eqy_exc_min_int"
TOTAL_HLDR_EQY_INC_MIN_INT = "total_hldr_eqy_inc_min_int"
MONEY_CAP = "money_cap"
ACCOUNTS_RECEIV = "accounts_receiv"
INVENTORIES = "inventories"

# 现金流量表核心科目
N_CASHFLOW_ACT = "n_cashflow_act"
N_CASHFLOW_INV_ACT = "n_cashflow_inv_act"
N_CASH_FLOWS_FNC_ACT = "n_cash_flows_fnc_act"
N_INCR_CASH_CASH_EQU = "n_incr_cash_cash_equ"
C_CASH_EQU_END_PERIOD = "c_cash_equ_end_period"
FREE_CASHFLOW = "free_cashflow"

# 财务指标核心字段
EPS = "eps"
BPS = "bps"
CFPS = "cfps"
ROE = "roe"
ROA = "roa"
GROSSPROFIT_MARGIN = "grossprofit_margin"
NETPROFIT_MARGIN = "netprofit_margin"
DEBT_TO_ASSETS = "debt_to_assets"
CURRENT_RATIO = "current_ratio"
QUICK_RATIO = "quick_ratio"

# ── 事件 / 业绩预告 point-in-time 字段 ──
EVENT_TYPE = "event_type"
EVENT_SERIES_ID = "event_series_id"
FIRST_ANNOUNCEMENT_DATE = "first_announcement_date"
TIME_PRECISION = "time_precision"
FORECAST_TYPE = "forecast_type"
PROFIT_CHANGE_MIN = "profit_change_min"
PROFIT_CHANGE_MAX = "profit_change_max"
NET_PROFIT_MIN = "net_profit_min"
NET_PROFIT_MAX = "net_profit_max"
LAST_PARENT_NET = "last_parent_net"
SUMMARY = "summary"
CHANGE_REASON = "change_reason"
