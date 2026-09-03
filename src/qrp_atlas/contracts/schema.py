"""
schema.py - 表结构定义

定义所有数据库表的列清单、主键、DuckDB建表SQL。
其他模块从这里获取表结构信息。

使用示例:
    from qrp_atlas.contracts import DAILY_MARKET_SNAPSHOT, get_table, init_database

    # 获取表结构
    schema = get_table("daily_market_snapshot")
    print(schema.column_names())

    # 获取建表 SQL
    print(schema.duckdb_create_sql())

    # 初始化数据库
    import duckdb
    con = duckdb.connect("quant.db")
    init_database(con)

表结构说明:
    - daily_market_snapshot: 每日全市场行情快照，主键(trade_date, ticker)
    - market_phase: 每日市场阶段判断，主键(trade_date)
    - trade_execution: 交易执行记录，主键(trade_id)
    - stock_info: 股票基础信息，主键(ticker)
    - trading_calendar: 交易日历，主键(trade_date)
"""

from dataclasses import dataclass
from typing import Tuple

from .fields import (
    TICKER, TS_CODE, SYMBOL, PROVIDER, IDENTITY_TYPE, STANDARD_TICKER,
    ISOLATION_REASON, CAPTURED_AT, TRADE_DATE, NAME, CREATED_AT,
    OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT,
    PCT_CHANGE, PRE_CLOSE, CHANGE, AVG_PRICE, TURNOVER, MARKET_CAP, FLOAT_CAP,
    IS_ST, IS_LIMIT_UP, IS_LIMIT_DOWN,
    PHASE, M1_CORE, M2_FRONT, M3_IDENTIFIABLE, V_TRIGGERED, NOTES,
    TRADE_ID, ENTRY_DATE, ENTRY_PRICE, PATH_TYPE,
    HALF_SELL_TRIGGER, HALF_SELL_DATE, HALF_SELL_PRICE,
    EXIT_DATE, EXIT_PRICE, POSITION_PCT,
    EXCHANGE, MARKET, AREA, INDUSTRY, FULLNAME, ENNAME, CNSPELL,
    CURR_TYPE, LIST_STATUS, IS_HS, ACT_NAME, ACT_ENT_TYPE,
    LIST_DATE, DELIST_DATE, IS_ACTIVE, UPDATED_AT,
    IS_OPEN, ADJ_FACTOR, YEAR_FIELD, MONTH_FIELD, QUARTER,
    SECU_CODE, SEC_NAME, NOTICE_DATE, RECEIVE_DATE, RECEIVE_WAY,
    RECEIVE_PLACE, RECEPTIONIST, ORG_COUNT, CONTENT,
    ANNOUNCEMENT_TITLE, ADJUNCT_URL, ADJUNCT_SIZE, SOURCE,
    INFO_CODE, TITLE, STOCK_CODE, STOCK_NAME, PUBLISH_DATE,
    COLUMN, REPORT_COLUMN, REPORT_TYPE, ENCODE_URL,
    EM_RATING_CODE, EM_RATING_VALUE, EM_RATING_NAME,
    LAST_EM_RATING_CODE, LAST_EM_RATING_VALUE, LAST_EM_RATING_NAME,
    S_RATING_CODE, S_RATING_NAME, RATING_CHANGE,
    INDV_AIM_PRICE_T, INDV_AIM_PRICE_L,
    PREDICT_THIS_YEAR_EPS, PREDICT_THIS_YEAR_PE,
    PREDICT_NEXT_YEAR_EPS, PREDICT_NEXT_YEAR_PE,
    PREDICT_NEXT_TWO_YEAR_EPS, PREDICT_NEXT_TWO_YEAR_PE,
    PREDICT_LAST_YEAR_EPS, PREDICT_LAST_YEAR_PE,
    ACTUAL_LAST_YEAR_EPS, ACTUAL_LAST_TWO_YEAR_EPS,
    ORG_CODE, ORG_NAME, ORG_SNAME, ORG_TYPE,
    AUTHOR, AUTHOR_ID, RESEARCHER, COUNT,
    INDV_INDU_CODE, INDV_INDU_NAME,
    INDV_IS_NEW, NEW_LISTING_DATE, NEW_PURCHASE_DATE,
    NEW_ISSUE_PRICE, NEW_PE_ISSUE_A,
    ATTACH_PAGES, ATTACH_SIZE, ATTACH_TYPE,
    INDUSTRY_CODE, INDUSTRY_NAME, EM_INDUSTRY_CODE,
    NOTICE_CONTENT, ATTACH_URL,
    INDEX_CODE, INDEX_NAME, FULL_NAME, PUBLISHER, INDEX_TYPE, CATEGORY,
    BASE_DATE, BASE_POINT, WEIGHT_RULE, DESCRIPTION, EXP_DATE, CHANGE,
    FIRST_BLOCK_TIME, CONSECUTIVE_BOARDS, BLOCK_FUND,
    CONSECUTIVE_DAYS, OPEN_COUNT,
    LAST_BLOCK_TIME, BLAST_COUNT, BLOCK_STATS,
    TOTAL_SHARES, BOARD_AMOUNT, PE_RATIO,
    TURNOVER_RATE, TURNOVER_RATE_F, VOLUME_RATIO,
    PE_TTM, PB, PS, PS_TTM,
    DV_RATIO, DV_TTM,
    FLOAT_SHARE, FREE_SHARE,
    TOTAL_MV, CIRC_MV, FLOAT_MV, LIMIT_STATUS,
    SUSPEND_TIMING, SUSPEND_TYPE, IS_SUSPENDED,
    TRADE_MARKET, REASON, PERIOD,
    INTERACTION_PID, COMPANY_CODE, COMPANY_SHORTNAME,
    QUESTION_CONTENT, REPLY_CONTENT, QUESTION_TIME, REPLY_TIME,
    REPLY_DATE, NICKNAME, KEYWORDS,
    REPORT_PERIOD,
    ANNOUNCEMENT_DATE,
    F_ANN_DATE,
    PUBLISHED_AT,
    AVAILABLE_TRADE_DATE,
    UPDATE_FLAG,
    COMP_TYPE,
    SOURCE_RECORD_ID,
    REVISION_ID,
    INGESTED_AT,
    ASSET_ID,
    CLASSIFICATION_SYSTEM,
    INDUSTRY_LEVEL,
    EFFECTIVE_FROM,
    EFFECTIVE_TO,
    SNAPSHOT_DATE,
    WEIGHT,
    BASIC_EPS,
    DILUTED_EPS,
    TOTAL_REVENUE,
    REVENUE,
    OPERATE_PROFIT,
    TOTAL_PROFIT,
    N_INCOME,
    N_INCOME_ATTR_P,
    EBIT,
    EBITDA,
    TOTAL_ASSETS,
    TOTAL_LIAB,
    TOTAL_CUR_ASSETS,
    TOTAL_NCA,
    TOTAL_CUR_LIAB,
    TOTAL_NCL,
    TOTAL_HLDR_EQY_EXC_MIN_INT,
    TOTAL_HLDR_EQY_INC_MIN_INT,
    MONEY_CAP,
    ACCOUNTS_RECEIV,
    INVENTORIES,
    N_CASHFLOW_ACT,
    N_CASHFLOW_INV_ACT,
    N_CASH_FLOWS_FNC_ACT,
    N_INCR_CASH_CASH_EQU,
    C_CASH_EQU_END_PERIOD,
    FREE_CASHFLOW,
    BPS,
    CFPS,
    ROE,
    ROA,
    GROSSPROFIT_MARGIN,
    NETPROFIT_MARGIN,
    DEBT_TO_ASSETS,
    CURRENT_RATIO,
    QUICK_RATIO,
    END_TYPE,
    EPS,
    EVENT_TYPE,
    EVENT_SERIES_ID,
    FIRST_ANNOUNCEMENT_DATE,
    TIME_PRECISION,
    FORECAST_TYPE,
    PROFIT_CHANGE_MIN,
    PROFIT_CHANGE_MAX,
    NET_PROFIT_MIN,
    NET_PROFIT_MAX,
    LAST_PARENT_NET,
    SUMMARY,
    CHANGE_REASON,
    FINALIZED_AT,
)
from .system_b import (
    ACTUAL_PAIR_CONTIGUOUS,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    CALCULATION_VERSION,
    COMPLETED_AT,
    DIAGNOSTICS,
    INPUT_SNAPSHOT_ID,
    IS_ABOVE_OR_EQUAL_MA5,
    IS_TRADING_DAY,
    LATEST_ACTUAL_CLOSE,
    LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    LATEST_ACTUAL_MA5,
    LATEST_ACTUAL_MA5_WINDOW_COMPLETE,
    LATEST_ACTUAL_TRADE_DATE,
    LIFECYCLE_STATE,
    LISTING_TRADING_DAY_NUMBER,
    LISTING_TRADING_DAY_NUMBER_IS_EXACT,
    MA5,
    MA5_WINDOW_COMPLETE,
    MARKET_FACT_STATUS,
    PARAMETER_SET_ID,
    PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    PREVIOUS_ACTUAL_MA5_WINDOW_COMPLETE,
    PREVIOUS_ACTUAL_TRADE_DATE,
    PREVIOUS_TREND_STATE,
    PRICE_ADJUSTMENT,
    PRODUCTION_RUN_ID,
    RULE_VERSION_SET_ID,
    SOURCE_RULE_IDS,
    STATE_CHANGED,
    STATE_BASIS_SEQUENCE_INTACT,
    SYSTEM_B_PRODUCTION_RUN_TABLE,
    SYSTEM_B_STATE_OBSERVATION_TABLE,
    TREND_STATE,
    CREATED_RUN_ID, DAYS_SINCE_CONFIRMED, DAYS_SINCE_START,
    DRAWDOWN_FROM_PEAK, EPISODE_CONFIRMED_DATE, EPISODE_END_DATE,
    EPISODE_ID, EPISODE_NO, EPISODE_RETURN, EPISODE_START_DATE,
    IS_EPISODE_CONFIRMED, IS_EPISODE_END, MA10, MA5_REENTRY_COUNT,
    PEAK_RETURN, RULE_VERSION, STATE_TRANSITION,
    SYSTEM_B_EPISODE_OBSERVATION_TABLE, SYSTEM_B_EPISODE_TABLE,
    SYSTEM_B_EPISODE_SEGMENT_TABLE,
    ACTIVE_SPRINT_NO, ANCHOR_CLOSE, ANCHOR_DATE, END_CLOSE, END_DATE,
    IS_OPEN, MAX_DRAWDOWN, PEAK_CLOSE, PEAK_DATE, SEGMENT_ID, SEGMENT_NO,
    SEGMENT_RETURN, SEGMENT_STATE, SEGMENT_VERSION, SOURCE_EPISODE_RULE_VERSION,
    START_CLOSE, START_DATE, TRADING_DAYS,
    SYSTEM_B_POOL_MEMBERSHIP_TABLE, SYSTEM_B_POOL_RUN_TABLE,
    POOL_TYPE, MEMBERSHIP_STATE, POOL_CYCLE_NO, ENTRY_DATE, EXIT_DATE,
    ENTRY_REASON, EXIT_REASON, METRICS_JSON, COMPLETED_RUN_ID,
    POOL_COMPLETED_AT, SYSTEM_B_POOL_RULE_VERSION,
)
from .stock_collection import (
    COLLECTION_ID,
    COLLECTION_TYPE,
    COLLECTION_SCOPE,
    NAMESPACE,
    SOURCE_KEY,
    CANONICAL_NAME,
    MEMBERSHIP_MODEL,
    STATUS,
    THEME_ID,
    MEMBERSHIP_ID,
    STOCK_COLLECTION_TABLE,
    THEME_TABLE,
    THEME_MEMBERSHIP_HISTORY_TABLE,
)
from .m4 import (
    THEME_DAILY_RETURN,
    THEME_LIMIT_UP_COUNT,
    THEME_RETURN_RANK,
    EFFECTIVE_MEMBER_COUNT,
    TOTAL_MEMBER_COUNT,
    COMPARISON_UNIVERSE_SIZE,
    COMPARISON_UNIVERSE_VERSION,
    QUALIFICATION_STATUS,
    INDEX_LEVEL,
    BASE_LEVEL,
    CUSTOM_INDEX_TREND_STATE,
    CUSTOM_INDEX_TREND_RUN_DAYS,
    CUSTOM_INDEX_EPISODE_ID,
    THEME_EFFECTIVE_MEMBER_DAILY_TABLE,
    THEME_CUSTOM_INDEX_DAILY_TABLE,
    THEME_CUSTOM_INDEX_STATE_TABLE,
    THEME_CUSTOM_INDEX_EPISODE_TABLE,
    THEME_M4_OBSERVATION_TABLE,
    THEME_PRODUCTION_RUN_TABLE,
    KNOWLEDGE_DATE,
    IS_THEME_MEMBER,
    IS_M4_EFFECTIVE_MEMBER,
    EXCLUSION_REASON,
)



@dataclass(frozen=True)
class ColumnSpec:
    """列规格定义

    Attributes:
        name: 列名(使用 fields.py 中的常量)
        dtype: DuckDB 数据类型
        nullable: 是否允许 NULL，默认 True
    """
    name: str
    dtype: str
    nullable: bool = True


@dataclass(frozen=True)
class TableSchema:
    """表结构定义

    Attributes:
        name: 表名
        columns: 列规格元组
        primary_key: 主键字段元组
    """
    name: str
    columns: Tuple[ColumnSpec, ...]
    primary_key: Tuple[str, ...]

    def column_names(self) -> Tuple[str, ...]:
        """返回所有列名"""
        return tuple(col.name for col in self.columns)

    def duckdb_create_sql(self) -> str:
        """生成 DuckDB 建表 SQL

        单列主键时在列定义中添加 PRIMARY KEY，
        多列主键时在表末尾添加 PRIMARY KEY 约束。
        """
        col_defs = []
        for col in self.columns:
            col_def = f"  {col.name} {col.dtype}"
            if col.name in self.primary_key and len(self.primary_key) == 1:
                col_def += " PRIMARY KEY"
            if col.name == "created_at":
                col_def += " DEFAULT CURRENT_TIMESTAMP"
            col_defs.append(col_def)
        if len(self.primary_key) > 1:
            pk_def = f"  PRIMARY KEY ({', '.join(self.primary_key)})"
            col_defs.append(pk_def)
        return f"CREATE TABLE IF NOT EXISTS {self.name} (\n" + ",\n".join(col_defs) + "\n);"


DAILY_MARKET_SNAPSHOT = TableSchema(
    name="daily_market_snapshot",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(NAME, "VARCHAR"),
        ColumnSpec(OPEN, "DOUBLE"),
        ColumnSpec(HIGH, "DOUBLE"),
        ColumnSpec(LOW, "DOUBLE"),
        ColumnSpec(CLOSE, "DOUBLE"),
        ColumnSpec(PCT_CHANGE, "DOUBLE"),
        ColumnSpec(PRE_CLOSE, "DOUBLE"),
        ColumnSpec(VOLUME, "BIGINT"),
        ColumnSpec(AMOUNT, "DOUBLE"),
        ColumnSpec(TURNOVER, "DOUBLE"),
        ColumnSpec(MARKET_CAP, "DOUBLE"),
        ColumnSpec(FLOAT_CAP, "DOUBLE"),
        ColumnSpec(IS_ST, "BOOLEAN"),
        ColumnSpec(IS_LIMIT_UP, "BOOLEAN"),
        ColumnSpec(IS_LIMIT_DOWN, "BOOLEAN"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, TICKER),
)

MARKET_PHASE = TableSchema(
    name="market_phase",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(PHASE, "VARCHAR"),
        ColumnSpec(M1_CORE, "BOOLEAN"),
        ColumnSpec(M2_FRONT, "BOOLEAN"),
        ColumnSpec(M3_IDENTIFIABLE, "BOOLEAN"),
        ColumnSpec(V_TRIGGERED, "BOOLEAN"),
        ColumnSpec(NOTES, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE,),
)

TRADE_EXECUTION = TableSchema(
    name="trade_execution",
    columns=(
        ColumnSpec(TRADE_ID, "VARCHAR", nullable=False),
        ColumnSpec(TICKER, "VARCHAR"),
        ColumnSpec(ENTRY_DATE, "DATE"),
        ColumnSpec(ENTRY_PRICE, "DOUBLE"),
        ColumnSpec(PATH_TYPE, "VARCHAR"),
        ColumnSpec(HALF_SELL_TRIGGER, "DOUBLE"),
        ColumnSpec(HALF_SELL_DATE, "DATE"),
        ColumnSpec(HALF_SELL_PRICE, "DOUBLE"),
        ColumnSpec(EXIT_DATE, "DATE"),
        ColumnSpec(EXIT_PRICE, "DOUBLE"),
        ColumnSpec(POSITION_PCT, "DOUBLE"),
        ColumnSpec(NOTES, "VARCHAR"),
    ),
    primary_key=(TRADE_ID,),
)

STOCK_INFO = TableSchema(
    name="stock_info",
    columns=(
        ColumnSpec(TS_CODE, "VARCHAR"),
        ColumnSpec(SYMBOL, "VARCHAR"),
        ColumnSpec(NAME, "VARCHAR"),
        ColumnSpec(AREA, "VARCHAR"),
        ColumnSpec(INDUSTRY, "VARCHAR"),
        ColumnSpec(FULLNAME, "VARCHAR"),
        ColumnSpec(ENNAME, "VARCHAR"),
        ColumnSpec(CNSPELL, "VARCHAR"),
        ColumnSpec(MARKET, "VARCHAR"),
        ColumnSpec(EXCHANGE, "VARCHAR"),
        ColumnSpec(CURR_TYPE, "VARCHAR"),
        ColumnSpec(LIST_STATUS, "VARCHAR"),
        ColumnSpec(LIST_DATE, "DATE"),
        ColumnSpec(DELIST_DATE, "DATE"),
        ColumnSpec(IS_HS, "VARCHAR"),
        ColumnSpec(ACT_NAME, "VARCHAR"),
        ColumnSpec(ACT_ENT_TYPE, "VARCHAR"),
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(IS_ACTIVE, "BOOLEAN"),
        ColumnSpec(UPDATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TICKER,),
)

STOCK_INFO_HISTORICAL_IDENTITY = TableSchema(
    name="stock_info_historical_identity",
    columns=(
        ColumnSpec(PROVIDER, "VARCHAR", nullable=False),
        ColumnSpec(TS_CODE, "VARCHAR", nullable=False),
        ColumnSpec(SYMBOL, "VARCHAR"),
        ColumnSpec(NAME, "VARCHAR"),
        ColumnSpec(AREA, "VARCHAR"),
        ColumnSpec(INDUSTRY, "VARCHAR"),
        ColumnSpec(FULLNAME, "VARCHAR"),
        ColumnSpec(ENNAME, "VARCHAR"),
        ColumnSpec(CNSPELL, "VARCHAR"),
        ColumnSpec(MARKET, "VARCHAR"),
        ColumnSpec(EXCHANGE, "VARCHAR", nullable=False),
        ColumnSpec(CURR_TYPE, "VARCHAR"),
        ColumnSpec(LIST_STATUS, "VARCHAR", nullable=False),
        ColumnSpec(LIST_DATE, "DATE"),
        ColumnSpec(DELIST_DATE, "DATE"),
        ColumnSpec(IS_HS, "VARCHAR"),
        ColumnSpec(ACT_NAME, "VARCHAR"),
        ColumnSpec(ACT_ENT_TYPE, "VARCHAR"),
        ColumnSpec(IDENTITY_TYPE, "VARCHAR", nullable=False),
        ColumnSpec(STANDARD_TICKER, "VARCHAR"),
        ColumnSpec(ISOLATION_REASON, "VARCHAR", nullable=False),
        ColumnSpec(SNAPSHOT_DATE, "DATE", nullable=False),
        ColumnSpec(CAPTURED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(PROVIDER, TS_CODE),
)

TRADING_CALENDAR = TableSchema(
    name="trading_calendar",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(IS_OPEN, "BOOLEAN"),
        ColumnSpec(YEAR_FIELD, "INTEGER"),
        ColumnSpec(MONTH_FIELD, "INTEGER"),
        ColumnSpec(QUARTER, "INTEGER"),
    ),
    primary_key=(TRADE_DATE,),
)

ADJ_FACTOR_CHANGES = TableSchema(
    name="adj_factor_changes",
    columns=(
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(ADJ_FACTOR, "DOUBLE"),
    ),
    primary_key=(TICKER, TRADE_DATE),
)

CNINFO_RESEARCH_VISITS = TableSchema(
    name="cninfo_research_visits",
    columns=(
        ColumnSpec(SECU_CODE, "VARCHAR", nullable=False),
        ColumnSpec(SEC_NAME, "VARCHAR"),
        ColumnSpec(NOTICE_DATE, "DATE", nullable=False),
        ColumnSpec(RECEIVE_DATE, "DATE", nullable=False),
        ColumnSpec(RECEIVE_WAY, "VARCHAR"),
        ColumnSpec(RECEIVE_PLACE, "VARCHAR"),
        ColumnSpec(RECEPTIONIST, "VARCHAR"),
        ColumnSpec(ORG_COUNT, "INTEGER"),
        ColumnSpec(CONTENT, "TEXT"),
        ColumnSpec(ANNOUNCEMENT_TITLE, "VARCHAR"),
        ColumnSpec(ADJUNCT_URL, "VARCHAR"),
        ColumnSpec(ADJUNCT_SIZE, "INTEGER"),
        ColumnSpec(SOURCE, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(SECU_CODE, NOTICE_DATE, RECEIVE_DATE),
)

RESEARCH_REPORT_STOCK = TableSchema(
    name="research_report_stock",
    columns=(
        ColumnSpec(INFO_CODE, "VARCHAR", nullable=False),
        ColumnSpec(TITLE, "VARCHAR"),
        ColumnSpec(STOCK_CODE, "VARCHAR"),
        ColumnSpec(STOCK_NAME, "VARCHAR"),
        ColumnSpec(PUBLISH_DATE, "DATE"),
        ColumnSpec(MARKET, "VARCHAR"),
        ColumnSpec(COLUMN, "VARCHAR"),
        ColumnSpec(REPORT_TYPE, "INTEGER"),
        ColumnSpec(ENCODE_URL, "VARCHAR"),
        ColumnSpec(EM_RATING_CODE, "VARCHAR"),
        ColumnSpec(EM_RATING_VALUE, "VARCHAR"),
        ColumnSpec(EM_RATING_NAME, "VARCHAR"),
        ColumnSpec(LAST_EM_RATING_CODE, "VARCHAR"),
        ColumnSpec(LAST_EM_RATING_VALUE, "VARCHAR"),
        ColumnSpec(LAST_EM_RATING_NAME, "VARCHAR"),
        ColumnSpec(S_RATING_CODE, "VARCHAR"),
        ColumnSpec(S_RATING_NAME, "VARCHAR"),
        ColumnSpec(RATING_CHANGE, "INTEGER"),
        ColumnSpec(INDV_AIM_PRICE_T, "VARCHAR"),
        ColumnSpec(INDV_AIM_PRICE_L, "VARCHAR"),
        ColumnSpec(PREDICT_THIS_YEAR_EPS, "VARCHAR"),
        ColumnSpec(PREDICT_THIS_YEAR_PE, "VARCHAR"),
        ColumnSpec(PREDICT_NEXT_YEAR_EPS, "VARCHAR"),
        ColumnSpec(PREDICT_NEXT_YEAR_PE, "VARCHAR"),
        ColumnSpec(PREDICT_NEXT_TWO_YEAR_EPS, "VARCHAR"),
        ColumnSpec(PREDICT_NEXT_TWO_YEAR_PE, "VARCHAR"),
        ColumnSpec(PREDICT_LAST_YEAR_EPS, "VARCHAR"),
        ColumnSpec(PREDICT_LAST_YEAR_PE, "VARCHAR"),
        ColumnSpec(ACTUAL_LAST_YEAR_EPS, "VARCHAR"),
        ColumnSpec(ACTUAL_LAST_TWO_YEAR_EPS, "VARCHAR"),
        ColumnSpec(ORG_CODE, "VARCHAR"),
        ColumnSpec(ORG_NAME, "VARCHAR"),
        ColumnSpec(ORG_SNAME, "VARCHAR"),
        ColumnSpec(ORG_TYPE, "VARCHAR"),
        ColumnSpec(AUTHOR, "VARCHAR"),
        ColumnSpec(AUTHOR_ID, "VARCHAR"),
        ColumnSpec(RESEARCHER, "VARCHAR"),
        ColumnSpec(COUNT, "INTEGER"),
        ColumnSpec(INDV_INDU_CODE, "VARCHAR"),
        ColumnSpec(INDV_INDU_NAME, "VARCHAR"),
        ColumnSpec(INDV_IS_NEW, "VARCHAR"),
        ColumnSpec(NEW_LISTING_DATE, "VARCHAR"),
        ColumnSpec(NEW_PURCHASE_DATE, "VARCHAR"),
        ColumnSpec(NEW_ISSUE_PRICE, "DOUBLE"),
        ColumnSpec(NEW_PE_ISSUE_A, "DOUBLE"),
        ColumnSpec(ATTACH_PAGES, "INTEGER"),
        ColumnSpec(ATTACH_SIZE, "INTEGER"),
        ColumnSpec(ATTACH_TYPE, "VARCHAR"),
        ColumnSpec(INDUSTRY_CODE, "VARCHAR"),
        ColumnSpec(INDUSTRY_NAME, "VARCHAR"),
        ColumnSpec(EM_INDUSTRY_CODE, "VARCHAR"),
        ColumnSpec(NOTICE_CONTENT, "TEXT"),
        ColumnSpec(ATTACH_URL, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(INFO_CODE,),
)

RESEARCH_REPORT_INDUSTRY = TableSchema(
    name="research_report_industry",
    columns=(
        ColumnSpec(INFO_CODE, "VARCHAR", nullable=False),
        ColumnSpec(TITLE, "VARCHAR"),
        ColumnSpec(STOCK_CODE, "VARCHAR"),
        ColumnSpec(STOCK_NAME, "VARCHAR"),
        ColumnSpec(PUBLISH_DATE, "DATE"),
        ColumnSpec(MARKET, "VARCHAR"),
        ColumnSpec(REPORT_COLUMN, "VARCHAR"),
        ColumnSpec(REPORT_TYPE, "INTEGER"),
        ColumnSpec(ENCODE_URL, "VARCHAR"),
        ColumnSpec(EM_RATING_CODE, "VARCHAR"),
        ColumnSpec(EM_RATING_VALUE, "VARCHAR"),
        ColumnSpec(EM_RATING_NAME, "VARCHAR"),
        ColumnSpec(LAST_EM_RATING_CODE, "VARCHAR"),
        ColumnSpec(LAST_EM_RATING_VALUE, "VARCHAR"),
        ColumnSpec(LAST_EM_RATING_NAME, "VARCHAR"),
        ColumnSpec(S_RATING_CODE, "VARCHAR"),
        ColumnSpec(S_RATING_NAME, "VARCHAR"),
        ColumnSpec(RATING_CHANGE, "INTEGER"),
        ColumnSpec(INDV_AIM_PRICE_T, "VARCHAR"),
        ColumnSpec(INDV_AIM_PRICE_L, "VARCHAR"),
        ColumnSpec(PREDICT_THIS_YEAR_EPS, "VARCHAR"),
        ColumnSpec(PREDICT_THIS_YEAR_PE, "VARCHAR"),
        ColumnSpec(PREDICT_NEXT_YEAR_EPS, "VARCHAR"),
        ColumnSpec(PREDICT_NEXT_YEAR_PE, "VARCHAR"),
        ColumnSpec(PREDICT_NEXT_TWO_YEAR_EPS, "VARCHAR"),
        ColumnSpec(PREDICT_NEXT_TWO_YEAR_PE, "VARCHAR"),
        ColumnSpec(PREDICT_LAST_YEAR_EPS, "VARCHAR"),
        ColumnSpec(PREDICT_LAST_YEAR_PE, "VARCHAR"),
        ColumnSpec(ACTUAL_LAST_YEAR_EPS, "VARCHAR"),
        ColumnSpec(ACTUAL_LAST_TWO_YEAR_EPS, "VARCHAR"),
        ColumnSpec(ORG_CODE, "VARCHAR"),
        ColumnSpec(ORG_NAME, "VARCHAR"),
        ColumnSpec(ORG_SNAME, "VARCHAR"),
        ColumnSpec(ORG_TYPE, "VARCHAR"),
        ColumnSpec(AUTHOR, "VARCHAR"),
        ColumnSpec(AUTHOR_ID, "VARCHAR"),
        ColumnSpec(RESEARCHER, "VARCHAR"),
        ColumnSpec(COUNT, "INTEGER"),
        ColumnSpec(INDV_INDU_CODE, "VARCHAR"),
        ColumnSpec(INDV_INDU_NAME, "VARCHAR"),
        ColumnSpec(INDV_IS_NEW, "VARCHAR"),
        ColumnSpec(NEW_LISTING_DATE, "VARCHAR"),
        ColumnSpec(NEW_PURCHASE_DATE, "VARCHAR"),
        ColumnSpec(NEW_ISSUE_PRICE, "DOUBLE"),
        ColumnSpec(NEW_PE_ISSUE_A, "DOUBLE"),
        ColumnSpec(ATTACH_PAGES, "INTEGER"),
        ColumnSpec(ATTACH_SIZE, "INTEGER"),
        ColumnSpec(ATTACH_TYPE, "VARCHAR"),
        ColumnSpec(INDUSTRY_CODE, "VARCHAR"),
        ColumnSpec(INDUSTRY_NAME, "VARCHAR"),
        ColumnSpec(EM_INDUSTRY_CODE, "VARCHAR"),
        ColumnSpec(NOTICE_CONTENT, "TEXT"),
        ColumnSpec(ATTACH_URL, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(INFO_CODE,),
)

INDEX_BASIC = TableSchema(
    name="index_basic",
    columns=(
        ColumnSpec(INDEX_CODE, "VARCHAR", nullable=False),
        ColumnSpec(NAME, "VARCHAR", nullable=False),
        ColumnSpec(FULL_NAME, "VARCHAR"),
        ColumnSpec(MARKET, "VARCHAR", nullable=False),
        ColumnSpec(PUBLISHER, "VARCHAR", nullable=False),
        ColumnSpec(INDEX_TYPE, "VARCHAR"),
        ColumnSpec(CATEGORY, "VARCHAR", nullable=False),
        ColumnSpec(BASE_DATE, "DATE"),
        ColumnSpec(BASE_POINT, "DOUBLE"),
        ColumnSpec(LIST_DATE, "DATE"),
        ColumnSpec(WEIGHT_RULE, "VARCHAR"),
        ColumnSpec(DESCRIPTION, "TEXT"),
        ColumnSpec(EXP_DATE, "DATE"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(INDEX_CODE,),
)

INDEX_DAILY = TableSchema(
    name="index_daily",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(INDEX_CODE, "VARCHAR", nullable=False),
        ColumnSpec(INDEX_NAME, "VARCHAR"),
        ColumnSpec(OPEN, "DOUBLE"),
        ColumnSpec(HIGH, "DOUBLE"),
        ColumnSpec(LOW, "DOUBLE"),
        ColumnSpec(CLOSE, "DOUBLE"),
        ColumnSpec(PRE_CLOSE, "DOUBLE"),
        ColumnSpec(CHANGE, "DOUBLE"),
        ColumnSpec(PCT_CHANGE, "DOUBLE"),
        ColumnSpec(VOLUME, "BIGINT"),
        ColumnSpec(AMOUNT, "DOUBLE"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, INDEX_CODE),
)

ETF_DAILY = TableSchema(
    name="etf_daily",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(OPEN, "DOUBLE"),
        ColumnSpec(HIGH, "DOUBLE"),
        ColumnSpec(LOW, "DOUBLE"),
        ColumnSpec(CLOSE, "DOUBLE"),
        ColumnSpec(PRE_CLOSE, "DOUBLE"),
        ColumnSpec(CHANGE, "DOUBLE"),
        ColumnSpec(PCT_CHANGE, "DOUBLE"),
        ColumnSpec(VOLUME, "BIGINT"),
        ColumnSpec(AMOUNT, "DOUBLE"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, TICKER),
)

ETF_ADJ_FACTOR = TableSchema(
    name="etf_adj_factor",
    columns=(
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(ADJ_FACTOR, "DOUBLE", nullable=False),
    ),
    primary_key=(TICKER, TRADE_DATE),
)

ZT_POOL = TableSchema(
    name="zt_pool",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(NAME, "VARCHAR"),
        ColumnSpec(CLOSE, "DOUBLE"),
        ColumnSpec(PCT_CHANGE, "DOUBLE"),
        ColumnSpec(AMOUNT, "DOUBLE"),
        ColumnSpec(FLOAT_CAP, "DOUBLE"),
        ColumnSpec(TOTAL_SHARES, "DOUBLE"),
        ColumnSpec(TURNOVER, "DOUBLE"),
        ColumnSpec(FIRST_BLOCK_TIME, "VARCHAR"),
        ColumnSpec(LAST_BLOCK_TIME, "VARCHAR"),
        ColumnSpec(CONSECUTIVE_BOARDS, "INTEGER"),
        ColumnSpec(BLOCK_FUND, "DOUBLE"),
        ColumnSpec(BLAST_COUNT, "INTEGER"),
        ColumnSpec(BLOCK_STATS, "VARCHAR"),
        ColumnSpec(INDUSTRY_NAME, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, TICKER),
)

DT_POOL = TableSchema(
    name="dt_pool",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(NAME, "VARCHAR"),
        ColumnSpec(CLOSE, "DOUBLE"),
        ColumnSpec(PCT_CHANGE, "DOUBLE"),
        ColumnSpec(AMOUNT, "DOUBLE"),
        ColumnSpec(FLOAT_CAP, "DOUBLE"),
        ColumnSpec(TOTAL_SHARES, "DOUBLE"),
        ColumnSpec(TURNOVER, "DOUBLE"),
        ColumnSpec(BLOCK_FUND, "DOUBLE"),
        ColumnSpec(CONSECUTIVE_DAYS, "INTEGER"),
        ColumnSpec(OPEN_COUNT, "INTEGER"),
        ColumnSpec(LAST_BLOCK_TIME, "VARCHAR"),
        ColumnSpec(BOARD_AMOUNT, "DOUBLE"),
        ColumnSpec(PE_RATIO, "DOUBLE"),
        ColumnSpec(INDUSTRY_NAME, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, TICKER),
)

DAILY_BASIC = TableSchema(
    name="daily_basic",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(CLOSE, "DOUBLE"),
        ColumnSpec(TURNOVER_RATE, "DOUBLE"),
        ColumnSpec(TURNOVER_RATE_F, "DOUBLE"),
        ColumnSpec(VOLUME_RATIO, "DOUBLE"),
        ColumnSpec(PE_RATIO, "DOUBLE"),
        ColumnSpec(PE_TTM, "DOUBLE"),
        ColumnSpec(PB, "DOUBLE"),
        ColumnSpec(PS, "DOUBLE"),
        ColumnSpec(PS_TTM, "DOUBLE"),
        ColumnSpec(DV_RATIO, "DOUBLE"),
        ColumnSpec(DV_TTM, "DOUBLE"),
        ColumnSpec(TOTAL_SHARES, "DOUBLE"),
        ColumnSpec(FLOAT_SHARE, "DOUBLE"),
        ColumnSpec(FREE_SHARE, "DOUBLE"),
        ColumnSpec(TOTAL_MV, "DOUBLE"),
        ColumnSpec(CIRC_MV, "DOUBLE"),
        ColumnSpec(LIMIT_STATUS, "INTEGER"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, TICKER),
)

SUSPEND_D = TableSchema(
    name="suspend_d",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(SUSPEND_TIMING, "VARCHAR"),
        ColumnSpec(SUSPEND_TYPE, "VARCHAR", nullable=False),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, TICKER, SUSPEND_TYPE),
)

LIMIT_STEP = TableSchema(
    name="limit_step",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(NAME, "VARCHAR"),
        ColumnSpec(CONSECUTIVE_BOARDS, "INTEGER", nullable=False),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, TICKER, CONSECUTIVE_BOARDS),
)

THS_DAILY = TableSchema(
    name="ths_daily",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(INDEX_CODE, "VARCHAR", nullable=False),
        ColumnSpec(CLOSE, "DOUBLE"),
        ColumnSpec(OPEN, "DOUBLE"),
        ColumnSpec(HIGH, "DOUBLE"),
        ColumnSpec(LOW, "DOUBLE"),
        ColumnSpec(PRE_CLOSE, "DOUBLE"),
        ColumnSpec(AVG_PRICE, "DOUBLE"),
        ColumnSpec(CHANGE, "DOUBLE"),
        ColumnSpec(PCT_CHANGE, "DOUBLE"),
        ColumnSpec(VOLUME, "DOUBLE"),
        ColumnSpec(TURNOVER_RATE, "DOUBLE"),
        ColumnSpec(TOTAL_MV, "DOUBLE"),
        ColumnSpec(FLOAT_MV, "DOUBLE"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, INDEX_CODE),
)

STK_HIGH_SHOCK = TableSchema(
    name="stk_high_shock",
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(NAME, "VARCHAR"),
        ColumnSpec(TRADE_MARKET, "VARCHAR"),
        ColumnSpec(REASON, "VARCHAR", nullable=False),
        ColumnSpec(PERIOD, "VARCHAR", nullable=False),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, TICKER, REASON, PERIOD),
)

SYSTEM_B_STATE_OBSERVATION = TableSchema(
    name=SYSTEM_B_STATE_OBSERVATION_TABLE,
    columns=(
        ColumnSpec(ASSET_ID, "VARCHAR", nullable=False),
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(LIFECYCLE_STATE, "VARCHAR"),
        ColumnSpec(TREND_STATE, "VARCHAR"),
        ColumnSpec(PREVIOUS_TREND_STATE, "VARCHAR"),
        ColumnSpec(STATE_CHANGED, "BOOLEAN"),
        ColumnSpec(MARKET_FACT_STATUS, "VARCHAR", nullable=False),
        ColumnSpec(IS_TRADING_DAY, "BOOLEAN", nullable=False),
        ColumnSpec(LISTING_TRADING_DAY_NUMBER, "INTEGER"),
        ColumnSpec(CONFIRMED_LISTING_TRADING_DAY_COUNT, "INTEGER", nullable=False),
        ColumnSpec(LISTING_TRADING_DAY_NUMBER_IS_EXACT, "BOOLEAN", nullable=False),
        ColumnSpec(CLOSE, "DOUBLE"),
        ColumnSpec(MA5, "DOUBLE"),
        ColumnSpec(MA5_WINDOW_COMPLETE, "BOOLEAN", nullable=False),
        ColumnSpec(IS_ABOVE_OR_EQUAL_MA5, "BOOLEAN"),
        ColumnSpec(LATEST_ACTUAL_TRADE_DATE, "DATE"),
        ColumnSpec(LATEST_ACTUAL_CLOSE, "DOUBLE"),
        ColumnSpec(LATEST_ACTUAL_MA5, "DOUBLE"),
        ColumnSpec(LATEST_ACTUAL_MA5_WINDOW_COMPLETE, "BOOLEAN", nullable=False),
        ColumnSpec(LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5, "BOOLEAN"),
        ColumnSpec(PREVIOUS_ACTUAL_TRADE_DATE, "DATE"),
        ColumnSpec(PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5, "BOOLEAN"),
        ColumnSpec(PREVIOUS_ACTUAL_MA5_WINDOW_COMPLETE, "BOOLEAN", nullable=False),
        ColumnSpec(STATE_BASIS_SEQUENCE_INTACT, "BOOLEAN", nullable=False),
        ColumnSpec(ACTUAL_PAIR_CONTIGUOUS, "BOOLEAN", nullable=False),
        ColumnSpec(PRICE_ADJUSTMENT, "VARCHAR", nullable=False),
        ColumnSpec(RULE_VERSION_SET_ID, "VARCHAR", nullable=False),
        ColumnSpec(PARAMETER_SET_ID, "VARCHAR", nullable=False),
        ColumnSpec(SOURCE_RULE_IDS, "VARCHAR", nullable=False),
        ColumnSpec(DIAGNOSTICS, "VARCHAR", nullable=False),
        ColumnSpec(PRODUCTION_RUN_ID, "VARCHAR", nullable=False),
        ColumnSpec(INPUT_SNAPSHOT_ID, "VARCHAR", nullable=False),
        ColumnSpec(CALCULATION_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(
        ASSET_ID,
        TRADE_DATE,
        RULE_VERSION_SET_ID,
        PARAMETER_SET_ID,
    ),
)

SYSTEM_B_PRODUCTION_RUN = TableSchema(
    name=SYSTEM_B_PRODUCTION_RUN_TABLE,
    columns=(
        ColumnSpec(PRODUCTION_RUN_ID, "VARCHAR", nullable=False),
        ColumnSpec("run_type", "VARCHAR", nullable=False),
        ColumnSpec("status", "VARCHAR", nullable=False),
        ColumnSpec("target_start_date", "DATE"),
        ColumnSpec("target_end_date", "DATE"),
        ColumnSpec(RULE_VERSION_SET_ID, "VARCHAR", nullable=False),
        ColumnSpec(PARAMETER_SET_ID, "VARCHAR", nullable=False),
        ColumnSpec(INPUT_SNAPSHOT_ID, "VARCHAR"),
        ColumnSpec(CALCULATION_VERSION, "VARCHAR", nullable=False),
        ColumnSpec("asset_count", "INTEGER", nullable=False),
        ColumnSpec("input_row_count", "BIGINT", nullable=False),
        ColumnSpec("output_row_count", "BIGINT", nullable=False),
        ColumnSpec("error_count", "BIGINT", nullable=False),
        ColumnSpec("metrics", "VARCHAR", nullable=False),
        ColumnSpec("error_code", "VARCHAR"),
        ColumnSpec("error_detail", "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
        ColumnSpec(COMPLETED_AT, "TIMESTAMP"),
    ),
    primary_key=(PRODUCTION_RUN_ID,),
)

SYSTEM_B_EPISODE = TableSchema(
    name=SYSTEM_B_EPISODE_TABLE,
    columns=(
        ColumnSpec(EPISODE_ID, "VARCHAR", nullable=False),
        ColumnSpec(ASSET_ID, "VARCHAR", nullable=False),
        ColumnSpec(EPISODE_NO, "INTEGER", nullable=False),
        ColumnSpec(EPISODE_START_DATE, "DATE", nullable=False),
        ColumnSpec(EPISODE_CONFIRMED_DATE, "DATE", nullable=False),
        ColumnSpec(EPISODE_END_DATE, "DATE"),
        ColumnSpec(MA5_REENTRY_COUNT, "INTEGER", nullable=False),
        ColumnSpec(CREATED_RUN_ID, "VARCHAR", nullable=False),
        ColumnSpec(RULE_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(EPISODE_ID,),
)

SYSTEM_B_EPISODE_OBSERVATION = TableSchema(
    name=SYSTEM_B_EPISODE_OBSERVATION_TABLE,
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(ASSET_ID, "VARCHAR", nullable=False),
        ColumnSpec(EPISODE_ID, "VARCHAR", nullable=False),
        ColumnSpec(DAYS_SINCE_START, "INTEGER", nullable=False),
        ColumnSpec(DAYS_SINCE_CONFIRMED, "INTEGER", nullable=False),
        ColumnSpec(CLOSE, "DOUBLE", nullable=False),
        ColumnSpec(MA5, "DOUBLE", nullable=False),
        ColumnSpec(MA10, "DOUBLE", nullable=False),
        ColumnSpec(TREND_STATE, "VARCHAR", nullable=False),
        ColumnSpec(PREVIOUS_TREND_STATE, "VARCHAR"),
        ColumnSpec(STATE_TRANSITION, "VARCHAR"),
        ColumnSpec(EPISODE_RETURN, "DOUBLE", nullable=False),
        ColumnSpec(PEAK_RETURN, "DOUBLE", nullable=False),
        ColumnSpec(DRAWDOWN_FROM_PEAK, "DOUBLE", nullable=False),
        ColumnSpec(MA5_REENTRY_COUNT, "INTEGER", nullable=False),
        ColumnSpec(IS_EPISODE_CONFIRMED, "BOOLEAN", nullable=False),
        ColumnSpec(IS_EPISODE_END, "BOOLEAN", nullable=False),
        ColumnSpec(CREATED_RUN_ID, "VARCHAR", nullable=False),
        ColumnSpec(RULE_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(TRADE_DATE, ASSET_ID, RULE_VERSION),
)

SYSTEM_B_EPISODE_SEGMENT = TableSchema(
    name=SYSTEM_B_EPISODE_SEGMENT_TABLE,
    columns=(
        ColumnSpec(SEGMENT_ID, "VARCHAR", nullable=False),
        ColumnSpec(EPISODE_ID, "VARCHAR", nullable=False),
        ColumnSpec(ASSET_ID, "VARCHAR", nullable=False),
        ColumnSpec(SEGMENT_NO, "INTEGER", nullable=False),
        ColumnSpec(SEGMENT_STATE, "VARCHAR", nullable=False),
        ColumnSpec(ACTIVE_SPRINT_NO, "INTEGER"),
        ColumnSpec(ANCHOR_DATE, "DATE", nullable=False),
        ColumnSpec(START_DATE, "DATE", nullable=False),
        ColumnSpec(END_DATE, "DATE", nullable=False),
        ColumnSpec(TRADING_DAYS, "INTEGER", nullable=False),
        ColumnSpec(ANCHOR_CLOSE, "DOUBLE", nullable=False),
        ColumnSpec(START_CLOSE, "DOUBLE", nullable=False),
        ColumnSpec(END_CLOSE, "DOUBLE", nullable=False),
        ColumnSpec(SEGMENT_RETURN, "DOUBLE", nullable=False),
        ColumnSpec(PEAK_CLOSE, "DOUBLE", nullable=False),
        ColumnSpec(PEAK_DATE, "DATE", nullable=False),
        ColumnSpec(PEAK_RETURN, "DOUBLE", nullable=False),
        ColumnSpec(MAX_DRAWDOWN, "DOUBLE", nullable=False),
        ColumnSpec(IS_OPEN, "BOOLEAN", nullable=False),
        ColumnSpec(SOURCE_EPISODE_RULE_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(SEGMENT_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(CREATED_RUN_ID, "VARCHAR", nullable=False),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(SEGMENT_ID,),
)

SYSTEM_B_POOL_MEMBERSHIP = TableSchema(
    name=SYSTEM_B_POOL_MEMBERSHIP_TABLE,
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(ASSET_ID, "VARCHAR", nullable=False),
        ColumnSpec(POOL_TYPE, "VARCHAR", nullable=False),
        ColumnSpec(MEMBERSHIP_STATE, "VARCHAR", nullable=False),
        ColumnSpec(POOL_CYCLE_NO, "INTEGER", nullable=False),
        ColumnSpec(ENTRY_DATE, "DATE", nullable=False),
        ColumnSpec(EXIT_DATE, "DATE"),
        ColumnSpec(ENTRY_REASON, "VARCHAR"),
        ColumnSpec(EXIT_REASON, "VARCHAR"),
        ColumnSpec(EPISODE_ID, "VARCHAR"),
        ColumnSpec(METRICS_JSON, "VARCHAR", nullable=False),
        ColumnSpec(COMPLETED_RUN_ID, "VARCHAR", nullable=False),
        ColumnSpec(RULE_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(TRADE_DATE, ASSET_ID, POOL_TYPE),
)

SYSTEM_B_POOL_RUN = TableSchema(
    name=SYSTEM_B_POOL_RUN_TABLE,
    columns=(
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(POOL_TYPE, "VARCHAR", nullable=False),
        ColumnSpec("status", "VARCHAR", nullable=False),
        ColumnSpec(COMPLETED_RUN_ID, "VARCHAR", nullable=False),
        ColumnSpec("input_snapshot_id", "VARCHAR", nullable=False),
        ColumnSpec("asset_count", "INTEGER", nullable=False),
        ColumnSpec("membership_row_count", "INTEGER", nullable=False),
        ColumnSpec("metrics", "VARCHAR", nullable=False),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
        ColumnSpec(POOL_COMPLETED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, POOL_TYPE),
)




_PIT_META_COLUMNS = (
    ColumnSpec(SOURCE, "VARCHAR", nullable=False),
    ColumnSpec(SOURCE_RECORD_ID, "VARCHAR", nullable=False),
    ColumnSpec(REVISION_ID, "VARCHAR", nullable=False),
    ColumnSpec(INGESTED_AT, "TIMESTAMP WITH TIME ZONE", nullable=False),
)

INCOME_STATEMENT = TableSchema(
    name="income_statement",
    columns=(
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(REPORT_PERIOD, "DATE", nullable=False),
        ColumnSpec(ANNOUNCEMENT_DATE, "DATE", nullable=False),
        ColumnSpec(F_ANN_DATE, "DATE"),
        ColumnSpec(PUBLISHED_AT, "TIMESTAMP"),
        ColumnSpec(AVAILABLE_TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(REPORT_TYPE, "VARCHAR", nullable=False),
        ColumnSpec(UPDATE_FLAG, "VARCHAR", nullable=False),
        ColumnSpec(COMP_TYPE, "VARCHAR"),
        ColumnSpec(END_TYPE, "VARCHAR"),
        ColumnSpec(BASIC_EPS, "DOUBLE"),
        ColumnSpec(DILUTED_EPS, "DOUBLE"),
        ColumnSpec(TOTAL_REVENUE, "DOUBLE"),
        ColumnSpec(REVENUE, "DOUBLE"),
        ColumnSpec(OPERATE_PROFIT, "DOUBLE"),
        ColumnSpec(TOTAL_PROFIT, "DOUBLE"),
        ColumnSpec(N_INCOME, "DOUBLE"),
        ColumnSpec(N_INCOME_ATTR_P, "DOUBLE"),
        ColumnSpec(EBIT, "DOUBLE"),
        ColumnSpec(EBITDA, "DOUBLE"),
        *_PIT_META_COLUMNS,
    ),
    primary_key=(REVISION_ID,),
)

BALANCE_SHEET = TableSchema(
    name="balance_sheet",
    columns=(
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(REPORT_PERIOD, "DATE", nullable=False),
        ColumnSpec(ANNOUNCEMENT_DATE, "DATE", nullable=False),
        ColumnSpec(F_ANN_DATE, "DATE"),
        ColumnSpec(PUBLISHED_AT, "TIMESTAMP"),
        ColumnSpec(AVAILABLE_TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(REPORT_TYPE, "VARCHAR", nullable=False),
        ColumnSpec(UPDATE_FLAG, "VARCHAR", nullable=False),
        ColumnSpec(COMP_TYPE, "VARCHAR"),
        ColumnSpec(END_TYPE, "VARCHAR"),
        ColumnSpec(TOTAL_ASSETS, "DOUBLE"),
        ColumnSpec(TOTAL_LIAB, "DOUBLE"),
        ColumnSpec(TOTAL_CUR_ASSETS, "DOUBLE"),
        ColumnSpec(TOTAL_NCA, "DOUBLE"),
        ColumnSpec(TOTAL_CUR_LIAB, "DOUBLE"),
        ColumnSpec(TOTAL_NCL, "DOUBLE"),
        ColumnSpec(TOTAL_HLDR_EQY_EXC_MIN_INT, "DOUBLE"),
        ColumnSpec(TOTAL_HLDR_EQY_INC_MIN_INT, "DOUBLE"),
        ColumnSpec(MONEY_CAP, "DOUBLE"),
        ColumnSpec(ACCOUNTS_RECEIV, "DOUBLE"),
        ColumnSpec(INVENTORIES, "DOUBLE"),
        *_PIT_META_COLUMNS,
    ),
    primary_key=(REVISION_ID,),
)

CASHFLOW_STATEMENT = TableSchema(
    name="cashflow_statement",
    columns=(
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(REPORT_PERIOD, "DATE", nullable=False),
        ColumnSpec(ANNOUNCEMENT_DATE, "DATE", nullable=False),
        ColumnSpec(F_ANN_DATE, "DATE"),
        ColumnSpec(PUBLISHED_AT, "TIMESTAMP"),
        ColumnSpec(AVAILABLE_TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(REPORT_TYPE, "VARCHAR", nullable=False),
        ColumnSpec(UPDATE_FLAG, "VARCHAR", nullable=False),
        ColumnSpec(COMP_TYPE, "VARCHAR"),
        ColumnSpec(END_TYPE, "VARCHAR"),
        ColumnSpec(N_CASHFLOW_ACT, "DOUBLE"),
        ColumnSpec(N_CASHFLOW_INV_ACT, "DOUBLE"),
        ColumnSpec(N_CASH_FLOWS_FNC_ACT, "DOUBLE"),
        ColumnSpec(N_INCR_CASH_CASH_EQU, "DOUBLE"),
        ColumnSpec(C_CASH_EQU_END_PERIOD, "DOUBLE"),
        ColumnSpec(FREE_CASHFLOW, "DOUBLE"),
        *_PIT_META_COLUMNS,
    ),
    primary_key=(REVISION_ID,),
)

FINANCIAL_INDICATOR = TableSchema(
    name="financial_indicator",
    columns=(
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(REPORT_PERIOD, "DATE", nullable=False),
        ColumnSpec(ANNOUNCEMENT_DATE, "DATE", nullable=False),
        ColumnSpec(PUBLISHED_AT, "TIMESTAMP"),
        ColumnSpec(AVAILABLE_TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(UPDATE_FLAG, "VARCHAR", nullable=False),
        ColumnSpec(EPS, "DOUBLE"),
        ColumnSpec(BPS, "DOUBLE"),
        ColumnSpec(CFPS, "DOUBLE"),
        ColumnSpec(ROE, "DOUBLE"),
        ColumnSpec(ROA, "DOUBLE"),
        ColumnSpec(GROSSPROFIT_MARGIN, "DOUBLE"),
        ColumnSpec(NETPROFIT_MARGIN, "DOUBLE"),
        ColumnSpec(DEBT_TO_ASSETS, "DOUBLE"),
        ColumnSpec(CURRENT_RATIO, "DOUBLE"),
        ColumnSpec(QUICK_RATIO, "DOUBLE"),
        *_PIT_META_COLUMNS,
    ),
    primary_key=(REVISION_ID,),
)

INDUSTRY_MEMBERSHIP_HISTORY = TableSchema(
    name="industry_membership_history",
    columns=(
        ColumnSpec(ASSET_ID, "VARCHAR", nullable=False),
        ColumnSpec(CLASSIFICATION_SYSTEM, "VARCHAR", nullable=False),
        ColumnSpec(INDUSTRY_LEVEL, "INTEGER", nullable=False),
        ColumnSpec(INDUSTRY_CODE, "VARCHAR", nullable=False),
        ColumnSpec(INDUSTRY_NAME, "VARCHAR", nullable=False),
        ColumnSpec(EFFECTIVE_FROM, "DATE", nullable=False),
        ColumnSpec(EFFECTIVE_TO, "DATE"),
        ColumnSpec(AVAILABLE_TRADE_DATE, "DATE", nullable=False),
        *_PIT_META_COLUMNS,
    ),
    primary_key=(REVISION_ID,),
)

INDEX_COMPONENT_HISTORY = TableSchema(
    name="index_component_history",
    columns=(
        ColumnSpec(INDEX_CODE, "VARCHAR", nullable=False),
        ColumnSpec(ASSET_ID, "VARCHAR", nullable=False),
        ColumnSpec(SNAPSHOT_DATE, "DATE", nullable=False),
        ColumnSpec(WEIGHT, "DOUBLE"),
        ColumnSpec(EFFECTIVE_FROM, "DATE", nullable=False),
        ColumnSpec(EFFECTIVE_TO, "DATE"),
        ColumnSpec(AVAILABLE_TRADE_DATE, "DATE", nullable=False),
        *_PIT_META_COLUMNS,
    ),
    primary_key=(REVISION_ID,),
)

IRM_INTERACTION_QA = TableSchema(
    name="irm_interaction_qa",
    columns=(
        ColumnSpec(INTERACTION_PID, "VARCHAR", nullable=False),
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(COMPANY_CODE, "VARCHAR", nullable=False),
        ColumnSpec(COMPANY_SHORTNAME, "VARCHAR"),
        ColumnSpec(QUESTION_CONTENT, "TEXT"),
        ColumnSpec(REPLY_CONTENT, "TEXT"),
        ColumnSpec(QUESTION_TIME, "TIMESTAMP"),
        ColumnSpec(REPLY_TIME, "TIMESTAMP", nullable=False),
        ColumnSpec(REPLY_DATE, "DATE", nullable=False),
        ColumnSpec(NICKNAME, "VARCHAR"),
        ColumnSpec(KEYWORDS, "VARCHAR"),
        ColumnSpec(SOURCE, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(INTERACTION_PID,),
)


EARNINGS_FORECAST_EVENT = TableSchema(
    name="earnings_forecast_event",
    columns=(
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(EVENT_TYPE, "VARCHAR", nullable=False),
        ColumnSpec(EVENT_SERIES_ID, "VARCHAR", nullable=False),
        ColumnSpec(REPORT_PERIOD, "DATE", nullable=False),
        ColumnSpec(ANNOUNCEMENT_DATE, "DATE", nullable=False),
        ColumnSpec(FIRST_ANNOUNCEMENT_DATE, "DATE"),
        ColumnSpec(PUBLISHED_AT, "TIMESTAMP"),
        ColumnSpec(TIME_PRECISION, "VARCHAR", nullable=False),
        ColumnSpec(AVAILABLE_TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(FORECAST_TYPE, "VARCHAR", nullable=False),
        ColumnSpec(PROFIT_CHANGE_MIN, "DOUBLE"),
        ColumnSpec(PROFIT_CHANGE_MAX, "DOUBLE"),
        ColumnSpec(NET_PROFIT_MIN, "DOUBLE"),
        ColumnSpec(NET_PROFIT_MAX, "DOUBLE"),
        ColumnSpec(LAST_PARENT_NET, "DOUBLE"),
        ColumnSpec(SUMMARY, "TEXT"),
        ColumnSpec(CHANGE_REASON, "TEXT"),
        *_PIT_META_COLUMNS,
    ),
    primary_key=(REVISION_ID,),
)

STOCK_COLLECTION = TableSchema(
    name=STOCK_COLLECTION_TABLE,
    columns=(
        ColumnSpec(COLLECTION_ID, "VARCHAR", nullable=False),
        ColumnSpec(COLLECTION_TYPE, "VARCHAR", nullable=False),
        ColumnSpec(COLLECTION_SCOPE, "VARCHAR", nullable=False),
        ColumnSpec(NAMESPACE, "VARCHAR", nullable=False),
        ColumnSpec(SOURCE_KEY, "VARCHAR", nullable=False),
        ColumnSpec(CANONICAL_NAME, "VARCHAR", nullable=False),
        ColumnSpec(MEMBERSHIP_MODEL, "VARCHAR", nullable=False),
        ColumnSpec(STATUS, "VARCHAR", nullable=False),
        ColumnSpec(EFFECTIVE_FROM, "DATE", nullable=False),
        ColumnSpec(EFFECTIVE_TO, "DATE"),
        ColumnSpec(AVAILABLE_TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(SOURCE, "VARCHAR", nullable=False),
        ColumnSpec(SOURCE_RECORD_ID, "VARCHAR"),
        ColumnSpec(REVISION_ID, "VARCHAR", nullable=False),
        ColumnSpec(INGESTED_AT, "TIMESTAMP WITH TIME ZONE", nullable=False),
    ),
    primary_key=(COLLECTION_ID, REVISION_ID),
)

THEME = TableSchema(
    name=THEME_TABLE,
    columns=(
        ColumnSpec(THEME_ID, "VARCHAR", nullable=False),
        ColumnSpec(COLLECTION_ID, "VARCHAR", nullable=False),
        ColumnSpec(CANONICAL_NAME, "VARCHAR", nullable=False),
        ColumnSpec(STATUS, "VARCHAR", nullable=False),
        ColumnSpec(EFFECTIVE_FROM, "DATE", nullable=False),
        ColumnSpec(EFFECTIVE_TO, "DATE"),
        ColumnSpec(AVAILABLE_TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(SOURCE, "VARCHAR", nullable=False),
        ColumnSpec(SOURCE_RECORD_ID, "VARCHAR"),
        ColumnSpec(REVISION_ID, "VARCHAR", nullable=False),
        ColumnSpec(INGESTED_AT, "TIMESTAMP WITH TIME ZONE", nullable=False),
    ),
    primary_key=(THEME_ID, REVISION_ID),
)

THEME_MEMBERSHIP_HISTORY = TableSchema(
    name=THEME_MEMBERSHIP_HISTORY_TABLE,
    columns=(
        ColumnSpec(MEMBERSHIP_ID, "VARCHAR", nullable=False),
        ColumnSpec(THEME_ID, "VARCHAR", nullable=False),
        ColumnSpec(COLLECTION_ID, "VARCHAR", nullable=False),
        ColumnSpec(ASSET_ID, "VARCHAR", nullable=False),
        ColumnSpec(EFFECTIVE_FROM, "DATE", nullable=False),
        ColumnSpec(EFFECTIVE_TO, "DATE"),
        ColumnSpec(AVAILABLE_TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(SOURCE, "VARCHAR", nullable=False),
        ColumnSpec(SOURCE_RECORD_ID, "VARCHAR"),
        ColumnSpec(REVISION_ID, "VARCHAR", nullable=False),
        ColumnSpec(INGESTED_AT, "TIMESTAMP WITH TIME ZONE", nullable=False),
    ),
    primary_key=(MEMBERSHIP_ID, REVISION_ID),
)

THEME_EFFECTIVE_MEMBER_DAILY = TableSchema(
    name=THEME_EFFECTIVE_MEMBER_DAILY_TABLE,
    columns=(
        ColumnSpec(COLLECTION_ID, "VARCHAR", nullable=False),
        ColumnSpec(THEME_ID, "VARCHAR", nullable=False),
        ColumnSpec(ASSET_ID, "VARCHAR", nullable=False),
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(IS_THEME_MEMBER, "BOOLEAN", nullable=False),
        ColumnSpec(CONFIRMED_LISTING_TRADING_DAY_COUNT, "BIGINT"),
        ColumnSpec(IS_SUSPENDED, "BOOLEAN", nullable=False),
        ColumnSpec(IS_M4_EFFECTIVE_MEMBER, "BOOLEAN", nullable=False),
        ColumnSpec(EXCLUSION_REASON, "VARCHAR"),
        ColumnSpec(CALCULATION_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(INPUT_SNAPSHOT_ID, "VARCHAR"),
        ColumnSpec(PRODUCTION_RUN_ID, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
        ColumnSpec(FINALIZED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(COLLECTION_ID, TRADE_DATE, ASSET_ID),
)

THEME_CUSTOM_INDEX_DAILY = TableSchema(
    name=THEME_CUSTOM_INDEX_DAILY_TABLE,
    columns=(
        ColumnSpec(THEME_ID, "VARCHAR", nullable=False),
        ColumnSpec(COLLECTION_ID, "VARCHAR", nullable=False),
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(THEME_DAILY_RETURN, "DOUBLE"),
        ColumnSpec(INDEX_LEVEL, "DOUBLE"),
        ColumnSpec(BASE_LEVEL, "DOUBLE", nullable=False),
        ColumnSpec(EFFECTIVE_MEMBER_COUNT, "BIGINT", nullable=False),
        ColumnSpec(TOTAL_MEMBER_COUNT, "BIGINT", nullable=False),
        ColumnSpec(CALCULATION_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(PRODUCTION_RUN_ID, "VARCHAR"),
        ColumnSpec(INPUT_SNAPSHOT_ID, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(THEME_ID, TRADE_DATE),
)

THEME_CUSTOM_INDEX_STATE = TableSchema(
    name=THEME_CUSTOM_INDEX_STATE_TABLE,
    columns=(
        ColumnSpec(THEME_ID, "VARCHAR", nullable=False),
        ColumnSpec(COLLECTION_ID, "VARCHAR", nullable=False),
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(CLOSE, "DOUBLE"),
        ColumnSpec(MA5, "DOUBLE"),
        ColumnSpec(MA10, "DOUBLE"),
        ColumnSpec(TREND_STATE, "VARCHAR"),
        ColumnSpec(PREVIOUS_TREND_STATE, "VARCHAR"),
        ColumnSpec(CUSTOM_INDEX_TREND_RUN_DAYS, "BIGINT", nullable=False),
        ColumnSpec(IS_ABOVE_OR_EQUAL_MA5, "BOOLEAN"),
        ColumnSpec(STATE_CHANGED, "BOOLEAN"),
        ColumnSpec(RULE_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(PRODUCTION_RUN_ID, "VARCHAR"),
        ColumnSpec(INPUT_SNAPSHOT_ID, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(THEME_ID, TRADE_DATE),
)

THEME_CUSTOM_INDEX_EPISODE = TableSchema(
    name=THEME_CUSTOM_INDEX_EPISODE_TABLE,
    columns=(
        ColumnSpec(EPISODE_ID, "VARCHAR", nullable=False),
        ColumnSpec(THEME_ID, "VARCHAR", nullable=False),
        ColumnSpec(COLLECTION_ID, "VARCHAR", nullable=False),
        ColumnSpec(EPISODE_NO, "BIGINT", nullable=False),
        ColumnSpec(EPISODE_START_DATE, "DATE", nullable=False),
        ColumnSpec(EPISODE_CONFIRMED_DATE, "DATE", nullable=False),
        ColumnSpec(EPISODE_END_DATE, "DATE"),
        ColumnSpec(MA5_REENTRY_COUNT, "BIGINT", nullable=False),
        ColumnSpec(EPISODE_RETURN, "DOUBLE"),
        ColumnSpec(RULE_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(PRODUCTION_RUN_ID, "VARCHAR"),
        ColumnSpec(INPUT_SNAPSHOT_ID, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(EPISODE_ID,),
)

THEME_M4_OBSERVATION = TableSchema(
    name=THEME_M4_OBSERVATION_TABLE,
    columns=(
        ColumnSpec(THEME_ID, "VARCHAR", nullable=False),
        ColumnSpec(COLLECTION_ID, "VARCHAR", nullable=False),
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec(THEME_DAILY_RETURN, "DOUBLE"),
        ColumnSpec(THEME_LIMIT_UP_COUNT, "BIGINT"),
        ColumnSpec(THEME_RETURN_RANK, "BIGINT"),
        ColumnSpec(EFFECTIVE_MEMBER_COUNT, "BIGINT", nullable=False),
        ColumnSpec(TOTAL_MEMBER_COUNT, "BIGINT", nullable=False),
        ColumnSpec(COMPARISON_UNIVERSE_SIZE, "BIGINT", nullable=False),
        ColumnSpec(COMPARISON_UNIVERSE_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(CUSTOM_INDEX_TREND_STATE, "VARCHAR"),
        ColumnSpec(CUSTOM_INDEX_TREND_RUN_DAYS, "BIGINT"),
        ColumnSpec(CUSTOM_INDEX_EPISODE_ID, "VARCHAR"),
        ColumnSpec(QUALIFICATION_STATUS, "VARCHAR", nullable=False),
        ColumnSpec(CALCULATION_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(PRODUCTION_RUN_ID, "VARCHAR"),
        ColumnSpec(INPUT_SNAPSHOT_ID, "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
    ),
    primary_key=(THEME_ID, TRADE_DATE),
)

THEME_PRODUCTION_RUN = TableSchema(
    name=THEME_PRODUCTION_RUN_TABLE,
    columns=(
        ColumnSpec(PRODUCTION_RUN_ID, "VARCHAR", nullable=False),
        ColumnSpec("run_type", "VARCHAR", nullable=False),
        ColumnSpec("status", "VARCHAR", nullable=False),
        ColumnSpec("target_start_date", "DATE"),
        ColumnSpec("target_end_date", "DATE"),
        ColumnSpec(KNOWLEDGE_DATE, "DATE", nullable=False),
        ColumnSpec(CALCULATION_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(RULE_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(COMPARISON_UNIVERSE_VERSION, "VARCHAR", nullable=False),
        ColumnSpec(INPUT_SNAPSHOT_ID, "VARCHAR", nullable=False),
        ColumnSpec("theme_count", "BIGINT", nullable=False),
        ColumnSpec("total_index_rows", "BIGINT", nullable=False),
        ColumnSpec("total_observation_rows", "BIGINT", nullable=False),
        ColumnSpec("error_code", "VARCHAR"),
        ColumnSpec("error_detail", "VARCHAR"),
        ColumnSpec(CREATED_AT, "TIMESTAMP", nullable=False),
        ColumnSpec(COMPLETED_AT, "TIMESTAMP"),
    ),
    primary_key=(PRODUCTION_RUN_ID,),
)

ALL_TABLES = (
    DAILY_MARKET_SNAPSHOT,
    MARKET_PHASE,
    TRADE_EXECUTION,
    STOCK_INFO,
    STOCK_INFO_HISTORICAL_IDENTITY,
    TRADING_CALENDAR,
    ADJ_FACTOR_CHANGES,
    CNINFO_RESEARCH_VISITS,
    RESEARCH_REPORT_STOCK,
    RESEARCH_REPORT_INDUSTRY,
    INDEX_BASIC,
    INDEX_DAILY,
    ETF_DAILY,
    ETF_ADJ_FACTOR,
    ZT_POOL,
    DT_POOL,
    DAILY_BASIC,
    SUSPEND_D,
    SYSTEM_B_STATE_OBSERVATION,
    SYSTEM_B_PRODUCTION_RUN,
    SYSTEM_B_EPISODE,
    SYSTEM_B_EPISODE_OBSERVATION,
    SYSTEM_B_EPISODE_SEGMENT,
    SYSTEM_B_POOL_MEMBERSHIP,
    SYSTEM_B_POOL_RUN,
    IRM_INTERACTION_QA,
    LIMIT_STEP,
    THS_DAILY,
    STK_HIGH_SHOCK,
    INCOME_STATEMENT,
    BALANCE_SHEET,
    CASHFLOW_STATEMENT,
    FINANCIAL_INDICATOR,
    INDUSTRY_MEMBERSHIP_HISTORY,
    INDEX_COMPONENT_HISTORY,
    EARNINGS_FORECAST_EVENT,
    STOCK_COLLECTION,
    THEME,
    THEME_MEMBERSHIP_HISTORY,
    THEME_EFFECTIVE_MEMBER_DAILY,
    THEME_CUSTOM_INDEX_DAILY,
    THEME_CUSTOM_INDEX_STATE,
    THEME_CUSTOM_INDEX_EPISODE,
    THEME_M4_OBSERVATION,
    THEME_PRODUCTION_RUN,
)

TABLE_BY_NAME = {table.name: table for table in ALL_TABLES}


def get_table(name: str) -> TableSchema:
    """根据表名获取表结构

    Args:
        name: 表名，如 "daily_market_snapshot"

    Returns:
        TableSchema 实例

    Raises:
        ValueError: 表名不存在时抛出
    """
    if name not in TABLE_BY_NAME:
        raise ValueError(f"Unknown table: {name}. Available: {list(TABLE_BY_NAME.keys())}")
    return TABLE_BY_NAME[name]


def init_database(con) -> None:
    """初始化主数据库，创建除 IRM 互动问答外的所有契约表。

    IRM 互动问答表属于独立数据库 ``irm_qa.duckdb``（``irm_qa_db`` 资源），
    由 :func:`init_irm_database` 创建；主库 bootstrap 不再创建可写 IRM 表。

    Args:
        con: DuckDB 连接对象

    Example:
        import duckdb
        con = duckdb.connect("quant.db")
        init_database(con)
    """
    for table in ALL_TABLES:
        if table is IRM_INTERACTION_QA:
            continue
        con.execute(table.duckdb_create_sql())


def init_irm_database(con) -> None:
    """初始化 IRM 独立数据库，创建 ``irm_interaction_qa`` 契约表。

    Args:
        con: DuckDB 连接对象

    Example:
        import duckdb
        con = duckdb.connect("irm_qa.duckdb")
        init_irm_database(con)
    """
    con.execute(IRM_INTERACTION_QA.duckdb_create_sql())


def migrate_stock_collections_ingested_at_to_timestamptz(con, source_timezone: str) -> None:
    """显式将 StockCollection 领域表历史遗留的 naive TIMESTAMP ingested_at 迁移为 TIMESTAMPTZ。

    要求调用方显式指定 legacy naive 时间戳的原始时区 (source_timezone)，例如 'UTC' 或 'Asia/Shanghai'。
    严禁默认无条件假定。迁移通过 USING timezone(?, ingested_at) 精确解释并转为 aware TIMESTAMPTZ。
    若迁移失败或最终 schema 断言不满足，必须 fail-closed 抛出异常。
    """
    if not source_timezone or not isinstance(source_timezone, str):
        raise ValueError("Explicit source_timezone string must be provided (e.g. 'UTC' or 'Asia/Shanghai')")

    # 验证 DuckDB 是否支持该时区名
    try:
        con.execute("SELECT timezone(?, '2026-01-01 00:00:00'::TIMESTAMP)", [source_timezone])
    except Exception as exc:
        raise ValueError(f"Invalid source_timezone '{source_timezone}': {exc}") from exc

    tables = (STOCK_COLLECTION_TABLE, THEME_TABLE, THEME_MEMBERSHIP_HISTORY_TABLE)
    for tbl in tables:
        type_row = con.execute(
            """
            SELECT data_type FROM information_schema.columns
            WHERE table_name = ? AND column_name = 'ingested_at'
            """,
            [tbl],
        ).fetchone()
        if not type_row:
            continue
        data_type = type_row[0].upper()
        if data_type == "TIMESTAMP":
            con.execute(
                f"ALTER TABLE {tbl} ALTER COLUMN {INGESTED_AT} TYPE TIMESTAMPTZ USING timezone('{source_timezone}', {INGESTED_AT})"
            )

    # 迁移后严格断言 schema：三个核心表的 ingested_at 必须为 TIMESTAMP WITH TIME ZONE
    for tbl in tables:
        curr_row = con.execute(
            """
            SELECT data_type FROM information_schema.columns
            WHERE table_name = ? AND column_name = 'ingested_at'
            """,
            [tbl],
        ).fetchone()
        if curr_row:
            curr_type = curr_row[0].upper()
            if curr_type not in ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"):
                raise RuntimeError(
                    f"Schema assertion failed for {tbl}.{INGESTED_AT}: expected TIMESTAMPTZ, found {curr_type}"
                )


def init_stock_collections_database(con) -> None:
    """初始化 StockCollection 领域数据库，创建 stock_collection、theme、theme_membership_history 及 theme_effective_member_daily 契约表。

    注意：新创建的表结构本身已直接定义为 TIMESTAMPTZ。
    对于历史遗留已存在的 naive TIMESTAMP 数据库，不得在此自动隐式迁移，必须由调用方显式调用
    migrate_stock_collections_ingested_at_to_timestamptz(con, source_timezone=...)。

    Args:
        con: DuckDB 连接对象
    """
    con.execute(STOCK_COLLECTION.duckdb_create_sql())
    con.execute(THEME.duckdb_create_sql())
    con.execute(THEME_MEMBERSHIP_HISTORY.duckdb_create_sql())
    con.execute(THEME_EFFECTIVE_MEMBER_DAILY.duckdb_create_sql())

