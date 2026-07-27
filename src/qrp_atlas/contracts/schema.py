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
    TICKER, TRADE_DATE, NAME, CREATED_AT,
    OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT,
    PCT_CHANGE, PRE_CLOSE, TURNOVER, MARKET_CAP, FLOAT_CAP,
    IS_ST, IS_LIMIT_UP, IS_LIMIT_DOWN,
    PHASE, M1_CORE, M2_FRONT, M3_IDENTIFIABLE, V_TRIGGERED, NOTES,
    TRADE_ID, ENTRY_DATE, ENTRY_PRICE, PATH_TYPE,
    HALF_SELL_TRIGGER, HALF_SELL_DATE, HALF_SELL_PRICE,
    EXIT_DATE, EXIT_PRICE, POSITION_PCT,
    EXCHANGE, MARKET, LIST_DATE, DELIST_DATE, IS_ACTIVE, UPDATED_AT,
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
    INDEX_CODE, INDEX_NAME,
    FIRST_BLOCK_TIME, CONSECUTIVE_BOARDS, BLOCK_FUND,
    CONSECUTIVE_DAYS, OPEN_COUNT,
    LAST_BLOCK_TIME, BLAST_COUNT, BLOCK_STATS,
    TOTAL_SHARES, BOARD_AMOUNT, PE_RATIO,
    TURNOVER_RATE, TURNOVER_RATE_F, VOLUME_RATIO,
    PE_TTM, PB, PS, PS_TTM,
    DV_RATIO, DV_TTM,
    FLOAT_SHARE, FREE_SHARE,
    TOTAL_MV, CIRC_MV, LIMIT_STATUS,
    SUSPEND_TIMING, SUSPEND_TYPE,
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
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(NAME, "VARCHAR"),
        ColumnSpec(EXCHANGE, "VARCHAR"),
        ColumnSpec(MARKET, "VARCHAR"),
        ColumnSpec(LIST_DATE, "DATE"),
        ColumnSpec(DELIST_DATE, "DATE"),
        ColumnSpec(IS_ACTIVE, "BOOLEAN"),
        ColumnSpec(UPDATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TICKER,),
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
        ColumnSpec(VOLUME, "BIGINT"),
        ColumnSpec(CREATED_AT, "TIMESTAMP"),
    ),
    primary_key=(TRADE_DATE, INDEX_CODE),
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




_PIT_META_COLUMNS = (
    ColumnSpec(SOURCE, "VARCHAR", nullable=False),
    ColumnSpec(SOURCE_RECORD_ID, "VARCHAR", nullable=False),
    ColumnSpec(REVISION_ID, "VARCHAR", nullable=False),
    ColumnSpec(INGESTED_AT, "TIMESTAMP", nullable=False),
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

ALL_TABLES = (DAILY_MARKET_SNAPSHOT, MARKET_PHASE, TRADE_EXECUTION, STOCK_INFO, TRADING_CALENDAR, ADJ_FACTOR_CHANGES, CNINFO_RESEARCH_VISITS, RESEARCH_REPORT_STOCK, RESEARCH_REPORT_INDUSTRY, INDEX_DAILY, ZT_POOL, DT_POOL, DAILY_BASIC, SUSPEND_D, SYSTEM_B_STATE_OBSERVATION, SYSTEM_B_PRODUCTION_RUN, SYSTEM_B_EPISODE, SYSTEM_B_EPISODE_OBSERVATION, IRM_INTERACTION_QA, INCOME_STATEMENT, BALANCE_SHEET, CASHFLOW_STATEMENT, FINANCIAL_INDICATOR, INDUSTRY_MEMBERSHIP_HISTORY, INDEX_COMPONENT_HISTORY, EARNINGS_FORECAST_EVENT)

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
    """初始化数据库，创建所有表

    Args:
        con: DuckDB 连接对象

    Example:
        import duckdb
        con = duckdb.connect("quant.db")
        init_database(con)
    """
    for table in ALL_TABLES:
        con.execute(table.duckdb_create_sql())
