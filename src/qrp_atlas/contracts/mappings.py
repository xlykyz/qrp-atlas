"""
mappings.py - 各数据源字段映射

mappings.py - 各数据源字段映射
数据清洗时使用这些映射进行字段转换。

使用示例:
    from qrp_atlas.contracts import apply_mapping, AKSHARE_DAILY_BAR

    # 方式1: 使用预定义映射
    df = apply_mapping(df, "akshare_daily_bar")

    # 方式2: 获取映射字典自行处理
    mapping = get_mapping("akshare_daily_bar")
    df = df.rename(columns=mapping)

    # 方式3: 自定义映射
    custom_mapping = build_custom_mapping({"股票代码": "ticker", "交易日期": "trade_date"})
    df = df.rename(columns=custom_mapping)

支持的数据源:
    - akshare_daily_bar: AKShare日线行情
    - akshare_realtime: AKShare实时行情
"""

from typing import Dict

from .fields import (
    TICKER, TS_CODE, SYMBOL, TRADE_DATE, NAME,
    OPEN, HIGH, LOW, CLOSE, VOLUME, AMOUNT,
    PCT_CHANGE, TURNOVER, MARKET_CAP, FLOAT_CAP, PRE_CLOSE, CHANGE, AVG_PRICE,
    INFO_CODE, TITLE, STOCK_CODE, STOCK_NAME, PUBLISH_DATE,
    MARKET, EXCHANGE, AREA, INDUSTRY, FULLNAME, ENNAME, CNSPELL,
    CURR_TYPE, LIST_STATUS, IS_HS, ACT_NAME, ACT_ENT_TYPE,
    REPORT_COLUMN, REPORT_TYPE, ENCODE_URL,
    FULL_NAME, PUBLISHER, INDEX_TYPE, CATEGORY, BASE_DATE, BASE_POINT,
    LIST_DATE, DELIST_DATE, WEIGHT_RULE, DESCRIPTION, EXP_DATE,
    INDEX_CODE, CONSECUTIVE_BOARDS, TURNOVER_RATE, TOTAL_MV, FLOAT_MV,
    TRADE_MARKET, REASON, PERIOD,
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
    SECU_CODE, SEC_NAME, NOTICE_DATE, RECEIVE_DATE,
    RECEIVE_WAY, RECEIVE_PLACE, RECEPTIONIST, CONTENT,
    ADJUNCT_URL,
    INTERACTION_PID, COMPANY_CODE, COMPANY_SHORTNAME,
    QUESTION_CONTENT, REPLY_CONTENT, QUESTION_TIME, REPLY_TIME,
    NICKNAME, SOURCE,
    REPORT_PERIOD, ANNOUNCEMENT_DATE, F_ANN_DATE, UPDATE_FLAG,
    COMP_TYPE, END_TYPE, REPORT_TYPE, BASIC_EPS, DILUTED_EPS,
    TOTAL_REVENUE, REVENUE, OPERATE_PROFIT, TOTAL_PROFIT, N_INCOME, N_INCOME_ATTR_P,
    EBIT, EBITDA, TOTAL_ASSETS, TOTAL_LIAB, TOTAL_CUR_ASSETS, TOTAL_NCA,
    TOTAL_CUR_LIAB, TOTAL_NCL, TOTAL_HLDR_EQY_EXC_MIN_INT, TOTAL_HLDR_EQY_INC_MIN_INT,
    MONEY_CAP, ACCOUNTS_RECEIV, INVENTORIES, N_CASHFLOW_ACT, N_CASHFLOW_INV_ACT,
    N_CASH_FLOWS_FNC_ACT, N_INCR_CASH_CASH_EQU, C_CASH_EQU_END_PERIOD, FREE_CASHFLOW,
    EPS, BPS, CFPS, ROE, ROA, GROSSPROFIT_MARGIN, NETPROFIT_MARGIN, DEBT_TO_ASSETS,
    CURRENT_RATIO, QUICK_RATIO, ASSET_ID, INDUSTRY_CODE, INDUSTRY_NAME, EFFECTIVE_FROM,
    EFFECTIVE_TO, INDEX_CODE, SNAPSHOT_DATE, WEIGHT,
    FIRST_ANNOUNCEMENT_DATE, FORECAST_TYPE, PROFIT_CHANGE_MIN, PROFIT_CHANGE_MAX,
    NET_PROFIT_MIN, NET_PROFIT_MAX, LAST_PARENT_NET, SUMMARY, CHANGE_REASON,
)


AKSHARE_DAILY_BAR: Dict[str, str] = {
    "代码": TICKER,
    "日期": TRADE_DATE,
    "开盘": OPEN,
    "最高": HIGH,
    "最低": LOW,
    "收盘": CLOSE,
    "成交量": VOLUME,
    "成交额": AMOUNT,
    "涨跌幅": PCT_CHANGE,
    "换手率": TURNOVER,
}

AKSHARE_REALTIME: Dict[str, str] = {
    "代码": TICKER,
    "名称": NAME,
    "最新价": CLOSE,
    "涨跌幅": PCT_CHANGE,
    "涨跌额": "chg",
    "成交量": VOLUME,
    "成交额": AMOUNT,
    "振幅": "amplitude",
    "最高": HIGH,
    "最低": LOW,
    "今开": OPEN,
    "昨收": PRE_CLOSE,
    "换手率": TURNOVER,
    "市盈率-动态": "pe_ttm",
    "市净率": "pb",
    "总市值": MARKET_CAP,
    "流通市值": FLOAT_CAP,
    "涨速": "rise_speed",
    "5分钟涨跌": "min5_chg",
    "60日涨跌幅": "day60_pct",
    "年初至今涨跌幅": "ytd_pct",
}

SINA_REALTIME: Dict[str, str] = {
    "代码": TICKER,
    "名称": NAME,
    "最新价": CLOSE,
    "涨跌额": "change",
    "涨跌幅": PCT_CHANGE,
    "昨收": PRE_CLOSE,
    "今开": OPEN,
    "最高": HIGH,
    "最低": LOW,
    "成交量": VOLUME,
    "成交额": AMOUNT,
}

TUSHARE_DAILY: Dict[str, str] = {
    "ts_code": TICKER,
    "trade_date": TRADE_DATE,
    "open": OPEN,
    "high": HIGH,
    "low": LOW,
    "close": CLOSE,
    "pre_close": PRE_CLOSE,
    "pct_chg": PCT_CHANGE,
    "vol": VOLUME,
    "amount": AMOUNT,
}

TUSHARE_INDEX_DAILY: Dict[str, str] = {
    "ts_code": INDEX_CODE,
    "trade_date": TRADE_DATE,
    "open": OPEN,
    "high": HIGH,
    "low": LOW,
    "close": CLOSE,
    "pre_close": PRE_CLOSE,
    "change": CHANGE,
    "pct_chg": PCT_CHANGE,
    "vol": VOLUME,
    "amount": AMOUNT,
}

TUSHARE_INDEX_BASIC: Dict[str, str] = {
    "ts_code": INDEX_CODE,
    "name": NAME,
    "fullname": FULL_NAME,
    "market": MARKET,
    "publisher": PUBLISHER,
    "index_type": INDEX_TYPE,
    "category": CATEGORY,
    "base_date": BASE_DATE,
    "base_point": BASE_POINT,
    "list_date": LIST_DATE,
    "weight_rule": WEIGHT_RULE,
    "desc": DESCRIPTION,
    "exp_date": EXP_DATE,
}

TUSHARE_STOCK_BASIC: Dict[str, str] = {
    "ts_code": TS_CODE,
    "symbol": SYMBOL,
    "name": NAME,
    "area": AREA,
    "industry": INDUSTRY,
    "fullname": FULLNAME,
    "enname": ENNAME,
    "cnspell": CNSPELL,
    "market": MARKET,
    "exchange": EXCHANGE,
    "curr_type": CURR_TYPE,
    "list_status": LIST_STATUS,
    "list_date": LIST_DATE,
    "delist_date": DELIST_DATE,
    "is_hs": IS_HS,
    "act_name": ACT_NAME,
    "act_ent_type": ACT_ENT_TYPE,
}

TUSHARE_LIMIT_STEP: Dict[str, str] = {
    "ts_code": TICKER,
    "name": NAME,
    "trade_date": TRADE_DATE,
    "nums": CONSECUTIVE_BOARDS,
}

TUSHARE_THS_DAILY: Dict[str, str] = {
    "ts_code": INDEX_CODE,
    "trade_date": TRADE_DATE,
    "close": CLOSE,
    "open": OPEN,
    "high": HIGH,
    "low": LOW,
    "pre_close": PRE_CLOSE,
    "avg_price": AVG_PRICE,
    "change": CHANGE,
    "pct_change": PCT_CHANGE,
    "vol": VOLUME,
    "turnover_rate": TURNOVER_RATE,
    "total_mv": TOTAL_MV,
    "float_mv": FLOAT_MV,
}

TUSHARE_STK_HIGH_SHOCK: Dict[str, str] = {
    "ts_code": TICKER,
    "trade_date": TRADE_DATE,
    "name": NAME,
    "trade_market": TRADE_MARKET,
    "reason": REASON,
    "period": PERIOD,
}

EASTMONEY_RESEARCH_REPORT: Dict[str, str] = {
    "infoCode": INFO_CODE,
    "title": TITLE,
    "stockCode": STOCK_CODE,
    "stockName": STOCK_NAME,
    "publishDate": PUBLISH_DATE,
    "market": MARKET,
    "column": REPORT_COLUMN,
    "reportType": REPORT_TYPE,
    "encodeUrl": ENCODE_URL,
    "emRatingCode": EM_RATING_CODE,
    "emRatingValue": EM_RATING_VALUE,
    "emRatingName": EM_RATING_NAME,
    "lastEmRatingCode": LAST_EM_RATING_CODE,
    "lastEmRatingValue": LAST_EM_RATING_VALUE,
    "lastEmRatingName": LAST_EM_RATING_NAME,
    "sRatingCode": S_RATING_CODE,
    "sRatingName": S_RATING_NAME,
    "ratingChange": RATING_CHANGE,
    "indvAimPriceT": INDV_AIM_PRICE_T,
    "indvAimPriceL": INDV_AIM_PRICE_L,
    "predictThisYearEps": PREDICT_THIS_YEAR_EPS,
    "predictThisYearPe": PREDICT_THIS_YEAR_PE,
    "predictNextYearEps": PREDICT_NEXT_YEAR_EPS,
    "predictNextYearPe": PREDICT_NEXT_YEAR_PE,
    "predictNextTwoYearEps": PREDICT_NEXT_TWO_YEAR_EPS,
    "predictNextTwoYearPe": PREDICT_NEXT_TWO_YEAR_PE,
    "predictLastYearEps": PREDICT_LAST_YEAR_EPS,
    "predictLastYearPe": PREDICT_LAST_YEAR_PE,
    "actualLastYearEps": ACTUAL_LAST_YEAR_EPS,
    "actualLastTwoYearEps": ACTUAL_LAST_TWO_YEAR_EPS,
    "orgCode": ORG_CODE,
    "orgName": ORG_NAME,
    "orgSName": ORG_SNAME,
    "orgType": ORG_TYPE,
    "author": AUTHOR,
    "authorID": AUTHOR_ID,
    "researcher": RESEARCHER,
    "count": COUNT,
    "indvInduCode": INDV_INDU_CODE,
    "indvInduName": INDV_INDU_NAME,
    "indvIsNew": INDV_IS_NEW,
    "newListingDate": NEW_LISTING_DATE,
    "newPurchaseDate": NEW_PURCHASE_DATE,
    "newIssuePrice": NEW_ISSUE_PRICE,
    "newPeIssueA": NEW_PE_ISSUE_A,
    "attachPages": ATTACH_PAGES,
    "attachSize": ATTACH_SIZE,
    "attachType": ATTACH_TYPE,
    "industryCode": INDUSTRY_CODE,
    "industryName": INDUSTRY_NAME,
    "emIndustryCode": EM_INDUSTRY_CODE,
    "noticeContent": NOTICE_CONTENT,
    "attachUrl": ATTACH_URL,
}

EASTMONEY_RESEARCH_INDUSTRY: Dict[str, str] = EASTMONEY_RESEARCH_REPORT.copy()

EASTMONEY_RESEARCH_VISITS: Dict[str, str] = {
    "SECUCODE": SECU_CODE,
    "SECURITY_NAME_ABBR": SEC_NAME,
    "NOTICE_DATE": NOTICE_DATE,
    "RECEIVE_START_DATE": RECEIVE_DATE,
    "RECEIVE_WAY_EXPLAIN": RECEIVE_WAY,
    "RECEIVE_PLACE": RECEIVE_PLACE,
    "RECEPTIONIST": RECEPTIONIST,
    "CONTENT": CONTENT,
    "URL": ADJUNCT_URL,
}


P5W_INTERACTION_QA: Dict[str, str] = {
    "pid": INTERACTION_PID,
    "companyCode": COMPANY_CODE,
    "companyShortname": COMPANY_SHORTNAME,
    "content": QUESTION_CONTENT,
    "replyContent": REPLY_CONTENT,
    "questionerTimeStr": QUESTION_TIME,
    "replyerTimeStr": REPLY_TIME,
    "nickname": NICKNAME,
}


TUSHARE_INCOME: Dict[str, str] = {
    "ts_code": TICKER,
    "end_date": REPORT_PERIOD,
    "ann_date": ANNOUNCEMENT_DATE,
    "f_ann_date": F_ANN_DATE,
    "report_type": REPORT_TYPE,
    "update_flag": UPDATE_FLAG,
    "comp_type": COMP_TYPE,
    "end_type": END_TYPE,
    "basic_eps": BASIC_EPS,
    "diluted_eps": DILUTED_EPS,
    "total_revenue": TOTAL_REVENUE,
    "revenue": REVENUE,
    "operate_profit": OPERATE_PROFIT,
    "total_profit": TOTAL_PROFIT,
    "n_income": N_INCOME,
    "n_income_attr_p": N_INCOME_ATTR_P,
    "ebit": EBIT,
    "ebitda": EBITDA,
}

TUSHARE_BALANCESHEET: Dict[str, str] = {
    "ts_code": TICKER,
    "end_date": REPORT_PERIOD,
    "ann_date": ANNOUNCEMENT_DATE,
    "f_ann_date": F_ANN_DATE,
    "report_type": REPORT_TYPE,
    "update_flag": UPDATE_FLAG,
    "comp_type": COMP_TYPE,
    "end_type": END_TYPE,
    "total_assets": TOTAL_ASSETS,
    "total_liab": TOTAL_LIAB,
    "total_cur_assets": TOTAL_CUR_ASSETS,
    "total_nca": TOTAL_NCA,
    "total_cur_liab": TOTAL_CUR_LIAB,
    "total_ncl": TOTAL_NCL,
    "total_hldr_eqy_exc_min_int": TOTAL_HLDR_EQY_EXC_MIN_INT,
    "total_hldr_eqy_inc_min_int": TOTAL_HLDR_EQY_INC_MIN_INT,
    "money_cap": MONEY_CAP,
    "accounts_receiv": ACCOUNTS_RECEIV,
    "inventories": INVENTORIES,
}

TUSHARE_CASHFLOW: Dict[str, str] = {
    "ts_code": TICKER,
    "end_date": REPORT_PERIOD,
    "ann_date": ANNOUNCEMENT_DATE,
    "f_ann_date": F_ANN_DATE,
    "report_type": REPORT_TYPE,
    "update_flag": UPDATE_FLAG,
    "comp_type": COMP_TYPE,
    "end_type": END_TYPE,
    "n_cashflow_act": N_CASHFLOW_ACT,
    "n_cashflow_inv_act": N_CASHFLOW_INV_ACT,
    "n_cash_flows_fnc_act": N_CASH_FLOWS_FNC_ACT,
    "n_incr_cash_cash_equ": N_INCR_CASH_CASH_EQU,
    "c_cash_equ_end_period": C_CASH_EQU_END_PERIOD,
    "free_cashflow": FREE_CASHFLOW,
}

TUSHARE_FINA_INDICATOR: Dict[str, str] = {
    "ts_code": TICKER,
    "end_date": REPORT_PERIOD,
    "ann_date": ANNOUNCEMENT_DATE,
    "update_flag": UPDATE_FLAG,
    "eps": EPS,
    "bps": BPS,
    "cfps": CFPS,
    "roe": ROE,
    "roa": ROA,
    "grossprofit_margin": GROSSPROFIT_MARGIN,
    "netprofit_margin": NETPROFIT_MARGIN,
    "debt_to_assets": DEBT_TO_ASSETS,
    "current_ratio": CURRENT_RATIO,
    "quick_ratio": QUICK_RATIO,
}

TUSHARE_INDEX_MEMBER_ALL: Dict[str, str] = {
    "ts_code": ASSET_ID,
    "l1_code": "l1_code",
    "l1_name": "l1_name",
    "l2_code": "l2_code",
    "l2_name": "l2_name",
    "l3_code": "l3_code",
    "l3_name": "l3_name",
    "in_date": EFFECTIVE_FROM,
    "out_date": EFFECTIVE_TO,
    "name": NAME,
}

TUSHARE_INDEX_WEIGHT: Dict[str, str] = {
    "index_code": INDEX_CODE,
    "con_code": ASSET_ID,
    "trade_date": SNAPSHOT_DATE,
    "weight": WEIGHT,
}


TUSHARE_FORECAST: Dict[str, str] = {
    "ts_code": TICKER,
    "ann_date": ANNOUNCEMENT_DATE,
    "end_date": REPORT_PERIOD,
    "type": FORECAST_TYPE,
    "p_change_min": PROFIT_CHANGE_MIN,
    "p_change_max": PROFIT_CHANGE_MAX,
    "net_profit_min": NET_PROFIT_MIN,
    "net_profit_max": NET_PROFIT_MAX,
    "last_parent_net": LAST_PARENT_NET,
    "first_ann_date": FIRST_ANNOUNCEMENT_DATE,
    "summary": SUMMARY,
    "change_reason": CHANGE_REASON,
}

SOURCE_MAPPINGS = {
    "akshare_daily_bar": AKSHARE_DAILY_BAR,
    "akshare_realtime": AKSHARE_REALTIME,
    "sina_realtime": SINA_REALTIME,
    "tushare_daily": TUSHARE_DAILY,
    "tushare_index_daily": TUSHARE_INDEX_DAILY,
    "tushare_index_basic": TUSHARE_INDEX_BASIC,
    "tushare_stock_basic": TUSHARE_STOCK_BASIC,
    "tushare_limit_step": TUSHARE_LIMIT_STEP,
    "tushare_ths_daily": TUSHARE_THS_DAILY,
    "tushare_stk_high_shock": TUSHARE_STK_HIGH_SHOCK,
    "eastmoney_research_report": EASTMONEY_RESEARCH_REPORT,
    "eastmoney_research_industry": EASTMONEY_RESEARCH_INDUSTRY,
    "eastmoney_research_visits": EASTMONEY_RESEARCH_VISITS,
    "p5w_interaction_qa": P5W_INTERACTION_QA,
    "tushare_income": TUSHARE_INCOME,
    "tushare_balancesheet": TUSHARE_BALANCESHEET,
    "tushare_cashflow": TUSHARE_CASHFLOW,
    "tushare_fina_indicator": TUSHARE_FINA_INDICATOR,
    "tushare_index_member_all": TUSHARE_INDEX_MEMBER_ALL,
    "tushare_index_weight": TUSHARE_INDEX_WEIGHT,
    "tushare_forecast": TUSHARE_FORECAST,
}


def get_mapping(source: str) -> Dict[str, str]:
    """根据数据源名称获取字段映射

    Args:
        source: 数据源名称，如 "akshare_daily_bar"

    Returns:
        字段映射字典 {源字段名: 标准字段名}

    Raises:
        ValueError: 数据源名称不存在时抛出
    """
    if source not in SOURCE_MAPPINGS:
        raise ValueError(f"Unknown source: {source}. Available: {list(SOURCE_MAPPINGS.keys())}")
    return SOURCE_MAPPINGS[source]


def apply_mapping(df, source: str, drop_extra: bool = False):
    """对 DataFrame 应用字段映射

    将数据源的字段名转换为标准字段名。

    Args:
        df: pandas DataFrame
        source: 数据源名称
        drop_extra: 是否删除未映射的额外列，默认 False

    Returns:
        转换后的 DataFrame

    Example:
        df = apply_mapping(df, "akshare_daily_bar")
        df = apply_mapping(df, "akshare_daily_bar", drop_extra=True)
    """
    mapping = get_mapping(source)
    reverse_mapping = {v: k for k, v in mapping.items()}
    rename_map = {}
    for col in df.columns:
        if col in mapping:
            rename_map[col] = mapping[col]
        elif col in reverse_mapping:
            pass
    df = df.rename(columns=rename_map)
    if drop_extra:
        standard_cols = set(mapping.values())
        extra_cols = set(df.columns) - standard_cols
        if extra_cols:
            df = df.drop(columns=list(extra_cols))
    return df


def build_custom_mapping(field_pairs: Dict[str, str]) -> Dict[str, str]:
    """构建自定义字段映射

    Args:
        field_pairs: 字段对 {源字段名: 标准字段名}

    Returns:
        字段映射字典

    Example:
        mapping = build_custom_mapping({"股票代码": "ticker", "交易日期": "trade_date"})
    """
    return {src: dst for src, dst in field_pairs.items()}
