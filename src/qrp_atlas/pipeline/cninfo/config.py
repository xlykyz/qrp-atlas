"""
config.py - cninfo 调研公告数据管道配置
"""

import random


# -- 东方财富 API 配置 --
EASTMONEY_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EASTMONEY_REPORT = "RPT_ORG_SURVEY"
EASTMONEY_PAGE_SIZE = 50
EASTMONEY_SORT_COLUMNS = "NOTICE_DATE"
EASTMONEY_SOURCE = "WEB"
EASTMONEY_CLIENT = "WEB"

EASTMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com/jgdy/xx.html",
}

# 请求间隔：随机 1~5 秒
EASTMONEY_INTERVAL_MIN = 1
EASTMONEY_INTERVAL_MAX = 5


def build_eastmoney_filter(date_str: str) -> str:
    """构建东财日期过滤"""
    return f'(IS_SOURCE="1")(NOTICE_DATE=\'{date_str} 00:00:00\')'


def eastmoney_sleep_interval() -> float:
    """随机休眠时间（秒）"""
    return random.uniform(EASTMONEY_INTERVAL_MIN, EASTMONEY_INTERVAL_MAX)
