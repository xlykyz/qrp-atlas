"""config.py - 个股研报管道配置"""

import random

# -- 研报列表 API --
REPORT_API_URL = "https://reportapi.eastmoney.com/report/list2"
REPORT_PAGE_SIZE = 50

REPORT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com/",
    "Content-Type": "application/json",
}

# -- 详情页 --
DETAIL_URL_TEMPLATE = "https://data.eastmoney.com/report/zw_brokerreport.jshtml?encodeUrl={encode_url}"

DETAIL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com/",
}

# -- 请求间隔：随机 1~3 秒 --
INTERVAL_MIN = 1
INTERVAL_MAX = 3


def sleep_interval() -> float:
    """随机休眠时间（秒）"""
    return random.uniform(INTERVAL_MIN, INTERVAL_MAX)
