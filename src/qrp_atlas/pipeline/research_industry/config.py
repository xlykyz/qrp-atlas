"""config.py - 行业研报管道配置"""

import random

# -- 研报列表 API (GET) --
REPORT_API_URL = "https://reportapi.eastmoney.com/report/list"
REPORT_PAGE_SIZE = 50
REPORT_QTYPE = 1  # 行业研报

REPORT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com/",
}

# -- 详情页 --
DETAIL_URL_TEMPLATE = "https://data.eastmoney.com/report/zw_industry.jshtml?infocode={info_code}"

DETAIL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com/",
}

# -- 请求间隔 --
INTERVAL_MIN = 1
INTERVAL_MAX = 3


def sleep_interval() -> float:
    return random.uniform(INTERVAL_MIN, INTERVAL_MAX)
