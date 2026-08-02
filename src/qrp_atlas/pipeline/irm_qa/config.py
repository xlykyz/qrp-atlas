"""config.py - 全景网互动问答数据管道配置"""

import random

# -- 全景网互动问答 API --
P5W_URL = "https://ir.p5w.net/interaction/getNewSearchR.shtml"
P5W_PAGE_SIZE = 10  # 服务端硬截断，实际最多返回 10 条
P5W_SOURCE = "p5w"
P5W_REQUEST_TIMEOUT = 15.0
P5W_PROVIDER_MAX_RETRIES = 2
P5W_RETRY_BACKOFF_BASE_SECONDS = 1.0

P5W_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://ir.p5w.net/",
    "Origin": "https://ir.p5w.net",
}

# 请求间隔：随机 1.0~2.0 秒（合规限速）
P5W_INTERVAL_MIN = 1.0
P5W_INTERVAL_MAX = 2.0

# 翻页安全上限（防止服务端越界循环时无限请求）
P5W_MAX_PAGES = 50


def p5w_sleep_interval() -> float:
    """随机休眠时间（秒）"""
    return random.uniform(P5W_INTERVAL_MIN, P5W_INTERVAL_MAX)
