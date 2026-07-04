"""
tushare_client.py - Tushare Pro 客户端初始化

使用方式:
    from qrp_atlas.config.tushare import get_tushare_pro

    pro = get_tushare_pro()
    df = pro.daily(trade_date="20260512")

安全说明:
    - Token 仅从 .env 文件或环境变量读取，绝不硬编码在源码中
    - 首次使用请复制 .env.example 为 .env，填入你的 TUSHARE_TOKEN

注意:
    - 通过第三方代理网关 https://fastapic.stockai888.top 访问（15000积分版）
    - 限速每分钟 100 次，客户端自带 __call__ 级限速（请求间隔 0.6s）
    - pro_bar() 等模块级函数必须手动传入 api=pro 参数
"""

import os
import time
import functools
import tushare as ts

_CUSTOM_API_URL = "https://fastapic.stockai888.top"
_RATE_LIMIT_INTERVAL = 0.6  # 100次/分钟 ≈ 0.6s/次
_last_call_time = 0.0


def _rate_limit(func):
    """简易调用级限速装饰器"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global _last_call_time
        elapsed = time.time() - _last_call_time
        if elapsed < _RATE_LIMIT_INTERVAL:
            time.sleep(_RATE_LIMIT_INTERVAL - elapsed)
        _last_call_time = time.time()
        return func(*args, **kwargs)
    return wrapper


# 从 .env 加载（兼容 dotenv）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ⚠️ Token 仅从环境变量读取，决不硬编码！
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")


def get_tushare_pro(token: str = None) -> ts.pro_api:
    """获取 tushare pro 客户端

    Args:
        token: tushare token，默认使用 TUSHARE_TOKEN（来自 .env）

    Returns:
        已配置自定义 API 地址的 tushare pro 客户端（自带调用级限速）

    Raises:
        ValueError: 未配置 TUSHARE_TOKEN 时抛出
    """
    token = token or TUSHARE_TOKEN

    if not token:
        raise ValueError(
            "TUSHARE_TOKEN 未配置！\n"
            "请复制 .env.example 为 .env，填入你的 token。\n"
        )

    pro = ts.pro_api(token)

    # ⚠️ 必须设置自定义 API 网关地址
    pro._DataApi__http_url = _CUSTOM_API_URL

    # 对 pro 对象的每个公开方法加限速
    for attr_name in dir(pro):
        if attr_name.startswith("_"):
            continue
        attr = getattr(pro, attr_name)
        if callable(attr):
            setattr(pro, attr_name, _rate_limit(attr))

    return pro


def _try_both_tokens() -> ts.pro_api:
    """获取可用的 tushare 客户端（仅尝试环境变量）"""
    token = TUSHARE_TOKEN
    if not token:
        raise ConnectionError(
            "TUSHARE_TOKEN 未配置！请复制 .env.example 为 .env 并填入 token。"
        )
    pro = get_tushare_pro(token)
    # 发一个轻量请求验证 token 有效性
    pro.index_basic(limit=1)
    return pro


# 快速验证（可直接运行）
if __name__ == "__main__":
    pro = _try_both_tokens()
    df = pro.index_basic(limit=5)
    print("index_basic 测试:")
    print(df)
    df2 = ts.pro_bar(api=pro, ts_code="000001.SZ", limit=3)
    print("\npro_bar 测试:")
    print(df2)
    print("\n✅ tushare 客户端初始化成功")