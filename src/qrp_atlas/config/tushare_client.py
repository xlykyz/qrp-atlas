"""
tushare_client.py - Tushare Pro 客户端初始化

使用方式:
    from qrp_atlas.config.tushare import get_tushare_pro

    pro = get_tushare_pro()
    df = pro.daily(trade_date="20260512")

安全说明:
    - Token 仅从 .env 文件或环境变量读取，绝不硬编码在源码中
    - 首次使用请复制 .env.example 为 .env，填入你的 TUSHARE_TOKEN
    - 获取地址: https://tushare.pro/user/token

注意:
    - 通过自定义 API 网关 http://124.220.22.110:8020/ 访问
"""

import os
import tushare as ts

_CUSTOM_API_URL = "http://124.220.22.110:8020/"

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
        已配置自定义 API 地址的 tushare pro 客户端

    Raises:
        ValueError: 未配置 TUSHARE_TOKEN 时抛出
    """
    token = token or TUSHARE_TOKEN

    if not token:
        raise ValueError(
            "TUSHARE_TOKEN 未配置！\n"
            "请复制 .env.example 为 .env，填入你的 token。\n"
            "获取地址: https://tushare.pro/user/token"
        )

    pro = ts.pro_api(token)

    # ⚠️ 必须设置自定义 API 网关地址
    pro._DataApi__http_url = _CUSTOM_API_URL

    return pro


def _try_both_tokens() -> ts.pro_api:
    """获取可用的 tushare 客户端（仅尝试环境变量）"""
    token = TUSHARE_TOKEN
    if not token:
        raise ConnectionError(
            "TUSHARE_TOKEN 未配置！请复制 .env.example 为 .env 并填入 token。"
        )
    pro = ts.pro_api(token)
    pro._DataApi__http_url = _CUSTOM_API_URL
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
