"""
tushare.py - Tushare Pro 客户端初始化

使用方式:
    from qrp_atlas.config.tushare import get_tushare_pro

    pro = get_tushare_pro()
    df = pro.daily(trade_date="20260512")

注意:
    - 通过自定义 API 网关 http://124.220.22.110:8020/ 访问
    - Token 从 .env 读取 TUSHARE_TOKEN，如不存在则用内置备用 token
    - 详细文档: http://124.220.22.110:8020/doc?token=92febdf9e55bdda40c1afe3f79f0d4182ee8bc2665ea04d2f607319ff2002772
"""

import os
import tushare as ts

# 备用 token（当 .env 中未配置时使用）
_FALLBACK_TOKEN = "92febdf9e55bdda40c1afe3f79f0d4182ee8bc2665ea04d2f607319ff2002772"
# 最低优先级备用 token（仅 120 积分，主用失败时自动降级）
_BACKUP_TOKEN = "9d29350331793168b472e10485b0a58b2bc118f4ff950aac82ec8137"
_CUSTOM_API_URL = "http://124.220.22.110:8020/"

# 从 .env 加载（兼容 dotenv）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# 优先读环境变量，没有则用备用 token
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", _FALLBACK_TOKEN)


def get_tushare_pro(token: str = None) -> ts.pro_api:
    """获取 tushare pro 客户端

    Args:
        token: tushare token，默认使用 TUSHARE_TOKEN

    Returns:
        已配置自定义 API 地址的 tushare pro 客户端

    优先级:
        ① env TUSHARE_TOKEN → ② 主用 token → ③ 备用 token（120积分）
    """
    token = token or TUSHARE_TOKEN

    pro = ts.pro_api(token)

    # ⚠️ 必须设置自定义 API 网关地址
    # 如果显示 Token 不对，请检查是否少了这行
    pro._DataApi__http_url = _CUSTOM_API_URL

    return pro


def _try_both_tokens() -> ts.pro_api:
    """依次尝试主用和备用 token，返回可用的客户端"""
    tokens = [os.getenv("TUSHARE_TOKEN"), _FALLBACK_TOKEN, _BACKUP_TOKEN]
    last_error = None
    for t in tokens:
        if not t:
            continue
        try:
            pro = ts.pro_api(t)
            pro._DataApi__http_url = _CUSTOM_API_URL
            # 发一个轻量请求验证 token 有效性
            pro.index_basic(limit=1)
            return pro
        except Exception as e:
            last_error = e
            continue
    raise ConnectionError(
        f"所有 tushare token 均不可用: {last_error}"
    )


# 快速验证（可直接运行）
if __name__ == "__main__":
    import tushare as _ts
    _pro = _ts.pro_api(TUSHARE_TOKEN)
    _pro._DataApi__http_url = _CUSTOM_API_URL
    _df = _pro.index_basic(limit=5)
    print("index_basic 测试:")
    print(_df)
    _df2 = _ts.pro_bar(api=_pro, ts_code="000001.SZ", limit=3)
    print("\npro_bar 测试:")
    print(_df2)
    print("\n✅ tushare 客户端初始化成功")
