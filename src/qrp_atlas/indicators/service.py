"""indicators 对外入口。

组合市场宽度与市场风险，提供单日市场快照的统一计算入口。核心函数接收
pandas.DataFrame，不直接连接数据库。
"""

from __future__ import annotations

import pandas as pd

from qrp_atlas.indicators.market.breadth import calculate_market_breadth
from qrp_atlas.indicators.market.risk import calculate_market_risk


def calculate_daily_market_snapshot(df: pd.DataFrame) -> dict:
    """计算单日市场快照指标。

    Args:
        df: 单日全市场行情快照。需含 ticker、pct_change；涨跌停标记和
            昨收可选（缺失时按 contracts 规则派生）。

    Returns:
        组合字典，包含：
            - breadth: 市场宽度指标 (见 calculate_market_breadth)
            - risk: 市场风险指标 (见 calculate_market_risk)
    """
    return {
        "breadth": calculate_market_breadth(df),
        "risk": calculate_market_risk(df),
    }
