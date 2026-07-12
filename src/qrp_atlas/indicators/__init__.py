"""qrp_atlas.indicators - 市场复合指标计算层。

本包基于 contracts 字段规则和已有行情数据，计算市场宽度、市场风险、
个股趋势、系统 B 基础状态等复合指标，服务于市场监测、复盘、回测和
交易系统判断。

依赖方向：contracts → indicators → review / backtest / api / frontend。

子模块：
    - market/    市场宽度与风险
    - stock/     个股趋势
    - system_b/  系统 B 基础状态检测
    - service    对外组合入口
"""

# 保留：指标元数据定义与注册表（非本次核心）
from qrp_atlas.indicators.definitions import (
    IndicatorDefinition,
    IndicatorLayer,
    IndicatorScope,
    UpdateFrequency,
)
from qrp_atlas.indicators.registry import get_indicator, list_indicators

# 新增：市场复合指标计算
from qrp_atlas.indicators.market import calculate_market_breadth, calculate_market_risk
from qrp_atlas.indicators.stock import calculate_stock_trend
from qrp_atlas.indicators.system_b import (
    detect_system_b_basic_state,
    detect_system_b_basic_state_from_prices,
)
from qrp_atlas.indicators.service import calculate_daily_market_snapshot

__all__ = [
    # 元数据定义（保留）
    "IndicatorDefinition",
    "IndicatorLayer",
    "IndicatorScope",
    "UpdateFrequency",
    "get_indicator",
    "list_indicators",
    # 市场复合指标
    "calculate_market_breadth",
    "calculate_market_risk",
    "calculate_stock_trend",
    "detect_system_b_basic_state",
    "detect_system_b_basic_state_from_prices",
    "calculate_daily_market_snapshot",
]
