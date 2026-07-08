"""
qrp_atlas.backtest - 通用回测引擎

通用回测引擎 v0.1，只关心: 资产 / 日期 / 价格 / 信号 / 入场 / 出场 / 仓位 / 成本 / 交易 / 收益 / 风险。
不包含任何具体策略概念（节点 / 涨停 / 五日线 / Ryan 系统等），这些属于外部信号生成器。

子模块:
- models.py:      数据结构、规则配置、结果对象
- validators.py:  price_df / signals_df / config 校验
- broker.py:      单笔交易撮合、入场出场 bar 定位、收益计算、MAE/MFE
- metrics.py:     trades 汇总指标
- engine.py:      BacktestEngine.run 主流程编排
- data.py:        项目数据库适配层（不属于引擎核心）
"""

from .models import (
    BacktestConfig,
    BacktestResult,
    CostRule,
    EntryRule,
    ExitRule,
    PositionRule,
    Skipped,
    Trade,
)
from .engine import BacktestEngine

__all__ = [
    "BacktestConfig",
    "BacktestResult",
    "CostRule",
    "EntryRule",
    "ExitRule",
    "PositionRule",
    "Skipped",
    "Trade",
    "BacktestEngine",
]
