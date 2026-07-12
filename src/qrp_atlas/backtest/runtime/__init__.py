"""Runtime adapters that connect strategy decisions to generic backtest execution."""

from .strategy import (
    StrategyBacktestRun,
    StrategyBacktestRuntime,
    prepare_strategy_data,
    run_strategy_backtest,
)

__all__ = [
    "StrategyBacktestRun",
    "StrategyBacktestRuntime",
    "prepare_strategy_data",
    "run_strategy_backtest",
]
