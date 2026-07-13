"""Generic backtest engine and optional strategy runtime adapters."""

from .engine import BacktestEngine
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
from .point_in_time import select_latest_available_records
from .runtime import StrategyBacktestRun, StrategyBacktestRuntime, prepare_strategy_data, run_strategy_backtest

__all__ = [
    "BacktestConfig",
    "BacktestEngine",
    "BacktestResult",
    "CostRule",
    "EntryRule",
    "ExitRule",
    "PositionRule",
    "Skipped",
    "StrategyBacktestRun",
    "StrategyBacktestRuntime",
    "Trade",
    "prepare_strategy_data",
    "run_strategy_backtest",
    "select_latest_available_records",
]
