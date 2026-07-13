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
from .portfolio import (
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioBacktestResult,
    PortfolioExecutionRule,
    PortfolioFill,
    PortfolioOrder,
    PortfolioSnapshot,
    PositionSnapshot,
    StrategyPortfolioBacktestRun,
    run_strategy_portfolio_backtest,
    strategy_decisions_to_target_weights,
)
from .runtime import StrategyBacktestRun, StrategyBacktestRuntime, prepare_strategy_data, run_strategy_backtest

__all__ = [
    "BacktestConfig",
    "BacktestEngine",
    "BacktestResult",
    "CostRule",
    "EntryRule",
    "ExitRule",
    "PortfolioBacktestConfig",
    "PortfolioBacktestEngine",
    "PortfolioBacktestResult",
    "PortfolioExecutionRule",
    "PortfolioFill",
    "PortfolioOrder",
    "PortfolioSnapshot",
    "PositionRule",
    "PositionSnapshot",
    "Skipped",
    "StrategyBacktestRun",
    "StrategyBacktestRuntime",
    "StrategyPortfolioBacktestRun",
    "Trade",
    "prepare_strategy_data",
    "run_strategy_backtest",
    "run_strategy_portfolio_backtest",
    "strategy_decisions_to_target_weights",
]
