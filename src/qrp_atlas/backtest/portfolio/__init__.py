"""Shared-cash portfolio backtest public API."""

from .engine import PortfolioBacktestEngine
from .models import (
    ORDER_FILLED,
    ORDER_PARTIALLY_FILLED,
    ORDER_REJECTED,
    PortfolioBacktestConfig,
    PortfolioBacktestResult,
    PortfolioExecutionRule,
    PortfolioFill,
    PortfolioOrder,
    PortfolioSnapshot,
    PositionSnapshot,
    REASON_BELOW_LOT_SIZE,
    REASON_INSUFFICIENT_CASH,
    REASON_INVALID_PRICE,
    REASON_LIMIT_DOWN_SELL_BLOCKED,
    REASON_LIMIT_UP_BUY_BLOCKED,
    REASON_MAX_POSITIONS_REACHED,
    REASON_NO_PRICE_DATA,
    REASON_SUSPENDED,
    REASON_T_PLUS_ONE_BLOCKED,
)
from .strategy import (
    StrategyPortfolioBacktestRun,
    run_strategy_portfolio_backtest,
    strategy_decisions_to_target_weights,
)
from .validators import validate_portfolio_config, validate_target_weights

__all__ = [
    "ORDER_FILLED",
    "ORDER_PARTIALLY_FILLED",
    "ORDER_REJECTED",
    "PortfolioBacktestConfig",
    "PortfolioBacktestEngine",
    "PortfolioBacktestResult",
    "PortfolioExecutionRule",
    "PortfolioFill",
    "PortfolioOrder",
    "PortfolioSnapshot",
    "PositionSnapshot",
    "REASON_BELOW_LOT_SIZE",
    "REASON_INSUFFICIENT_CASH",
    "REASON_INVALID_PRICE",
    "REASON_LIMIT_DOWN_SELL_BLOCKED",
    "REASON_LIMIT_UP_BUY_BLOCKED",
    "REASON_MAX_POSITIONS_REACHED",
    "REASON_NO_PRICE_DATA",
    "REASON_SUSPENDED",
    "REASON_T_PLUS_ONE_BLOCKED",
    "StrategyPortfolioBacktestRun",
    "run_strategy_portfolio_backtest",
    "strategy_decisions_to_target_weights",
    "validate_portfolio_config",
    "validate_target_weights",
]
