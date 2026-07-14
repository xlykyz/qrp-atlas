"""qrp_atlas.backtest.results - 回测结果读写层。

从本地 JSON 文件读取回测结果，对外暴露 service 函数和 schema；
组合回测可通过 BacktestRunWriter 写入同一文件契约。
"""

from .loader import BacktestRunsLoader, ResultFileMissingError, RunNotFoundError
from .schemas import (
    BacktestConfigSnapshot,
    BacktestRunMeta,
    BacktestSummary,
    BacktestTrade,
    EquityPoint,
    SkippedTrade,
)
from .service import (
    get_config,
    get_equity,
    get_run_meta,
    get_skipped,
    get_summary,
    get_trades,
    list_runs,
)
from .writer import BacktestRunWriter, portfolio_fills_to_trades
from .robustness_writer import ResidualRobustnessWriter

__all__ = [
    "BacktestRunsLoader",
    "RunNotFoundError",
    "ResultFileMissingError",
    "BacktestRunMeta",
    "BacktestSummary",
    "EquityPoint",
    "BacktestTrade",
    "SkippedTrade",
    "BacktestConfigSnapshot",
    "BacktestRunWriter",
    "ResidualRobustnessWriter",
    "portfolio_fills_to_trades",
    "list_runs",
    "get_run_meta",
    "get_summary",
    "get_equity",
    "get_trades",
    "get_skipped",
    "get_config",
]
