"""qrp_atlas.backtest.results - 回测结果读取层。

从本地 JSON 文件读取回测结果，对外暴露 service 函数和 schema。
不依赖数据库，未来切换 DuckDB 时替换 loader 即可。
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
    "list_runs",
    "get_run_meta",
    "get_summary",
    "get_equity",
    "get_trades",
    "get_skipped",
    "get_config",
]
