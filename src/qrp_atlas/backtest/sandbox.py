"""非持久化的交互式策略沙盒。

用途：为前端研究台（Monaco 编辑器 + 运行控制台）提供“代码即写即跑”的能力。
本模块是**旁路研究能力**，不参与正式 Backtest Task / Job / Pipeline：

- 行情只读（DuckDB read_only 连接），不写任何数据库；
- 不调用 ``BacktestRunWriter``，不创建 task，不落业务表；
- 撮合、成本与绩效全部复用既有 ``PortfolioBacktestEngine`` 与
  ``results.analytics`` 公共函数，不复制第二套口径。

策略协议（与前端模板一致）::

    def initialize(context): ...                       # 可选
    def handle_bar(context, market_data):              # 必需
        return {"000001.SZ": 0.5, "600519.SH": 0.5}    # 当日完整目标权重

``handle_bar`` 返回的是**当日完整目标权重快照**：未出现的既有持仓会被清算为 0，
返回空字典等价于“当日目标为全现金”。

隔离方式：每次请求派生一个独立的 ``multiprocessing`` 子进程，带硬超时掐断，
FastAPI 工作进程内绝不直接执行用户代码。

性能约定：全市场行情只用于策略选股；交给引擎结算的行情会裁剪到策略实际引用过的
标的（见 ``_engine_panel``）。引擎逐行标记全量面板的成本与市场标的数成正比，而
它只对“持仓或当日目标”中的标的取价，因此该裁剪不改变结算结果。
"""

from __future__ import annotations

import contextlib
import io
import math
import multiprocessing as mp
import time
import traceback
from queue import Empty
from types import SimpleNamespace
from typing import Any, Callable, Mapping, MutableSequence

import pandas as pd

from ..config.settings import get_settings
from .data import load_index_prices, load_stock_prices
from .models import CostRule
from .portfolio.engine import PortfolioBacktestEngine
from .portfolio.models import (
    PortfolioBacktestConfig,
    PortfolioBacktestResult,
    PortfolioExecutionRule,
)
from .results.analytics import (
    align_benchmark_series,
    annualized_return_pct,
    benchmark_summary,
    calmar_ratio,
    daily_returns_from_equity,
    json_safe,
    sharpe_ratio,
    sortino_ratio,
)
from .results.writer import portfolio_fills_to_trades

# 单次沙盒运行的硬超时上限（30 分钟）。
# 策略研究与全市场计算动辄数分钟，因此这是“防挂死”而不是“防慢”的上限：
# 正常策略远不会用满，只有死循环或不可取消的阻塞才会被掐断。
DEFAULT_TIMEOUT_SEC = 1800
MAX_POSITIONS = 100

# 沙盒运行不落库，因此不能复用正式 run_id；该标记只用于前端展示。
SANDBOX_RUN_ID = "sandbox_unpersisted"

# 前端模板使用的字段别名 → 标准 PriceFrame 列名。
_FIELD_ALIASES: dict[str, str] = {"money": "amount"}

_TARGET_COLUMNS = ("trade_date", "asset_id", "target_weight")


class SandboxMarketData:
    """交给用户策略代码的只读行情门面。

    数据在回测开始前一次性组织好：按交易日的可交易成员清单，以及按字段的
    日期 × 标的透视表。``get_history`` 只截取“当前正在回测的交易日及之前”的
    切片，用户代码无法观测到未来行情。
    """

    def __init__(self, price_df: pd.DataFrame) -> None:
        frame = price_df.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])  # type: ignore[assignment]
        frame["asset_id"] = frame["asset_id"].astype(str)
        self._frame = frame
        self._dates: list[pd.Timestamp] = sorted(
            timestamp for timestamp in frame["trade_date"].unique()
        )
        self._pivots: dict[str, pd.DataFrame] = {}
        self._universe = self._build_universe(frame)
        self.current_date: pd.Timestamp | None = None

    @property
    def trading_dates(self) -> list[pd.Timestamp]:
        """回测区间内的全部交易日（升序）。"""

        return list(self._dates)

    @staticmethod
    def _build_universe(frame: pd.DataFrame) -> dict[pd.Timestamp, list[str]]:
        """预组织每个交易日的可交易股票（剔除停牌与指数）。

        指数不是“全市场候选池”的成员；策略需要指数时通过 ``get_history`` 显式指定。
        """

        if "asset_type" in frame.columns:
            frame = frame[frame["asset_type"] == "stock"]
        universe: dict[pd.Timestamp, list[str]] = {}
        for trade_date, part in frame.groupby("trade_date", sort=True):
            tradable = part
            if "is_suspended" in part.columns:
                suspended = part["is_suspended"].fillna(False).astype(bool)
                tradable = part.loc[~suspended]
            universe[trade_date] = tradable["asset_id"].tolist()
        return universe

    def get_active_universe(self, date: Any) -> list[str]:
        """返回指定交易日上市且未停牌的标的列表。"""

        return list(self._universe.get(pd.Timestamp(date), []))

    def get_history(
        self,
        tickers: Any,
        field: str = "close",
        bars: int = 20,
    ) -> pd.DataFrame:
        """返回指定标的历史切片（行是交易日、列是标的）。

        只包含当前正在回测的交易日及之前的 ``bars`` 个交易日，绝不包含未来数据。
        ``field`` 支持前端模板使用的别名，例如 ``money`` → ``amount``。
        """

        resolved = _FIELD_ALIASES.get(str(field), str(field))
        pivot = self._pivot(resolved)
        columns = list(dict.fromkeys(str(ticker) for ticker in tickers))
        selected = pivot.reindex(columns=columns)
        if self.current_date is not None:
            selected = selected.loc[: self.current_date]
        return selected.tail(int(bars))

    def _pivot(self, field: str) -> pd.DataFrame:
        """按字段惰性构建一次透视表，避免每日重新透视全市场。"""

        pivot = self._pivots.get(field)
        if pivot is None:
            if field not in self._frame.columns:
                available = ", ".join(sorted(str(name) for name in self._frame.columns))
                raise ValueError(
                    f"market_data 不支持字段 {field!r}；可用字段: {available}"
                )
            pivot = self._frame.pivot(
                index="trade_date", columns="asset_id", values=field
            ).sort_index()
            self._pivots[field] = pivot
        return pivot


def _iso_date(value: Any) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _captured_lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.strip()]


def _sanitize_code(value: Any, *, iso: str, asset_id: str) -> float:
    try:
        weight = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"[{iso}] {asset_id} 的目标权重不是数字: {value!r}"
        ) from exc
    if not math.isfinite(weight):
        raise ValueError(f"[{iso}] {asset_id} 的目标权重不是有限数值: {value!r}")
    if weight < 0:
        raise ValueError(f"[{iso}] {asset_id} 的目标权重不能为负: {weight}")
    return weight


def _normalize_decision(decision: Any, iso: str) -> dict[str, float]:
    """把 ``handle_bar`` 的返回值规范成 ``{asset_id: target_weight}``。"""

    if decision is None:
        return {}
    if not isinstance(decision, Mapping):
        raise TypeError(
            f"[{iso}] handle_bar 必须返回 {{asset_id: target_weight}} 字典，"
            f"实际返回 {type(decision).__name__}。"
        )
    targets: dict[str, float] = {}
    for key, value in decision.items():
        asset_id = str(key).strip()
        if not asset_id:
            raise ValueError(f"[{iso}] handle_bar 返回了空标的代码。")
        targets[asset_id] = _sanitize_code(value, iso=iso, asset_id=asset_id)
    total = sum(targets.values())
    if total > 1.0 + 1e-9:
        raise ValueError(f"[{iso}] 当日目标权重合计 {total:.6f} 超过 1.0。")
    return targets


def _compile_strategy(
    code: str,
    *,
    start_date: str,
    end_date: str,
    initial_cash: float,
) -> tuple[SimpleNamespace, Callable[..., Any]]:
    """编译用户代码并执行 ``initialize``，返回 (context, handle_bar)。"""

    environment: dict[str, Any] = {"__name__": "__sandbox_strategy__"}
    exec(compile(code, "<qrp_sandbox_strategy>", "exec"), environment)

    handle_bar = environment.get("handle_bar")
    if not callable(handle_bar):
        raise ValueError(
            "策略代码必须定义 handle_bar(context, market_data) 函数。"
        )

    context = SimpleNamespace(
        initial_cash=float(initial_cash),
        start_date=start_date,
        end_date=end_date,
        current_date=start_date,
    )
    initialize = environment.get("initialize")
    if initialize is not None:
        if not callable(initialize):
            raise ValueError("initialize 必须是可调用对象。")
        initialize(context)
    return context, handle_bar


def _duckdb_price_loader(
    start_date: str,
    end_date: str,
) -> Callable[[], tuple[pd.DataFrame, pd.DataFrame]]:
    """构造只读行情加载器；连接在子进程内按需建立并立即关闭。"""

    def load() -> tuple[pd.DataFrame, pd.DataFrame]:
        db_path = get_settings().paths.duckdb_path
        stocks = load_stock_prices(
            db_path=db_path, start_date=start_date, end_date=end_date
        )
        indices = load_index_prices(
            db_path=db_path, start_date=start_date, end_date=end_date
        )
        return stocks, indices

    return load


def _combine_price_frames(
    stocks: pd.DataFrame,
    indices: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
    log: MutableSequence[str],
) -> pd.DataFrame:
    """合并股票与指数行情成单个 PriceFrame。"""

    parts = [
        frame
        for frame in (stocks, indices)
        if isinstance(frame, pd.DataFrame) and not frame.empty
    ]
    if not parts:
        raise ValueError(
            f"区间 {start_date} 至 {end_date} 内无任何行情数据，无法运行策略。"
        )
    combined = pd.concat(parts, ignore_index=True) if len(parts) > 1 else parts[0].copy()
    combined["asset_id"] = combined["asset_id"].astype(str)
    combined = combined.sort_values(
        ["asset_id", "trade_date"], kind="mergesort"
    ).reset_index(drop=True)
    log.append(
        f"[沙盒引擎] 行情装载完成: 股票 {0 if stocks is None else len(stocks)} 行, "
        f"指数 {0 if indices is None else len(indices)} 行。"
    )
    return combined


def _select_benchmark(
    indices: pd.DataFrame,
    benchmark_id: str | None,
    *,
    log: MutableSequence[str],
) -> tuple[str | None, pd.DataFrame | None]:
    """从指数行情中取出基准切片；缺失时如实记录，不做静默替代。"""

    if not benchmark_id:
        return None, None
    resolved = str(benchmark_id).strip().upper() or None
    if resolved is None:
        return None, None
    if indices is None or indices.empty:
        log.append(f"[沙盒引擎] index_daily 无数据，基准 {resolved} 不可用。")
        return resolved, None
    subset = indices[indices["asset_id"] == resolved]
    if subset.empty:
        available = ", ".join(sorted(indices["asset_id"].unique().tolist()))
        log.append(
            f"[沙盒引擎] 基准 {resolved} 在 index_daily 中不存在，"
            f"基准与超额指标将为空。当前可用指数: {available}"
        )
        return resolved, None
    return resolved, subset


def _build_summary(
    result: PortfolioBacktestResult,
    equity_curve: list[dict[str, Any]],
    daily_returns: list[float | None],
    benchmark_id: str | None,
    benchmark_frame: pd.DataFrame | None,
) -> dict[str, Any]:
    """组装与前端 ``BacktestSummary`` 对齐的绩效摘要。

    绩效口径全部复用既有实现：引擎 summary、``results.analytics`` 公共纯函数、
    ``portfolio_fills_to_trades`` 成交配对，不新建第二套算法。
    """

    trades = portfolio_fills_to_trades(result)
    closed = [trade for trade in trades if trade.get("status") == "closed"]
    returns = [
        float(trade["return_pct"])
        for trade in closed
        if trade.get("return_pct") is not None
    ]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    holdings = [
        int(trade["holding_days"])
        for trade in closed
        if trade.get("holding_days") is not None
    ]
    avg_win = sum(wins) / len(wins) if wins else None
    avg_loss = sum(losses) / len(losses) if losses else None
    profit_loss_ratio = (
        avg_win / abs(avg_loss)
        if avg_win is not None and avg_loss not in (None, 0)
        else None
    )

    max_drawdown_pct = float(result.summary["max_drawdown_pct"])
    annual_return_pct = (
        annualized_return_pct(
            result.summary.get("total_return"),
            result.snapshots[0].trade_date,
            result.snapshots[-1].trade_date,
        )
        if len(result.snapshots) >= 2
        else None
    )

    summary: dict[str, Any] = {
        "run_id": SANDBOX_RUN_ID,
        "total_return_pct": float(result.summary["total_return_pct"]),
        "annual_return_pct": annual_return_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "sharpe": sharpe_ratio(daily_returns),
        "sortino": sortino_ratio(daily_returns),
        "calmar": calmar_ratio(annual_return_pct, max_drawdown_pct),
        "win_rate_pct": (len(wins) / len(closed) * 100.0) if closed else None,
        "profit_loss_ratio": profit_loss_ratio,
        "trade_count": len(closed),
        "avg_holding_days": (sum(holdings) / len(holdings)) if holdings else None,
        "max_trade_loss_pct": min(losses) if losses else None,
        "max_trade_profit_pct": max(wins) if wins else None,
        "skipped_count": int(result.summary["skipped_count"]),
        "turnover": float(result.summary["turnover"]),
        "commission": float(result.summary["commission"]),
        "stamp_tax": float(result.summary["stamp_tax"]),
        "slippage_cost": float(result.summary["slippage_cost"]),
        "total_cost": float(result.summary["total_cost"]),
        "final_equity": float(result.summary["final_equity"]),
        "benchmark_id": benchmark_id,
        "benchmark_total_return_pct": None,
        "portfolio_total_return_pct": None,
        "excess_percentage_point_pct": None,
        "relative_return_pct": None,
        "excess_total_return_pct": None,
        "full_range_excess_available": None,
        "benchmark_sharpe": None,
        "excess_sharpe": None,
        "daily_active_sharpe": None,
    }

    if benchmark_frame is not None and not benchmark_frame.empty:
        aligned, _diagnostics = align_benchmark_series(
            [point["date"] for point in equity_curve],
            benchmark_frame,
            portfolio_returns=daily_returns,
        )
        bench = benchmark_summary(aligned)
        summary.update(
            {
                "benchmark_total_return_pct": bench["benchmark_total_return_pct"],
                "portfolio_total_return_pct": bench["portfolio_total_return_pct"],
                "excess_percentage_point_pct": bench["excess_percentage_point_pct"],
                "relative_return_pct": bench["relative_return_pct"],
                "excess_total_return_pct": bench["excess_total_return_pct"],
                "full_range_excess_available": bench["full_range_excess_available"],
                "benchmark_sharpe": bench["benchmark_sharpe"],
                "excess_sharpe": bench["excess_sharpe"],
                "daily_active_sharpe": bench["daily_active_sharpe"],
            }
        )
    return json_safe(summary)


def _engine_panel(
    price_df: pd.DataFrame,
    referenced: set[str],
    trading_dates: list[pd.Timestamp],
    *,
    log: MutableSequence[str],
) -> pd.DataFrame:
    """把交给引擎的行情裁剪到策略实际引用过的标的。

    引擎逐行标记全量面板的成本与市场标的数成正比，与策略实际持仓规模无关；
    而引擎只对“持仓或当日目标”中的标的取价，因此裁剪到策略引用过的标的集合
    不会改变结算结果。若裁剪后面板缺失任一交易日，则回退到全量面板。
    """

    if not referenced:
        return price_df
    trimmed = price_df[price_df["asset_id"].isin(referenced)]
    if trimmed.empty:
        return price_df
    covered = pd.DatetimeIndex(trimmed["trade_date"].unique())
    required = pd.DatetimeIndex(trading_dates)
    if not bool(required.isin(covered).all()):
        log.append("[沙盒引擎] 裁剪面板缺少部分交易日，回退到全量行情面板结算。")
        return price_df
    log.append(
        f"[沙盒引擎] 结算面板裁剪: {price_df['asset_id'].nunique()} → "
        f"{trimmed['asset_id'].nunique()} 个标的，{len(price_df)} → {len(trimmed)} 行。"
    )
    return trimmed.reset_index(drop=True)


def run_strategy(
    *,
    code: str,
    start_date: str,
    end_date: str,
    initial_cash: float,
    price_loader: Callable[[], tuple[pd.DataFrame, pd.DataFrame]],
    benchmark_id: str | None = None,
    notes: MutableSequence[str] | None = None,
) -> dict[str, Any]:
    """在已装载的行情上运行策略并返回 summary / equity_points。

    先编译用户代码、再加载行情：语法与初始化错误可以快速失败，不必等待取数。
    """

    log: MutableSequence[str] = notes if notes is not None else []
    context, handle_bar = _compile_strategy(
        code,
        start_date=start_date,
        end_date=end_date,
        initial_cash=initial_cash,
    )
    log.append("[沙盒引擎] 策略代码编译成功，开始只读装载行情...")

    stocks, indices = price_loader()
    price_df = _combine_price_frames(
        stocks, indices, start_date=start_date, end_date=end_date, log=log
    )
    resolved_benchmark_id, benchmark_frame = _select_benchmark(
        indices, benchmark_id, log=log
    )

    market_data = SandboxMarketData(price_df)
    trading_dates = market_data.trading_dates
    if not trading_dates:
        raise ValueError(
            f"区间 {start_date} 至 {end_date} 内无任何行情数据，无法运行策略。"
        )
    log.append(
        f"[沙盒引擎] 回测区间 {trading_dates[0].date()} 至 {trading_dates[-1].date()}，"
        f"共 {len(trading_dates)} 个交易日，"
        f"标的 {price_df['asset_id'].nunique()} 个，初始资金 {float(initial_cash):,.2f}。"
    )

    rows: list[dict[str, Any]] = []
    referenced: set[str] = set()
    previous_assets: set[str] = set()
    for trade_date in trading_dates:
        iso = _iso_date(trade_date)
        market_data.current_date = trade_date
        context.current_date = iso
        day_targets = _normalize_decision(handle_bar(context, market_data), iso)
        for asset_id, weight in day_targets.items():
            rows.append(
                {
                    "trade_date": iso,
                    "asset_id": asset_id,
                    "target_weight": weight,
                }
            )
        # handle_bar 返回的是当日完整目标：上一快照中的标的若未再出现，必须显式
        # 归零，当日快照才会被引擎识别为“已清算”，否则会被当成“保持不动”。
        for asset_id in sorted(previous_assets - set(day_targets)):
            rows.append(
                {"trade_date": iso, "asset_id": asset_id, "target_weight": 0.0}
            )
        referenced.update(day_targets)
        previous_assets = set(day_targets)

    if not rows:
        raise ValueError("策略在整个回测区间内未输出任何目标持仓权重，无法结算。")
    target_weights_df = pd.DataFrame(rows, columns=list(_TARGET_COLUMNS))
    engine_panel = _engine_panel(price_df, referenced, trading_dates, log=log)

    config = PortfolioBacktestConfig(
        name="sandbox_run",
        initial_cash=float(initial_cash),
        max_positions=MAX_POSITIONS,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0003, stamp_tax_rate=0.001, slippage_bps=2.0),
        execution=PortfolioExecutionRule(),
    )
    result = PortfolioBacktestEngine().run(
        price_df=engine_panel,
        target_weights_df=target_weights_df,
        config=config,
    )

    equity_curve = [
        {
            "date": snapshot.trade_date,
            "equity": float(snapshot.equity),
            "drawdown_pct": float(snapshot.drawdown) * 100.0,
        }
        for snapshot in result.snapshots
    ]
    daily_returns = [
        row.get("daily_return")
        for row in daily_returns_from_equity(equity_curve)
    ]

    skipped = int(result.summary["skipped_count"])
    if skipped:
        no_price = sum(
            1
            for order in result.orders
            if order.reason == "NO_PRICE_DATA"
        )
        log.append(
            f"[沙盒引擎] 未成交委托 {skipped} 笔，其中标的缺行情 {no_price} 笔"
            "（策略引用了区间内没有行情的标的，如指数或退市标的）。"
        )
    log.append(
        f"[沙盒引擎] 结算完成: 最终权益 {float(result.summary['final_equity']):,.2f}，"
        f"累计收益 {float(result.summary['total_return_pct']):.2f}%。"
    )

    return {
        "summary": _build_summary(
            result,
            equity_curve,
            daily_returns,
            resolved_benchmark_id,
            benchmark_frame,
        ),
        "equity_points": json_safe(equity_curve),
    }


def _worker(payload: dict[str, Any], result_queue: Any) -> None:
    """子进程入口：隔离执行用户代码并回传结果。"""

    started = time.perf_counter()
    buffer = io.StringIO()
    notes: list[str] = []
    outcome: dict[str, Any]
    try:
        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            produced = run_strategy(
                code=payload["code"],
                start_date=payload["start_date"],
                end_date=payload["end_date"],
                initial_cash=float(payload["initial_cash"]),
                benchmark_id=payload.get("benchmark_id"),
                price_loader=_duckdb_price_loader(
                    payload["start_date"], payload["end_date"]
                ),
                notes=notes,
            )
            outcome = {
                "success": True,
                "summary": produced["summary"],
                "equity_points": produced["equity_points"],
                "logs": [],
                "error_message": None,
            }
    except Exception:
        outcome = {
            "success": False,
            "summary": None,
            "equity_points": [],
            "logs": [],
            "error_message": traceback.format_exc(),
        }
    outcome["logs"] = list(notes) + _captured_lines(buffer.getvalue())
    outcome["duration_ms"] = int((time.perf_counter() - started) * 1000)
    result_queue.put(outcome)


def execute_sandbox_code(
    payload: Mapping[str, Any],
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> dict[str, Any]:
    """主进程调度入口：一次请求一个子进程，带硬超时掐断。

    子进程与数据库操作全部隔离在外，FastAPI 工作进程不会直接执行用户代码。
    """

    context = mp.get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(target=_worker, args=(dict(payload), result_queue))
    process.start()
    process.join(timeout_sec)

    if process.is_alive():
        process.terminate()
        process.join(5)
        if process.is_alive():
            process.kill()
            process.join(5)
        confirmed = "已确认终止" if not process.is_alive() else "未能确认终止"
        result_queue.close()
        return {
            "success": False,
            "summary": None,
            "equity_points": [],
            "logs": [
                f"[沙盒超时] 策略运行超过 {timeout_sec} 秒上限，子进程已强制中断"
                f"（{confirmed}）。请检查是否存在死循环或过重的全市场计算。"
            ],
            "error_message": (
                f"TimeoutError: 沙盒执行超过 {timeout_sec} 秒上限，已强制中断。"
            ),
            "duration_ms": timeout_sec * 1000,
        }

    try:
        return result_queue.get(timeout=2.0)
    except Empty:
        return {
            "success": False,
            "summary": None,
            "equity_points": [],
            "logs": ["[沙盒异常] 子进程已退出但未返回任何结果。"],
            "error_message": "SandboxError: 沙盒子进程异常退出，未产出结果。",
            "duration_ms": 0,
        }
    finally:
        result_queue.close()


__all__ = [
    "DEFAULT_TIMEOUT_SEC",
    "SANDBOX_RUN_ID",
    "SandboxMarketData",
    "execute_sandbox_code",
    "run_strategy",
]
