"""Product orchestration for classic strategy backtest tasks."""

from __future__ import annotations

import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from qrp_atlas.backtest.data import load_stock_prices
from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import PortfolioBacktestConfig, PortfolioExecutionRule
from qrp_atlas.backtest.portfolio.strategy import strategy_decisions_to_target_weights
from qrp_atlas.backtest.portfolio.engine import PortfolioBacktestEngine
from qrp_atlas.backtest.results import BacktestRunWriter
from qrp_atlas.backtest.runtime.strategy import prepare_strategy_data
from qrp_atlas.strategies import StrategyInput, get_strategy
from qrp_atlas.strategies.registry import StrategyNotFoundError
from qrp_atlas.strategies.validation import StrategyValidationError, resolve_parameters

from .catalog import PRODUCT_SUPPORTED_STRATEGY_CODES
from .schemas import (
    BacktestTaskRecord,
    CreateBacktestTaskRequest,
    CreateBacktestTaskResponse,
)
from .task_store import BacktestTaskStore
from .timing import (
    REASON_NO_EXECUTION_DATE_IN_RANGE,
    market_trade_dates,
    shift_target_weights_to_execution_dates,
)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ENTRY_TIMING = frozenset({"next_open", "same_close", "next_close"})


class BacktestTaskValidationError(ValueError):
    """Raised when a product task request fails validation."""


class BacktestTaskExecutionError(RuntimeError):
    """Raised when a validated task cannot complete successfully."""


def default_product_runs_dir() -> Path:
    """Product and result API share one SSOT: QRP_ATLAS_BACKTEST_RUNS_DIR."""

    import os
    from qrp_atlas.config.paths import PROJECT_ROOT

    env = os.getenv("QRP_ATLAS_BACKTEST_RUNS_DIR")
    if env:
        return Path(env)
    return PROJECT_ROOT / "data" / "backtest_runs"


def _normalize_date(value: str, field: str) -> str:
    text = str(value or "").strip()
    if _DATE_RE.match(text):
        return text
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    raise BacktestTaskValidationError(f"{field} must be YYYY-MM-DD or YYYYMMDD")


def _normalize_tickers(values: list[str] | None) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        text = str(raw).strip().upper()
        if not text:
            continue
        if text not in seen:
            cleaned.append(text)
            seen.add(text)
    return cleaned


def validate_create_request(request: CreateBacktestTaskRequest) -> CreateBacktestTaskRequest:
    """Validate and normalize a create-task request (backend is authoritative)."""

    strategy_code = str(request.strategy_code or "").strip()
    strategy_version = str(request.strategy_version or "").strip()
    if not strategy_code:
        raise BacktestTaskValidationError("strategy_code is required")
    if strategy_code not in PRODUCT_SUPPORTED_STRATEGY_CODES:
        raise BacktestTaskValidationError(
            f"strategy not supported by product path: {strategy_code}"
        )
    if not strategy_version:
        raise BacktestTaskValidationError("strategy_version is required")

    try:
        strategy = get_strategy(strategy_code, strategy_version)
    except StrategyNotFoundError as exc:
        raise BacktestTaskValidationError(str(exc)) from exc

    try:
        resolved = resolve_parameters(strategy.definition, request.strategy_params or {})
    except StrategyValidationError as exc:
        raise BacktestTaskValidationError(str(exc)) from exc

    validate_relationships = getattr(strategy, "_validate_relationships", None)
    if callable(validate_relationships):
        try:
            validate_relationships(resolved)
        except StrategyValidationError as exc:
            raise BacktestTaskValidationError(str(exc)) from exc

    start_date = _normalize_date(request.start_date, "start_date")
    end_date = _normalize_date(request.end_date, "end_date")
    if start_date > end_date:
        raise BacktestTaskValidationError("start_date must be <= end_date")

    universe_mode = str(request.universe_mode or "tickers").strip().lower()
    if universe_mode not in {"tickers", "preset"}:
        raise BacktestTaskValidationError("universe_mode must be tickers or preset")

    tickers = _normalize_tickers(request.tickers)
    universe_preset = request.universe_preset
    if universe_mode == "tickers":
        if not tickers:
            raise BacktestTaskValidationError("tickers required when universe_mode is tickers")
        universe_preset = None
    else:
        raise BacktestTaskValidationError(
            "universe_mode=preset is not supported in 07-A; provide tickers"
        )

    position = request.position
    if position.initial_cash <= 0:
        raise BacktestTaskValidationError("initial_cash must be > 0")
    if position.max_positions < 1:
        raise BacktestTaskValidationError("max_positions must be >= 1")
    if not 0 < position.max_weight_per_symbol <= 1:
        raise BacktestTaskValidationError("max_weight_per_symbol must be in (0, 1]")

    cost = request.cost
    if cost.commission_rate < 0:
        raise BacktestTaskValidationError("commission_rate must be >= 0")
    if cost.stamp_tax_rate < 0:
        raise BacktestTaskValidationError("stamp_tax_rate must be >= 0")
    if cost.slippage_bps < 0:
        raise BacktestTaskValidationError("slippage_bps must be >= 0")

    entry_timing = str(request.execution.entry_timing or "next_open").strip()
    if entry_timing not in _ENTRY_TIMING:
        raise BacktestTaskValidationError(
            f"entry_timing must be one of {sorted(_ENTRY_TIMING)}"
        )

    return CreateBacktestTaskRequest(
        name=request.name,
        strategy_code=strategy_code,
        strategy_version=strategy.definition.version,
        strategy_params=dict(resolved),
        universe_mode=universe_mode,
        universe_preset=universe_preset,
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        position=position,
        cost=cost,
        execution=request.execution.model_copy(update={"entry_timing": entry_timing}),
    )


def _execution_rule(entry_timing: str) -> PortfolioExecutionRule:
    # Execution price field only; calendar shift is handled before the engine.
    if entry_timing == "next_open":
        return PortfolioExecutionRule(price_field="open", mark_price_field="close")
    return PortfolioExecutionRule(price_field="close", mark_price_field="close")


def _universe_label(request: CreateBacktestTaskRequest) -> str:
    if request.universe_mode == "tickers":
        return ",".join(request.tickers or [])
    return request.universe_preset or "preset"


def _lookback_padding_days(strategy_code: str, params: dict[str, Any]) -> int:
    windows: list[int] = []
    for key in ("lookback", "fast_window", "slow_window", "entry_window", "exit_window", "window"):
        value = params.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            windows.append(int(value))
    if strategy_code == "system_b_basic":
        windows.append(10)
    return max(windows or [30]) + 5


def _load_prices(request: CreateBacktestTaskRequest, *, db_path: Path | None = None) -> pd.DataFrame:
    padding = _lookback_padding_days(request.strategy_code, request.strategy_params)
    start = pd.Timestamp(request.start_date) - pd.Timedelta(days=padding * 2)
    load_kwargs: dict[str, Any] = {
        "tickers": request.tickers,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": request.end_date,
    }
    if db_path is not None:
        load_kwargs["db_path"] = db_path
    try:
        price_df = load_stock_prices(**load_kwargs)
    except Exception as exc:  # noqa: BLE001
        raise BacktestTaskExecutionError(f"failed to load market data: {exc}") from exc

    if price_df is None or price_df.empty:
        raise BacktestTaskExecutionError(
            "no market data found for requested tickers and date range"
        )

    price_df = price_df.copy()
    price_df["trade_date"] = pd.to_datetime(price_df["trade_date"])
    in_window = price_df[
        (price_df["trade_date"] >= pd.Timestamp(request.start_date))
        & (price_df["trade_date"] <= pd.Timestamp(request.end_date))
    ]
    if in_window.empty:
        raise BacktestTaskExecutionError(
            "insufficient market data inside the requested date range"
        )
    present = set(in_window["asset_id"].astype(str).unique())
    missing = [ticker for ticker in (request.tickers or []) if ticker not in present]
    if missing:
        raise BacktestTaskExecutionError(
            f"missing market data for tickers: {', '.join(missing)}"
        )
    return price_df


def _formal_price_frame(price_df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    mask = (
        (price_df["trade_date"] >= pd.Timestamp(start_date))
        & (price_df["trade_date"] <= pd.Timestamp(end_date))
    )
    formal = price_df.loc[mask].copy()
    if formal.empty:
        raise BacktestTaskExecutionError(
            "insufficient market data inside the requested date range"
        )
    return formal.reset_index(drop=True)


def _run_product_portfolio(
    request: CreateBacktestTaskRequest,
    price_df: pd.DataFrame,
) -> tuple[Any, pd.DataFrame, Any, list[dict[str, str]]]:
    """Prepare warmup-isolated decisions and execute on formal range only."""

    strategy = get_strategy(request.strategy_code, request.strategy_version)
    resolved = dict(request.strategy_params)
    prepared_full = prepare_strategy_data(price_df, strategy.definition, resolved)

    formal_start = pd.Timestamp(request.start_date)
    formal_end = pd.Timestamp(request.end_date)
    prepared_formal = prepared_full[
        (pd.to_datetime(prepared_full["trade_date"]) >= formal_start)
        & (pd.to_datetime(prepared_full["trade_date"]) <= formal_end)
    ].copy()
    if prepared_formal.empty:
        raise BacktestTaskExecutionError(
            "no prepared strategy bars inside the requested date range"
        )

    strategy_result = strategy.run(
        StrategyInput(
            prepared_data=prepared_formal.reset_index(drop=True),
            parameters=resolved,
            initial_positions={},
            runtime_context={},
        )
    )

    config = PortfolioBacktestConfig(
        name=request.name or f"{request.strategy_code}@{request.strategy_version}",
        initial_cash=float(request.position.initial_cash),
        max_positions=int(request.position.max_positions),
        max_weight_per_asset=float(request.position.max_weight_per_symbol),
        cost=CostRule(
            commission_rate=float(request.cost.commission_rate),
            stamp_tax_rate=float(request.cost.stamp_tax_rate),
            slippage_bps=float(request.cost.slippage_bps),
        ),
        execution=_execution_rule(request.execution.entry_timing),
    )

    emit_unchanged_snapshots = strategy.definition.code in {
        "cross_sectional_momentum_long_only",
        "multifactor_long_only",
    }
    signal_targets = strategy_decisions_to_target_weights(
        strategy_result,
        max_positions=config.max_positions,
        max_weight_per_asset=config.max_weight_per_asset,
        default_weight=None,
        cash_buffer=0.0,
        emit_unchanged_snapshots=emit_unchanged_snapshots,
    )

    formal_prices = _formal_price_frame(price_df, request.start_date, request.end_date)
    trade_dates = market_trade_dates(formal_prices)
    execution_targets, skipped_signals = shift_target_weights_to_execution_dates(
        signal_targets,
        entry_timing=request.execution.entry_timing,
        trade_dates=trade_dates,
        end_date=request.end_date,
    )

    portfolio_result = PortfolioBacktestEngine().run(
        formal_prices,
        execution_targets,
        config,
    )
    return strategy_result, execution_targets, portfolio_result, skipped_signals


def execute_validated_task(
    request: CreateBacktestTaskRequest,
    *,
    run_id: str | None = None,
    runs_dir: Path | None = None,
    db_path: Path | None = None,
) -> tuple[str, Path]:
    """Run strategy + portfolio engine and persist a standard results package."""

    strategy = get_strategy(request.strategy_code, request.strategy_version)
    price_df = _load_prices(request, db_path=db_path)
    strategy_result, execution_targets, portfolio_result, skipped_signals = _run_product_portfolio(
        request, price_df
    )

    # Guard: all formal result dates must stay inside the request window.
    for snapshot in portfolio_result.snapshots:
        if snapshot.trade_date < request.start_date or snapshot.trade_date > request.end_date:
            raise BacktestTaskExecutionError(
                f"result date outside request range: {snapshot.trade_date}"
            )
    for order in portfolio_result.orders:
        if order.trade_date < request.start_date or order.trade_date > request.end_date:
            raise BacktestTaskExecutionError(
                f"order date outside request range: {order.trade_date}"
            )
    for fill in portfolio_result.fills:
        if fill.trade_date < request.start_date or fill.trade_date > request.end_date:
            raise BacktestTaskExecutionError(
                f"fill date outside request range: {fill.trade_date}"
            )

    resolved_run_id = run_id or f"run_{uuid.uuid4().hex[:12]}"
    writer_root = Path(runs_dir) if runs_dir is not None else default_product_runs_dir()
    writer = BacktestRunWriter(writer_root)

    config_overlay = {
        "product_request": request.model_dump(mode="json"),
        "entry_timing": request.execution.entry_timing,
        "strategy_params": dict(request.strategy_params),
        "requested_start_date": request.start_date,
        "requested_end_date": request.end_date,
        "effective_start_date": (
            portfolio_result.snapshots[0].trade_date if portfolio_result.snapshots else request.start_date
        ),
        "effective_end_date": (
            portfolio_result.snapshots[-1].trade_date if portfolio_result.snapshots else request.end_date
        ),
        "execution_semantics": {
            "signal_date": "strategy decision date after warmup-isolated prepared data",
            "entry_timing": request.execution.entry_timing,
            "same_close_warning": (
                "same_close executes on the signal bar close and is not strict point-in-time safe"
                if request.execution.entry_timing == "same_close"
                else None
            ),
            "skipped_signals": skipped_signals,
            "no_execution_date_reason": REASON_NO_EXECUTION_DATE_IN_RANGE,
        },
        "strategy_code": strategy.definition.code,
        "strategy_version": strategy.definition.version,
        "decision_count": len(strategy_result.decisions),
        "execution_target_rows": int(len(execution_targets)),
    }

    try:
        run_dir = writer.write_portfolio_run(
            portfolio_result,
            run_id=resolved_run_id,
            strategy_name=f"{strategy.definition.code}@{strategy.definition.version}",
            universe=_universe_label(request),
            name=portfolio_result.config.name,
            created_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            overwrite=False,
            config_overlay=config_overlay,
        )
    except Exception as exc:  # noqa: BLE001
        raise BacktestTaskExecutionError(f"failed to persist backtest results: {exc}") from exc

    return resolved_run_id, run_dir


class BacktestProductService:
    """Create, list, and execute product backtest tasks with file persistence."""

    def __init__(
        self,
        *,
        task_store: BacktestTaskStore | None = None,
        runs_dir: Path | None = None,
        db_path: Path | None = None,
        execute_inline: bool = True,
    ) -> None:
        self.task_store = task_store or BacktestTaskStore()
        self.runs_dir = Path(runs_dir) if runs_dir is not None else default_product_runs_dir()
        self.db_path = Path(db_path) if db_path is not None else None
        self.execute_inline = execute_inline
        self._bg_lock = threading.Lock()

    def create_task(self, request: CreateBacktestTaskRequest) -> CreateBacktestTaskResponse:
        validated = validate_create_request(request)
        record = self.task_store.create(validated)
        if self.execute_inline:
            self._run_task(record.task_id)
            record = self.task_store.get(record.task_id)
        return CreateBacktestTaskResponse(task=record)

    def list_tasks(self) -> list[BacktestTaskRecord]:
        return self.task_store.list()

    def get_task(self, task_id: str) -> BacktestTaskRecord:
        return self.task_store.get(task_id)

    def _run_task(self, task_id: str) -> BacktestTaskRecord:
        with self._bg_lock:
            record = self.task_store.get(task_id)
            if record.status not in {"pending", "running"}:
                return record
            self.task_store.update(task_id, status="running", clear_error=True)
            request = CreateBacktestTaskRequest.model_validate(record.request_snapshot)
            try:
                run_id, _ = execute_validated_task(
                    request,
                    runs_dir=self.runs_dir,
                    db_path=self.db_path,
                )
                return self.task_store.update(
                    task_id,
                    status="succeeded",
                    run_id=run_id,
                    clear_error=True,
                )
            except (BacktestTaskValidationError, BacktestTaskExecutionError) as exc:
                return self.task_store.update(
                    task_id,
                    status="failed",
                    error_message=str(exc),
                )
            except Exception as exc:  # noqa: BLE001
                return self.task_store.update(
                    task_id,
                    status="failed",
                    error_message=f"unexpected error: {exc}",
                )


_default_service: BacktestProductService | None = None
_default_lock = threading.Lock()


def get_product_service() -> BacktestProductService:
    global _default_service
    with _default_lock:
        if _default_service is None:
            _default_service = BacktestProductService()
        return _default_service


def reset_product_service_for_tests(
    service: BacktestProductService | None = None,
) -> None:
    global _default_service
    with _default_lock:
        _default_service = service
