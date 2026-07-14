"""Product orchestration for classic strategy backtest tasks."""

from __future__ import annotations

import os
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
from qrp_atlas.backtest.portfolio.strategy import run_strategy_portfolio_backtest
from qrp_atlas.backtest.results import BacktestRunWriter
from qrp_atlas.config.paths import BACKTEST_RUNS_DIR, PROJECT_ROOT
from qrp_atlas.strategies import get_strategy
from qrp_atlas.strategies.registry import StrategyNotFoundError
from qrp_atlas.strategies.validation import StrategyValidationError, resolve_parameters

from .catalog import PRODUCT_SUPPORTED_STRATEGY_CODES
from .schemas import (
    BacktestTaskRecord,
    CreateBacktestTaskRequest,
    CreateBacktestTaskResponse,
)
from .task_store import BacktestTaskStore

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ENTRY_TIMING = frozenset({"next_open", "same_close", "next_close"})
_PRODUCT_RUNS_ENV = "QRP_ATLAS_PRODUCT_BACKTEST_RUNS_DIR"


class BacktestTaskValidationError(ValueError):
    """Raised when a product task request fails validation."""


class BacktestTaskExecutionError(RuntimeError):
    """Raised when a validated task cannot complete successfully."""


def default_product_runs_dir() -> Path:
    env = os.getenv(_PRODUCT_RUNS_ENV)
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

    # Surface strategy-specific cross-parameter rules before enqueue/execution.
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
        # 07-A product path only executes explicit ticker lists.
        # Preset modes remain schema-compatible but are rejected until 07-B.
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
    # Portfolio engine currently executes/mark-to-market on the decision date bar.
    # next_open uses open as execution price; close timings use close.
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
    except Exception as exc:  # noqa: BLE001 - surface data readiness cleanly
        raise BacktestTaskExecutionError(f"failed to load market data: {exc}") from exc

    if price_df is None or price_df.empty:
        raise BacktestTaskExecutionError(
            "no market data found for requested tickers and date range"
        )

    # Keep indicator warmup history, but require some bars inside the requested window.
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

    try:
        run = run_strategy_portfolio_backtest(
            request.strategy_code,
            price_df,
            config,
            parameters=request.strategy_params,
            version=request.strategy_version,
        )
    except Exception as exc:  # noqa: BLE001
        raise BacktestTaskExecutionError(f"strategy/portfolio execution failed: {exc}") from exc

    resolved_run_id = run_id or f"run_{uuid.uuid4().hex[:12]}"
    writer_root = Path(runs_dir) if runs_dir is not None else default_product_runs_dir()
    writer = BacktestRunWriter(writer_root)
    run_dir = writer.write_portfolio_run(
        run.portfolio_result,
        run_id=resolved_run_id,
        strategy_name=f"{strategy.definition.code}@{strategy.definition.version}",
        universe=_universe_label(request),
        name=config.name,
        created_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        overwrite=False,
    )

    # Augment config.json with product request snapshot for re-openability.
    config_path = run_dir / "config.json"
    try:
        import json

        existing = json.loads(config_path.read_text(encoding="utf-8"))
        existing["product_request"] = request.model_dump(mode="json")
        existing["entry_timing"] = request.execution.entry_timing
        existing["strategy_params"] = dict(request.strategy_params)
        config_path.write_text(
            json.dumps(existing, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
    except Exception:
        pass

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
