"""Product orchestration for classic, cross-sectional, and event backtest tasks."""

from __future__ import annotations

import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from qrp_atlas.backtest.data import load_index_prices, load_stock_prices
from qrp_atlas.backtest.exposure_data import prepare_cross_section_exposure_panel
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
from .cross_section import (
    CrossSectionProductError,
    is_cross_sectional_product_strategy,
    resolve_cross_section_product_params,
    run_cross_sectional_momentum_product_backtest,
)
from .event import (
    EventProductError,
    is_event_product_strategy,
    resolve_event_product_params,
    run_event_drift_product_backtest,
)
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
    if universe_mode not in {"tickers", "preset", "index_components"}:
        raise BacktestTaskValidationError(
            "universe_mode must be tickers, preset, or index_components"
        )

    tickers = _normalize_tickers(request.tickers)
    universe_preset = request.universe_preset
    index_code = (request.index_code or None)
    if index_code is not None:
        index_code = str(index_code).strip().upper() or None

    if is_cross_sectional_product_strategy(strategy_code):
        if universe_mode != "index_components":
            raise BacktestTaskValidationError(
                "cross_sectional_momentum_long_only requires universe_mode=index_components"
            )
        if not index_code:
            raise BacktestTaskValidationError(
                "index_code is required when universe_mode is index_components"
            )
        tickers = []
        universe_preset = None
    elif is_event_product_strategy(strategy_code):
        if universe_mode != "tickers":
            raise BacktestTaskValidationError(
                "event_drift_basic requires universe_mode=tickers"
            )
        if not tickers:
            raise BacktestTaskValidationError(
                "tickers required when universe_mode is tickers for event_drift_basic"
            )
        universe_preset = None
        index_code = None
    elif universe_mode == "tickers":
        if not tickers:
            raise BacktestTaskValidationError("tickers required when universe_mode is tickers")
        universe_preset = None
        index_code = None
    elif universe_mode == "index_components":
        raise BacktestTaskValidationError(
            "universe_mode=index_components is only supported for "
            "cross_sectional_momentum_long_only"
        )
    else:
        raise BacktestTaskValidationError(
            "universe_mode=preset is not supported; provide tickers or index_components"
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

    # Cross-sectional product: only next_open; apply portfolio SSOT into strategy params.
    if is_event_product_strategy(strategy_code):
        if entry_timing != "next_open":
            raise BacktestTaskValidationError(
                "event_drift_basic only supports entry_timing=next_open"
            )
        try:
            resolved = resolve_event_product_params(
                CreateBacktestTaskRequest(
                    name=request.name,
                    strategy_code=strategy_code,
                    strategy_version=strategy.definition.version,
                    strategy_params=dict(resolved),
                    universe_mode=universe_mode,
                    universe_preset=universe_preset,
                    index_code=index_code,
                    tickers=tickers,
                    start_date=start_date,
                    end_date=end_date,
                    benchmark_id=request.benchmark_id,
                    position=position,
                    cost=cost,
                    execution=request.execution.model_copy(update={"entry_timing": entry_timing}),
                )
            )
        except EventProductError as exc:
            raise BacktestTaskValidationError(str(exc)) from exc
    elif is_cross_sectional_product_strategy(strategy_code):
        if entry_timing != "next_open":
            raise BacktestTaskValidationError(
                "cross_sectional_momentum_long_only only supports entry_timing=next_open"
            )
        try:
            draft = CreateBacktestTaskRequest(
                name=request.name,
                strategy_code=strategy_code,
                strategy_version=strategy.definition.version,
                strategy_params=dict(resolved),
                universe_mode=universe_mode,
                universe_preset=universe_preset,
                index_code=index_code,
                tickers=tickers,
                start_date=start_date,
                end_date=end_date,
                benchmark_id=request.benchmark_id,
                position=position,
                cost=cost,
                execution=request.execution.model_copy(update={"entry_timing": entry_timing}),
            )
            resolved = resolve_cross_section_product_params(draft)
        except CrossSectionProductError as exc:
            raise BacktestTaskValidationError(str(exc)) from exc

    benchmark_id = (request.benchmark_id or None)
    if benchmark_id is not None:
        benchmark_id = str(benchmark_id).strip().upper() or None

    return CreateBacktestTaskRequest(
        name=request.name,
        strategy_code=strategy_code,
        strategy_version=strategy.definition.version,
        strategy_params=dict(resolved),
        universe_mode=universe_mode,
        universe_preset=universe_preset,
        index_code=index_code,
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        benchmark_id=benchmark_id,
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
    if request.universe_mode == "index_components":
        return f"index_components:{request.index_code}"
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



def _load_benchmark_frame(
    request: CreateBacktestTaskRequest,
    *,
    db_path: Path | None,
) -> tuple[pd.DataFrame | None, str | None, list[str]]:
    """Load optional index benchmark prices for product analytics."""

    diagnostics: list[str] = []
    benchmark_id = (getattr(request, "benchmark_id", None) or "").strip().upper() or None
    if not benchmark_id:
        return None, None, diagnostics
    if db_path is None:
        diagnostics.append("benchmark_requested_but_db_missing")
        return None, benchmark_id, diagnostics
    try:
        frame = load_index_prices(
            codes=[benchmark_id],
            start_date=request.start_date,
            end_date=request.end_date,
            db_path=db_path,
        )
    except Exception as exc:  # noqa: BLE001
        diagnostics.append(f"benchmark_load_failed:{exc}")
        return None, benchmark_id, diagnostics
    if frame is None or frame.empty:
        diagnostics.append("benchmark_empty")
        return None, benchmark_id, diagnostics
    # normalize columns for align_benchmark_series
    work = frame.copy()
    if "trade_date" not in work.columns and "date" in work.columns:
        work = work.rename(columns={"date": "trade_date"})
    return work, benchmark_id, diagnostics


def _data_fingerprint(frame: Any, *, cols: list[str] | None = None) -> dict[str, Any] | None:
    """Deterministic lightweight fingerprint for a price/event frame."""

    if frame is None or getattr(frame, "empty", True):
        return None
    import hashlib
    import json

    work = frame.copy()
    use_cols = [c for c in (cols or list(work.columns)) if c in work.columns]
    if not use_cols:
        use_cols = list(work.columns)
    work = work[use_cols]
    # stable string rows
    payload = work.astype(str).sort_values(by=list(work.columns)).to_csv(index=False)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return {
        "row_count": int(len(work)),
        "columns": list(use_cols),
        "sha256": digest,
    }


def _build_reproducibility_snapshot(
    request: CreateBacktestTaskRequest,
    *,
    strategy: Any,
    portfolio_result: Any,
    cross_section_meta: dict[str, Any] | None = None,
    event_meta: dict[str, Any] | None = None,
    benchmark_id: str | None = None,
    resolved_universe_assets: list[str] | None = None,
    price_frame: Any | None = None,
    benchmark_frame: Any | None = None,
) -> dict[str, Any]:
    definition = strategy.definition.to_dict() if hasattr(strategy.definition, "to_dict") else {}
    # strip non-serializable frames from nested meta copies
    cs_meta = None
    if cross_section_meta:
        cs_meta = {
            k: v
            for k, v in cross_section_meta.items()
            if k not in {"price_frame"}
        }
    event_payload = None
    if event_meta:
        event_payload = (event_meta or {}).get("event") if isinstance(event_meta, dict) else event_meta
    assets = list(resolved_universe_assets or request.tickers or [])
    return {
        "strategy_code": strategy.definition.code,
        "strategy_version": strategy.definition.version,
        "strategy_definition_snapshot": definition,
        "strategy_params": dict(request.strategy_params or {}),
        "indicator_requests": definition.get("indicator_requests") or [],
        "universe": {
            "mode": request.universe_mode,
            "requested_tickers": list(request.tickers or []),
            "resolved_assets": assets,
            "resolved_asset_count": len(assets),
            "index_code": request.index_code,
            "universe_preset": request.universe_preset,
        },
        "date_range": {
            "start_date": request.start_date,
            "end_date": request.end_date,
            "effective_start_date": (
                portfolio_result.snapshots[0].trade_date if portfolio_result.snapshots else request.start_date
            ),
            "effective_end_date": (
                portfolio_result.snapshots[-1].trade_date if portfolio_result.snapshots else request.end_date
            ),
        },
        "pit": {
            "available_date_semantics": "product uses historical membership/events available as of formal dates",
            "cross_section": cs_meta,
            "event": event_payload,
        },
        "data_fingerprints": {
            "prices": _data_fingerprint(
                price_frame,
                cols=["trade_date", "asset_id", "ticker", "open", "high", "low", "close"],
            ),
            "benchmark": _data_fingerprint(
                benchmark_frame,
                cols=["trade_date", "asset_id", "ticker", "close", "open"],
            ),
        },
        "execution": request.execution.model_dump(mode="json"),
        "cost": request.cost.model_dump(mode="json"),
        "position": request.position.model_dump(mode="json"),
        "benchmark_id": benchmark_id,
        "product_request": request.model_dump(mode="json"),
        "replay": {
            "supported": True,
            "entry": "POST /api/backtest/runs/{run_id}/replay",
            "uses_locked_snapshot": True,
            "does_not_use_current_registry_defaults": True,
        },
    }


def _build_exposure_payload(
    request: CreateBacktestTaskRequest,
    *,
    portfolio_result: Any,
    execution_targets: Any = None,
    cross_section_meta: dict[str, Any] | None = None,
    db_path: Path | None = None,
    price_frame: Any | None = None,
) -> dict[str, Any]:
    """Industry / market-cap exposures for product runs.

    Position concentration is stored separately and never labeled as market_cap.
    """

    # Always compute concentration from snapshots.
    concentration_rows: list[dict[str, Any]] = []
    for snap in getattr(portfolio_result, "snapshots", []) or []:
        positions = getattr(snap, "positions", None) or []
        weights = []
        for pos in positions:
            w = getattr(pos, "weight", None)
            if w is None and isinstance(pos, dict):
                w = pos.get("weight")
            if w is None:
                continue
            weights.append(float(w))
        if not weights:
            continue
        concentration_rows.append(
            {
                "trade_date": getattr(snap, "trade_date", None)
                or (snap.get("trade_date") if isinstance(snap, dict) else None),
                "position_count": len(weights),
                "max_weight": max(weights),
                "sum_weight": sum(weights),
            }
        )

    if not is_cross_sectional_product_strategy(request.strategy_code):
        return {
            "available": False,
            "industry_available": False,
            "market_cap_available": False,
            "reason": "industry_market_cap_exposures_require_cross_sectional_product_run",
            "industry": [],
            "market_cap": [],
            "position_concentration": concentration_rows,
            "note": "Non-CS runs expose position concentration only.",
        }

    if cross_section_meta and isinstance(cross_section_meta.get("exposures"), dict):
        payload = dict(cross_section_meta["exposures"])
        payload.setdefault("position_concentration", concentration_rows)
        payload.setdefault("available", True)
        return payload

    industry_rows: list[dict[str, Any]] = []
    market_cap_rows: list[dict[str, Any]] = []
    industry_available = False
    market_cap_available = False
    reason = None

    # Build signal-date holdings from execution targets when possible.
    holdings: list[tuple[str, str, float]] = []  # trade_date, asset, weight
    if execution_targets is not None and hasattr(execution_targets, "empty") and not execution_targets.empty:
        work = execution_targets.copy()
        for col in ("trade_date", "asset_id", "target_weight"):
            if col not in work.columns:
                holdings = []
                break
        else:
            for row in work.itertuples(index=False):
                w = float(getattr(row, "target_weight"))
                if w <= 0:
                    continue
                holdings.append((str(getattr(row, "trade_date")), str(getattr(row, "asset_id")), w))

    if not holdings:
        reason = "no_positive_target_weights_for_exposure"
    else:
        import math
        import pandas as pd
        from collections import defaultdict

        dates = sorted({d for d, _, _ in holdings})
        assets = sorted({a for _, a, _ in holdings})
        uni_rows = [{"trade_date": d, "asset_id": a} for d in dates for a in assets]
        universe = pd.DataFrame(uni_rows)

        # Market-cap exposure from same-day size fields (no forward fill).
        size_panel = None
        if price_frame is not None and not getattr(price_frame, "empty", True):
            pf = price_frame.copy()
            if "asset_id" not in pf.columns and "ticker" in pf.columns:
                pf["asset_id"] = pf["ticker"]
            if "trade_date" in pf.columns and "asset_id" in pf.columns:
                size_cols = [c for c in ("market_cap", "float_cap") if c in pf.columns]
                if size_cols:
                    size_panel = pf[["trade_date", "asset_id", size_cols[0]]].rename(
                        columns={size_cols[0]: "market_cap"}
                    )
                    size_panel["trade_date"] = pd.to_datetime(size_panel["trade_date"]).dt.strftime("%Y-%m-%d")
                    size_panel["asset_id"] = size_panel["asset_id"].astype(str)

        mcap_acc: dict[str, list[tuple[float, float]]] = defaultdict(list)
        if size_panel is not None and not size_panel.empty:
            size_lookup = {
                (str(r.trade_date), str(r.asset_id)): float(r.market_cap)
                for r in size_panel.itertuples(index=False)
                if r.market_cap is not None and float(r.market_cap) > 0
            }
            for d, a, w in holdings:
                mv = size_lookup.get((d, a))
                if mv is None:
                    continue
                try:
                    lm = math.log(float(mv))
                except (TypeError, ValueError):
                    continue
                if math.isfinite(lm):
                    mcap_acc[d].append((w, lm))
                    market_cap_available = True
            for d, pairs in sorted(mcap_acc.items()):
                tw = sum(w for w, _ in pairs) or 1.0
                wmean = sum(w * lm for w, lm in pairs) / tw
                market_cap_rows.append(
                    {
                        "trade_date": d,
                        "weighted_log_market_cap": wmean,
                        "covered_weight": tw,
                        "name_count": len(pairs),
                    }
                )

        # Industry weights via PIT panel when membership data is available.
        if db_path is None:
            if not market_cap_available:
                reason = "market_database_required_for_pit_industry_exposures"
            else:
                reason = "industry_unavailable_without_market_database"
        else:
            try:
                panel = prepare_cross_section_exposure_panel(
                    universe,
                    db_path=db_path,
                    size_panel=size_panel,
                )
                ind_acc: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
                for d, a, w in holdings:
                    sub = panel[
                        (panel["trade_date"].astype(str) == d)
                        & (panel["asset_id"].astype(str) == a)
                    ]
                    if sub.empty:
                        continue
                    ind = sub.iloc[0].get("industry_code")
                    if ind is not None and str(ind) not in {"", "nan", "None", "<NA>"}:
                        ind_acc[d][str(ind)] += w
                        industry_available = True
                for d, mapping in sorted(ind_acc.items()):
                    total = sum(mapping.values()) or 1.0
                    for ind, w in sorted(mapping.items()):
                        industry_rows.append(
                            {
                                "trade_date": d,
                                "industry_code": ind,
                                "weight": w,
                                "weight_share": w / total,
                            }
                        )
                if not industry_available and not market_cap_available:
                    reason = "pit_exposure_panel_unavailable_or_empty"
                elif not industry_available:
                    reason = "industry_membership_unavailable_position_and_market_cap_only"
            except Exception as exc:  # noqa: BLE001
                if market_cap_available:
                    reason = f"industry_exposure_unavailable:{exc}"
                else:
                    reason = f"exposure_build_failed:{exc}"

    available = bool(industry_available or market_cap_available)
    return {
        "available": available,
        "industry_available": industry_available,
        "market_cap_available": market_cap_available,
        "reason": reason,
        "industry": industry_rows,
        "market_cap": market_cap_rows,
        "position_concentration": concentration_rows,
        "note": (
            "industry/market_cap use PIT exposure panel when available; "
            "position_concentration is separate and not market-cap exposure."
        ),
    }


def execute_validated_task(
    request: CreateBacktestTaskRequest,
    *,
    run_id: str | None = None,
    runs_dir: Path | None = None,
    db_path: Path | None = None,
) -> tuple[str, Path]:
    """Run strategy + portfolio engine and persist a standard results package."""

    strategy = get_strategy(request.strategy_code, request.strategy_version)
    cross_section_meta: dict[str, Any] = {}
    event_meta: dict[str, Any] = {}
    if is_cross_sectional_product_strategy(request.strategy_code):
        if db_path is None:
            raise BacktestTaskExecutionError(
                "cross-sectional product tasks require a market database path"
            )
        try:
            cs_run, skipped_signals, cross_section_meta = (
                run_cross_sectional_momentum_product_backtest(
                    request,
                    db_path=db_path,
                )
            )
        except CrossSectionProductError as exc:
            raise BacktestTaskExecutionError(str(exc)) from exc
        strategy_result = cs_run.strategy_result
        execution_targets = cs_run.target_weights
        portfolio_result = cs_run.portfolio_result
    elif is_event_product_strategy(request.strategy_code):
        if db_path is None:
            raise BacktestTaskExecutionError(
                "event product tasks require a market database path"
            )
        try:
            event_run, skipped_signals, event_meta = run_event_drift_product_backtest(
                request,
                db_path=db_path,
            )
        except EventProductError as exc:
            raise BacktestTaskExecutionError(str(exc)) from exc
        strategy_result = event_run.strategy_result
        execution_targets = event_run.target_weights
        portfolio_result = event_run.portfolio_result
    else:
        price_df = _load_prices(request, db_path=db_path)
        strategy_result, execution_targets, portfolio_result, skipped_signals = (
            _run_product_portfolio(request, price_df)
        )

    execution_signal_map: dict[tuple[str, str], str] = {}
    if execution_targets is not None and not execution_targets.empty:
        for row in execution_targets.itertuples(index=False):
            execution_signal_map[(str(row.trade_date), str(row.asset_id))] = str(
                getattr(row, "signal_date", row.trade_date)
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
        "benchmark_id": getattr(request, "benchmark_id", None),
        "decision_count": len(strategy_result.decisions),
        "execution_target_rows": int(len(execution_targets)),
        "cross_section": (
            {
                k: v
                for k, v in (cross_section_meta or {}).items()
                if k not in {"price_frame"}
            }
            or None
        ),
        "event": (event_meta or {}).get("event") if event_meta else None,
    }

    # MAE/MFE prices: prefer assets that actually traded (fills/orders/trades).
    # Path-local frames (classic price_df / CS price_frame) are fallbacks only.
    price_frame_for_analytics = locals().get("price_df")
    if price_frame_for_analytics is None and cross_section_meta:
        price_frame_for_analytics = cross_section_meta.get("price_frame")
    traded_assets: list[str] = sorted(
        {
            str(getattr(f, "asset_id", "") or "")
            for f in portfolio_result.fills
            if getattr(f, "asset_id", None)
        }
        | {
            str(getattr(o, "asset_id", "") or "")
            for o in portfolio_result.orders
            if getattr(o, "asset_id", None)
        }
    )
    traded_assets = [a for a in traded_assets if a]
    # Always try to load OHLC for traded assets when a market DB is available so
    # CS/event paths are not blocked by missing price_frame or empty request.tickers.
    if db_path is not None:
        assets_to_load = traded_assets or list(request.tickers or [])
        if not assets_to_load and cross_section_meta:
            assets_to_load = list(cross_section_meta.get("traded_or_universe_assets") or [])
        if assets_to_load:
            try:
                import pandas as pd

                start = request.start_date
                end = request.end_date
                if portfolio_result.snapshots:
                    start = min(start, portfolio_result.snapshots[0].trade_date)
                    end = max(end, portfolio_result.snapshots[-1].trade_date)
                # small buffer so hold windows at range edges still have OHLC
                start_buf = (pd.Timestamp(start) - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
                end_buf = (pd.Timestamp(end) + pd.Timedelta(days=5)).strftime("%Y-%m-%d")
                loaded = load_stock_prices(
                    tickers=assets_to_load,
                    start_date=start_buf,
                    end_date=end_buf,
                    db_path=db_path,
                )
                if loaded is not None and not getattr(loaded, "empty", True):
                    price_frame_for_analytics = loaded
            except Exception:  # noqa: BLE001
                # keep any path-local frame already captured
                pass

    benchmark_frame, benchmark_id, benchmark_load_diag = _load_benchmark_frame(
        request, db_path=db_path
    )
    if benchmark_load_diag:
        config_overlay = {
            **config_overlay,
            "benchmark_load_diagnostics": benchmark_load_diag,
        }
    resolved_universe_assets = traded_assets or list(
        (cross_section_meta or {}).get("traded_or_universe_assets") or request.tickers or []
    )
    repro_snapshot = _build_reproducibility_snapshot(
        request,
        strategy=strategy,
        portfolio_result=portfolio_result,
        cross_section_meta=cross_section_meta,
        event_meta=locals().get("event_meta") if "event_meta" in locals() else None,
        benchmark_id=benchmark_id,
        resolved_universe_assets=resolved_universe_assets,
        price_frame=price_frame_for_analytics,
        benchmark_frame=benchmark_frame,
    )
    exposure_payload = _build_exposure_payload(
        request,
        portfolio_result=portfolio_result,
        execution_targets=execution_targets,
        cross_section_meta=cross_section_meta,
        db_path=db_path,
        price_frame=price_frame_for_analytics,
    )

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
            execution_signal_map=execution_signal_map,
            extra_skipped=skipped_signals,
            price_frame=price_frame_for_analytics,
            benchmark_frame=benchmark_frame,
            benchmark_id=benchmark_id,
            exposures=exposure_payload,
            reproducibility_snapshot=repro_snapshot,
        )
    except Exception as exc:  # noqa: BLE001
        raise BacktestTaskExecutionError(f"failed to persist backtest results: {exc}") from exc

    return resolved_run_id, run_dir



def replay_product_run(
    run_id: str,
    *,
    runs_dir: Path | None = None,
    db_path: Path | None = None,
    new_run_id: str | None = None,
) -> dict[str, Any]:
    """Re-execute a product run from its locked reproducibility snapshot.

    Uses snapshot product_request / strategy version / params rather than current
    registry defaults. Compares business fields of equity/summary/orders/fills.
    """

    from qrp_atlas.backtest.results.loader import BacktestRunsLoader

    root = Path(runs_dir) if runs_dir is not None else default_product_runs_dir()
    loader = BacktestRunsLoader(root)
    repro = loader.load_reproducibility(run_id)
    if not repro:
        # fallback config
        cfg = loader.load_config(run_id)
        repro = cfg.get("reproducibility") if isinstance(cfg, dict) else None
    if not isinstance(repro, dict):
        raise BacktestTaskExecutionError(f"run {run_id} has no reproducibility snapshot")
    req_payload = repro.get("product_request")
    if not isinstance(req_payload, dict):
        raise BacktestTaskExecutionError(f"run {run_id} reproducibility lacks product_request")
    # lock strategy identity from snapshot, not registry-latest defaults
    if repro.get("strategy_code"):
        req_payload = {**req_payload, "strategy_code": repro["strategy_code"]}
    if repro.get("strategy_version"):
        req_payload = {**req_payload, "strategy_version": repro["strategy_version"]}
    if isinstance(repro.get("strategy_params"), dict):
        req_payload = {**req_payload, "strategy_params": dict(repro["strategy_params"])}

    request = CreateBacktestTaskRequest.model_validate(req_payload)
    replay_id = new_run_id or f"replay_{run_id}"
    # overwrite false: if exists, create unique
    if (root / replay_id).exists():
        replay_id = f"replay_{run_id}_{uuid.uuid4().hex[:8]}"
    new_id, new_dir = execute_validated_task(
        request,
        run_id=replay_id,
        runs_dir=root,
        db_path=db_path,
    )
    old_summary = loader.load_summary(run_id)
    new_loader = BacktestRunsLoader(root)
    new_summary = new_loader.load_summary(new_id)
    old_equity = loader.load_equity(run_id)
    new_equity = new_loader.load_equity(new_id)
    old_fills = loader.load_fills(run_id)
    new_fills = new_loader.load_fills(new_id)
    old_orders = loader.load_orders(run_id)
    new_orders = new_loader.load_orders(new_id)

    def _strip_non_business_fills(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        for r in rows or []:
            item = {
                k: r.get(k)
                for k in (
                    "trade_date",
                    "asset_id",
                    "side",
                    "quantity",
                    "price",
                    "amount",
                    "commission",
                    "stamp_tax",
                    "slippage_cost",
                )
                if k in r
            }
            out.append(item)
        return out

    def _strip_equity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {"date": r.get("date"), "equity": r.get("equity"), "drawdown_pct": r.get("drawdown_pct")}
            for r in rows or []
        ]

    business_keys = [
        "total_return_pct",
        "annual_return_pct",
        "max_drawdown_pct",
        "final_equity",
        "turnover",
        "total_cost",
        "trade_count",
    ]

    def _num_close(a, b) -> bool:
        if a == b:
            return True
        try:
            if a is None or b is None:
                return a is None and b is None
            return abs(float(a) - float(b)) < 1e-9
        except (TypeError, ValueError):
            return False

    summary_match = all(_num_close(old_summary.get(k), new_summary.get(k)) for k in business_keys)
    equity_match = _strip_equity(old_equity) == _strip_equity(new_equity)
    fills_match = _strip_non_business_fills(old_fills) == _strip_non_business_fills(new_fills)
    orders_match = _strip_non_business_fills(old_orders) == _strip_non_business_fills(new_orders)

    def _strip_trades(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        keys = (
            "asset_id",
            "signal_date",
            "entry_date",
            "entry_price",
            "exit_date",
            "exit_price",
            "holding_days",
            "return_pct",
            "status",
            "exit_reason",
        )
        out = []
        for r in rows or []:
            out.append({k: r.get(k) for k in keys})
        return out

    old_trades = loader.load_trades(run_id)
    new_trades = new_loader.load_trades(new_id)
    trades_match = _strip_trades(old_trades) == _strip_trades(new_trades)

    # targets / decisions are recovered from execution artifacts when present
    old_snaps = loader.load_snapshots(run_id)
    new_snaps = new_loader.load_snapshots(new_id)

    def _strip_targets_from_snaps(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        for r in rows or []:
            positions = r.get("positions") or []
            out.append(
                {
                    "trade_date": r.get("trade_date") or r.get("date"),
                    "positions": sorted(
                        [
                            {
                                "asset_id": p.get("asset_id") if isinstance(p, dict) else getattr(p, "asset_id", None),
                                "weight": p.get("weight") if isinstance(p, dict) else getattr(p, "weight", None),
                            }
                            for p in positions
                        ],
                        key=lambda x: str(x.get("asset_id") or ""),
                    ),
                }
            )
        return out

    targets_match = _strip_targets_from_snaps(old_snaps) == _strip_targets_from_snaps(new_snaps)

    old_hash = repro.get("snapshot_hash")
    new_repro = new_loader.load_reproducibility(new_id) or {}
    all_business = (
        summary_match
        and equity_match
        and fills_match
        and orders_match
        and trades_match
        and targets_match
    )
    return {
        "source_run_id": run_id,
        "replay_run_id": new_id,
        "replay_dir": str(new_dir),
        "source_snapshot_hash": old_hash,
        "replay_snapshot_hash": new_repro.get("snapshot_hash"),
        "source_resolved_universe": (repro.get("universe") or {}).get("resolved_assets"),
        "replay_resolved_universe": (new_repro.get("universe") or {}).get("resolved_assets"),
        "source_data_fingerprints": repro.get("data_fingerprints"),
        "replay_data_fingerprints": new_repro.get("data_fingerprints"),
        "match": {
            "summary_business_fields": summary_match,
            "equity": equity_match,
            "fills": fills_match,
            "orders": orders_match,
            "trades": trades_match,
            "targets_from_snapshots": targets_match,
            "all_business": all_business,
        },
        "allowed_to_differ": ["run_id", "created_at", "replay_run_id", "name"],
    }


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
