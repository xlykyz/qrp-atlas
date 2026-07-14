"""Cross-sectional momentum product runner for 07-B1.

Public chain:
request
→ rebalance schedule (strategy owns signal→execution dates)
→ PIT historical index universe
→ momentum factor generation (task 04 API)
→ get_strategy("cross_sectional_momentum_long_only")
→ strategy.run(...)
→ strategy_decisions_to_target_weights(..., emit_unchanged_snapshots=True)
→ PortfolioBacktestEngine
→ BacktestRunWriter (via product service)

Date-mapping ownership:
- Cross-sectional strategies already embed next-trading-day execution dates in
  decisions.trade_date. Product timing must NOT apply a second next_open shift.
- End-of-range signals without an execution date inside [start, end] are
  filtered here and recorded as NO_EXECUTION_DATE_IN_RANGE skips.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from qrp_atlas.backtest.data import load_stock_prices
from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import (
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioExecutionRule,
    StrategyPortfolioBacktestRun,
    strategy_decisions_to_target_weights,
)
from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import normalize_trade_date
from qrp_atlas.indicators.cross_section.factors import (
    FactorRequest,
    generate_factor_frame,
    get_factor_definition,
)
from qrp_atlas.indicators.cross_section.universe import build_historical_universe
from qrp_atlas.strategies import StrategyInput, get_strategy
from qrp_atlas.strategies.selection.rebalance import (
    REBALANCE_FREQUENCIES,
    build_rebalance_schedule,
)
from qrp_atlas.strategies.validation import resolve_parameters

from .schemas import CreateBacktestTaskRequest
from .timing import REASON_NO_EXECUTION_DATE_IN_RANGE, market_trade_dates

CROSS_SECTIONAL_MOMENTUM_CODE = "cross_sectional_momentum_long_only"
MOMENTUM_FACTOR_CODE = "momentum"
DEFAULT_SCORE_COLUMN = "momentum"


class CrossSectionProductError(ValueError):
    """Raised when the cross-sectional product path cannot run."""


def is_cross_sectional_product_strategy(strategy_code: str) -> bool:
    return str(strategy_code or "").strip() == CROSS_SECTIONAL_MOMENTUM_CODE


def _iso(value: Any) -> str:
    return normalize_trade_date(value).strftime("%Y-%m-%d")


def resolve_cross_section_product_params(
    request: CreateBacktestTaskRequest,
) -> dict[str, Any]:
    """Resolve strategy params and apply portfolio SSOT overrides.

    PortfolioBacktestConfig owns capacity / cash / weight caps. Strategy
    parameters receive the same values so Top-N selection and weight
    construction stay consistent with product config.
    """
    strategy = get_strategy(request.strategy_code, request.strategy_version)
    resolved = resolve_parameters(strategy.definition, request.strategy_params or {})

    top_n = int(resolved["top_n"])
    max_positions = int(request.position.max_positions)
    max_weight = float(request.position.max_weight_per_symbol)
    cash_buffer = float(resolved.get("cash_buffer") or 0.0)
    frequency = str(resolved.get("rebalance_frequency") or "weekly")
    lookback = int(resolved.get("momentum_lookback") or 20)

    if frequency not in REBALANCE_FREQUENCIES or frequency == "explicit":
        # Product path uses calendar frequencies only; explicit dates remain research-only.
        if frequency == "explicit":
            raise CrossSectionProductError(
                "rebalance_frequency=explicit is not supported on the product path"
            )
        raise CrossSectionProductError(
            f"unsupported rebalance_frequency: {frequency!r}; "
            f"expected one of {sorted(set(REBALANCE_FREQUENCIES) - {'explicit'})}"
        )
    if top_n < 1:
        raise CrossSectionProductError("top_n must be >= 1")
    if top_n > max_positions:
        raise CrossSectionProductError(
            f"top_n ({top_n}) must be <= max_positions ({max_positions})"
        )
    if not 0.0 <= cash_buffer < 1.0:
        raise CrossSectionProductError("cash_buffer must be in [0, 1)")
    if not 0.0 < max_weight <= 1.0:
        raise CrossSectionProductError("max_weight_per_symbol must be in (0, 1]")
    if top_n * max_weight + 1e-12 < (1.0 - cash_buffer):
        # Allow residual cash; reject only impossible over-weight plans? Spec:
        # top_n * max_weight must be able to form a legal target (<= 1 - cash_buffer is OK;
        # if top_n * max_weight < target, residual cash remains. That is legal.
        pass
    if lookback < 1:
        raise CrossSectionProductError("momentum lookback must be >= 1")

    # SSOT: strategy capacity/weight fields mirror portfolio config.
    resolved["top_n"] = top_n
    resolved["max_positions"] = max_positions
    resolved["max_weight_per_asset"] = max_weight
    resolved["cash_buffer"] = cash_buffer
    resolved["rebalance_frequency"] = frequency
    resolved["score_column"] = str(resolved.get("score_column") or DEFAULT_SCORE_COLUMN)
    resolved["momentum_lookback"] = lookback
    # Keep score column name stable for prepared factor frame.
    if resolved["score_column"] != DEFAULT_SCORE_COLUMN:
        # Product always generates the canonical momentum column.
        resolved["score_column"] = DEFAULT_SCORE_COLUMN
    return resolved


def _calendar_from_db(
    *,
    start_date: str,
    end_date: str,
    db_path: Any,
    warmup_calendar_days: int,
) -> list[pd.Timestamp]:
    """Load a broad market calendar using any stock bars available in range."""

    cal_start = (pd.Timestamp(start_date) - pd.Timedelta(days=warmup_calendar_days)).strftime(
        "%Y-%m-%d"
    )
    try:
        # Prefer an unrestricted calendar if caller provides enough data; load_stock_prices
        # requires tickers. Use a temporary universe probe via DuckDB trading dates if present.
        import duckdb

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            rows = con.execute(
                """
                SELECT DISTINCT trade_date
                FROM daily_market_snapshot
                WHERE trade_date >= ? AND trade_date <= ?
                ORDER BY trade_date
                """,
                [cal_start, end_date],
            ).fetchall()
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        raise CrossSectionProductError(f"failed to load trading calendar: {exc}") from exc

    if not rows:
        raise CrossSectionProductError("no trading calendar rows in market data range")
    return [pd.Timestamp(row[0]).normalize() for row in rows]


def _filter_execution_targets(
    target_weights: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
    signal_by_execution: dict[str, str],
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    """Keep only execution dates inside the formal request window.

    Strategies already mapped signal→next open. We only drop executions that
    fall outside [start, end] and record end-of-range skips.
    """
    if target_weights is None or target_weights.empty:
        empty = pd.DataFrame(
            columns=["trade_date", "asset_id", "target_weight", "priority", "signal_date"]
        )
        return empty, []

    frame = target_weights.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.normalize()
    frame["asset_id"] = frame["asset_id"].astype(str)
    frame["target_weight"] = pd.to_numeric(frame["target_weight"], errors="coerce").fillna(0.0)
    if "priority" not in frame.columns:
        frame["priority"] = 0.0

    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    skipped: list[dict[str, str]] = []
    rows: list[dict[str, object]] = []

    for exec_date, group in frame.groupby("trade_date", sort=True):
        exec_ts = pd.Timestamp(exec_date).normalize()
        exec_iso = exec_ts.strftime("%Y-%m-%d")
        signal_iso = signal_by_execution.get(exec_iso, exec_iso)
        if exec_ts < start_ts or exec_ts > end_ts:
            skipped.append(
                {
                    "asset_id": None,
                    "signal_date": signal_iso,
                    "reason": REASON_NO_EXECUTION_DATE_IN_RANGE,
                    "detail": (
                        "cross_section strategy already maps signal→next_open; "
                        f"execution_date={exec_iso} outside requested "
                        f"[{start_date}, {end_date}]"
                    ),
                }
            )
            continue
        for item in group.itertuples(index=False):
            rows.append(
                {
                    "trade_date": exec_iso,
                    "asset_id": str(item.asset_id),
                    "target_weight": float(item.target_weight),
                    "priority": float(getattr(item, "priority", 0.0) or 0.0),
                    "signal_date": signal_iso,
                }
            )

    if not rows:
        empty = pd.DataFrame(
            columns=["trade_date", "asset_id", "target_weight", "priority", "signal_date"]
        )
        return empty, skipped
    return pd.DataFrame(rows), skipped


def run_cross_sectional_momentum_product_backtest(
    request: CreateBacktestTaskRequest,
    *,
    db_path: Any,
) -> tuple[StrategyPortfolioBacktestRun, list[dict[str, str]], dict[str, Any]]:
    """Execute the public cross-sectional momentum product closed loop."""

    if not is_cross_sectional_product_strategy(request.strategy_code):
        raise CrossSectionProductError(
            f"unsupported cross-section product strategy: {request.strategy_code}"
        )
    if str(request.universe_mode).strip().lower() != "index_components":
        raise CrossSectionProductError(
            "cross_sectional_momentum_long_only requires universe_mode=index_components"
        )
    index_code = str(request.index_code or "").strip().upper()
    if not index_code:
        raise CrossSectionProductError("index_code is required for index_components universe")
    if str(request.execution.entry_timing or "").strip() != "next_open":
        raise CrossSectionProductError(
            "cross_sectional_momentum_long_only only supports entry_timing=next_open"
        )

    resolved = resolve_cross_section_product_params(request)
    lookback = int(resolved["momentum_lookback"])
    frequency = str(resolved["rebalance_frequency"])
    cash_buffer = float(resolved["cash_buffer"])
    max_positions = int(resolved["max_positions"])
    max_weight = float(resolved["max_weight_per_asset"])

    # Warmup calendar padding for momentum lookback + schedule context.
    warmup_calendar_days = max(lookback * 3, lookback + 40, 60)
    calendar = _calendar_from_db(
        start_date=request.start_date,
        end_date=request.end_date,
        db_path=db_path,
        warmup_calendar_days=warmup_calendar_days,
    )
    formal_start = pd.Timestamp(request.start_date).normalize()
    formal_end = pd.Timestamp(request.end_date).normalize()
    formal_calendar = [d for d in calendar if formal_start <= d <= formal_end]
    if not formal_calendar:
        raise CrossSectionProductError(
            "no trading days inside the requested date range"
        )

    # Strategy schedule owns signal→execution mapping once.
    schedule = build_rebalance_schedule(
        calendar,
        frequency=frequency,  # type: ignore[arg-type]
        start_date=request.start_date,
        end_date=request.end_date,
    )
    signal_dates = [
        normalize_trade_date(value) for value in schedule["signal_date"].tolist()
    ] if not schedule.empty else []
    signal_by_execution = {
        _iso(row.trade_date): _iso(row.signal_date)
        for row in schedule.itertuples(index=False)
    } if not schedule.empty else {}

    # PIT historical universe only on signal dates (empty day stays empty).
    universe = build_historical_universe(
        signal_dates,
        index_code=index_code,
        source="index",
        db_path=db_path,
    )
    # Diagnostics for empty-universe signal days.
    universe_diagnostics: list[dict[str, Any]] = []
    assets_by_signal: dict[str, set[str]] = {}
    if not universe.empty:
        for trade_date, group in universe.groupby(TRADE_DATE, sort=True):
            key = _iso(trade_date)
            assets_by_signal[key] = set(group[ASSET_ID].astype(str).tolist())
    for signal in signal_dates:
        key = _iso(signal)
        count = len(assets_by_signal.get(key, set()))
        universe_diagnostics.append(
            {
                "signal_date": key,
                "component_count": count,
                "empty": count == 0,
            }
        )

    union_assets = sorted({asset for assets in assets_by_signal.values() for asset in assets})
    if not union_assets:
        # Completely empty historical membership across all signal dates.
        # Use a placeholder market series only so the portfolio engine can mark
        # cash over the formal calendar; targets stay empty (all-cash).
        import duckdb

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            row = con.execute(
                """
                SELECT ticker
                FROM daily_market_snapshot
                WHERE trade_date >= ? AND trade_date <= ?
                LIMIT 1
                """,
                [request.start_date, request.end_date],
            ).fetchone()
        finally:
            con.close()
        if not row:
            raise CrossSectionProductError(
                "empty historical index universe and no market data for formal range"
            )
        placeholder = str(row[0])
        price_df = load_stock_prices(
            tickers=[placeholder],
            start_date=request.start_date,
            end_date=request.end_date,
            db_path=db_path,
        )
        price_df = price_df.copy()
        price_df["trade_date"] = pd.to_datetime(price_df["trade_date"]).dt.normalize()
        config = PortfolioBacktestConfig(
            name=request.name or f"{request.strategy_code}@{request.strategy_version}",
            initial_cash=float(request.position.initial_cash),
            max_positions=max_positions,
            max_weight_per_asset=max_weight,
            cost=CostRule(
                commission_rate=float(request.cost.commission_rate),
                stamp_tax_rate=float(request.cost.stamp_tax_rate),
                slippage_bps=float(request.cost.slippage_bps),
            ),
            execution=PortfolioExecutionRule(price_field="open", mark_price_field="close"),
        )
        empty_targets = pd.DataFrame(
            columns=["trade_date", "asset_id", "target_weight", "priority", "signal_date"]
        )
        portfolio_result = PortfolioBacktestEngine().run(
            price_df.reset_index(drop=True), empty_targets, config
        )
        strategy = get_strategy(request.strategy_code, request.strategy_version)
        from qrp_atlas.strategies.models import StrategyRunResult

        strategy_result = StrategyRunResult(
            strategy.definition,
            resolved,
            (),
            ("empty_historical_universe",),
        )
        run = StrategyPortfolioBacktestRun(
            strategy_result=strategy_result,
            target_weights=empty_targets,
            portfolio_result=portfolio_result,
        )
        meta = {
            "date_mapping_owner": "strategy_rebalance_schedule",
            "product_timing_shift": False,
            "index_code": index_code,
            "universe_mode": "index_components",
            "momentum_factor": {
                "code": MOMENTUM_FACTOR_CODE,
                "parameters": {"lookback": lookback},
                "output_column": DEFAULT_SCORE_COLUMN,
            },
            "resolved_strategy_params": resolved,
            "universe_diagnostics": universe_diagnostics,
            "rebalance_schedule_rows": int(len(schedule)),
            "signal_dates": [_iso(v) for v in signal_dates],
            "warmup": {
                "momentum_lookback": lookback,
                "calendar_padding_days": warmup_calendar_days,
                "formal_decisions_not_before": request.start_date,
            },
        }
        # End-of-range schedule executions outside window still produce skips.
        skipped: list[dict[str, str]] = []
        for row in schedule.itertuples(index=False):
            exec_iso = _iso(row.trade_date)
            signal_iso = _iso(row.signal_date)
            exec_ts = pd.Timestamp(exec_iso).normalize()
            if exec_ts < formal_start or exec_ts > formal_end:
                skipped.append(
                    {
                        "asset_id": None,
                        "signal_date": signal_iso,
                        "reason": REASON_NO_EXECUTION_DATE_IN_RANGE,
                        "detail": (
                            "end-of-range rebalance signal has no execution date "
                            f"within requested end_date; execution_date={exec_iso}"
                        ),
                    }
                )
        return run, skipped, meta

    # Price load: union of historical members + warmup history.
    price_start = (pd.Timestamp(request.start_date) - pd.Timedelta(days=warmup_calendar_days)).strftime(
        "%Y-%m-%d"
    )
    try:
        price_df = load_stock_prices(
            tickers=union_assets,
            start_date=price_start,
            end_date=request.end_date,
            db_path=db_path,
        )
    except Exception as exc:  # noqa: BLE001
        raise CrossSectionProductError(f"failed to load market data: {exc}") from exc
    if price_df is None or price_df.empty:
        raise CrossSectionProductError("no market data found for historical universe assets")
    price_df = price_df.copy()
    price_df["trade_date"] = pd.to_datetime(price_df["trade_date"]).dt.normalize()

    formal_prices = price_df[
        (price_df["trade_date"] >= formal_start) & (price_df["trade_date"] <= formal_end)
    ].copy()
    if formal_prices.empty:
        raise CrossSectionProductError(
            "insufficient market data inside the requested date range"
        )

    # Factor universe restricted to signal-date historical membership.
    factor_frame = generate_factor_frame(
        [FactorRequest(code=MOMENTUM_FACTOR_CODE, parameters={"lookback": lookback})],
        universe=universe,
        prices=price_df.rename(columns={"asset_id": ASSET_ID}) if ASSET_ID not in price_df.columns else price_df,
    )
    # Ensure score column name.
    if DEFAULT_SCORE_COLUMN not in factor_frame.columns:
        # generate_factor_frame may alias non-default lookback.
        score_cols = [c for c in factor_frame.columns if c not in {TRADE_DATE, ASSET_ID}]
        if not score_cols:
            raise CrossSectionProductError("momentum factor frame has no score column")
        factor_frame = factor_frame.rename(columns={score_cols[0]: DEFAULT_SCORE_COLUMN})

    # Prepared data for strategy: factor rows only (already universe-aligned).
    prepared = factor_frame.copy()
    if "ticker" not in prepared.columns:
        prepared["ticker"] = prepared[ASSET_ID]
    if ASSET_ID not in prepared.columns and "ticker" in prepared.columns:
        prepared[ASSET_ID] = prepared["ticker"]

    strategy = get_strategy(request.strategy_code, request.strategy_version)
    strategy_result = strategy.run(
        StrategyInput(
            prepared_data=prepared,
            parameters=resolved,
            initial_positions={},
            runtime_context={
                "trading_days": list(calendar),
            },
        )
    )

    target_weights = strategy_decisions_to_target_weights(
        strategy_result,
        max_positions=max_positions,
        max_weight_per_asset=max_weight,
        default_weight=None,
        cash_buffer=cash_buffer,
        emit_unchanged_snapshots=True,
    )

    execution_targets, skipped_signals = _filter_execution_targets(
        target_weights,
        start_date=request.start_date,
        end_date=request.end_date,
        signal_by_execution=signal_by_execution,
    )

    # Also record end-of-range signals that never received an in-range execution.
    # Strategy schedule may either:
    # 1) omit last-day signals when next open does not exist on full calendar, or
    # 2) map them to an execution date outside the formal request window.
    present_signal_in_targets = set()
    if not execution_targets.empty and "signal_date" in execution_targets.columns:
        present_signal_in_targets = set(execution_targets["signal_date"].astype(str).tolist())
    skipped_keys = {(item.get("signal_date"), item.get("reason")) for item in skipped_signals}

    # Signals that strategy schedule retained but execution is outside formal range.
    for row in schedule.itertuples(index=False):
        exec_iso = _iso(row.trade_date)
        signal_iso = _iso(row.signal_date)
        exec_ts = pd.Timestamp(exec_iso).normalize()
        if exec_ts < formal_start or exec_ts > formal_end:
            key = (signal_iso, REASON_NO_EXECUTION_DATE_IN_RANGE)
            if key not in skipped_keys:
                skipped_signals.append(
                    {
                        "asset_id": None,
                        "signal_date": signal_iso,
                        "reason": REASON_NO_EXECUTION_DATE_IN_RANGE,
                        "detail": (
                            "end-of-range rebalance signal has no execution date "
                            f"within requested end_date; execution_date={exec_iso}"
                        ),
                    }
                )
                skipped_keys.add(key)

    # Formal-range candidate signals dropped entirely by schedule (no next open).
    formal_signal_candidates = [d for d in formal_calendar]
    if frequency == "daily":
        candidate_signals = formal_signal_candidates
    else:
        # For non-daily frequencies, reconstruct schedule without end clipping of execution.
        candidate_schedule = build_rebalance_schedule(
            calendar,
            frequency=frequency,  # type: ignore[arg-type]
            start_date=request.start_date,
            end_date=None,
        )
        candidate_signals = [
            normalize_trade_date(v)
            for v in candidate_schedule["signal_date"].tolist()
            if formal_start <= normalize_trade_date(v) <= formal_end
        ] if not candidate_schedule.empty else []

    scheduled_signals = { _iso(v) for v in signal_dates }
    for signal in candidate_signals:
        signal_iso = _iso(signal)
        if signal_iso in scheduled_signals:
            # retained by schedule; outside-range handled above
            if signal_iso in present_signal_in_targets:
                continue
            # retained but produced no in-range target rows and not yet skipped
            # Check if its execution was outside range (already skipped) or missing.
            continue
        key = (signal_iso, REASON_NO_EXECUTION_DATE_IN_RANGE)
        if key not in skipped_keys:
            skipped_signals.append(
                {
                    "asset_id": None,
                    "signal_date": signal_iso,
                    "reason": REASON_NO_EXECUTION_DATE_IN_RANGE,
                    "detail": (
                        "rebalance signal has no next open execution date "
                        "within requested end_date"
                    ),
                }
            )
            skipped_keys.add(key)

    config = PortfolioBacktestConfig(
        name=request.name or f"{request.strategy_code}@{request.strategy_version}",
        initial_cash=float(request.position.initial_cash),
        max_positions=max_positions,
        max_weight_per_asset=max_weight,
        cost=CostRule(
            commission_rate=float(request.cost.commission_rate),
            stamp_tax_rate=float(request.cost.stamp_tax_rate),
            slippage_bps=float(request.cost.slippage_bps),
        ),
        execution=PortfolioExecutionRule(price_field="open", mark_price_field="close"),
    )
    portfolio_result = PortfolioBacktestEngine().run(
        formal_prices.reset_index(drop=True),
        execution_targets,
        config,
    )
    run = StrategyPortfolioBacktestRun(
        strategy_result=strategy_result,
        target_weights=execution_targets,
        portfolio_result=portfolio_result,
    )
    factor_def = get_factor_definition(MOMENTUM_FACTOR_CODE)
    meta = {
        "date_mapping_owner": "strategy_rebalance_schedule",
        "product_timing_shift": False,
        "index_code": index_code,
        "universe_mode": "index_components",
        "momentum_factor": {
            "code": factor_def.code,
            "name": factor_def.name,
            "parameters": {"lookback": lookback},
            "output_column": DEFAULT_SCORE_COLUMN,
            "time_semantics": factor_def.time_semantics,
        },
        "resolved_strategy_params": resolved,
        "universe_diagnostics": universe_diagnostics,
        "rebalance_schedule_rows": int(len(schedule)),
        "signal_dates": [_iso(v) for v in signal_dates],
        "union_asset_count": len(union_assets),
        "warmup": {
            "momentum_lookback": lookback,
            "calendar_padding_days": warmup_calendar_days,
            "formal_decisions_not_before": request.start_date,
        },
        "market_trade_date_count": len(market_trade_dates(formal_prices)),
    }
    return run, skipped_signals, meta
