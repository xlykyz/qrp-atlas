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
- Signal calendar is limited to [start_date, end_date].
- Execution-mapping calendar may include the first trade day after end_date so
  terminal weekly/monthly signals map to an out-of-range next open and enter
  standard skipped records instead of disappearing.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from qrp_atlas.backtest.data import load_stock_prices
from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import (
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioBacktestResult,
    PortfolioExecutionRule,
    PortfolioSnapshot,
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
from qrp_atlas.strategies.models import StrategyRunResult
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
    if lookback < 1:
        raise CrossSectionProductError("momentum lookback must be >= 1")

    resolved["top_n"] = top_n
    resolved["max_positions"] = max_positions
    resolved["max_weight_per_asset"] = max_weight
    resolved["cash_buffer"] = cash_buffer
    resolved["rebalance_frequency"] = frequency
    resolved["score_column"] = DEFAULT_SCORE_COLUMN
    resolved["momentum_lookback"] = lookback
    return resolved


def _market_calendar_from_db(
    *,
    start_date: str,
    end_date: str,
    db_path: Any,
) -> list[pd.Timestamp]:
    """Load deterministic market trade dates from DISTINCT trade_date rows."""

    try:
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
                [start_date, end_date],
            ).fetchall()
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        raise CrossSectionProductError(f"failed to load trading calendar: {exc}") from exc

    if not rows:
        raise CrossSectionProductError("no trading calendar rows in market data range")
    return [pd.Timestamp(row[0]).normalize() for row in rows]


def _extend_calendar_with_next_open(
    *,
    calendar: list[pd.Timestamp],
    formal_end: pd.Timestamp,
    db_path: Any,
) -> list[pd.Timestamp]:
    """Append the first market day strictly after formal_end when available.

    This extra day is only for signal→execution mapping so terminal weekly /
    monthly signals can become out-of-range executions and enter skipped.
    """
    if not calendar:
        return calendar
    try:
        import duckdb

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            row = con.execute(
                """
                SELECT MIN(trade_date)
                FROM daily_market_snapshot
                WHERE trade_date > ?
                """,
                [formal_end.strftime("%Y-%m-%d")],
            ).fetchone()
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        raise CrossSectionProductError(
            f"failed to load post-end execution calendar day: {exc}"
        ) from exc

    if not row or row[0] is None:
        return list(calendar)
    next_day = pd.Timestamp(row[0]).normalize()
    if next_day in calendar:
        return list(calendar)
    return list(calendar) + [next_day]


def build_cash_only_portfolio_result(
    *,
    config: PortfolioBacktestConfig,
    formal_trade_dates: list[pd.Timestamp],
) -> PortfolioBacktestResult:
    """Build a deterministic all-cash portfolio result with no assets.

    Used when the historical index universe is empty for every signal date.
    Dates come from the market trade calendar, not any security price series.
    """
    snapshots: list[PortfolioSnapshot] = []
    cash = float(config.initial_cash)
    for trade_date in formal_trade_dates:
        snapshots.append(
            PortfolioSnapshot(
                trade_date=_iso(trade_date),
                cash=cash,
                market_value=0.0,
                equity=cash,
                daily_return=0.0,
                drawdown=0.0,
                turnover=0.0,
                commission=0.0,
                stamp_tax=0.0,
                slippage_cost=0.0,
                cumulative_cost=0.0,
                positions=(),
            )
        )
    summary = {
        "initial_cash": float(config.initial_cash),
        "final_equity": float(config.initial_cash),
        "total_return": 0.0,
        "total_return_pct": 0.0,
        "max_drawdown": 0.0,
        "max_drawdown_pct": 0.0,
        "turnover": 0.0,
        "order_count": 0,
        "fill_count": 0,
        "trade_count": 0,
        "skipped_count": 0,
        "commission": 0.0,
        "stamp_tax": 0.0,
        "slippage_cost": 0.0,
        "total_cost": 0.0,
    }
    equity_curve = tuple(
        {
            "date": snapshot.trade_date,
            "equity": 1.0,
            "drawdown_pct": 0.0,
        }
        for snapshot in snapshots
    )
    return PortfolioBacktestResult(
        config=config,
        summary=summary,
        orders=(),
        fills=(),
        snapshots=tuple(snapshots),
        equity_curve=equity_curve,
    )


def _filter_execution_targets(
    target_weights: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
    signal_by_execution: dict[str, str],
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    """Keep only execution dates inside the formal request window."""
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


def _collect_out_of_range_skips(
    schedule: pd.DataFrame,
    *,
    formal_start: pd.Timestamp,
    formal_end: pd.Timestamp,
) -> list[dict[str, str]]:
    skipped: list[dict[str, str]] = []
    if schedule is None or schedule.empty:
        return skipped
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
    return skipped


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

    warmup_calendar_days = max(lookback * 3, lookback + 40, 60)
    cal_start = (
        pd.Timestamp(request.start_date) - pd.Timedelta(days=warmup_calendar_days)
    ).strftime("%Y-%m-%d")
    formal_start = pd.Timestamp(request.start_date).normalize()
    formal_end = pd.Timestamp(request.end_date).normalize()

    # Base market calendar through formal end, then optionally one post-end day
    # for terminal next-open mapping only.
    base_calendar = _market_calendar_from_db(
        start_date=cal_start,
        end_date=request.end_date,
        db_path=db_path,
    )
    mapping_calendar = _extend_calendar_with_next_open(
        calendar=base_calendar,
        formal_end=formal_end,
        db_path=db_path,
    )
    formal_calendar = [d for d in base_calendar if formal_start <= d <= formal_end]
    if not formal_calendar:
        raise CrossSectionProductError("no trading days inside the requested date range")

    # Signal calendar restricted to formal range; execution mapping may use the
    # extra post-end day present in mapping_calendar.
    schedule = build_rebalance_schedule(
        mapping_calendar,
        frequency=frequency,  # type: ignore[arg-type]
        start_date=request.start_date,
        end_date=request.end_date,
    )
    signal_dates = (
        [normalize_trade_date(value) for value in schedule["signal_date"].tolist()]
        if not schedule.empty
        else []
    )
    # Guard: never allow signal dates after formal end.
    signal_dates = [d for d in signal_dates if formal_start <= d <= formal_end]
    if not schedule.empty:
        schedule = schedule[
            schedule["signal_date"].map(normalize_trade_date).isin(set(signal_dates))
        ].reset_index(drop=True)

    signal_by_execution = (
        {_iso(row.trade_date): _iso(row.signal_date) for row in schedule.itertuples(index=False)}
        if not schedule.empty
        else {}
    )

    universe = build_historical_universe(
        signal_dates,
        index_code=index_code,
        source="index",
        db_path=db_path,
    )

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

    union_assets = sorted({asset for assets in assets_by_signal.values() for asset in assets})
    skipped_signals = _collect_out_of_range_skips(
        schedule,
        formal_start=formal_start,
        formal_end=formal_end,
    )

    if not union_assets:
        # Full-window empty historical universe → deterministic cash-only result.
        # No real or placeholder tickers are injected.
        empty_targets = pd.DataFrame(
            columns=["trade_date", "asset_id", "target_weight", "priority", "signal_date"]
        )
        portfolio_result = build_cash_only_portfolio_result(
            config=config,
            formal_trade_dates=formal_calendar,
        )
        strategy = get_strategy(request.strategy_code, request.strategy_version)
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
            "empty_historical_universe": True,
            "cash_only_result": True,
            "warmup": {
                "momentum_lookback": lookback,
                "calendar_padding_days": warmup_calendar_days,
                "formal_decisions_not_before": request.start_date,
            },
            "market_trade_date_count": len(formal_calendar),
            "mapping_calendar_extra_days": max(0, len(mapping_calendar) - len(base_calendar)),
        }
        return run, skipped_signals, meta

    price_start = (
        pd.Timestamp(request.start_date) - pd.Timedelta(days=warmup_calendar_days)
    ).strftime("%Y-%m-%d")
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

    factor_frame = generate_factor_frame(
        [FactorRequest(code=MOMENTUM_FACTOR_CODE, parameters={"lookback": lookback})],
        universe=universe,
        prices=(
            price_df.rename(columns={"asset_id": ASSET_ID})
            if ASSET_ID not in price_df.columns
            else price_df
        ),
    )
    if DEFAULT_SCORE_COLUMN not in factor_frame.columns:
        score_cols = [c for c in factor_frame.columns if c not in {TRADE_DATE, ASSET_ID}]
        if not score_cols:
            raise CrossSectionProductError("momentum factor frame has no score column")
        factor_frame = factor_frame.rename(columns={score_cols[0]: DEFAULT_SCORE_COLUMN})

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
            # Mapping calendar includes optional post-end day so strategy schedule
            # can form terminal next-open executions once.
            runtime_context={"trading_days": list(mapping_calendar)},
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

    execution_targets, filter_skips = _filter_execution_targets(
        target_weights,
        start_date=request.start_date,
        end_date=request.end_date,
        signal_by_execution=signal_by_execution,
    )
    # Merge skips deterministically by (signal_date, reason).
    skip_map: dict[tuple[str | None, str | None], dict[str, str]] = {}
    for item in skipped_signals + filter_skips:
        key = (item.get("signal_date"), item.get("reason"))
        skip_map[key] = item
    skipped_signals = [
        skip_map[key]
        for key in sorted(skip_map.keys(), key=lambda item: (item[0] or "", item[1] or ""))
    ]

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
        "empty_historical_universe": False,
        "cash_only_result": False,
        "warmup": {
            "momentum_lookback": lookback,
            "calendar_padding_days": warmup_calendar_days,
            "formal_decisions_not_before": request.start_date,
        },
        "market_trade_date_count": len(market_trade_dates(formal_prices)),
        "mapping_calendar_extra_days": max(0, len(mapping_calendar) - len(base_calendar)),
    }
    return run, skipped_signals, meta
