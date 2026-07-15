"""Product orchestration for earnings-forecast event drift backtests.

Reuses:
- query_earnings_forecast_as_of / to_earnings_forecast_event_frame
- event_drift_basic
- run_event_drift_portfolio_backtest
- PortfolioBacktestConfig / BacktestRunWriter (via service)

Does **not**:
- recompute available_trade_date
- apply a second next_open timing shift
- copy strategy state machine or portfolio execution logic
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from qrp_atlas.backtest.data import load_stock_prices
from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.pit_queries import (
    query_earnings_forecast_as_of,
    to_earnings_forecast_event_frame,
)
from qrp_atlas.backtest.portfolio import PortfolioBacktestConfig, PortfolioExecutionRule
from qrp_atlas.backtest.portfolio.strategy import (
    StrategyPortfolioBacktestRun,
    run_event_drift_portfolio_backtest,
)
from qrp_atlas.strategies import get_strategy
from qrp_atlas.strategies.validation import StrategyValidationError, resolve_parameters

from .cross_section import build_cash_only_portfolio_result
from .schemas import CreateBacktestTaskRequest

EVENT_DRIFT_BASIC_CODE = "event_drift_basic"
EVENT_PRODUCT_STRATEGY_CODES: frozenset[str] = frozenset({EVENT_DRIFT_BASIC_CODE})

REASON_NO_EXECUTION_DATE_IN_RANGE = "NO_EXECUTION_DATE_IN_RANGE"
REASON_EXIT_OUTSIDE_RANGE = "EXIT_OR_TARGET_OUTSIDE_REQUEST_RANGE"


class EventProductError(RuntimeError):
    """Raised when the event product path cannot complete safely."""


def is_event_product_strategy(strategy_code: str) -> bool:
    return str(strategy_code or "").strip() in EVENT_PRODUCT_STRATEGY_CODES


def _iso(value: Any) -> str:
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise EventProductError(f"invalid date: {value!r}")
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts.normalize().strftime("%Y-%m-%d")


def resolve_event_product_params(request: CreateBacktestTaskRequest) -> dict[str, Any]:
    """Resolve strategy params; portfolio capacity remains owned by position config."""

    strategy = get_strategy(request.strategy_code, request.strategy_version)
    try:
        resolved = resolve_parameters(strategy.definition, request.strategy_params or {})
    except StrategyValidationError as exc:
        raise EventProductError(str(exc)) from exc

    hold_days = int(resolved["hold_days"])
    min_mid = float(resolved["min_profit_change_midpoint"])
    if hold_days < 1:
        raise EventProductError("hold_days must be >= 1")

    # Capacity / weight SSOT is portfolio config, not a second strategy source.
    resolved["hold_days"] = hold_days
    resolved["min_profit_change_midpoint"] = min_mid
    return resolved


def _market_trade_dates(price_df: pd.DataFrame) -> list[str]:
    if price_df is None or price_df.empty or "trade_date" not in price_df.columns:
        return []
    days = sorted({_iso(v) for v in price_df["trade_date"].tolist()})
    return days


def _filter_targets_to_range(
    targets: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    """Keep only formal-range execution rows; out-of-range become skipped diagnostics."""

    if targets is None or targets.empty:
        return (
            pd.DataFrame(columns=["trade_date", "asset_id", "target_weight", "signal_date"]),
            [],
        )

    work = targets.copy()
    work["trade_date"] = work["trade_date"].map(_iso)
    if "signal_date" in work.columns:
        work["signal_date"] = work["signal_date"].map(
            lambda v: _iso(v) if v is not None and str(v) != "nan" else None
        )
    else:
        work["signal_date"] = work["trade_date"]

    in_range = (work["trade_date"] >= start_date) & (work["trade_date"] <= end_date)
    kept = work.loc[in_range].reset_index(drop=True)
    dropped = work.loc[~in_range]
    skipped: list[dict[str, str]] = []
    for row in dropped.itertuples(index=False):
        skipped.append(
            {
                "signal_date": str(getattr(row, "signal_date", None) or row.trade_date),
                "execution_date": str(row.trade_date),
                "asset_id": str(row.asset_id),
                "reason": REASON_EXIT_OUTSIDE_RANGE
                if str(row.trade_date) > end_date
                else REASON_NO_EXECUTION_DATE_IN_RANGE,
            }
        )
    # Stable order
    skipped.sort(key=lambda item: (item.get("signal_date") or "", item.get("asset_id") or "", item.get("reason") or ""))
    return kept, skipped


def _query_product_events(
    request: CreateBacktestTaskRequest,
    *,
    db_path: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load market-available formal disclosures as of the request end date.

    Product path never recomputes available_trade_date. Entry timing is already
    encoded in the event frame from 05-A.

    Disclosure selection:
    - ``include_all_disclosures=True`` so multiple formal disclosures under the
      same ``event_series_id`` remain tradable history (disclosure 1 then 2).
    - ``include_all_revisions=False`` so each formal disclosure keeps only its
      current canonical technical revision stored in the DB at task runtime.

    Technical-revision boundary (honest product claim):
    - Formal disclosure market timing has PIT isolation via available_trade_date.
    - Source data does not provide reliable technical-revision publication times,
      so product runs do **not** claim technical-revision knowledge-as-of isolation.
    - Snapshots record the actual ``source_record_id`` / ``revision_id`` used.
    """

    tickers = list(request.tickers or []) or None
    try:
        events = query_earnings_forecast_as_of(
            as_of_date=request.end_date,
            tickers=tickers,
            include_all_disclosures=True,
            include_all_revisions=False,
            as_event_frame=True,
            db_path=db_path,
        )
    except Exception as exc:  # noqa: BLE001
        raise EventProductError(f"failed to query earnings_forecast_event: {exc}") from exc

    events = to_earnings_forecast_event_frame(events)
    diagnostics: dict[str, Any] = {
        "as_of_date": request.end_date,
        "query_tickers": tickers,
        "raw_event_rows": int(len(events)),
        "event_type": "earnings_forecast",
        "query_mode": {
            "include_all_disclosures": True,
            "include_all_revisions": False,
            "reason": (
                "keep every market-available formal disclosure; one canonical "
                "technical revision per source_record_id"
            ),
        },
        "time_semantics": {
            "announcement_date": "evidence only; cannot trade same day",
            "available_trade_date": "entry trade date from 05-A (strictly next open after announcement)",
            "entry_price": "open on available_trade_date",
            "product_recomputes_available_trade_date": False,
            "product_second_next_open_shift": False,
            "formal_disclosure_pit": True,
            "technical_revision_knowledge_as_of": False,
            "technical_revision_policy": (
                "use current canonical technical revision stored for each "
                "source_record_id at task runtime; snapshot source_record_id "
                "and revision_id; do not claim revision publication-time isolation"
            ),
        },
    }
    if events is None or events.empty:
        diagnostics["selected_event_rows"] = 0
        diagnostics["source_record_ids"] = []
        diagnostics["revision_ids"] = []
        diagnostics["event_series_ids"] = []
        return to_earnings_forecast_event_frame(pd.DataFrame()), diagnostics

    work = events.copy()
    work["available_trade_date"] = work["available_trade_date"].map(_iso)
    work["announcement_date"] = work["announcement_date"].map(_iso)

    # Future events relative to the task window must not enter.
    work = work[work["available_trade_date"] <= request.end_date].copy()
    # Formal product entries only on/after request start; earlier availability is ignored.
    work = work[work["available_trade_date"] >= request.start_date].copy()
    # Hard PIT guard: never allow same-day announcement trade.
    same_day = work["available_trade_date"] <= work["announcement_date"]
    rejected_same_day = int(same_day.sum())
    if rejected_same_day:
        work = work.loc[~same_day].copy()

    # One canonical revision per formal disclosure should already hold from the
    # query layer; re-assert for product diagnostics/audit.
    if "source_record_id" in work.columns and not work.empty:
        before = int(len(work))
        work = (
            work.sort_values(
                [c for c in ("source_record_id", "revision_id", "ingested_at") if c in work.columns],
                kind="mergesort",
            )
            .drop_duplicates(subset=["source_record_id"], keep="last")
            .reset_index(drop=True)
        )
        diagnostics["collapsed_extra_revisions_per_source_record"] = before - int(len(work))
    else:
        diagnostics["collapsed_extra_revisions_per_source_record"] = 0

    source_ids = sorted({str(v) for v in work.get("source_record_id", pd.Series(dtype=str)).dropna().tolist()})
    revision_ids = sorted({str(v) for v in work.get("revision_id", pd.Series(dtype=str)).dropna().tolist()})
    series_ids = sorted({str(v) for v in work.get("event_series_id", pd.Series(dtype=str)).dropna().tolist()})
    disclosure_count_by_series: dict[str, int] = {}
    if "event_series_id" in work.columns and not work.empty:
        disclosure_count_by_series = {
            str(k): int(v)
            for k, v in work.groupby("event_series_id", dropna=False).size().items()
        }

    diagnostics["rejected_same_day_or_not_after_announcement"] = rejected_same_day
    diagnostics["selected_event_rows"] = int(len(work))
    diagnostics["event_tickers"] = sorted({str(t) for t in work.get("ticker", pd.Series(dtype=str)).tolist()})
    diagnostics["source_record_ids"] = source_ids
    diagnostics["revision_ids"] = revision_ids
    diagnostics["event_series_ids"] = series_ids
    diagnostics["disclosure_count_by_series"] = disclosure_count_by_series
    diagnostics["used_events"] = [
        {
            "ticker": str(row.ticker),
            "event_series_id": str(getattr(row, "event_series_id", "") or ""),
            "source_record_id": str(getattr(row, "source_record_id", "") or ""),
            "revision_id": str(getattr(row, "revision_id", "") or ""),
            "announcement_date": str(row.announcement_date),
            "available_trade_date": str(row.available_trade_date),
        }
        for row in work.itertuples(index=False)
    ]
    return work.reset_index(drop=True), diagnostics


def run_event_drift_product_backtest(
    request: CreateBacktestTaskRequest,
    *,
    db_path: Path | str,
) -> tuple[StrategyPortfolioBacktestRun, list[dict[str, str]], dict[str, Any]]:
    """Execute the product closed loop for event_drift_basic."""

    if not is_event_product_strategy(request.strategy_code):
        raise EventProductError(
            f"unsupported event product strategy: {request.strategy_code}"
        )
    if str(request.execution.entry_timing or "").strip() != "next_open":
        raise EventProductError("event_drift_basic only supports entry_timing=next_open")

    db_path = Path(db_path)
    resolved = resolve_event_product_params(request)
    hold_days = int(resolved["hold_days"])

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
        # Event runner requires open execution; no second product timing shift.
        execution=PortfolioExecutionRule(price_field="open", mark_price_field="close"),
    )

    events, event_meta = _query_product_events(request, db_path=db_path)

    # Market calendar from distinct trade dates in the formal window.
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
                [request.start_date, request.end_date],
            ).fetchall()
            post = con.execute(
                """
                SELECT MIN(trade_date)
                FROM daily_market_snapshot
                WHERE trade_date > ?
                """,
                [request.end_date],
            ).fetchone()
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        raise EventProductError(f"failed to load market calendar: {exc}") from exc

    formal_dates = [pd.Timestamp(r[0]).normalize() for r in rows]
    if not formal_dates:
        raise EventProductError("no trading days inside the requested date range")

    mapping_dates = list(formal_dates)
    if post and post[0] is not None:
        next_day = pd.Timestamp(post[0]).normalize()
        if next_day not in mapping_dates:
            mapping_dates.append(next_day)
    open_dates = [_iso(d) for d in mapping_dates]
    formal_iso = [_iso(d) for d in formal_dates]

    if events.empty:
        portfolio_result = build_cash_only_portfolio_result(
            config=config,
            formal_trade_dates=formal_dates,
        )
        strategy = get_strategy(request.strategy_code, request.strategy_version)
        from qrp_atlas.strategies.models import StrategyRunResult

        # Empty EventFrame is a valid product outcome; do not force strategy
        # indicator attachment on a zero-row frame.
        strategy_result = StrategyRunResult(
            definition=strategy.definition,
            parameters=resolved,
            decisions=(),
            diagnostics=("no_events_in_request_range", "cash_only_result"),
        )
        run = StrategyPortfolioBacktestRun(
            strategy_result=strategy_result,
            target_weights=pd.DataFrame(
                columns=["trade_date", "asset_id", "target_weight", "signal_date"]
            ),
            portfolio_result=portfolio_result,
        )
        meta = {
            "event": {
                **event_meta,
                "empty_events": True,
                "cash_only_result": True,
                "hold_days": hold_days,
                "resolved_strategy_params": resolved,
                "date_mapping_owner": "event_available_trade_date",
                "product_timing_shift": False,
            }
        }
        return run, [], meta

    tickers = sorted({str(t) for t in events["ticker"].tolist()})
    # Pad prices for hold window exits and any pre-start bars if needed.
    pad_days = max(hold_days * 3, 20)
    price_start = (
        pd.Timestamp(request.start_date) - pd.Timedelta(days=pad_days)
    ).strftime("%Y-%m-%d")
    price_end = open_dates[-1]
    try:
        price_df = load_stock_prices(
            tickers=tickers,
            start_date=price_start,
            end_date=price_end,
            db_path=db_path,
        )
    except Exception as exc:  # noqa: BLE001
        raise EventProductError(f"failed to load market data: {exc}") from exc
    if price_df is None or price_df.empty:
        raise EventProductError("no market data found for event tickers")

    price_df = price_df.copy()
    price_df["trade_date"] = pd.to_datetime(price_df["trade_date"]).dt.normalize()
    formal_prices = price_df[
        (price_df["trade_date"] >= pd.Timestamp(request.start_date))
        & (price_df["trade_date"] <= pd.Timestamp(request.end_date))
    ].copy()
    if formal_prices.empty:
        raise EventProductError("insufficient market data inside the requested date range")

    # Runner owns open execution and injects max_positions; product does not shift dates.
    try:
        run = run_event_drift_portfolio_backtest(
            events,
            # Provide calendar through optional post-end day so exit mapping can form.
            price_df=price_df.reset_index(drop=True),
            config=config,
            trading_days=open_dates,
            parameters=resolved,
            version=request.strategy_version,
            cash_buffer=0.0,
            strategy_code=request.strategy_code,
        )
    except Exception as exc:  # noqa: BLE001
        raise EventProductError(f"event portfolio backtest failed: {exc}") from exc

    # Restrict formal engine outputs to request window by re-running only if needed:
    # filter targets first, then re-run engine on formal prices when rows were dropped.
    filtered_targets, skipped = _filter_targets_to_range(
        run.target_weights,
        start_date=request.start_date,
        end_date=request.end_date,
    )

    if len(skipped) > 0 or (not filtered_targets.empty and len(filtered_targets) != len(run.target_weights)):
        from qrp_atlas.backtest.portfolio.engine import PortfolioBacktestEngine

        portfolio_result = PortfolioBacktestEngine().run(
            formal_prices.reset_index(drop=True),
            filtered_targets,
            config,
        )
        run = StrategyPortfolioBacktestRun(
            strategy_result=run.strategy_result,
            target_weights=filtered_targets,
            portfolio_result=portfolio_result,
        )
    else:
        # Ensure portfolio snapshots stay in formal window even if engine saw extra days.
        snapshots = [
            s
            for s in run.portfolio_result.snapshots
            if request.start_date <= s.trade_date <= request.end_date
        ]
        if len(snapshots) != len(run.portfolio_result.snapshots):
            from qrp_atlas.backtest.portfolio.engine import PortfolioBacktestEngine

            portfolio_result = PortfolioBacktestEngine().run(
                formal_prices.reset_index(drop=True),
                filtered_targets if not filtered_targets.empty else run.target_weights,
                config,
            )
            run = StrategyPortfolioBacktestRun(
                strategy_result=run.strategy_result,
                target_weights=filtered_targets if not filtered_targets.empty else run.target_weights,
                portfolio_result=portfolio_result,
            )

    diagnostics = list(run.strategy_result.diagnostics or ())
    meta = {
        "event": {
            **event_meta,
            "empty_events": False,
            "cash_only_result": False,
            "hold_days": hold_days,
            "resolved_strategy_params": resolved,
            "date_mapping_owner": "event_available_trade_date",
            "product_timing_shift": False,
            "open_dates_count": len(open_dates),
            "formal_trade_date_count": len(formal_iso),
            "decision_count": len(run.strategy_result.decisions),
            "target_rows": int(len(run.target_weights)),
            "skipped_out_of_range": len(skipped),
            "strategy_diagnostics": diagnostics[:50],
            "universe_mode": request.universe_mode,
            "tickers_filter": list(request.tickers or []),
        }
    }
    return run, skipped, meta
