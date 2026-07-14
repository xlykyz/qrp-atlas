"""Adapters from registered strategy decisions to portfolio target weights."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qrp_atlas.strategies import (
    StrategyAction,
    StrategyInput,
    StrategyRunResult,
    get_strategy,
)
from qrp_atlas.strategies.validation import resolve_parameters

from ..runtime.strategy import prepare_strategy_data
from .engine import PortfolioBacktestEngine
from .models import PortfolioBacktestConfig, PortfolioBacktestResult


@dataclass(frozen=True)
class StrategyPortfolioBacktestRun:
    """Strategy decisions, generated targets, and portfolio execution output."""

    strategy_result: StrategyRunResult
    target_weights: pd.DataFrame
    portfolio_result: PortfolioBacktestResult


@dataclass
class _Candidate:
    weight: float | None = None
    score: float = 0.0
    priority: float | None = None
    rank: int | None = None


def _finite_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _decision_rank(decision) -> int | None:
    evidence = decision.evidence or {}
    raw = evidence.get("rank")
    if raw is None:
        return None
    try:
        rank = int(raw)
    except (TypeError, ValueError):
        return None
    return rank if rank > 0 else None


def _decision_priority(decision, score: float | None) -> float:
    """Resolve selection priority; rank is authoritative when present."""
    rank = _decision_rank(decision)
    if rank is not None:
        return float(-rank)
    evidence = decision.evidence or {}
    raw = evidence.get("priority")
    parsed = _finite_or_none(raw)
    if parsed is not None:
        return parsed
    return float(score or 0.0)


def strategy_decisions_to_target_weights(
    strategy_result: StrategyRunResult,
    *,
    max_positions: int,
    max_weight_per_asset: float,
    default_weight: float | None = None,
    cash_buffer: float = 0.0,
    emit_unchanged_snapshots: bool = False,
) -> pd.DataFrame:
    """Convert long-only ENTER/HOLD/EXIT decisions into full target snapshots.

    Capacity is resolved by rank when available (``priority = -rank``), else by
    latest score, then asset code. Explicit positive ``decision.weight`` wins;
    otherwise selected assets receive ``default_weight`` or equal weight inside
    ``1 - cash_buffer``. Weights above the gross target are scaled down; sub-target
    totals preserve residual cash and never re-inflate past
    ``max_weight_per_asset``.

    Parameters
    ----------
    emit_unchanged_snapshots:
        When False (default, legacy behavior), dates with no ENTER/EXIT and no
        score/weight change are skipped. When True, every decision date emits a
        complete target snapshot even if holdings are unchanged. Cross-sectional
        strategies should enable this so rebalances can correct drift and retry
        blocked fills.
    """

    if max_positions <= 0:
        raise ValueError("max_positions must be positive")
    if not 0 < max_weight_per_asset <= 1:
        raise ValueError("max_weight_per_asset must be in (0, 1]")
    if not 0 <= cash_buffer < 1:
        raise ValueError("cash_buffer must be in [0, 1)")
    if default_weight is not None and not 0 < default_weight <= max_weight_per_asset:
        raise ValueError("default_weight must be in (0, max_weight_per_asset]")

    decisions = sorted(
        strategy_result.decisions,
        key=lambda item: (pd.Timestamp(item.trade_date), item.asset_id),
    )
    active: dict[str, _Candidate] = {}
    previous_selected: set[str] = set()
    rows: list[dict[str, Any]] = []

    for trade_date, group in _group_decisions_by_date(decisions):
        changed = False
        for decision in group:
            if decision.direction != "long":
                raise ValueError("portfolio strategy adapter supports long decisions only")
            score = _finite_or_none(decision.score)
            weight = _finite_or_none(decision.weight)
            if weight is not None and weight <= 0:
                weight = None
            rank = _decision_rank(decision)
            priority = _decision_priority(decision, score)

            if decision.action is StrategyAction.ENTER:
                active[decision.asset_id] = _Candidate(
                    weight=weight,
                    score=score or 0.0,
                    priority=priority,
                    rank=rank,
                )
                changed = True
            elif decision.action is StrategyAction.EXIT:
                changed = active.pop(decision.asset_id, None) is not None or changed
            elif decision.action is StrategyAction.HOLD and decision.asset_id in active:
                candidate = active[decision.asset_id]
                next_score = candidate.score if score is None else score
                next_weight = candidate.weight if weight is None else weight
                next_rank = candidate.rank if rank is None else rank
                next_priority = (
                    candidate.priority if priority is None else priority
                )
                if (
                    next_score != candidate.score
                    or next_weight != candidate.weight
                    or next_rank != candidate.rank
                    or next_priority != candidate.priority
                ):
                    active[decision.asset_id] = _Candidate(
                        next_weight,
                        next_score,
                        next_priority,
                        next_rank,
                    )
                    changed = True

        if not changed and not emit_unchanged_snapshots:
            continue

        selected_items = sorted(
            active.items(),
            key=lambda item: (
                -(
                    item[1].priority
                    if item[1].priority is not None
                    else item[1].score
                ),
                item[0],
            ),
        )[:max_positions]
        selected = {asset_id for asset_id, _candidate in selected_items}
        weights = _resolve_weights(
            selected_items,
            max_weight_per_asset=max_weight_per_asset,
            default_weight=default_weight,
            cash_buffer=cash_buffer,
        )
        priority_map = {
            asset_id: (
                candidate.priority
                if candidate.priority is not None
                else candidate.score
            )
            for asset_id, candidate in selected_items
        }
        for asset_id in sorted(selected | previous_selected):
            rows.append(
                {
                    "trade_date": trade_date,
                    "asset_id": asset_id,
                    "target_weight": weights.get(asset_id, 0.0),
                    "priority": priority_map.get(asset_id, 0.0),
                }
            )
        previous_selected = selected

    return pd.DataFrame(
        rows,
        columns=["trade_date", "asset_id", "target_weight", "priority"],
    )


def _group_decisions_by_date(decisions):
    current_date = None
    group = []
    for decision in decisions:
        trade_date = pd.Timestamp(decision.trade_date).strftime("%Y-%m-%d")
        if current_date is not None and trade_date != current_date:
            yield current_date, group
            group = []
        current_date = trade_date
        group.append(decision)
    if current_date is not None:
        yield current_date, group


def _resolve_weights(
    selected_items: list[tuple[str, _Candidate]],
    *,
    max_weight_per_asset: float,
    default_weight: float | None,
    cash_buffer: float = 0.0,
) -> dict[str, float]:
    if not selected_items:
        return {}
    target_gross = 1.0 - float(cash_buffer)
    if default_weight is not None:
        fallback = min(float(default_weight), max_weight_per_asset, target_gross)
    else:
        fallback = min(target_gross / len(selected_items), max_weight_per_asset)
    weights = {
        asset_id: min(
            candidate.weight if candidate.weight is not None else fallback,
            max_weight_per_asset,
        )
        for asset_id, candidate in selected_items
    }
    total = sum(weights.values())
    # Scale down when over the cash-buffered gross target; never re-inflate.
    if total > target_gross + 1e-12:
        scale = target_gross / total
        weights = {asset_id: weight * scale for asset_id, weight in weights.items()}
    return weights


def run_strategy_portfolio_backtest(
    code: str,
    price_df: pd.DataFrame,
    config: PortfolioBacktestConfig,
    *,
    parameters: Mapping[str, Any] | None = None,
    version: str | None = None,
    initial_positions: Mapping[str, bool] | None = None,
    runtime_context: Mapping[str, Any] | None = None,
    default_weight: float | None = None,
    cash_buffer: float = 0.0,
    emit_unchanged_snapshots: bool | None = None,
) -> StrategyPortfolioBacktestRun:
    """Run a registered strategy and execute its target-weight portfolio."""

    if any(bool(held) for held in (initial_positions or {}).values()):
        raise ValueError(
            "seeded holdings are not supported by PortfolioBacktestEngine; "
            "provide a cash-only initial account"
        )

    strategy = get_strategy(code, version)
    resolved_parameters = resolve_parameters(strategy.definition, parameters or {})
    prepared = prepare_strategy_data(price_df, strategy.definition, resolved_parameters)
    strategy_result = strategy.run(
        StrategyInput(
            prepared_data=prepared,
            parameters=resolved_parameters,
            initial_positions={},
            runtime_context=runtime_context or {},
        )
    )
    # Cross-sectional strategies always emit full rebalance snapshots.
    if emit_unchanged_snapshots is None:
        emit_unchanged_snapshots = strategy.definition.code in {
            "cross_sectional_momentum_long_only",
            "multifactor_long_only",
        }
    if cash_buffer == 0.0 and "cash_buffer" in resolved_parameters:
        try:
            cash_buffer = float(resolved_parameters["cash_buffer"] or 0.0)
        except (TypeError, ValueError):
            cash_buffer = 0.0
    target_weights = strategy_decisions_to_target_weights(
        strategy_result,
        max_positions=config.max_positions,
        max_weight_per_asset=config.max_weight_per_asset,
        default_weight=default_weight,
        cash_buffer=cash_buffer,
        emit_unchanged_snapshots=emit_unchanged_snapshots,
    )
    portfolio_result = PortfolioBacktestEngine().run(
        price_df,
        target_weights,
        config,
    )
    return StrategyPortfolioBacktestRun(
        strategy_result=strategy_result,
        target_weights=target_weights,
        portfolio_result=portfolio_result,
    )
