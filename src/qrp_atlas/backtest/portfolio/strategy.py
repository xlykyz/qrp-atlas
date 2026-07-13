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


def _finite_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def strategy_decisions_to_target_weights(
    strategy_result: StrategyRunResult,
    *,
    max_positions: int,
    max_weight_per_asset: float,
    default_weight: float | None = None,
) -> pd.DataFrame:
    """Convert long-only ENTER/HOLD/EXIT decisions into full target snapshots.

    Capacity is resolved by latest score, then asset code. Explicit positive
    ``decision.weight`` wins; otherwise selected assets receive ``default_weight``
    or equal weight. Weights are capped and proportionally scaled to a total of 1.
    """

    if max_positions <= 0:
        raise ValueError("max_positions must be positive")
    if not 0 < max_weight_per_asset <= 1:
        raise ValueError("max_weight_per_asset must be in (0, 1]")
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

            if decision.action is StrategyAction.ENTER:
                active[decision.asset_id] = _Candidate(
                    weight=weight,
                    score=score or 0.0,
                )
                changed = True
            elif decision.action is StrategyAction.EXIT:
                changed = active.pop(decision.asset_id, None) is not None or changed
            elif decision.action is StrategyAction.HOLD and decision.asset_id in active:
                candidate = active[decision.asset_id]
                next_score = candidate.score if score is None else score
                next_weight = candidate.weight if weight is None else weight
                if next_score != candidate.score or next_weight != candidate.weight:
                    active[decision.asset_id] = _Candidate(next_weight, next_score)
                    changed = True

        if not changed:
            continue

        selected_items = sorted(
            active.items(),
            key=lambda item: (-item[1].score, item[0]),
        )[:max_positions]
        selected = {asset_id for asset_id, _candidate in selected_items}
        weights = _resolve_weights(
            selected_items,
            max_weight_per_asset=max_weight_per_asset,
            default_weight=default_weight,
        )
        priority = {
            asset_id: candidate.score
            for asset_id, candidate in selected_items
        }
        for asset_id in sorted(selected | previous_selected):
            rows.append(
                {
                    "trade_date": trade_date,
                    "asset_id": asset_id,
                    "target_weight": weights.get(asset_id, 0.0),
                    "priority": priority.get(asset_id, 0.0),
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
) -> dict[str, float]:
    if not selected_items:
        return {}
    fallback = default_weight or min(1.0 / len(selected_items), max_weight_per_asset)
    weights = {
        asset_id: min(candidate.weight or fallback, max_weight_per_asset)
        for asset_id, candidate in selected_items
    }
    total = sum(weights.values())
    if total > 1.0 + 1e-12:
        weights = {asset_id: weight / total for asset_id, weight in weights.items()}
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
    target_weights = strategy_decisions_to_target_weights(
        strategy_result,
        max_positions=config.max_positions,
        max_weight_per_asset=config.max_weight_per_asset,
        default_weight=default_weight,
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
