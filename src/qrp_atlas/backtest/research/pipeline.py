"""Orchestrate the cross-sectional research and portfolio evaluation loop."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from qrp_atlas.backtest.portfolio import (
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioBacktestResult,
    strategy_decisions_to_target_weights,
    validate_target_weights,
)
from qrp_atlas.contracts import ASSET_ID, TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    normalize_feature_columns,
    normalize_trade_date,
)
from qrp_atlas.strategies import (
    StrategyInput,
    StrategyRunResult,
    build_rebalance_schedule,
    get_strategy,
    run_strategy,
)
from qrp_atlas.strategies.validation import resolve_parameters

from .exposures import TargetExposureResult, analyze_target_exposures
from .forward_returns import (
    DEFAULT_FORWARD_HORIZONS,
    compute_forward_returns,
)
from .groups import GroupReturnResult, assign_factor_groups, compute_group_returns
from .ic import compute_information_coefficient, summarize_information_coefficient


class CrossSectionResearchError(ValueError):
    """Raised when the research loop cannot be orchestrated."""


@dataclass(frozen=True)
class CrossSectionResearchResult:
    """Structured, deterministic output of one research run."""

    forward_returns: pd.DataFrame
    daily_ic: pd.DataFrame
    ic_summary: pd.DataFrame
    group_assignments: pd.DataFrame
    group_returns: pd.DataFrame
    group_spreads: pd.DataFrame
    strategy_result: StrategyRunResult | None
    target_weights: pd.DataFrame
    target_exposures: TargetExposureResult
    portfolio_result: PortfolioBacktestResult | None
    diagnostics: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "forward_returns": self.forward_returns.to_dict(orient="list"),
            "daily_ic": self.daily_ic.to_dict(orient="list"),
            "ic_summary": self.ic_summary.to_dict(orient="list"),
            "group_assignments": self.group_assignments.to_dict(orient="list"),
            "group_returns": self.group_returns.to_dict(orient="list"),
            "group_spreads": self.group_spreads.to_dict(orient="list"),
            "strategy_result": (
                None if self.strategy_result is None else self.strategy_result.to_dict()
            ),
            "target_weights": self.target_weights.to_dict(orient="list"),
            "target_exposures": {
                "numeric": self.target_exposures.numeric.to_dict(orient="list"),
                "categorical": self.target_exposures.categorical.to_dict(
                    orient="list"
                ),
            },
            "portfolio_result": (
                None if self.portfolio_result is None else self.portfolio_result.to_dict()
            ),
            "diagnostics": list(self.diagnostics),
            "metadata": dict(self.metadata),
        }


def run_cross_section_research(
    *,
    factor_frame: pd.DataFrame,
    price_df: pd.DataFrame,
    trading_days: Sequence[Any],
    factor_columns: str | Sequence[str],
    strategy_code: str | None = "cross_sectional_momentum_long_only",
    strategy_parameters: Mapping[str, Any] | None = None,
    strategy_version: str | None = None,
    eligibility: pd.DataFrame | None = None,
    exposure_panel: pd.DataFrame | None = None,
    portfolio_config: PortfolioBacktestConfig | None = None,
    horizons: Sequence[int] = DEFAULT_FORWARD_HORIZONS,
    n_groups: int = 5,
    rebalance_frequency: str | None = None,
    explicit_dates: Sequence[Any] | None = None,
    score_column: str | None = None,
    price_field: str = "close",
    run_portfolio: bool = True,
    numeric_exposures: Sequence[str] | None = None,
    categorical_exposures: Sequence[str] | None = None,
    min_ic_obs: int = 3,
) -> CrossSectionResearchResult:
    """Orchestrate factor evaluation, 04-D strategy, portfolio and exposures.

    Future returns only enter evaluation outputs. Selection, decisions and
    target weights are produced solely from prepared factors/eligibility.

    Parameter priority for shared schedule knobs:

    ```text
    explicit run_cross_section_research convenience args
    > strategy_parameters
    > strategy definition defaults
    ```

    The same resolved parameter set drives the research schedule, strategy
    runtime and exposure signal/trade mapping.
    """
    factors = normalize_feature_columns(factor_columns)
    if not factors:
        raise CrossSectionResearchError("factor_columns must be non-empty")
    if factor_frame is None or not isinstance(factor_frame, pd.DataFrame):
        raise CrossSectionResearchError("factor_frame must be a pandas DataFrame")
    if price_df is None or not isinstance(price_df, pd.DataFrame):
        raise CrossSectionResearchError("price_df must be a pandas DataFrame")

    diagnostics: list[str] = []
    prepared_factors = factor_frame.copy()
    empty_universe = prepared_factors.empty
    if empty_universe:
        diagnostics.append("empty_factor_universe")

    # Empty research universe must not expand into the full price panel.
    if empty_universe:
        forward_returns = compute_forward_returns(
            price_df,
            trading_days=trading_days,
            horizons=horizons,
            price_field=price_field,
            as_of_dates=[],
            assets=[],
        )
    else:
        forward_returns = compute_forward_returns(
            price_df,
            trading_days=trading_days,
            horizons=horizons,
            price_field=price_field,
            as_of_dates=sorted(
                {
                    normalize_trade_date(value)
                    for value in prepared_factors[TRADE_DATE].tolist()
                }
            )
            if TRADE_DATE in prepared_factors.columns
            else [],
            assets=sorted(
                {str(value) for value in prepared_factors[ASSET_ID].tolist()}
            )
            if ASSET_ID in prepared_factors.columns
            else [],
        )

    daily_ic = compute_information_coefficient(
        prepared_factors,
        forward_returns,
        factor_columns=factors,
        horizons=horizons,
        min_obs=min_ic_obs,
    )
    ic_summary = summarize_information_coefficient(daily_ic)

    assignments = assign_factor_groups(
        prepared_factors,
        factor_columns=factors,
        n_groups=n_groups,
    )
    group_result: GroupReturnResult = compute_group_returns(
        assignments,
        forward_returns,
        horizons=horizons,
    )

    strategy_result: StrategyRunResult | None = None
    target_weights = pd.DataFrame(
        columns=["trade_date", "asset_id", "target_weight", "priority"]
    )
    portfolio_result: PortfolioBacktestResult | None = None
    resolved_parameters: dict[str, Any] = {}
    schedule = pd.DataFrame(
        {
            "signal_date": pd.Series(dtype="datetime64[ns]"),
            "trade_date": pd.Series(dtype="datetime64[ns]"),
        }
    )

    if strategy_code is None:
        diagnostics.append("strategy_skipped")
        # Without a strategy, still allow schedule construction from convenience
        # args / raw strategy_parameters for exposure mapping when targets exist.
        candidate = dict(strategy_parameters or {})
        if rebalance_frequency is not None:
            candidate["rebalance_frequency"] = rebalance_frequency
        if explicit_dates is not None:
            candidate["explicit_dates_json"] = json.dumps(
                [
                    normalize_trade_date(value).strftime("%Y-%m-%d")
                    for value in explicit_dates
                ]
            )
        resolved_parameters = candidate
        schedule = _build_schedule_from_resolved(
            trading_days,
            resolved_parameters,
            explicit_dates_override=explicit_dates,
        )
    else:
        strategy = get_strategy(strategy_code, strategy_version)
        merged = _merge_strategy_parameters(
            strategy.definition,
            strategy_parameters=strategy_parameters,
            rebalance_frequency=rebalance_frequency,
            explicit_dates=explicit_dates,
            score_column=score_column,
            factor_columns=factors,
        )
        try:
            resolved_parameters = resolve_parameters(strategy.definition, merged)
        except Exception as exc:  # StrategyValidationError and friends
            raise CrossSectionResearchError(str(exc)) from exc

        schedule = _build_schedule_from_resolved(
            trading_days,
            resolved_parameters,
            explicit_dates_override=explicit_dates,
        )

        runtime_context: dict[str, Any] = {"trading_days": list(trading_days)}
        if eligibility is not None:
            runtime_context["eligibility"] = eligibility
        explicit_for_runtime = _resolved_explicit_dates(
            resolved_parameters, explicit_dates
        )
        if explicit_for_runtime is not None:
            runtime_context["explicit_dates"] = list(explicit_for_runtime)

        strategy_result = run_strategy(
            strategy_code,
            StrategyInput(
                prepared_data=prepared_factors,
                parameters=resolved_parameters,
                runtime_context=runtime_context,
            ),
            version=strategy_version,
        )

        max_positions = int(resolved_parameters.get("max_positions") or 10)
        max_weight = float(resolved_parameters.get("max_weight_per_asset") or 1.0)
        cash_buffer = float(resolved_parameters.get("cash_buffer") or 0.0)
        if portfolio_config is not None:
            max_positions = min(max_positions, int(portfolio_config.max_positions))
            max_weight = min(max_weight, float(portfolio_config.max_weight_per_asset))

        target_weights = strategy_decisions_to_target_weights(
            strategy_result,
            max_positions=max_positions,
            max_weight_per_asset=max_weight,
            cash_buffer=cash_buffer,
            emit_unchanged_snapshots=True,
        )
        if portfolio_config is not None:
            validate_target_weights(target_weights, portfolio_config)
            if run_portfolio:
                portfolio_result = PortfolioBacktestEngine().run(
                    price_df,
                    target_weights,
                    portfolio_config,
                )
        else:
            diagnostics.append("portfolio_config_missing")

    if schedule.empty:
        diagnostics.append("empty_rebalance_schedule")

    if target_weights.empty or schedule.empty:
        from .exposures import (
            empty_categorical_exposures,
            empty_numeric_exposures,
        )

        exposure_result = TargetExposureResult(
            numeric=empty_numeric_exposures(),
            categorical=empty_categorical_exposures(),
        )
        if not target_weights.empty and schedule.empty:
            diagnostics.append("exposure_skipped_empty_schedule")
    else:
        exposure_result = analyze_target_exposures(
            target_weights,
            schedule=schedule,
            factor_frame=prepared_factors,
            exposure_panel=exposure_panel,
            numeric_exposures=numeric_exposures,
            categorical_exposures=categorical_exposures,
        )

    metadata = {
        "factor_columns": factors,
        "horizons": list(horizons),
        "n_groups": n_groups,
        "strategy_code": strategy_code,
        "strategy_version": strategy_version,
        "resolved_parameters": dict(resolved_parameters),
        "rebalance_frequency": resolved_parameters.get("rebalance_frequency"),
        "price_field": price_field,
    }
    return CrossSectionResearchResult(
        forward_returns=forward_returns,
        daily_ic=daily_ic,
        ic_summary=ic_summary,
        group_assignments=group_result.assignments,
        group_returns=group_result.group_returns,
        group_spreads=group_result.spreads,
        strategy_result=strategy_result,
        target_weights=target_weights,
        target_exposures=exposure_result,
        portfolio_result=portfolio_result,
        diagnostics=tuple(diagnostics),
        metadata=metadata,
    )


def _merge_strategy_parameters(
    definition,
    *,
    strategy_parameters: Mapping[str, Any] | None,
    rebalance_frequency: str | None,
    explicit_dates: Sequence[Any] | None,
    score_column: str | None,
    factor_columns: Sequence[str],
) -> dict[str, Any]:
    """Merge convenience args into declared strategy parameters only.

    Priority:
    convenience run args > strategy_parameters > definition defaults (later).
    """
    schema = set(definition.parameter_schema)
    raw = dict(strategy_parameters or {})
    unknown = sorted(set(raw) - schema)
    if unknown:
        raise CrossSectionResearchError(
            f"unknown strategy parameters for {definition.code}: {unknown}"
        )

    merged = dict(raw)

    # Convenience arguments override strategy_parameters when provided.
    if rebalance_frequency is not None and "rebalance_frequency" in schema:
        merged["rebalance_frequency"] = rebalance_frequency
    if explicit_dates is not None and "explicit_dates_json" in schema:
        merged["explicit_dates_json"] = json.dumps(
            [
                normalize_trade_date(value).strftime("%Y-%m-%d")
                for value in explicit_dates
            ]
        )
    if score_column is not None and "score_column" in schema:
        merged["score_column"] = score_column
    elif (
        "score_column" in schema
        and "score_column" not in merged
        and factor_columns
    ):
        # Momentum-style default only when the strategy declares score_column.
        merged["score_column"] = factor_columns[0]

    # Multifactor convenience: if caller provided research factor columns and
    # the strategy expects JSON factor config but no weights were given, keep
    # caller-provided JSON only. Do not invent weights.
    return merged


def _build_schedule_from_resolved(
    trading_days: Sequence[Any],
    resolved_parameters: Mapping[str, Any],
    *,
    explicit_dates_override: Sequence[Any] | None,
) -> pd.DataFrame:
    frequency = str(resolved_parameters.get("rebalance_frequency") or "weekly")
    explicit = _resolved_explicit_dates(resolved_parameters, explicit_dates_override)
    return build_rebalance_schedule(
        trading_days,
        frequency=frequency,  # type: ignore[arg-type]
        explicit_dates=explicit,
    )


def _resolved_explicit_dates(
    resolved_parameters: Mapping[str, Any],
    explicit_dates_override: Sequence[Any] | None,
) -> list[Any] | None:
    if explicit_dates_override is not None:
        return list(explicit_dates_override)
    raw = resolved_parameters.get("explicit_dates_json")
    if raw in (None, ""):
        return None
    if isinstance(raw, (list, tuple)):
        return list(raw)
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise CrossSectionResearchError(
                "explicit_dates_json must be valid JSON"
            ) from exc
        if not isinstance(parsed, list):
            raise CrossSectionResearchError("explicit_dates_json must be a JSON list")
        return list(parsed)
    raise CrossSectionResearchError("explicit_dates_json must be a JSON list/string")
