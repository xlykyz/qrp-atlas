"""Residual relative-value research analytics and public portfolio runner."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import (
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioBacktestResult,
    PortfolioExecutionRule,
    strategy_decisions_to_target_weights,
)
from qrp_atlas.backtest.product.timing import (
    REASON_NO_EXECUTION_DATE_IN_RANGE,
    market_trade_dates,
    shift_target_weights_to_execution_dates,
)
from qrp_atlas.backtest.residual_data import (
    ResidualDataError,
    ResidualPanelPreparation,
    prepare_market_residual_panel,
)
from qrp_atlas.contracts import ASSET_ID, TICKER, TRADE_DATE
from qrp_atlas.indicators.stock.residual import (
    RESIDUAL_RETURN,
    RESIDUAL_ZSCORE,
    ResidualIndicatorError,
    calculate_market_residuals,
)
from qrp_atlas.strategies import StrategyInput, StrategyRunResult, get_strategy
from qrp_atlas.strategies.builtin.residual import (
    STRATEGY_CODE,
    STRATEGY_VERSION,
)
from qrp_atlas.strategies.validation import resolve_parameters

from .forward_returns import DEFAULT_FORWARD_HORIZONS, compute_forward_returns
from .groups import assign_factor_groups, compute_group_returns


class ResidualResearchError(ValueError):
    """Raised when residual research cannot be orchestrated."""


@dataclass(frozen=True)
class ResidualResearchResult:
    """Structured residual research outputs for post-hoc evaluation only."""

    residual_panel: pd.DataFrame
    group_assignments: pd.DataFrame
    group_returns: pd.DataFrame
    group_spreads: pd.DataFrame
    extreme_group_comparison: pd.DataFrame
    diagnostics: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "residual_panel": self.residual_panel.to_dict(orient="list"),
            "group_assignments": self.group_assignments.to_dict(orient="list"),
            "group_returns": self.group_returns.to_dict(orient="list"),
            "group_spreads": self.group_spreads.to_dict(orient="list"),
            "extreme_group_comparison": self.extreme_group_comparison.to_dict(
                orient="list"
            ),
            "diagnostics": list(self.diagnostics),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class ResidualStrategyBacktestRun:
    """Public residual strategy decisions plus portfolio execution output."""

    preparation: ResidualPanelPreparation
    strategy_result: StrategyRunResult
    signal_target_weights: pd.DataFrame
    execution_target_weights: pd.DataFrame
    portfolio_result: PortfolioBacktestResult
    skipped_signals: tuple[dict[str, str], ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "preparation": self.preparation.to_dict(),
            "strategy_result": self.strategy_result.to_dict(),
            "signal_target_weights": self.signal_target_weights.to_dict(orient="list"),
            "execution_target_weights": self.execution_target_weights.to_dict(
                orient="list"
            ),
            "portfolio_result": self.portfolio_result.to_dict(),
            "skipped_signals": list(self.skipped_signals),
            "metadata": dict(self.metadata),
        }


def run_residual_research(
    asset_prices: pd.DataFrame,
    benchmark_prices: pd.DataFrame,
    *,
    benchmark_id: str | None = None,
    window: int = 60,
    min_periods: int | None = None,
    z_window: int = 60,
    fit_intercept: bool = True,
    n_groups: int = 5,
    horizons: Sequence[int] = DEFAULT_FORWARD_HORIZONS,
    residual_panel: pd.DataFrame | None = None,
) -> ResidualResearchResult:
    """Evaluate residual signals with forward returns; never feeds strategy inputs."""

    diagnostics: list[str] = []
    if residual_panel is None:
        try:
            preparation = prepare_market_residual_panel(
                asset_prices,
                benchmark_prices,
                benchmark_id=benchmark_id,
                window=window,
                min_periods=min_periods,
                z_window=z_window,
                fit_intercept=fit_intercept,
                compute_residuals=True,
            )
        except ResidualDataError as exc:
            raise ResidualResearchError(str(exc)) from exc
        panel = preparation.panel.copy()
        diagnostics.extend(preparation.diagnostics)
        prep_meta = dict(preparation.metadata)
    else:
        if not isinstance(residual_panel, pd.DataFrame):
            raise ResidualResearchError("residual_panel must be a pandas DataFrame")
        panel = residual_panel.copy()
        if RESIDUAL_ZSCORE not in panel.columns:
            try:
                computed = calculate_market_residuals(
                    panel,
                    window=window,
                    min_periods=min_periods,
                    z_window=z_window,
                    fit_intercept=fit_intercept,
                )
            except ResidualIndicatorError as exc:
                raise ResidualResearchError(str(exc)) from exc
            value_cols = [
                col
                for col in [
                    "rolling_alpha",
                    "rolling_beta",
                    "rolling_r2",
                    RESIDUAL_RETURN,
                    RESIDUAL_ZSCORE,
                    "diagnostic_code",
                ]
                if col in computed.frame.columns
            ]
            keys = [TRADE_DATE]
            if ASSET_ID in panel.columns and ASSET_ID in computed.frame.columns:
                keys.append(ASSET_ID)
            elif TICKER in panel.columns and TICKER in computed.frame.columns:
                keys.append(TICKER)
            panel = panel.merge(
                computed.frame[keys + value_cols],
                on=keys,
                how="left",
                sort=False,
            )
            diagnostics.extend(computed.diagnostics)
            prep_meta = dict(computed.metadata)
        else:
            prep_meta = {
                "benchmark_id": benchmark_id,
                "window": window,
                "min_periods": min_periods if min_periods is not None else window,
                "z_window": z_window,
                "fit_intercept": fit_intercept,
            }

    if panel.empty:
        empty_groups = pd.DataFrame()
        return ResidualResearchResult(
            residual_panel=panel,
            group_assignments=empty_groups,
            group_returns=empty_groups,
            group_spreads=empty_groups,
            extreme_group_comparison=empty_groups,
            diagnostics=tuple(diagnostics),
            metadata={
                **prep_meta,
                "usable_sample_count": 0,
                "n_groups": n_groups,
                "horizons": list(horizons),
            },
        )

    research_frame = panel.copy()
    if ASSET_ID not in research_frame.columns and TICKER in research_frame.columns:
        research_frame[ASSET_ID] = research_frame[TICKER]
    usable = research_frame[RESIDUAL_ZSCORE].notna().sum() if RESIDUAL_ZSCORE in research_frame else 0

    trading_days = sorted(pd.to_datetime(asset_prices[TRADE_DATE]).unique())
    forward = compute_forward_returns(
        asset_prices.rename(columns={TICKER: ASSET_ID}) if ASSET_ID not in asset_prices.columns and TICKER in asset_prices.columns else asset_prices,
        trading_days=trading_days,
        horizons=horizons,
        price_field="close",
    )
    try:
        assignments = assign_factor_groups(
            research_frame[[TRADE_DATE, ASSET_ID, RESIDUAL_ZSCORE]].rename(
                columns={RESIDUAL_ZSCORE: "residual_zscore"}
            ),
            factor_columns="residual_zscore",
            n_groups=n_groups,
        )
        group_result = compute_group_returns(
            assignments,
            forward,
            horizons=horizons,
        )
    except Exception as exc:  # noqa: BLE001 - normalize research boundary errors
        raise ResidualResearchError(str(exc)) from exc

    extreme = _extreme_group_comparison(group_result.group_returns, n_groups=n_groups)
    metadata = {
        **prep_meta,
        "usable_sample_count": int(usable),
        "n_groups": int(n_groups),
        "horizons": [int(value) for value in horizons],
        "benchmark_id": prep_meta.get("benchmark_id", benchmark_id),
        "note": "forward outcomes are evaluation-only and never enter strategy decisions",
    }
    return ResidualResearchResult(
        residual_panel=research_frame.sort_values(
            [TRADE_DATE, ASSET_ID], kind="mergesort"
        ).reset_index(drop=True),
        group_assignments=group_result.assignments,
        group_returns=group_result.group_returns,
        group_spreads=group_result.spreads,
        extreme_group_comparison=extreme,
        diagnostics=tuple(diagnostics),
        metadata=metadata,
    )


def run_market_residual_mean_reversion_backtest(
    asset_prices: pd.DataFrame,
    benchmark_prices: pd.DataFrame,
    config: PortfolioBacktestConfig,
    *,
    benchmark_id: str | None = None,
    parameters: Mapping[str, Any] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    entry_timing: str = "next_open",
    version: str | None = None,
) -> ResidualStrategyBacktestRun:
    """Public long-only residual mean-reversion portfolio loop.

    Chain:
    prices + benchmark
    -> prepare_market_residual_panel
    -> market_residual_mean_reversion decisions
    -> strategy_decisions_to_target_weights
    -> next-open execution shift
    -> PortfolioBacktestEngine
    """

    strategy = get_strategy(STRATEGY_CODE, version or STRATEGY_VERSION)
    resolved = resolve_parameters(strategy.definition, parameters or {})
    validate_relationships = getattr(strategy, "_validate_relationships", None)
    if callable(validate_relationships):
        validate_relationships(resolved)

    try:
        preparation = prepare_market_residual_panel(
            asset_prices,
            benchmark_prices,
            benchmark_id=benchmark_id,
            window=int(resolved["window"]),
            min_periods=int(resolved["min_periods"]),
            z_window=int(resolved["z_window"]),
            fit_intercept=bool(resolved["fit_intercept"]),
            compute_residuals=True,
        )
    except ResidualDataError as exc:
        raise ResidualResearchError(str(exc)) from exc

    panel = preparation.panel.copy()
    if panel.empty:
        raise ResidualResearchError("residual panel is empty")

    if TICKER not in panel.columns:
        panel[TICKER] = panel[ASSET_ID].astype(str)

    formal_start = pd.Timestamp(start_date) if start_date is not None else None
    formal_end = pd.Timestamp(end_date) if end_date is not None else None
    prepared = panel
    if formal_start is not None:
        prepared = prepared[pd.to_datetime(prepared[TRADE_DATE]) >= formal_start]
    if formal_end is not None:
        prepared = prepared[pd.to_datetime(prepared[TRADE_DATE]) <= formal_end]
    prepared = prepared.sort_values([TICKER, TRADE_DATE], kind="mergesort").reset_index(
        drop=True
    )
    if prepared.empty:
        raise ResidualResearchError("no residual rows inside the requested date range")

    strategy_result = strategy.run(
        StrategyInput(
            prepared_data=prepared,
            parameters=resolved,
            initial_positions={},
            runtime_context={
                "benchmark_id": preparation.metadata.get("benchmark_id", benchmark_id)
            },
        )
    )

    signal_targets = strategy_decisions_to_target_weights(
        strategy_result,
        max_positions=config.max_positions,
        max_weight_per_asset=config.max_weight_per_asset,
        default_weight=None,
        cash_buffer=0.0,
        emit_unchanged_snapshots=True,
    )

    execution_prices = asset_prices.copy()
    if ASSET_ID not in execution_prices.columns and TICKER in execution_prices.columns:
        execution_prices[ASSET_ID] = execution_prices[TICKER]
    if formal_start is not None or formal_end is not None:
        dates = pd.to_datetime(execution_prices[TRADE_DATE])
        mask = pd.Series(True, index=execution_prices.index)
        if formal_start is not None:
            mask &= dates >= formal_start
        if formal_end is not None:
            mask &= dates <= formal_end
        formal_prices = execution_prices.loc[mask].copy()
    else:
        formal_prices = execution_prices

    if formal_prices.empty:
        raise ResidualResearchError("no asset prices inside the requested date range")

    trade_dates = market_trade_dates(formal_prices)
    execution_targets, skipped = shift_target_weights_to_execution_dates(
        signal_targets,
        entry_timing=entry_timing,
        trade_dates=trade_dates,
        end_date=end_date,
    )

    portfolio_result = PortfolioBacktestEngine().run(
        formal_prices,
        execution_targets.drop(columns=["signal_date"], errors="ignore"),
        config,
    )

    metadata = {
        "strategy_code": strategy.definition.code,
        "strategy_version": strategy.definition.version,
        "benchmark_id": preparation.metadata.get("benchmark_id", benchmark_id),
        "entry_timing": entry_timing,
        "start_date": start_date,
        "end_date": end_date,
        "parameters": dict(resolved),
        "residual_preparation": dict(preparation.metadata),
        "indicator_version": (
            preparation.metadata.get("residual_calculation", {}) or {}
        ).get("calculation_version"),
        "signal_semantics": {
            "signal_date": "strategy decision date after close",
            "execution_date": "next market session under next_open",
            "benchmark_in_portfolio": False,
            "position_style": "long_only_equities",
        },
        "no_execution_date_reason": REASON_NO_EXECUTION_DATE_IN_RANGE,
    }
    return ResidualStrategyBacktestRun(
        preparation=preparation,
        strategy_result=strategy_result,
        signal_target_weights=signal_targets,
        execution_target_weights=execution_targets,
        portfolio_result=portfolio_result,
        skipped_signals=tuple(skipped),
        metadata=metadata,
    )


def _extreme_group_comparison(
    group_returns: pd.DataFrame, *, n_groups: int
) -> pd.DataFrame:
    if group_returns is None or group_returns.empty:
        return pd.DataFrame(
            columns=[
                "horizon",
                "extreme_negative_group",
                "other_groups_mean",
                "spread",
                "extreme_obs",
                "other_obs",
            ]
        )
    rows: list[dict[str, Any]] = []
    extreme_group = 1
    for horizon, horizon_df in group_returns.groupby("horizon", sort=True):
        extreme = horizon_df[horizon_df["group"] == extreme_group]["group_return"]
        others = horizon_df[horizon_df["group"] != extreme_group]["group_return"]
        extreme_mean = float(extreme.mean()) if not extreme.empty else float("nan")
        other_mean = float(others.mean()) if not others.empty else float("nan")
        rows.append(
            {
                "horizon": int(horizon),
                "extreme_negative_group": extreme_mean,
                "other_groups_mean": other_mean,
                "spread": extreme_mean - other_mean
                if pd.notna(extreme_mean) and pd.notna(other_mean)
                else float("nan"),
                "extreme_obs": int(extreme.notna().sum()),
                "other_obs": int(others.notna().sum()),
                "n_groups": int(n_groups),
            }
        )
    return pd.DataFrame(rows)


__all__ = [
    "ResidualResearchError",
    "ResidualResearchResult",
    "ResidualStrategyBacktestRun",
    "run_market_residual_mean_reversion_backtest",
    "run_residual_research",
]
