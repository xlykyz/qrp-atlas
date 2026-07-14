"""Walk-forward residual robustness validation and cost stress testing.

This module orchestrates train / validation / test splits for
``market_residual_mean_reversion`` using the existing public residual runner.
It does not reimplement residual math, strategy state machines, or portfolio
execution.
"""

from __future__ import annotations

import itertools
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from typing import Any

import pandas as pd

from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio.models import PortfolioBacktestConfig, PortfolioBacktestResult
from qrp_atlas.backtest.research.residual import (
    ResidualResearchError,
    ResidualStrategyBacktestRun,
    run_market_residual_mean_reversion_backtest,
)
from qrp_atlas.contracts import TRADE_DATE
from qrp_atlas.indicators.cross_section.conventions import (
    CrossSectionFrameError,
    normalize_trade_date,
)
from qrp_atlas.strategies import get_strategy
from qrp_atlas.strategies.builtin.residual import STRATEGY_CODE, STRATEGY_VERSION
from qrp_atlas.strategies.validation import resolve_parameters

SELECTION_OBJECTIVES = ("net_total_return", "net_sharpe", "net_calmar")
DEFAULT_SELECTION_OBJECTIVE = "net_calmar"
DEFAULT_MAX_CANDIDATES = 256
DEFAULT_ROLLING_WINDOWS = (20, 60, 120)
TRADING_DAYS_PER_YEAR = 252
RESULT_SCHEMA_VERSION = "1.0.0"

class ResidualRobustnessError(ValueError):
    """Raised when residual robustness validation cannot proceed."""


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, pd.DataFrame):
        return value.to_dict(orient="list")
    return value


def _finite_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return float(number)


def annualized_net_return_from_growth(
    total_growth: float | None,
    observation_count: int,
    *,
    periods_per_year: int = TRADING_DAYS_PER_YEAR,
) -> float | None:
    """Geometric/CAGR annualization from realized equity growth.

    ``total_growth`` is final_equity / initial_cash (or chained OOS equity).
    Uses trading-day compounding:
    ``total_growth ** (periods_per_year / observation_count) - 1``.
    """

    if observation_count <= 0 or total_growth is None:
        return None
    try:
        growth = float(total_growth)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(growth) or growth < 0.0:
        return None
    # Total loss is a valid geometric outcome: 0 ** positive_power - 1 = -1.
    if growth == 0.0:
        return -1.0
    try:
        annualized = growth ** (float(periods_per_year) / float(observation_count)) - 1.0
    except (OverflowError, ValueError, ZeroDivisionError):
        return None
    if not math.isfinite(annualized):
        return None
    return float(annualized)


def declared_strategy_parameter_keys() -> frozenset[str]:
    """Return declared parameter names from the residual strategy definition."""

    strategy = get_strategy(STRATEGY_CODE, STRATEGY_VERSION)
    return frozenset(strategy.definition.parameter_schema.keys())


def normalize_trading_dates(trade_dates: Sequence[Any] | pd.Series | pd.Index) -> list[pd.Timestamp]:
    """Normalize unique trading dates preserving first-seen order."""

    if trade_dates is None:
        return []
    if isinstance(trade_dates, (pd.Series, pd.Index)):
        values = list(trade_dates.tolist())
    elif isinstance(trade_dates, (str, bytes)) or not isinstance(trade_dates, Sequence):
        values = [trade_dates]
    else:
        values = list(trade_dates)

    seen: set[pd.Timestamp] = set()
    result: list[pd.Timestamp] = []
    for value in values:
        try:
            date = normalize_trade_date(value)
        except CrossSectionFrameError as exc:
            raise ResidualRobustnessError(str(exc)) from exc
        if date in seen:
            continue
        seen.add(date)
        result.append(date)
    # Walk-forward lengths are defined on unique sorted trading days.
    return sorted(result)


def extract_trading_dates(asset_prices: pd.DataFrame) -> list[pd.Timestamp]:
    if not isinstance(asset_prices, pd.DataFrame):
        raise ResidualRobustnessError("asset_prices must be a pandas DataFrame")
    if TRADE_DATE not in asset_prices.columns:
        raise ResidualRobustnessError("asset_prices missing trade_date")
    return normalize_trading_dates(asset_prices[TRADE_DATE])


@dataclass(frozen=True)
class WalkForwardConfig:
    """Trading-day walk-forward split configuration."""

    train_size: int
    validation_size: int
    test_size: int
    step_size: int | None = None
    expanding_train: bool = False
    max_folds: int | None = None
    min_train_size: int | None = None

    def __post_init__(self) -> None:
        for name in ("train_size", "validation_size", "test_size"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ResidualRobustnessError(f"{name} must be a positive integer")
        step = self.step_size if self.step_size is not None else self.test_size
        if not isinstance(step, int) or isinstance(step, bool) or step < 1:
            raise ResidualRobustnessError("step_size must be a positive integer")
        if step < self.test_size:
            raise ResidualRobustnessError(
                "step_size must be >= test_size to avoid overlapping OOS windows"
            )
        object.__setattr__(self, "step_size", step)
        if self.max_folds is not None and (
            not isinstance(self.max_folds, int)
            or isinstance(self.max_folds, bool)
            or self.max_folds < 1
        ):
            raise ResidualRobustnessError("max_folds must be a positive integer when provided")
        if self.min_train_size is not None and (
            not isinstance(self.min_train_size, int)
            or isinstance(self.min_train_size, bool)
            or self.min_train_size < 1
        ):
            raise ResidualRobustnessError(
                "min_train_size must be a positive integer when provided"
            )
        if not isinstance(self.expanding_train, bool):
            raise ResidualRobustnessError("expanding_train must be a bool")

    def to_dict(self) -> dict[str, Any]:
        return {
            "train_size": self.train_size,
            "validation_size": self.validation_size,
            "test_size": self.test_size,
            "step_size": self.step_size,
            "expanding_train": self.expanding_train,
            "max_folds": self.max_folds,
            "min_train_size": self.min_train_size,
        }


@dataclass(frozen=True)
class WalkForwardSplit:
    fold_id: str
    fold_index: int
    train_start: str
    train_end: str
    validation_start: str
    validation_end: str
    test_start: str
    test_end: str
    train_size: int
    validation_size: int
    test_size: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ParameterCandidate:
    candidate_id: str
    parameters: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "parameters": dict(self.parameters),
        }


@dataclass(frozen=True)
class CostStressScenario:
    code: str
    commission_multiplier: float = 1.0
    stamp_tax_multiplier: float = 1.0
    slippage_multiplier: float = 1.0
    minimum_commission_multiplier: float = 1.0
    commission_rate: float | None = None
    stamp_tax_rate: float | None = None
    slippage_bps: float | None = None
    minimum_commission: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not self.code.strip():
            raise ResidualRobustnessError("cost scenario code must be a non-empty string")
        for name in (
            "commission_multiplier",
            "stamp_tax_multiplier",
            "slippage_multiplier",
            "minimum_commission_multiplier",
        ):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ResidualRobustnessError(f"{name} must be numeric")
            if float(value) < 0:
                raise ResidualRobustnessError(f"{name} must be >= 0")
        for name in ("commission_rate", "stamp_tax_rate", "slippage_bps", "minimum_commission"):
            value = getattr(self, name)
            if value is None:
                continue
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ResidualRobustnessError(f"{name} must be numeric when provided")
            if float(value) < 0:
                raise ResidualRobustnessError(f"{name} must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "commission_multiplier": float(self.commission_multiplier),
            "stamp_tax_multiplier": float(self.stamp_tax_multiplier),
            "slippage_multiplier": float(self.slippage_multiplier),
            "minimum_commission_multiplier": float(self.minimum_commission_multiplier),
            "commission_rate": self.commission_rate,
            "stamp_tax_rate": self.stamp_tax_rate,
            "slippage_bps": self.slippage_bps,
            "minimum_commission": self.minimum_commission,
        }


DEFAULT_COST_SCENARIOS: tuple[CostStressScenario, ...] = (
    CostStressScenario(code="baseline"),
    CostStressScenario(
        code="cost_1_5x",
        commission_multiplier=1.5,
        stamp_tax_multiplier=1.5,
        slippage_multiplier=1.5,
        minimum_commission_multiplier=1.5,
    ),
    CostStressScenario(
        code="cost_2x",
        commission_multiplier=2.0,
        stamp_tax_multiplier=2.0,
        slippage_multiplier=2.0,
        minimum_commission_multiplier=2.0,
    ),
)


@dataclass(frozen=True)
class ResidualRobustnessResult:
    """Structured walk-forward robustness package."""

    splits: tuple[WalkForwardSplit, ...]
    candidates: tuple[ParameterCandidate, ...]
    train_metrics: tuple[dict[str, Any], ...]
    validation_metrics: tuple[dict[str, Any], ...]
    selected_parameters: tuple[dict[str, Any], ...]
    fold_test_metrics: tuple[dict[str, Any], ...]
    oos_equity: pd.DataFrame
    oos_summary: Mapping[str, Any]
    cost_stress: tuple[dict[str, Any], ...]
    parameter_sensitivity: tuple[dict[str, Any], ...]
    rolling_performance: tuple[dict[str, Any], ...]
    diagnostics: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)
    selected_test_runs: Mapping[str, ResidualStrategyBacktestRun] = field(
        default_factory=dict
    )
    cost_stress_runs: Mapping[str, ResidualStrategyBacktestRun] = field(
        default_factory=dict
    )

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(
            {
                "splits": [split.to_dict() for split in self.splits],
                "candidates": [candidate.to_dict() for candidate in self.candidates],
                "train_metrics": list(self.train_metrics),
                "validation_metrics": list(self.validation_metrics),
                "selected_parameters": list(self.selected_parameters),
                "fold_test_metrics": list(self.fold_test_metrics),
                "oos_equity": self.oos_equity.to_dict(orient="list"),
                "oos_summary": dict(self.oos_summary),
                "cost_stress": list(self.cost_stress),
                "parameter_sensitivity": list(self.parameter_sensitivity),
                "rolling_performance": list(self.rolling_performance),
                "diagnostics": list(self.diagnostics),
                "metadata": dict(self.metadata),
                "selected_test_run_fold_ids": sorted(self.selected_test_runs.keys()),
                "cost_stress_run_keys": sorted(self.cost_stress_runs.keys()),
            }
        )


def build_walk_forward_splits(
    trade_dates: Sequence[Any],
    config: WalkForwardConfig,
) -> tuple[tuple[WalkForwardSplit, ...], tuple[str, ...]]:
    """Build non-overlapping train/validation/test folds on trading days."""

    if not isinstance(config, WalkForwardConfig):
        raise ResidualRobustnessError("config must be a WalkForwardConfig")
    dates = normalize_trading_dates(trade_dates)
    diagnostics: list[str] = []
    if not dates:
        return (), ("NO_TRADING_DATES",)

    train_size = config.train_size
    val_size = config.validation_size
    test_size = config.test_size
    step = config.step_size if config.step_size is not None else test_size
    min_train = config.min_train_size or train_size
    if min_train > train_size and not config.expanding_train:
        diagnostics.append("MIN_TRAIN_SIZE_EXCEEDS_TRAIN_SIZE")

    splits: list[WalkForwardSplit] = []
    fold_index = 0
    # rolling: train starts at 0, step, 2*step, ...
    # expanding: train always starts at 0, end expands.
    offset = 0
    while True:
        if config.expanding_train:
            train_start_idx = 0
            train_end_idx = train_size + offset - 1
            train_len = train_end_idx - train_start_idx + 1
        else:
            train_start_idx = offset
            train_end_idx = offset + train_size - 1
            train_len = train_size

        if train_len < min_train:
            diagnostics.append(f"FOLD_SKIPPED_MIN_TRAIN|offset={offset}")
            break

        val_start_idx = train_end_idx + 1
        val_end_idx = val_start_idx + val_size - 1
        test_start_idx = val_end_idx + 1
        test_end_idx = test_start_idx + test_size - 1

        if test_end_idx >= len(dates):
            remaining = len(dates) - test_start_idx
            if remaining > 0:
                diagnostics.append(
                    f"TAIL_DISCARDED|remaining_test_days={remaining}|required={test_size}"
                )
            break

        split = WalkForwardSplit(
            fold_id=f"fold_{fold_index:03d}",
            fold_index=fold_index,
            train_start=dates[train_start_idx].strftime("%Y-%m-%d"),
            train_end=dates[train_end_idx].strftime("%Y-%m-%d"),
            validation_start=dates[val_start_idx].strftime("%Y-%m-%d"),
            validation_end=dates[val_end_idx].strftime("%Y-%m-%d"),
            test_start=dates[test_start_idx].strftime("%Y-%m-%d"),
            test_end=dates[test_end_idx].strftime("%Y-%m-%d"),
            train_size=int(train_len),
            validation_size=int(val_size),
            test_size=int(test_size),
        )
        splits.append(split)
        fold_index += 1
        if config.max_folds is not None and fold_index >= config.max_folds:
            break
        offset += step

    if not splits:
        diagnostics.append("NO_COMPLETE_FOLDS")
    return tuple(splits), tuple(diagnostics)


def _canonical_parameter_id(parameters: Mapping[str, Any]) -> str:
    payload = {key: parameters[key] for key in sorted(parameters)}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def build_parameter_candidates(
    parameter_grid: Mapping[str, Sequence[Any]] | None,
    *,
    base_parameters: Mapping[str, Any] | None = None,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
) -> tuple[ParameterCandidate, ...]:
    """Expand an explicit strategy parameter grid into deterministic candidates."""

    if max_candidates < 1:
        raise ResidualRobustnessError("max_candidates must be >= 1")
    strategy = get_strategy(STRATEGY_CODE, STRATEGY_VERSION)
    base = resolve_parameters(strategy.definition, dict(base_parameters or {}))
    allowed_keys = frozenset(strategy.definition.parameter_schema.keys())

    grid = {} if parameter_grid is None else dict(parameter_grid)
    # Do not mutate caller structures.
    for key in grid:
        if key not in allowed_keys:
            raise ResidualRobustnessError(
                f"parameter_grid key {key!r} is not a declared strategy parameter"
            )
        values = grid[key]
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            raise ResidualRobustnessError(
                f"parameter_grid[{key!r}] must be a non-string sequence"
            )
        if len(values) == 0:
            raise ResidualRobustnessError(f"parameter_grid[{key!r}] must be non-empty")

    if not grid:
        params = dict(sorted(base.items()))
        return (ParameterCandidate(candidate_id=_canonical_parameter_id(params), parameters=params),)

    keys = sorted(grid.keys())
    value_lists = [list(grid[key]) for key in keys]
    combos = list(itertools.product(*value_lists))
    if len(combos) > max_candidates:
        raise ResidualRobustnessError(
            f"parameter_grid expands to {len(combos)} candidates; max allowed is {max_candidates}"
        )

    candidates: list[ParameterCandidate] = []
    seen: set[str] = set()
    for combo in combos:
        params = dict(base)
        for key, value in zip(keys, combo, strict=True):
            params[key] = value
        # Validate against strategy schema / relationships.
        resolved = resolve_parameters(strategy.definition, params)
        validate = getattr(strategy, "_validate_relationships", None)
        if callable(validate):
            validate(resolved)
        ordered = {key: resolved[key] for key in sorted(resolved)}
        candidate_id = _canonical_parameter_id(ordered)
        if candidate_id in seen:
            continue
        seen.add(candidate_id)
        candidates.append(ParameterCandidate(candidate_id=candidate_id, parameters=ordered))

    candidates.sort(key=lambda item: item.candidate_id)
    return tuple(candidates)


def compute_portfolio_performance_metrics(
    result: PortfolioBacktestResult,
) -> dict[str, Any]:
    """Unified net / cost / gross-before-recorded-costs metrics from a portfolio result.

    ``gross_pnl_before_recorded_costs`` re-adds recorded costs to the realized
    net path. It is not a separately simulated zero-cost portfolio.
    """

    if not isinstance(result, PortfolioBacktestResult):
        raise ResidualRobustnessError("result must be a PortfolioBacktestResult")

    summary = dict(result.summary)
    initial_cash = float(summary.get("initial_cash", result.config.initial_cash))
    final_equity = float(summary.get("final_equity", initial_cash))
    commission = float(summary.get("commission", 0.0))
    stamp_tax = float(summary.get("stamp_tax", 0.0))
    slippage_cost = float(summary.get("slippage_cost", 0.0))
    total_recorded_cost = float(
        summary.get("total_cost", commission + stamp_tax + slippage_cost)
    )
    net_pnl = final_equity - initial_cash
    net_total_return = net_pnl / initial_cash if initial_cash else None
    gross_pnl = net_pnl + total_recorded_cost
    gross_return = gross_pnl / initial_cash if initial_cash else None
    cost_drag = (
        total_recorded_cost / initial_cash if initial_cash else None
    )

    daily_returns = [
        float(snapshot.daily_return)
        for snapshot in result.snapshots
        if snapshot.daily_return is not None
        and math.isfinite(float(snapshot.daily_return))
    ]
    n = len(daily_returns)
    total_growth = (
        final_equity / initial_cash
        if initial_cash and math.isfinite(final_equity / initial_cash)
        else None
    )
    annualized_net = annualized_net_return_from_growth(total_growth, n)
    if n >= 2:
        mean_r = sum(daily_returns) / n
        var = sum((value - mean_r) ** 2 for value in daily_returns) / (n - 1)
        vol = math.sqrt(var)
        annualized_vol = vol * math.sqrt(TRADING_DAYS_PER_YEAR)
        if vol > 0:
            net_sharpe = (mean_r / vol) * math.sqrt(TRADING_DAYS_PER_YEAR)
        else:
            net_sharpe = None
    elif n == 1:
        annualized_vol = 0.0
        net_sharpe = None
    else:
        annualized_vol = None
        net_sharpe = None

    max_drawdown = float(summary.get("max_drawdown", 0.0))
    abs_dd = abs(max_drawdown)
    if annualized_net is None or abs_dd == 0.0:
        net_calmar = None
    else:
        net_calmar = annualized_net / abs_dd

    return _json_safe(
        {
            "initial_cash": initial_cash,
            "final_equity": final_equity,
            "net_pnl": net_pnl,
            "net_total_return": net_total_return,
            "net_total_return_pct": (
                None if net_total_return is None else net_total_return * 100.0
            ),
            "annualized_net_return": annualized_net,
            "annualized_volatility": annualized_vol,
            "net_sharpe": net_sharpe,
            "net_calmar": net_calmar,
            "max_drawdown": max_drawdown,
            "max_drawdown_pct": float(summary.get("max_drawdown_pct", max_drawdown * 100.0)),
            "turnover": float(summary.get("turnover", 0.0)),
            "trade_count": int(summary.get("trade_count", 0)),
            "order_count": int(summary.get("order_count", 0)),
            "fill_count": int(summary.get("fill_count", 0)),
            "skipped_count": int(summary.get("skipped_count", 0)),
            "commission": commission,
            "stamp_tax": stamp_tax,
            "slippage_cost": slippage_cost,
            "total_recorded_cost": total_recorded_cost,
            "gross_pnl_before_recorded_costs": gross_pnl,
            "gross_return_before_recorded_costs": gross_return,
            "cost_drag": cost_drag,
            "observation_count": n,
            "gross_definition": (
                "final_equity - initial_cash + total_recorded_cost on the same "
                "realized fill path; not a re-simulated zero-cost portfolio"
            ),
        }
    )


def _selection_score(metrics: Mapping[str, Any], objective: str) -> float | None:
    if objective not in SELECTION_OBJECTIVES:
        raise ResidualRobustnessError(
            f"unsupported selection_objective: {objective!r}; "
            f"expected one of {SELECTION_OBJECTIVES}"
        )
    return _finite_or_none(metrics.get(objective))


def _clip_prices(
    prices: pd.DataFrame,
    *,
    max_date: str | pd.Timestamp,
) -> pd.DataFrame:
    end = normalize_trade_date(max_date)
    dates = pd.to_datetime(prices[TRADE_DATE])
    # preserve local date labels already present; compare via normalize
    mask = dates.map(lambda value: normalize_trade_date(value) <= end)
    return prices.loc[mask].copy()


def _apply_cost_scenario(
    config: PortfolioBacktestConfig,
    scenario: CostStressScenario,
) -> PortfolioBacktestConfig:
    base_cost = config.cost
    commission_rate = (
        float(scenario.commission_rate)
        if scenario.commission_rate is not None
        else float(base_cost.commission_rate) * float(scenario.commission_multiplier)
    )
    stamp_tax_rate = (
        float(scenario.stamp_tax_rate)
        if scenario.stamp_tax_rate is not None
        else float(base_cost.stamp_tax_rate) * float(scenario.stamp_tax_multiplier)
    )
    slippage_bps = (
        float(scenario.slippage_bps)
        if scenario.slippage_bps is not None
        else float(base_cost.slippage_bps) * float(scenario.slippage_multiplier)
    )
    minimum_commission = (
        float(scenario.minimum_commission)
        if scenario.minimum_commission is not None
        else float(config.execution.minimum_commission)
        * float(scenario.minimum_commission_multiplier)
    )
    new_cost = CostRule(
        commission_rate=commission_rate,
        stamp_tax_rate=stamp_tax_rate,
        slippage_bps=slippage_bps,
    )
    new_execution = replace(config.execution, minimum_commission=minimum_commission)
    return replace(config, cost=new_cost, execution=new_execution)


def _run_segment(
    *,
    asset_prices: pd.DataFrame,
    benchmark_prices: pd.DataFrame,
    portfolio_config: PortfolioBacktestConfig,
    benchmark_id: str | None,
    parameters: Mapping[str, Any],
    segment_start: str,
    segment_end: str,
) -> ResidualStrategyBacktestRun:
    clipped_assets = _clip_prices(asset_prices, max_date=segment_end)
    clipped_bench = _clip_prices(benchmark_prices, max_date=segment_end)
    # Fresh cash / empty positions for each segment are enforced by the runner.
    return run_market_residual_mean_reversion_backtest(
        clipped_assets,
        clipped_bench,
        portfolio_config,
        benchmark_id=benchmark_id,
        parameters=dict(parameters),
        start_date=segment_start,
        end_date=segment_end,
        entry_timing="next_open",
    )


def _candidate_qualifies(
    metrics: Mapping[str, Any],
    *,
    minimum_trades: int,
    objective: str,
) -> bool:
    if int(metrics.get("trade_count") or 0) < int(minimum_trades):
        return False
    if metrics.get("observation_count", 0) < 1:
        return False
    if _selection_score(metrics, objective) is None:
        return False
    return True


def _rank_key(
    row: Mapping[str, Any],
    *,
    objective: str,
) -> tuple[Any, ...]:
    score = _selection_score(row["metrics"], objective)
    # Missing scores sort last.
    score_key = float("-inf") if score is None else float(score)
    dd = _finite_or_none(row["metrics"].get("max_drawdown"))
    abs_dd = abs(dd) if dd is not None else float("inf")
    trades = int(row["metrics"].get("trade_count") or 0)
    return (-score_key, abs_dd, -trades, str(row["candidate_id"]))


def stitch_oos_equity(
    fold_results: Sequence[tuple[WalkForwardSplit, ResidualStrategyBacktestRun]],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Chain test-window daily returns into one OOS equity curve starting at 1.0."""

    rows: list[dict[str, Any]] = []
    equity = 1.0
    peak = 1.0
    seen_dates: set[str] = set()
    for split, run in fold_results:
        for snapshot in run.portfolio_result.snapshots:
            date = str(snapshot.trade_date)
            if date < split.test_start or date > split.test_end:
                continue
            if date in seen_dates:
                raise ResidualRobustnessError(f"overlapping OOS trade_date: {date}")
            seen_dates.add(date)
            daily_return = float(snapshot.daily_return)
            equity = equity * (1.0 + daily_return)
            peak = max(peak, equity)
            drawdown = equity / peak - 1.0 if peak else 0.0
            rows.append(
                {
                    "trade_date": date,
                    "fold_id": split.fold_id,
                    "daily_return": daily_return,
                    "oos_equity": equity,
                    "drawdown": drawdown,
                }
            )

    frame = pd.DataFrame(
        rows,
        columns=["trade_date", "fold_id", "daily_return", "oos_equity", "drawdown"],
    )
    if frame.empty:
        summary = {
            "observation_count": 0,
            "net_total_return": None,
            "annualized_net_return": None,
            "annualized_volatility": None,
            "net_sharpe": None,
            "net_calmar": None,
            "max_drawdown": None,
            "start_date": None,
            "end_date": None,
        }
        return frame, _json_safe(summary)

    daily = [float(value) for value in frame["daily_return"].tolist()]
    n = len(daily)
    mean_r = sum(daily) / n if n else 0.0
    if n >= 2:
        var = sum((value - mean_r) ** 2 for value in daily) / (n - 1)
        vol = math.sqrt(var)
        annualized_vol = vol * math.sqrt(TRADING_DAYS_PER_YEAR)
        net_sharpe = (
            (mean_r / vol) * math.sqrt(TRADING_DAYS_PER_YEAR) if vol > 0 else None
        )
    else:
        annualized_vol = 0.0
        net_sharpe = None
    final_growth = float(frame["oos_equity"].iloc[-1])
    annualized_net = annualized_net_return_from_growth(final_growth, n)
    max_dd = float(frame["drawdown"].min())
    abs_dd = abs(max_dd)
    net_calmar = (
        annualized_net / abs_dd
        if annualized_net is not None and abs_dd > 0
        else None
    )
    net_total_return = final_growth - 1.0
    summary = {
        "observation_count": n,
        "net_total_return": net_total_return,
        "annualized_net_return": annualized_net,
        "annualized_volatility": annualized_vol,
        "net_sharpe": net_sharpe,
        "net_calmar": net_calmar,
        "max_drawdown": max_dd,
        "start_date": frame["trade_date"].iloc[0],
        "end_date": frame["trade_date"].iloc[-1],
    }
    return frame, _json_safe(summary)


def compute_rolling_performance(
    oos_equity: pd.DataFrame,
    windows: Sequence[int] = DEFAULT_ROLLING_WINDOWS,
) -> tuple[dict[str, Any], ...]:
    if oos_equity is None or oos_equity.empty:
        return ()
    required = {"trade_date", "daily_return", "oos_equity"}
    missing = required - set(oos_equity.columns)
    if missing:
        raise ResidualRobustnessError(f"oos_equity missing columns: {sorted(missing)}")
    frame = oos_equity.sort_values("trade_date", kind="mergesort").reset_index(drop=True)
    daily = [float(value) for value in frame["daily_return"].tolist()]
    dates = [str(value) for value in frame["trade_date"].tolist()]
    rows: list[dict[str, Any]] = []
    for window in windows:
        if not isinstance(window, int) or isinstance(window, bool) or window < 1:
            raise ResidualRobustnessError("rolling windows must be positive integers")
        if len(daily) < window:
            continue
        for end_idx in range(window - 1, len(daily)):
            start_idx = end_idx - window + 1
            segment = daily[start_idx : end_idx + 1]
            growth = 1.0
            peak = 1.0
            max_dd = 0.0
            for ret in segment:
                growth *= 1.0 + ret
                peak = max(peak, growth)
                dd = growth / peak - 1.0
                max_dd = min(max_dd, dd)
            mean_r = sum(segment) / window
            if window >= 2:
                var = sum((value - mean_r) ** 2 for value in segment) / (window - 1)
                vol = math.sqrt(var)
                annualized_vol = vol * math.sqrt(TRADING_DAYS_PER_YEAR)
                net_sharpe = (
                    (mean_r / vol) * math.sqrt(TRADING_DAYS_PER_YEAR) if vol > 0 else None
                )
            else:
                annualized_vol = 0.0
                net_sharpe = None
            rows.append(
                _json_safe(
                    {
                        "window": window,
                        "window_start": dates[start_idx],
                        "window_end": dates[end_idx],
                        "observation_count": window,
                        "net_return": growth - 1.0,
                        "annualized_volatility": annualized_vol,
                        "net_sharpe": net_sharpe,
                        "max_drawdown": max_dd,
                    }
                )
            )
    return tuple(rows)


def _aggregate_cost_stress(
    scenario_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    by_code: dict[str, list[Mapping[str, Any]]] = {}
    for row in scenario_rows:
        by_code.setdefault(str(row["scenario_code"]), []).append(row)
    summaries: list[dict[str, Any]] = []
    for code in sorted(by_code):
        rows = by_code[code]
        returns = [
            float(item["metrics"]["net_total_return"])
            for item in rows
            if _finite_or_none(item["metrics"].get("net_total_return")) is not None
        ]
        grosses = [
            float(item["metrics"]["gross_return_before_recorded_costs"])
            for item in rows
            if _finite_or_none(
                item["metrics"].get("gross_return_before_recorded_costs")
            )
            is not None
        ]
        costs = [
            float(item["metrics"]["total_recorded_cost"])
            for item in rows
            if _finite_or_none(item["metrics"].get("total_recorded_cost")) is not None
        ]
        drags = [
            float(item["metrics"]["cost_drag"])
            for item in rows
            if _finite_or_none(item["metrics"].get("cost_drag")) is not None
        ]
        summaries.append(
            _json_safe(
                {
                    "scenario_code": code,
                    "fold_count": len(rows),
                    "aggregate_net_return": (
                        None
                        if not returns
                        else math.prod(1.0 + value for value in returns) - 1.0
                    ),
                    "aggregate_gross_before_recorded_costs": (
                        None
                        if not grosses
                        else math.prod(1.0 + value for value in grosses) - 1.0
                    ),
                    "aggregate_recorded_cost": sum(costs) if costs else 0.0,
                    "aggregate_cost_drag": sum(drags) if drags else 0.0,
                    "worst_fold_return": min(returns) if returns else None,
                    "mean_fold_return": (
                        sum(returns) / len(returns) if returns else None
                    ),
                    "fold_metrics": list(rows),
                }
            )
        )
    return tuple(summaries)


def _parameter_sensitivity(
    validation_rows: Sequence[Mapping[str, Any]],
    selected_rows: Sequence[Mapping[str, Any]],
    *,
    objective: str,
) -> tuple[dict[str, Any], ...]:
    by_candidate: dict[str, list[Mapping[str, Any]]] = {}
    for row in validation_rows:
        by_candidate.setdefault(str(row["candidate_id"]), []).append(row)
    selection_counts: dict[str, int] = {}
    for row in selected_rows:
        if row.get("status") != "selected":
            continue
        cid = str(row["candidate_id"])
        selection_counts[cid] = selection_counts.get(cid, 0) + 1

    results: list[dict[str, Any]] = []
    for candidate_id in sorted(by_candidate):
        rows = by_candidate[candidate_id]
        valid_rows = [row for row in rows if row.get("qualified")]
        invalid_rows = [row for row in rows if not row.get("qualified")]
        objectives = [
            _selection_score(row["metrics"], objective)
            for row in valid_rows
            if _selection_score(row["metrics"], objective) is not None
        ]
        returns = [
            _finite_or_none(row["metrics"].get("net_total_return"))
            for row in valid_rows
        ]
        returns = [value for value in returns if value is not None]
        drawdowns = [
            _finite_or_none(row["metrics"].get("max_drawdown")) for row in valid_rows
        ]
        drawdowns = [value for value in drawdowns if value is not None]
        trades = [int(row["metrics"].get("trade_count") or 0) for row in valid_rows]
        params = dict(rows[0]["parameters"])
        results.append(
            _json_safe(
                {
                    "candidate_id": candidate_id,
                    "parameters": params,
                    "valid_fold_count": len(valid_rows),
                    "invalid_fold_count": len(invalid_rows),
                    "selection_count": int(selection_counts.get(candidate_id, 0)),
                    "mean_validation_objective": (
                        sum(objectives) / len(objectives) if objectives else None
                    ),
                    "median_validation_objective": (
                        sorted(objectives)[len(objectives) // 2] if objectives else None
                    ),
                    "worst_validation_objective": min(objectives) if objectives else None,
                    "mean_validation_return": (
                        sum(returns) / len(returns) if returns else None
                    ),
                    "mean_validation_drawdown": (
                        sum(drawdowns) / len(drawdowns) if drawdowns else None
                    ),
                    "mean_validation_trade_count": (
                        sum(trades) / len(trades) if trades else None
                    ),
                    "fold_metrics": [
                        {
                            "fold_id": row["fold_id"],
                            "qualified": row.get("qualified"),
                            "metrics": row["metrics"],
                        }
                        for row in rows
                    ],
                }
            )
        )
    results.sort(
        key=lambda item: (
            -(item["selection_count"] or 0),
            float("-inf")
            if item["mean_validation_objective"] is None
            else -float(item["mean_validation_objective"]),
            item["candidate_id"],
        )
    )
    return tuple(results)


def run_residual_robustness_study(
    asset_prices: pd.DataFrame,
    benchmark_prices: pd.DataFrame,
    portfolio_config: PortfolioBacktestConfig,
    *,
    benchmark_id: str | None = None,
    base_parameters: Mapping[str, Any] | None = None,
    parameter_grid: Mapping[str, Sequence[Any]] | None = None,
    walk_forward_config: WalkForwardConfig,
    selection_objective: str = DEFAULT_SELECTION_OBJECTIVE,
    minimum_validation_trades: int = 1,
    minimum_train_trades: int = 0,
    cost_scenarios: Sequence[CostStressScenario] | None = None,
    rolling_windows: Sequence[int] = DEFAULT_ROLLING_WINDOWS,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
) -> ResidualRobustnessResult:
    """Run residual walk-forward selection, OOS tests, and cost stress.

    Parameter selection uses validation metrics only. Test windows never enter
    selection. Cost stress re-executes the selected parameters without reselecting.
    """

    if not isinstance(portfolio_config, PortfolioBacktestConfig):
        raise ResidualRobustnessError("portfolio_config must be a PortfolioBacktestConfig")
    if selection_objective not in SELECTION_OBJECTIVES:
        raise ResidualRobustnessError(
            f"unsupported selection_objective: {selection_objective!r}"
        )
    if minimum_validation_trades < 0 or minimum_train_trades < 0:
        raise ResidualRobustnessError("minimum trade constraints must be >= 0")

    # Defensive copies so callers cannot observe mutation.
    assets = asset_prices.copy()
    benchmark = benchmark_prices.copy()
    base_config = portfolio_config

    trade_dates = extract_trading_dates(assets)
    splits, split_diagnostics = build_walk_forward_splits(trade_dates, walk_forward_config)
    candidates = build_parameter_candidates(
        parameter_grid,
        base_parameters=base_parameters,
        max_candidates=max_candidates,
    )
    scenarios = tuple(cost_scenarios) if cost_scenarios is not None else DEFAULT_COST_SCENARIOS
    if not scenarios:
        raise ResidualRobustnessError("cost_scenarios must be non-empty")
    scenario_codes = [scenario.code for scenario in scenarios]
    if len(scenario_codes) != len(set(scenario_codes)):
        raise ResidualRobustnessError("cost scenario codes must be unique")

    diagnostics: list[str] = list(split_diagnostics)
    train_metrics: list[dict[str, Any]] = []
    validation_metrics: list[dict[str, Any]] = []
    selected_parameters: list[dict[str, Any]] = []
    fold_test_metrics: list[dict[str, Any]] = []
    selected_test_runs: dict[str, ResidualStrategyBacktestRun] = {}
    cost_stress_runs: dict[str, ResidualStrategyBacktestRun] = {}
    cost_stress_rows: list[dict[str, Any]] = []
    successful_oos: list[tuple[WalkForwardSplit, ResidualStrategyBacktestRun]] = []

    for split in splits:
        fold_train_rows: list[dict[str, Any]] = []
        fold_val_rows: list[dict[str, Any]] = []
        for candidate in candidates:
            # Train diagnostics.
            train_status = "ok"
            train_error = None
            train_metric_payload: dict[str, Any] | None = None
            try:
                train_run = _run_segment(
                    asset_prices=assets,
                    benchmark_prices=benchmark,
                    portfolio_config=base_config,
                    benchmark_id=benchmark_id,
                    parameters=candidate.parameters,
                    segment_start=split.train_start,
                    segment_end=split.train_end,
                )
                train_metric_payload = compute_portfolio_performance_metrics(
                    train_run.portfolio_result
                )
                if int(train_metric_payload.get("trade_count") or 0) < minimum_train_trades:
                    train_status = "failed_min_trades"
            except (ResidualResearchError, ResidualRobustnessError, Exception) as exc:  # noqa: BLE001
                train_status = "failed"
                train_error = str(exc)
            train_row = {
                "fold_id": split.fold_id,
                "candidate_id": candidate.candidate_id,
                "parameters": dict(candidate.parameters),
                "status": train_status,
                "error": train_error,
                "metrics": train_metric_payload or {},
                "segment": "train",
            }
            fold_train_rows.append(train_row)
            train_metrics.append(train_row)

            # Validation selection metrics (independent of train failure unless we want eligibility).
            val_status = "ok"
            val_error = None
            val_metrics: dict[str, Any] | None = None
            qualified = False
            try:
                val_run = _run_segment(
                    asset_prices=assets,
                    benchmark_prices=benchmark,
                    portfolio_config=base_config,
                    benchmark_id=benchmark_id,
                    parameters=candidate.parameters,
                    segment_start=split.validation_start,
                    segment_end=split.validation_end,
                )
                val_metrics = compute_portfolio_performance_metrics(val_run.portfolio_result)
                qualified = _candidate_qualifies(
                    val_metrics,
                    minimum_trades=minimum_validation_trades,
                    objective=selection_objective,
                )
                # Train must succeed basic validity before a candidate is selectable.
                if train_status != "ok":
                    qualified = False
            except (ResidualResearchError, ResidualRobustnessError, Exception) as exc:  # noqa: BLE001
                val_status = "failed"
                val_error = str(exc)
                qualified = False
            val_row = {
                "fold_id": split.fold_id,
                "candidate_id": candidate.candidate_id,
                "parameters": dict(candidate.parameters),
                "status": val_status,
                "error": val_error,
                "metrics": val_metrics or {},
                "qualified": qualified,
                "segment": "validation",
                "selection_objective": selection_objective,
                "objective_value": (
                    _selection_score(val_metrics or {}, selection_objective)
                    if val_metrics
                    else None
                ),
            }
            fold_val_rows.append(val_row)
            validation_metrics.append(val_row)

        eligible = [row for row in fold_val_rows if row.get("qualified")]
        if not eligible:
            selected_parameters.append(
                {
                    "fold_id": split.fold_id,
                    "status": "selection_failed",
                    "candidate_id": None,
                    "parameters": None,
                    "reason": "no_qualified_candidates",
                    "selection_objective": selection_objective,
                }
            )
            diagnostics.append(f"{split.fold_id}|selection_failed")
            fold_test_metrics.append(
                {
                    "fold_id": split.fold_id,
                    "status": "skipped_selection_failed",
                    "metrics": {},
                    "candidate_id": None,
                    "parameters": None,
                }
            )
            continue

        eligible_sorted = sorted(
            eligible,
            key=lambda row: _rank_key(row, objective=selection_objective),
        )
        winner = eligible_sorted[0]
        selected_parameters.append(
            {
                "fold_id": split.fold_id,
                "status": "selected",
                "candidate_id": winner["candidate_id"],
                "parameters": dict(winner["parameters"]),
                "validation_objective_value": winner.get("objective_value"),
                "selection_objective": selection_objective,
                "tie_break": [
                    "objective_desc",
                    "abs_max_drawdown_asc",
                    "trade_count_desc",
                    "candidate_id_asc",
                ],
            }
        )

        # Baseline test.
        try:
            test_run = _run_segment(
                asset_prices=assets,
                benchmark_prices=benchmark,
                portfolio_config=base_config,
                benchmark_id=benchmark_id,
                parameters=winner["parameters"],
                segment_start=split.test_start,
                segment_end=split.test_end,
            )
            test_metrics = compute_portfolio_performance_metrics(test_run.portfolio_result)
            fold_test_metrics.append(
                {
                    "fold_id": split.fold_id,
                    "status": "ok",
                    "candidate_id": winner["candidate_id"],
                    "parameters": dict(winner["parameters"]),
                    "metrics": test_metrics,
                    "segment": "test",
                }
            )
            selected_test_runs[split.fold_id] = test_run
            successful_oos.append((split, test_run))
        except (ResidualResearchError, ResidualRobustnessError, Exception) as exc:  # noqa: BLE001
            diagnostics.append(f"{split.fold_id}|test_failed|{exc}")
            fold_test_metrics.append(
                {
                    "fold_id": split.fold_id,
                    "status": "failed",
                    "candidate_id": winner["candidate_id"],
                    "parameters": dict(winner["parameters"]),
                    "metrics": {},
                    "error": str(exc),
                    "segment": "test",
                }
            )
            continue

        # Cost stress with fixed selected parameters.
        for scenario in scenarios:
            stressed_config = _apply_cost_scenario(base_config, scenario)
            try:
                stressed_run = _run_segment(
                    asset_prices=assets,
                    benchmark_prices=benchmark,
                    portfolio_config=stressed_config,
                    benchmark_id=benchmark_id,
                    parameters=winner["parameters"],
                    segment_start=split.test_start,
                    segment_end=split.test_end,
                )
                stressed_metrics = compute_portfolio_performance_metrics(
                    stressed_run.portfolio_result
                )
                key = f"{split.fold_id}:{scenario.code}"
                cost_stress_runs[key] = stressed_run
                cost_stress_rows.append(
                    {
                        "fold_id": split.fold_id,
                        "scenario_code": scenario.code,
                        "candidate_id": winner["candidate_id"],
                        "parameters": dict(winner["parameters"]),
                        "status": "ok",
                        "metrics": stressed_metrics,
                        "scenario": scenario.to_dict(),
                        "decision_count": len(stressed_run.strategy_result.decisions),
                        "signal_target_count": int(len(stressed_run.signal_target_weights)),
                    }
                )
            except (ResidualResearchError, ResidualRobustnessError, Exception) as exc:  # noqa: BLE001
                diagnostics.append(
                    f"{split.fold_id}|cost_stress_failed|{scenario.code}|{exc}"
                )
                cost_stress_rows.append(
                    {
                        "fold_id": split.fold_id,
                        "scenario_code": scenario.code,
                        "candidate_id": winner["candidate_id"],
                        "parameters": dict(winner["parameters"]),
                        "status": "failed",
                        "metrics": {},
                        "error": str(exc),
                        "scenario": scenario.to_dict(),
                    }
                )

    oos_equity, oos_summary = stitch_oos_equity(successful_oos)
    rolling = compute_rolling_performance(oos_equity, rolling_windows)
    sensitivity = _parameter_sensitivity(
        validation_metrics,
        selected_parameters,
        objective=selection_objective,
    )
    cost_summary = _aggregate_cost_stress(cost_stress_rows)

    input_start = trade_dates[0].strftime("%Y-%m-%d") if trade_dates else None
    input_end = trade_dates[-1].strftime("%Y-%m-%d") if trade_dates else None
    metadata = {
        "strategy_code": STRATEGY_CODE,
        "strategy_version": STRATEGY_VERSION,
        "benchmark_id": benchmark_id,
        "selection_objective": selection_objective,
        "selection_objective_definitions": {
            "net_total_return": "final_equity / initial_cash - 1",
            "net_sharpe": "mean(daily_return)/std(daily_return)*sqrt(252); rf=0",
            "net_calmar": "annualized_net_return / abs(max_drawdown)",
            "annualized_net_return": (
                "(final_equity / initial_cash) ** (252 / observation_count) - 1 "
                "from realized equity growth (geometric/CAGR), not (1+mean_daily)**252-1"
            ),
        },
        "walk_forward_config": walk_forward_config.to_dict(),
        "base_parameters": dict(base_parameters or {}),
        "parameter_grid": {
            key: list(values) for key, values in dict(parameter_grid or {}).items()
        },
        "candidate_count": len(candidates),
        "cost_scenarios": [scenario.to_dict() for scenario in scenarios],
        "rolling_windows": [int(value) for value in rolling_windows],
        "minimum_validation_trades": int(minimum_validation_trades),
        "minimum_train_trades": int(minimum_train_trades),
        "input_date_range": {"start": input_start, "end": input_end},
        "oos_date_range": {
            "start": oos_summary.get("start_date"),
            "end": oos_summary.get("end_date"),
        },
        "fold_count": len(splits),
        "successful_test_fold_count": len(successful_oos),
        "failed_or_skipped_test_fold_count": len(splits) - len(successful_oos),
        "result_schema_version": RESULT_SCHEMA_VERSION,
        "portfolio_config": asdict(base_config),
    }

    return ResidualRobustnessResult(
        splits=splits,
        candidates=candidates,
        train_metrics=tuple(_json_safe(row) for row in train_metrics),
        validation_metrics=tuple(_json_safe(row) for row in validation_metrics),
        selected_parameters=tuple(_json_safe(row) for row in selected_parameters),
        fold_test_metrics=tuple(_json_safe(row) for row in fold_test_metrics),
        oos_equity=oos_equity,
        oos_summary=oos_summary,
        cost_stress=cost_summary,
        parameter_sensitivity=sensitivity,
        rolling_performance=rolling,
        diagnostics=tuple(diagnostics),
        metadata=_json_safe(metadata),
        selected_test_runs=selected_test_runs,
        cost_stress_runs=cost_stress_runs,
    )


__all__ = [
    "DEFAULT_COST_SCENARIOS",
    "DEFAULT_MAX_CANDIDATES",
    "DEFAULT_ROLLING_WINDOWS",
    "DEFAULT_SELECTION_OBJECTIVE",
    "RESULT_SCHEMA_VERSION",
    "SELECTION_OBJECTIVES",
    "CostStressScenario",
    "ParameterCandidate",
    "ResidualRobustnessError",
    "ResidualRobustnessResult",
    "WalkForwardConfig",
    "WalkForwardSplit",
    "annualized_net_return_from_growth",
    "build_parameter_candidates",
    "build_walk_forward_splits",
    "compute_portfolio_performance_metrics",
    "compute_rolling_performance",
    "extract_trading_dates",
    "normalize_trading_dates",
    "run_residual_robustness_study",
    "stitch_oos_equity",
]
