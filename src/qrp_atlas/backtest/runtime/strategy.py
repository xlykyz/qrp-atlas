"""Adapter that prepares declared strategy inputs and executes dynamic decisions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd

from qrp_atlas.contracts import TICKER, TRADE_DATE
from qrp_atlas.indicators.stock.trend import (
    CLOSE_ABOVE_MA5,
    CLOSE_ABOVE_MA5_DAYS,
    CLOSE_BELOW_MA5,
    CLOSE_BELOW_MA5_DAYS,
    MA5,
    calculate_stock_trend,
)
from qrp_atlas.indicators.system_b.detector import (
    SYSTEM_B_EXIT_TRIGGERED,
    SYSTEM_B_TREND_VALID,
    calculate_system_b_basic_states,
)
from qrp_atlas.strategies import (
    StrategyAction,
    StrategyDefinition,
    StrategyInput,
    StrategyRunResult,
    get_strategy,
)

from ..broker import (
    REASON_INVALID_DIRECTION,
    REASON_NO_NEXT_BAR_FOR_ENTRY,
    AssetPriceIndex,
    build_price_index,
    simulate_open_position,
)
from ..metrics import summarize_trades
from ..models import BacktestConfig, BacktestResult, Skipped, Trade
from ..validators import validate_config, validate_price_df

_TREND_INDICATORS = frozenset(
    {MA5, CLOSE_ABOVE_MA5, CLOSE_BELOW_MA5, CLOSE_ABOVE_MA5_DAYS, CLOSE_BELOW_MA5_DAYS}
)
_SYSTEM_B_INDICATORS = frozenset({SYSTEM_B_TREND_VALID, SYSTEM_B_EXIT_TRIGGERED})


@dataclass(frozen=True)
class StrategyBacktestRun:
    """Optional detailed result containing both decisions and execution output."""

    strategy_result: StrategyRunResult
    backtest_result: BacktestResult


def prepare_strategy_data(price_df: pd.DataFrame, definition: StrategyDefinition) -> pd.DataFrame:
    """Prepare only declared raw fields plus registered indicator outputs.

    The adapter performs no database access.  Indicator dependency knowledge
    remains in the indicators layer, while strategies receive only the finished
    columns they declared.
    """

    validate_price_df(price_df)
    prepared = price_df.copy()
    if TICKER not in prepared.columns:
        prepared[TICKER] = prepared["asset_id"].astype(str)
    if TRADE_DATE not in prepared.columns:
        raise ValueError("price_df must include trade_date")
    prepared = prepared.sort_values([TICKER, TRADE_DATE], kind="mergesort").reset_index(drop=True)

    missing_fields = [field for field in definition.required_fields if field not in prepared.columns]
    if missing_fields:
        raise ValueError(f"price_df missing declared strategy fields: {missing_fields}")

    required_indicators = set(definition.required_indicators)
    if required_indicators & (_TREND_INDICATORS | _SYSTEM_B_INDICATORS):
        trend = calculate_stock_trend(prepared)
        for column in _TREND_INDICATORS & required_indicators:
            prepared[column] = trend[column].to_numpy()
    if required_indicators & _SYSTEM_B_INDICATORS:
        states = calculate_system_b_basic_states(trend)
        for column in _SYSTEM_B_INDICATORS & required_indicators:
            prepared[column] = states[column].to_numpy()

    unresolved = [column for column in definition.required_indicators if column not in prepared.columns]
    if unresolved:
        raise ValueError(f"no indicator preparation path for: {unresolved}")
    columns = list(dict.fromkeys((*definition.required_fields, *definition.required_indicators)))
    return prepared.loc[:, columns].copy()


class StrategyBacktestRuntime:
    """Run a registered strategy then execute its ENTER/HOLD/EXIT decisions."""

    def run(
        self,
        code: str,
        price_df: pd.DataFrame,
        config: BacktestConfig,
        *,
        parameters: Mapping[str, Any] | None = None,
        version: str | None = None,
        initial_positions: Mapping[str, bool] | None = None,
        runtime_context: Mapping[str, Any] | None = None,
    ) -> StrategyBacktestRun:
        """Return decision output and standard BacktestResult for a strategy run."""

        validate_config(config)
        strategy = get_strategy(code, version)
        prepared = prepare_strategy_data(price_df, strategy.definition)
        strategy_result = strategy.run(
            StrategyInput(
                prepared_data=prepared,
                parameters=parameters or {},
                initial_positions=initial_positions or {},
                runtime_context=runtime_context or {},
            )
        )
        backtest_result = self._execute_decisions(
            price_df, strategy_result, config, initial_positions or {}
        )
        return StrategyBacktestRun(strategy_result=strategy_result, backtest_result=backtest_result)

    @staticmethod
    def _entry_position(api: AssetPriceIndex, decision_date: str, timing: str) -> int | None:
        timestamp = pd.Timestamp(pd.to_datetime(decision_date))
        signal_pos = api.date_to_pos.get(timestamp)
        if signal_pos is None:
            return None
        if timing == "signal_close":
            return signal_pos
        next_pos = signal_pos + 1
        return next_pos if next_pos < len(api.df) else None

    def _execute_decisions(
        self,
        price_df: pd.DataFrame,
        strategy_result: StrategyRunResult,
        config: BacktestConfig,
        initial_positions: Mapping[str, bool],
    ) -> BacktestResult:
        price_index = build_price_index(price_df)
        open_positions: dict[str, int] = {}
        # Initial positions influence strategy decisions but lack a known entry
        # price/date, so they cannot become a synthetic Trade in this adapter.
        seeded_assets = {asset_id for asset_id, held in initial_positions.items() if held}
        trades: list[Trade] = []
        skipped: list[Skipped] = []

        for decision in strategy_result.decisions:
            asset_id = decision.asset_id
            api = price_index.get(asset_id)
            if api is None:
                skipped.append(Skipped(asset_id, decision.trade_date, "NO_PRICE_DATA", "strategy asset missing from price_df"))
                continue
            if decision.direction != "long":
                skipped.append(Skipped(asset_id, decision.trade_date, REASON_INVALID_DIRECTION, "only long execution is supported"))
                continue

            if decision.action is StrategyAction.ENTER:
                if asset_id in open_positions or asset_id in seeded_assets:
                    continue
                entry_pos = self._entry_position(api, decision.trade_date, config.entry.timing)
                if entry_pos is None:
                    skipped.append(Skipped(asset_id, decision.trade_date, REASON_NO_NEXT_BAR_FOR_ENTRY, "no entry bar for strategy decision"))
                    continue
                open_positions[asset_id] = entry_pos
            elif decision.action is StrategyAction.EXIT:
                entry_pos = open_positions.pop(asset_id, None)
                if entry_pos is None:
                    continue
                exit_timestamp = pd.Timestamp(pd.to_datetime(decision.trade_date))
                exit_pos = api.date_to_pos.get(exit_timestamp)
                if exit_pos is None:
                    skipped.append(Skipped(asset_id, decision.trade_date, "SIGNAL_DATE_NOT_FOUND", "exit decision date missing from price_df"))
                    continue
                simulated = simulate_open_position(
                    api,
                    signal_date=decision.trade_date,
                    entry_pos=entry_pos,
                    exit_pos=exit_pos,
                    config=config,
                    signal_name=strategy_result.definition.code,
                    meta={
                        "strategy_code": decision.strategy_code,
                        "strategy_version": decision.strategy_version,
                        "entry_reason": "strategy_enter",
                        "exit_reason": decision.reason_code,
                    },
                )
                if isinstance(simulated, Trade):
                    trades.append(simulated)
                else:
                    skipped.append(simulated)

        return BacktestResult(
            config=config,
            summary=summarize_trades(trades, skipped),
            trades=trades,
            skipped=skipped,
            equity_curve=[],
        )


def run_strategy_backtest(
    code: str,
    price_df: pd.DataFrame,
    config: BacktestConfig,
    *,
    parameters: Mapping[str, Any] | None = None,
    version: str | None = None,
) -> BacktestResult:
    """Convenience API returning the standard BacktestResult only."""

    return StrategyBacktestRuntime().run(
        code, price_df, config, parameters=parameters, version=version
    ).backtest_result
