"""Tests for cross-sectional long-only strategies and portfolio engine handoff."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import (
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioExecutionRule,
    strategy_decisions_to_target_weights,
    validate_target_weights,
)
from qrp_atlas.strategies import (
    StrategyAction,
    StrategyInput,
    StrategyValidationError,
    compute_composite_score,
    get_strategy,
    list_strategies,
    run_strategy,
)


def _prices(rows):
    raw = pd.DataFrame(rows)
    return raw.assign(asset_name="x", asset_type="stock")[
        [
            "trade_date",
            "asset_id",
            "asset_name",
            "asset_type",
            "open",
            "high",
            "low",
            "close",
        ]
    ]


def _momentum_frame() -> pd.DataFrame:
    rows = []
    for day, scores in {
        "2024-01-02": {"A": 0.1, "B": 0.3, "C": 0.2},
        "2024-01-03": {"A": 0.4, "B": 0.1, "C": 0.2},
        "2024-01-04": {"A": 0.2, "B": 0.5, "C": 0.1},
        "2024-01-05": {"A": 0.1, "B": 0.2, "C": 0.3},
        "2024-01-08": {"A": 0.5, "B": 0.4, "C": 0.1},
    }.items():
        for asset_id, score in scores.items():
            rows.append(
                {
                    "trade_date": day,
                    "asset_id": asset_id,
                    "ticker": asset_id,
                    "momentum": score,
                }
            )
    return pd.DataFrame(rows)


def test_strategies_are_registered() -> None:
    codes = [item.code for item in list_strategies()]
    assert "cross_sectional_momentum_long_only" in codes
    assert "multifactor_long_only" in codes
    assert get_strategy("cross_sectional_momentum_long_only").definition.version == "1.0.0"


def test_momentum_top_n_enter_hold_exit_and_execution_lag() -> None:
    frame = _momentum_frame()
    original = frame.copy(deep=True)
    result = run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=frame,
            parameters={
                "top_n": 1,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "cash_buffer": 0.0,
                "score_column": "momentum",
                "rebalance_frequency": "daily",
            },
            runtime_context={
                "trading_days": [
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                    "2024-01-08",
                ]
            },
        ),
    )
    pd.testing.assert_frame_equal(frame, original)

    by_date = {}
    for decision in result.decisions:
        by_date.setdefault(decision.trade_date, []).append(decision)

    # signal 01-02 -> execute 01-03, top is B
    day_03 = {item.asset_id: item for item in by_date["2024-01-03"]}
    assert day_03["B"].action is StrategyAction.ENTER
    assert day_03["B"].evidence["signal_date"] == "2024-01-02"
    assert day_03["B"].evidence["execution_trade_date"] == "2024-01-03"
    assert day_03["B"].weight == pytest.approx(1.0)

    # signal 01-03 -> execute 01-04, top becomes A; B exits
    day_04 = {item.asset_id: item for item in by_date["2024-01-04"]}
    assert day_04["A"].action is StrategyAction.ENTER
    assert day_04["B"].action is StrategyAction.EXIT
    assert day_04["B"].weight == pytest.approx(0.0)

    # signal 01-04 -> execute 01-05, top is B again
    day_05 = {item.asset_id: item for item in by_date["2024-01-05"]}
    assert day_05["B"].action is StrategyAction.ENTER
    assert day_05["A"].action is StrategyAction.EXIT

    # Re-running is deterministic.
    again = run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=frame.sample(frac=1, random_state=3),
            parameters={
                "top_n": 1,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "cash_buffer": 0.0,
                "score_column": "momentum",
                "rebalance_frequency": "daily",
            },
            runtime_context={
                "trading_days": [
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                    "2024-01-08",
                ]
            },
        ),
    )
    assert [item.to_dict() for item in again.decisions] == [
        item.to_dict() for item in result.decisions
    ]


def test_multifactor_complete_case_composite_and_selection() -> None:
    frame = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"] * 3 + ["2024-01-03"] * 3,
            "asset_id": ["A", "B", "C", "A", "B", "C"],
            "ticker": ["A", "B", "C", "A", "B", "C"],
            "momentum": [1.0, 0.5, 0.0, 0.2, 0.9, 0.1],
            "value": [0.0, 1.0, 0.5, 0.8, math.nan, 0.4],
        }
    )
    composite = compute_composite_score(
        frame,
        factor_columns=["momentum", "value"],
        factor_weights=[0.5, 0.5],
    )
    assert composite.tolist()[0] == pytest.approx(0.5)
    assert math.isnan(composite.tolist()[4])

    result = run_strategy(
        "multifactor_long_only",
        StrategyInput(
            prepared_data=frame,
            parameters={
                "top_n": 1,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "factor_columns_json": '["momentum","value"]',
                "factor_weights_json": "[0.5,0.5]",
                "rebalance_frequency": "daily",
            },
            runtime_context={"trading_days": ["2024-01-02", "2024-01-03", "2024-01-04"]},
        ),
    )
    # signal 01-02: A composite 0.5, B 0.75, C 0.25 -> B
    # execute 01-03
    first = [item for item in result.decisions if item.trade_date == "2024-01-03"]
    assert [item.asset_id for item in first if item.action is StrategyAction.ENTER] == [
        "B"
    ]
    # signal 01-03: B invalid complete-case, A 0.5, C 0.25 -> A
    second = [item for item in result.decisions if item.trade_date == "2024-01-04"]
    actions = {item.asset_id: item.action for item in second}
    assert actions["A"] is StrategyAction.ENTER
    assert actions["B"] is StrategyAction.EXIT
    assert first[0].evidence["composite_method"] == "complete_case_linear"


def test_eligibility_panel_excludes_assets() -> None:
    frame = _momentum_frame()
    eligibility = pd.DataFrame(
        {
            "trade_date": ["2024-01-02", "2024-01-02", "2024-01-02"],
            "asset_id": ["A", "B", "C"],
            "eligible": [True, False, True],
            "reason_code": ["OK", "ST", "OK"],
        }
    )
    result = run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=frame,
            parameters={
                "top_n": 1,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "rebalance_frequency": "explicit",
                "explicit_dates_json": '["2024-01-02"]',
            },
            runtime_context={
                "trading_days": ["2024-01-02", "2024-01-03"],
                "eligibility": eligibility,
            },
        ),
    )
    assert len(result.decisions) == 1
    assert result.decisions[0].asset_id == "C"
    assert result.decisions[0].trade_date == "2024-01-03"


def test_strategy_does_not_query_duckdb(monkeypatch: pytest.MonkeyPatch) -> None:
    import duckdb

    def _blocked(*_args, **_kwargs):
        raise AssertionError("strategies must not open DuckDB")

    monkeypatch.setattr(duckdb, "connect", _blocked)
    run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=_momentum_frame(),
            parameters={
                "top_n": 1,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "rebalance_frequency": "explicit",
                "explicit_dates_json": '["2024-01-02"]',
            },
            runtime_context={"trading_days": ["2024-01-02", "2024-01-03"]},
        ),
    )


def test_end_to_end_portfolio_engine_consumes_targets() -> None:
    frame = _momentum_frame()
    result = run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=frame,
            parameters={
                "top_n": 2,
                "max_positions": 2,
                "max_weight_per_asset": 0.5,
                "cash_buffer": 0.0,
                "rebalance_frequency": "explicit",
                "explicit_dates_json": '["2024-01-02","2024-01-04"]',
            },
            runtime_context={
                "trading_days": [
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                    "2024-01-08",
                ]
            },
        ),
    )
    targets = strategy_decisions_to_target_weights(
        result,
        max_positions=2,
        max_weight_per_asset=0.5,
        cash_buffer=0.0,
    )
    config = PortfolioBacktestConfig(
        name="cs-momentum",
        initial_cash=100_000.0,
        max_positions=2,
        max_weight_per_asset=0.5,
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0),
        execution=PortfolioExecutionRule(
            price_field="close",
            mark_price_field="close",
            lot_size=100,
            minimum_commission=0.0,
            enforce_t_plus_one=True,
        ),
    )
    validate_target_weights(targets, config)

    # signal 01-02 scores B=0.3,C=0.2,A=0.1 -> B,C execute 01-03
    day1 = targets[targets["trade_date"] == "2024-01-03"].set_index("asset_id")
    assert set(day1.index) == {"B", "C"}
    assert day1["target_weight"].tolist() == pytest.approx([0.5, 0.5])

    # signal 01-04 scores B=0.5,A=0.2,C=0.1 -> B,A execute 01-05; C zeroed
    day2 = targets[targets["trade_date"] == "2024-01-05"].set_index("asset_id")
    assert day2["target_weight"].to_dict() == pytest.approx(
        {"A": 0.5, "B": 0.5, "C": 0.0}
    )

    price_df = _prices(
        [
            {
                "trade_date": day,
                "asset_id": asset,
                "open": 10,
                "high": 10,
                "low": 10,
                "close": 10,
            }
            for day in [
                "2024-01-02",
                "2024-01-03",
                "2024-01-04",
                "2024-01-05",
                "2024-01-08",
            ]
            for asset in ["A", "B", "C"]
        ]
    )
    portfolio = PortfolioBacktestEngine().run(price_df, targets, config)
    assert portfolio.orders
    assert portfolio.fills
    assert portfolio.snapshots
    assert portfolio.equity_curve
    # Execution happens on trade_date 01-03, not signal date 01-02.
    assert all(order.trade_date != "2024-01-02" or order.side for order in portfolio.orders)
    assert any(order.trade_date == "2024-01-03" for order in portfolio.orders)
    # T+1 remains enforced by engine: first buy available only next day.
    first_buy_day = next(
        snapshot for snapshot in portfolio.snapshots if snapshot.trade_date == "2024-01-03"
    )
    assert first_buy_day.positions
    for position in first_buy_day.positions:
        assert position.available_quantity == 0


def test_cash_buffer_and_cap_are_honored_in_decision_weights() -> None:
    frame = _momentum_frame()
    result = run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=frame,
            parameters={
                "top_n": 3,
                "max_positions": 3,
                "max_weight_per_asset": 0.2,
                "cash_buffer": 0.1,
                "rebalance_frequency": "explicit",
                "explicit_dates_json": '["2024-01-02"]',
            },
            runtime_context={"trading_days": ["2024-01-02", "2024-01-03"]},
        ),
    )
    enters = [item for item in result.decisions if item.action is StrategyAction.ENTER]
    assert len(enters) == 3
    assert all(item.weight == pytest.approx(0.2) for item in enters)
    targets = strategy_decisions_to_target_weights(
        result,
        max_positions=3,
        max_weight_per_asset=0.2,
        cash_buffer=0.1,
    )
    assert float(targets["target_weight"].sum()) == pytest.approx(0.6)


def test_invalid_parameters_are_rejected() -> None:
    frame = _momentum_frame()
    with pytest.raises(StrategyValidationError):
        run_strategy(
            "cross_sectional_momentum_long_only",
            StrategyInput(
                prepared_data=frame,
                parameters={"top_n": 0, "max_weight_per_asset": 1.0},
            ),
        )
    with pytest.raises(StrategyValidationError):
        run_strategy(
            "multifactor_long_only",
            StrategyInput(
                prepared_data=frame,
                parameters={
                    "factor_columns_json": '["momentum"]',
                    "factor_weights_json": "[0.0]",
                    "max_weight_per_asset": 1.0,
                },
            ),
        )
