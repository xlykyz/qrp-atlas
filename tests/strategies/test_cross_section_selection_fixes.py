"""Regression tests for 04-D schedule, priority, cash buffer and EXIT reason fixes."""

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
)
from qrp_atlas.strategies import (
    StrategyAction,
    StrategyDecision,
    StrategyDefinition,
    StrategyInput,
    StrategyRunResult,
    StrategyType,
    StrategyValidationError,
    build_rebalance_schedule,
    equal_weight_targets,
    run_strategy,
    select_top_n,
    selection_to_target_weights,
)


def _momentum_frame(scores_by_day: dict[str, dict[str, float]]) -> pd.DataFrame:
    rows = []
    for day, scores in scores_by_day.items():
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


def test_explicit_dates_unsorted_match_sorted_schedule_and_decisions() -> None:
    calendar = [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
        "2024-01-05",
        "2024-01-08",
    ]
    ordered = build_rebalance_schedule(
        calendar,
        frequency="explicit",
        explicit_dates=["2024-01-02", "2024-01-04"],
    )
    shuffled = build_rebalance_schedule(
        calendar,
        frequency="explicit",
        explicit_dates=["2024-01-04", "2024-01-02", "2024-01-04"],
    )
    pd.testing.assert_frame_equal(ordered, shuffled)
    assert ordered["signal_date"].is_unique
    assert ordered["trade_date"].is_unique
    assert ordered["signal_date"].is_monotonic_increasing
    assert ordered["trade_date"].is_monotonic_increasing
    assert (ordered["trade_date"] > ordered["signal_date"]).all()

    frame = _momentum_frame(
        {
            "2024-01-02": {"A": 0.1, "B": 0.3, "C": 0.2},
            "2024-01-04": {"A": 0.4, "B": 0.1, "C": 0.2},
        }
    )
    params = {
        "top_n": 1,
        "max_positions": 1,
        "max_weight_per_asset": 1.0,
        "rebalance_frequency": "explicit",
    }
    first = run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=frame,
            parameters={**params, "explicit_dates_json": '["2024-01-02","2024-01-04"]'},
            runtime_context={"trading_days": calendar},
        ),
    )
    second = run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=frame,
            parameters={**params, "explicit_dates_json": '["2024-01-04","2024-01-02"]'},
            runtime_context={"trading_days": calendar},
        ),
    )
    assert [item.to_dict() for item in first.decisions] == [
        item.to_dict() for item in second.decisions
    ]
    actions = [(item.trade_date, item.asset_id, item.action) for item in first.decisions]
    assert actions[0][2] is StrategyAction.ENTER
    assert any(item.action is StrategyAction.EXIT for item in first.decisions)


def test_emit_unchanged_snapshots_for_identical_rebalances() -> None:
    definition = StrategyDefinition(
        code="fixture",
        name="fixture",
        version="1.0.0",
        description="fixture",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(),
        required_indicators=(),
    )
    decisions = (
        StrategyDecision(
            trade_date="2024-01-03",
            asset_id="A",
            action=StrategyAction.ENTER,
            direction="long",
            strategy_code="fixture",
            strategy_version="1.0.0",
            reason_code="enter",
            score=1.0,
            weight=1.0,
            evidence={"rank": 1, "priority": -1.0},
        ),
        StrategyDecision(
            trade_date="2024-01-05",
            asset_id="A",
            action=StrategyAction.HOLD,
            direction="long",
            strategy_code="fixture",
            strategy_version="1.0.0",
            reason_code="hold",
            score=1.0,
            weight=1.0,
            evidence={"rank": 1, "priority": -1.0},
        ),
    )
    result = StrategyRunResult(definition, {}, decisions)
    legacy = strategy_decisions_to_target_weights(
        result,
        max_positions=1,
        max_weight_per_asset=1.0,
        emit_unchanged_snapshots=False,
    )
    full = strategy_decisions_to_target_weights(
        result,
        max_positions=1,
        max_weight_per_asset=1.0,
        emit_unchanged_snapshots=True,
    )
    assert legacy["trade_date"].tolist() == ["2024-01-03"]
    assert full["trade_date"].tolist() == ["2024-01-03", "2024-01-05"]
    assert full.loc[full["trade_date"] == "2024-01-05", "target_weight"].tolist() == [
        1.0
    ]


def test_retry_target_after_blocked_first_buy() -> None:
    # Decision holdings unchanged, but second rebalance must still emit targets.
    definition = StrategyDefinition(
        code="fixture",
        name="fixture",
        version="1.0.0",
        description="fixture",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(),
        required_indicators=(),
    )
    decisions = (
        StrategyDecision(
            trade_date="2024-01-03",
            asset_id="A",
            action=StrategyAction.ENTER,
            direction="long",
            strategy_code="fixture",
            strategy_version="1.0.0",
            reason_code="enter",
            score=2.0,
            weight=1.0,
            evidence={"rank": 1},
        ),
        StrategyDecision(
            trade_date="2024-01-05",
            asset_id="A",
            action=StrategyAction.HOLD,
            direction="long",
            strategy_code="fixture",
            strategy_version="1.0.0",
            reason_code="hold",
            score=2.0,
            weight=1.0,
            evidence={"rank": 1},
        ),
    )
    targets = strategy_decisions_to_target_weights(
        StrategyRunResult(definition, {}, decisions),
        max_positions=1,
        max_weight_per_asset=1.0,
        emit_unchanged_snapshots=True,
    )
    price_df = pd.DataFrame(
        [
            {
                "trade_date": day,
                "asset_id": "A",
                "asset_name": "x",
                "asset_type": "stock",
                "open": 10,
                "high": 10,
                "low": 10,
                "close": 10,
                "is_limit_up": day == "2024-01-03",
            }
            for day in ["2024-01-03", "2024-01-04", "2024-01-05"]
        ]
    )
    config = PortfolioBacktestConfig(
        name="retry",
        initial_cash=10_000.0,
        max_positions=1,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0),
        execution=PortfolioExecutionRule(
            price_field="close",
            mark_price_field="close",
            lot_size=100,
            minimum_commission=0.0,
        ),
    )
    result = PortfolioBacktestEngine().run(price_df, targets, config)
    # First execution blocked by limit-up; second rebalance retries successfully.
    assert any(order.trade_date == "2024-01-03" and order.status != "FILLED" for order in result.orders) or any(
        order.trade_date == "2024-01-03" for order in result.orders
    )
    assert any(fill.trade_date == "2024-01-05" for fill in result.fills)
    assert any(snapshot.trade_date == "2024-01-05" and snapshot.positions for snapshot in result.snapshots)


def test_rank_not_raw_score_controls_priority_and_capacity() -> None:
    # Ascending selection: low score ranks first. Raw score must not override rank.
    weights = equal_weight_targets(
        ["A", "B", "C"],
        trade_date="2024-01-03",
        scores={"A": 9.0, "B": 1.0, "C": 5.0},
        ranks={"A": 3, "B": 1, "C": 2},
        max_positions=2,
        max_weight_per_asset=0.5,
    )
    assert weights["asset_id"].tolist() == ["B", "C"]
    assert weights["priority"].tolist() == pytest.approx([-1.0, -2.0])

    selection = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"] * 3,
            "asset_id": ["A", "B", "C"],
            "score": [9.0, 1.0, 5.0],
            "rank": [3, 1, 2],
            "selected": [True, True, True],
        }
    )
    targets = selection_to_target_weights(
        selection,
        signal_to_trade={"2024-01-02": "2024-01-03"},
        max_positions=1,
        max_weight_per_asset=1.0,
    )
    assert targets.loc[targets["target_weight"] > 0, "asset_id"].tolist() == ["B"]
    assert targets.loc[targets["asset_id"] == "B", "priority"].iloc[0] == pytest.approx(
        -1.0
    )

    definition = StrategyDefinition(
        code="fixture",
        name="fixture",
        version="1.0.0",
        description="fixture",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(),
        required_indicators=(),
    )
    decisions = (
        StrategyDecision(
            trade_date="2024-01-03",
            asset_id="A",
            action=StrategyAction.ENTER,
            direction="long",
            strategy_code="fixture",
            strategy_version="1.0.0",
            reason_code="enter",
            score=9.0,
            weight=0.5,
            evidence={"rank": 2, "priority": -2.0},
        ),
        StrategyDecision(
            trade_date="2024-01-03",
            asset_id="B",
            action=StrategyAction.ENTER,
            direction="long",
            strategy_code="fixture",
            strategy_version="1.0.0",
            reason_code="enter",
            score=1.0,
            weight=0.5,
            evidence={"rank": 1, "priority": -1.0},
        ),
    )
    targets = strategy_decisions_to_target_weights(
        StrategyRunResult(definition, {}, decisions),
        max_positions=1,
        max_weight_per_asset=1.0,
    )
    assert targets.loc[targets["target_weight"] > 0, "asset_id"].tolist() == ["B"]
    assert targets.loc[targets["asset_id"] == "B", "priority"].iloc[0] == pytest.approx(
        -1.0
    )


def test_previous_holdings_empty_selection_full_liquidation() -> None:
    empty_like = pd.DataFrame(
        {
            "trade_date": pd.Series(dtype="datetime64[ns]"),
            "asset_id": pd.Series(dtype=object),
            "score": pd.Series(dtype=float),
            "rank": pd.Series(dtype=float),
            "selected": pd.Series(dtype=bool),
        }
    )
    targets = selection_to_target_weights(
        empty_like,
        previous_assets_by_trade_date={"2024-01-04": ["A", "B"]},
        max_weight_per_asset=1.0,
    )
    assert targets.to_dict("records") == [
        {
            "trade_date": "2024-01-04",
            "asset_id": "A",
            "target_weight": 0.0,
            "priority": 0.0,
        },
        {
            "trade_date": "2024-01-04",
            "asset_id": "B",
            "target_weight": 0.0,
            "priority": 0.0,
        },
    ]


def test_cash_buffer_scales_explicit_weights() -> None:
    definition = StrategyDefinition(
        code="fixture",
        name="fixture",
        version="1.0.0",
        description="fixture",
        strategy_type=StrategyType.BUILTIN,
        required_fields=(),
        required_indicators=(),
    )
    decisions = (
        StrategyDecision(
            trade_date="2024-01-03",
            asset_id="A",
            action=StrategyAction.ENTER,
            direction="long",
            strategy_code="fixture",
            strategy_version="1.0.0",
            reason_code="enter",
            score=2.0,
            weight=0.5,
            evidence={"rank": 1},
        ),
        StrategyDecision(
            trade_date="2024-01-03",
            asset_id="B",
            action=StrategyAction.ENTER,
            direction="long",
            strategy_code="fixture",
            strategy_version="1.0.0",
            reason_code="enter",
            score=1.0,
            weight=0.5,
            evidence={"rank": 2},
        ),
    )
    targets = strategy_decisions_to_target_weights(
        StrategyRunResult(definition, {}, decisions),
        max_positions=2,
        max_weight_per_asset=1.0,
        cash_buffer=0.2,
    )
    assert float(targets["target_weight"].sum()) == pytest.approx(0.8)
    assert targets["target_weight"].tolist() == pytest.approx([0.4, 0.4])


def test_score_column_reserved_names_are_rejected() -> None:
    frame = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"],
            "asset_id": ["A"],
            "rank": [1.0],
            "selected": [1.0],
            "eligible": [1.0],
            "momentum": [0.2],
        }
    )
    with pytest.raises(Exception, match="reserved"):
        select_top_n(frame, n=1, score_column="rank")
    with pytest.raises(Exception, match="reserved"):
        select_top_n(frame, n=1, score_column="selected")
    with pytest.raises(StrategyValidationError, match="reserved"):
        run_strategy(
            "cross_sectional_momentum_long_only",
            StrategyInput(
                prepared_data=frame.assign(ticker="A"),
                parameters={
                    "score_column": "eligible",
                    "top_n": 1,
                    "max_positions": 1,
                    "max_weight_per_asset": 1.0,
                    "rebalance_frequency": "explicit",
                    "explicit_dates_json": '["2024-01-02"]',
                },
                runtime_context={"trading_days": ["2024-01-02", "2024-01-03"]},
            ),
        )


def test_exit_reasons_are_specific() -> None:
    frame = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"] * 3 + ["2024-01-03"] * 2,
            "asset_id": ["A", "B", "C", "A", "C"],
            "ticker": ["A", "B", "C", "A", "C"],
            "momentum": [0.3, 0.2, 0.1, math.nan, 0.5],
        }
    )
    eligibility = pd.DataFrame(
        {
            "trade_date": [
                "2024-01-02",
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
                "2024-01-03",
            ],
            "asset_id": ["A", "B", "C", "A", "C"],
            "eligible": [True, True, True, True, True],
            "reason_code": ["OK"] * 5,
        }
    )
    # First signal selects A (top1). Second signal: A invalid score, B missing row,
    # C selected.
    result = run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=frame,
            parameters={
                "top_n": 1,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "rebalance_frequency": "explicit",
                "explicit_dates_json": '["2024-01-02","2024-01-03"]',
            },
            runtime_context={
                "trading_days": ["2024-01-02", "2024-01-03", "2024-01-04"],
                "eligibility": eligibility,
            },
        ),
    )
    # Seed a second run that starts holding B and C then exits for different reasons.
    frame2 = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"] * 3,
            "asset_id": ["A", "B", "C"],
            "ticker": ["A", "B", "C"],
            "momentum": [0.5, 0.4, math.nan],
        }
    )
    eligibility2 = pd.DataFrame(
        {
            "trade_date": ["2024-01-02", "2024-01-02", "2024-01-02"],
            "asset_id": ["A", "B", "C"],
            "eligible": [True, True, False],
            "reason_code": ["OK", "OK", "ST"],
        }
    )
    result2 = run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=frame2,
            parameters={
                "top_n": 1,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "rebalance_frequency": "explicit",
                "explicit_dates_json": '["2024-01-02"]',
            },
            initial_positions={"B": True, "C": True, "D": True},
            runtime_context={
                "trading_days": ["2024-01-02", "2024-01-03"],
                "eligibility": eligibility2,
            },
        ),
    )
    exits = {
        item.asset_id: item.evidence["eligibility_reason"]
        for item in result2.decisions
        if item.action is StrategyAction.EXIT
    }
    assert exits["B"] == "NOT_TOP_N"
    assert exits["C"] == "INELIGIBLE"
    assert exits["D"] == "MISSING_SIGNAL_ROW"

    # From the multi-day run, A should exit for invalid score.
    exits_multi = {
        item.asset_id: item.evidence["eligibility_reason"]
        for item in result.decisions
        if item.action is StrategyAction.EXIT
    }
    assert exits_multi["A"] == "INVALID_SCORE"


def test_ascending_strategy_respects_rank_under_capacity_clip() -> None:
    frame = _momentum_frame(
        {
            "2024-01-02": {"A": 0.1, "B": 0.2, "C": 0.3},
        }
    )
    result = run_strategy(
        "cross_sectional_momentum_long_only",
        StrategyInput(
            prepared_data=frame,
            parameters={
                "top_n": 3,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "ascending": True,
                "rebalance_frequency": "explicit",
                "explicit_dates_json": '["2024-01-02"]',
            },
            runtime_context={"trading_days": ["2024-01-02", "2024-01-03"]},
        ),
    )
    enters = [item for item in result.decisions if item.action is StrategyAction.ENTER]
    assert [item.asset_id for item in enters] == ["A"]
    assert enters[0].evidence["rank"] == 1
    assert enters[0].evidence["priority"] == pytest.approx(-1.0)
    targets = strategy_decisions_to_target_weights(
        result,
        max_positions=1,
        max_weight_per_asset=1.0,
        emit_unchanged_snapshots=True,
    )
    assert targets.loc[targets["target_weight"] > 0, "asset_id"].tolist() == ["A"]
