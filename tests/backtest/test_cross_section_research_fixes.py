"""Regression tests for 04-E research orchestration and exposure contract repairs."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.backtest import (
    CostRule,
    PortfolioBacktestConfig,
    PortfolioExecutionRule,
    analyze_target_exposures,
    assign_factor_groups,
    compute_forward_returns,
    compute_group_returns,
    run_cross_section_research,
)
from qrp_atlas.backtest.research.exposures import ExposureAnalysisError
from qrp_atlas.backtest.research.pipeline import CrossSectionResearchError


def _prices(rows):
    return pd.DataFrame(rows).assign(asset_name="x", asset_type="stock")


def _calendar():
    return [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
        "2024-01-05",
        "2024-01-08",
        "2024-01-09",
    ]


def _config(**kwargs):
    return PortfolioBacktestConfig(
        name="research-fix",
        initial_cash=100_000.0,
        max_positions=kwargs.get("max_positions", 3),
        max_weight_per_asset=kwargs.get("max_weight_per_asset", 1.0),
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0),
        execution=PortfolioExecutionRule(
            price_field="close",
            mark_price_field="close",
            lot_size=100,
            minimum_commission=0.0,
            enforce_t_plus_one=True,
        ),
    )


def _price_panel(calendar, assets=("A", "B", "C")):
    rows = []
    for idx, day in enumerate(calendar):
        for j, asset in enumerate(assets):
            px = 10 + idx + j
            rows.append(
                {
                    "trade_date": day,
                    "asset_id": asset,
                    "open": px,
                    "high": px,
                    "low": px,
                    "close": px,
                }
            )
    return _prices(rows)


def test_multifactor_loop_uses_factor_json_not_score_column() -> None:
    calendar = _calendar()
    factors = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"] * 3 + ["2024-01-04"] * 3,
            "asset_id": ["A", "B", "C"] * 2,
            "ticker": ["A", "B", "C"] * 2,
            "momentum": [0.1, 0.4, 0.2, 0.5, 0.1, 0.2],
            "value": [0.3, 0.1, 0.2, 0.2, 0.4, 0.1],
        }
    )
    result = run_cross_section_research(
        factor_frame=factors,
        price_df=_price_panel(calendar),
        trading_days=calendar,
        factor_columns=["momentum", "value"],
        strategy_code="multifactor_long_only",
        strategy_parameters={
            "top_n": 2,
            "max_positions": 2,
            "max_weight_per_asset": 0.5,
            "cash_buffer": 0.0,
            "rebalance_frequency": "explicit",
            "explicit_dates_json": '["2024-01-02","2024-01-04"]',
            "factor_columns_json": '["momentum","value"]',
            "factor_weights_json": "[0.5,0.5]",
        },
        portfolio_config=_config(max_positions=2, max_weight_per_asset=0.5),
        horizons=(1,),
        n_groups=2,
        run_portfolio=True,
    )
    assert result.strategy_result is not None
    assert "score_column" not in result.metadata["resolved_parameters"]
    assert result.metadata["resolved_parameters"]["factor_columns_json"] == (
        '["momentum","value"]'
    )
    assert result.strategy_result.decisions
    assert not result.target_weights.empty
    assert result.portfolio_result is not None
    # schedule and strategy evidence must agree
    schedule_signals = {
        pd.Timestamp(value).strftime("%Y-%m-%d")
        for value in result.metadata and []
    }
    # use decision evidence instead
    evidence_signals = {
        item.evidence["signal_date"] for item in result.strategy_result.decisions
    }
    assert evidence_signals == {"2024-01-02", "2024-01-04"}


def test_unknown_score_column_for_multifactor_is_rejected() -> None:
    calendar = _calendar()
    factors = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"] * 3,
            "asset_id": ["A", "B", "C"],
            "ticker": ["A", "B", "C"],
            "momentum": [0.1, 0.2, 0.3],
            "value": [0.3, 0.2, 0.1],
        }
    )
    with pytest.raises(CrossSectionResearchError, match="unknown strategy parameters"):
        run_cross_section_research(
            factor_frame=factors,
            price_df=_price_panel(calendar),
            trading_days=calendar,
            factor_columns=["momentum", "value"],
            strategy_code="multifactor_long_only",
            strategy_parameters={
                "score_column": "momentum",
                "factor_columns_json": '["momentum","value"]',
                "factor_weights_json": "[0.5,0.5]",
                "max_weight_per_asset": 1.0,
            },
            portfolio_config=_config(),
            run_portfolio=False,
        )


def test_explicit_dates_json_drives_same_schedule_and_strategy() -> None:
    calendar = _calendar()
    factors = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"] * 3 + ["2024-01-04"] * 3,
            "asset_id": ["A", "B", "C"] * 2,
            "ticker": ["A", "B", "C"] * 2,
            "momentum": [0.1, 0.3, 0.2, 0.4, 0.1, 0.2],
        }
    )
    result = run_cross_section_research(
        factor_frame=factors,
        price_df=_price_panel(calendar),
        trading_days=calendar,
        factor_columns=["momentum"],
        strategy_code="cross_sectional_momentum_long_only",
        strategy_parameters={
            "top_n": 1,
            "max_positions": 1,
            "max_weight_per_asset": 1.0,
            "rebalance_frequency": "explicit",
            "explicit_dates_json": '["2024-01-04","2024-01-02"]',
        },
        portfolio_config=_config(max_weight_per_asset=1.0),
        horizons=(1,),
        run_portfolio=False,
    )
    signals = sorted(
        {
            item.evidence["signal_date"]
            for item in result.strategy_result.decisions
        }
    )
    assert signals == ["2024-01-02", "2024-01-04"]
    trades = sorted(set(result.target_weights["trade_date"]))
    assert trades == ["2024-01-03", "2024-01-05"]
    assert result.metadata["resolved_parameters"]["rebalance_frequency"] == "explicit"


def test_convenience_args_override_strategy_parameters() -> None:
    calendar = _calendar()
    factors = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"] * 3 + ["2024-01-04"] * 3,
            "asset_id": ["A", "B", "C"] * 2,
            "ticker": ["A", "B", "C"] * 2,
            "momentum": [0.1, 0.3, 0.2, 0.4, 0.1, 0.2],
        }
    )
    result = run_cross_section_research(
        factor_frame=factors,
        price_df=_price_panel(calendar),
        trading_days=calendar,
        factor_columns=["momentum"],
        strategy_code="cross_sectional_momentum_long_only",
        strategy_parameters={
            "top_n": 1,
            "max_positions": 1,
            "max_weight_per_asset": 1.0,
            "rebalance_frequency": "weekly",
            "explicit_dates_json": '["2024-01-02"]',
        },
        rebalance_frequency="explicit",
        explicit_dates=["2024-01-04"],
        portfolio_config=_config(max_weight_per_asset=1.0),
        horizons=(1,),
        run_portfolio=False,
    )
    signals = {
        item.evidence["signal_date"] for item in result.strategy_result.decisions
    }
    assert signals == {"2024-01-04"}
    assert result.metadata["resolved_parameters"]["rebalance_frequency"] == "explicit"


def test_exposure_rejects_invalid_targets_and_schedules() -> None:
    schedule = pd.DataFrame(
        {"signal_date": ["2024-01-02"], "trade_date": ["2024-01-03"]}
    )
    with pytest.raises(ExposureAnalysisError, match="duplicate"):
        analyze_target_exposures(
            pd.DataFrame(
                [
                    {"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.5},
                    {"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.4},
                ]
            ),
            schedule=schedule,
        )
    with pytest.raises(ExposureAnalysisError, match="sum to <= 1"):
        analyze_target_exposures(
            pd.DataFrame(
                [
                    {"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.7},
                    {"trade_date": "2024-01-03", "asset_id": "B", "target_weight": 0.5},
                ]
            ),
            schedule=schedule,
        )
    with pytest.raises(ExposureAnalysisError, match="finite"):
        analyze_target_exposures(
            pd.DataFrame(
                [
                    {
                        "trade_date": "2024-01-03",
                        "asset_id": "A",
                        "target_weight": math.nan,
                    }
                ]
            ),
            schedule=schedule,
        )
    with pytest.raises(ExposureAnalysisError, match=">= 0"):
        analyze_target_exposures(
            pd.DataFrame(
                [{"trade_date": "2024-01-03", "asset_id": "A", "target_weight": -0.1}]
            ),
            schedule=schedule,
        )
    with pytest.raises(ExposureAnalysisError, match="unique"):
        analyze_target_exposures(
            pd.DataFrame(
                [{"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.5}]
            ),
            schedule=pd.DataFrame(
                {
                    "signal_date": ["2024-01-02", "2024-01-02"],
                    "trade_date": ["2024-01-03", "2024-01-04"],
                }
            ),
        )
    with pytest.raises(ExposureAnalysisError, match="unique"):
        analyze_target_exposures(
            pd.DataFrame(
                [{"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.5}]
            ),
            schedule=pd.DataFrame(
                {
                    "signal_date": ["2024-01-02", "2024-01-04"],
                    "trade_date": ["2024-01-03", "2024-01-03"],
                }
            ),
        )


def test_single_group_spread_is_nan() -> None:
    assignments = assign_factor_groups(
        pd.DataFrame(
            {
                "trade_date": ["2024-01-02", "2024-01-02"],
                "asset_id": ["A", "B"],
                "momentum": [1.0, 2.0],
            }
        ),
        factor_columns=["momentum"],
        n_groups=1,
    )
    returns = pd.DataFrame(
        {
            "trade_date": ["2024-01-02", "2024-01-02"],
            "asset_id": ["A", "B"],
            "forward_return_1d": [0.1, 0.2],
        }
    )
    result = compute_group_returns(assignments, returns, horizons=(1,))
    assert len(result.spreads) == 1
    assert result.spreads.iloc[0]["high_group"] == 1
    assert result.spreads.iloc[0]["low_group"] == 1
    assert math.isnan(result.spreads.iloc[0]["spread_return"])


def test_empty_factor_frame_does_not_expand_to_price_universe() -> None:
    calendar = _calendar()
    empty = pd.DataFrame(columns=["trade_date", "asset_id", "momentum"])
    result = run_cross_section_research(
        factor_frame=empty,
        price_df=_price_panel(calendar),
        trading_days=calendar,
        factor_columns=["momentum"],
        strategy_code="cross_sectional_momentum_long_only",
        strategy_parameters={
            "top_n": 1,
            "max_positions": 1,
            "max_weight_per_asset": 1.0,
            "rebalance_frequency": "explicit",
            "explicit_dates_json": '["2024-01-02"]',
        },
        portfolio_config=_config(max_weight_per_asset=1.0),
        horizons=(1, 5),
        run_portfolio=False,
    )
    assert result.forward_returns.empty
    assert list(result.forward_returns.columns) == [
        "trade_date",
        "asset_id",
        "forward_return_1d",
        "forward_return_5d",
    ]
    assert "empty_factor_universe" in result.diagnostics
    assert result.daily_ic.empty
    assert result.group_returns.empty


def test_future_returns_still_do_not_change_decisions() -> None:
    calendar = _calendar()
    factors = pd.DataFrame(
        {
            "trade_date": ["2024-01-02"] * 3,
            "asset_id": ["A", "B", "C"],
            "ticker": ["A", "B", "C"],
            "momentum": [0.1, 0.3, 0.2],
        }
    )
    prices = _price_panel(calendar)
    params = {
        "top_n": 1,
        "max_positions": 1,
        "max_weight_per_asset": 1.0,
        "rebalance_frequency": "explicit",
        "explicit_dates_json": '["2024-01-02"]',
    }
    base = run_cross_section_research(
        factor_frame=factors,
        price_df=prices,
        trading_days=calendar,
        factor_columns=["momentum"],
        strategy_code="cross_sectional_momentum_long_only",
        strategy_parameters=params,
        portfolio_config=_config(max_weight_per_asset=1.0),
        horizons=(1,),
        run_portfolio=False,
    )
    altered = prices.copy()
    altered.loc[altered["trade_date"] > "2024-01-02", "close"] *= 5
    changed = run_cross_section_research(
        factor_frame=factors,
        price_df=altered,
        trading_days=calendar,
        factor_columns=["momentum"],
        strategy_code="cross_sectional_momentum_long_only",
        strategy_parameters=params,
        portfolio_config=_config(max_weight_per_asset=1.0),
        horizons=(1,),
        run_portfolio=False,
    )
    assert [item.to_dict() for item in base.strategy_result.decisions] == [
        item.to_dict() for item in changed.strategy_result.decisions
    ]
    pd.testing.assert_frame_equal(base.target_weights, changed.target_weights)
    assert not base.forward_returns.equals(changed.forward_returns)
