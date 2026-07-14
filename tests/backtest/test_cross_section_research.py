"""Tests for cross-sectional research analytics (task 04-E)."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.backtest import (
    PortfolioBacktestConfig,
    PortfolioExecutionRule,
    CostRule,
    analyze_target_exposures,
    assign_factor_groups,
    compute_forward_returns,
    compute_group_returns,
    compute_information_coefficient,
    run_cross_section_research,
    summarize_information_coefficient,
)
from qrp_atlas.strategies import StrategyAction, build_rebalance_schedule


def _prices(rows):
    return pd.DataFrame(rows).assign(asset_name="x", asset_type="stock")


def _calendar(*days: str) -> list[str]:
    return list(days)


class TestForwardReturns:
    def test_formula_and_calendar_alignment(self) -> None:
        calendar = _calendar(
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
        )
        prices = _prices(
            [
                {"trade_date": "2024-01-02", "asset_id": "A", "close": 10.0},
                {"trade_date": "2024-01-03", "asset_id": "A", "close": 11.0},
                {"trade_date": "2024-01-04", "asset_id": "A", "close": 12.0},
                {"trade_date": "2024-01-05", "asset_id": "A", "close": 13.0},
                {"trade_date": "2024-01-08", "asset_id": "A", "close": 15.0},
                {"trade_date": "2024-01-09", "asset_id": "A", "close": 16.0},
            ]
        )
        original = prices.copy(deep=True)
        out = compute_forward_returns(
            prices,
            trading_days=calendar,
            horizons=(1, 5),
            as_of_dates=["2024-01-02", "2024-01-03"],
        )
        pd.testing.assert_frame_equal(prices, original)
        row = out.set_index("trade_date").loc[pd.Timestamp("2024-01-02")]
        assert row["forward_return_1d"] == pytest.approx(0.1)
        # T+5 on full market calendar: 01-02 -> 01-09
        assert row["forward_return_5d"] == pytest.approx(0.6)

    def test_missing_asset_day_does_not_skip_to_farther_date(self) -> None:
        calendar = _calendar("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05")
        prices = _prices(
            [
                {"trade_date": "2024-01-02", "asset_id": "A", "close": 10.0},
                # A missing on 01-03; present later
                {"trade_date": "2024-01-04", "asset_id": "A", "close": 12.0},
                {"trade_date": "2024-01-05", "asset_id": "A", "close": 13.0},
                {"trade_date": "2024-01-02", "asset_id": "B", "close": 20.0},
                {"trade_date": "2024-01-03", "asset_id": "B", "close": 22.0},
                {"trade_date": "2024-01-04", "asset_id": "B", "close": 24.0},
                {"trade_date": "2024-01-05", "asset_id": "B", "close": 26.0},
            ]
        )
        out = compute_forward_returns(
            prices,
            trading_days=calendar,
            horizons=(1,),
            as_of_dates=["2024-01-02"],
        )
        by_asset = out.set_index("asset_id")
        assert math.isnan(by_asset.loc["A", "forward_return_1d"])
        assert by_asset.loc["B", "forward_return_1d"] == pytest.approx(0.1)

    def test_non_positive_and_out_of_range(self) -> None:
        calendar = _calendar("2024-01-02", "2024-01-03")
        prices = _prices(
            [
                {"trade_date": "2024-01-02", "asset_id": "A", "close": 0.0},
                {"trade_date": "2024-01-03", "asset_id": "A", "close": 10.0},
                {"trade_date": "2024-01-02", "asset_id": "B", "close": 10.0},
                {"trade_date": "2024-01-03", "asset_id": "B", "close": -1.0},
            ]
        )
        out = compute_forward_returns(
            prices,
            trading_days=calendar,
            horizons=(1, 5),
            as_of_dates=["2024-01-02", "2024-01-03"],
        )
        assert out["forward_return_1d"].isna().all() or math.isnan(
            out.loc[out["asset_id"] == "A", "forward_return_1d"].iloc[0]
        )
        # last date has no T+1 / T+5
        last = out[out["trade_date"] == pd.Timestamp("2024-01-03")]
        assert last["forward_return_1d"].isna().all()
        assert last["forward_return_5d"].isna().all()
        assert len(out) == 4


class TestIC:
    def test_known_pearson_and_spearman(self) -> None:
        factors = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 4,
                "asset_id": ["A", "B", "C", "D"],
                "momentum": [1.0, 2.0, 3.0, 4.0],
            }
        )
        returns = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 4,
                "asset_id": ["A", "B", "C", "D"],
                "forward_return_1d": [1.0, 2.0, 3.0, 4.0],
            }
        )
        daily = compute_information_coefficient(
            factors,
            returns,
            factor_columns=["momentum"],
            horizons=(1,),
        )
        pearson = daily[(daily["method"] == "pearson")].iloc[0]
        spearman = daily[(daily["method"] == "spearman")].iloc[0]
        assert pearson["ic"] == pytest.approx(1.0)
        assert spearman["ic"] == pytest.approx(1.0)
        assert pearson["n_obs"] == 4

        negative = returns.assign(forward_return_1d=[4.0, 3.0, 2.0, 1.0])
        daily_neg = compute_information_coefficient(
            factors, negative, factor_columns=["momentum"], horizons=(1,)
        )
        assert daily_neg[daily_neg["method"] == "pearson"].iloc[0]["ic"] == pytest.approx(
            -1.0
        )

    def test_constant_and_insufficient_sample(self) -> None:
        factors = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 3,
                "asset_id": ["A", "B", "C"],
                "momentum": [1.0, 1.0, 1.0],
            }
        )
        returns = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 3,
                "asset_id": ["A", "B", "C"],
                "forward_return_1d": [0.1, 0.2, 0.3],
            }
        )
        daily = compute_information_coefficient(
            factors, returns, factor_columns=["momentum"], horizons=(1,), min_obs=3
        )
        assert math.isnan(daily.iloc[0]["ic"])

        small = factors.iloc[:2]
        small_ret = returns.iloc[:2]
        daily_small = compute_information_coefficient(
            small, small_ret, factor_columns=["momentum"], horizons=(1,), min_obs=3
        )
        assert math.isnan(daily_small.iloc[0]["ic"])
        assert daily_small.iloc[0]["n_obs"] == 2

    def test_summary_stats(self) -> None:
        daily = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(
                    ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
                ),
                "factor": ["momentum"] * 4,
                "horizon": [1] * 4,
                "method": ["pearson"] * 4,
                "ic": [0.2, -0.1, 0.4, 0.1],
                "n_obs": [10] * 4,
            }
        )
        summary = summarize_information_coefficient(daily)
        row = summary.iloc[0]
        assert row["n_dates"] == 4
        assert row["mean_ic"] == pytest.approx(0.15)
        assert row["positive_rate"] == pytest.approx(0.75)
        assert row["ic_ir"] == pytest.approx(row["mean_ic"] / row["std_ic"])


class TestGroups:
    def test_five_groups_ties_and_order_independence(self) -> None:
        frame = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 5,
                "asset_id": ["E", "D", "C", "B", "A"],
                "momentum": [5.0, 4.0, 3.0, 2.0, 1.0],
            }
        )
        shuffled = frame.sample(frac=1, random_state=3)
        a = assign_factor_groups(frame, factor_columns=["momentum"], n_groups=5)
        b = assign_factor_groups(shuffled, factor_columns=["momentum"], n_groups=5)
        left = a.sort_values(["asset_id"]).reset_index(drop=True)
        right = b.sort_values(["asset_id"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(left, right)
        by_asset = a.set_index("asset_id")["group"].to_dict()
        assert by_asset == {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5}

        tied = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 4,
                "asset_id": ["B", "A", "D", "C"],
                "momentum": [1.0, 1.0, 2.0, 2.0],
            }
        )
        groups = assign_factor_groups(tied, factor_columns=["momentum"], n_groups=2)
        # low group gets A then B by asset_id tie-break
        low = groups[groups["group"] == 1]["asset_id"].tolist()
        high = groups[groups["group"] == 2]["asset_id"].tolist()
        assert low == ["A", "B"]
        assert high == ["C", "D"]

    def test_group_returns_and_spread(self) -> None:
        assignments = assign_factor_groups(
            pd.DataFrame(
                {
                    "trade_date": ["2024-01-02"] * 4,
                    "asset_id": ["A", "B", "C", "D"],
                    "momentum": [1.0, 2.0, 3.0, 4.0],
                }
            ),
            factor_columns=["momentum"],
            n_groups=2,
        )
        returns = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 4,
                "asset_id": ["A", "B", "C", "D"],
                "forward_return_1d": [0.1, math.nan, 0.3, 0.5],
            }
        )
        result = compute_group_returns(assignments, returns, horizons=(1,))
        g1 = result.group_returns[result.group_returns["group"] == 1].iloc[0]
        g2 = result.group_returns[result.group_returns["group"] == 2].iloc[0]
        assert g1["member_count"] == 2
        assert g1["valid_return_count"] == 1
        assert g1["group_return"] == pytest.approx(0.1)
        assert g2["group_return"] == pytest.approx(0.4)
        spread = result.spreads.iloc[0]
        assert spread["spread_return"] == pytest.approx(0.3)


class TestExposures:
    def test_numeric_and_categorical_on_signal_date(self) -> None:
        targets = pd.DataFrame(
            [
                {"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.5},
                {"trade_date": "2024-01-03", "asset_id": "B", "target_weight": 0.3},
                {"trade_date": "2024-01-03", "asset_id": "C", "target_weight": 0.0},
            ]
        )
        schedule = pd.DataFrame(
            {"signal_date": ["2024-01-02"], "trade_date": ["2024-01-03"]}
        )
        factors = pd.DataFrame(
            {
                "trade_date": ["2024-01-02", "2024-01-02", "2024-01-03"],
                "asset_id": ["A", "B", "A"],
                "momentum": [1.0, 3.0, 99.0],
            }
        )
        exposure = pd.DataFrame(
            {
                "trade_date": ["2024-01-02", "2024-01-02", "2024-01-03"],
                "asset_id": ["A", "B", "A"],
                "industry_code": ["I1", "I2", "FUTURE"],
                "log_market_cap": [1.0, 3.0, 9.0],
            }
        )
        result = analyze_target_exposures(
            targets,
            schedule=schedule,
            factor_frame=factors,
            exposure_panel=exposure,
            numeric_exposures=["momentum", "log_market_cap"],
            categorical_exposures=["industry_code"],
        )
        num = result.numeric.set_index("exposure")
        assert num.loc["momentum", "weighted_mean"] == pytest.approx(
            (0.5 * 1.0 + 0.3 * 3.0) / 0.8
        )
        assert num.loc["log_market_cap", "covered_weight"] == pytest.approx(0.8)
        cats = result.categorical.set_index("category")["target_weight"].to_dict()
        assert cats == pytest.approx({"I1": 0.5, "I2": 0.3})
        assert "FUTURE" not in cats


class TestResearchLoopAndLeakage:
    def _fixture(self):
        calendar = [
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
        ]
        factors = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 3 + ["2024-01-04"] * 3,
                "asset_id": ["A", "B", "C"] * 2,
                "ticker": ["A", "B", "C"] * 2,
                "momentum": [0.1, 0.3, 0.2, 0.4, 0.1, 0.2],
            }
        )
        prices = _prices(
            [
                {
                    "trade_date": day,
                    "asset_id": asset,
                    "open": 10 + i,
                    "high": 10 + i,
                    "low": 10 + i,
                    "close": 10 + i + (0 if day != "2024-01-09" else 1),
                }
                for i, day in enumerate(calendar)
                for asset in ["A", "B", "C"]
            ]
        )
        exposure = pd.DataFrame(
            {
                "trade_date": ["2024-01-02", "2024-01-02", "2024-01-02"],
                "asset_id": ["A", "B", "C"],
                "industry_code": ["I1", "I1", "I2"],
                "log_market_cap": [1.0, 2.0, 3.0],
            }
        )
        config = PortfolioBacktestConfig(
            name="research",
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
        return calendar, factors, prices, exposure, config

    def test_end_to_end_research_loop(self) -> None:
        calendar, factors, prices, exposure, config = self._fixture()
        result = run_cross_section_research(
            factor_frame=factors,
            price_df=prices,
            trading_days=calendar,
            factor_columns=["momentum"],
            strategy_code="cross_sectional_momentum_long_only",
            strategy_parameters={
                "top_n": 2,
                "max_positions": 2,
                "max_weight_per_asset": 0.5,
                "cash_buffer": 0.0,
                "rebalance_frequency": "explicit",
            },
            explicit_dates=["2024-01-02", "2024-01-04"],
            exposure_panel=exposure,
            portfolio_config=config,
            horizons=(1, 5),
            n_groups=2,
            categorical_exposures=["industry_code"],
            numeric_exposures=["momentum", "log_market_cap"],
        )
        assert not result.forward_returns.empty
        assert not result.daily_ic.empty
        assert not result.ic_summary.empty
        assert not result.group_returns.empty
        assert result.strategy_result is not None
        assert not result.target_weights.empty
        assert result.portfolio_result is not None
        # T signal -> T+1 execution
        assert set(result.target_weights["trade_date"]) <= {
            "2024-01-03",
            "2024-01-05",
        }
        assert all(
            item.evidence["signal_date"] in {"2024-01-02", "2024-01-04"}
            for item in result.strategy_result.decisions
        )
        assert result.portfolio_result.orders
        assert result.portfolio_result.snapshots

    def test_future_returns_do_not_change_selection(self) -> None:
        calendar, factors, prices, exposure, config = self._fixture()
        base = run_cross_section_research(
            factor_frame=factors,
            price_df=prices,
            trading_days=calendar,
            factor_columns=["momentum"],
            strategy_code="cross_sectional_momentum_long_only",
            strategy_parameters={
                "top_n": 1,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "rebalance_frequency": "explicit",
            },
            explicit_dates=["2024-01-02"],
            exposure_panel=exposure,
            portfolio_config=config,
            horizons=(1,),
            n_groups=2,
            run_portfolio=False,
        )
        altered_prices = prices.copy()
        # Dramatically change only future prices after signal date.
        altered_prices.loc[
            altered_prices["trade_date"] > "2024-01-02", "close"
        ] = altered_prices.loc[
            altered_prices["trade_date"] > "2024-01-02", "close"
        ] * 3
        changed = run_cross_section_research(
            factor_frame=factors,
            price_df=altered_prices,
            trading_days=calendar,
            factor_columns=["momentum"],
            strategy_code="cross_sectional_momentum_long_only",
            strategy_parameters={
                "top_n": 1,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "rebalance_frequency": "explicit",
            },
            explicit_dates=["2024-01-02"],
            exposure_panel=exposure,
            portfolio_config=config,
            horizons=(1,),
            n_groups=2,
            run_portfolio=False,
        )
        assert [item.to_dict() for item in base.strategy_result.decisions] == [
            item.to_dict() for item in changed.strategy_result.decisions
        ]
        pd.testing.assert_frame_equal(base.target_weights, changed.target_weights)
        # IC / group returns must react to future return changes.
        assert not base.forward_returns.equals(changed.forward_returns)
        base_group = base.group_returns["group_return"].fillna(999.0)
        changed_group = changed.group_returns["group_return"].fillna(999.0)
        assert not base_group.equals(changed_group)

    def test_future_exposure_revision_does_not_pollute_signal(self) -> None:
        calendar, factors, prices, exposure, config = self._fixture()
        schedule = build_rebalance_schedule(
            calendar, frequency="explicit", explicit_dates=["2024-01-02"]
        )
        targets = pd.DataFrame(
            [
                {"trade_date": "2024-01-03", "asset_id": "B", "target_weight": 1.0},
            ]
        )
        base = analyze_target_exposures(
            targets,
            schedule=schedule,
            exposure_panel=exposure,
            categorical_exposures=["industry_code"],
            numeric_exposures=["log_market_cap"],
        )
        revised = exposure.copy()
        revised.loc[len(revised)] = {
            "trade_date": "2024-01-08",
            "asset_id": "B",
            "industry_code": "NEW",
            "log_market_cap": 99.0,
        }
        after = analyze_target_exposures(
            targets,
            schedule=schedule,
            exposure_panel=revised,
            categorical_exposures=["industry_code"],
            numeric_exposures=["log_market_cap"],
        )
        pd.testing.assert_frame_equal(base.numeric, after.numeric)
        pd.testing.assert_frame_equal(base.categorical, after.categorical)

    def test_research_does_not_query_duckdb(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import duckdb

        def _blocked(*_args, **_kwargs):
            raise AssertionError("research must not open DuckDB")

        monkeypatch.setattr(duckdb, "connect", _blocked)
        calendar, factors, prices, exposure, config = self._fixture()
        run_cross_section_research(
            factor_frame=factors,
            price_df=prices,
            trading_days=calendar,
            factor_columns=["momentum"],
            strategy_code="cross_sectional_momentum_long_only",
            strategy_parameters={
                "top_n": 1,
                "max_positions": 1,
                "max_weight_per_asset": 1.0,
                "rebalance_frequency": "explicit",
            },
            explicit_dates=["2024-01-02"],
            exposure_panel=exposure,
            portfolio_config=config,
            horizons=(1,),
            run_portfolio=True,
        )
