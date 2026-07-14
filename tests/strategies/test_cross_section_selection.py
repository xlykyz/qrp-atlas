"""Tests for rebalance calendars, eligibility, Top-N and equal-weight targets."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from qrp_atlas.strategies import (
    EligibilityError,
    RebalanceScheduleError,
    SelectionError,
    WeightConstructionError,
    apply_eligibility,
    build_rebalance_schedule,
    equal_weight_targets,
    select_top_n,
    selection_to_target_weights,
)
from qrp_atlas.strategies.selection import next_trading_day


def _dates(*values: str) -> list[str]:
    return list(values)


class TestRebalanceSchedule:
    def test_daily_weekly_monthly_and_explicit(self) -> None:
        calendar = _dates(
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-31",
            "2024-02-01",
        )
        daily = build_rebalance_schedule(calendar, frequency="daily")
        assert daily["signal_date"].dt.strftime("%Y-%m-%d").tolist() == [
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-31",
        ]
        assert daily["trade_date"].dt.strftime("%Y-%m-%d").tolist() == [
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-31",
            "2024-02-01",
        ]

        weekly = build_rebalance_schedule(calendar, frequency="weekly")
        # 2024-01-31 and 2024-02-01 share ISO week 5; last trading day is 02-01,
        # which has no next trading day and therefore produces no schedule row.
        assert weekly["signal_date"].dt.strftime("%Y-%m-%d").tolist() == [
            "2024-01-05",
            "2024-01-09",
        ]
        assert weekly["trade_date"].dt.strftime("%Y-%m-%d").tolist() == [
            "2024-01-08",
            "2024-01-31",
        ]

        monthly = build_rebalance_schedule(calendar, frequency="monthly")
        assert monthly["signal_date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-31"]
        assert monthly["trade_date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-02-01"]

        explicit = build_rebalance_schedule(
            calendar,
            frequency="explicit",
            explicit_dates=["2024-01-04", "2024-01-09"],
        )
        assert explicit["signal_date"].dt.strftime("%Y-%m-%d").tolist() == [
            "2024-01-04",
            "2024-01-09",
        ]
        assert explicit["trade_date"].dt.strftime("%Y-%m-%d").tolist() == [
            "2024-01-05",
            "2024-01-31",
        ]

    def test_skips_weekend_gaps_and_terminal_signal(self) -> None:
        calendar = ["2024-01-05", "2024-01-08", "2024-01-09"]
        schedule = build_rebalance_schedule(calendar, frequency="daily")
        assert schedule["signal_date"].dt.strftime("%Y-%m-%d").tolist() == [
            "2024-01-05",
            "2024-01-08",
        ]
        assert schedule["trade_date"].dt.strftime("%Y-%m-%d").tolist() == [
            "2024-01-08",
            "2024-01-09",
        ]
        assert next_trading_day(calendar, "2024-01-09") is None

    def test_unordered_duplicates_and_invalid_dates(self) -> None:
        calendar = [
            "2024-01-05",
            "2024-01-02",
            "2024-01-02",
            "2024-01-03",
        ]
        schedule = build_rebalance_schedule(calendar, frequency="daily")
        assert schedule["signal_date"].dt.strftime("%Y-%m-%d").tolist() == [
            "2024-01-02",
            "2024-01-03",
        ]
        with pytest.raises(RebalanceScheduleError):
            build_rebalance_schedule(["not-a-date"], frequency="daily")

    def test_explicit_missing_date_fails(self) -> None:
        with pytest.raises(RebalanceScheduleError, match="must exist"):
            build_rebalance_schedule(
                ["2024-01-02", "2024-01-03"],
                frequency="explicit",
                explicit_dates=["2024-01-04"],
            )

    def test_future_calendar_extension_does_not_change_history(self) -> None:
        base = ["2024-01-02", "2024-01-03", "2024-01-04"]
        first = build_rebalance_schedule(base, frequency="daily")
        extended = build_rebalance_schedule(
            base + ["2024-01-05", "2024-01-08"],
            frequency="daily",
            end_date="2024-01-04",
        )
        # Historical signal->execution pairs through 01-03 remain identical.
        hist = extended[extended["signal_date"] <= pd.Timestamp("2024-01-03")]
        assert hist["signal_date"].tolist() == first["signal_date"].tolist()
        assert hist["trade_date"].tolist() == first["trade_date"].tolist()


class TestEligibilityAndTopN:
    def _frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 4 + ["2024-01-03"] * 3,
                "asset_id": ["B", "A", "C", "D", "A", "B", "C"],
                "score": [1.0, 2.0, 3.0, math.nan, 5.0, float("inf"), 4.0],
            }
        )

    def test_descending_ascending_and_stable_ties(self) -> None:
        frame = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 3,
                "asset_id": ["B", "A", "C"],
                "score": [2.0, 2.0, 1.0],
            }
        )
        selected = select_top_n(frame, n=2)
        assert selected.loc[selected["selected"], "asset_id"].tolist() == ["A", "B"]
        ascending = select_top_n(frame, n=1, ascending=True)
        assert ascending.loc[ascending["selected"], "asset_id"].tolist() == ["C"]

    def test_input_order_independence(self) -> None:
        frame = self._frame().sample(frac=1, random_state=7)
        one = select_top_n(frame, n=2)
        two = select_top_n(self._frame().sample(frac=1, random_state=99), n=2)
        cols = ["trade_date", "asset_id", "score", "rank", "selected"]
        left = one[cols].assign(
            trade_date=lambda df: pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
        )
        right = two[cols].assign(
            trade_date=lambda df: pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
        )
        pd.testing.assert_frame_equal(left.reset_index(drop=True), right.reset_index(drop=True))

    def test_nan_inf_and_eligibility_filters(self) -> None:
        eligibility = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 3,
                "asset_id": ["A", "B", "C"],
                "eligible": [True, False, True],
                "reason_code": ["OK", "ST", "OK"],
            }
        )
        frame = pd.DataFrame(
            {
                "trade_date": ["2024-01-02"] * 4,
                "asset_id": ["A", "B", "C", "D"],
                "score": [1.0, 9.0, math.nan, 5.0],
            }
        )
        selected = select_top_n(frame, n=2, eligibility=eligibility)
        winners = selected.loc[selected["selected"], "asset_id"].tolist()
        assert winners == ["A"]
        by_asset = selected.set_index("asset_id")
        assert bool(by_asset.loc["B", "selected"]) is False
        assert by_asset.loc["C", "reason_code"] == "INVALID_SCORE"
        assert by_asset.loc["D", "reason_code"] == "MISSING_ELIGIBILITY"

    def test_fewer_than_n_and_empty_universe(self) -> None:
        frame = pd.DataFrame(
            {
                "trade_date": ["2024-01-02", "2024-01-02"],
                "asset_id": ["A", "B"],
                "score": [1.0, 2.0],
            }
        )
        selected = select_top_n(frame, n=5)
        assert selected["selected"].sum() == 2
        empty = select_top_n(
            pd.DataFrame(columns=["trade_date", "asset_id", "score"]),
            n=3,
        )
        assert list(empty.columns)[:5] == [
            "trade_date",
            "asset_id",
            "score",
            "rank",
            "selected",
        ]
        assert empty.empty

    def test_dates_are_isolated(self) -> None:
        selected = select_top_n(self._frame(), n=1)
        day1 = selected[
            (selected["trade_date"] == pd.Timestamp("2024-01-02")) & selected["selected"]
        ]
        day2 = selected[
            (selected["trade_date"] == pd.Timestamp("2024-01-03")) & selected["selected"]
        ]
        assert day1["asset_id"].tolist() == ["C"]
        assert day2["asset_id"].tolist() == ["A"]

    def test_apply_eligibility_defaults(self) -> None:
        frame = pd.DataFrame(
            {
                "trade_date": ["2024-01-02", "2024-01-02"],
                "asset_id": ["A", "B"],
                "score": [1.0, math.nan],
            }
        )
        annotated = apply_eligibility(frame, score_column="score")
        assert annotated["eligible"].tolist() == [True, True]
        assert annotated["selection_eligible"].tolist() == [True, False]


class TestWeights:
    def test_equal_weight_cash_buffer_and_cap(self) -> None:
        weights = equal_weight_targets(
            ["A", "B"],
            trade_date="2024-01-03",
            scores={"A": 2.0, "B": 1.0},
            max_weight_per_asset=1.0,
            cash_buffer=0.2,
        )
        assert weights["target_weight"].tolist() == pytest.approx([0.4, 0.4])
        assert float(weights["target_weight"].sum()) == pytest.approx(0.8)

        capped = equal_weight_targets(
            ["A", "B", "C"],
            trade_date="2024-01-03",
            max_weight_per_asset=0.2,
            cash_buffer=0.0,
        )
        assert capped["target_weight"].tolist() == pytest.approx([0.2, 0.2, 0.2])
        assert float(capped["target_weight"].sum()) == pytest.approx(0.6)

    def test_max_positions_and_zero_targets_for_exits(self) -> None:
        selection = pd.DataFrame(
            {
                "trade_date": ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
                "asset_id": ["A", "B", "B", "C"],
                "score": [3.0, 2.0, 2.0, 4.0],
                "rank": [1, 2, 2, 1],
                "selected": [True, True, True, True],
            }
        )
        schedule = pd.DataFrame(
            {
                "signal_date": ["2024-01-02", "2024-01-03"],
                "trade_date": ["2024-01-03", "2024-01-04"],
            }
        )
        targets = selection_to_target_weights(
            selection,
            signal_to_trade=schedule,
            max_positions=2,
            max_weight_per_asset=0.5,
            cash_buffer=0.0,
        )
        day1 = targets[targets["trade_date"] == "2024-01-03"]
        day2 = targets[targets["trade_date"] == "2024-01-04"]
        assert set(day1["asset_id"]) == {"A", "B"}
        assert day1["target_weight"].tolist() == pytest.approx([0.5, 0.5])
        assert day2.set_index("asset_id")["target_weight"].to_dict() == pytest.approx(
            {"A": 0.0, "B": 0.5, "C": 0.5}
        )

    def test_empty_selection_clears_previous(self) -> None:
        selection = pd.DataFrame(
            {
                "trade_date": ["2024-01-02", "2024-01-03"],
                "asset_id": ["A", "A"],
                "score": [1.0, 1.0],
                "rank": [1, 1],
                "selected": [True, False],
            }
        )
        schedule = pd.DataFrame(
            {
                "signal_date": ["2024-01-02", "2024-01-03"],
                "trade_date": ["2024-01-03", "2024-01-04"],
            }
        )
        targets = selection_to_target_weights(
            selection,
            signal_to_trade=schedule,
            max_weight_per_asset=1.0,
        )
        day2 = targets[targets["trade_date"] == "2024-01-04"]
        assert day2.to_dict("records") == [
            {
                "trade_date": "2024-01-04",
                "asset_id": "A",
                "target_weight": 0.0,
                "priority": 0.0,
            }
        ]

    def test_weight_validation_bounds(self) -> None:
        with pytest.raises(WeightConstructionError):
            equal_weight_targets(["A"], trade_date="2024-01-02", cash_buffer=1.0)
        with pytest.raises(WeightConstructionError):
            equal_weight_targets(["A"], trade_date="2024-01-02", max_weight_per_asset=0.0)
        with pytest.raises(SelectionError):
            select_top_n(
                pd.DataFrame(
                    {"trade_date": ["2024-01-02"], "asset_id": ["A"], "score": [1.0]}
                ),
                n=0,
            )
        with pytest.raises(EligibilityError):
            apply_eligibility(
                pd.DataFrame({"trade_date": ["2024-01-02"], "asset_id": ["A"]}),
                score_column="score",
            )
