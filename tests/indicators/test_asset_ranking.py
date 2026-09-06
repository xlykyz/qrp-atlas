"""Task06-A pure ranking semantics."""

from datetime import date

import pandas as pd
import pytest

from qrp_atlas.indicators.system_b.asset_ranking import (
    AssetRankingError,
    HIGHER_IS_BETTER,
    calculate_asset_ranking,
    normalized_rank_score,
    rank_component,
)


TARGET = date(2026, 1, 10)


def test_normalized_rank_uses_average_business_ties_and_is_order_invariant() -> None:
    first = normalized_rank_score(pd.Series([10.0, 20.0, 20.0, 5.0], index=["A", "B", "C", "D"]))
    second = normalized_rank_score(pd.Series([5.0, 20.0, 10.0, 20.0], index=["D", "C", "A", "B"]))
    assert first.loc["B"] == first.loc["C"] == pytest.approx(83.3333333333)
    assert first.loc["A"] == pytest.approx(33.3333333333)
    assert first.loc["D"] == 0
    pd.testing.assert_series_equal(first.sort_index(), second.sort_index())


@pytest.mark.parametrize(
    ("values", "status"),
    [([1.0], "INSUFFICIENT_UNIVERSE"), ([1.0, 1.0], "NO_VARIATION")],
)
def test_rank_special_cases(values: list[float], status: str) -> None:
    result = rank_component(values, direction=HIGHER_IS_BETTER)
    assert result.status == status
    assert result.scores.isna().all()


def _base_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    tickers = ["A", "B", "C", "D"]
    universe = pd.DataFrame({"trade_date": [TARGET] * 4, "ticker": tickers})
    dates = [date(2026, 1, day) for day in range(1, 11)]
    market = pd.DataFrame(
        [
            {
                "trade_date": day,
                "ticker": ticker,
                "close": float(10 + index + (day.day if ticker == "A" else 0)),
                "amount": float((100, 150, 130, 190)[index] + day.day),
                "market_fact_status": "ACTUAL_TRADING",
            }
            for index, ticker in enumerate(tickers)
            for day in dates
        ]
    )
    episode = pd.DataFrame(
        {"trade_date": [TARGET] * 4, "ticker": tickers, "episode_return": [0.4, 0.3, 0.2, 0.1]}
    )
    memberships = pd.DataFrame(
        {
            "trade_date": [TARGET] * 12,
            "ticker": tickers * 3,
            "pool_type": ["CAPACITY"] * 4 + ["HEIGHT"] * 4 + ["RECOGNITION"] * 4,
            "membership_state": ["IN_POOL"] * 12,
            "metrics_json": ["{}"] * 4
            + [
                f'{{"height_start_date":"2026-01-05","height_since_start_return":{value}}}'
                for value in (0.1, 0.2, 0.3, 0.4)
            ]
            + ["{}"] * 4,
        }
    )
    return universe, market, episode, memberships


def test_asset_ranking_materializes_all_dimensions_and_uses_shift_four_and_nine() -> None:
    universe, market, episode, memberships = _base_inputs()
    hot_tickers = ["A", "B", "C", "D"] + [f"other-{index:03d}" for index in range(96)]
    popularity = {
        source: pd.DataFrame(
            {
                "trade_date": [TARGET] * 100,
                "ticker": hot_tickers,
                "snapshot_seq": [1] * 100,
                "rank_position": list(range(1, 101)),
            }
        )
        for source in ("dc_hot", "ths_hot")
    }
    result = calculate_asset_ranking(
        universe,
        trade_date=TARGET,
        market_series=market,
        episode_observations=episode,
        memberships=memberships,
        popularity=popularity,
        popularity_availability={
            "dc_hot": {"source_status": "AVAILABLE"},
            "ths_hot": {"source_status": "AVAILABLE"},
        },
    )
    assert len(result.snapshot) == 4
    assert len(result.component_audit) == 4 * 7
    assert set(result.snapshot["m1_status"]) == {"OK"}
    # Height metrics are present for all four members and a complete market
    # history gives the price windows needed by Recognition.
    assert set(result.snapshot["m2_status"]) == {"OK"}
    assert set(result.snapshot["m3_status"]) == {"OK"}
    m3 = result.component_audit.query("dimension == 'M3'")
    assert set(m3["component"]) == {
        "episode_return",
        "return5",
        "return10",
        "popularity",
    }


def test_expected_popularity_unavailable_only_degrades_m3() -> None:
    universe, market, episode, memberships = _base_inputs()
    result = calculate_asset_ranking(
        universe,
        trade_date=TARGET,
        market_series=market,
        episode_observations=episode,
        memberships=memberships,
        popularity={
            "dc_hot": pd.DataFrame(),
            "ths_hot": pd.DataFrame(
                {
                    "trade_date": [TARGET] * 100,
                    "ticker": ["A", "B", "C", "D"] + [f"other-{index:03d}" for index in range(96)],
                    "snapshot_seq": [1] * 100,
                    "rank_position": list(range(1, 101)),
                }
            ),
        },
        popularity_availability={
            "dc_hot": {"source_status": "UNAVAILABLE", "input_version": "dc-v1"},
            "ths_hot": {"source_status": "AVAILABLE", "input_version": "ths-v1"},
        },
    )
    assert set(result.snapshot["m1_status"]) == {"OK"}
    assert set(result.snapshot["m2_status"]) == {"OK"}
    assert set(result.snapshot["m3_status"]) == {"INCOMPLETE_COMPONENTS"}
    assert result.snapshot["m3_score"].isna().all()
    assert result.diagnostics == ("DC_HOT_SOURCE_UNAVAILABLE",)


def test_short_top100_is_rejected() -> None:
    with pytest.raises(AssetRankingError, match="POPULARITY_INCOMPLETE_TOP100"):
        from qrp_atlas.indicators.system_b.asset_ranking import calculate_popularity_scores

        calculate_popularity_scores(
            {"dc_hot": pd.DataFrame({"ticker": ["A"], "rank_position": [1]})},
            target_date=TARGET,
            availability={"dc_hot": {"source_status": "AVAILABLE"}, "ths_hot": {"source_status": "UNAVAILABLE"}},
        )
