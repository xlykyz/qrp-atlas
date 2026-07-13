"""Tests for point-in-time selection of versioned historical records."""

import pandas as pd
import pytest

from qrp_atlas.backtest import select_latest_available_records


def _records() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "report_period": "2024Q1",
                "available_trade_date": "2024-04-15",
                "published_at": "2024-04-14 18:00:00",
                "ingested_at": "2024-04-14 19:00:00",
                "revision_id": "original",
                "value": 10,
            },
            {
                "ticker": "000001.SZ",
                "report_period": "2024Q1",
                "available_trade_date": "2024-05-20",
                "published_at": "2024-05-19 18:00:00",
                "ingested_at": "2024-05-19 19:00:00",
                "revision_id": "corrected",
                "value": 12,
            },
        ]
    )


def test_data_published_after_as_of_date_is_not_returned():
    selected = select_latest_available_records(
        _records(),
        as_of_date="2024-05-01",
        entity_keys=["ticker", "report_period"],
    )

    assert selected["value"].tolist() == [10]
    assert selected["available_trade_date"].tolist() == ["2024-04-15"]


def test_new_revision_does_not_pollute_queries_before_its_effective_date():
    selected = select_latest_available_records(
        _records(),
        as_of_date="2024-05-19",
        entity_keys=["ticker", "report_period"],
    )

    assert selected["revision_id"].tolist() == ["original"]


def test_new_revision_is_selected_on_its_effective_date():
    selected = select_latest_available_records(
        _records(),
        as_of_date="2024-05-20",
        entity_keys=["ticker", "report_period"],
    )

    assert selected["revision_id"].tolist() == ["corrected"]


def test_available_trade_dates_are_compared_as_calendar_days():
    records = pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "available_trade_date": "2024-05-20 18:30:00",
                "value": "effective-that-day",
            }
        ]
    )

    selected = select_latest_available_records(
        records,
        as_of_date="2024-05-20",
        entity_keys="ticker",
    )

    assert selected["value"].tolist() == ["effective-that-day"]


def test_multiple_tickers_and_report_periods_are_selected_independently():
    records = pd.DataFrame(
        [
            {"ticker": "000001.SZ", "report_period": "2024Q1", "available_trade_date": "2024-04-01", "value": 1},
            {"ticker": "000001.SZ", "report_period": "2024Q1", "available_trade_date": "2024-05-01", "value": 2},
            {"ticker": "000001.SZ", "report_period": "2024Q2", "available_trade_date": "2024-05-01", "value": 3},
            {"ticker": "600000.SH", "report_period": "2024Q1", "available_trade_date": "2024-04-15", "value": 4},
            {"ticker": "600000.SH", "report_period": "2024Q1", "available_trade_date": "2024-06-01", "value": 5},
        ]
    )

    selected = select_latest_available_records(
        records,
        as_of_date="2024-05-20",
        entity_keys=["ticker", "report_period"],
    )

    assert selected[["ticker", "report_period", "value"]].to_dict("records") == [
        {"ticker": "000001.SZ", "report_period": "2024Q1", "value": 2},
        {"ticker": "000001.SZ", "report_period": "2024Q2", "value": 3},
        {"ticker": "600000.SH", "report_period": "2024Q1", "value": 4},
    ]


def test_same_available_date_uses_timestamps_then_revision_as_stable_tiebreaker():
    records = pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "available_trade_date": "2024-05-01",
                "published_at": "2024-05-01 09:00:00",
                "ingested_at": "2024-05-01 10:00:00",
                "revision_id": "revision-a",
                "value": "first",
            },
            {
                "ticker": "000001.SZ",
                "available_trade_date": "2024-05-01",
                "published_at": "2024-05-01 10:00:00",
                "ingested_at": "2024-05-01 10:05:00",
                "revision_id": "revision-z",
                "value": "newer-publication",
            },
            {
                "ticker": "600000.SH",
                "available_trade_date": "2024-05-01",
                "published_at": "2024-05-01 09:00:00",
                "ingested_at": "2024-05-01 10:00:00",
                "revision_id": "revision-a",
                "value": "revision-a",
            },
            {
                "ticker": "600000.SH",
                "available_trade_date": "2024-05-01",
                "published_at": "2024-05-01 09:00:00",
                "ingested_at": "2024-05-01 10:00:00",
                "revision_id": "revision-z",
                "value": "revision-z",
            },
        ]
    )

    selected = select_latest_available_records(
        records,
        as_of_date="2024-05-01",
        entity_keys="ticker",
    )

    assert selected[["ticker", "value"]].to_dict("records") == [
        {"ticker": "000001.SZ", "value": "newer-publication"},
        {"ticker": "600000.SH", "value": "revision-z"},
    ]


def test_missing_optional_ordering_columns_still_selects_latest_available_record():
    records = pd.DataFrame(
        [
            {"ticker": "000001.SZ", "available_trade_date": "2024-04-01", "value": 1},
            {"ticker": "000001.SZ", "available_trade_date": "2024-05-01", "value": 2},
        ]
    )

    selected = select_latest_available_records(
        records,
        as_of_date="2024-05-01",
        entity_keys="ticker",
    )

    assert selected["value"].tolist() == [2]


def test_invalid_or_empty_available_dates_are_never_selected():
    records = pd.DataFrame(
        [
            {"ticker": "000001.SZ", "available_trade_date": "not-a-date", "value": "invalid"},
            {"ticker": "000001.SZ", "available_trade_date": None, "value": "missing"},
            {"ticker": "000001.SZ", "available_trade_date": "2024-04-01", "value": "valid"},
            {"ticker": "600000.SH", "available_trade_date": "", "value": "empty"},
        ]
    )

    selected = select_latest_available_records(
        records,
        as_of_date="2024-05-01",
        entity_keys="ticker",
    )

    assert selected[["ticker", "value"]].to_dict("records") == [
        {"ticker": "000001.SZ", "value": "valid"},
    ]


def test_empty_input_preserves_fields_and_returns_reset_index():
    records = pd.DataFrame(
        columns=["ticker", "available_trade_date", "value"],
        index=[8],
    )

    selected = select_latest_available_records(
        records,
        as_of_date="2024-05-01",
        entity_keys="ticker",
    )

    assert selected.empty
    assert selected.columns.tolist() == records.columns.tolist()
    assert selected.index.tolist() == []


@pytest.mark.parametrize(
    ("records", "entity_keys", "as_of_date", "message"),
    [
        (pd.DataFrame({"ticker": ["000001.SZ"]}), "ticker", "2024-05-01", "missing required columns"),
        (pd.DataFrame({"available_trade_date": ["2024-05-01"]}), [], "2024-05-01", "entity_keys must not be empty"),
        (pd.DataFrame({"ticker": ["000001.SZ"], "available_trade_date": ["2024-05-01"]}), "ticker", "not-a-date", "as_of_date"),
    ],
)
def test_invalid_required_inputs_raise_clear_value_errors(records, entity_keys, as_of_date, message):
    with pytest.raises(ValueError, match=message):
        select_latest_available_records(
            records,
            as_of_date=as_of_date,
            entity_keys=entity_keys,
        )


def test_input_dataframe_is_not_modified():
    records = _records()
    original = records.copy(deep=True)

    select_latest_available_records(
        records,
        as_of_date="2024-05-20",
        entity_keys=["ticker", "report_period"],
    )

    pd.testing.assert_frame_equal(records, original)
