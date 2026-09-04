"""Acceptance tests for the pure M5 popularity fact calculation."""

from __future__ import annotations

from datetime import date

import pandas as pd

from qrp_atlas.indicators.m5 import calculate_m5_raw_observations


TARGET = date(2026, 3, 2)


def _members() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"theme_id": "THM:A", "collection_id": "COL:A", "asset_id": "000001.SZ", "trade_date": TARGET},
            {"theme_id": "THM:A", "collection_id": "COL:A", "asset_id": "000002.SZ", "trade_date": TARGET},
            # An accidental duplicate membership must not multiply a record.
            {"theme_id": "THM:A", "collection_id": "COL:A", "asset_id": "000002.SZ", "trade_date": TARGET},
            {"theme_id": "THM:B", "collection_id": "COL:B", "asset_id": "000001.SZ", "trade_date": TARGET},
            {"theme_id": "THM:B", "collection_id": "COL:B", "asset_id": "000003.SZ", "trade_date": TARGET},
        ]
    )


def test_m5_uses_all_records_and_maps_one_record_to_every_theme() -> None:
    members = _members()
    popularity = pd.DataFrame(
        [
            {"ticker": "000001.SZ", "source": "EASTMONEY", "trade_date": TARGET, "snapshot_seq": 1},
            {"ticker": "000001.SZ", "source": "EASTMONEY", "trade_date": TARGET, "snapshot_seq": 2},
            {"ticker": "000002.SZ", "source": "THS", "trade_date": TARGET, "snapshot_seq": 1},
            {"ticker": "000004.SZ", "source": "THS", "trade_date": TARGET, "snapshot_seq": 1},
        ]
    )
    universe = pd.DataFrame(
        [
            {"theme_id": "THM:A", "collection_id": "COL:A"},
            {"theme_id": "THM:B", "collection_id": "COL:B"},
        ]
    )

    result = calculate_m5_raw_observations(members, popularity, universe, trade_date=TARGET)
    a = result[result.theme_id == "THM:A"].iloc[0]
    b = result[result.theme_id == "THM:B"].iloc[0]

    assert int(a.theme_member_count) == 2
    assert int(a.theme_hot_stock_count) == 2
    assert a.theme_hot_stock_ratio == 1.0
    assert int(a.theme_hot_list_appearance_count) == 3
    assert int(a.theme_hot_source_count) == 2
    assert int(b.theme_member_count) == 2
    assert int(b.theme_hot_stock_count) == 1
    assert b.theme_hot_stock_ratio == 0.5
    assert int(b.theme_hot_list_appearance_count) == 2
    assert int(b.theme_hot_source_count) == 1


def test_m5_keeps_zero_hit_themes_and_distinguishes_zero_member_ratio() -> None:
    members = pd.DataFrame(
        [
            {"theme_id": "THM:HIT_NONE", "collection_id": "COL:HIT_NONE", "asset_id": "000010.SZ", "trade_date": TARGET},
        ]
    )
    popularity = pd.DataFrame(
        [{"ticker": "000099.SZ", "source": "EASTMONEY", "trade_date": TARGET}]
    )
    universe = pd.DataFrame(
        [
            {"theme_id": "THM:HIT_NONE", "collection_id": "COL:HIT_NONE"},
            {"theme_id": "THM:EMPTY", "collection_id": "COL:EMPTY"},
        ]
    )

    result = calculate_m5_raw_observations(members, popularity, universe, trade_date=TARGET)
    no_hit = result[result.theme_id == "THM:HIT_NONE"].iloc[0]
    empty = result[result.theme_id == "THM:EMPTY"].iloc[0]
    assert int(no_hit.theme_member_count) == 1
    assert int(no_hit.theme_hot_stock_count) == 0
    assert no_hit.theme_hot_stock_ratio == 0.0
    assert int(no_hit.theme_hot_list_appearance_count) == 0
    assert int(no_hit.theme_hot_source_count) == 0
    assert int(empty.theme_member_count) == 0
    assert empty.theme_hot_stock_ratio is None


def test_m5_does_not_apply_m4_effective_member_filters_and_does_not_mutate_inputs() -> None:
    members = pd.DataFrame(
        [
            {
                "theme_id": "THM:NEW_SUSPENDED",
                "collection_id": "COL:NEW_SUSPENDED",
                "asset_id": "000020.SZ",
                "trade_date": TARGET,
                "is_theme_member": True,
                "is_m4_effective_member": False,
            }
        ]
    )
    popularity = pd.DataFrame(
        [{"ticker": "20", "source": "THS", "trade_date": TARGET}]
    )
    before_members = members.copy(deep=True)
    before_popularity = popularity.copy(deep=True)

    result = calculate_m5_raw_observations(members, popularity, trade_date=TARGET)
    row = result.iloc[0]
    assert int(row.theme_member_count) == 1
    assert int(row.theme_hot_stock_count) == 1
    assert row.theme_hot_stock_ratio == 1.0
    pd.testing.assert_frame_equal(members, before_members)
    pd.testing.assert_frame_equal(popularity, before_popularity)
