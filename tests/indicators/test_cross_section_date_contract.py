"""Additional regressions for trade_date timezone and de-duplication."""

from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import INDEX_COMPONENT_HISTORY, init_database
from qrp_atlas.indicators import (
    CrossSectionFrameError,
    HistoricalUniverseRequest,
    build_historical_universe,
    normalize_trade_date,
    normalize_trade_dates,
    process_cross_section,
    resolve_historical_universe,
)


def _day(value: str) -> pd.Timestamp:
    return normalize_trade_date(value)


def _insert_df(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    con.register("tmp_tz", df)
    cols = ", ".join(df.columns)
    con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM tmp_tz")
    con.unregister("tmp_tz")


@pytest.fixture
def index_db(tmp_path: Path) -> Path:
    path = tmp_path / "tz_universe.duckdb"
    con = duckdb.connect(str(path))
    try:
        init_database(con)
        rows = [
            {
                "index_code": "000300.SH",
                "asset_id": "AAA",
                "snapshot_date": date(2024, 1, 2),
                "weight": 0.5,
                "effective_from": date(2024, 1, 2),
                "effective_to": None,
                "available_trade_date": date(2024, 1, 2),
                "source": "tushare",
                "source_record_id": "t1",
                "revision_id": "rt1",
                "ingested_at": datetime(2024, 1, 2, 1, 0, 0),
            },
            {
                "index_code": "000300.SH",
                "asset_id": "BBB",
                "snapshot_date": date(2024, 1, 2),
                "weight": 0.5,
                "effective_from": date(2024, 1, 2),
                "effective_to": None,
                "available_trade_date": date(2024, 1, 2),
                "source": "tushare",
                "source_record_id": "t2",
                "revision_id": "rt2",
                "ingested_at": datetime(2024, 1, 2, 1, 0, 0),
            },
        ]
        _insert_df(con, INDEX_COMPONENT_HISTORY.name, pd.DataFrame(rows))
    finally:
        con.close()
    return path


def test_timezone_aware_keeps_local_wall_date() -> None:
    plus8 = timezone(timedelta(hours=8))
    ts = pd.Timestamp(datetime(2024, 1, 2, 0, 30, tzinfo=plus8))
    # Asia/Shanghai alias path
    ts_named = pd.Timestamp("2024-01-02 00:30:00").tz_localize("Asia/Shanghai")
    assert normalize_trade_date(ts) == _day("2024-01-02")
    assert normalize_trade_date(ts_named) == _day("2024-01-02")
    # Must not shift back to previous UTC day.
    assert normalize_trade_date(ts) != _day("2024-01-01")


def test_timezone_aware_features_merge_with_string_universe(index_db: Path) -> None:
    plus8 = timezone(timedelta(hours=8))
    features = pd.DataFrame(
        [
            {
                "trade_date": pd.Timestamp(datetime(2024, 1, 2, 0, 30, tzinfo=plus8)),
                "asset_id": "AAA",
                "momentum": 0.1,
            },
            {
                "trade_date": pd.Timestamp("2024-01-02 00:30:00").tz_localize("Asia/Shanghai"),
                "asset_id": "BBB",
                "momentum": 0.3,
            },
        ]
    )
    original = features.copy(deep=True)
    out = process_cross_section(
        features,
        feature_columns="momentum",
        trade_dates="2024-01-02",
        index_code="000300.SH",
        operators=("rank",),
        db_path=index_db,
    )
    pd.testing.assert_frame_equal(features, original)
    assert len(out) == 2
    assert (out["trade_date"] == _day("2024-01-02")).all()
    assert set(out["asset_id"]) == {"AAA", "BBB"}
    assert out.loc[out["asset_id"] == "AAA", "momentum_rank"].iloc[0] == 1.0


def test_normalize_trade_dates_dedupes_equivalent_days() -> None:
    values = [
        "2024-01-02",
        date(2024, 1, 2),
        datetime(2024, 1, 2, 15, 0),
        pd.Timestamp("2024-01-02"),
        "2024-01-03",
        pd.Timestamp("2024-01-02 09:00:00"),
    ]
    out = normalize_trade_dates(values)
    assert out == [_day("2024-01-02"), _day("2024-01-03")]


def test_explicit_universe_dedupes_equivalent_dates() -> None:
    out = build_historical_universe(
        [
            "2024-01-02",
            date(2024, 1, 2),
            datetime(2024, 1, 2, 15, 0),
            pd.Timestamp("2024-01-02"),
        ],
        asset_ids=["A", "B"],
        source="explicit",
    )
    assert out["trade_date"].nunique() == 1
    assert len(out) == 2
    assert set(out["asset_id"]) == {"A", "B"}
    keys = out[["trade_date", "asset_id"]].values.tolist()
    assert len(keys) == len({tuple(k) for k in keys})


def test_index_universe_request_dedupes_equivalent_dates(index_db: Path) -> None:
    request = HistoricalUniverseRequest(
        trade_dates=[
            "2024-01-02",
            date(2024, 1, 2),
            datetime(2024, 1, 2, 15, 0),
            pd.Timestamp("2024-01-02"),
        ],
        source="index",
        index_code="000300.SH",
    )
    out = resolve_historical_universe(request, db_path=index_db)
    assert out["trade_date"].nunique() == 1
    assert set(out["asset_id"]) == {"AAA", "BBB"}


def test_process_cross_section_accepts_duplicate_date_inputs_without_pk_error(
    index_db: Path,
) -> None:
    features = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "AAA", "momentum": 0.2},
            {"trade_date": "2024-01-02", "asset_id": "BBB", "momentum": 0.1},
        ]
    )
    out = process_cross_section(
        features,
        feature_columns="momentum",
        trade_dates=[
            "2024-01-02",
            date(2024, 1, 2),
            datetime(2024, 1, 2, 15, 0),
            pd.Timestamp("2024-01-02"),
        ],
        index_code="000300.SH",
        operators=("rank",),
        db_path=index_db,
    )
    assert out["trade_date"].nunique() == 1
    assert set(out["asset_id"]) == {"AAA", "BBB"}


def test_caller_duplicate_feature_still_rejected() -> None:
    features = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "x": 1.0},
            {"trade_date": date(2024, 1, 2), "asset_id": "A", "x": 2.0},
        ]
    )
    with pytest.raises(CrossSectionFrameError, match="duplicate cross-section primary key"):
        process_cross_section(features, feature_columns="x", operators=("rank",))
