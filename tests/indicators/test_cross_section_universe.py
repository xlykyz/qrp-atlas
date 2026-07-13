"""Tests for historical universe construction (task 04-A)."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import INDEX_COMPONENT_HISTORY, init_database
from qrp_atlas.indicators import (
    HistoricalUniverseRequest,
    build_historical_universe,
    process_cross_section,
    resolve_historical_universe,
)


def _insert_df(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    con.register("tmp_u", df)
    cols = ", ".join(df.columns)
    con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM tmp_u")
    con.unregister("tmp_u")


@pytest.fixture
def index_db(tmp_path: Path) -> Path:
    path = tmp_path / "universe.duckdb"
    con = duckdb.connect(str(path))
    try:
        init_database(con)
        rows = [
            # CSI300 first snapshot available 2024-02-01
            {
                "index_code": "000300.SH",
                "asset_id": "AAA",
                "snapshot_date": date(2024, 2, 1),
                "weight": 0.4,
                "effective_from": date(2024, 2, 1),
                "effective_to": None,
                "available_trade_date": date(2024, 2, 1),
                "source": "tushare",
                "source_record_id": "c1",
                "revision_id": "r1",
                "ingested_at": datetime(2024, 2, 1, 1, 0, 0),
            },
            {
                "index_code": "000300.SH",
                "asset_id": "BBB",
                "snapshot_date": date(2024, 2, 1),
                "weight": 0.6,
                "effective_from": date(2024, 2, 1),
                "effective_to": None,
                "available_trade_date": date(2024, 2, 1),
                "source": "tushare",
                "source_record_id": "c2",
                "revision_id": "r2",
                "ingested_at": datetime(2024, 2, 1, 1, 0, 0),
            },
            # later snapshot replaces membership
            {
                "index_code": "000300.SH",
                "asset_id": "BBB",
                "snapshot_date": date(2024, 3, 1),
                "weight": 0.5,
                "effective_from": date(2024, 3, 1),
                "effective_to": None,
                "available_trade_date": date(2024, 3, 1),
                "source": "tushare",
                "source_record_id": "c3",
                "revision_id": "r3",
                "ingested_at": datetime(2024, 3, 1, 1, 0, 0),
            },
            {
                "index_code": "000300.SH",
                "asset_id": "CCC",
                "snapshot_date": date(2024, 3, 1),
                "weight": 0.5,
                "effective_from": date(2024, 3, 1),
                "effective_to": None,
                "available_trade_date": date(2024, 3, 1),
                "source": "tushare",
                "source_record_id": "c4",
                "revision_id": "r4",
                "ingested_at": datetime(2024, 3, 1, 1, 0, 0),
            },
            # different index must stay isolated
            {
                "index_code": "000905.SH",
                "asset_id": "ZZZ",
                "snapshot_date": date(2024, 2, 1),
                "weight": 1.0,
                "effective_from": date(2024, 2, 1),
                "effective_to": None,
                "available_trade_date": date(2024, 2, 1),
                "source": "tushare",
                "source_record_id": "z1",
                "revision_id": "rz1",
                "ingested_at": datetime(2024, 2, 1, 1, 0, 0),
            },
        ]
        _insert_df(con, INDEX_COMPONENT_HISTORY.name, pd.DataFrame(rows))
    finally:
        con.close()
    return path


def test_explicit_asset_list_and_multi_date() -> None:
    out = build_historical_universe(
        ["2024-01-02", "2024-01-03"],
        asset_ids=["B", "A", "B"],
        source="explicit",
    )
    assert out[["trade_date", "asset_id"]].values.tolist() == [
        ["2024-01-02", "A"],
        ["2024-01-02", "B"],
        ["2024-01-03", "A"],
        ["2024-01-03", "B"],
    ]


def test_empty_asset_list_does_not_expand() -> None:
    out = build_historical_universe(["2024-01-02"], asset_ids=[], source="explicit")
    assert out.empty
    assert list(out.columns) == ["trade_date", "asset_id"]


def test_index_before_first_snapshot_is_empty(index_db: Path) -> None:
    out = build_historical_universe(
        "2024-01-31",
        index_code="000300.SH",
        db_path=index_db,
    )
    assert out.empty


def test_index_snapshot_switch_boundary(index_db: Path) -> None:
    first = build_historical_universe(
        "2024-02-01",
        index_code="000300.SH",
        db_path=index_db,
    )
    mid = build_historical_universe(
        "2024-02-20",
        index_code="000300.SH",
        db_path=index_db,
    )
    second = build_historical_universe(
        "2024-03-01",
        index_code="000300.SH",
        db_path=index_db,
    )
    assert set(first["asset_id"]) == {"AAA", "BBB"}
    assert set(mid["asset_id"]) == {"AAA", "BBB"}
    assert set(second["asset_id"]) == {"BBB", "CCC"}
    # no future leakage into earlier date
    assert "CCC" not in set(first["asset_id"])
    assert "CCC" not in set(mid["asset_id"])
    assert "AAA" not in set(second["asset_id"])


def test_multi_index_isolation(index_db: Path) -> None:
    csi = build_historical_universe("2024-02-01", index_code="000300.SH", db_path=index_db)
    zz = build_historical_universe("2024-02-01", index_code="000905.SH", db_path=index_db)
    assert set(csi["asset_id"]) == {"AAA", "BBB"}
    assert set(zz["asset_id"]) == {"ZZZ"}
    assert set(csi["index_code"]) == {"000300.SH"}
    assert set(zz["index_code"]) == {"000905.SH"}


def test_multi_date_index_universe_stable_sort(index_db: Path) -> None:
    out = build_historical_universe(
        ["2024-03-01", "2024-02-01"],
        index_code="000300.SH",
        db_path=index_db,
    )
    keys = out[["trade_date", "asset_id"]].astype(str).values.tolist()
    assert keys == sorted(keys)
    # both dates present
    assert out["trade_date"].nunique() == 2


def test_resolve_request_and_no_future_components(index_db: Path) -> None:
    request = HistoricalUniverseRequest(
        trade_dates=["2024-02-15"],
        source="index",
        index_code="000300.SH",
    )
    out = resolve_historical_universe(request, db_path=index_db)
    assert set(out["asset_id"]) == {"AAA", "BBB"}


def test_process_cross_section_joins_universe(index_db: Path) -> None:
    features = pd.DataFrame(
        [
            {"trade_date": "2024-02-01", "asset_id": "AAA", "momentum": 0.1},
            {"trade_date": "2024-02-01", "asset_id": "BBB", "momentum": 0.3},
            # future-only asset must not enter earlier universe through features alone
            {"trade_date": "2024-02-01", "asset_id": "CCC", "momentum": 0.9},
            {"trade_date": "2024-03-01", "asset_id": "BBB", "momentum": 0.2},
            {"trade_date": "2024-03-01", "asset_id": "CCC", "momentum": 0.4},
        ]
    )
    original = features.copy(deep=True)
    out = process_cross_section(
        features,
        feature_columns="momentum",
        trade_dates=["2024-02-01", "2024-03-01"],
        index_code="000300.SH",
        operators=("rank",),
        db_path=index_db,
    )
    pd.testing.assert_frame_equal(features, original)
    day1 = out[out["trade_date"] == "2024-02-01"]
    assert set(day1["asset_id"]) == {"AAA", "BBB"}
    assert "CCC" not in set(day1["asset_id"])
    assert day1.loc[day1["asset_id"] == "AAA", "momentum_rank"].iloc[0] == 1.0
    assert day1.loc[day1["asset_id"] == "BBB", "momentum_rank"].iloc[0] == 2.0
