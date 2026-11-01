from datetime import date
import hashlib

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import get_table
from qrp_atlas.pipeline.lake.migration import import_legacy
from qrp_atlas.pipeline.lake.store import DataPortal, DuckLakeStore, LakeLocation


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def setup(tmp_path):
    source = tmp_path / "old.duckdb"
    with duckdb.connect(str(source)) as con:
        con.execute(get_table("trading_calendar").duckdb_create_sql())
        con.execute("INSERT INTO trading_calendar VALUES ('2026-09-29',true,2026,9,3), ('2026-09-30',true,2026,9,3)")
    store = DuckLakeStore(LakeLocation(str(tmp_path / "catalog.ducklake"), tmp_path / "lake"))
    store.initialize()
    return source, store


def test_legacy_dry_run_preserves_source_and_leaves_destination_unpublished(tmp_path):
    source, store = setup(tmp_path)
    before = digest(source)
    reports = import_legacy(source, store, "trading_calendar", start=date(2026, 9, 29), end=date(2026, 9, 30))
    assert [row["rows"] for row in reports] == [1, 1]
    assert all(row["dry_run"] for row in reports)
    assert digest(source) == before
    with store.connect() as con:
        assert con.execute("SELECT count(*) FROM lake._qrp_publications").fetchone()[0] == 0


def test_legacy_import_retries_without_duplicate_rows_and_retains_pit_limit(tmp_path):
    source, store = setup(tmp_path)
    before = digest(source)
    first = import_legacy(source, store, "trading_calendar", start=date(2026, 9, 29), end=date(2026, 9, 30), dry_run=False)
    second = import_legacy(source, store, "trading_calendar", start=date(2026, 9, 29), end=date(2026, 9, 30), dry_run=False)
    assert [row["publication_id"] for row in first] == [row["publication_id"] for row in second]
    assert digest(source) == before
    portal = DataPortal(store, first[-1]["snapshot_id"])
    assert len(portal.read("trading_calendar")) == 2
    with pytest.raises(ValueError, match="PIT"):
        portal.read("trading_calendar", strict_pit=True)


def test_legacy_reference_needs_one_explicit_label_and_never_claims_historical_knowledge(tmp_path):
    source, store = setup(tmp_path)
    with duckdb.connect(str(source)) as con:
        con.execute(get_table("stock_info").duckdb_create_sql())
        con.execute("INSERT INTO stock_info(ticker,ts_code,name,list_date,is_active) VALUES ('000001.SZ','000001.SZ','Example','1991-04-03',true)")
    with pytest.raises(ValueError, match="one explicit"):
        import_legacy(source, store, "stock_info", start=date(2026, 9, 29), end=date(2026, 9, 30))
    result = import_legacy(source, store, "stock_info", start=date(2026, 9, 30), end=date(2026, 9, 30), dry_run=False)
    portal = DataPortal(store, result[0]["snapshot_id"])
    assert portal.read("stock_info").ticker.tolist() == ["000001.SZ"]
    with pytest.raises(ValueError, match="PIT"):
        portal.read("stock_info", strict_pit=True)
