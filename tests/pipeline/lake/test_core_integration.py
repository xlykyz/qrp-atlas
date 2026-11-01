"""Real RawStore -> core -> DuckLake transaction tests in isolated temp paths.

Run with the pinned lake environment after explicitly installing its DuckLake
extension. This validates the core adapter; PostgreSQL multi-process catalog
acceptance is owned by the store integration suite.
"""
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from qrp_atlas.pipeline.lake.core import CoreRunner, CoreQualityError
from qrp_atlas.pipeline.lake.raw import RawStore
from qrp_atlas.pipeline.lake.store import DataPortal, DuckLakeStore, LakeLocation


def test_core_calendar_raw_replay_idempotent_and_revision_pinned(tmp_path):
    raw = RawStore(tmp_path / "raw")
    store = DuckLakeStore(LakeLocation(str(tmp_path / "catalog.ducklake"), tmp_path / "lake"))
    store.initialize()
    runner = CoreRunner(raw, store)
    target = date(2026, 9, 30)
    observed = datetime(2026, 9, 30, 1, tzinfo=timezone.utc)
    batch = raw.save_frame("tushare", "trading_calendar",
                           pd.DataFrame([{"cal_date": "20260930", "is_open": 1}]),
                           parameters={}, complete=True, observed_at=observed)
    first = runner.replay("trading_calendar", batch.batch_id, target_date=target)
    retry = runner.replay("trading_calendar", batch.batch_id, target_date=target)
    assert first.publication_id == retry.publication_id
    assert first.snapshot_id == retry.snapshot_id
    revised = raw.save_frame("tushare", "trading_calendar",
                             pd.DataFrame([{"cal_date": "20260930", "is_open": 0}]),
                             parameters={}, complete=True, observed_at=observed)
    second = runner.replay("trading_calendar", revised.batch_id, target_date=target,
                           expected_previous=first.publication_id)
    old_frame = DataPortal(store, first.snapshot_id).read("trading_calendar")
    new_frame = DataPortal(store, second.snapshot_id).read("trading_calendar")
    assert len(old_frame) == len(new_frame) == 1
    assert bool(old_frame.is_open.iloc[0]) is True
    assert bool(new_frame.is_open.iloc[0]) is False
    with pytest.raises(ValueError, match="PIT"):
        DataPortal(store, first.snapshot_id).read("trading_calendar", strict_pit=True)


def test_partial_raw_never_reaches_ducklake(tmp_path):
    raw = RawStore(tmp_path / "raw")
    store = DuckLakeStore(LakeLocation(str(tmp_path / "catalog.ducklake"), tmp_path / "lake"))
    store.initialize()
    batch = raw.save_frame("tushare", "trading_calendar",
                           pd.DataFrame([{"cal_date": "20260930", "is_open": 1}]),
                           parameters={}, complete=False, status="partial")
    with pytest.raises(CoreQualityError, match="INCOMPLETE_RAW_BATCH"):
        CoreRunner(raw, store).replay("trading_calendar", batch.batch_id, target_date=date(2026, 9, 30))
    with store.connect() as con:
        assert con.execute("SELECT count(*) FROM lake._qrp_publications").fetchone()[0] == 0
