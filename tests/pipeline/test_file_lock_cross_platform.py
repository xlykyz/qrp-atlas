"""Cross-platform process file-lock semantics for PIT backfill safety."""

from __future__ import annotations

import multiprocessing as mp
import textwrap
import time
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.pipeline.pit_backfill.safety import (
    FileLock,
    ensure_load_backup,
    load_backup_marker,
    pipeline_db_lock,
)


def _worker_hold(lock_path: str, ready_q, release_q, held_q, timeout_s: float | None = None):
    lock = FileLock(lock_path, timeout_s=timeout_s)
    lock.acquire()
    held_q.put("held")
    ready_q.put("ready")
    release_q.get()
    lock.release()
    held_q.put("released")


def test_lock_acquire_and_release(tmp_path: Path):
    lock_path = tmp_path / "locks" / "db.lock"
    with pipeline_db_lock(lock_path, timeout_s=2) as lock:
        assert lock.path == lock_path
        assert lock_path.exists()
    # re-acquire after release
    with pipeline_db_lock(lock_path, timeout_s=2):
        pass


def test_lock_creates_parent_directory(tmp_path: Path):
    lock_path = tmp_path / "nested" / "a" / "b.lock"
    with FileLock(lock_path, timeout_s=1):
        assert lock_path.parent.is_dir()


def test_cross_process_mutual_exclusion_and_timeout(tmp_path: Path):
    lock_path = tmp_path / "cross.lock"
    ctx = mp.get_context("spawn")
    ready_q = ctx.Queue()
    release_q = ctx.Queue()
    held_q = ctx.Queue()
    proc = ctx.Process(
        target=_worker_hold,
        args=(str(lock_path), ready_q, release_q, held_q, None),
    )
    proc.start()
    try:
        assert ready_q.get(timeout=10) == "ready"
        # Second independent process-level acquisition must time out while held.
        with pytest.raises(TimeoutError, match="Could not acquire lock"):
            FileLock(lock_path, timeout_s=0.5).acquire()
    finally:
        release_q.put("release")
        proc.join(timeout=10)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
    assert held_q.get(timeout=5) == "held"
    assert held_q.get(timeout=5) == "released"

    # After first holder releases, second acquirer succeeds.
    with FileLock(lock_path, timeout_s=2):
        pass


def test_second_holder_succeeds_after_first_releases(tmp_path: Path):
    lock_path = tmp_path / "seq.lock"
    ctx = mp.get_context("spawn")
    ready_q = ctx.Queue()
    release_q = ctx.Queue()
    held_q = ctx.Queue()
    proc = ctx.Process(
        target=_worker_hold,
        args=(str(lock_path), ready_q, release_q, held_q, None),
    )
    proc.start()
    try:
        ready_q.get(timeout=10)
        release_q.put("release")
        proc.join(timeout=10)
    finally:
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
    with pipeline_db_lock(lock_path, timeout_s=2):
        pass


def test_context_exception_still_releases_lock(tmp_path: Path):
    lock_path = tmp_path / "exc.lock"
    with pytest.raises(RuntimeError, match="boom"):
        with pipeline_db_lock(lock_path, timeout_s=2):
            raise RuntimeError("boom")
    # Must be acquirable after exception path.
    with pipeline_db_lock(lock_path, timeout_s=2):
        pass


def test_ensure_load_backup_marker_reuse_windows(tmp_path: Path):
    db = tmp_path / "quant.db"
    con = duckdb.connect(str(db))
    con.execute("create table t(i int); insert into t values (1)")
    con.close()
    state = tmp_path / "state"
    info1 = ensure_load_backup(db, state_dir=state, tag="t1", lock_path=tmp_path / "lock")
    assert Path(info1["backup_path"]).exists()
    assert info1["reused"] is False
    info2 = ensure_load_backup(db, state_dir=state, tag="t1", lock_path=tmp_path / "lock")
    assert info2["reused"] is True
    assert info2["backup_path"] == info1["backup_path"]
    marker = load_backup_marker(state)
    assert marker["tag"] == "t1"
    c = duckdb.connect(info1["backup_path"], read_only=True)
    assert c.execute("select count(*) from t").fetchone()[0] == 1
    c.close()


def test_manifest_lock_cross_platform(tmp_path: Path):
    from qrp_atlas.pipeline.pit_backfill.manifest import ManifestStore, BatchRecord, STATUS_PENDING

    store = ManifestStore(tmp_path / "manifest.jsonl")
    store.upsert(BatchRecord(batch_id="b1", dataset="d", key="k"))
    rec = store.get("b1")
    assert rec is not None
    assert rec.status == STATUS_PENDING
