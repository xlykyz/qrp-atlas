from datetime import date
from pathlib import Path
import os
import uuid

import pandas as pd
import pytest

from qrp_atlas.contracts.schema import ColumnSpec, TableSchema
from qrp_atlas.pipeline.lake.store import DataPortal, DuckLakeStore, LakeLocation
from qrp_atlas.pipeline.lake.raw import RawStore

SCHEMA = TableSchema("prices", (ColumnSpec("trade_date", "DATE", False), ColumnSpec("ticker", "VARCHAR", False), ColumnSpec("close", "DOUBLE", False)), ("trade_date", "ticker"))


@pytest.fixture
def store(tmp_path):
    # Same DuckLake engine, embedded metadata for isolated single-client tests.
    result = DuckLakeStore(LakeLocation(str(tmp_path / "catalog.ducklake"), tmp_path))
    result.initialize()
    return result


@pytest.fixture
def pg_store(tmp_path):
    """Real multi-client catalog; never use an existing application database.

Set QRP_TEST_POSTGRES_DSN to an isolated loopback PostgreSQL test instance.
Each test creates and drops only its own random database.
"""
    dsn = os.environ.get("QRP_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("isolated PostgreSQL DSN is not configured")
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo

    database = "qrp_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        if admin.info.host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("PostgreSQL integration tests require a loopback test instance")
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        try:
            result = DuckLakeStore(LakeLocation("postgres:" + make_conninfo(dsn, dbname=database), tmp_path))
            result.initialize()
            yield result
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))


def publish(store, key="v1", price=10, **kwargs):
    kwargs.setdefault("expected_previous", store.current_publication("prices", "2026-09-30"))
    return store.publish(SCHEMA, pd.DataFrame({"trade_date": [date(2026, 9, 30)], "ticker": ["000001.SZ"], "close": [float(price)]}), partition="2026-09-30", idempotency_key=key, lineage={"raw_batch_id": "recorded"}, **kwargs)


def test_revisions_and_idempotent_recovery(store):
    a = publish(store)
    assert publish(store) == a
    b = publish(store, "v2", 11)
    assert b.snapshot_id > a.snapshot_id
    assert DataPortal(store, a.snapshot_id).read("prices").close.tolist() == [10]
    assert DataPortal(store, b.snapshot_id).read("prices").close.tolist() == [11]
    assert store.recover(a.publication_id) == a
    with pytest.raises(ValueError, match="different output"):
        publish(store, "v1", 12)
    with pytest.raises(ValueError, match="PIT"):
        DataPortal(store, b.snapshot_id).read("prices", strict_pit=True)


@pytest.mark.parametrize("when", ["before_commit", "after_commit"])
def test_crash_boundary(store, when):
    a = publish(store)
    def crash(point):
        if point == when:
            raise RuntimeError("simulated process failure")
    with pytest.raises(RuntimeError):
        publish(store, "v2", 11, fault=crash)
    assert DataPortal(store, a.snapshot_id).read("prices").close.tolist() == [10]
    recovered = publish(store, "v2", 11)
    assert len(DataPortal(store, recovered.snapshot_id).read("prices")) == 1
    assert publish(store, "v2", 11) == recovered


def test_stale_compare_and_swap_rejected(store):
    a = publish(store)
    publish(store, "v2", 11)
    with pytest.raises(ValueError, match="changed"):
        publish(store, "v3", 12, expected_previous=a.publication_id)


def test_raw_unknown_fields_and_failed_pages_survive(tmp_path):
    raw = RawStore(tmp_path)
    original = pd.DataFrame({"unknown": ["原始字段"], "amount": [1.25]})
    batch = raw.save_frame("tushare", "daily", original, parameters={"token": "test-secret"}, complete=False, status="partial")
    pd.testing.assert_frame_equal(original, raw.load_frame(batch.batch_id))
    assert batch.parameters["token"] == "[REDACTED]"
    with pytest.raises(FileExistsError):
        raw.save_frame("tushare", "daily", original, parameters={}, complete=True, fetch_id=batch.batch_id)
    (batch.directory / "payload").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        raw.load_frame(batch.batch_id)


def test_pit_requires_row_evidence_and_excludes_later_knowledge(store):
    schema = TableSchema("known_prices", SCHEMA.columns + (
        ColumnSpec("available_at", "TIMESTAMP WITH TIME ZONE", False),
        ColumnSpec("observed_at", "TIMESTAMP WITH TIME ZONE", False)), SCHEMA.primary_key)
    frame = pd.DataFrame({"trade_date":[date(2026,9,30),date(2026,9,30)], "ticker":["A","B"], "close":[10.,20.],
        "available_at":pd.to_datetime(["2026-09-30T10:00:00Z","2026-10-01T10:00:00Z"]),
        "observed_at":pd.to_datetime(["2026-09-30T10:01:00Z","2026-10-01T10:01:00Z"])})
    pub = store.publish(schema,frame,partition="2026-09-30",idempotency_key="pit",
        lineage={"pit_evidence":{"kind":"recorded_test_publication"}},time_model="pit")
    portal = DataPortal(store,pub.snapshot_id)
    assert portal.read("known_prices",strict_pit=True,knowledge_at="2026-09-30T12:00:00Z").ticker.tolist() == ["A"]
    with pytest.raises(ValueError,match="cutoff"):
        portal.read("known_prices",strict_pit=True)


def test_postgres_backup_restores_fixed_history(pg_store, tmp_path):
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    from qrp_atlas.pipeline.lake.backup import backup, restore
    bindir = os.environ.get("QRP_TEST_PG_BIN")
    if not bindir:
        pytest.skip("PostgreSQL client utility directory not configured")
    first = publish(pg_store)
    publish(pg_store,"v2",11)
    suffix = ".exe" if os.name == "nt" else ""
    backup(pg_store,tmp_path/"backup",pg_dump=Path(bindir)/("pg_dump"+suffix),external_writers_quiesced=True)
    database = "qrp_restore_"+uuid.uuid4().hex
    dsn = os.environ["QRP_TEST_POSTGRES_DSN"]
    with psycopg.connect(dsn,autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        try:
            target = LakeLocation("postgres:"+make_conninfo(dsn,dbname=database),tmp_path/"restored")
            restored = restore(tmp_path/"backup",target,pg_restore=Path(bindir)/("pg_restore"+suffix))
            assert DataPortal(restored,first.snapshot_id).read("prices").close.tolist()==[10.]
            assert restored.recover(first.publication_id).content_hash == first.content_hash
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))


def test_post_cast_duplicate_dates_rejected(store):
    frame = pd.DataFrame({
        "trade_date": ["2026-09-30 10:00:00", "2026-09-30 11:00:00"],
        "ticker": ["000001.SZ", "000001.SZ"], "close": [10., 11.],
    })
    with pytest.raises(ValueError, match="duplicate business key after contract cast"):
        store.publish(SCHEMA, frame, partition="2026-09-30", idempotency_key="bad", lineage={})
    assert store.current_publication("prices", "2026-09-30") == ""


def test_schema_only_snapshot_is_not_published_data(store):
    first = publish(store)
    portal = DataPortal(store, first.snapshot_id - 1)
    with pytest.raises(ValueError, match="not published"):
        portal.read("prices")
    with pytest.raises(ValueError, match="not published"):
        with portal.sql_connection(("prices",)):
            pytest.fail("unpublished table became a consumer view")


def test_post_cast_numeric_key_alias_rejected(store):
    schema = TableSchema("codes", (ColumnSpec("code", "INTEGER", False),), ("code",))
    with pytest.raises(ValueError, match="duplicate business key after contract cast"):
        store.publish(schema, pd.DataFrame({"code": ["01", "1"]}), partition="snapshot", partition_column=None, idempotency_key="bad", lineage={})


def test_canonical_typed_order_has_same_publication_identity(store):
    schema = TableSchema("typed_codes", (ColumnSpec("code", "INTEGER", False),), ("code",))
    first = store.publish(schema, pd.DataFrame({"code": ["10", "02"]}), partition="snapshot", partition_column=None, idempotency_key="same", lineage={})
    second = store.publish(schema, pd.DataFrame({"code": [2, 10]}), partition="snapshot", partition_column=None, idempotency_key="same", lineage={})
    assert first == second


def test_no_implicit_overwrite_without_cas(store):
    first = publish(store)
    with pytest.raises(ValueError, match="changed since computation"):
        publish(store, "blind", 11, expected_previous=None)
    # Lost acknowledgement still reconciles the exact existing publication.
    assert publish(store, expected_previous=None) == first


def test_whole_table_cas_crosses_observation_date(store):
    schema = TableSchema("identities", (ColumnSpec("ticker", "VARCHAR", False),), ("ticker",))
    frame = pd.DataFrame({"ticker": ["A"]})
    first = store.publish(schema, frame, partition="2026-09-30", partition_column=None, idempotency_key="a", lineage={})
    assert store.current_publication("identities", "2026-10-01", whole_table=True) == first.publication_id
    with pytest.raises(ValueError, match="changed since computation"):
        store.publish(schema, frame, partition="2026-10-01", partition_column=None, idempotency_key="b", lineage={}, expected_previous="")
    second = store.publish(schema, pd.DataFrame({"ticker": ["B"]}), partition="2026-10-01", partition_column=None, idempotency_key="b", lineage={}, expected_previous=first.publication_id)
    assert DataPortal(store, first.snapshot_id).read("identities").ticker.tolist() == ["A"]
    assert DataPortal(store, second.snapshot_id).read("identities").ticker.tolist() == ["B"]


def test_contract_order_key_and_partition_mode_cannot_drift(store):
    publish(store)
    frame = pd.DataFrame({"trade_date": [date(2026, 9, 30)], "ticker": ["000001.SZ"], "close": [10.]})
    reordered = TableSchema(SCHEMA.name, tuple(reversed(SCHEMA.columns)), SCHEMA.primary_key)
    changed_key = TableSchema(SCHEMA.name, SCHEMA.columns, ("ticker",))
    for schema, partition_column in [(reordered, "trade_date"), (changed_key, "trade_date"), (SCHEMA, None)]:
        with pytest.raises(ValueError, match="contract changed"):
            store.publish(schema, frame, partition="2026-09-30", partition_column=partition_column, idempotency_key="v1", lineage={"raw_batch_id": "recorded"})


def test_parallel_whole_table_candidates_only_one_wins(pg_store):
    from concurrent.futures import ThreadPoolExecutor

    store = pg_store
    schema = TableSchema("identity_race", (ColumnSpec("ticker", "VARCHAR", False),), ("ticker",))
    first = store.publish(schema, pd.DataFrame({"ticker": ["A"]}), partition="2026-09-30", partition_column=None, idempotency_key="a", lineage={})
    def attempt(day):
        try:
            return store.publish(schema, pd.DataFrame({"ticker": [day]}), partition=day, partition_column=None, idempotency_key=day, lineage={}, expected_previous=first.publication_id)
        except ValueError as exc:
            return str(exc)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(attempt, ["2026-10-01", "2026-10-02"]))
    assert sum(not isinstance(item, str) for item in results) == 1
    assert sum(isinstance(item, str) and "changed since computation" in item for item in results) == 1


@pytest.mark.parametrize("when", ["before_commit", "after_commit"])
def test_postgres_crash_boundary(pg_store, when):
    test_crash_boundary(pg_store, when)


@pytest.mark.parametrize("when", ["before_commit", "after_commit"])
def test_postgres_abrupt_process_exit_recovers(pg_store, when):
    import subprocess
    import sys

    first = publish(pg_store)
    environment = dict(os.environ, QRP_TEST_CRASH_CATALOG=pg_store.location.catalog)
    result = subprocess.run([
        sys.executable, str(Path(__file__).with_name("store_crash_worker.py")),
        str(pg_store.location.root), when,
    ], env=environment, capture_output=True, text=True, timeout=45)
    assert result.returncode == 91, result.stderr
    current = pg_store.current_publication("prices", "2026-09-30")
    assert (current == first.publication_id) == (when == "before_commit")
    assert DataPortal(pg_store, first.snapshot_id).read("prices").close.tolist() == [10.]
    recovered = publish(pg_store, "v2", 11)
    assert publish(pg_store, "v2", 11) == recovered
    assert DataPortal(pg_store, recovered.snapshot_id).read("prices").close.tolist() == [11.]
    with pg_store.connect() as connection:
        count = connection.execute("SELECT count(*) FROM lake._qrp_publications WHERE publication_id=?", [recovered.publication_id]).fetchone()[0]
    assert count == 1


def test_parallel_provider_waits_do_not_hold_publication_right(store, tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier, Event
    from qrp_atlas.pipeline.lake.provider import SDKProvider

    waiting = Barrier(3)
    release = Event()
    raw = RawStore(tmp_path / "parallel-raw")

    class Client:
        def daily(self, **query):
            waiting.wait(timeout=15)
            assert release.wait(timeout=20)
            return pd.DataFrame({"trade_date": [query["trade_date"]], "untouched": ["raw"]})

    provider = SDKProvider(Client(), raw, sdk_version="test", page_size=10)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(provider.acquire, "daily_market_snapshot", {"trade_date": day})
                   for day in ("20260929", "20260930")]
        try:
            waiting.wait(timeout=15)
            committed = publish(store)
            assert DataPortal(store, committed.snapshot_id).read("prices").close.tolist() == [10.]
            assert not any(future.done() for future in futures)
        finally:
            release.set()
        batches = [future.result(timeout=15) for future in futures]
    assert len({batch.batch_id for batch in batches}) == 2
    assert [raw.load_frame(batch.batch_id).trade_date.iloc[0] for batch in batches] == ["20260929", "20260930"]


def test_independent_date_failure_does_not_abort_parallel_publications(pg_store):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    first = publish(pg_store)
    ready = Barrier(3)

    def attempt(day):
        frame = pd.DataFrame({"trade_date": [date(2026, 10, day)], "ticker": ["000001.SZ"], "close": [10. + day]})
        if day == 3:
            frame = pd.concat([frame, frame], ignore_index=True)
        ready.wait(timeout=15)
        try:
            return pg_store.publish(SCHEMA, frame, partition=f"2026-10-{day:02d}",
                                    idempotency_key=f"date-{day}", lineage={}, expected_previous="")
        except ValueError:
            if day != 3:
                raise
            return None

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(attempt, (1, 2, 3)))
    assert results[0] is not None and results[1] is not None and results[2] is None
    latest = max(result.snapshot_id for result in results if result)
    assert sorted(DataPortal(pg_store, latest).read("prices").close.tolist()) == [10., 11., 12.]
    assert DataPortal(pg_store, first.snapshot_id).read("prices").close.tolist() == [10.]
    assert pg_store.current_publication("prices", "2026-10-03") == ""
