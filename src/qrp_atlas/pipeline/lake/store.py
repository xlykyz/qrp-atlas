"""DuckLake transactions, immutable publication identities and pinned reads.

No network acquisition or normalization happens under the publication lock.
Partition locks are OS locks (not expiring leases). All writers on this single
host must use the same root. PostgreSQL provides catalog concurrency.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import tempfile
import time
import uuid

import duckdb
from filelock import FileLock
import pandas as pd

from qrp_atlas.contracts.schema import TableSchema
from .raw import identifier


def qi(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def literal(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class Publication:
    publication_id: str
    dataset: str
    partition: str
    content_hash: str
    snapshot_id: int
    lineage: dict
    time_model: str
    quality: str


@dataclass(frozen=True)
class LakeLocation:
    catalog: str = field(repr=False)
    root: Path
    domain: str = "core"
    threads: int = 2
    memory_limit: str = "4GB"
    relocated: bool = False

    def __post_init__(self):
        if self.domain not in {"core", "extension"}:
            raise ValueError("invalid lake domain")
        if not Path(self.root).is_absolute():
            raise ValueError("lake root must be absolute")


class DuckLakeStore:
    def __init__(self, location: LakeLocation, *, read_only=False):
        self.location = location
        self.read_only = read_only
        self.base = Path(location.root) / location.domain

    @contextmanager
    def connect(self, *, read_only=True):
        con = duckdb.connect(config={"threads": self.location.threads, "memory_limit": self.location.memory_limit})
        guard = None
        try:
            con.execute("SET temp_directory = " + literal(str(Path(tempfile.gettempdir()) / ("qrp-spill-" + uuid.uuid4().hex))))
            # Installation is an explicit setup command; normal execution never upgrades extensions.
            con.execute("LOAD ducklake")
            if self.location.catalog.startswith("postgres:"):
                con.execute("LOAD postgres")
                if not read_only:
                    import psycopg
                    guard = psycopg.connect(self.location.catalog.removeprefix("postgres:"), autocommit=True)
                    guard.execute("SET statement_timeout='30s'")
                    guard.execute("SELECT pg_advisory_lock_shared(72715001)")
            elif self.location.catalog.startswith("sqlite:"):
                con.execute("LOAD sqlite")
            options = "READ_ONLY" if read_only else "DATA_INLINING_ROW_LIMIT 0"
            if not read_only:
                options += ", DATA_PATH " + literal((self.base / "files").as_posix())
            if self.location.relocated:
                if read_only:
                    options += ", DATA_PATH " + literal((self.base / "files").as_posix())
                options += ", OVERRIDE_DATA_PATH true"
            try:
                con.execute("ATTACH " + literal("ducklake:" + self.location.catalog) + " AS lake (" + options + ")")
            except Exception:
                raise RuntimeError("lake catalog attachment failed; verify catalog, extension and permissions") from None
            yield con
        finally:
            con.close()
            if guard:
                guard.close()

    def initialize(self):
        if self.read_only:
            raise PermissionError("read-only lake")
        self.base.mkdir(parents=True, exist_ok=True)
        (self.base / "locks").mkdir(exist_ok=True)
        (self.base / "staging").mkdir(exist_ok=True)
        with FileLock(str(self.base / "locks" / "init.lock"), timeout=30):
            with self.connect(read_only=False) as con:
                con.execute("CREATE TABLE IF NOT EXISTS lake._qrp_publications (publication_id VARCHAR, dataset VARCHAR, partition_id VARCHAR, content_hash VARCHAR, lineage VARCHAR, time_model VARCHAR, quality VARCHAR, published_at TIMESTAMPTZ)")
                con.execute("CREATE TABLE IF NOT EXISTS lake._qrp_contracts (dataset VARCHAR, contract_json VARCHAR)")

    def _bind_contract(self, schema: TableSchema, contract_text: str) -> None:
        """Bind immutable dataset write semantics under a short schema lock.

This closes the first-publication race: even before a first data receipt is
committed, another writer cannot treat the same table as a different partition
mode or key. No acquisition or computation runs inside this lock.
"""
        lock_name = hashlib.sha256((schema.name + "\0schema").encode()).hexdigest()
        with FileLock(str(self.base / "locks" / (lock_name + ".lock")), timeout=30):
            with self.connect(read_only=False) as con:
                row = con.execute("SELECT contract_json FROM lake._qrp_contracts WHERE dataset=?", [schema.name]).fetchone()
                if row is not None:
                    if row[0] != contract_text:
                        raise ValueError("dataset contract changed; publish a new versioned dataset")
                    return
                columns_sql = ",".join(qi(c.name) + " " + c.dtype + (" NOT NULL" if not c.nullable else "") for c in schema.columns)
                con.execute("BEGIN")
                con.execute("CREATE TABLE IF NOT EXISTS lake." + qi(schema.name) + " (" + columns_sql + ")")
                actual = con.execute("DESCRIBE lake." + qi(schema.name)).fetchall()
                expected = [(column.name, column.dtype.upper(), "YES" if column.nullable else "NO") for column in schema.columns]
                if [(row[0], row[1], row[2]) for row in actual] != expected:
                    raise ValueError("existing table schema differs from contract")
                con.execute("INSERT INTO lake._qrp_contracts VALUES (?,?)", [schema.name, contract_text])
                con.execute("COMMIT")

    def _publication(self, con, publication_id: str) -> Publication | None:
        row = con.execute("SELECT publication_id,dataset,partition_id,content_hash,lineage,time_model,quality FROM lake._qrp_publications WHERE publication_id=?", [publication_id]).fetchone()
        if row is None:
            return None
        # Find the first committed snapshot containing the receipt. This also
        # recovers exact identities after a crash between COMMIT and reporting.
        snapshots = [r[0] for r in con.execute("SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id").fetchall()]
        lo, hi = 0, len(snapshots)-1
        while lo < hi:
            mid = (lo+hi)//2
            try:
                exists = con.execute(f"SELECT count(*) FROM lake._qrp_publications AT (VERSION => {int(snapshots[mid])}) WHERE publication_id=?", [publication_id]).fetchone()[0]
            except duckdb.CatalogException:
                exists = 0
            if exists:
                hi = mid
            else:
                lo = mid+1
        return Publication(row[0], row[1], row[2], row[3], int(snapshots[lo]), json.loads(row[4]), row[5], row[6])

    def recover(self, publication_id: str) -> Publication | None:
        with self.connect() as con:
            return self._publication(con, publication_id)

    def current_publication(self, dataset: str, partition: str, *, whole_table=False) -> str:
        with self.connect() as con:
            where = "dataset=?" + ("" if whole_table else " AND partition_id=?")
            values = [dataset] if whole_table else [dataset, str(partition)]
            row = con.execute("SELECT publication_id FROM lake._qrp_publications WHERE " + where + " ORDER BY published_at DESC LIMIT 1", values).fetchone()
            return row[0] if row else ""

    def publish(self, schema: TableSchema, frame: pd.DataFrame, *, partition: str,
                idempotency_key: str, lineage: dict, partition_column: str | None = "trade_date",
                time_model: str = "observed", quality: str = "complete",
                expected_previous: str | None = None, fault=None) -> Publication:
        if self.read_only:
            raise PermissionError("read-only lake")
        identifier(schema.name)
        if schema.name.startswith("_qrp"):
            raise ValueError("reserved publication namespace")
        if quality not in {"complete", "legal_empty"} or (frame.empty and quality != "legal_empty"):
            raise ValueError("incomplete or unexplained empty output cannot publish")
        if time_model not in {"observed", "legacy", "pit"}:
            raise ValueError("invalid time model")
        if time_model == "pit" and (not lineage.get("pit_evidence") or not {"available_at", "observed_at"} <= set(frame.columns)):
            raise ValueError("strict PIT needs explicit source evidence")
        if set(frame.columns) != set(schema.column_names()):
            raise ValueError("output columns differ from contract")
        if not idempotency_key or len(idempotency_key) > 256:
            raise ValueError("invalid idempotency key")
        if not schema.primary_key or not set(schema.primary_key) <= set(schema.column_names()):
            raise ValueError("explicit valid business key required")
        payload = frame.loc[:, schema.column_names()].copy()
        if payload[list(schema.primary_key)].isna().any().any() or payload.duplicated(list(schema.primary_key)).any():
            raise ValueError("null or duplicate business key")
        for col in schema.columns:
            if not col.nullable and payload[col.name].isna().any():
                raise ValueError("null in required column " + col.name)
        if partition_column:
            if partition_column not in payload:
                raise ValueError("partition column absent")
            if next(column.dtype.upper() for column in schema.columns if column.name == partition_column) != "DATE":
                raise ValueError("date partition column must have DATE contract type")
            values = pd.to_datetime(payload[partition_column], errors="raise").dt.date.astype(str)
            if not values.eq(str(partition)).all():
                raise ValueError("rows outside target partition")
        payload = payload.sort_values(list(schema.primary_key), kind="stable").reset_index(drop=True)
        if "_qrp_contract" in lineage:
            raise ValueError("reserved contract lineage field")
        contract = {
            "columns": [(c.name, c.dtype, c.nullable) for c in schema.columns],
            "primary_key": list(schema.primary_key), "partition_column": partition_column,
        }
        contract_text = json.dumps(contract, sort_keys=True, separators=(",", ":"))
        lineage_text = json.dumps({**lineage, "_qrp_contract": contract}, sort_keys=True, separators=(",", ":"), default=str)
        # Staging is per attempt, never a shared mutable filename.
        staging_root = self.base / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="publish-", dir=staging_root) as stage:
            path = Path(stage) / "candidate.parquet"
            with duckdb.connect(config={"threads": self.location.threads, "memory_limit": self.location.memory_limit}) as check:
                check.register("candidate", payload)
                cast_columns = ",".join(f"CAST({qi(c.name)} AS {c.dtype}) AS {qi(c.name)}" for c in schema.columns)
                keys = ",".join(qi(key) for key in schema.primary_key)
                check.execute("COPY (SELECT * FROM (SELECT " + cast_columns + " FROM candidate) AS casted ORDER BY " + keys + ") TO " + literal(path.as_posix()) + " (FORMAT PARQUET)")
                # Validate the *published* types. Different input keys can
                # collapse during casting (timestamps -> DATE, '01' -> INT).
                duplicates = check.execute("SELECT count(*) FROM (SELECT " + keys + " FROM read_parquet(?) GROUP BY " + keys + " HAVING count(*)>1)", [path.as_posix()]).fetchone()[0]
                if duplicates:
                    raise ValueError("duplicate business key after contract cast")
                required = set(schema.primary_key) | {col.name for col in schema.columns if not col.nullable}
                nulls = " OR ".join(qi(name) + " IS NULL" for name in sorted(required))
                if check.execute("SELECT count(*) FROM read_parquet(?) WHERE " + nulls, [path.as_posix()]).fetchone()[0]:
                    raise ValueError("null required field after contract cast")
                if partition_column and check.execute("SELECT count(*) FROM read_parquet(?) WHERE CAST(" + qi(partition_column) + " AS VARCHAR) <> ? OR " + qi(partition_column) + " IS NULL", [path.as_posix(), str(partition)]).fetchone()[0]:
                    raise ValueError("rows outside target partition after contract cast")
            digest = hashlib.sha256()
            with path.open("rb") as staged:
                for chunk in iter(lambda: staged.read(1024 * 1024), b""):
                    digest.update(chunk)
            digest.update(lineage_text.encode())
            digest.update(time_model.encode())
            digest.update(quality.encode())
            content_hash = digest.hexdigest()
            pub_id = hashlib.sha256((schema.name + "\0" + str(partition) + "\0" + idempotency_key).encode()).hexdigest()
            lock_partition = str(partition) if partition_column else "__whole_table__"
            lock_name = hashlib.sha256((schema.name + "\0" + lock_partition).encode()).hexdigest()
            locks = self.base / "locks"
            locks.mkdir(exist_ok=True)
            self._bind_contract(schema, contract_text)
            with FileLock(str(locks / (lock_name + ".lock")), timeout=30):
                for attempt in range(3):
                    with self.connect(read_only=False) as con:
                        existing = self._publication(con, pub_id)
                        if existing:
                            if existing.content_hash != content_hash:
                                raise ValueError("idempotency key reused with different output")
                            return existing
                        try:
                            con.execute("BEGIN")
                            where = "dataset=?" + (" AND partition_id=?" if partition_column else "")
                            values = [schema.name, str(partition)] if partition_column else [schema.name]
                            previous = con.execute("SELECT publication_id FROM lake._qrp_publications WHERE " + where + " ORDER BY published_at DESC LIMIT 1", values).fetchone()
                            if (previous[0] if previous else "") != (expected_previous or ""):
                                raise ValueError("partition changed since computation")
                            table = "lake." + qi(schema.name)
                            if partition_column:
                                con.execute("DELETE FROM " + table + " WHERE " + qi(partition_column) + "=?", [partition])
                            else:
                                con.execute("DELETE FROM " + table)
                            column_list = ",".join(qi(column.name) for column in schema.columns)
                            con.execute("INSERT INTO " + table + " (" + column_list + ") SELECT " + column_list + " FROM read_parquet(?)", [path.as_posix()])
                            con.execute("INSERT INTO lake._qrp_publications VALUES (?,?,?,?,?,?,?,current_timestamp)", [pub_id, schema.name, str(partition), content_hash, lineage_text, time_model, quality])
                            if fault:
                                fault("before_commit")
                            con.execute("COMMIT")
                            if fault:
                                fault("after_commit")
                            return self._publication(con, pub_id)
                        except duckdb.TransactionException:
                            try:
                                con.execute("ROLLBACK")
                            except duckdb.Error:
                                pass
                            if attempt == 2:
                                raise
                    time.sleep(0.1 * (attempt+1))
        raise RuntimeError("publication did not complete")


class DataPortal:
    def __init__(self, store: DuckLakeStore, snapshot_id: int):
        if not isinstance(snapshot_id, int) or snapshot_id < 0:
            raise ValueError("explicit valid snapshot required")
        self.store = store
        self.snapshot_id = snapshot_id

    def read(self, dataset: str, *, strict_pit=False, publication_id=None, knowledge_at=None) -> pd.DataFrame:
        identifier(dataset)
        with self.store.connect() as con:
            self._require_published(con, dataset)
            if publication_id:
                receipt = con.execute(f"SELECT dataset FROM lake._qrp_publications AT (VERSION => {self.snapshot_id}) WHERE publication_id=?", [publication_id]).fetchone()
                if not receipt or receipt[0] != dataset:
                    raise ValueError("input publication is absent at requested snapshot")
                # Exact receipt snapshot prevents a newer revision hiding behind an old receipt.
                expected = self.store._publication(con, publication_id)
                if expected.snapshot_id != self.snapshot_id:
                    raise ValueError("publication snapshot mismatch")
            if strict_pit:
                if knowledge_at is None or pd.Timestamp(knowledge_at).tzinfo is None:
                    raise ValueError("strict PIT requires a timezone-aware knowledge cutoff")
                rows = con.execute(f"SELECT time_model FROM lake._qrp_publications AT (VERSION => {self.snapshot_id}) WHERE dataset=?", [dataset]).fetchall()
                if not rows or any(r[0] != "pit" for r in rows):
                    raise ValueError("source does not prove strict PIT")
            frame = con.execute(f"SELECT * FROM lake.{qi(dataset)} AT (VERSION => {self.snapshot_id})").fetchdf()
            if strict_pit:
                if not {"available_at", "observed_at"} <= set(frame):
                    raise ValueError("source has no row-level PIT evidence")
                available = pd.to_datetime(frame.available_at, utc=True, errors="coerce")
                observed = pd.to_datetime(frame.observed_at, utc=True, errors="coerce")
                if available.isna().any() or observed.isna().any():
                    raise ValueError("incomplete row-level PIT evidence")
                frame = frame.loc[(available <= pd.Timestamp(knowledge_at)) & (observed <= pd.Timestamp(knowledge_at))]
            return frame

    def _require_published(self, con, dataset):
        row = con.execute(f"SELECT count(*) FROM lake._qrp_publications AT (VERSION => {self.snapshot_id}) WHERE dataset=?", [dataset]).fetchone()
        if not row or not row[0]:
            raise ValueError("dataset is not published at requested snapshot")

    @contextmanager
    def sql_connection(self, datasets):
        with self.store.connect() as con:
            for dataset in datasets:
                identifier(dataset)
                self._require_published(con, dataset)
                con.execute(f"CREATE TEMP VIEW {qi(dataset)} AS SELECT * FROM lake.{qi(dataset)} AT (VERSION => {self.snapshot_id})")
            yield con
