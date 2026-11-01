"""PostgreSQL catalog + local files backup, with an exclusive maintenance gate.

All QRP writers use a shared catalog advisory lock. Operators must separately
quiesce external DuckLake writers, compaction and cleanup. Restore never writes
the source database or overwrites an existing destination directory.
"""
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

import psycopg
from psycopg.conninfo import conninfo_to_dict

from .store import DuckLakeStore, LakeLocation


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024*1024), b""):
            result.update(block)
    return result.hexdigest()


def pg_environment(catalog):
    if not catalog.startswith("postgres:"):
        raise ValueError("backup requires the PostgreSQL catalog path")
    values = conninfo_to_dict(catalog.removeprefix("postgres:"))
    mapping = {"host":"PGHOST", "port":"PGPORT", "user":"PGUSER", "password":"PGPASSWORD",
               "dbname":"PGDATABASE", "sslmode":"PGSSLMODE", "service":"PGSERVICE"}
    if set(values)-set(mapping):
        raise ValueError("unsupported backup connection options")
    env = {k:v for k,v in os.environ.items() if not k.startswith("PG")}
    env.update({mapping[k]:v for k,v in values.items()})
    return env


def run_pg(executable, args, catalog):
    result = subprocess.run([str(executable), *map(str,args)], env=pg_environment(catalog),
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=300,
                            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    if result.returncode:
        raise RuntimeError("PostgreSQL backup/restore utility failed")


def backup(store, destination: Path, *, pg_dump="pg_dump", external_writers_quiesced=False):
    if not external_writers_quiesced:
        raise ValueError("confirm external writers, compaction and cleanup are quiesced")
    destination = Path(destination).resolve()
    if destination.is_relative_to(store.base.resolve()) or store.base.resolve().is_relative_to(destination):
        raise ValueError("backup destination must be separate from data domain")
    destination.mkdir(parents=True, exist_ok=False)
    with psycopg.connect(store.location.catalog.removeprefix("postgres:"), autocommit=True) as guard:
        guard.execute("SET statement_timeout='30s'")
        guard.execute("SELECT pg_advisory_lock(72715001)")
        with store.connect() as con:
            snapshots = [int(r[0]) for r in con.execute("SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id").fetchall()]
        run_pg(pg_dump, ["--format=custom", "--no-owner", "--file", destination/"catalog.dump"], store.location.catalog)
        shutil.copytree(store.base/"files", destination/"files")
        checksums = {p.relative_to(destination).as_posix():digest(p) for p in destination.rglob("*") if p.is_file()}
        receipt = {"format":1, "domain":store.location.domain, "snapshots":snapshots,
                   "checksums":checksums, "duckdb_version":__import__("duckdb").__version__}
        (destination/"backup.json").write_text(json.dumps(receipt, sort_keys=True), encoding="utf-8")
    return receipt


def restore(source: Path, location: LakeLocation, *, pg_restore="pg_restore"):
    source = Path(source).resolve(strict=True)
    manifest = json.loads((source/"backup.json").read_text(encoding="utf-8"))
    if manifest["domain"] != location.domain:
        raise ValueError("backup domain mismatch")
    for name, expected in manifest["checksums"].items():
        path = (source/name).resolve()
        if not path.is_relative_to(source) or digest(path) != expected:
            raise ValueError("backup checksum mismatch")
    store = DuckLakeStore(replace(location, relocated=True))
    if store.base.exists():
        raise FileExistsError("restore requires a new data domain directory")
    if not location.catalog.startswith("postgres:"):
        raise ValueError("restore requires PostgreSQL")
    with psycopg.connect(location.catalog.removeprefix("postgres:"), autocommit=True) as con:
        if con.execute("SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')").fetchone()[0]:
            raise ValueError("restore catalog must be empty")
    store.base.mkdir(parents=True)
    shutil.copytree(source/"files", store.base/"files")
    run_pg(pg_restore, ["--exit-on-error", "--no-owner", "--no-acl", "--dbname", "", source/"catalog.dump"], location.catalog)
    # Prove restored history is queryable, not just that metadata opens.
    with store.connect() as con:
        for snapshot in manifest["snapshots"]:
            if snapshot > 0:
                try:
                    con.execute(f"SELECT count(*) FROM lake._qrp_publications AT (VERSION => {snapshot})").fetchone()
                except __import__("duckdb").CatalogException:
                    pass  # The initialization snapshot predates the receipt table.
    return store
