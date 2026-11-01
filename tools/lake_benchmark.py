"""A12 synthetic, opt-in benchmark in new, empty PostgreSQL lake catalogs.

Does not contact a market provider, migrate legacy data, or run a scheduler.
Use --plan for a no-write preflight. See pipeline-lake-benchmark.md.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
import multiprocessing as mp
import os
from pathlib import Path
import platform
import queue
import sys
import tempfile
import threading
import time

import duckdb
import numpy as np
import pandas as pd
import psutil
import psycopg

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts.schema import ColumnSpec, TableSchema
from qrp_atlas.pipeline.lake.store import DataPortal, DuckLakeStore, LakeLocation


def schema(name):
    return TableSchema(name, (
        ColumnSpec("trade_date", "DATE", False), ColumnSpec("ticker", "VARCHAR", False),
        ColumnSpec("close", "DOUBLE", False), ColumnSpec("ma20", "DOUBLE", False),
    ), ("trade_date", "ticker"))


def positive(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--days", type=positive, default=20)
    parser.add_argument("--assets", type=positive, default=1000)
    parser.add_argument("--history-days", type=positive, default=20)
    parser.add_argument("--max-core-p95-seconds", type=float)
    parser.add_argument("--max-slowdown-ratio", type=float)
    parser.add_argument("--plan", action="store_true", help="validate isolation without creating anything")
    return parser.parse_args()


def empty_catalog(catalog):
    """Reject even an initialized catalog: this run owns brand-new databases."""
    if not catalog or not catalog.startswith("postgres:"):
        raise ValueError("benchmark requires dedicated PostgreSQL catalogs")
    with psycopg.connect(catalog.removeprefix("postgres:"), connect_timeout=10) as connection:
        tables = connection.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')").fetchone()[0]
        if tables:
            raise ValueError("benchmark catalog is not empty; provide a new database")


def prepare(args):
    if not args.env_file.is_absolute():
        raise ValueError("--env-file must be absolute")
    if args.env_file.name.lower() in {"root.env", ".env"}:
        raise ValueError("use a dedicated benchmark configuration, not a production/default env file")
    settings = AppSettings.load(env_file=args.env_file, environ={}, project_root=args.env_file.parent)
    cfg = settings.lake
    if settings.runtime.environment.value != "test" or settings.runtime.read_only:
        raise ValueError("benchmark requires explicit writable QRP_RUNTIME_ENV=test")
    if cfg.root is None or not cfg.core_catalog or not cfg.extension_catalog or not cfg.core_read_catalog:
        raise ValueError("benchmark needs a new root and separate core, core-reader and extension catalogs")
    if cfg.daily_source != "legacy":
        raise ValueError("benchmark cannot enable production API lake routing")
    root = cfg.root.resolve()
    old_data = settings.paths.data_dir.resolve()
    if root == old_data or root.is_relative_to(old_data) or old_data.is_relative_to(root):
        raise ValueError("benchmark root overlaps legacy data directory")
    if root.exists() and any(root.iterdir()):
        raise ValueError("benchmark root must be new or completely empty")
    for value in (args.max_core_p95_seconds, args.max_slowdown_ratio):
        if value is not None and (not np.isfinite(value) or value <= 0):
            raise ValueError("performance thresholds must be positive and finite")
    if args.history_days < 20:
        raise ValueError("history-days must be at least 20 for a complete MA20 window")
    # These checks are read-only; initialize is called only after both pass.
    empty_catalog(cfg.core_catalog)
    empty_catalog(cfg.extension_catalog)
    # PostgreSQL readers must target the same database as the core writer.
    writer_db = psycopg.conninfo.conninfo_to_dict(cfg.core_catalog.removeprefix("postgres:"))
    reader_db = psycopg.conninfo.conninfo_to_dict(cfg.core_read_catalog.removeprefix("postgres:"))
    ext_db = psycopg.conninfo.conninfo_to_dict(cfg.extension_catalog.removeprefix("postgres:"))
    address = lambda item: tuple(item.get(key, "") for key in ("host", "port", "dbname"))
    if address(writer_db) != address(reader_db) or address(writer_db) == address(ext_db):
        raise ValueError("core reader must address core database; extension database must be separate")
    return settings


def synthetic_history(assets, history_days):
    days = pd.bdate_range("2020-01-01", periods=history_days).date
    asset = np.tile(np.arange(assets), history_days)
    step = np.repeat(np.arange(history_days), assets)
    return pd.DataFrame({"trade_date": np.repeat(days, assets),
                         "ticker": [f"synthetic-{number:06d}" for number in asset],
                         "close": 10.0 + asset / 100 + step / 100,
                         "ma20": 10.0 + asset / 100 + step / 100 - 0.095})


def compute(history, target, step, assets, threads, memory):
    current = pd.DataFrame({"trade_date": [target] * assets,
                            "ticker": [f"synthetic-{i:06d}" for i in range(assets)],
                            "close": 10.0 + np.arange(assets) / 100 + step / 100,
                            "ma20": np.zeros(assets)})
    panel = pd.concat([history, current], ignore_index=True)
    with duckdb.connect(config={"threads": threads, "memory_limit": memory}) as con:
        con.register("panel", panel)
        result = con.execute("SELECT trade_date,ticker,close,ma20 FROM (SELECT trade_date,ticker,close,avg(close) OVER (PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20 FROM panel) WHERE trade_date=? ORDER BY ticker", [target]).fetchdf()
    return result


class Samples:
    """Sample parent/worker RSS and this run's staging/spill directories."""
    def __init__(self, root):
        self.root, self.finished = root, threading.Event()
        self.peak_core_rss = self.peak_total_rss = self.peak_temp = 0
        self.thread = threading.Thread(target=self.loop, daemon=True)

    def loop(self):
        process = psutil.Process()
        while not self.finished.is_set():
            try:
                core = process.memory_info().rss
                total = core
                for child in process.children(recursive=True):
                    try:
                        total += child.memory_info().rss
                    except psutil.NoSuchProcess:
                        pass
                temporary = 0
                for path in (self.root / "tmp", self.root / "core" / "staging", self.root / "extension" / "staging"):
                    for item in path.rglob("*") if path.exists() else ():
                        try:
                            if item.is_file():
                                temporary += item.stat().st_size
                        except FileNotFoundError:
                            pass
                self.peak_core_rss = max(self.peak_core_rss, core)
                self.peak_total_rss = max(self.peak_total_rss, total)
                self.peak_temp = max(self.peak_temp, temporary)
            except psutil.NoSuchProcess:
                break
            self.finished.wait(0.05)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.finished.set()
        self.thread.join(timeout=2)

    def result(self):
        return dict(sample_interval_seconds=0.05, peak_core_rss_bytes=self.peak_core_rss,
                    peak_process_tree_rss_bytes=self.peak_total_rss, peak_temporary_bytes=self.peak_temp,
                    caveat="sampled peaks may miss short spikes; includes Python overhead, not cgroup accounting")


def extension_load(core_reader_location, extension_location, snapshot, stop, ready, output):
    """Trusted benchmark workload, with only the read-core and write-extension locations."""
    try:
        os.environ.update({name: str(extension_location.threads) for name in (
            "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS")})
        temp = Path(extension_location.root) / "tmp" / "extension"
        temp.mkdir(parents=True, exist_ok=True)
        tempfile.tempdir = str(temp)
        core = DuckLakeStore(core_reader_location, read_only=True)
        extension = DuckLakeStore(extension_location)
        input_frame = DataPortal(core, snapshot).read("benchmark_history")
        table = schema("benchmark_extension_load")
        loops = 0
        start_cpu = time.process_time()
        ready.set()
        while not stop.is_set():
            # A full historical rolling scan plus a separate catalog commit.
            with duckdb.connect(config={"threads": extension_location.threads,
                                        "memory_limit": extension_location.memory_limit}) as con:
                con.register("panel", input_frame)
                frame = con.execute("SELECT * FROM (SELECT trade_date,ticker,close,avg(close) OVER (PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20 FROM panel) WHERE trade_date=(SELECT max(trade_date) FROM panel)").fetchdf()
            partition = str(pd.Timestamp(frame.trade_date.iloc[0]).date())
            previous = extension.current_publication(table.name, partition)
            extension.publish(table, frame, partition=partition,
                              idempotency_key=f"synthetic-extension-{loops}",
                              expected_previous=previous,
                              lineage={"synthetic": True, "input_snapshot": snapshot, "iteration": loops})
            loops += 1
        output.put({"iterations": loops, "cpu_seconds": time.process_time() - start_cpu})
    except Exception as exc:
        output.put({"error_type": type(exc).__name__})
        ready.set()


def summary(rows):
    result = {}
    for metric in ("input_read_seconds", "compute_seconds", "publication_seconds", "end_to_end_seconds"):
        values = np.array([row[metric] for row in rows])
        result[metric] = {"p50": float(np.percentile(values, 50)), "p95": float(np.percentile(values, 95)),
                          "min": float(values.min()), "max": float(values.max())}
    return result


def phase(name, settings, args, store, history_snapshot, extension_location, reader_location):
    root, cfg = settings.lake.root, settings.lake
    ctx = mp.get_context("spawn")
    stop, ready, output = ctx.Event(), ctx.Event(), ctx.Queue()
    process = None
    if name == "with_extension":
        process = ctx.Process(target=extension_load,
                             args=(reader_location, extension_location, history_snapshot, stop, ready, output), daemon=True)
        process.start()
        if not ready.wait(timeout=60):
            process.terminate()
            process.join(timeout=5)
            raise RuntimeError("benchmark extension did not become ready")
    rows, hashes = [], []
    baseline_cpu = time.process_time()
    table = schema("benchmark_core_" + name)
    dates = pd.bdate_range("2020-01-01", periods=args.history_days + args.days).date[args.history_days:]
    try:
        with Samples(root) as sample:
            for index, target in enumerate(dates):
                if process and not process.is_alive():
                    raise RuntimeError("benchmark extension exited before measurements completed")
                began = time.perf_counter()
                history = DataPortal(store, history_snapshot).read("benchmark_history")
                # Include prior target days to keep MA20 inputs contiguous.
                if index:
                    prior = DataPortal(store, publication.snapshot_id).read(table.name)
                    history = pd.concat([history, prior], ignore_index=True)
                read_done = time.perf_counter()
                frame = compute(history, target, args.history_days + index, args.assets, cfg.threads, cfg.memory_limit)
                computed = time.perf_counter()
                publication = store.publish(table, frame, partition=str(target),
                    idempotency_key=f"synthetic-{name}-{target}",
                    lineage={"synthetic": True, "input_history_snapshot": history_snapshot})
                ended = time.perf_counter()
                values = frame.sort_values("ticker").close.to_numpy()
                expected_close = 10.0 + np.arange(args.assets) / 100 + (args.history_days + index) / 100
                expected_ma = expected_close - 0.095
                if len(frame) != args.assets or not np.allclose(values, expected_close, rtol=0, atol=1e-10) or not np.allclose(frame.sort_values("ticker").ma20.to_numpy(), expected_ma, rtol=0, atol=1e-10):
                    raise AssertionError("synthetic rolling output differs from independent linear-series expectation")
                restored = DataPortal(store, publication.snapshot_id).read(table.name)
                restored = restored.loc[pd.to_datetime(restored.trade_date).dt.date == target].sort_values("ticker")
                if len(restored) != args.assets or not np.allclose(restored.ma20, expected_ma, rtol=0, atol=1e-10):
                    raise AssertionError("published snapshot differs from calculated output")
                hashes.append(hashlib.sha256(np.column_stack([values, frame.sort_values("ticker").ma20]).tobytes()).hexdigest())
                rows.append(dict(target=str(target), rows=len(frame), input_rows=len(history),
                                 input_read_seconds=read_done-began, compute_seconds=computed-read_done,
                                 publication_seconds=ended-computed, end_to_end_seconds=ended-began,
                                 validation_seconds=time.perf_counter()-ended,
                                 snapshot_id=publication.snapshot_id))
    finally:
        if process:
            stop.set()
            process.join(timeout=15)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
    extension_result = None
    if process:
        try:
            extension_result = output.get(timeout=2)
        except queue.Empty:
            raise RuntimeError("benchmark extension did not report a clean completion") from None
        if "error_type" in extension_result or extension_result.get("iterations", 0) < 1:
            raise RuntimeError("benchmark extension failed or performed no concurrent work")
    return dict(name=name, samples=rows, timings=summary(rows), resource_samples=sample.result(),
                core_cpu_seconds=time.process_time()-baseline_cpu, extension=extension_result,
                output_hashes=hashes, queue_wait_seconds=None, lock_wait_seconds=None,
                wait_note="not separately instrumented; publication includes staging, validation, locks, catalog work and receipt recovery")


def main():
    args = arguments()
    try:
        settings = prepare(args)
        plan = dict(synthetic=True, days=args.days, assets=args.assets, history_days=args.history_days,
                    history_rows=args.history_days*args.assets, measured_core_rows_per_phase=args.days*args.assets,
                    thresholds={"core_p95_seconds": args.max_core_p95_seconds,
                                "slowdown_ratio": args.max_slowdown_ratio})
        if args.plan:
            print(json.dumps({"preflight": "passed_no_writes", **plan}, indent=2))
            return 0
        cfg, root = settings.lake, settings.lake.root
        root.mkdir(parents=True, exist_ok=True)
        (root / "tmp").mkdir()
        tempfile.tempdir = str(root / "tmp")
        core_location = LakeLocation(cfg.core_catalog, root, "core", cfg.threads, cfg.memory_limit)
        extension_location = LakeLocation(cfg.extension_catalog, root, "extension", cfg.threads, cfg.memory_limit)
        reader_location = LakeLocation(cfg.core_read_catalog, root, "core", cfg.threads, cfg.memory_limit)
        core = DuckLakeStore(core_location)
        extension = DuckLakeStore(extension_location)
        core.initialize()
        extension.initialize()
        history = synthetic_history(args.assets, args.history_days)
        seed = core.publish(schema("benchmark_history"), history, partition="synthetic-history",
                            partition_column=None, idempotency_key="synthetic-history-v1",
                            lineage={"synthetic": True, "days": args.history_days, "assets": args.assets})
        phases = [phase(name, settings, args, core, seed.snapshot_id, extension_location, reader_location)
                  for name in ("baseline", "with_extension")]
        if phases[0]["output_hashes"] != phases[1]["output_hashes"]:
            raise AssertionError("baseline and concurrent results differ")
        base = phases[0]["timings"]["end_to_end_seconds"]["p95"]
        concurrent = phases[1]["timings"]["end_to_end_seconds"]["p95"]
        slowdown = concurrent / base
        checks = []
        if args.max_core_p95_seconds is not None:
            checks.append(concurrent <= args.max_core_p95_seconds)
        if args.max_slowdown_ratio is not None:
            checks.append(slowdown <= args.max_slowdown_ratio)
        report = {**plan, "created_at": datetime.now(timezone.utc).isoformat(),
                  "environment": {"platform": platform.platform(), "python": platform.python_version(),
                                  "physical_cores": psutil.cpu_count(logical=False), "logical_cpus": psutil.cpu_count(),
                                  "system_memory_bytes": psutil.virtual_memory().total,
                                  "duckdb": version("duckdb"), "pandas": version("pandas"),
                                  "threads_per_compute": cfg.threads, "duckdb_memory_limit": cfg.memory_limit},
                  "phases": phases, "concurrent_to_baseline_p95_ratio": slowdown,
                  "correctness": "passed", "performance_status": "passed" if checks and all(checks) else "failed" if checks else "measurement_only_no_thresholds",
                  "limits": ["synthetic workload, not real provider or production data", "trusted extension load, not an OS permission-isolation test",
                             "baseline runs before concurrent phase; filesystem/cache warming and host activity may affect comparison",
                             "per-day end-to-end starts at pinned input read and ends after publication receipt; subsequent validation is separate",
                             "catalog server CPU/RSS and scheduler queue time are not measured", "short runs do not establish production P95"]}
        output = root / "benchmark-results.json"
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps({"report": str(output), "correctness": report["correctness"],
                          "performance_status": report["performance_status"], "p95_slowdown_ratio": slowdown}))
        return 0 if not checks or all(checks) else 2
    except Exception as exc:
        # Connection exceptions may contain a DSN. A machine-readable type is
        # enough for an unsuccessful run; no provider/catalog secrets are echoed.
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(main())
