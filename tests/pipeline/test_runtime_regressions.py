"""Regression coverage for Pipeline runtime scheduling and direct execution."""

from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from qrp_atlas.pipeline.runtime.cli import main as pipeline_cli
from qrp_atlas.pipeline.runtime.definitions import DefinitionValidationError, load_definitions
from qrp_atlas.pipeline.runtime.models import OverlapPolicy, PipelineDefinition, PipelineStatus
from qrp_atlas.pipeline.runtime.runner import PipelineRunner, PipelineRuntimePaths
from qrp_atlas.pipeline.runtime.scheduler import PipelineScheduler
from qrp_atlas.pipeline.runtime.store import PipelineRuntimeStore, RunClaimFailure


def instant(hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2026, 1, 2, hour, minute, tzinfo=UTC)


def definition(
    tmp_path: Path,
    pipeline_id: str = "example",
    *,
    schedule: str = "* * * * *",
    timezone: str = "UTC",
    dependencies: tuple[str, ...] = (),
    overlap_policy: OverlapPolicy = OverlapPolicy.FORBID,
    resource_locks: tuple[str, ...] = (),
    command: tuple[str, ...] | None = None,
    timeout_seconds: int | None = 5,
    definition_version: str = "test-v1",
) -> PipelineDefinition:
    return PipelineDefinition(
        pipeline_id=pipeline_id,
        name=pipeline_id,
        enabled=True,
        schedule=schedule,
        timezone=timezone,
        command=command or (sys.executable, "-c", "pass"),
        working_directory=tmp_path,
        dependencies=dependencies,
        timeout_seconds=timeout_seconds,
        max_retries=2,
        overlap_policy=overlap_policy,
        resource_locks=resource_locks,
        definition_version=definition_version,
    )


def create_store(tmp_path: Path) -> PipelineRuntimeStore:
    store = PipelineRuntimeStore(tmp_path / "runtime" / "pipeline.sqlite3")
    store.initialize()
    return store


def claim(store: PipelineRuntimeStore, run_id: str, item: PipelineDefinition, tmp_path: Path) -> object:
    return store.claim_run(
        run_id,
        pipeline_id=item.pipeline_id,
        definition_version=item.definition_version,
        overlap_policy=item.overlap_policy,
        resource_locks=item.resource_locks,
        stdout_path=tmp_path / f"{run_id}.stdout",
        stderr_path=tmp_path / f"{run_id}.stderr",
        lease_seconds=30,
    )


def manifest_payload(items: list[PipelineDefinition]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "definitions": [
            {
                "pipeline_id": item.pipeline_id,
                "name": item.name,
                "enabled": item.enabled,
                "schedule": item.schedule,
                "timezone": item.timezone,
                "command": list(item.command),
                "working_directory": str(item.working_directory) if item.working_directory else None,
                "dependencies": list(item.dependencies),
                "timeout_seconds": item.timeout_seconds,
                "max_retries": item.max_retries,
                "overlap_policy": item.overlap_policy.value,
                "resource_locks": list(item.resource_locks),
                "performance_budget": {},
                "freshness_checks": [],
                "definition_version": item.definition_version,
            }
            for item in items
        ],
    }


def write_manifest(path: Path, items: list[PipelineDefinition]) -> None:
    path.write_text(json.dumps(manifest_payload(items)), encoding="utf-8")


def test_run_pending_cli_idle_and_explicit_rejections(tmp_path: Path, capsys) -> None:
    runtime_dir = tmp_path / "runtime"
    manifest = tmp_path / "definitions.json"
    item = definition(tmp_path, "cli")
    write_manifest(manifest, [item])
    prefix = ["--runtime-dir", str(runtime_dir)]
    assert pipeline_cli(prefix + ["run-pending", "--definitions", str(manifest)]) == 0
    assert json.loads(capsys.readouterr().out) == {"reason": "NO_PENDING_RUN", "status": "IDLE"}
    assert PipelineRuntimeStore(runtime_dir / "pipeline_runtime.sqlite3").list_runs() == []
    assert pipeline_cli(prefix + ["init"]) == 0
    capsys.readouterr()

    assert pipeline_cli(prefix + ["run-pending", "--definitions", str(manifest)]) == 0
    assert json.loads(capsys.readouterr().out) == {"reason": "NO_PENDING_RUN", "status": "IDLE"}

    assert pipeline_cli(prefix + ["run-pending", "--definitions", str(manifest), "--run-id", "missing"]) != 0
    assert "RUN_NOT_FOUND" in capsys.readouterr().err

    blocked, _ = PipelineRuntimeStore(runtime_dir / "pipeline_runtime.sqlite3").create_scheduled_run(
        item,
        scheduled_at=instant(),
        status=PipelineStatus.BLOCKED,
    )
    assert pipeline_cli(prefix + ["run-pending", "--definitions", str(manifest), "--run-id", blocked.run_id]) != 0
    assert "RUN_NOT_PENDING" in capsys.readouterr().err

    running, _ = PipelineRuntimeStore(runtime_dir / "pipeline_runtime.sqlite3").create_scheduled_run(
        item,
        scheduled_at=instant(0, 1),
    )
    store = PipelineRuntimeStore(runtime_dir / "pipeline_runtime.sqlite3")
    claim(store, running.run_id, item, tmp_path)
    assert pipeline_cli(prefix + ["run-pending", "--definitions", str(manifest), "--run-id", running.run_id]) != 0
    assert "RUN_NOT_PENDING" in capsys.readouterr().err

    missing = definition(tmp_path, "missing-definition")
    missing_run, _ = store.create_scheduled_run(missing, scheduled_at=instant(0, 2))
    assert pipeline_cli(prefix + ["run-pending", "--definitions", str(manifest), "--run-id", missing_run.run_id]) != 0
    assert "DEFINITION_MISSING" in capsys.readouterr().err

    mismatch = definition(tmp_path, "versioned", definition_version="v1")
    mismatch_run, _ = store.create_scheduled_run(mismatch, scheduled_at=instant(0, 3))
    write_manifest(manifest, [item, definition(tmp_path, "versioned", definition_version="v2")])
    assert pipeline_cli(prefix + ["run-pending", "--definitions", str(manifest), "--run-id", mismatch_run.run_id]) != 0
    assert "DEFINITION_VERSION_MISMATCH" in capsys.readouterr().err


def test_run_pending_cli_reports_success_failure_and_timeout(tmp_path: Path, capsys) -> None:
    runtime_dir = tmp_path / "runtime"
    manifest = tmp_path / "definitions.json"
    successful = definition(tmp_path, "success", command=(sys.executable, "-c", "pass"))
    failing = definition(tmp_path, "failure", command=(sys.executable, "-c", "raise SystemExit(7)"))
    timed_out = definition(
        tmp_path,
        "timeout",
        timeout_seconds=1,
        command=(sys.executable, "-c", "import time; time.sleep(10)"),
    )
    write_manifest(manifest, [successful, failing, timed_out])
    prefix = ["--runtime-dir", str(runtime_dir)]
    assert pipeline_cli(prefix + ["init"]) == 0
    capsys.readouterr()
    store = PipelineRuntimeStore(runtime_dir / "pipeline_runtime.sqlite3")
    success_run, _ = store.create_scheduled_run(successful, scheduled_at=instant())
    failure_run, _ = store.create_scheduled_run(failing, scheduled_at=instant(0, 1))
    timeout_run, _ = store.create_scheduled_run(timed_out, scheduled_at=instant(0, 2))

    assert pipeline_cli(prefix + ["run-pending", "--definitions", str(manifest), "--run-id", success_run.run_id]) == 0
    assert '"status": "SUCCESS"' in capsys.readouterr().out
    assert pipeline_cli(prefix + ["run-pending", "--definitions", str(manifest), "--run-id", failure_run.run_id]) != 0
    assert '"status": "FAILED"' in capsys.readouterr().out
    assert pipeline_cli(prefix + ["run-pending", "--definitions", str(manifest), "--run-id", timeout_run.run_id]) != 0
    assert '"status": "TIMED_OUT"' in capsys.readouterr().out


@pytest.mark.parametrize(
    ("dependency_map", "cycle"),
    [
        ({"A": ("A",)}, "A -> A"),
        ({"A": ("B",), "B": ("A",)}, "A -> B -> A"),
        ({"A": ("B",), "B": ("C",), "C": ("A",)}, "A -> B -> C -> A"),
        ({"A": ("B",), "B": ("A",), "C": ("D",), "D": ("C",)}, "A -> B -> A"),
    ],
)
def test_definition_cycles_fail_closed_without_scheduler_writes(
    tmp_path: Path,
    dependency_map: dict[str, tuple[str, ...]],
    cycle: str,
    capsys,
) -> None:
    manifest = tmp_path / "cyclic.json"
    write_manifest(
        manifest,
        [definition(tmp_path, pipeline_id, dependencies=dependencies) for pipeline_id, dependencies in dependency_map.items()],
    )
    with pytest.raises(DefinitionValidationError, match=cycle):
        load_definitions(manifest)
    runtime_dir = tmp_path / "runtime"
    assert pipeline_cli(["--runtime-dir", str(runtime_dir), "scan", "--definitions", str(manifest)]) != 0
    assert "pipeline dependency cycle detected" in capsys.readouterr().err
    assert not (runtime_dir / "pipeline_runtime.sqlite3").exists()


def test_definition_legal_disconnected_dags(tmp_path: Path) -> None:
    manifest = tmp_path / "dag.json"
    write_manifest(
        manifest,
        [
            definition(tmp_path, "A"),
            definition(tmp_path, "B", dependencies=("A",)),
            definition(tmp_path, "C"),
            definition(tmp_path, "D", dependencies=("C",)),
        ],
    )
    assert {item.pipeline_id for item in load_definitions(manifest)} == {"A", "B", "C", "D"}


def test_scheduler_cursor_catches_up_and_bounds_history(tmp_path: Path) -> None:
    store = create_store(tmp_path)
    item = definition(tmp_path, "every-five", schedule="*/5 * * * *", overlap_policy=OverlapPolicy.ALLOW)
    scheduler = PipelineScheduler(store, (item,))
    first = scheduler.scan(now=instant())
    assert len(first) == 1
    assert first.requested_start_at == instant()
    assert store.get_scheduler_cursor("default").last_scanned_at == instant()  # type: ignore[union-attr]

    catch_up = scheduler.scan(now=instant(0, 20))
    assert [run.scheduled_at for run in catch_up] == [instant(0, 5), instant(0, 10), instant(0, 15), instant(0, 20)]
    assert catch_up.requested_start_at == instant(0, 1)
    assert catch_up.scan_start_at == instant(0, 1)
    assert not catch_up.catch_up_limited
    assert len(scheduler.scan(now=instant(0, 20))) == 0

    limited_store = create_store(tmp_path / "limited")
    limited = PipelineScheduler(
        limited_store,
        (definition(tmp_path, "every-minute", overlap_policy=OverlapPolicy.ALLOW),),
        max_catch_up_minutes=5,
    )
    limited.scan(now=instant())
    result = limited.scan(now=instant(0, 20))
    assert result.catch_up_limited
    assert result.requested_start_at == instant(0, 1)
    assert result.scan_start_at == instant(0, 16)
    assert len(result) == 5
    assert limited_store.get_scheduler_cursor("default").last_scanned_at == instant(0, 20)  # type: ignore[union-attr]


def test_scheduler_cursor_no_match_error_and_timezones(tmp_path: Path, monkeypatch) -> None:
    store = create_store(tmp_path)
    no_match = definition(tmp_path, "no-match", schedule="0 1 * * *", overlap_policy=OverlapPolicy.ALLOW)
    scheduler = PipelineScheduler(store, (no_match,))
    scheduler.scan(now=instant())
    assert len(scheduler.scan(now=instant(0, 20))) == 0

    failing_store = create_store(tmp_path / "failing")
    failing = PipelineScheduler(failing_store, (definition(tmp_path, "failing"),))
    monkeypatch.setattr(failing_store, "commit_scheduler_scan", lambda **_: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError, match="boom"):
        failing.scan(now=instant())
    assert failing_store.get_scheduler_cursor("default") is None

    dst_store = create_store(tmp_path / "dst")
    dst = definition(
        tmp_path,
        "dst",
        schedule="30 1 * * *",
        timezone="America/New_York",
        overlap_policy=OverlapPolicy.ALLOW,
    )
    dst_scheduler = PipelineScheduler(dst_store, (dst,), max_catch_up_minutes=120)
    first = dst_scheduler.scan(now=datetime(2026, 11, 1, 5, 30, tzinfo=UTC))
    second = dst_scheduler.scan(now=datetime(2026, 11, 1, 6, 30, tzinfo=UTC))
    assert len(first) == len(second) == 1
    scheduled = dst_store.list_runs(pipeline_id="dst", limit=10)
    assert len(scheduled) == 2
    assert len({run.scheduled_at for run in scheduled}) == 2
    assert all(run.scheduled_at.tzinfo is not None for run in scheduled)


def test_concurrent_schedulers_commit_one_cursor_interval(tmp_path: Path) -> None:
    store = create_store(tmp_path)
    other_connection = PipelineRuntimeStore(store.database_path)
    item = definition(tmp_path, "concurrent", overlap_policy=OverlapPolicy.ALLOW)
    barrier = threading.Barrier(2)
    failures: list[BaseException] = []

    def scan(connection: PipelineRuntimeStore) -> None:
        try:
            barrier.wait()
            PipelineScheduler(connection, (item,)).scan(now=instant())
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    threads = [threading.Thread(target=scan, args=(store,)), threading.Thread(target=scan, args=(other_connection,))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert failures == []
    assert len(store.list_runs(pipeline_id="concurrent")) == 1
    assert store.get_scheduler_cursor("default").last_scanned_at == instant()  # type: ignore[union-attr]


def test_scheduler_scan_recovers_stale_runs_and_expired_leases(tmp_path: Path) -> None:
    store = create_store(tmp_path)
    item = definition(tmp_path, "recover", resource_locks=("quant_db_writer",), overlap_policy=OverlapPolicy.ALLOW)
    stale, _ = store.create_scheduled_run(item, scheduled_at=instant())
    store.claim_run(
        stale.run_id,
        pipeline_id=item.pipeline_id,
        definition_version=item.definition_version,
        overlap_policy=item.overlap_policy,
        resource_locks=item.resource_locks,
        stdout_path=tmp_path / "stale.out",
        stderr_path=tmp_path / "stale.err",
        lease_seconds=30,
        now=instant(),
    )
    fresh = definition(tmp_path, "fresh", resource_locks=("fresh_lock",), overlap_policy=OverlapPolicy.ALLOW)
    fresh_run, _ = store.create_scheduled_run(fresh, scheduled_at=instant())
    store.claim_run(
        fresh_run.run_id,
        pipeline_id=fresh.pipeline_id,
        definition_version=fresh.definition_version,
        overlap_policy=fresh.overlap_policy,
        resource_locks=fresh.resource_locks,
        stdout_path=tmp_path / "fresh.out",
        stderr_path=tmp_path / "fresh.err",
        lease_seconds=30,
        now=instant(),
    )
    assert store.heartbeat(fresh_run.run_id, lease_seconds=30, now=instant() + timedelta(seconds=40))
    scheduler = PipelineScheduler(
        store,
        (item, fresh),
        heartbeat_interval_seconds=5,
        lease_seconds=30,
        stale_after_seconds=31,
    )
    result = scheduler.scan(now=instant(0, 1))
    assert result.stale_runs_recovered == 1
    recovered = store.get_run(stale.run_id)
    assert recovered is not None and recovered.status is PipelineStatus.FAILED
    assert recovered.error_summary == "stale heartbeat recovery"
    assert not store.has_active_resource_lock("quant_db_writer", now=instant(0, 1))
    assert store.get_run(fresh_run.run_id).status is PipelineStatus.RUNNING  # type: ignore[union-attr]
    assert store.has_active_resource_lock("fresh_lock", now=instant(0, 1))
    assert any(run.pipeline_id == item.pipeline_id and run.status is PipelineStatus.PENDING for run in result)
    repeat = scheduler.scan(now=instant(0, 1))
    assert repeat.stale_runs_recovered == 0


def test_atomic_claim_enforces_overlap_locks_and_retries_under_threads(tmp_path: Path) -> None:
    same_store = create_store(tmp_path / "same")
    same_connection = PipelineRuntimeStore(same_store.database_path)
    same = definition(tmp_path, "same")
    same_run, _ = same_store.create_scheduled_run(same, scheduled_at=instant())
    barrier = threading.Barrier(2)
    results: list[str] = []

    def concurrent_claim(connection: PipelineRuntimeStore, run_id: str, item: PipelineDefinition) -> None:
        try:
            barrier.wait()
            claim(connection, run_id, item, tmp_path)
            results.append("SUCCESS")
        except RunClaimFailure as exc:
            results.append(exc.code)

    threads = [
        threading.Thread(target=concurrent_claim, args=(same_store, same_run.run_id, same)),
        threading.Thread(target=concurrent_claim, args=(same_connection, same_run.run_id, same)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(results) == ["RUN_NOT_PENDING", "SUCCESS"]

    store = create_store(tmp_path)
    second_connection = PipelineRuntimeStore(store.database_path)
    forbid = definition(tmp_path, "forbid")
    first, _ = store.create_scheduled_run(forbid, scheduled_at=instant())
    second, _ = store.create_scheduled_run(forbid, scheduled_at=instant(0, 1))
    barrier = threading.Barrier(2)
    results = []

    threads = [
        threading.Thread(target=concurrent_claim, args=(store, first.run_id, forbid)),
        threading.Thread(target=concurrent_claim, args=(second_connection, second.run_id, forbid)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(results) == ["OVERLAP_FORBIDDEN", "SUCCESS"]

    allow_store = create_store(tmp_path / "allow")
    allow_connection = PipelineRuntimeStore(allow_store.database_path)
    allow = definition(tmp_path, "allow", overlap_policy=OverlapPolicy.ALLOW)
    allow_a, _ = allow_store.create_scheduled_run(allow, scheduled_at=instant())
    allow_b, _ = allow_store.create_scheduled_run(allow, scheduled_at=instant(0, 1))
    barrier = threading.Barrier(2)
    results = []
    threads = [
        threading.Thread(target=concurrent_claim, args=(allow_store, allow_a.run_id, allow)),
        threading.Thread(target=concurrent_claim, args=(allow_connection, allow_b.run_id, allow)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results == ["SUCCESS", "SUCCESS"]

    locked_store = create_store(tmp_path / "locked")
    locked_connection = PipelineRuntimeStore(locked_store.database_path)
    locked = definition(
        tmp_path,
        "locked",
        overlap_policy=OverlapPolicy.ALLOW,
        resource_locks=("quant_db_writer",),
    )
    locked_a, _ = locked_store.create_scheduled_run(locked, scheduled_at=instant())
    locked_b, _ = locked_store.create_scheduled_run(locked, scheduled_at=instant(0, 1))
    barrier = threading.Barrier(2)
    results = []
    threads = [
        threading.Thread(target=concurrent_claim, args=(locked_store, locked_a.run_id, locked)),
        threading.Thread(target=concurrent_claim, args=(locked_connection, locked_b.run_id, locked)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(results) == ["RESOURCE_LOCK_UNAVAILABLE", "SUCCESS"]

    retry_store = create_store(tmp_path / "retry")
    retry_connection = PipelineRuntimeStore(retry_store.database_path)
    retry_definition = definition(tmp_path, "retry")
    failed, _ = retry_store.create_scheduled_run(retry_definition, scheduled_at=instant())
    claim(retry_store, failed.run_id, retry_definition, tmp_path)
    retry_store.finish_run(
        failed.run_id,
        status=PipelineStatus.FAILED,
        exit_code=1,
        timed_out=False,
        error_summary="fixture failure",
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
    )
    retry = retry_store.retry_run(failed.run_id)
    other, _ = retry_store.create_scheduled_run(retry_definition, scheduled_at=instant(0, 1))
    claim(retry_store, other.run_id, retry_definition, tmp_path)
    with pytest.raises(RunClaimFailure, match="OVERLAP_FORBIDDEN"):
        claim(retry_connection, retry.run_id, retry_definition, tmp_path)


@pytest.mark.parametrize("failure_mode", ["false", "exception"])
@pytest.mark.skipif(sys.platform == "win32", reason="process-group assertion uses POSIX signals")
def test_heartbeat_failure_stops_process_group_and_releases_lock(
    tmp_path: Path,
    failure_mode: str,
    monkeypatch,
) -> None:
    store = create_store(tmp_path)
    marker = tmp_path / f"child-survived-{failure_mode}"
    child = f"import time; from pathlib import Path; time.sleep(1); Path({str(marker)!r}).write_text('alive')"
    parent = f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', {child!r}]); time.sleep(10)"
    item = definition(
        tmp_path,
        f"heartbeat-{failure_mode}",
        resource_locks=("quant_db_writer",),
        command=(sys.executable, "-c", parent),
    )
    run, _ = store.create_scheduled_run(item, scheduled_at=instant())
    if failure_mode == "false":
        monkeypatch.setattr(store, "heartbeat", lambda *_, **__: False)
    else:
        def raise_sqlite(*_, **__) -> bool:
            raise sqlite3.OperationalError("fixture heartbeat failure")

        monkeypatch.setattr(store, "heartbeat", raise_sqlite)
    result = PipelineRunner(
        store,
        PipelineRuntimePaths(tmp_path / "runtime"),
        heartbeat_interval_seconds=0.05,
        lease_seconds=1,
    ).run(run.run_id, item)
    assert result.status is PipelineStatus.FAILED
    assert result.timed_out is False
    assert "heartbeat failure" in (result.error_summary or "")
    assert not store.has_active_resource_lock("quant_db_writer")
    time.sleep(1.2)
    assert not marker.exists()
