from __future__ import annotations

import json
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from qrp_atlas.pipeline.runtime.cron import CronExpression
from qrp_atlas.pipeline.runtime.cli import main as pipeline_cli
from qrp_atlas.pipeline.runtime.definitions import DefinitionValidationError, load_definitions
from qrp_atlas.pipeline.runtime.models import (
    OverlapPolicy,
    PipelineDefinition,
    PipelineStatus,
    assert_status_transition,
)
from qrp_atlas.pipeline.runtime.runner import PipelineRunner, PipelineRuntimePaths
from qrp_atlas.pipeline.runtime.scheduler import PipelineScheduler
from qrp_atlas.pipeline.runtime.store import PipelineRuntimeStore


def instant(hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2026, 1, 2, hour, minute, tzinfo=UTC)


def definition(
    tmp_path: Path,
    pipeline_id: str = "example",
    *,
    schedule: str = "0 8 * * *",
    dependencies: tuple[str, ...] = (),
    timeout_seconds: int | None = 5,
    overlap_policy: OverlapPolicy = OverlapPolicy.FORBID,
    resource_locks: tuple[str, ...] = (),
    command: tuple[str, ...] | None = None,
) -> PipelineDefinition:
    return PipelineDefinition(
        pipeline_id=pipeline_id,
        name=pipeline_id,
        enabled=True,
        schedule=schedule,
        timezone="Asia/Shanghai",
        command=command or (sys.executable, "-c", "print('ok')"),
        working_directory=tmp_path,
        dependencies=dependencies,
        timeout_seconds=timeout_seconds,
        max_retries=2,
        overlap_policy=overlap_policy,
        resource_locks=resource_locks,
        definition_version="test-v1",
    )


@pytest.fixture
def store(tmp_path: Path) -> PipelineRuntimeStore:
    result = PipelineRuntimeStore(tmp_path / "runtime" / "pipeline.sqlite3")
    result.initialize()
    return result


def test_definition_manifest_validation_and_timezone(tmp_path: Path) -> None:
    manifest = tmp_path / "definitions.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "definitions": [
                    {
                        "pipeline_id": "sample",
                        "name": "Sample",
                        "enabled": True,
                        "schedule": "0 9 * * 1-5",
                        "timezone": "Asia/Shanghai",
                        "command": [sys.executable, "-c", "pass"],
                        "working_directory": str(tmp_path),
                        "dependencies": [],
                        "timeout_seconds": 60,
                        "max_retries": 1,
                        "overlap_policy": "FORBID",
                        "resource_locks": ["quant_db_writer"],
                        "performance_budget": {"wall_duration_ms": 60000},
                        "freshness_checks": [],
                        "definition_version": "v1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    definitions = load_definitions(manifest)
    assert definitions[0].pipeline_id == "sample"
    assert CronExpression.parse("0 9 * * 1-5").matches(instant(1).astimezone(ZoneInfo(definitions[0].timezone)))
    assert CronExpression.parse("0 0 1 * 0").matches(datetime(2026, 1, 4, 0, 0, tzinfo=UTC))
    manifest.write_text('{"schema_version": 1, "definitions": [{"pipeline_id": "bad"}]}', encoding="utf-8")
    with pytest.raises(DefinitionValidationError):
        load_definitions(manifest)


def test_cli_uses_only_explicit_temporary_runtime(tmp_path: Path, capsys) -> None:
    runtime_dir = tmp_path / "runtime"
    manifest = tmp_path / "definitions.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "definitions": [
                    {
                        "pipeline_id": "cli-sample",
                        "name": "CLI sample",
                        "enabled": True,
                        "schedule": "0 8 * * *",
                        "timezone": "Asia/Shanghai",
                        "command": [sys.executable, "-c", "raise SystemExit(99)"],
                        "working_directory": str(tmp_path),
                        "dependencies": [],
                        "timeout_seconds": 60,
                        "max_retries": 0,
                        "overlap_policy": "FORBID",
                        "resource_locks": [],
                        "performance_budget": {},
                        "freshness_checks": [],
                        "definition_version": "v1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    prefix = ["--runtime-dir", str(runtime_dir)]
    assert pipeline_cli(prefix + ["init"]) == 0
    assert pipeline_cli(prefix + ["validate-definitions", "--definitions", str(manifest)]) == 0
    assert pipeline_cli(prefix + ["scan", "--definitions", str(manifest), "--at", "2026-01-02T00:00:00Z"]) == 0
    assert pipeline_cli(prefix + ["status", "--pipeline-id", "cli-sample"]) == 0
    assert pipeline_cli(prefix + ["cleanup", "--stale-after-seconds", "60"]) == 0
    assert (runtime_dir / "pipeline_runtime.sqlite3").exists()
    assert "99" not in capsys.readouterr().out


def test_scheduler_same_schedule_point_is_idempotent_and_thread_safe(tmp_path: Path, store: PipelineRuntimeStore) -> None:
    item = definition(tmp_path)
    scan_time = instant(0)
    barrier = threading.Barrier(2)
    failures: list[BaseException] = []

    def scan() -> None:
        try:
            barrier.wait()
            PipelineScheduler(store, (item,)).scan(now=scan_time)
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    threads = [threading.Thread(target=scan), threading.Thread(target=scan)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert failures == []
    runs = store.list_runs(pipeline_id=item.pipeline_id)
    assert len(runs) == 1
    assert runs[0].status is PipelineStatus.PENDING


def test_scheduler_dependencies_and_blocked_status(tmp_path: Path, store: PipelineRuntimeStore) -> None:
    upstream = definition(tmp_path, "upstream", schedule="0 8 * * *")
    downstream = definition(tmp_path, "downstream", schedule="0 9 * * *", dependencies=("upstream",))
    scheduler = PipelineScheduler(store, (upstream, downstream))
    upstream_run = scheduler.scan(now=instant(0))[0]
    assert store.claim_run(
        upstream_run.run_id,
        resource_locks=(),
        stdout_path=tmp_path / "u.out",
        stderr_path=tmp_path / "u.err",
        lease_seconds=30,
        now=instant(0),
    )
    store.finish_run(
        upstream_run.run_id,
        status=PipelineStatus.SUCCESS,
        exit_code=0,
        timed_out=False,
        error_summary=None,
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
        now=instant(0, 1),
    )
    ready = scheduler.scan(now=instant(1))
    assert ready[0].pipeline_id == "downstream"
    assert ready[0].status is PipelineStatus.PENDING

    failed, _ = store.create_scheduled_run(upstream, scheduled_at=instant(1, 30))
    assert store.claim_run(
        failed.run_id,
        resource_locks=(),
        stdout_path=tmp_path / "f.out",
        stderr_path=tmp_path / "f.err",
        lease_seconds=30,
        now=instant(1, 30),
    )
    store.finish_run(
        failed.run_id,
        status=PipelineStatus.FAILED,
        exit_code=1,
        timed_out=False,
        error_summary="failure",
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
        now=instant(1, 31),
    )
    later = definition(tmp_path, "later", schedule="0 10 * * *", dependencies=("upstream",))
    blocked = PipelineScheduler(store, (later,)).scan(now=instant(2))[0]
    assert blocked.status is PipelineStatus.BLOCKED
    assert "upstream" in (blocked.error_summary or "")
    recovered, _ = store.create_scheduled_run(upstream, scheduled_at=instant(1, 45))
    assert store.claim_run(
        recovered.run_id,
        resource_locks=(),
        stdout_path=tmp_path / "r.out",
        stderr_path=tmp_path / "r.err",
        lease_seconds=30,
        now=instant(2, 30),
    )
    store.finish_run(
        recovered.run_id,
        status=PipelineStatus.SUCCESS,
        exit_code=0,
        timed_out=False,
        error_summary=None,
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
        now=instant(2, 31),
    )
    PipelineScheduler(store, (later,)).scan(now=instant(3))
    assert store.get_run(blocked.run_id).status is PipelineStatus.PENDING  # type: ignore[union-attr]


def test_overlap_and_resource_leases(tmp_path: Path, store: PipelineRuntimeStore) -> None:
    single = definition(tmp_path, "single", schedule="*/5 * * * *")
    scheduler = PipelineScheduler(store, (single,))
    first = scheduler.scan(now=instant(0))[0]
    second = scheduler.scan(now=instant(0, 5))[0]
    assert first.status is PipelineStatus.PENDING
    assert second.status is PipelineStatus.BLOCKED

    writer_a = definition(tmp_path, "writer-a", resource_locks=("quant_db_writer",))
    writer_b = definition(tmp_path, "writer-b", resource_locks=("quant_db_writer",))
    run_a, _ = store.create_scheduled_run(writer_a, scheduled_at=instant(3))
    run_b, _ = store.create_scheduled_run(writer_b, scheduled_at=instant(4))
    assert store.claim_run(
        run_a.run_id,
        resource_locks=writer_a.resource_locks,
        stdout_path=tmp_path / "a.out",
        stderr_path=tmp_path / "a.err",
        lease_seconds=1,
        now=instant(3),
    )
    assert store.claim_run(
        run_b.run_id,
        resource_locks=writer_b.resource_locks,
        stdout_path=tmp_path / "b.out",
        stderr_path=tmp_path / "b.err",
        lease_seconds=1,
        now=instant(3),
    ) is None
    stale_runs, expired_locks = store.recover_stale(stale_after_seconds=3600, now=instant(3) + timedelta(seconds=2))
    assert stale_runs == 0
    assert expired_locks == 1
    assert store.claim_run(
        run_b.run_id,
        resource_locks=writer_b.resource_locks,
        stdout_path=tmp_path / "b.out",
        stderr_path=tmp_path / "b.err",
        lease_seconds=30,
        now=instant(4),
    )


def test_status_machine_and_stage_metrics(tmp_path: Path, store: PipelineRuntimeStore) -> None:
    with pytest.raises(ValueError):
        assert_status_transition(PipelineStatus.PENDING, PipelineStatus.SUCCESS)
    item = definition(tmp_path)
    run, _ = store.create_scheduled_run(item, scheduled_at=instant())
    stage = store.start_stage(run.run_id, "fetch", input_rows=3, metadata={"source": "fixture"}, now=instant())
    finished = store.finish_stage(
        run.run_id,
        "fetch",
        status=PipelineStatus.SUCCESS,
        output_rows=2,
        now=instant() + timedelta(milliseconds=15),
    )
    assert stage.status is PipelineStatus.RUNNING
    assert finished.duration_ms == 15
    assert finished.input_rows == 3
    assert finished.output_rows == 2


def test_cleanup_marks_zombie_running_run_failed(tmp_path: Path, store: PipelineRuntimeStore) -> None:
    item = definition(tmp_path, "zombie", resource_locks=("quant_db_writer",))
    run, _ = store.create_scheduled_run(item, scheduled_at=instant())
    assert store.claim_run(
        run.run_id,
        resource_locks=item.resource_locks,
        stdout_path=tmp_path / "z.out",
        stderr_path=tmp_path / "z.err",
        lease_seconds=30,
        now=instant(),
    )
    stale_runs, expired_locks = store.recover_stale(stale_after_seconds=1, now=instant() + timedelta(seconds=2))
    assert stale_runs == 1
    assert expired_locks == 0
    recovered = store.get_run(run.run_id)
    assert recovered is not None and recovered.status is PipelineStatus.FAILED
    assert "stale heartbeat" in (recovered.error_summary or "")


def test_runner_success_nonzero_timeout_heartbeat_and_retry(tmp_path: Path, store: PipelineRuntimeStore) -> None:
    paths = PipelineRuntimePaths(tmp_path / "runtime")
    runner = PipelineRunner(store, paths, heartbeat_interval_seconds=0.05, lease_seconds=1)

    successful = definition(
        tmp_path,
        "success",
        command=(sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr)"),
    )
    success_run, _ = store.create_scheduled_run(successful, scheduled_at=instant())
    success_result = runner.run(success_run.run_id, successful)
    assert success_result is not None and success_result.status is PipelineStatus.SUCCESS
    assert success_result.wall_duration_ms is not None
    assert success_result.stdout_path is not None and success_result.stdout_path.read_text().strip() == "out"
    assert success_result.stderr_path is not None and success_result.stderr_path.read_text().strip() == "err"

    failing = definition(tmp_path, "failure", command=(sys.executable, "-c", "import sys; sys.exit(7)"))
    failed_run, _ = store.create_scheduled_run(failing, scheduled_at=instant(1))
    failed_result = runner.run(failed_run.run_id, failing)
    assert failed_result is not None and failed_result.status is PipelineStatus.FAILED
    assert failed_result.exit_code == 7
    retry = store.retry_run(failed_result.run_id)
    assert retry.attempt == 2
    assert store.get_run(failed_result.run_id).status is PipelineStatus.FAILED  # type: ignore[union-attr]

    timed = definition(
        tmp_path,
        "timed",
        timeout_seconds=1,
        command=(sys.executable, "-c", "import time; time.sleep(10)"),
    )
    timed_run, _ = store.create_scheduled_run(timed, scheduled_at=instant(2))
    timed_result = runner.run(timed_run.run_id, timed)
    assert timed_result is not None and timed_result.status is PipelineStatus.TIMED_OUT
    assert timed_result.timed_out is True

    beating = definition(
        tmp_path,
        "heartbeat",
        command=(sys.executable, "-c", "import time; time.sleep(0.35)"),
    )
    beating_run, _ = store.create_scheduled_run(beating, scheduled_at=instant(3))
    result_box: list = []
    worker = threading.Thread(target=lambda: result_box.append(runner.run(beating_run.run_id, beating)))
    worker.start()
    time.sleep(0.15)
    during = store.get_run(beating_run.run_id)
    worker.join()
    assert during is not None and during.status is PipelineStatus.RUNNING
    assert during.heartbeat_at is not None and during.started_at is not None
    assert during.heartbeat_at > during.started_at
    assert result_box[0].status is PipelineStatus.SUCCESS


@pytest.mark.skipif(sys.platform == "win32", reason="process-group assertion uses POSIX signals")
def test_runner_timeout_terminates_child_process_group(tmp_path: Path, store: PipelineRuntimeStore) -> None:
    marker = tmp_path / "child-survived"
    child = f"import time; from pathlib import Path; time.sleep(1); Path({str(marker)!r}).write_text('alive')"
    parent = f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', {child!r}]); time.sleep(10)"
    item = definition(
        tmp_path,
        "group-timeout",
        timeout_seconds=1,
        command=(sys.executable, "-c", parent),
    )
    run, _ = store.create_scheduled_run(item, scheduled_at=instant())
    result = PipelineRunner(store, PipelineRuntimePaths(tmp_path / "runtime"), heartbeat_interval_seconds=0.05).run(
        run.run_id, item
    )
    assert result is not None and result.status is PipelineStatus.TIMED_OUT
    time.sleep(1.2)
    assert not marker.exists()
