from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from qrp_atlas.orchestration.cron import CronExpression
from qrp_atlas.jobs_cli import main as pipeline_cli
from qrp_atlas.orchestration.definitions import DefinitionValidationError, load_definitions
from qrp_atlas.orchestration.models import (
    OverlapPolicy,
    JobDefinition,
    JobStatus,
    assert_status_transition,
)
from qrp_atlas.orchestration.runner import JobRunner, JobRuntimePaths
from qrp_atlas.orchestration.scheduler import JobScheduler
from qrp_atlas.orchestration.store import JobRuntimeStore, JobClaimFailure


def instant(hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2026, 1, 2, hour, minute, tzinfo=UTC)


def definition(
    tmp_path: Path,
    job_id: str = "example",
    *,
    schedule: str = "0 8 * * *",
    dependencies: tuple[str, ...] = (),
    timeout_seconds: int | None = 5,
    overlap_policy: OverlapPolicy = OverlapPolicy.FORBID,
    resource_locks: tuple[str, ...] = (),
    command: tuple[str, ...] | None = None,
) -> JobDefinition:
    return JobDefinition(
        job_id=job_id,
        name=job_id,
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
def store(tmp_path: Path) -> JobRuntimeStore:
    result = JobRuntimeStore(tmp_path / "runtime" / "pipeline.sqlite3")
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
                        "job_id": "sample",
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
    assert definitions[0].job_id == "sample"
    assert CronExpression.parse("0 9 * * 1-5").matches(instant(1).astimezone(ZoneInfo(definitions[0].timezone)))
    assert CronExpression.parse("0 0 1 * 0").matches(datetime(2026, 1, 4, 0, 0, tzinfo=UTC))
    manifest.write_text('{"schema_version": 1, "definitions": [{"job_id": "bad"}]}', encoding="utf-8")
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
                        "job_id": "cli-sample",
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
    assert (runtime_dir / "job_runtime.sqlite3").exists()
    assert '"exit_code": 99' not in capsys.readouterr().out


def test_scheduler_same_schedule_point_is_idempotent_and_thread_safe(tmp_path: Path, store: JobRuntimeStore) -> None:
    item = definition(tmp_path)
    scan_time = instant(0)
    barrier = threading.Barrier(2)
    failures: list[BaseException] = []

    def scan() -> None:
        try:
            barrier.wait()
            JobScheduler(store, (item,)).scan(now=scan_time)
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    threads = [threading.Thread(target=scan), threading.Thread(target=scan)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert failures == []
    runs = store.list_runs(job_id=item.job_id)
    assert len(runs) == 1
    assert runs[0].status is JobStatus.PENDING


def test_scheduler_dependencies_and_blocked_status(tmp_path: Path, store: JobRuntimeStore) -> None:
    upstream = definition(tmp_path, "upstream", schedule="0 8 * * *")
    downstream = definition(tmp_path, "downstream", schedule="0 9 * * *", dependencies=("upstream",))
    scheduler = JobScheduler(store, (upstream, downstream))
    upstream_run = scheduler.scan(now=instant(0))[0]
    assert store.claim_run(
        upstream_run.run_id,
        job_id=upstream.job_id,
        definition_version=upstream.definition_version,
        overlap_policy=upstream.overlap_policy,
        resource_locks=(),
        stdout_path=tmp_path / "u.out",
        stderr_path=tmp_path / "u.err",
        lease_seconds=30,
        now=instant(0),
    )
    store.finish_run(
        upstream_run.run_id,
        status=JobStatus.SUCCESS,
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
    assert ready[0].job_id == "downstream"
    assert ready[0].status is JobStatus.PENDING

    failed, _ = store.create_scheduled_run(upstream, scheduled_at=instant(1, 30))
    assert store.claim_run(
        failed.run_id,
        job_id=upstream.job_id,
        definition_version=upstream.definition_version,
        overlap_policy=upstream.overlap_policy,
        resource_locks=(),
        stdout_path=tmp_path / "f.out",
        stderr_path=tmp_path / "f.err",
        lease_seconds=30,
        now=instant(1, 30),
    )
    store.finish_run(
        failed.run_id,
        status=JobStatus.FAILED,
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
    blocked = JobScheduler(store, (later,)).scan(now=instant(2))[0]
    assert blocked.status is JobStatus.BLOCKED
    assert "upstream" in (blocked.error_summary or "")
    recovered, _ = store.create_scheduled_run(upstream, scheduled_at=instant(1, 45))
    assert store.claim_run(
        recovered.run_id,
        job_id=upstream.job_id,
        definition_version=upstream.definition_version,
        overlap_policy=upstream.overlap_policy,
        resource_locks=(),
        stdout_path=tmp_path / "r.out",
        stderr_path=tmp_path / "r.err",
        lease_seconds=30,
        now=instant(2, 30),
    )
    store.finish_run(
        recovered.run_id,
        status=JobStatus.SUCCESS,
        exit_code=0,
        timed_out=False,
        error_summary=None,
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
        now=instant(2, 31),
    )
    JobScheduler(store, (later,)).scan(now=instant(3))
    assert store.get_run(blocked.run_id).status is JobStatus.PENDING  # type: ignore[union-attr]


def test_overlap_and_resource_leases(tmp_path: Path, store: JobRuntimeStore) -> None:
    single = definition(tmp_path, "single", schedule="*/5 * * * *")
    scheduler = JobScheduler(store, (single,))
    first = scheduler.scan(now=instant(0))[0]
    second = scheduler.scan(now=instant(0, 5))[0]
    assert first.status is JobStatus.PENDING
    assert second.status is JobStatus.BLOCKED

    writer_a = definition(tmp_path, "writer-a", resource_locks=("quant_db_writer",))
    writer_b = definition(tmp_path, "writer-b", resource_locks=("quant_db_writer",))
    run_a, _ = store.create_scheduled_run(writer_a, scheduled_at=instant(3))
    run_b, _ = store.create_scheduled_run(writer_b, scheduled_at=instant(4))
    assert store.claim_run(
        run_a.run_id,
        job_id=writer_a.job_id,
        definition_version=writer_a.definition_version,
        overlap_policy=writer_a.overlap_policy,
        resource_locks=writer_a.resource_locks,
        stdout_path=tmp_path / "a.out",
        stderr_path=tmp_path / "a.err",
        lease_seconds=1,
        now=instant(3),
    )
    with pytest.raises(JobClaimFailure, match="RESOURCE_LOCK_UNAVAILABLE"):
        store.claim_run(
            run_b.run_id,
            job_id=writer_b.job_id,
            definition_version=writer_b.definition_version,
            overlap_policy=writer_b.overlap_policy,
            resource_locks=writer_b.resource_locks,
            stdout_path=tmp_path / "b.out",
            stderr_path=tmp_path / "b.err",
            lease_seconds=1,
            now=instant(3),
        )
    stale_runs, expired_locks = store.recover_stale(stale_after_seconds=3600, now=instant(3) + timedelta(seconds=2))
    assert stale_runs == 0
    assert expired_locks == 1
    assert store.claim_run(
        run_b.run_id,
        job_id=writer_b.job_id,
        definition_version=writer_b.definition_version,
        overlap_policy=writer_b.overlap_policy,
        resource_locks=writer_b.resource_locks,
        stdout_path=tmp_path / "b.out",
        stderr_path=tmp_path / "b.err",
        lease_seconds=30,
        now=instant(4),
    )


def test_status_machine_and_stage_metrics(tmp_path: Path, store: JobRuntimeStore) -> None:
    with pytest.raises(ValueError):
        assert_status_transition(JobStatus.PENDING, JobStatus.SUCCESS)
    item = definition(tmp_path)
    run, _ = store.create_scheduled_run(item, scheduled_at=instant())
    stage = store.start_stage(run.run_id, "fetch", input_rows=3, metadata={"source": "fixture"}, now=instant())
    finished = store.finish_stage(
        run.run_id,
        "fetch",
        status=JobStatus.SUCCESS,
        output_rows=2,
        now=instant() + timedelta(milliseconds=15),
    )
    assert stage.status is JobStatus.RUNNING
    assert finished.duration_ms == 15
    assert finished.input_rows == 3
    assert finished.output_rows == 2


def test_cleanup_marks_zombie_running_run_failed(tmp_path: Path, store: JobRuntimeStore) -> None:
    item = definition(tmp_path, "zombie", resource_locks=("quant_db_writer",))
    run, _ = store.create_scheduled_run(item, scheduled_at=instant())
    assert store.claim_run(
        run.run_id,
        job_id=item.job_id,
        definition_version=item.definition_version,
        overlap_policy=item.overlap_policy,
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
    assert recovered is not None and recovered.status is JobStatus.FAILED
    assert "stale heartbeat" in (recovered.error_summary or "")
    payload = store.get_result(run.run_id)
    assert payload is not None
    assert payload["result_type"] == "orchestration"
    assert payload["status"] == "FAILED"
    assert payload["error_code"] == "ORCHESTRATION_STALE_RUN"
    assert payload["business_result"] is None


def test_every_terminal_run_has_exactly_one_queryable_result(tmp_path: Path, store: JobRuntimeStore) -> None:
    terminal_statuses = (
        JobStatus.SUCCESS,
        JobStatus.FAILED,
        JobStatus.TIMED_OUT,
        JobStatus.CANCELLED,
        JobStatus.SKIPPED,
    )
    run_ids: list[str] = []

    for index, status in enumerate(terminal_statuses):
        item = definition(tmp_path, f"terminal-{status.value.lower()}")
        run, _ = store.create_scheduled_run(
            item,
            scheduled_at=instant(4, index),
            status=JobStatus.SKIPPED if status is JobStatus.SKIPPED else JobStatus.PENDING,
            error_summary="fixture terminal outcome",
        )
        if status is not JobStatus.SKIPPED:
            store.claim_run(
                run.run_id,
                job_id=item.job_id,
                definition_version=item.definition_version,
                overlap_policy=item.overlap_policy,
                resource_locks=(),
                stdout_path=tmp_path / f"{status.value}.out",
                stderr_path=tmp_path / f"{status.value}.err",
                lease_seconds=30,
                now=instant(4, index),
            )
            run = store.finish_run(
                run.run_id,
                status=status,
                exit_code=0 if status is JobStatus.SUCCESS else 1,
                timed_out=status is JobStatus.TIMED_OUT,
                error_summary="fixture terminal outcome",
                wall_duration_ms=1,
                user_cpu_ms=0,
                system_cpu_ms=0,
                peak_rss_kb=1,
                result_payload=(
                    {"status": "SUCCESS", "business_payload": "must not survive"}
                    if status in {JobStatus.TIMED_OUT, JobStatus.CANCELLED}
                    else None
                ),
                now=instant(4, index) + timedelta(seconds=1),
            )
        assert run.status is status
        assert run.finished_at is not None
        run_ids.append(run.run_id)

    connection = sqlite3.connect(store.database_path)
    try:
        counts = connection.execute(
            "SELECT run_id, COUNT(*) FROM job_result GROUP BY run_id"
        ).fetchall()
    finally:
        connection.close()
    assert {run_id for run_id, count in counts if count == 1} == set(run_ids)
    assert all(count == 1 for _, count in counts)
    for run_id in run_ids:
        payload = store.get_result(run_id)
        assert payload is not None
        assert payload["result_type"] == "orchestration"
        assert payload["terminal_status"] in {status.value for status in terminal_statuses}
        assert payload["business_result"] is None


def test_initialize_backfills_terminal_result_from_older_runtime(tmp_path: Path, store: JobRuntimeStore) -> None:
    item = definition(tmp_path, "migrated")
    run, _ = store.create_scheduled_run(item, scheduled_at=instant(5))
    store.claim_run(
        run.run_id,
        job_id=item.job_id,
        definition_version=item.definition_version,
        overlap_policy=item.overlap_policy,
        resource_locks=(),
        stdout_path=tmp_path / "migrated.out",
        stderr_path=tmp_path / "migrated.err",
        lease_seconds=30,
        now=instant(5),
    )
    store.finish_run(
        run.run_id,
        status=JobStatus.FAILED,
        exit_code=1,
        timed_out=False,
        error_summary="legacy failure",
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
        now=instant(5, 1),
    )
    connection = sqlite3.connect(store.database_path)
    try:
        connection.execute("DELETE FROM job_result WHERE run_id = ?", [run.run_id])
        connection.commit()
    finally:
        connection.close()

    assert store.get_result(run.run_id) is None
    store.initialize()
    payload = store.get_result(run.run_id)
    assert payload is not None
    assert payload["result_type"] == "orchestration"
    assert payload["status"] == "FAILED"
    assert payload["error_code"] == "ORCHESTRATION_MIGRATED_TERMINAL"


def _create_historical_terminal_run(
    tmp_path: Path,
    store: JobRuntimeStore,
    status: JobStatus,
):
    item = definition(tmp_path, f"historical-{status.value.lower()}")
    run, _ = store.create_scheduled_run(
        item,
        scheduled_at=instant(10),
        status=JobStatus.SKIPPED if status is JobStatus.SKIPPED else JobStatus.PENDING,
        error_summary="historical terminal",
    )
    if status is JobStatus.SKIPPED:
        return run

    store.claim_run(
        run.run_id,
        job_id=item.job_id,
        definition_version=item.definition_version,
        overlap_policy=item.overlap_policy,
        resource_locks=(),
        stdout_path=tmp_path / "historical-terminal.out",
        stderr_path=tmp_path / "historical-terminal.err",
        lease_seconds=30,
        now=instant(10),
    )
    return store.finish_run(
        run.run_id,
        status=status,
        exit_code=1,
        timed_out=status is JobStatus.TIMED_OUT,
        error_summary="historical terminal",
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
        now=instant(10, 1),
    )


@pytest.mark.parametrize(
    "terminal_status",
    [JobStatus.TIMED_OUT, JobStatus.CANCELLED, JobStatus.SKIPPED],
)
@pytest.mark.parametrize(
    ("corruption", "value"),
    [
        ("missing_terminal_status", None),
        ("run_id", "wrong-run-id"),
        ("source", "legacy-runtime"),
        ("status", "FAILED"),
    ],
)
def test_initialize_repairs_noncanonical_terminal_orchestration_result(
    tmp_path: Path,
    store: JobRuntimeStore,
    terminal_status: JobStatus,
    corruption: str,
    value: str | None,
) -> None:
    run = _create_historical_terminal_run(tmp_path, store, terminal_status)
    payload = dict(store.get_result(run.run_id) or {})
    if corruption == "missing_terminal_status":
        del payload["terminal_status"]
    else:
        payload[corruption] = value

    connection = sqlite3.connect(store.database_path)
    try:
        connection.execute(
            "UPDATE job_result SET result_json = ? WHERE run_id = ?",
            [json.dumps(payload), run.run_id],
        )
        connection.commit()
    finally:
        connection.close()

    store.initialize()
    repaired = store.get_result(run.run_id)
    assert repaired is not None
    assert repaired["result_type"] == "orchestration"
    assert repaired["source"] == "orchestration"
    assert repaired["run_id"] == run.run_id
    assert repaired["status"] == terminal_status.value
    assert repaired["terminal_status"] == terminal_status.value
    assert repaired["business_result"] is None
    assert repaired["error_code"] == "ORCHESTRATION_MIGRATED_TERMINAL"


@pytest.mark.parametrize(
    "terminal_status",
    [JobStatus.TIMED_OUT, JobStatus.CANCELLED, JobStatus.SKIPPED],
)
def test_initialize_preserves_canonical_terminal_orchestration_result(
    tmp_path: Path,
    store: JobRuntimeStore,
    terminal_status: JobStatus,
) -> None:
    run = _create_historical_terminal_run(tmp_path, store, terminal_status)
    expected = dict(store.get_result(run.run_id) or {})
    expected["historical_marker"] = "preserve"

    connection = sqlite3.connect(store.database_path)
    try:
        connection.execute(
            "UPDATE job_result SET result_json = ? WHERE run_id = ?",
            [json.dumps(expected), run.run_id],
        )
        connection.commit()
    finally:
        connection.close()

    store.initialize()
    assert store.get_result(run.run_id) == expected


@pytest.mark.parametrize(
    ("requested_status", "result_payload", "expected_code"),
    [
        (JobStatus.SUCCESS, {"status": "FAILED"}, "ORCHESTRATION_RESULT_STATUS_MISMATCH"),
        (JobStatus.FAILED, {"status": "SUCCESS"}, "ORCHESTRATION_RESULT_STATUS_MISMATCH"),
        (
            JobStatus.SUCCESS,
            {"status": "SUCCESS", "not_json": object()},
            "ORCHESTRATION_RESULT_INVALID",
        ),
    ],
)
def test_finish_run_closes_direct_payload_mismatch_or_serialization_failure(
    tmp_path: Path,
    store: JobRuntimeStore,
    requested_status: JobStatus,
    result_payload: dict[str, object],
    expected_code: str,
) -> None:
    item = definition(tmp_path, "direct-result-boundary")
    run, _ = store.create_scheduled_run(item, scheduled_at=instant(6))
    store.claim_run(
        run.run_id,
        job_id=item.job_id,
        definition_version=item.definition_version,
        overlap_policy=item.overlap_policy,
        resource_locks=(),
        stdout_path=tmp_path / "direct.out",
        stderr_path=tmp_path / "direct.err",
        lease_seconds=30,
        now=instant(6),
    )

    finished = store.finish_run(
        run.run_id,
        status=requested_status,
        exit_code=0 if requested_status is JobStatus.SUCCESS else 1,
        timed_out=False,
        error_summary=None,
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
        result_payload=result_payload,
        now=instant(6, 1),
    )

    assert finished.status is JobStatus.FAILED
    payload = store.get_result(finished.run_id)
    assert payload is not None
    assert payload["result_type"] == "orchestration"
    assert payload["status"] == "FAILED"
    assert payload["error_code"] == expected_code
    assert payload["business_result"] is None


def test_finish_run_never_reuses_nonterminal_prewrite_as_terminal_evidence(
    tmp_path: Path,
    store: JobRuntimeStore,
) -> None:
    item = definition(tmp_path, "prewrite-boundary")
    run, _ = store.create_scheduled_run(item, scheduled_at=instant(7))
    store.record_result(run.run_id, {"status": "SUCCESS", "prewrite": True})
    store.claim_run(
        run.run_id,
        job_id=item.job_id,
        definition_version=item.definition_version,
        overlap_policy=item.overlap_policy,
        resource_locks=(),
        stdout_path=tmp_path / "prewrite.out",
        stderr_path=tmp_path / "prewrite.err",
        lease_seconds=30,
        now=instant(7),
    )

    finished = store.finish_run(
        run.run_id,
        status=JobStatus.SUCCESS,
        exit_code=0,
        timed_out=False,
        error_summary=None,
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
        now=instant(7, 1),
    )

    assert finished.status is JobStatus.SUCCESS
    payload = store.get_result(finished.run_id)
    assert payload is not None
    assert payload["result_type"] == "orchestration"
    assert payload["status"] == "SUCCESS"
    assert payload["error_code"] == "ORCHESTRATION_RESULT_MISSING"
    assert payload["business_result"] is None


@pytest.mark.parametrize(
    "raw_result",
    ["not-json", "null", '{"status": "FAILED"}', '{"status": []}'],
)
def test_initialize_repairs_historical_malformed_or_mismatched_result(
    tmp_path: Path,
    store: JobRuntimeStore,
    raw_result: str,
) -> None:
    item = definition(tmp_path, "historical-invalid")
    run, _ = store.create_scheduled_run(item, scheduled_at=instant(8))
    store.claim_run(
        run.run_id,
        job_id=item.job_id,
        definition_version=item.definition_version,
        overlap_policy=item.overlap_policy,
        resource_locks=(),
        stdout_path=tmp_path / "historical.out",
        stderr_path=tmp_path / "historical.err",
        lease_seconds=30,
        now=instant(8),
    )
    store.finish_run(
        run.run_id,
        status=JobStatus.SUCCESS,
        exit_code=0,
        timed_out=False,
        error_summary=None,
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
        result_payload={"status": "SUCCESS", "business": "valid before corruption"},
        now=instant(8, 1),
    )
    connection = sqlite3.connect(store.database_path)
    try:
        connection.execute(
            "UPDATE job_result SET result_json = ? WHERE run_id = ?",
            [raw_result, run.run_id],
        )
        connection.commit()
    finally:
        connection.close()

    store.initialize()
    payload = store.get_result(run.run_id)
    assert payload is not None
    assert payload["result_type"] == "orchestration"
    assert payload["status"] == "SUCCESS"
    assert payload["error_code"] == "ORCHESTRATION_MIGRATED_TERMINAL"
    assert payload["business_result"] is None


def test_initialize_preserves_matching_historical_business_result(
    tmp_path: Path,
    store: JobRuntimeStore,
) -> None:
    item = definition(tmp_path, "historical-valid")
    run, _ = store.create_scheduled_run(item, scheduled_at=instant(9))
    store.claim_run(
        run.run_id,
        job_id=item.job_id,
        definition_version=item.definition_version,
        overlap_policy=item.overlap_policy,
        resource_locks=(),
        stdout_path=tmp_path / "historical-valid.out",
        stderr_path=tmp_path / "historical-valid.err",
        lease_seconds=30,
        now=instant(9),
    )
    expected = {"status": "SUCCESS", "business": "preserve"}
    store.finish_run(
        run.run_id,
        status=JobStatus.SUCCESS,
        exit_code=0,
        timed_out=False,
        error_summary=None,
        wall_duration_ms=1,
        user_cpu_ms=0,
        system_cpu_ms=0,
        peak_rss_kb=1,
        result_payload=expected,
        now=instant(9, 1),
    )

    store.initialize()
    assert store.get_result(run.run_id) == expected


def test_runner_success_nonzero_timeout_heartbeat_and_retry(tmp_path: Path, store: JobRuntimeStore) -> None:
    paths = JobRuntimePaths(tmp_path / "runtime")
    runner = JobRunner(store, paths, heartbeat_interval_seconds=0.05, lease_seconds=1)

    successful = definition(
        tmp_path,
        "success",
        command=(sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr)"),
    )
    success_run, _ = store.create_scheduled_run(successful, scheduled_at=instant())
    success_result = runner.run(success_run.run_id, successful)
    assert success_result is not None and success_result.status is JobStatus.SUCCESS
    assert success_result.wall_duration_ms is not None
    assert success_result.stdout_path is not None and success_result.stdout_path.read_text().strip() == "out"
    assert success_result.stderr_path is not None and success_result.stderr_path.read_text().strip() == "err"

    failing = definition(tmp_path, "failure", command=(sys.executable, "-c", "import sys; sys.exit(7)"))
    failed_run, _ = store.create_scheduled_run(failing, scheduled_at=instant(1))
    failed_result = runner.run(failed_run.run_id, failing)
    assert failed_result is not None and failed_result.status is JobStatus.FAILED
    assert failed_result.exit_code == 7
    retry = store.retry_run(failed_result.run_id)
    assert retry.attempt == 2
    assert store.get_run(failed_result.run_id).status is JobStatus.FAILED  # type: ignore[union-attr]

    timed = definition(
        tmp_path,
        "timed",
        timeout_seconds=1,
        command=(sys.executable, "-c", "import time; time.sleep(10)"),
    )
    timed_run, _ = store.create_scheduled_run(timed, scheduled_at=instant(2))
    timed_result = runner.run(timed_run.run_id, timed)
    assert timed_result is not None and timed_result.status is JobStatus.TIMED_OUT
    assert timed_result.timed_out is True
    timeout_payload = store.get_result(timed_result.run_id)
    assert timeout_payload is not None
    assert timeout_payload["result_type"] == "orchestration"
    assert timeout_payload["status"] == "TIMED_OUT"
    assert timeout_payload["error_code"] == "ORCHESTRATION_TIMEOUT"
    assert timeout_payload["business_result"] is None

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
    assert during is not None and during.status is JobStatus.RUNNING
    assert during.heartbeat_at is not None and during.started_at is not None
    assert during.heartbeat_at > during.started_at
    assert result_box[0].status is JobStatus.SUCCESS


@pytest.mark.skipif(sys.platform == "win32", reason="process-group assertion uses POSIX signals")
def test_runner_timeout_terminates_child_process_group(tmp_path: Path, store: JobRuntimeStore) -> None:
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
    result = JobRunner(store, JobRuntimePaths(tmp_path / "runtime"), heartbeat_interval_seconds=0.05).run(
        run.run_id, item
    )
    assert result is not None and result.status is JobStatus.TIMED_OUT
    time.sleep(1.2)
    assert not marker.exists()
