"""End-to-end coverage for the long-running Pipeline service surface."""

from __future__ import annotations

import json
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.pipeline.runtime.cli import main as pipeline_cli
from qrp_atlas.pipeline.runtime.models import OverlapPolicy, PipelineDefinition, PipelineStatus
from qrp_atlas.pipeline.runtime.service import PipelineService
from qrp_atlas.pipeline.runtime.store import PipelineRuntimeStore, RunClaimFailure
from qrp_atlas.pipeline.runtime.runner import PipelineRuntimePaths


def instant(hour: int, minute: int) -> datetime:
    return datetime(2026, 7, 30, hour, minute, tzinfo=UTC)


def fixture_definition(
    tmp_path: Path,
    pipeline_id: str,
    *,
    schedule: str = "* * * * *",
    dependencies: tuple[str, ...] = (),
    succeeds: bool = True,
    resource_locks: tuple[str, ...] = (),
    resource_reads: tuple[str, ...] = (),
    sleep_seconds: float = 0.0,
) -> tuple[PipelineDefinition, Path]:
    marker = tmp_path / f"{pipeline_id}.marker"
    marker.parent.mkdir(parents=True, exist_ok=True)
    source = (
        "from pathlib import Path; import os, time; "
        f"time.sleep({sleep_seconds!r}); "
        "Path(os.environ['FIXTURE_MARKER']).write_text(os.environ['QRP_PIPELINE_ID'], encoding='utf-8')"
    )
    command = (sys.executable, "-c", source if succeeds else "raise SystemExit(7)")
    return (
        PipelineDefinition(
            pipeline_id=pipeline_id,
            name=pipeline_id,
            enabled=True,
            schedule=schedule,
            timezone="Asia/Shanghai",
            command=command,
            working_directory=None,
            dependencies=dependencies,
            timeout_seconds=10,
            max_retries=1,
            overlap_policy=OverlapPolicy.FORBID,
            resource_locks=resource_locks,
            resource_reads=resource_reads,
            inherit_environment=False,
            environment={"FIXTURE_MARKER": str(marker)},
        ),
        marker,
    )


def service(tmp_path: Path, definitions: tuple[PipelineDefinition, ...], *, owner_id: str = "fixture-service") -> PipelineService:
    paths = PipelineRuntimePaths(
        tmp_path / "runtime",
        result_logs_dir_override=tmp_path / "external-audit" / "pipeline",
    )
    return PipelineService(
        PipelineRuntimeStore(paths.database_path),
        paths,
        definitions,
        scheduler_id="fixture-scheduler",
        heartbeat_interval_seconds=0.05,
        lease_seconds=1,
        stale_after_seconds=2,
        max_catch_up_minutes=30,
        poll_interval_seconds=0.05,
        service_name="fixture-service",
        owner_id=owner_id,
    )


class _InProcessResult:
    def __init__(self, run: object, *, status: str = "SUCCESS", diagnostics: list[dict[str, object]] | None = None) -> None:
        self.run = run
        self.status = status
        self.diagnostics = diagnostics or []

    def as_dict(self) -> dict[str, object]:
        run = self.run
        return {
            "run_id": run.run_id,
            "pipeline_id": run.pipeline_id,
            "status": self.status,
            "target_window": {"target_date": run.scheduled_at.date().isoformat()},
            "metrics": {"rows_written": 1},
            "outputs": [],
            "diagnostics": self.diagnostics,
        }


def in_process_definition(
    tmp_path: Path,
    pipeline_id: str,
    *,
    table: str,
    resource_locks: tuple[str, ...] = (),
    resource_reads: tuple[str, ...] = (),
    sleep_seconds: float = 0.0,
    started: threading.Event | None = None,
) -> PipelineDefinition:
    database = tmp_path / "shared.duckdb"

    def execute(run: object) -> _InProcessResult:
        if started is not None:
            started.set()
        time.sleep(sleep_seconds)
        connection = duckdb.connect(str(database))
        try:
            connection.execute(f"INSERT INTO {table} VALUES (?)", [run.run_id])
        finally:
            connection.close()
        return _InProcessResult(run)

    return PipelineDefinition(
        pipeline_id=pipeline_id,
        name=pipeline_id,
        enabled=True,
        schedule="* * * * *",
        timezone="Asia/Shanghai",
        command=(),
        working_directory=None,
        dependencies=(),
        timeout_seconds=10,
        max_retries=0,
        overlap_policy=OverlapPolicy.ALLOW,
        resource_locks=resource_locks,
        resource_reads=resource_reads,
        definition_version="in-process-v1",
        in_process_executor=execute,
    )
def test_service_waits_until_target_then_bootstraps_and_does_not_repeat(tmp_path: Path) -> None:
    definition, marker = fixture_definition(tmp_path, "daily", schedule="5 8 * * *")
    first = service(tmp_path, (definition,))
    first.start()
    try:
        before = first.run_once(now=instant(0, 4))
        assert before.executed_runs == ()
        assert not marker.exists()
        assert not list((tmp_path / "external-audit" / "pipeline").glob("*.jsonl"))
        after = first.run_once(now=instant(0, 5))
        assert [run.status for run in after.executed_runs] == [PipelineStatus.SUCCESS]
        assert marker.read_text(encoding="utf-8") == "daily"
    finally:
        first.stop()

    # Restarting after the target time locates the same deterministic scheduled
    # occurrence but the unique durable run prevents duplicate execution.
    restarted = service(tmp_path, (definition,), owner_id="fixture-service-restarted")
    restarted.start()
    try:
        result = restarted.run_once(now=instant(0, 6))
        assert result.executed_runs == ()
    finally:
        restarted.stop()

    store = PipelineRuntimeStore(tmp_path / "runtime" / "pipeline_runtime.sqlite3")
    runs = store.list_runs(pipeline_id="daily")
    assert len(runs) == 1
    assert runs[0].status is PipelineStatus.SUCCESS


def test_service_continues_after_task_failure_and_writes_safe_result_logs(tmp_path: Path) -> None:
    failing, failing_marker = fixture_definition(tmp_path, "failing", succeeds=False)
    independent, independent_marker = fixture_definition(tmp_path, "independent")
    blocked, _ = fixture_definition(tmp_path, "blocked", dependencies=("failing",))
    runtime = service(tmp_path, (failing, independent, blocked))
    runtime.start()
    try:
        cycle = runtime.run_once(now=instant(0, 0))
    finally:
        runtime.stop()

    outcomes = {run.pipeline_id: run.status for run in cycle.executed_runs}
    assert outcomes == {"failing": PipelineStatus.FAILED, "independent": PipelineStatus.SUCCESS}
    assert not failing_marker.exists()
    assert independent_marker.read_text(encoding="utf-8") == "independent"
    store = PipelineRuntimeStore(tmp_path / "runtime" / "pipeline_runtime.sqlite3")
    assert store.list_runs(pipeline_id="blocked")[0].status is PipelineStatus.BLOCKED
    records = [json.loads(line) for path in (tmp_path / "external-audit" / "pipeline").glob("*.jsonl") for line in path.read_text(encoding="utf-8").splitlines()]
    assert {record["pipeline_id"] for record in records} == {"failing", "independent"}
    assert all(set(record) >= {"run_id", "business_date", "tasks", "duration_ms", "status"} for record in records)
    assert "FIXTURE_MARKER" not in json.dumps(records)


def test_service_lease_rejects_second_scheduler_process(tmp_path: Path) -> None:
    definition, _ = fixture_definition(tmp_path, "only_once")
    first = service(tmp_path, (definition,), owner_id="first")
    second = service(tmp_path, (definition,), owner_id="second")
    first.start()
    try:
        with pytest.raises(RunClaimFailure, match="SCHEDULER_SERVICE_ACTIVE"):
            second.start()
    finally:
        first.stop()


def test_service_parallelizes_only_resource_independent_tasks(tmp_path: Path) -> None:
    independent_a, _ = fixture_definition(tmp_path, "independent_a", sleep_seconds=0.35)
    independent_b, _ = fixture_definition(tmp_path, "independent_b", sleep_seconds=0.35)
    concurrent = service(tmp_path / "concurrent", (independent_a, independent_b))
    concurrent.start()
    try:
        started = time.monotonic()
        concurrent_cycle = concurrent.run_once(now=instant(0, 0))
        concurrent_elapsed = time.monotonic() - started
    finally:
        concurrent.stop()
    assert {run.status for run in concurrent_cycle.executed_runs} == {PipelineStatus.SUCCESS}

    writer, _ = fixture_definition(
        tmp_path / "serial",
        "writer",
        sleep_seconds=0.35,
        resource_locks=("shared_duckdb",),
    )
    reader, _ = fixture_definition(
        tmp_path / "serial",
        "reader",
        sleep_seconds=0.35,
        resource_reads=("shared_duckdb",),
    )
    serialized = service(tmp_path / "serial", (writer, reader))
    serialized.start()
    try:
        started = time.monotonic()
        serialized_cycle = serialized.run_once(now=instant(0, 0))
        serialized_elapsed = time.monotonic() - started
    finally:
        serialized.stop()
    assert {run.status for run in serialized_cycle.executed_runs} == {PipelineStatus.SUCCESS}
    # The same Runtime and host execute both batches.  A shared writer/read
    # pair must wait for two child durations, while independent tasks overlap.
    assert serialized_elapsed > concurrent_elapsed + 0.20


def test_formal_contract_runs_in_serve_process_without_stdout_stderr_files(tmp_path: Path) -> None:
    database = tmp_path / "shared.duckdb"
    connection = duckdb.connect(str(database))
    try:
        connection.execute("CREATE TABLE target (run_id VARCHAR)")
    finally:
        connection.close()
    definition = in_process_definition(
        tmp_path,
        "formal_in_process",
        table="target",
        resource_locks=(f"duckdb://{database}#target",),
    )
    runtime_paths = PipelineRuntimePaths(
        tmp_path / "runtime",
        result_logs_dir_override=tmp_path / "audit" / "pipeline",
    )
    store = PipelineRuntimeStore(runtime_paths.database_path)
    run, _ = store.create_scheduled_run(definition, scheduled_at=instant(0, 0))
    result = PipelineService(
        store,
        runtime_paths,
        (definition,),
        scheduler_id="in-process-scheduler",
        heartbeat_interval_seconds=0.05,
        lease_seconds=1,
        stale_after_seconds=2,
        max_catch_up_minutes=30,
        poll_interval_seconds=0.05,
        max_workers=1,
        service_name="in-process-service",
    )
    result.start()
    try:
        cycle = result.run_once(now=instant(0, 0))
    finally:
        result.stop()
    assert cycle.executed_runs[0].run_id == run.run_id
    assert cycle.executed_runs[0].status is PipelineStatus.SUCCESS
    assert cycle.executed_runs[0].stdout_path is None
    assert cycle.executed_runs[0].stderr_path is None
    assert not (runtime_paths.logs_dir).exists()
    assert store.get_result(run.run_id)["status"] == "SUCCESS"  # type: ignore[index]
    connection = duckdb.connect(str(database), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM target").fetchone()[0] == 1
    finally:
        connection.close()


def test_same_duckdb_file_different_tables_run_concurrently(tmp_path: Path) -> None:
    database = tmp_path / "shared.duckdb"
    connection = duckdb.connect(str(database))
    try:
        connection.execute("CREATE TABLE table_a (run_id VARCHAR)")
        connection.execute("CREATE TABLE table_b (run_id VARCHAR)")
    finally:
        connection.close()
    table_a = in_process_definition(
        tmp_path,
        "table_a_writer",
        table="table_a",
        resource_locks=(f"duckdb://{database}#table_a",),
        sleep_seconds=0.30,
    )
    table_b = in_process_definition(
        tmp_path,
        "table_b_writer",
        table="table_b",
        resource_locks=(f"duckdb://{database}#table_b",),
        sleep_seconds=0.30,
    )
    runtime = service(tmp_path, (table_a, table_b))
    runtime.start()
    try:
        started = time.monotonic()
        cycle = runtime.run_once(now=instant(0, 0))
        elapsed = time.monotonic() - started
    finally:
        runtime.stop()
    assert {run.status for run in cycle.executed_runs} == {PipelineStatus.SUCCESS}
    assert elapsed < 0.75


def test_same_duckdb_table_read_write_conflict_is_serialized(tmp_path: Path) -> None:
    database = tmp_path / "shared.duckdb"
    connection = duckdb.connect(str(database))
    try:
        connection.execute("CREATE TABLE shared_table (run_id VARCHAR)")
    finally:
        connection.close()
    writer = in_process_definition(
        tmp_path,
        "table_writer",
        table="shared_table",
        resource_locks=(f"duckdb://{database}#shared_table",),
        sleep_seconds=0.30,
    )
    reader = in_process_definition(
        tmp_path,
        "table_reader",
        table="shared_table",
        resource_reads=(f"duckdb://{database}#shared_table",),
        sleep_seconds=0.30,
    )
    runtime = service(tmp_path, (writer, reader))
    runtime.start()
    try:
        started = time.monotonic()
        cycle = runtime.run_once(now=instant(0, 0))
        elapsed = time.monotonic() - started
    finally:
        runtime.stop()
    assert {run.status for run in cycle.executed_runs} == {PipelineStatus.SUCCESS}
    assert elapsed > 0.55


def test_formal_failure_summary_is_bounded_and_redacted(tmp_path: Path) -> None:
    def fail(_run: object) -> _InProcessResult:
        raise RuntimeError("token=super-secret " + "x" * 1_000)

    definition = PipelineDefinition(
        pipeline_id="formal_failure",
        name="formal_failure",
        enabled=True,
        schedule="* * * * *",
        timezone="Asia/Shanghai",
        command=(),
        working_directory=None,
        dependencies=(),
        timeout_seconds=10,
        max_retries=0,
        overlap_policy=OverlapPolicy.ALLOW,
        resource_locks=(),
        in_process_executor=fail,
    )
    runtime = service(tmp_path, (definition,))
    runtime.start()
    try:
        cycle = runtime.run_once(now=instant(0, 0))
    finally:
        runtime.stop()
    failed = cycle.executed_runs[0]
    assert failed.status is PipelineStatus.FAILED
    assert failed.error_summary is not None and len(failed.error_summary) <= 500
    assert "super-secret" not in failed.error_summary
    assert "[REDACTED]" in failed.error_summary


def test_cli_submits_to_active_service_without_running_business_process(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    definition, marker = fixture_definition(tmp_path, "submitted", schedule="0 0 1 1 *")
    manifest = tmp_path / "submitted-definitions.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "definitions": [
                    {
                        "pipeline_id": definition.pipeline_id,
                        "name": definition.name,
                        "enabled": definition.enabled,
                        "schedule": definition.schedule,
                        "timezone": definition.timezone,
                        "command": list(definition.command),
                        "working_directory": None,
                        "dependencies": [],
                        "timeout_seconds": definition.timeout_seconds,
                        "max_retries": definition.max_retries,
                        "overlap_policy": definition.overlap_policy.value,
                        "resource_locks": [],
                        "resource_reads": [],
                        "environment": dict(definition.environment),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    paths = PipelineRuntimePaths(
        tmp_path / "runtime",
        result_logs_dir_override=tmp_path / "audit" / "pipeline",
    )
    runtime = PipelineService(
        PipelineRuntimeStore(paths.database_path),
        paths,
        (definition,),
        scheduler_id="submit-scheduler",
        heartbeat_interval_seconds=0.05,
        lease_seconds=1,
        stale_after_seconds=2,
        max_catch_up_minutes=30,
        service_name="submit-service",
        owner_id="submit-owner",
    )
    scheduled_for = datetime.now(UTC).replace(microsecond=0)
    runtime.start()
    try:
        assert (
            pipeline_cli(
                [
                    "--runtime-dir",
                    str(paths.runtime_dir),
                    "--result-log-dir",
                    str(paths.result_logs_dir),
                    "run",
                    "submitted",
                    "--definitions",
                    str(manifest),
                    "--scheduled-for",
                    scheduled_for.isoformat(),
                ]
            )
            == 0
        )
        submission = json.loads(capsys.readouterr().out)
        assert submission["status"] == "PENDING"
        assert submission["submitted_to_service"] is True
        assert not marker.exists()
        cycle = runtime.run_once(now=scheduled_for)
    finally:
        runtime.stop()
    assert [run.pipeline_id for run in cycle.executed_runs] == ["submitted"]
    assert marker.read_text(encoding="utf-8") == "submitted"


def test_cli_serve_once_executes_due_definition_and_reports_stopped_afterward(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    definition, marker = fixture_definition(tmp_path, "cli_service", schedule="* * * * *")
    manifest = tmp_path / "serve-definitions.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "definitions": [
                    {
                        "pipeline_id": definition.pipeline_id,
                        "name": definition.name,
                        "enabled": definition.enabled,
                        "schedule": definition.schedule,
                        "timezone": definition.timezone,
                        "command": list(definition.command),
                        "working_directory": None,
                        "dependencies": [],
                        "timeout_seconds": definition.timeout_seconds,
                        "max_retries": definition.max_retries,
                        "overlap_policy": definition.overlap_policy.value,
                        "resource_locks": [],
                        "environment": dict(definition.environment),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    runtime_dir = tmp_path / "cli-runtime"
    audit_dir = tmp_path / "cli-audit" / "pipeline"
    assert (
        pipeline_cli(
            [
                "--runtime-dir",
                str(runtime_dir),
                "--result-log-dir",
                str(audit_dir),
                "serve",
                "--definitions",
                str(manifest),
                "--once",
            ]
        )
        == 0
    )
    assert marker.read_text(encoding="utf-8") == "cli_service"
    served = json.loads(capsys.readouterr().out)
    assert served["status"] == "SERVED_ONCE"
    assert served["executed_runs"] == 1
    assert pipeline_cli(["--runtime-dir", str(runtime_dir), "health"]) == 1
    health = json.loads(capsys.readouterr().out)
    assert health["status"] == "STOPPED"
    assert len(list(audit_dir.glob("pipeline-results-*.jsonl"))) == 1


def test_cli_manual_dependency_chain_and_external_log(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    upstream, upstream_marker = fixture_definition(tmp_path, "upstream", schedule="0 8 * * *")
    downstream, downstream_marker = fixture_definition(
        tmp_path,
        "downstream",
        schedule="1 8 * * *",
        dependencies=("upstream",),
    )
    manifest = tmp_path / "definitions.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "definitions": [
                    {
                        "pipeline_id": item.pipeline_id,
                        "name": item.name,
                        "enabled": item.enabled,
                        "schedule": item.schedule,
                        "timezone": item.timezone,
                        "command": list(item.command),
                        "working_directory": None,
                        "dependencies": list(item.dependencies),
                        "timeout_seconds": item.timeout_seconds,
                        "max_retries": item.max_retries,
                        "overlap_policy": item.overlap_policy.value,
                        "resource_locks": [],
                        "environment": dict(item.environment),
                    }
                    for item in (upstream, downstream)
                ],
            }
        ),
        encoding="utf-8",
    )
    runtime_dir = tmp_path / "runtime"
    audit_dir = tmp_path / "audit" / "pipeline"
    code = pipeline_cli(
        [
            "--runtime-dir",
            str(runtime_dir),
            "--result-log-dir",
            str(audit_dir),
            "run",
            "downstream",
            "--definitions",
            str(manifest),
            "--target-date",
            "2026-07-30",
            "--with-dependencies",
        ]
    )
    assert code == 0
    assert upstream_marker.read_text(encoding="utf-8") == "upstream"
    assert downstream_marker.read_text(encoding="utf-8") == "downstream"
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [item["pipeline_id"] for item in output] == ["upstream", "downstream"]
    assert (
        pipeline_cli(
            [
                "--runtime-dir",
                str(runtime_dir),
                "status",
                "--run-id",
                output[-1]["run_id"],
                "--include-result",
            ]
        )
        == 0
    )
    status = json.loads(capsys.readouterr().out)
    assert status["pipeline_id"] == "downstream"
    assert status["status"] == "SUCCESS"
    assert len(list(audit_dir.glob("pipeline-results-*.jsonl"))) == 1
