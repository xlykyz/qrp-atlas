"""Failure-path coverage for formal PipelineResult handoff to the runtime."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from qrp_atlas.orchestration.models import OverlapPolicy, JobDefinition, JobStatus
from qrp_atlas.orchestration.runner import JobRunner, JobRuntimePaths
from qrp_atlas.orchestration.store import JobRuntimeStore


def instant() -> datetime:
    return datetime(2026, 1, 2, tzinfo=UTC)


def definition(
    tmp_path: Path,
    *,
    command: tuple[str, ...],
    requires_structured_result: bool = True,
) -> JobDefinition:
    return JobDefinition(
        job_id="structured_result_fixture",
        name="structured result fixture",
        enabled=True,
        schedule="* * * * *",
        timezone="UTC",
        command=command,
        working_directory=tmp_path,
        dependencies=(),
        timeout_seconds=5,
        max_retries=0,
        overlap_policy=OverlapPolicy.FORBID,
        resource_locks=(),
        definition_version="test-v1",
        requires_structured_result=requires_structured_result,
    )


def run_item(tmp_path: Path, item: JobDefinition) -> tuple[JobRuntimeStore, JobRuntimePaths, object]:
    paths = JobRuntimePaths(tmp_path / "runtime")
    store = JobRuntimeStore(paths.database_path)
    run, _ = store.create_scheduled_run(item, scheduled_at=instant())
    result = JobRunner(store, paths, heartbeat_interval_seconds=0.05, lease_seconds=1).run(run.run_id, item)
    return store, paths, result


def result_writer(status: str, *, job_id_expression: str = "os.environ['QRP_PIPELINE_ID']") -> tuple[str, ...]:
    script = f"""
import json
import os
from pathlib import Path
payload = {{
    'run_id': os.environ['QRP_PIPELINE_RUN_ID'],
    'job_id': {job_id_expression},
    'status': {status!r},
}}
Path(os.environ['QRP_PIPELINE_RESULT_PATH']).write_text(json.dumps(payload), encoding='utf-8')
"""
    return (sys.executable, "-c", script)


@pytest.mark.parametrize(
    ("command", "expected_error"),
    [
        ((sys.executable, "-c", "pass"), "STRUCTURED_RESULT_MISSING"),
        (
            (
                sys.executable,
                "-c",
                "import os; from pathlib import Path; Path(os.environ['QRP_PIPELINE_RESULT_PATH']).write_text('{bad json')",
            ),
            "STRUCTURED_RESULT_INVALID:",
        ),
        (result_writer("SUCCESS", job_id_expression="'wrong_pipeline'"), "STRUCTURED_RESULT_IDENTITY_MISMATCH"),
    ],
)
def test_structured_result_missing_invalid_or_mismatched_fails_closed(
    tmp_path: Path,
    command: tuple[str, ...],
    expected_error: str,
) -> None:
    store, paths, result = run_item(tmp_path, definition(tmp_path, command=command))

    assert result.status is JobStatus.FAILED
    assert expected_error in (result.error_summary or "")
    payload = store.get_result(result.run_id)
    assert payload is not None
    assert payload["result_type"] == "orchestration"
    assert payload["status"] == "FAILED"
    assert payload["business_result"] is None
    assert expected_error in str(payload["error_summary"])
    assert not (paths.results_dir / f"{result.run_id}.json").exists()


def test_exit_zero_with_failed_structured_result_fails_and_persists_evidence(tmp_path: Path) -> None:
    store, paths, result = run_item(tmp_path, definition(tmp_path, command=result_writer("FAILED")))

    assert result.status is JobStatus.FAILED
    assert "STRUCTURED_RESULT_STATUS_MISMATCH" in (result.error_summary or "")
    payload = store.get_result(result.run_id)
    assert payload is not None and payload["status"] == "FAILED"
    assert not (paths.results_dir / f"{result.run_id}.json").exists()


def test_failed_subprocess_discards_success_payload_and_synthesizes_result(tmp_path: Path) -> None:
    script = result_writer("SUCCESS")[-1] + "\nraise SystemExit(7)\n"
    store, paths, result = run_item(
        tmp_path,
        definition(tmp_path, command=(sys.executable, "-c", script)),
    )

    assert result.status is JobStatus.FAILED
    payload = store.get_result(result.run_id)
    assert payload is not None
    assert payload["result_type"] == "orchestration"
    assert payload["status"] == "FAILED"
    assert payload["business_result"] is None
    assert not (paths.results_dir / f"{result.run_id}.json").exists()


def test_terminal_transition_persists_structured_result(tmp_path: Path) -> None:
    item = definition(tmp_path, command=result_writer("SUCCESS"))
    paths = JobRuntimePaths(tmp_path / "runtime")
    store = JobRuntimeStore(paths.database_path)
    run, _ = store.create_scheduled_run(item, scheduled_at=instant())

    result = JobRunner(store, paths, heartbeat_interval_seconds=0.05, lease_seconds=1).run(run.run_id, item)

    assert result.status is JobStatus.SUCCESS
    payload = store.get_result(result.run_id)
    assert payload is not None
    assert payload["status"] == "SUCCESS"
    assert payload.get("result_type") != "orchestration"
    assert not (paths.results_dir / f"{result.run_id}.json").exists()


def test_legacy_definition_does_not_require_or_persist_structured_result(tmp_path: Path) -> None:
    command = (
        sys.executable,
        "-c",
        "import os; assert 'QRP_PIPELINE_RESULT_PATH' not in os.environ",
    )
    store, paths, result = run_item(
        tmp_path,
        definition(tmp_path, command=command, requires_structured_result=False),
    )

    assert result.status is JobStatus.SUCCESS
    payload = store.get_result(result.run_id)
    assert payload is not None
    assert payload["result_type"] == "orchestration"
    assert payload["status"] == "SUCCESS"
    assert payload["business_result"] is None
    assert not paths.results_dir.exists()
