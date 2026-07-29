"""Execute one claimed Pipeline run with process-group timeout and heartbeats."""

from __future__ import annotations

import os
import json
import signal
import subprocess
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

try:
    import resource
except ImportError:  # pragma: no cover - Windows fallback
    resource = None  # type: ignore[assignment]

from .models import PipelineDefinition, PipelineRun, PipelineStatus
from .store import PipelineRuntimeStore


@dataclass(frozen=True, slots=True)
class PipelineRuntimePaths:
    runtime_dir: Path

    @property
    def database_path(self) -> Path:
        return self.runtime_dir / "pipeline_runtime.sqlite3"

    @property
    def logs_dir(self) -> Path:
        return self.runtime_dir / "logs"

    @property
    def results_dir(self) -> Path:
        return self.runtime_dir / "results"

    @classmethod
    def from_settings(cls, settings) -> "PipelineRuntimePaths":
        return cls(runtime_dir=settings.paths.pipeline_runtime_dir)


class PipelineRunner:
    """Runs argv definitions only; it never invokes a shell or an LLM."""

    def __init__(
        self,
        store: PipelineRuntimeStore,
        runtime_paths: PipelineRuntimePaths,
        *,
        heartbeat_interval_seconds: float = 5.0,
        lease_seconds: int = 30,
    ) -> None:
        if not lease_seconds > heartbeat_interval_seconds > 0:
            raise ValueError("lease_seconds must be greater than heartbeat_interval_seconds, both positive")
        self.store = store
        self.runtime_paths = runtime_paths
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.lease_seconds = lease_seconds

    def run(self, run_id: str, definition: PipelineDefinition) -> PipelineRun:
        """Atomically claim and execute one PENDING record."""

        self.runtime_paths.logs_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = self.runtime_paths.logs_dir / f"{run_id}.stdout.log"
        stderr_path = self.runtime_paths.logs_dir / f"{run_id}.stderr.log"
        claimed = self.store.claim_run(
            run_id,
            pipeline_id=definition.pipeline_id,
            definition_version=definition.definition_version,
            overlap_policy=definition.overlap_policy,
            resource_locks=definition.resource_locks,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            lease_seconds=self.lease_seconds,
        )
        return self._execute_claimed(claimed, definition)

    def _execute_claimed(self, claimed: PipelineRun, definition: PipelineDefinition) -> PipelineRun:
        if claimed.stdout_path is None or claimed.stderr_path is None:
            raise RuntimeError("claimed pipeline run is missing log paths")
        stdout_path = claimed.stdout_path
        stderr_path = claimed.stderr_path
        started = time.monotonic()
        before_usage = self._children_usage()
        stop_heartbeat = threading.Event()
        heartbeat_failed = threading.Event()
        heartbeat_failure_reason: list[str] = []
        heartbeat_failure_lock = threading.Lock()
        peak_rss_kb = [0]
        process: subprocess.Popen[bytes] | None = None
        result_payload: Mapping[str, object] | None = None
        result_path: Path | None = None

        def report_heartbeat_failure(reason: str) -> None:
            with heartbeat_failure_lock:
                if not heartbeat_failed.is_set():
                    heartbeat_failure_reason.append(reason)
                    heartbeat_failed.set()

        def heartbeat_loop() -> None:
            try:
                while not stop_heartbeat.wait(self.heartbeat_interval_seconds):
                    try:
                        healthy = self.store.heartbeat(claimed.run_id, lease_seconds=self.lease_seconds)
                    except BaseException as exc:  # Never lose lease-health failures in a daemon thread.
                        report_heartbeat_failure(f"heartbeat update raised {type(exc).__name__}: {exc}")
                        return
                    if not healthy:
                        report_heartbeat_failure("heartbeat update returned False")
                        return
                    if process is not None:
                        peak_rss_kb[0] = max(peak_rss_kb[0], self._sample_rss_kb(process.pid))
            except BaseException as exc:  # pragma: no cover - defensive protection for thread failures.
                report_heartbeat_failure(f"heartbeat thread raised {type(exc).__name__}: {exc}")
            finally:
                if not stop_heartbeat.is_set() and not heartbeat_failed.is_set():
                    report_heartbeat_failure("heartbeat thread exited unexpectedly")

        heartbeater: threading.Thread | None = None
        status = PipelineStatus.FAILED
        exit_code: int | None = None
        timed_out = False
        error_summary: str | None = None
        try:
            environment = self._environment(definition)
            environment.update(
                {
                    "QRP_PIPELINE_RUN_ID": claimed.run_id,
                    "QRP_PIPELINE_ID": claimed.pipeline_id,
                    "QRP_PIPELINE_SCHEDULED_FOR": claimed.scheduled_at.isoformat(),
                    "QRP_PIPELINE_ATTEMPT": str(claimed.attempt),
                }
            )
            if definition.requires_structured_result:
                self.runtime_paths.results_dir.mkdir(parents=True, exist_ok=True)
                result_path = self.runtime_paths.results_dir / f"{claimed.run_id}.json"
                environment["QRP_PIPELINE_RESULT_PATH"] = str(result_path)
            working_directory = str(definition.working_directory) if definition.working_directory else None
            with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
                popen_kwargs: dict[str, object] = {
                    "cwd": working_directory,
                    "env": environment,
                    "stdout": stdout,
                    "stderr": stderr,
                }
                if os.name == "posix":
                    popen_kwargs["start_new_session"] = True
                elif hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
                    popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
                process = subprocess.Popen(list(definition.command), **popen_kwargs)
                peak_rss_kb[0] = max(peak_rss_kb[0], self._sample_rss_kb(process.pid))
                heartbeater = threading.Thread(
                    target=heartbeat_loop,
                    name=f"pipeline-heartbeat-{claimed.run_id}",
                    daemon=True,
                )
                heartbeater.start()
                deadline = (
                    time.monotonic() + definition.timeout_seconds
                    if definition.timeout_seconds is not None
                    else None
                )
                poll_interval = min(0.25, max(0.05, self.heartbeat_interval_seconds / 2))
                while True:
                    if heartbeat_failed.is_set():
                        error_summary = f"heartbeat failure: {heartbeat_failure_reason[0]}"
                        self._terminate_process_group(process)
                        exit_code = process.wait()
                        break
                    exit_code = process.poll()
                    if exit_code is not None:
                        break
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        timed_out = True
                        error_summary = f"pipeline exceeded timeout_seconds={definition.timeout_seconds}"
                        self._terminate_process_group(process)
                        exit_code = process.wait()
                        break
                    heartbeat_failed.wait(timeout=poll_interval if remaining is None else min(poll_interval, remaining))
            if heartbeat_failed.is_set():
                status = PipelineStatus.FAILED
            elif timed_out:
                status = PipelineStatus.TIMED_OUT
            elif exit_code == 0:
                status = PipelineStatus.SUCCESS
            else:
                status = PipelineStatus.FAILED
                error_summary = f"process exited with code {exit_code}"
            if definition.requires_structured_result:
                result_payload, result_error = self._load_structured_result(
                    result_path,
                    run_id=claimed.run_id,
                    pipeline_id=claimed.pipeline_id,
                )
                if result_error is not None:
                    status = PipelineStatus.FAILED
                    error_summary = result_error
                elif result_payload is not None:
                    result_status = result_payload["status"]
                    if status is PipelineStatus.SUCCESS and result_status not in {"SUCCESS", "NOOP"}:
                        status = PipelineStatus.FAILED
                        error_summary = f"STRUCTURED_RESULT_STATUS_MISMATCH: runtime=SUCCESS result={result_status}"
                    elif status is not PipelineStatus.SUCCESS and result_status != "FAILED":
                        error_summary = (
                            error_summary
                            or f"STRUCTURED_RESULT_STATUS_MISMATCH: runtime={status.value} result={result_status}"
                        )
        except OSError as exc:
            error_summary = f"failed to start process: {type(exc).__name__}: {exc}"
        finally:
            stop_heartbeat.set()
            if heartbeater is not None:
                heartbeater.join(timeout=max(1.0, self.heartbeat_interval_seconds * 2))
            if process is not None:
                peak_rss_kb[0] = max(peak_rss_kb[0], self._sample_rss_kb(process.pid))
        after_usage = self._children_usage()
        duration_ms = max(0, int((time.monotonic() - started) * 1000))
        if result_payload is not None:
            try:
                self.store.record_result(claimed.run_id, result_payload)
            except Exception as exc:
                status = PipelineStatus.FAILED
                error_summary = f"failed to persist structured result: {type(exc).__name__}: {exc}"
        if definition.requires_structured_result:
            self._remove_transient_result(result_path)
        return self.store.finish_run(
            claimed.run_id,
            status=status,
            exit_code=exit_code,
            timed_out=timed_out,
            error_summary=error_summary,
            wall_duration_ms=duration_ms,
            user_cpu_ms=self._usage_delta_ms(before_usage, after_usage, "ru_utime"),
            system_cpu_ms=self._usage_delta_ms(before_usage, after_usage, "ru_stime"),
            peak_rss_kb=peak_rss_kb[0] or None,
        )

    def _environment(self, definition: PipelineDefinition) -> Mapping[str, str]:
        if definition.inherit_environment:
            environment = os.environ.copy()
        else:
            environment = {
                key: value
                for key in ("PATH", "LANG", "LC_ALL", "TZ")
                if (value := os.environ.get(key)) is not None
            }
        environment.update(definition.environment)
        return environment

    @staticmethod
    def _load_structured_result(
        result_path: Path | None,
        *,
        run_id: str,
        pipeline_id: str,
    ) -> tuple[Mapping[str, object] | None, str | None]:
        if result_path is None or not result_path.is_file():
            return None, "STRUCTURED_RESULT_MISSING"
        try:
            if result_path.stat().st_size > 5_000_000:
                return None, "STRUCTURED_RESULT_TOO_LARGE"
            payload = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return None, f"STRUCTURED_RESULT_INVALID: {type(exc).__name__}: {exc}"
        if not isinstance(payload, dict):
            return None, "STRUCTURED_RESULT_INVALID: result must be an object"
        if payload.get("run_id") != run_id or payload.get("pipeline_id") != pipeline_id:
            return None, "STRUCTURED_RESULT_IDENTITY_MISMATCH"
        if payload.get("status") not in {"SUCCESS", "FAILED", "NOOP"}:
            return None, "STRUCTURED_RESULT_INVALID_STATUS"
        return payload, None

    @staticmethod
    def _remove_transient_result(result_path: Path | None) -> None:
        """A result JSON is IPC only; durable history belongs in runtime SQLite."""

        if result_path is None:
            return
        try:
            result_path.unlink(missing_ok=True)
        except OSError:
            # Result persistence/status is authoritative even if best-effort file cleanup fails.
            pass

    @staticmethod
    def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
        """Terminate a whole child process group, escalating only when needed."""

        if process.poll() is not None:
            return
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                return
            try:
                process.wait(timeout=2)
                return
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                return
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()

    @staticmethod
    def _sample_rss_kb(pid: int) -> int:
        """Best-effort leader-process RSS sampling; unsupported platforms return zero."""

        status_path = Path(f"/proc/{pid}/status")
        try:
            for line in status_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
        except (OSError, ValueError, IndexError):
            pass
        return 0

    @staticmethod
    def _children_usage():
        return resource.getrusage(resource.RUSAGE_CHILDREN) if resource is not None else None

    @staticmethod
    def _usage_delta_ms(before, after, field_name: str) -> int | None:
        if before is None or after is None:
            return None
        return max(0, int((getattr(after, field_name) - getattr(before, field_name)) * 1000))
