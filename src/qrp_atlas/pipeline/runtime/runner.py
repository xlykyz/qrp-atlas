"""Execute one claimed Pipeline run with process-group timeout and heartbeats."""

from __future__ import annotations

import os
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
        if heartbeat_interval_seconds <= 0 or lease_seconds <= 0:
            raise ValueError("heartbeat interval and lease duration must be positive")
        self.store = store
        self.runtime_paths = runtime_paths
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.lease_seconds = lease_seconds

    def run(self, run_id: str, definition: PipelineDefinition) -> PipelineRun | None:
        """Claim and execute one PENDING record, returning None when not claimable."""

        existing = self.store.get_run(run_id)
        if existing is None:
            raise KeyError(f"unknown pipeline run {run_id}")
        if definition.pipeline_id != existing.pipeline_id:
            raise ValueError("definition does not match pipeline run")
        if existing.definition_version != definition.definition_version:
            raise ValueError("definition version does not match pipeline run")
        self.runtime_paths.logs_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = self.runtime_paths.logs_dir / f"{run_id}.stdout.log"
        stderr_path = self.runtime_paths.logs_dir / f"{run_id}.stderr.log"
        claimed = self.store.claim_run(
            run_id,
            resource_locks=definition.resource_locks,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            lease_seconds=self.lease_seconds,
        )
        if claimed is None:
            return None
        return self._execute_claimed(claimed, definition)

    def _execute_claimed(self, claimed: PipelineRun, definition: PipelineDefinition) -> PipelineRun:
        if claimed.stdout_path is None or claimed.stderr_path is None:
            raise RuntimeError("claimed pipeline run is missing log paths")
        stdout_path = claimed.stdout_path
        stderr_path = claimed.stderr_path
        started = time.monotonic()
        before_usage = self._children_usage()
        stop_heartbeat = threading.Event()
        peak_rss_kb = [0]
        process: subprocess.Popen[bytes] | None = None

        def heartbeat_loop() -> None:
            while not stop_heartbeat.wait(self.heartbeat_interval_seconds):
                self.store.heartbeat(claimed.run_id, lease_seconds=self.lease_seconds)
                if process is not None:
                    peak_rss_kb[0] = max(peak_rss_kb[0], self._sample_rss_kb(process.pid))

        heartbeater = threading.Thread(target=heartbeat_loop, name=f"pipeline-heartbeat-{claimed.run_id}", daemon=True)
        heartbeater.start()
        status = PipelineStatus.FAILED
        exit_code: int | None = None
        timed_out = False
        error_summary: str | None = None
        try:
            environment = self._environment(definition)
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
                try:
                    exit_code = process.wait(timeout=definition.timeout_seconds)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    error_summary = f"pipeline exceeded timeout_seconds={definition.timeout_seconds}"
                    self._terminate_process_group(process)
                    exit_code = process.wait()
            if timed_out:
                status = PipelineStatus.TIMED_OUT
            elif exit_code == 0:
                status = PipelineStatus.SUCCESS
            else:
                status = PipelineStatus.FAILED
                error_summary = f"process exited with code {exit_code}"
        except OSError as exc:
            error_summary = f"failed to start process: {type(exc).__name__}: {exc}"
        finally:
            stop_heartbeat.set()
            heartbeater.join(timeout=max(1.0, self.heartbeat_interval_seconds * 2))
            if process is not None:
                peak_rss_kb[0] = max(peak_rss_kb[0], self._sample_rss_kb(process.pid))
        after_usage = self._children_usage()
        duration_ms = max(0, int((time.monotonic() - started) * 1000))
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
