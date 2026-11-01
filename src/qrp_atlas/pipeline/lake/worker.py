"""Bounded subprocess execution for the lake runtime, not a second scheduler.

Dagster owns retries, run state and concurrency. This module owns only the
lifetime of a single child process. Linux cgroups/identities remain necessary
for hard memory and filesystem isolation; polling is not a security boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable, Mapping, Sequence
import os
import signal
import subprocess
import sys
import time

import psutil


@dataclass(frozen=True)
class WorkerBudget:
    timeout_seconds: float = 1800
    memory_bytes: int = 8 * 1024**3
    temporary_bytes: int = 16 * 1024**3
    threads: int = 2
    poll_seconds: float = 0.1

    def __post_init__(self):
        if min(self.timeout_seconds, self.memory_bytes, self.temporary_bytes,
               self.threads, self.poll_seconds) <= 0:
            raise ValueError("Worker budgets must be positive")


@dataclass(frozen=True)
class WorkerResult:
    returncode: int
    duration_seconds: float
    peak_rss_bytes: int
    stdout_path: Path
    stderr_path: Path


class WorkerFailure(RuntimeError):
    def __init__(self, code: str, result: WorkerResult | None = None):
        super().__init__(code)  # never echo argv, environment or provider output
        self.code = code
        self.result = result


def child_environment(supplied: Mapping[str, str], budget: WorkerBudget,
                      temporary_directory: Path) -> dict[str, str]:
    """Use an explicit environment, never inherit catalog/provider credentials."""
    result = dict(supplied)
    for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
        result[key] = str(budget.threads)
    result.update(TMP=str(temporary_directory), TEMP=str(temporary_directory),
                  TMPDIR=str(temporary_directory), PYTHONUNBUFFERED="1")
    return result


def _stop_tree(process: subprocess.Popen, known: dict[int, psutil.Process]):
    # Include descendants already observed even if their parent exited first.
    if os.name != "nt":
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    else:
        try:
            known.update({p.pid: p for p in psutil.Process(process.pid).children(recursive=True)})
        except psutil.NoSuchProcess:
            pass
    for child in reversed(list(known.values())):
        try:
            child.kill()
        except psutil.NoSuchProcess:
            pass
    if process.poll() is None:
        process.kill()
    process.wait(timeout=10)
    psutil.wait_procs(list(known.values()), timeout=5)


def run_worker(argv: Sequence[str], *, directory: Path, budget: WorkerBudget,
               environment: Mapping[str, str],
               cancel_requested: Callable[[], bool] = lambda: False) -> WorkerResult:
    """Run once; fail closed on timeout, cancellation, budget or nonzero exit.

    The caller gives a NEW per-attempt directory. Logs and temp files are kept
    there for diagnosis; no live directory is recursively deleted. RSS and
    disk limits are sampled and may overshoot; use cgroups/quotas in production.
    """
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    temporary = directory / "tmp"
    temporary.mkdir()
    stdout_path, stderr_path = directory / "stdout.log", directory / "stderr.log"
    if cancel_requested():
        raise WorkerFailure("WORKER_CANCELLED")
    started = time.monotonic()
    peak = 0
    known: dict[int, psutil.Process] = {}
    reason = None
    flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
        guarded = [sys.executable, str(Path(__file__).with_name("worker_guard.py")),
                   str(os.getpid()), str(psutil.Process().create_time()), *argv]
        process = subprocess.Popen(guarded, cwd=directory, env=child_environment(environment, budget, temporary),
                                   stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                                   start_new_session=os.name != "nt", creationflags=flags)
        try:
            parent = psutil.Process(process.pid)
            while process.poll() is None:
                if cancel_requested():
                    reason = "WORKER_CANCELLED"
                    break
                if time.monotonic() - started >= budget.timeout_seconds:
                    reason = "WORKER_TIMEOUT"
                    break
                try:
                    children = parent.children(recursive=True)
                    known.update({p.pid: p for p in children})
                    rss = 0
                    for p in [parent, *children]:
                        try:
                            rss += p.memory_info().rss
                        except psutil.NoSuchProcess:
                            pass
                    peak = max(peak, rss)
                    if rss > budget.memory_bytes:
                        reason = "WORKER_MEMORY_LIMIT"
                        break
                except psutil.NoSuchProcess:
                    pass
                disk = sum(p.stat().st_size for p in directory.rglob("*") if p.is_file())
                if disk > budget.temporary_bytes:
                    reason = "WORKER_DISK_LIMIT"
                    break
                time.sleep(budget.poll_seconds)
        except BaseException:
            _stop_tree(process, known)
            raise
        finally:
            # On successful root exit also terminate any remaining descendants.
            _stop_tree(process, known)
    result = WorkerResult(process.returncode, time.monotonic() - started, peak, stdout_path, stderr_path)
    if reason:
        raise WorkerFailure(reason, result)
    if result.returncode:
        raise WorkerFailure("WORKER_EXIT_FAILED", result)
    if sum(p.stat().st_size for p in directory.rglob("*") if p.is_file()) > budget.temporary_bytes:
        raise WorkerFailure("WORKER_DISK_LIMIT", result)
    return result
