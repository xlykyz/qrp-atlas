"""Long-running, single-leader Pipeline service built on the runtime store."""

from __future__ import annotations

import sqlite3
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime

from .models import PipelineDefinition, PipelineRun, PipelineStatus
from .planning import dependency_plan
from .result_log import PipelineResultLog, ResultLogConfigurationError
from .runner import PipelineRunner, PipelineRuntimePaths
from .scheduler import PipelineScheduler
from .store import PipelineRuntimeStore, RunClaimFailure


@dataclass(frozen=True, slots=True)
class ServiceCycleResult:
    """One quiet scheduler pass; idle cycles intentionally produce no log record."""

    created_runs: tuple[PipelineRun, ...]
    executed_runs: tuple[PipelineRun, ...]
    stale_runs_recovered: int
    expired_locks_reclaimed: int


class PipelineServiceFatalError(RuntimeError):
    """The service can no longer make safe scheduling decisions."""


class PipelineService:
    """Run scheduled work continuously with a durable service lease and heartbeat."""

    def __init__(
        self,
        store: PipelineRuntimeStore,
        paths: PipelineRuntimePaths,
        definitions: tuple[PipelineDefinition, ...],
        *,
        scheduler_id: str,
        heartbeat_interval_seconds: float,
        lease_seconds: int,
        stale_after_seconds: int,
        max_catch_up_minutes: int,
        poll_interval_seconds: float = 5.0,
        max_workers: int = 4,
        service_name: str = "pipeline-scheduler",
        owner_id: str | None = None,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        self.store = store
        self.paths = paths
        self.definitions = definitions
        self.scheduler = PipelineScheduler(
            store,
            definitions,
            scheduler_id=scheduler_id,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            lease_seconds=lease_seconds,
            stale_after_seconds=stale_after_seconds,
            max_catch_up_minutes=max_catch_up_minutes,
            bootstrap_catch_up=True,
        )
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.lease_seconds = lease_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.max_workers = max_workers
        self.service_name = service_name
        self.owner_id = owner_id or str(uuid.uuid4())
        self.result_log = PipelineResultLog(paths.result_logs_dir)
        self._stop_event = threading.Event()
        self._lost_lease = threading.Event()
        self._lease_heartbeater: threading.Thread | None = None
        self._last_error: str | None = None
        self._fatal_reason: str | None = None
        self._started = False

    def start(self) -> None:
        """Validate destination and claim service leadership before any execution."""

        if self._started:
            return
        self.result_log.validate()
        self.store.initialize()
        self.store.claim_service_lease(
            service_name=self.service_name,
            owner_id=self.owner_id,
            lease_seconds=self.lease_seconds,
        )
        self._started = True
        self._lease_heartbeater = threading.Thread(
            target=self._heartbeat_loop,
            name=f"pipeline-service-heartbeat-{self.owner_id}",
            daemon=True,
        )
        self._lease_heartbeater.start()

    def stop(self) -> None:
        """Request a graceful stop and release leadership after an active child returns."""

        self._stop_event.set()
        if self._lease_heartbeater is not None:
            self._lease_heartbeater.join(timeout=max(1.0, self.heartbeat_interval_seconds * 2))
            self._lease_heartbeater = None
        if self._started:
            try:
                self.store.release_service_lease(service_name=self.service_name, owner_id=self.owner_id)
            except Exception:
                # A broken store cannot be safely cleaned up by this process;
                # the bounded lease leaves an explicit recovery path.
                pass
        self._started = False

    def request_stop(self) -> None:
        self._stop_event.set()

    def run_once(self, *, now: datetime | None = None) -> ServiceCycleResult:
        """Scan due work, then execute all currently runnable tasks in dependency order."""

        if not self._started:
            raise RuntimeError("pipeline service must be started before run_once")
        if self._lost_lease.is_set():
            raise PipelineServiceFatalError(self._fatal_reason or "pipeline scheduler service lease was lost")
        instant = (now or datetime.now(UTC)).astimezone(UTC)
        try:
            scan = self.scheduler.scan(now=instant)
            executed = self._execute_available(instant)
            self._last_error = None
            return ServiceCycleResult(
                created_runs=scan.created_runs,
                executed_runs=tuple(executed),
                stale_runs_recovered=scan.stale_runs_recovered,
                expired_locks_reclaimed=scan.expired_locks_reclaimed,
            )
        except PipelineServiceFatalError:
            raise
        except Exception as exc:
            self._last_error = f"{type(exc).__name__}: {exc}"[:500]
            self._record_error_or_raise_fatal(exc)
            raise

    def run_forever(self, *, on_cycle_error: Callable[[Exception], None] | None = None) -> None:
        """Keep scanning after a scheduler error; individual task failures stay isolated."""

        self.start()
        try:
            while not self._stop_event.is_set():
                try:
                    self.run_once()
                except PipelineServiceFatalError as exc:
                    raise
                except Exception as exc:
                    if on_cycle_error is not None:
                        on_cycle_error(exc)
                self._stop_event.wait(self.poll_interval_seconds)
            if self._lost_lease.is_set():
                raise PipelineServiceFatalError(self._fatal_reason or "pipeline scheduler service lease was lost")
        finally:
            self.stop()

    def _heartbeat_loop(self) -> None:
        interval = min(self.heartbeat_interval_seconds, max(0.5, self.lease_seconds / 3))
        while not self._stop_event.wait(interval):
            try:
                renewed = self.store.heartbeat_service_lease(
                    service_name=self.service_name,
                    owner_id=self.owner_id,
                    lease_seconds=self.lease_seconds,
                    last_error=self._last_error,
                )
            except Exception as exc:
                self._fatal_reason = f"pipeline runtime store is unavailable: {type(exc).__name__}: {exc}"
                renewed = False
            if not renewed:
                self._fatal_reason = self._fatal_reason or "pipeline scheduler service lease was lost"
                self._lost_lease.set()
                self._stop_event.set()
                return

    def _record_error_or_raise_fatal(self, original: Exception) -> None:
        """Persist a recoverable-cycle error, or escalate an unavailable store."""

        try:
            renewed = self.store.heartbeat_service_lease(
                service_name=self.service_name,
                owner_id=self.owner_id,
                lease_seconds=self.lease_seconds,
                last_error=self._last_error,
            )
        except Exception as heartbeat_error:
            raise PipelineServiceFatalError(
                f"pipeline runtime store is unavailable: {type(heartbeat_error).__name__}: {heartbeat_error}"
            ) from original
        if not renewed:
            raise PipelineServiceFatalError("pipeline scheduler service lease was lost") from original
        if isinstance(original, (sqlite3.Error, OSError, ResultLogConfigurationError)):
            raise PipelineServiceFatalError(
                f"pipeline service cannot continue safely: {type(original).__name__}: {original}"
            ) from original

    def _execution_order(self) -> dict[str, int]:
        ordered: list[str] = []
        for definition in self.definitions:
            for item in dependency_plan(self.definitions, definition.pipeline_id):
                if item.pipeline_id not in ordered:
                    ordered.append(item.pipeline_id)
        return {pipeline_id: index for index, pipeline_id in enumerate(ordered)}

    def _execute_available(self, now: datetime) -> list[PipelineRun]:
        by_id = {definition.pipeline_id: definition for definition in self.definitions}
        order = self._execution_order()
        completed: list[PipelineRun] = []
        # A successful upstream task can release a BLOCKED downstream task in
        # this same service pass.  A fixed point also lets independent work run
        # when another chain has failed.
        while not self._stop_event.is_set():
            self.scheduler.refresh_blocked_runs()
            pending = [
                run
                for run in self.store.list_runs(status=PipelineStatus.PENDING, limit=10_000)
                if run.scheduled_at <= now
                and run.pipeline_id in by_id
                and by_id[run.pipeline_id].definition_version == run.definition_version
            ]
            if not pending:
                break
            pending.sort(key=lambda run: (run.scheduled_at, order.get(run.pipeline_id, len(order)), run.attempt))
            progressed = False

            def execute(pending_run: PipelineRun) -> PipelineRun | None:
                definition = by_id[pending_run.pipeline_id]
                try:
                    finished = PipelineRunner(
                        self.store,
                        self.paths,
                        heartbeat_interval_seconds=self.heartbeat_interval_seconds,
                        lease_seconds=self.lease_seconds,
                    ).run(pending_run.run_id, definition)
                except RunClaimFailure:
                    # Another permitted runtime may have claimed it between the
                    # list and claim.  Keep the service healthy and rescan later.
                    return None
                self.result_log.write(finished, self.store.get_result(finished.run_id))
                return finished

            workers = min(self.max_workers, len(pending))
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="pipeline-worker") as executor:
                futures = [executor.submit(execute, pending_run) for pending_run in pending]
                for future in as_completed(futures):
                    finished = future.result()
                    if finished is not None:
                        completed.append(finished)
                        progressed = True
            if not progressed:
                break
        return completed
