"""Generic cooperative deadline and cancellation controls for Job execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import threading
import time


class ExecutionControlError(RuntimeError):
    """Raised when a Job must stop before doing more work."""

    def __init__(self, code: str, detail: str | None = None) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail or code)


@dataclass(slots=True)
class ExecutionControl:
    """Cooperative cancellation and deadline state shared by one invocation."""

    deadline: datetime | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)
    deadline_monotonic: float | None = field(default=None, repr=False)
    _cancel_reason: str | None = field(default=None, init=False, repr=False)
    _reason_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def cancel(self, reason: str) -> None:
        with self._reason_lock:
            if self._cancel_reason is None:
                self._cancel_reason = reason[:500]
            self.cancel_event.set()

    @property
    def cancel_reason(self) -> str | None:
        with self._reason_lock:
            return self._cancel_reason

    def remaining_seconds(self) -> float | None:
        if self.deadline_monotonic is not None:
            return max(0.0, self.deadline_monotonic - time.monotonic())
        if self.deadline is None:
            return None
        if self.deadline.tzinfo is None:
            raise ValueError("execution deadline must be timezone-aware")
        return max(0.0, (self.deadline - datetime.now(self.deadline.tzinfo)).total_seconds())

    def bounded_timeout(self, requested: float | None = None) -> float | None:
        """Return a wait/network timeout that cannot exceed this deadline."""

        remaining = self.remaining_seconds()
        if requested is not None and requested < 0:
            raise ValueError("requested timeout must be non-negative")
        if remaining is None:
            return requested
        return remaining if requested is None else min(remaining, requested)

    def wait(self, event: threading.Event, timeout: float | None = None) -> bool:
        self.check()
        woke = event.wait(self.bounded_timeout(timeout))
        self.check()
        return woke

    def check(self) -> None:
        remaining = self.remaining_seconds()
        if remaining is not None and remaining <= 0:
            self.cancel("execution deadline exceeded")
            raise ExecutionControlError("EXECUTION_TIMED_OUT", "execution deadline exceeded")
        if self.cancel_event.is_set():
            raise ExecutionControlError("EXECUTION_CANCELLED", self.cancel_reason or "execution cancelled")
