"""Single-thread request pacing for Tushare historical backfill."""

from __future__ import annotations

import threading
import time
from typing import Callable, TypeVar

from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError

T = TypeVar("T")

# User limit is 100/min; operate at 80/min => >= 0.75s between requests.
DEFAULT_MIN_INTERVAL = 0.75
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_BASE = 2.0


class RateLimiter:
    """Serialize and pace outbound API calls."""

    def __init__(self, min_interval: float = DEFAULT_MIN_INTERVAL):
        if min_interval <= 0:
            raise ValueError("min_interval must be positive")
        self.min_interval = float(min_interval)
        self._lock = threading.Lock()
        self._last_call = 0.0
        self.call_count = 0

    def wait(self, execution_control: ExecutionControl | None = None) -> None:
        with self._lock:
            if execution_control is not None:
                execution_control.check()
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self.min_interval:
                delay = self.min_interval - elapsed
                if execution_control is None:
                    time.sleep(delay)
                else:
                    execution_control.wait(threading.Event(), timeout=delay)
            self._last_call = time.monotonic()
            self.call_count += 1
            if execution_control is not None:
                execution_control.check()

    def call(
        self,
        func: Callable[..., T],
        *args,
        execution_control: ExecutionControl | None = None,
        **kwargs,
    ) -> T:
        self.wait(execution_control)
        result = func(*args, **kwargs)
        if execution_control is not None:
            execution_control.check()
        return result


def is_rate_limit_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    needles = (
        "频率",
        "限流",
        "rate limit",
        "too many",
        "exceed",
        "每分钟",
        "访问频率",
        "ip次数",
    )
    return any(n in text for n in needles)


def call_with_rate_limit(
    limiter: RateLimiter,
    func: Callable[..., T],
    *args,
    retries: int = DEFAULT_MAX_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    execution_control: ExecutionControl | None = None,
    **kwargs,
) -> T:
    """Invoke func under rate limit; retries and backs off on transient failures."""
    last_err: BaseException | None = None
    attempts = max(1, int(retries))
    for i in range(attempts):
        if execution_control is not None:
            execution_control.check()
        try:
            return limiter.call(func, *args, execution_control=execution_control, **kwargs)
        except ExecutionControlError:
            raise
        except Exception as exc:  # network / gateway / rate limit
            last_err = exc
            if i + 1 >= attempts:
                break
            sleep_s = backoff_base * (i + 1)
            if is_rate_limit_error(exc):
                sleep_s = max(sleep_s, 30.0 * (i + 1))
            if execution_control is None:
                time.sleep(sleep_s)
            else:
                execution_control.wait(threading.Event(), timeout=sleep_s)
    assert last_err is not None
    raise last_err
