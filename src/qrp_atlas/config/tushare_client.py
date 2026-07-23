"""Tushare Pro client construction backed by unified runtime settings."""

from __future__ import annotations

import functools
import time
from typing import Any, Callable, TypeVar

import tushare as ts

from qrp_atlas.config.settings import AppSettings, apply_proxy_environment, get_settings


F = TypeVar("F", bound=Callable[..., Any])
_EFFECTIVE = get_settings().external_services
_CUSTOM_API_URL = _EFFECTIVE.tushare_api_url
_RATE_LIMIT_INTERVAL = _EFFECTIVE.tushare_request_interval_seconds
TUSHARE_TOKEN = _EFFECTIVE.tushare_token
_last_call_time = 0.0


def _configured_call(
    func: F,
    *,
    interval_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
) -> F:
    """Apply process-local pacing and bounded retries to one client method."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any):
        global _last_call_time
        elapsed = time.monotonic() - _last_call_time
        if elapsed < interval_seconds:
            time.sleep(interval_seconds - elapsed)
        attempt = 0
        while True:
            _last_call_time = time.monotonic()
            try:
                return func(*args, **kwargs)
            except Exception:
                if attempt >= max_retries:
                    raise
                attempt += 1
                time.sleep(retry_backoff_seconds * attempt)

    return wrapper  # type: ignore[return-value]


def get_tushare_pro(
    token: str | None = None,
    *,
    settings: AppSettings | None = None,
):
    """Return a configured Tushare Pro client.

    The token is optional at application startup and required only when this
    external service is used. Error messages never include the token value.
    """

    effective = settings or AppSettings.load()
    external = effective.external_services
    selected_token = token or external.tushare_token
    if not selected_token:
        raise ValueError("TUSHARE_TOKEN is required to use Tushare")

    apply_proxy_environment(effective)
    pro = ts.pro_api(selected_token)
    pro._DataApi__http_url = external.tushare_api_url

    for attr_name in dir(pro):
        if attr_name.startswith("_"):
            continue
        attr = getattr(pro, attr_name)
        if callable(attr):
            setattr(
                pro,
                attr_name,
                _configured_call(
                    attr,
                    interval_seconds=external.tushare_request_interval_seconds,
                    max_retries=external.tushare_max_retries,
                    retry_backoff_seconds=external.tushare_retry_backoff_seconds,
                ),
            )
    return pro


def _try_both_tokens():
    """Compatibility helper that validates the single configured token."""

    pro = get_tushare_pro()
    pro.index_basic(limit=1)
    return pro


if __name__ == "__main__":
    client = _try_both_tokens()
    print(client.index_basic(limit=5))
    print(ts.pro_bar(api=client, ts_code="000001.SZ", limit=3))
