"""QRP Atlas runtime configuration public API."""

from .auth import AuthMode, AuthSettings, DEFAULT_LOCAL_USER_ID
from .paths import (
    BACKTEST_FIXTURE_RUNS_DIR,
    BACKTEST_RUNS_DIR,
    BACKTEST_TASKS_DIR,
    CANONICAL_DIR,
    DATA_DIR,
    DB_DIR,
    DB_PATH,
    DAILY_MARKET_SNAPSHOT_CANONICAL_DIR,
    DAILY_SNAPSHOT_RAW_DIR,
    DECLARATIVE_STRATEGIES_DIR,
    LOG_DIR,
    PIPELINE_RUNTIME_DIR,
    PROJECT_ROOT,
    RAW_DIR,
    RESEARCH_PDFS_DIR,
    ROBUSTNESS_RUNS_DIR,
    STATE_DIR,
    TMP_DIR,
    WEB_DIR,
    current_paths,
    ensure_dirs,
)
from .settings import (
    AppSettings,
    ConfigError,
    get_settings,
    require_writable,
    reset_settings_cache,
)

DB_READ_ONLY = get_settings().database.read_only


def __getattr__(name: str):
    if name in {"TUSHARE_TOKEN", "get_tushare_pro", "_try_both_tokens"}:
        from . import tushare_client

        return getattr(tushare_client, name)
    raise AttributeError(name)


__all__ = [
    "AppSettings",
    "ConfigError",
    "get_settings",
    "reset_settings_cache",
    "require_writable",
    "PROJECT_ROOT",
    "DATA_DIR",
    "RAW_DIR",
    "CANONICAL_DIR",
    "DB_DIR",
    "DB_PATH",
    "STATE_DIR",
    "PIPELINE_RUNTIME_DIR",
    "DAILY_SNAPSHOT_RAW_DIR",
    "DAILY_MARKET_SNAPSHOT_CANONICAL_DIR",
    "RESEARCH_PDFS_DIR",
    "WEB_DIR",
    "BACKTEST_RUNS_DIR",
    "BACKTEST_TASKS_DIR",
    "ROBUSTNESS_RUNS_DIR",
    "DECLARATIVE_STRATEGIES_DIR",
    "LOG_DIR",
    "TMP_DIR",
    "BACKTEST_FIXTURE_RUNS_DIR",
    "current_paths",
    "ensure_dirs",
    "DB_READ_ONLY",
    "AuthMode",
    "AuthSettings",
    "DEFAULT_LOCAL_USER_ID",
    "TUSHARE_TOKEN",
    "get_tushare_pro",
    "_try_both_tokens",
]
