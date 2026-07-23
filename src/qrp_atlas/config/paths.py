"""Compatibility path constants backed by the unified settings object."""

from __future__ import annotations

from pathlib import Path

from qrp_atlas.config.settings import (
    AppSettings,
    PROJECT_ROOT,
    get_settings,
    require_writable,
)


_EFFECTIVE = get_settings()

DATA_DIR = _EFFECTIVE.paths.data_dir
RAW_DIR = _EFFECTIVE.paths.raw_dir
CANONICAL_DIR = _EFFECTIVE.paths.canonical_dir
DB_DIR = _EFFECTIVE.paths.db_dir
DB_PATH = _EFFECTIVE.paths.duckdb_path
STATE_DIR = _EFFECTIVE.paths.state_dir
DAILY_SNAPSHOT_RAW_DIR = RAW_DIR / "daily_snapshot"
DAILY_MARKET_SNAPSHOT_CANONICAL_DIR = CANONICAL_DIR / "daily_market_snapshot"
RESEARCH_PDFS_DIR = _EFFECTIVE.paths.research_pdfs_dir
WEB_DIR = _EFFECTIVE.paths.web_dir
BACKTEST_RUNS_DIR = _EFFECTIVE.paths.backtest_runs_dir
BACKTEST_TASKS_DIR = _EFFECTIVE.paths.backtest_tasks_dir
ROBUSTNESS_RUNS_DIR = _EFFECTIVE.paths.robustness_runs_dir
DECLARATIVE_STRATEGIES_DIR = _EFFECTIVE.paths.declarative_strategies_dir
LOG_DIR = _EFFECTIVE.paths.log_dir
TMP_DIR = _EFFECTIVE.paths.tmp_dir
BACKTEST_FIXTURE_RUNS_DIR = _EFFECTIVE.paths.backtest_fixture_runs_dir


def current_paths(*, settings: AppSettings | None = None):
    """Return paths from supplied settings or a freshly parsed configuration."""

    return (settings or AppSettings.load()).paths


def ensure_dirs(*, settings: AppSettings | None = None) -> None:
    """Create legacy core directories, respecting configured read-only mode."""

    effective = require_writable(
        settings or get_settings(),
        operation="creating or preparing persistent directories",
    )
    directories: tuple[Path, ...] = (
        effective.paths.data_dir,
        effective.paths.raw_dir,
        effective.paths.canonical_dir,
        effective.paths.db_dir,
        effective.paths.backtest_runs_dir,
    )
    for directory in directories:
        if directory.exists():
            if not directory.is_dir():
                raise RuntimeError(f"configured directory path is not a directory: {directory}")
            continue
        directory.mkdir(parents=True, exist_ok=True)
