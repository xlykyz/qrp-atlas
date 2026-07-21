"""Write-boundary coverage for QRP_READ_ONLY."""

from __future__ import annotations

from pathlib import Path

import pytest

from qrp_atlas.backtest.product.task_store import BacktestTaskStore
from qrp_atlas.backtest.results.robustness_writer import ResidualRobustnessWriter
from qrp_atlas.backtest.results.writer import BacktestRunWriter
from qrp_atlas.config.paths import ensure_dirs
from qrp_atlas.config.settings import AppSettings
from qrp_atlas.strategies.declarative.store import DeclarativeStrategyStore


def read_only_settings(tmp_path: Path) -> AppSettings:
    return AppSettings.load(
        environ={"QRP_READ_ONLY": "true"},
        project_root=tmp_path,
    )


def test_ensure_dirs_always_rejects_read_only_mode(tmp_path: Path) -> None:
    writable = AppSettings.load(environ={}, project_root=tmp_path)
    ensure_dirs(settings=writable)
    settings = read_only_settings(tmp_path)

    with pytest.raises(RuntimeError, match="QRP_READ_ONLY=true"):
        ensure_dirs(settings=settings)


def test_default_backtest_task_store_rejects_writes_without_creating_root(
    tmp_path: Path,
) -> None:
    settings = read_only_settings(tmp_path)
    store = BacktestTaskStore(settings=settings)

    with pytest.raises(RuntimeError, match="QRP_READ_ONLY=true"):
        store._write_atomic(store.root / "task_readonly.json", {})

    assert not store.root.exists()


def test_default_result_writers_reject_before_mutating_filesystem(tmp_path: Path) -> None:
    settings = read_only_settings(tmp_path)

    with pytest.raises(RuntimeError, match="QRP_READ_ONLY=true"):
        BacktestRunWriter(settings=settings).write_portfolio_run(
            None,  # type: ignore[arg-type]
            run_id="run_readonly",
            strategy_name="readonly",
            universe="readonly",
        )

    with pytest.raises(RuntimeError, match="QRP_READ_ONLY=true"):
        ResidualRobustnessWriter(settings=settings).write(
            None,
            run_id="robustness_readonly",
        )

    assert not settings.paths.backtest_runs_dir.exists()
    assert not settings.paths.robustness_runs_dir.exists()


def test_default_declarative_store_rejects_before_validation(tmp_path: Path) -> None:
    settings = read_only_settings(tmp_path)
    store = DeclarativeStrategyStore(settings=settings)

    with pytest.raises(RuntimeError, match="QRP_READ_ONLY=true"):
        store.create({}, owner_user_id="local-user")

    assert not store.root.exists()


def test_explicit_test_root_remains_writable_in_read_only_process(tmp_path: Path) -> None:
    settings = read_only_settings(tmp_path)
    explicit_root = tmp_path / "isolated-test-output"
    store = BacktestTaskStore(explicit_root, settings=settings)

    output = explicit_root / "task_test.json"
    store._write_atomic(output, {"ok": True})

    assert output.is_file()
