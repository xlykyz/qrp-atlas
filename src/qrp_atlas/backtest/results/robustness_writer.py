"""Atomic writer for residual robustness study artifacts."""

from __future__ import annotations

import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from qrp_atlas.backtest.results.writer import BacktestRunWriter
from qrp_atlas.config.settings import AppSettings, require_writable
from qrp_atlas.pipeline.pit_backfill.safety import FileLock

_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
ROBUSTNESS_SCHEMA_VERSION = "1.0.0"

_TOP_LEVEL_FILES = (
    "manifest.json",
    "splits.json",
    "candidates.json",
    "train_metrics.json",
    "validation_metrics.json",
    "selected_parameters.json",
    "fold_test_metrics.json",
    "oos_equity.json",
    "oos_summary.json",
    "cost_stress.json",
    "parameter_sensitivity.json",
    "rolling_performance.json",
    "diagnostics.json",
)


def _validate_run_id(run_id: str) -> str:
    if not run_id or not _RUN_ID_PATTERN.match(run_id):
        raise ValueError(f"invalid run_id: {run_id!r}")
    if ".." in run_id or "/" in run_id or "\\" in run_id:
        raise ValueError(f"invalid run_id path components: {run_id!r}")
    return run_id


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _default_root() -> Path:
    return AppSettings.load().paths.robustness_runs_dir


class ResidualRobustnessWriter:
    """Persist a residual robustness study under an atomic run directory."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        settings: AppSettings | None = None,
    ) -> None:
        effective = settings or AppSettings.load()
        configured_root = effective.paths.robustness_runs_dir
        self.root = Path(root) if root is not None else configured_root
        self._write_settings = (
            effective
            if self.root.resolve(strict=False) == configured_root.resolve(strict=False)
            else None
        )

    def write(
        self,
        result: Any,
        *,
        run_id: str,
        overwrite: bool = False,
        created_at: str | None = None,
    ) -> Path:
        if self._write_settings is not None:
            require_writable(
                self._write_settings,
                operation="writing configured robustness result storage",
            )
        # Lazy import avoids product/results <-> research circular import at module load.
        from qrp_atlas.backtest.research.robustness import ResidualRobustnessResult

        if not isinstance(result, ResidualRobustnessResult):
            raise TypeError("result must be a ResidualRobustnessResult")
        _validate_run_id(run_id)
        self.root.mkdir(parents=True, exist_ok=True)
        run_dir = self.root / run_id
        lock_path = self.root / f".{run_id}.lock"
        temp_dir = self.root / f".{run_id}.tmp"

        with FileLock(lock_path, timeout_s=30):
            if run_dir.exists() and not overwrite:
                raise FileExistsError(f"robustness run already exists: {run_id}")

            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            temp_dir.mkdir()

            try:
                meta = dict(result.metadata)
                created = created_at or datetime.now(timezone.utc).isoformat(
                    timespec="seconds"
                )
                manifest = {
                    "run_id": run_id,
                    "created_at": created,
                    "strategy_code": meta.get("strategy_code"),
                    "strategy_version": meta.get("strategy_version"),
                    "benchmark_id": meta.get("benchmark_id"),
                    "indicator_version": (
                        (meta.get("portfolio_config") or {})
                        # residual indicator version is recorded per preparation; keep explicit slot
                    ),
                    "portfolio_config": meta.get("portfolio_config"),
                    "base_parameters": meta.get("base_parameters"),
                    "parameter_grid": meta.get("parameter_grid"),
                    "split_config": meta.get("walk_forward_config"),
                    "selection_objective": meta.get("selection_objective"),
                    "cost_scenarios": meta.get("cost_scenarios"),
                    "rolling_windows": meta.get("rolling_windows"),
                    "input_date_range": meta.get("input_date_range"),
                    "oos_date_range": meta.get("oos_date_range"),
                    "fold_count": meta.get("fold_count"),
                    "successful_test_fold_count": meta.get("successful_test_fold_count"),
                    "failed_or_skipped_test_fold_count": meta.get(
                        "failed_or_skipped_test_fold_count"
                    ),
                    "result_schema_version": meta.get(
                        "result_schema_version", ROBUSTNESS_SCHEMA_VERSION
                    ),
                    "writer_schema_version": ROBUSTNESS_SCHEMA_VERSION,
                    "artifact_files": list(_TOP_LEVEL_FILES) + ["folds/"],
                }
                # Prefer residual calculation version if present in first successful run.
                indicator_version = None
                for run in result.selected_test_runs.values():
                    prep_meta = dict(run.preparation.metadata)
                    residual_calc = prep_meta.get("residual_calculation") or {}
                    indicator_version = residual_calc.get("calculation_version")
                    if indicator_version:
                        break
                manifest["indicator_version"] = indicator_version

                payloads = {
                    "manifest.json": manifest,
                    "splits.json": [split.to_dict() for split in result.splits],
                    "candidates.json": [
                        candidate.to_dict() for candidate in result.candidates
                    ],
                    "train_metrics.json": list(result.train_metrics),
                    "validation_metrics.json": list(result.validation_metrics),
                    "selected_parameters.json": list(result.selected_parameters),
                    "fold_test_metrics.json": list(result.fold_test_metrics),
                    "oos_equity.json": result.oos_equity.to_dict(orient="list"),
                    "oos_summary.json": dict(result.oos_summary),
                    "cost_stress.json": list(result.cost_stress),
                    "parameter_sensitivity.json": list(result.parameter_sensitivity),
                    "rolling_performance.json": list(result.rolling_performance),
                    "diagnostics.json": list(result.diagnostics),
                }
                for filename, payload in payloads.items():
                    _write_json(temp_dir / filename, payload)

                folds_root = temp_dir / "folds"
                folds_root.mkdir(parents=True, exist_ok=True)
                for fold_id, run in sorted(result.selected_test_runs.items()):
                    # Nested standard portfolio package under folds/<fold_id>/test
                    nested_root = folds_root / fold_id
                    nested_root.mkdir(parents=True, exist_ok=True)
                    nested_writer = BacktestRunWriter(root=nested_root)
                    nested_writer.write_portfolio_run(
                        run.portfolio_result,
                        run_id="test",
                        strategy_name=str(meta.get("strategy_code") or "market_residual_mean_reversion"),
                        universe=str(meta.get("benchmark_id") or "residual"),
                        name=f"{run_id}:{fold_id}:test",
                        created_at=created,
                        overwrite=True,
                        config_overlay={
                            "fold_id": fold_id,
                            "parameters": dict(run.metadata.get("parameters") or {}),
                            "segment": "test",
                        },
                        extra_skipped=[
                            {
                                "asset_id": item.get("asset_id"),
                                "signal_date": item.get("signal_date"),
                                "reason": item.get("reason"),
                                "detail": item.get("detail"),
                            }
                            for item in run.skipped_signals
                        ],
                    )

                # Atomic promotion with rollback-safe overwrite:
                # write fully into temp first; only then swap directories.
                backup_dir = self.root / f".{run_id}.bak"
                if backup_dir.exists():
                    shutil.rmtree(backup_dir)

                replaced_existing = False
                try:
                    if run_dir.exists():
                        # Move existing run aside so a failed promotion can restore it.
                        run_dir.replace(backup_dir)
                        replaced_existing = True
                    temp_dir.replace(run_dir)
                except Exception:
                    # Best-effort restore of the previous formal directory.
                    if replaced_existing and backup_dir.exists() and not run_dir.exists():
                        try:
                            backup_dir.replace(run_dir)
                        except Exception:
                            pass
                    raise
                else:
                    if backup_dir.exists():
                        shutil.rmtree(backup_dir)
            except Exception:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
                raise

        return run_dir


__all__ = ["ROBUSTNESS_SCHEMA_VERSION", "ResidualRobustnessWriter"]
