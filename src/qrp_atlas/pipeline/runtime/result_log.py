"""Small, stable audit log for final Pipeline execution outcomes."""

from __future__ import annotations

import json
import os
import re
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping

from .models import PipelineRun


class ResultLogConfigurationError(ValueError):
    """Raised before execution when the configured audit destination is unsafe."""


_SECRET_PATTERN = re.compile(r"(?i)(password|passwd|secret|token|api[_ -]?key)\s*([=:])\s*[^\s,;]+")


def _repository_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise RuntimeError("cannot locate repository root for pipeline audit-log validation")


def _redact(value: str | None) -> str | None:
    if value is None:
        return None
    shortened = value.replace("\n", " ").replace("\r", " ")[:500]
    return _SECRET_PATTERN.sub(lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]", shortened)


class PipelineResultLog:
    """Append one secret-safe JSON record per completed Pipeline run."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory).resolve(strict=False)
        self._write_lock = threading.Lock()

    def validate(self) -> None:
        repository_root = _repository_root()
        try:
            self.directory.relative_to(repository_root)
        except ValueError:
            pass
        else:
            raise ResultLogConfigurationError(
                f"pipeline result logs must be outside the source repository: {self.directory}"
            )
        if self.directory.exists() and not self.directory.is_dir():
            raise ResultLogConfigurationError(f"pipeline result log path is not a directory: {self.directory}")
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ResultLogConfigurationError(
                f"cannot create pipeline result log directory {self.directory}: {exc}"
            ) from exc
        if not os.access(self.directory, os.W_OK | os.X_OK):
            raise ResultLogConfigurationError(f"pipeline result log directory is not writable: {self.directory}")

    def write(self, run: PipelineRun, result: Mapping[str, object] | None) -> Path:
        self.validate()
        recorded_at = datetime.now(UTC)
        target_window = result.get("target_window") if isinstance(result, Mapping) else None
        business_date = target_window.get("target_date") if isinstance(target_window, Mapping) else None
        metrics = result.get("metrics") if isinstance(result, Mapping) else None
        outputs = result.get("outputs") if isinstance(result, Mapping) else None
        output_summary: list[dict[str, object]] = []
        if isinstance(outputs, list):
            for item in outputs:
                if not isinstance(item, Mapping):
                    continue
                output_summary.append(
                    {
                        "output_id": item.get("output_id"),
                        "rows_written": item.get("rows_written"),
                        "completed": item.get("completed"),
                    }
                )
        row = {
            "recorded_at": recorded_at.isoformat(),
            "pipeline_id": run.pipeline_id,
            "definition_version": run.definition_version,
            "run_id": run.run_id,
            "business_date": business_date,
            "scheduled_at": run.scheduled_at.isoformat(),
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "status": run.status.value,
            "duration_ms": run.wall_duration_ms,
            "tasks": [
                {
                    "task_id": run.pipeline_id,
                    "status": run.status.value,
                    "rows_written": metrics.get("rows_written") if isinstance(metrics, Mapping) else None,
                }
            ],
            "outputs": output_summary,
            "error_type": "PIPELINE_EXECUTION_ERROR" if run.error_summary else None,
            "error_summary": _redact(run.error_summary),
        }
        destination = self.directory / f"pipeline-results-{recorded_at.date().isoformat()}.jsonl"
        try:
            with self._write_lock, destination.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            raise ResultLogConfigurationError(f"cannot write pipeline result log {destination}: {exc}") from exc
        return destination
