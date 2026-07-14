"""Filesystem-backed persistence for product backtest tasks."""

from __future__ import annotations

import json
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from qrp_atlas.config.paths import PROJECT_ROOT

from .schemas import BacktestTaskRecord, CreateBacktestTaskRequest

_TASK_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_STATUS_PENDING = "pending"
_STATUS_RUNNING = "running"
_STATUS_SUCCEEDED = "succeeded"
_STATUS_FAILED = "failed"
VALID_STATUSES = frozenset(
    {_STATUS_PENDING, _STATUS_RUNNING, _STATUS_SUCCEEDED, _STATUS_FAILED}
)


def default_tasks_dir() -> Path:
    env = os.getenv("QRP_ATLAS_BACKTEST_TASKS_DIR")
    if env:
        return Path(env)
    return PROJECT_ROOT / "data" / "backtest_tasks"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _validate_task_id(task_id: str) -> str:
    if not task_id or not _TASK_ID_PATTERN.match(task_id):
        raise ValueError(f"invalid task_id: {task_id!r}")
    return task_id


class BacktestTaskStore:
    """Persist task JSON files under a local directory.

    Status values use the product contract:
    pending / running / succeeded / failed.
    """

    def __init__(self, root: Path | None = None) -> None:
        self.root = Path(root) if root is not None else default_tasks_dir()
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def _path(self, task_id: str) -> Path:
        return self.root / f"{_validate_task_id(task_id)}.json"

    def _write_atomic(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        tmp.replace(path)

    def create(self, request: CreateBacktestTaskRequest) -> BacktestTaskRecord:
        with self._lock:
            task_id = f"task_{uuid.uuid4().hex[:12]}"
            created_at = _utc_now()
            name = (request.name or "").strip() or (
                f"{request.strategy_code}@{request.strategy_version} "
                f"{request.start_date}~{request.end_date}"
            )
            tickers = list(request.tickers or [])
            snapshot = request.model_dump(mode="json")
            record = BacktestTaskRecord(
                task_id=task_id,
                run_id=None,
                name=name,
                strategy_code=request.strategy_code,
                strategy_version=request.strategy_version,
                strategy_params=dict(request.strategy_params or {}),
                universe_mode=request.universe_mode,
                universe_preset=request.universe_preset,
                index_code=request.index_code,
                tickers=tickers,
                start_date=request.start_date,
                end_date=request.end_date,
                position=request.position,
                cost=request.cost,
                execution=request.execution,
                status=_STATUS_PENDING,
                error_message=None,
                created_at=created_at,
                updated_at=created_at,
                is_mock=False,
                request_snapshot=snapshot,
            )
            self._write_atomic(self._path(task_id), record.model_dump(mode="json"))
            return record

    def get(self, task_id: str) -> BacktestTaskRecord:
        with self._lock:
            path = self._path(task_id)
            if not path.exists():
                raise KeyError(f"task not found: {task_id}")
            data = json.loads(path.read_text(encoding="utf-8"))
            return BacktestTaskRecord.model_validate(data)

    def list(self) -> list[BacktestTaskRecord]:
        with self._lock:
            records: list[BacktestTaskRecord] = []
            for path in sorted(self.root.glob("task_*.json"), reverse=True):
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    records.append(BacktestTaskRecord.model_validate(data))
                except Exception:
                    continue
            records.sort(key=lambda item: item.created_at, reverse=True)
            return records

    def update(
        self,
        task_id: str,
        *,
        status: str | None = None,
        run_id: str | None = None,
        error_message: str | None = None,
        clear_error: bool = False,
    ) -> BacktestTaskRecord:
        with self._lock:
            record = self.get(task_id)
            payload = record.model_dump(mode="json")
            if status is not None:
                if status not in VALID_STATUSES:
                    raise ValueError(f"invalid status: {status}")
                payload["status"] = status
            if run_id is not None:
                payload["run_id"] = run_id
            if clear_error:
                payload["error_message"] = None
            elif error_message is not None:
                payload["error_message"] = error_message
            payload["updated_at"] = _utc_now()
            updated = BacktestTaskRecord.model_validate(payload)
            self._write_atomic(self._path(task_id), updated.model_dump(mode="json"))
            return updated
