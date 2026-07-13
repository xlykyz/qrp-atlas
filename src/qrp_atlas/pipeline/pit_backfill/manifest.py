"""JSON-line manifest for PIT backfill progress / resume."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_EMPTY = "empty"
STATUS_FAILED = "failed"

TERMINAL_SKIP = {STATUS_SUCCESS, STATUS_EMPTY}
ALL_STATUSES = {
    STATUS_PENDING,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    STATUS_EMPTY,
    STATUS_FAILED,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class BatchRecord:
    batch_id: str
    dataset: str
    key: str
    status: str = STATUS_PENDING
    period: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    attempts: int = 0
    fetched_rows: int = 0
    cleaned_rows: int = 0
    inserted_rows: int = 0
    raw_path: str | None = None
    error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BatchRecord":
        known = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        payload = {k: v for k, v in data.items() if k in known}
        payload.setdefault("meta", {})
        return cls(**payload)

    @classmethod
    def from_batch(cls, batch) -> "BatchRecord":
        return cls(
            batch_id=batch.batch_id,
            dataset=batch.dataset,
            key=batch.key,
            period=batch.period,
            start_date=batch.start_date,
            end_date=batch.end_date,
            meta=dict(batch.meta or {}),
            status=STATUS_PENDING,
        )


class ManifestStore:
    """Atomic JSONL manifest keyed by batch_id."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._records: dict[str, BatchRecord] = {}
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                rec = BatchRecord.from_dict(data)
                self._records[rec.batch_id] = rec

    def _flush(self) -> None:
        # Atomic rewrite keeps resume consistent after crashes.
        fd, tmp_name = tempfile.mkstemp(
            prefix=self.path.name + ".",
            suffix=".tmp",
            dir=str(self.path.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                for rec in self._records.values():
                    fh.write(json.dumps(rec.to_dict(), ensure_ascii=False, sort_keys=True))
                    fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, self.path)
        except Exception:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def ensure_batches(self, batches: Iterable) -> None:
        changed = False
        for batch in batches:
            if batch.batch_id not in self._records:
                self._records[batch.batch_id] = BatchRecord.from_batch(batch)
                changed = True
        if changed:
            self._flush()

    def get(self, batch_id: str) -> BatchRecord | None:
        return self._records.get(batch_id)

    def upsert(self, record: BatchRecord) -> None:
        self._records[record.batch_id] = record
        self._flush()

    def update(self, batch_id: str, **fields: Any) -> BatchRecord:
        rec = self._records[batch_id]
        for k, v in fields.items():
            if not hasattr(rec, k):
                raise AttributeError(k)
            setattr(rec, k, v)
        self._flush()
        return rec

    def reset_running_to_pending(self) -> int:
        n = 0
        for rec in self._records.values():
            if rec.status == STATUS_RUNNING:
                rec.status = STATUS_PENDING
                rec.error = (rec.error or "") + " | reset_running_on_resume"
                n += 1
        if n:
            self._flush()
        return n

    def iter_records(self) -> Iterator[BatchRecord]:
        yield from self._records.values()

    def counts(self) -> dict[str, int]:
        out = {s: 0 for s in ALL_STATUSES}
        for rec in self._records.values():
            out[rec.status] = out.get(rec.status, 0) + 1
        out["total"] = len(self._records)
        return out

    def should_process(self, batch_id: str, *, resume: bool) -> bool:
        rec = self._records.get(batch_id)
        if rec is None:
            return True
        if not resume:
            return True
        if rec.status in TERMINAL_SKIP:
            return False
        return True
