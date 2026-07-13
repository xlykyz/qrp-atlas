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

STAGE_FETCH = "fetch"
STAGE_CLEAN = "clean"
STAGE_LOAD = "load"
ALL_STAGES = (STAGE_FETCH, STAGE_CLEAN, STAGE_LOAD)

TERMINAL_OK = {STATUS_SUCCESS, STATUS_EMPTY}
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
    status: str = STATUS_PENDING  # aggregate batch status
    period: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    attempts: int = 0
    fetched_rows: int = 0
    cleaned_rows: int = 0
    inserted_rows: int = 0
    raw_path: str | None = None
    cleaned_path: str | None = None
    error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    # Decoupled stage fields
    fetch_status: str = STATUS_PENDING
    clean_status: str = STATUS_PENDING
    load_status: str = STATUS_PENDING
    fetch_error: str | None = None
    clean_error: str | None = None
    load_error: str | None = None
    fetch_finished_at: str | None = None
    clean_finished_at: str | None = None
    load_finished_at: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BatchRecord":
        known = set(cls.__dataclass_fields__.keys())  # type: ignore[attr-defined]
        payload = {k: v for k, v in data.items() if k in known}
        payload.setdefault("meta", {})
        rec = cls(**payload)
        rec.migrate_legacy()
        return rec

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

    def migrate_legacy(self) -> None:
        """Infer stage statuses from older single-status manifests."""
        # If stage fields already populated beyond defaults, keep them.
        stage_touched = any(
            getattr(self, f"{s}_status") != STATUS_PENDING for s in ALL_STAGES
        )
        if stage_touched:
            self.recompute_status()
            return

        if self.status in TERMINAL_OK:
            # Legacy completed batch: all stages done equivalently.
            for s in ALL_STAGES:
                setattr(self, f"{s}_status", self.status if s != STAGE_LOAD else (
                    STATUS_EMPTY if self.status == STATUS_EMPTY else STATUS_SUCCESS
                ))
            if self.status == STATUS_EMPTY:
                self.fetch_status = STATUS_EMPTY
                self.clean_status = STATUS_EMPTY
                self.load_status = STATUS_EMPTY
            else:
                # success with inserted=0 still means load succeeded (idempotent)
                self.fetch_status = STATUS_SUCCESS if (self.fetched_rows or 0) > 0 else STATUS_EMPTY
                self.clean_status = STATUS_SUCCESS if (self.cleaned_rows or 0) > 0 else (
                    STATUS_EMPTY if self.fetch_status == STATUS_EMPTY else STATUS_SUCCESS
                )
                self.load_status = STATUS_SUCCESS
            if self.raw_path and self.fetch_status == STATUS_PENDING:
                self.fetch_status = STATUS_SUCCESS if (self.fetched_rows or 0) > 0 else STATUS_EMPTY
        elif self.status == STATUS_FAILED:
            # Unknown which stage failed; leave stages pending so resume re-evaluates by artifacts.
            pass
        elif self.status == STATUS_RUNNING:
            # Interrupted mid-batch.
            self.status = STATUS_PENDING

        # Artifact-based inference for partial progress.
        if self.raw_path and Path(self.raw_path).exists() and self.fetch_status == STATUS_PENDING:
            self.fetch_status = STATUS_SUCCESS if (self.fetched_rows or 0) > 0 else STATUS_EMPTY
        if self.cleaned_path and Path(self.cleaned_path).exists() and self.clean_status == STATUS_PENDING:
            self.clean_status = STATUS_SUCCESS if (self.cleaned_rows or 0) > 0 else STATUS_EMPTY

        self.recompute_status()

    def recompute_status(self) -> str:
        stages = [self.fetch_status, self.clean_status, self.load_status]
        if any(s == STATUS_FAILED for s in stages):
            self.status = STATUS_FAILED
        elif any(s == STATUS_RUNNING for s in stages):
            self.status = STATUS_RUNNING
        elif all(s in TERMINAL_OK for s in stages):
            # Aggregate empty only when fetch is empty (nothing to clean/load)
            if self.fetch_status == STATUS_EMPTY:
                self.status = STATUS_EMPTY
            else:
                self.status = STATUS_SUCCESS
        else:
            self.status = STATUS_PENDING
        return self.status

    def stage_status(self, stage: str) -> str:
        return getattr(self, f"{stage}_status")

    def set_stage(
        self,
        stage: str,
        status: str,
        *,
        error: str | None = None,
        finished: bool = False,
    ) -> None:
        setattr(self, f"{stage}_status", status)
        setattr(self, f"{stage}_error", error)
        if finished:
            setattr(self, f"{stage}_finished_at", utc_now_iso())
        # Keep aggregate error as last non-empty stage error
        errs = [self.fetch_error, self.clean_error, self.load_error]
        self.error = next((e for e in reversed(errs) if e), None)
        self.recompute_status()


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
        record.recompute_status()
        self._records[record.batch_id] = record
        self._flush()

    def update(self, batch_id: str, **fields: Any) -> BatchRecord:
        rec = self._records[batch_id]
        for k, v in fields.items():
            if not hasattr(rec, k):
                raise AttributeError(k)
            setattr(rec, k, v)
        rec.recompute_status()
        self._flush()
        return rec

    def save(self, rec: BatchRecord) -> BatchRecord:
        rec.recompute_status()
        self._records[rec.batch_id] = rec
        self._flush()
        return rec

    def reset_running_to_pending(self) -> int:
        n = 0
        for rec in self._records.values():
            changed = False
            if rec.status == STATUS_RUNNING:
                rec.status = STATUS_PENDING
                changed = True
            for stage in ALL_STAGES:
                if rec.stage_status(stage) == STATUS_RUNNING:
                    setattr(rec, f"{stage}_status", STATUS_PENDING)
                    prev = getattr(rec, f"{stage}_error") or ""
                    setattr(
                        rec,
                        f"{stage}_error",
                        (prev + " | reset_running_on_resume").strip(" |"),
                    )
                    changed = True
            if changed:
                note = rec.error or ""
                rec.error = (note + " | reset_running_on_resume").strip(" |")
                rec.recompute_status()
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

    def stage_counts(self) -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = {}
        for stage in ALL_STAGES:
            c = {s: 0 for s in ALL_STATUSES}
            for rec in self._records.values():
                st = rec.stage_status(stage)
                c[st] = c.get(st, 0) + 1
            c["total"] = len(self._records)
            out[stage] = c
        return out

    def should_process(self, batch_id: str, *, resume: bool, stages: Iterable[str]) -> bool:
        rec = self._records.get(batch_id)
        if rec is None:
            return True
        if not resume:
            return True
        # Process if any requested stage is not terminal-ok.
        for stage in stages:
            if rec.stage_status(stage) not in TERMINAL_OK:
                return True
        return False
