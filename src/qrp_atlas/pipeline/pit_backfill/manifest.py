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


class _ManifestFileLock:
    """Cross-platform exclusive lock for manifest read-modify-write."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._lock = None

    def __enter__(self):
        from filelock import FileLock as _FileLock

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = _FileLock(str(self.path), timeout=-1)
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._lock is not None:
            self._lock.release(force=True)
            self._lock = None


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
    cleaned_path: str | None = None
    error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
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
        stage_touched = any(getattr(self, f"{s}_status") != STATUS_PENDING for s in ALL_STAGES)
        if stage_touched:
            self.recompute_status()
            return

        if self.status in TERMINAL_OK:
            if self.status == STATUS_EMPTY:
                self.fetch_status = STATUS_EMPTY
                self.clean_status = STATUS_EMPTY
                self.load_status = STATUS_EMPTY
            else:
                self.fetch_status = STATUS_SUCCESS if (self.fetched_rows or 0) > 0 else STATUS_EMPTY
                if (self.cleaned_rows or 0) > 0:
                    self.clean_status = STATUS_SUCCESS
                else:
                    self.clean_status = STATUS_EMPTY if self.fetch_status == STATUS_EMPTY else STATUS_SUCCESS
                self.load_status = STATUS_SUCCESS
        elif self.status == STATUS_RUNNING:
            self.status = STATUS_PENDING

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
            self.status = STATUS_EMPTY if self.fetch_status == STATUS_EMPTY else STATUS_SUCCESS
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
        errs = [self.fetch_error, self.clean_error, self.load_error]
        self.error = next((e for e in reversed(errs) if e), None)
        self.recompute_status()


def _stage_rank(status: str) -> int:
    return {
        STATUS_PENDING: 0,
        STATUS_RUNNING: 1,
        STATUS_FAILED: 2,
        STATUS_EMPTY: 3,
        STATUS_SUCCESS: 3,
    }.get(status, 0)


def _merge_records(existing: BatchRecord, incoming: BatchRecord) -> BatchRecord:
    """Merge multi-process updates without clobbering terminal stage progress.

    Terminal success/empty must not be overwritten by pending/running/failed from
    another worker. Explicit re-open to pending is allowed only when the incoming
    record carries a reset/corrupt/gate reason (operator-driven retry).
    """
    out = BatchRecord.from_dict(existing.to_dict())
    for f in ("period", "start_date", "end_date", "raw_path", "cleaned_path", "meta"):
        iv = getattr(incoming, f)
        if iv:
            setattr(out, f, iv)
    out.attempts = max(int(existing.attempts or 0), int(incoming.attempts or 0))
    out.fetched_rows = max(int(existing.fetched_rows or 0), int(incoming.fetched_rows or 0))
    out.cleaned_rows = max(int(existing.cleaned_rows or 0), int(incoming.cleaned_rows or 0))
    out.inserted_rows = max(int(existing.inserted_rows or 0), int(incoming.inserted_rows or 0))

    def _allows_terminal_reopen(stage: str) -> bool:
        err = (getattr(incoming, f"{stage}_error") or incoming.error or "").lower()
        markers = (
            "corrupt",
            "raw missing",
            "reset after",
            "gate",
            "re-fetch",
            "refetch",
            "reset_running",
        )
        return any(m in err for m in markers)

    for stage in ALL_STAGES:
        es = getattr(existing, f"{stage}_status")
        ins = getattr(incoming, f"{stage}_status")
        if es in TERMINAL_OK and ins not in TERMINAL_OK:
            if ins == STATUS_PENDING and _allows_terminal_reopen(stage):
                setattr(out, f"{stage}_status", ins)
                setattr(out, f"{stage}_error", getattr(incoming, f"{stage}_error"))
                setattr(out, f"{stage}_finished_at", None)
            # otherwise keep terminal success/empty
            continue
        if _stage_rank(ins) > _stage_rank(es) or (ins in TERMINAL_OK and es not in TERMINAL_OK):
            setattr(out, f"{stage}_status", ins)
            setattr(out, f"{stage}_error", getattr(incoming, f"{stage}_error"))
            fin = getattr(incoming, f"{stage}_finished_at") or getattr(existing, f"{stage}_finished_at")
            setattr(out, f"{stage}_finished_at", fin)
        elif ins == es and ins in TERMINAL_OK:
            fin = getattr(incoming, f"{stage}_finished_at") or getattr(existing, f"{stage}_finished_at")
            setattr(out, f"{stage}_finished_at", fin)

    if incoming.started_at and (not out.started_at or incoming.started_at > (out.started_at or "")):
        # keep earliest started_at for stale detection stability
        if not out.started_at:
            out.started_at = incoming.started_at
    if incoming.finished_at:
        out.finished_at = incoming.finished_at
    # Prefer non-empty newer error when stage changed
    out.error = incoming.error if incoming.error is not None else existing.error
    out.recompute_status()
    return out


class ManifestStore:
    """Atomic JSONL manifest keyed by batch_id with cross-process locking."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path = Path(str(self.path) + ".lock")
        self._records: dict[str, BatchRecord] = {}
        with _ManifestFileLock(self._lock_path):
            self._load()

    def _load(self) -> None:
        self._records = {}
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = BatchRecord.from_dict(json.loads(line))
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
        with _ManifestFileLock(self._lock_path):
            self._load()
            changed = False
            for batch in batches:
                if batch.batch_id not in self._records:
                    self._records[batch.batch_id] = BatchRecord.from_batch(batch)
                    changed = True
            if changed:
                self._flush()

    def get(self, batch_id: str) -> BatchRecord | None:
        # lightweight in-memory read; callers that need freshness should save/reload
        return self._records.get(batch_id)

    def reload(self) -> None:
        with _ManifestFileLock(self._lock_path):
            self._load()

    def upsert(self, record: BatchRecord) -> None:
        with _ManifestFileLock(self._lock_path):
            self._load()
            record.recompute_status()
            self._records[record.batch_id] = record
            self._flush()

    def update(self, batch_id: str, **fields: Any) -> BatchRecord:
        with _ManifestFileLock(self._lock_path):
            self._load()
            rec = self._records[batch_id]
            for k, v in fields.items():
                if not hasattr(rec, k):
                    raise AttributeError(k)
                setattr(rec, k, v)
            rec.recompute_status()
            self._records[batch_id] = rec
            self._flush()
            return rec

    def save(self, rec: BatchRecord) -> BatchRecord:
        with _ManifestFileLock(self._lock_path):
            self._load()
            existing = self._records.get(rec.batch_id)
            if existing is not None:
                rec = _merge_records(existing, rec)
            rec.recompute_status()
            self._records[rec.batch_id] = rec
            self._flush()
            return rec

    def reset_running_to_pending(
        self,
        *,
        stages: Iterable[str] | None = None,
        stale_seconds: float | None = 3600.0,
        owner_stages_only: bool = True,
    ) -> int:
        """Reset running stage flags.

        Only touches stages in `stages` (default ALL_STAGES). When stale_seconds
        is set, only reset stages whose started/finished timestamps are older
        than the threshold or missing (treat as stale). This prevents a
        clean/load worker from clobbering an active fetch.
        """
        wanted = tuple(stages) if stages is not None else ALL_STAGES
        wanted = tuple(s for s in wanted if s in ALL_STAGES)
        now = datetime.now(timezone.utc)

        def _is_stale(rec: BatchRecord, stage: str) -> bool:
            if stale_seconds is None:
                return True
            # Running stages should not yet have finished_at; use batch started_at.
            # Stage finished_at present with status running is inconsistent -> treat as stale.
            ts = rec.started_at
            stage_finished = getattr(rec, f"{stage}_finished_at")
            if stage_finished and rec.stage_status(stage) == STATUS_RUNNING:
                return True
            if not ts:
                return True
            try:
                text_ts = str(ts).replace("Z", "+00:00")
                dt = datetime.fromisoformat(text_ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return (now - dt).total_seconds() >= float(stale_seconds)
            except Exception:
                return True

        with _ManifestFileLock(self._lock_path):
            self._load()
            n = 0
            for rec in self._records.values():
                changed = False
                for stage in wanted:
                    if rec.stage_status(stage) != STATUS_RUNNING:
                        continue
                    if not _is_stale(rec, stage):
                        continue
                    setattr(rec, f"{stage}_status", STATUS_PENDING)
                    prev = getattr(rec, f"{stage}_error") or ""
                    setattr(
                        rec,
                        f"{stage}_error",
                        (prev + " | reset_running_on_resume").strip(" |"),
                    )
                    changed = True
                if changed:
                    # recompute aggregate; do not force-reset status independently
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
        for stage in stages:
            if rec.stage_status(stage) not in TERMINAL_OK:
                return True
        return False
