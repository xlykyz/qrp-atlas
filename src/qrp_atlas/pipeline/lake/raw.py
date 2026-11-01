"""Immutable observed payloads, committed before any business normalization."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc


def safe_parameters(value):
    if isinstance(value, dict):
        return {str(k): ("[REDACTED]" if re.search(r"token|secret|password|credential|authorization|dsn", str(k), re.I)
                         else safe_parameters(v)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe_parameters(v) for v in value]
    if isinstance(value, str) and ("://" in value or "token=" in value.lower()):
        return "[REDACTED URL]"
    return value


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_\-]*", value):
        raise ValueError("invalid dataset/source identifier")
    return value


def durable_write(path: Path, content: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


@dataclass(frozen=True)
class RawBatch:
    batch_id: str
    directory: Path
    checksum: str
    complete: bool
    status: str
    observed_at: str
    source: str
    dataset: str
    format: str
    parameters: dict


class RawStore:
    def __init__(self, root: Path, *, read_only: bool = False):
        self.root = Path(root).resolve()
        self.read_only = read_only

    def save_bytes(self, source, dataset, payload, *, parameters, complete,
                   observed_at=None, sdk_version="unknown", status="complete",
                   fetch_id=None, format="source_bytes") -> RawBatch:
        if self.read_only:
            raise PermissionError("raw store is read only")
        identifier(source)
        identifier(dataset)
        batch_id = fetch_id or uuid.uuid4().hex
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", batch_id):
            raise ValueError("invalid raw batch identity")
        if status not in {"complete", "partial", "failed", "empty"}:
            raise ValueError("invalid raw status")
        if complete and status in {"partial", "failed"}:
            raise ValueError("failed/partial raw cannot be complete")
        instant = observed_at or datetime.now(UTC)
        if isinstance(instant, datetime):
            if instant.tzinfo is None:
                raise ValueError("observed_at must have timezone")
            instant = instant.isoformat()
        elif datetime.fromisoformat(instant).tzinfo is None:
            raise ValueError("observed_at must have timezone")
        destination = self.root / batch_id
        self.root.mkdir(parents=True, exist_ok=True)
        # mkdir is the atomic reservation; even incomplete attempts are never overwritten.
        destination.mkdir()
        digest = hashlib.sha256(payload).hexdigest()
        durable_write(destination / "payload", payload)
        meta = dict(batch_id=batch_id, checksum=digest, complete=bool(complete), status=status,
                    observed_at=instant, source=source, dataset=dataset, format=format,
                    sdk_version=sdk_version, parameters=safe_parameters(parameters),
                    size_bytes=len(payload), observation_layer="sdk_output" if format == "arrow_ipc" else "source_bytes")
        durable_write(destination / "batch.json", json.dumps(meta, ensure_ascii=False, sort_keys=True).encode())
        return self.metadata(batch_id)

    def save_frame(self, source, dataset, frame, **kwargs) -> RawBatch:
        table = pa.Table.from_pandas(frame, preserve_index=True)
        sink = pa.BufferOutputStream()
        with ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        return self.save_bytes(source, dataset, sink.getvalue().to_pybytes(), format="arrow_ipc", **kwargs)

    def metadata(self, batch_id: str) -> RawBatch:
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", batch_id):
            raise ValueError("invalid raw batch identity")
        directory = self.root / batch_id
        meta = json.loads((directory / "batch.json").read_text(encoding="utf-8"))
        content = (directory / "payload").read_bytes()
        if hashlib.sha256(content).hexdigest() != meta["checksum"]:
            raise ValueError("raw checksum mismatch")
        return RawBatch(directory=directory, **{k: meta[k] for k in RawBatch.__dataclass_fields__ if k != "directory"})

    def load_frame(self, batch_id: str) -> pd.DataFrame:
        batch = self.metadata(batch_id)
        if batch.format != "arrow_ipc":
            raise ValueError("batch is not an SDK frame")
        with ipc.open_file(batch.directory / "payload") as reader:
            return reader.read_all().to_pandas()
