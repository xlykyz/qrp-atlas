"""Durable input bindings for one caller-owned invocation, not job scheduling.

Dagster still owns retries and lifecycle. This record only freezes the first
input references, Raw batch and CAS baseline, so a lost reply can be replayed.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import uuid

from filelock import FileLock


class InvocationBinding:
    def __init__(self, path: Path, data: dict):
        self.path, self.data = path, data

    def save(self):
        temporary = self.path.with_name(self.path.name + "." + uuid.uuid4().hex)
        try:
            with temporary.open("x", encoding="utf-8") as stream:
                json.dump(self.data, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            if os.name != "nt":
                fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)

    def value(self, name, factory):
        if name not in self.data:
            self.data[name] = factory()
            self.save()
        return self.data[name]


@contextmanager
def bind_invocation(root, invocation_id, scope, request):
    if not isinstance(invocation_id, str) or not 1 <= len(invocation_id) <= 256:
        raise ValueError("invalid invocation identity")
    directory = Path(root) / "core-runtime" / "invocations"
    directory.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(invocation_id.encode()).hexdigest()
    path = directory / (key + ".json")
    with FileLock(str(directory / (key + ".lock")), timeout=30):
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("scope") != scope:
                raise ValueError("invocation identity reused for a different operation")
        else:
            data = {"scope": scope, "request": request()}
        binding = InvocationBinding(path, data)
        if not path.exists():
            binding.save()
        yield binding
