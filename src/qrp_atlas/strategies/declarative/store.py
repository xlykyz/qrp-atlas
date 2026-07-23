"""Versioned persistence for declarative strategies.

Immutable version records; no overwrite of run-referenced versions.
Local auth still binds a stable owner_user_id.
"""

from __future__ import annotations

import json
import os
import re
import threading
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID
from uuid import uuid4

from filelock import FileLock

from qrp_atlas.config.settings import AppSettings, require_writable
from qrp_atlas.strategies.registry import list_strategies

from .evaluator import DeclarativeStrategy
from .models import DeclarativeStrategySpec

_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{1,63}$")
_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")


class DeclarativeStoreError(ValueError):
    """Invalid declarative strategy store operation."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _default_root() -> Path:
    return AppSettings.load().paths.declarative_strategies_dir


def deterministic_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def semver_key(version: str) -> tuple[int, int, int]:
    if not _VERSION_RE.match(version):
        raise DeclarativeStoreError(f"invalid semantic version: {version}")
    major, minor, patch = version.split(".")
    return int(major), int(minor), int(patch)


@dataclass(frozen=True)
class DeclarativeStrategyRecord:
    code: str
    version: str
    owner_user_id: str
    name: str
    description: str
    status: str  # active | archived | disabled
    definition: dict[str, Any]
    created_at: str
    archived_at: str | None = None
    referenced_by_runs: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DeclarativeStrategyRecord":
        return cls(
            code=str(payload["code"]),
            version=str(payload["version"]),
            owner_user_id=str(payload["owner_user_id"]),
            name=str(payload["name"]),
            description=str(payload.get("description") or ""),
            status=str(payload.get("status") or "active"),
            definition=dict(payload["definition"]),
            created_at=str(payload["created_at"]),
            archived_at=payload.get("archived_at"),
            referenced_by_runs=bool(payload.get("referenced_by_runs", False)),
        )


def validate_declarative_payload(payload: dict[str, Any]) -> DeclarativeStrategySpec:
    """Static validation: whitelist operators/refs/types; no eval/exec path."""

    if not isinstance(payload, dict):
        raise DeclarativeStoreError("definition must be an object")
    code = str(payload.get("code") or "").strip()
    version = str(payload.get("version") or "").strip()
    if not _CODE_RE.match(code):
        raise DeclarativeStoreError(
            "code must match ^[a-z][a-z0-9_]{1,63}$"
        )
    if not _VERSION_RE.match(version):
        raise DeclarativeStoreError("version must be semver like 1.0.0")
    # forbid dangerous keys
    text = deterministic_json(payload)
    for banned in ("__import__", "eval(", "exec(", "os.system", "subprocess", "open("):
        if banned in text:
            raise DeclarativeStoreError(f"forbidden token in definition: {banned}")
    try:
        spec = DeclarativeStrategySpec.from_dict(payload)
        # instantiate to run full static checks
        DeclarativeStrategy(spec)
    except Exception as exc:  # noqa: BLE001
        raise DeclarativeStoreError(str(exc)) from exc
    return spec


class DeclarativeStrategyStore:
    """Filesystem store: one JSON file per code@version under owner namespace."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        settings: AppSettings | None = None,
    ) -> None:
        effective = settings or AppSettings.load()
        configured_root = effective.paths.declarative_strategies_dir
        self.root = Path(root) if root is not None else configured_root
        self._write_settings = (
            effective
            if self.root.resolve(strict=False) == configured_root.resolve(strict=False)
            else None
        )
        if self._write_settings is None or not effective.runtime.read_only:
            self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._file_lock_path = self.root / ".store.lock"

    def _require_writable(self) -> None:
        if self._write_settings is not None:
            require_writable(
                self._write_settings,
                operation="writing configured declarative strategy storage",
            )

    @contextmanager
    def _exclusive(self):
        with self._lock:
            with FileLock(str(self._file_lock_path), timeout=30):
                yield

    @staticmethod
    def _write_atomic(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            with temp_path.open("w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
                    + "\n"
                )
                handle.flush()
                os.fsync(handle.fileno())
            temp_path.replace(path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def _path(self, owner_user_id: str, code: str, version: str) -> Path:
        safe_owner = re.sub(r"[^A-Za-z0-9_-]", "_", owner_user_id)
        return self.root / safe_owner / f"{code}@{version}.json"

    def _index_path(self, owner_user_id: str) -> Path:
        safe_owner = re.sub(r"[^A-Za-z0-9_-]", "_", owner_user_id)
        return self.root / safe_owner / "index.json"

    def _load_index(self, owner_user_id: str) -> dict[str, Any]:
        path = self._index_path(owner_user_id)
        if not path.exists():
            return {"owner_user_id": owner_user_id, "items": []}
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_index(self, owner_user_id: str, index: dict[str, Any]) -> None:
        path = self._index_path(owner_user_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._write_atomic(path, index)

    def validate(self, payload: dict[str, Any]) -> dict[str, Any]:
        spec = validate_declarative_payload(payload)
        return {
            "ok": True,
            "code": spec.definition.code,
            "version": spec.definition.version,
            "normalized": spec.to_dict(),
            "canonical_json": deterministic_json(spec.to_dict()),
        }

    def create(
        self,
        payload: dict[str, Any],
        *,
        owner_user_id: str | UUID,
        allow_overwrite: bool = False,
    ) -> DeclarativeStrategyRecord:
        """Create a versioned definition.

        Version identity is content-addressed by owner/code/version and is strictly
        immutable from creation. ``allow_overwrite`` is retained only to reject legacy
        callers explicitly rather than silently changing behavior.
        """

        self._require_writable()
        owner = str(owner_user_id)
        spec = validate_declarative_payload(payload)
        builtin_codes = {definition.code for definition in list_strategies()}
        if spec.definition.code in builtin_codes:
            raise DeclarativeStoreError(
                f"declarative strategy code conflicts with builtin strategy: {spec.definition.code}"
            )
        if allow_overwrite:
            raise DeclarativeStoreError("declarative strategy versions are strictly immutable")
        definition = spec.to_dict()
        # freeze exact definition content
        record = DeclarativeStrategyRecord(
            code=spec.definition.code,
            version=spec.definition.version,
            owner_user_id=owner,
            name=spec.definition.name,
            description=spec.definition.description,
            status="active",
            definition=definition,
            created_at=_now(),
        )
        path = self._path(owner, record.code, record.version)
        with self._exclusive():
            if path.exists():
                raise DeclarativeStoreError(
                    f"version already exists and is immutable: {record.code}@{record.version}"
                )
            self._write_atomic(path, record.to_dict())
            index = self._load_index(owner)
            items = [
                item
                for item in index.get("items", [])
                if not (
                    item.get("code") == record.code and item.get("version") == record.version
                )
            ]
            items.append(
                {
                    "code": record.code,
                    "version": record.version,
                    "name": record.name,
                    "status": record.status,
                    "created_at": record.created_at,
                }
            )
            items.sort(key=lambda x: (x["code"], semver_key(str(x["version"]))))
            index["items"] = items
            self._save_index(owner, index)
        return record

    def create_new_version(
        self,
        code: str,
        payload: dict[str, Any],
        *,
        owner_user_id: str | UUID,
    ) -> DeclarativeStrategyRecord:
        data = dict(payload)
        data["code"] = code
        # require explicit new version
        if "version" not in data:
            raise DeclarativeStoreError("new version payload must include version")
        return self.create(data, owner_user_id=owner_user_id, allow_overwrite=False)

    def get(
        self,
        code: str,
        version: str,
        *,
        owner_user_id: str | UUID = "local-user",
    ) -> DeclarativeStrategyRecord:
        path = self._path(str(owner_user_id), code, version)
        if not path.exists():
            raise DeclarativeStoreError(f"strategy not found: {code}@{version}")
        return DeclarativeStrategyRecord.from_dict(
            json.loads(path.read_text(encoding="utf-8"))
        )

    def list(
        self,
        *,
        owner_user_id: str | UUID = "local-user",
        include_archived: bool = False,
    ) -> list[DeclarativeStrategyRecord]:
        records: list[DeclarativeStrategyRecord] = []
        roots = [self.root / re.sub(r"[^A-Za-z0-9_-]", "_", str(owner_user_id))]
        for owner_dir in roots:
            if not owner_dir.exists():
                continue
            for path in owner_dir.glob("*@*.json"):
                record = DeclarativeStrategyRecord.from_dict(
                    json.loads(path.read_text(encoding="utf-8"))
                )
                if not include_archived and record.status in {"archived", "disabled"}:
                    continue
                records.append(record)
        records.sort(key=lambda r: (r.code, semver_key(r.version)))
        return records

    def set_status(
        self,
        code: str,
        version: str,
        *,
        owner_user_id: str | UUID,
        status: str,
    ) -> DeclarativeStrategyRecord:
        self._require_writable()
        if status not in {"active", "archived", "disabled"}:
            raise DeclarativeStoreError("status must be active|archived|disabled")
        owner = str(owner_user_id)
        path = self._path(owner, code, version)
        if not path.exists():
            raise DeclarativeStoreError(f"strategy not found: {code}@{version}")
        with self._exclusive():
            record = DeclarativeStrategyRecord.from_dict(
                json.loads(path.read_text(encoding="utf-8"))
            )
            if record.owner_user_id != owner:
                raise DeclarativeStoreError("owner mismatch")
            if record.referenced_by_runs and status == "active" and record.status != "active":
                # re-activating archived is ok; overwriting definition is not
                pass
            updated = DeclarativeStrategyRecord(
                code=record.code,
                version=record.version,
                owner_user_id=record.owner_user_id,
                name=record.name,
                description=record.description,
                status=status,
                definition=record.definition,
                created_at=record.created_at,
                archived_at=_now() if status in {"archived", "disabled"} else None,
                referenced_by_runs=record.referenced_by_runs,
            )
            self._write_atomic(path, updated.to_dict())
            index = self._load_index(owner)
            for item in index.get("items", []):
                if item.get("code") == code and item.get("version") == version:
                    item["status"] = status
            self._save_index(owner, index)
            return updated

    def mark_referenced(self, code: str, version: str, *, owner_user_id: str | UUID) -> None:
        self._require_writable()
        owner = str(owner_user_id)
        path = self._path(owner, code, version)
        if not path.exists():
            raise DeclarativeStoreError(f"strategy not found: {code}@{version}")
        with self._exclusive():
            record = DeclarativeStrategyRecord.from_dict(
                json.loads(path.read_text(encoding="utf-8"))
            )
            if record.referenced_by_runs:
                return
            updated = DeclarativeStrategyRecord(
                **{**record.to_dict(), "referenced_by_runs": True}
            )
            self._write_atomic(path, updated.to_dict())


_store: DeclarativeStrategyStore | None = None


def get_declarative_store() -> DeclarativeStrategyStore:
    global _store
    if _store is None:
        _store = DeclarativeStrategyStore()
    return _store


def reset_declarative_store_for_tests(store: DeclarativeStrategyStore | None) -> None:
    global _store
    _store = store
