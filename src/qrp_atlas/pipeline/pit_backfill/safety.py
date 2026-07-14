"""Pre-run safety checks, DB backup, and file locking."""

from __future__ import annotations

import json
import os
import shutil
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import duckdb

from qrp_atlas.config import DB_PATH, PROJECT_ROOT

MIN_FREE_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB
DEFAULT_LOCK_PATH = PROJECT_ROOT / "data" / "state" / "quant.db.write.lock"
BACKUP_MARKER_NAME = "backup_marker.json"


def free_disk_bytes(path: str | Path) -> int:
    usage = shutil.disk_usage(str(path))
    return int(usage.free)


def assert_disk_space(path: str | Path, *, min_free_bytes: int = MIN_FREE_BYTES) -> int:
    free = free_disk_bytes(path)
    if free < min_free_bytes:
        gb = free / (1024 ** 3)
        need = min_free_bytes / (1024 ** 3)
        raise RuntimeError(f"Insufficient disk space: {gb:.1f} GB free, need >= {need:.1f} GB")
    return free


def assert_db_readable(db_path: str | Path) -> None:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"Database not found: {path}")
    con = duckdb.connect(str(path), read_only=True)
    try:
        con.execute("SELECT 1").fetchone()
    finally:
        con.close()


def checkpoint_and_close(db_path: str | Path) -> None:
    """Force DuckDB checkpoint so file copy is consistent."""
    path = Path(db_path)
    con = duckdb.connect(str(path))
    try:
        try:
            con.execute("CHECKPOINT")
        except Exception:
            # older/readonly variants
            try:
                con.execute("FORCE CHECKPOINT")
            except Exception:
                pass
    finally:
        con.close()


def create_db_backup(
    db_path: str | Path,
    backup_dir: str | Path,
    *,
    tag: str | None = None,
    checkpoint: bool = True,
) -> Path:
    """Create a verified DB backup. Caller should hold write lock when checkpoint=True."""
    src = Path(db_path)
    dest_dir = Path(backup_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    stamp = tag or datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = dest_dir / f"quant.db.pre_pit_backfill_{stamp}"
    if dest.exists():
        assert_db_readable(dest)
        return dest
    if checkpoint:
        checkpoint_and_close(src)
    shutil.copy2(src, dest)
    wal = Path(str(src) + ".wal")
    if wal.exists():
        shutil.copy2(wal, Path(str(dest) + ".wal"))
    assert_db_readable(dest)
    return dest


def backup_marker_path(state_dir: str | Path) -> Path:
    return Path(state_dir) / BACKUP_MARKER_NAME


def load_backup_marker(state_dir: str | Path) -> dict | None:
    path = backup_marker_path(state_dir)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_backup_marker(state_dir: str | Path, *, backup_path: str | Path, tag: str) -> Path:
    path = backup_marker_path(state_dir)
    bp = Path(backup_path)
    payload = {
        "tag": tag,
        "backup_path": str(bp.resolve()),
        "backup_size_bytes": bp.stat().st_size if bp.exists() else None,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "pid": os.getpid(),
        "verified_readonly": True,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def ensure_load_backup(
    db_path: str | Path,
    *,
    state_dir: str | Path,
    tag: str,
    lock_path: str | Path | None = None,
) -> dict:
    """Gate for load stage: under write lock, checkpoint + backup + marker.

    Safe to call multiple times for same tag; reuses existing verified marker/backup.
    """
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    marker = load_backup_marker(state_dir)
    if marker and marker.get("tag") == tag and marker.get("backup_path"):
        bp = Path(marker["backup_path"])
        if bp.exists():
            assert_db_readable(bp)
            return {
                "backup_path": str(bp),
                "marker_path": str(backup_marker_path(state_dir)),
                "backup_size_bytes": bp.stat().st_size,
                "reused": True,
            }

    with pipeline_db_lock(lock_path or DEFAULT_LOCK_PATH):
        # re-check under lock
        marker = load_backup_marker(state_dir)
        if marker and marker.get("tag") == tag and marker.get("backup_path"):
            bp = Path(marker["backup_path"])
            if bp.exists():
                assert_db_readable(bp)
                return {
                    "backup_path": str(bp),
                    "marker_path": str(backup_marker_path(state_dir)),
                    "backup_size_bytes": bp.stat().st_size,
                    "reused": True,
                }
        assert_db_readable(db_path)
        backup_path = create_db_backup(
            db_path,
            state_dir / "backups",
            tag=tag,
            checkpoint=True,
        )
        marker_path = save_backup_marker(state_dir, backup_path=backup_path, tag=tag)
        return {
            "backup_path": str(backup_path.resolve()),
            "marker_path": str(marker_path),
            "backup_size_bytes": backup_path.stat().st_size,
            "reused": False,
        }


class FileLock:
    """Exclusive flock-based lock."""

    def __init__(self, path: str | Path, *, timeout_s: float | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.timeout_s = timeout_s
        self._fd: int | None = None

    def acquire(self) -> None:
        import fcntl

        self._fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o644)
        start = time.time()
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                os.ftruncate(self._fd, 0)
                os.write(
                    self._fd,
                    f"pid={os.getpid()} time={datetime.now().isoformat()}\n".encode(),
                )
                return
            except BlockingIOError:
                if self.timeout_s is not None and (time.time() - start) >= self.timeout_s:
                    raise TimeoutError(f"Could not acquire lock: {self.path}")
                time.sleep(0.5)

    def release(self) -> None:
        import fcntl

        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


@contextmanager
def pipeline_db_lock(
    lock_path: str | Path | None = None, *, timeout_s: float | None = None
) -> Iterator[FileLock]:
    lock = FileLock(lock_path or DEFAULT_LOCK_PATH, timeout_s=timeout_s)
    lock.acquire()
    try:
        yield lock
    finally:
        lock.release()


def preflight(
    db_path: str | Path,
    *,
    state_dir: str | Path,
    min_free_bytes: int = MIN_FREE_BYTES,
    create_backup: bool = False,
    backup_tag: str | None = None,
) -> dict:
    """Lightweight preflight. Load-stage backup is handled by ensure_load_backup()."""
    db_path = Path(db_path)
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    free = assert_disk_space(
        db_path.parent if db_path.exists() else Path.cwd(),
        min_free_bytes=min_free_bytes,
    )
    assert_db_readable(db_path)
    backup_path = None
    if create_backup:
        info = ensure_load_backup(
            db_path,
            state_dir=state_dir,
            tag=backup_tag or "preflight",
        )
        backup_path = info["backup_path"]
    return {
        "free_bytes": free,
        "free_gb": round(free / (1024 ** 3), 2),
        "db_path": str(db_path.resolve()),
        "backup_path": backup_path,
        "lock_path": str(DEFAULT_LOCK_PATH),
    }
