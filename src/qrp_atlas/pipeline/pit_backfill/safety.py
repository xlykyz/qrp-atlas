"""Pre-run safety checks, DB backup, and file locking."""

from __future__ import annotations

import os
import shutil
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

import duckdb

from qrp_atlas.config import DB_PATH, PROJECT_ROOT

MIN_FREE_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB
DEFAULT_LOCK_PATH = PROJECT_ROOT / "data" / "state" / "quant.db.write.lock"


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


def create_db_backup(
    db_path: str | Path,
    backup_dir: str | Path,
    *,
    tag: str | None = None,
) -> Path:
    src = Path(db_path)
    dest_dir = Path(backup_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    stamp = tag or datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = dest_dir / f"quant.db.pre_pit_backfill_{stamp}"
    if dest.exists():
        return dest
    # Prefer DuckDB EXPORT for consistency; fall back to file copy.
    try:
        con = duckdb.connect(str(src), read_only=True)
        try:
            # COPY DATABASE requires a connection target; file copy is fine for offline file DB.
            pass
        finally:
            con.close()
    except Exception:
        pass
    shutil.copy2(src, dest)
    # Also copy WAL if present (DuckDB may keep one).
    wal = Path(str(src) + ".wal")
    if wal.exists():
        shutil.copy2(wal, Path(str(dest) + ".wal"))
    return dest


class FileLock:
    """Exclusive flock-based lock (non-blocking open + blocking acquire)."""

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
                os.write(self._fd, f"pid={os.getpid()} time={datetime.now().isoformat()}\n".encode())
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
def pipeline_db_lock(lock_path: str | Path | None = None, *, timeout_s: float | None = None) -> Iterator[FileLock]:
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
    create_backup: bool = True,
    backup_tag: str | None = None,
) -> dict:
    db_path = Path(db_path)
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    free = assert_disk_space(db_path.parent if db_path.exists() else Path.cwd(), min_free_bytes=min_free_bytes)
    assert_db_readable(db_path)
    backup_path = None
    if create_backup:
        backup_path = create_db_backup(db_path, state_dir / "backups", tag=backup_tag)
    return {
        "free_bytes": free,
        "free_gb": round(free / (1024 ** 3), 2),
        "db_path": str(db_path.resolve()),
        "backup_path": str(backup_path) if backup_path else None,
        "lock_path": str(DEFAULT_LOCK_PATH),
    }
