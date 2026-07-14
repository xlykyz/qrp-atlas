"""Parquet raw / cleaned archive helpers for PIT backfill."""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import pandas as pd


class CorruptParquetError(RuntimeError):
    """Raised when an on-disk parquet cannot be read/validated."""

    def __init__(self, path: str | Path, reason: str, quarantined: str | Path | None = None):
        self.path = Path(path)
        self.reason = reason
        self.quarantined = Path(quarantined) if quarantined else None
        msg = f"corrupt parquet {self.path}: {reason}"
        if self.quarantined is not None:
            msg += f" (quarantined to {self.quarantined})"
        super().__init__(msg)


def safe_batch_name(batch_id: str) -> str:
    return batch_id.replace(":", "__").replace("/", "_")


def raw_file_path(raw_dir: str | Path, batch_id: str) -> Path:
    return Path(raw_dir) / f"{safe_batch_name(batch_id)}.parquet"


def cleaned_file_path(cleaned_dir: str | Path, batch_id: str) -> Path:
    return Path(cleaned_dir) / f"{safe_batch_name(batch_id)}.parquet"


def quarantine_corrupt(path: str | Path) -> Path:
    """Move unreadable parquet aside as `.corrupt.<timestamp>` and return path."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    stamp = time.strftime("%Y%m%d%H%M%S")
    dest = path.with_name(f"{path.name}.corrupt.{stamp}")
    n = 1
    while dest.exists():
        dest = path.with_name(f"{path.name}.corrupt.{stamp}.{n}")
        n += 1
    os.replace(path, dest)
    return dest


def validate_parquet(path: str | Path) -> int:
    """Read parquet and return row count; raise CorruptParquetError on failure."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        if path.stat().st_size <= 0:
            raise CorruptParquetError(path, "empty file")
        df = pd.read_parquet(path)
    except CorruptParquetError:
        raise
    except Exception as exc:  # pyarrow / pandas decode failures
        raise CorruptParquetError(path, str(exc)) from exc
    return 0 if df is None else int(len(df))


def save_parquet(df: pd.DataFrame, path: str | Path) -> Path:
    """Atomically write parquet: temp -> write -> flush/fsync -> validate -> replace."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df if df is not None else pd.DataFrame()
    fd, tmp_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp.parquet",
        dir=str(path.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        out.to_parquet(tmp_path, index=False)
        # flush/fsync durable bytes before validation/replace
        with open(tmp_path, "rb+") as fh:
            fh.flush()
            os.fsync(fh.fileno())
        n = validate_parquet(tmp_path)
        if n != len(out):
            raise CorruptParquetError(tmp_path, f"row count mismatch wrote={len(out)} read={n}")
        os.replace(tmp_path, path)
        validate_parquet(path)
        return path
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise


def load_parquet(path: str | Path, *, quarantine: bool = False) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        validate_parquet(path)
        return pd.read_parquet(path)
    except CorruptParquetError as exc:
        if quarantine:
            q = quarantine_corrupt(path)
            raise CorruptParquetError(path, exc.reason, quarantined=q) from exc
        raise


def load_parquet_or_quarantine(path: str | Path) -> pd.DataFrame:
    return load_parquet(path, quarantine=True)


# Back-compat aliases
save_raw_parquet = save_parquet
load_raw_parquet = load_parquet
