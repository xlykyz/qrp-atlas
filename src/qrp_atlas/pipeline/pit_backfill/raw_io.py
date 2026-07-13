"""Parquet raw / cleaned archive helpers for PIT backfill."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def safe_batch_name(batch_id: str) -> str:
    return batch_id.replace(":", "__").replace("/", "_")


def raw_file_path(raw_dir: str | Path, batch_id: str) -> Path:
    return Path(raw_dir) / f"{safe_batch_name(batch_id)}.parquet"


def cleaned_file_path(cleaned_dir: str | Path, batch_id: str) -> Path:
    return Path(cleaned_dir) / f"{safe_batch_name(batch_id)}.parquet"


def save_parquet(df: pd.DataFrame, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df if df is not None else pd.DataFrame()
    out.to_parquet(path, index=False)
    return path


def load_parquet(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path)


# Back-compat aliases
save_raw_parquet = save_parquet
load_raw_parquet = load_parquet
