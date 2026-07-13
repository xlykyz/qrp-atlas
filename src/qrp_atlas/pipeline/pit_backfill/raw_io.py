"""Parquet raw archive helpers for PIT backfill."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def raw_file_path(raw_dir: str | Path, batch_id: str) -> Path:
    # batch_id uses ':' which is awkward on some FS; sanitize.
    safe = batch_id.replace(":", "__").replace("/", "_")
    return Path(raw_dir) / f"{safe}.parquet"


def save_raw_parquet(df: pd.DataFrame, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Empty frames still get an empty parquet for offline resume.
    out = df if df is not None else pd.DataFrame()
    out.to_parquet(path, index=False)
    return path


def load_raw_parquet(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path)
