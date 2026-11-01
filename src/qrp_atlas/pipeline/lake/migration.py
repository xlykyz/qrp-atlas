"""Read-only legacy export/import. Never labels normalized legacy rows as Raw."""
from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path

import duckdb
import pandas as pd

from qrp_atlas.contracts import get_table
from qrp_atlas.contracts.lake_core import CORE_DATASETS
from .store import DataPortal, DuckLakeStore, qi


def import_legacy(source: Path, store: DuckLakeStore, dataset: str, *, start: date,
                  end: date, dry_run: bool = True):
    if dataset not in CORE_DATASETS:
        raise ValueError("dataset is not in this migration scope")
    if start > end:
        raise ValueError("invalid migration range")
    source = Path(source).resolve(strict=True)
    if source.is_relative_to(store.base.resolve()):
        raise ValueError("source and destination overlap")
    schema = get_table(dataset)
    is_dated = "trade_date" in schema.column_names()
    reports = []
    with duckdb.connect(str(source), read_only=True) as old:
        old.execute("BEGIN")
        if is_dated:
            dates = [r[0] for r in old.execute(f"SELECT DISTINCT trade_date FROM {qi(dataset)} WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date", [start,end]).fetchall()]
        else:
            # Legacy current reference cannot be asserted to be historical.
            if start != end:
                raise ValueError("legacy reference requires one explicit import label date")
            dates = [end]
        for partition in dates:
            columns = ",".join(map(qi, schema.column_names()))
            sql = f"SELECT {columns} FROM {qi(dataset)}"
            frame = old.execute(sql + (" WHERE trade_date=?" if is_dated else ""), [partition] if is_dated else []).fetchdf()
            ordered = frame.sort_values(list(schema.primary_key)).reset_index(drop=True)
            checksum = hashlib.sha256(pd.util.hash_pandas_object(ordered, index=False).values.tobytes()).hexdigest()
            evidence = {"source_kind": "legacy_normalized", "source_file": source.name,
                        "source_size": source.stat().st_size, "mapping_version": "legacy-v1",
                        "rows": len(frame), "row_fingerprint": checksum,
                        "pit_limit": "No new publication or observation history is inferred"}
            row = {"dataset": dataset, "partition": str(partition), "rows": len(frame),
                   "fingerprint": checksum, "dry_run": dry_run, "source_observation": evidence}
            if not dry_run:
                key = "legacy-v1:" + checksum
                identity = hashlib.sha256((dataset + "\0" + str(partition) + "\0" + key).encode()).hexdigest()
                publication = store.recover(identity)
                if publication is not None:
                    # Only migration's file observation is allowed to differ.
                    # Verify stable mapping/content, then compare actual rows
                    # below; never weaken the general publisher lineage check.
                    stable = ("source_kind", "mapping_version", "rows", "row_fingerprint", "pit_limit")
                    if publication.time_model != "legacy" or any(publication.lineage.get(k) != evidence[k] for k in stable):
                        raise ValueError("legacy import identity conflicts with existing evidence")
                else:
                    publication = store.publish(schema, frame, partition=str(partition),
                        partition_column="trade_date" if is_dated else None,
                        idempotency_key=key, lineage=evidence, time_model="legacy")
                restored = DataPortal(store, publication.snapshot_id).read(dataset)
                if is_dated:
                    restored = restored.loc[pd.to_datetime(restored.trade_date).dt.date == partition]
                left = frame.sort_values(list(schema.primary_key)).reset_index(drop=True)
                right = restored.sort_values(list(schema.primary_key)).reset_index(drop=True)
                pd.testing.assert_frame_equal(left, right, check_dtype=False)
                row.update(publication_id=publication.publication_id, snapshot_id=publication.snapshot_id,
                           comparison="equal")
            reports.append(row)
    return reports
