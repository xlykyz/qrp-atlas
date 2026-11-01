"""Subprocess helper for abrupt-exit publication recovery tests only."""

from datetime import date
import os
from pathlib import Path
import sys

import pandas as pd

from qrp_atlas.contracts.schema import ColumnSpec, TableSchema
from qrp_atlas.pipeline.lake.store import DuckLakeStore, LakeLocation


def main():
    store = DuckLakeStore(LakeLocation(os.environ["QRP_TEST_CRASH_CATALOG"], Path(sys.argv[1])))
    schema = TableSchema("prices", (
        ColumnSpec("trade_date", "DATE", False), ColumnSpec("ticker", "VARCHAR", False),
        ColumnSpec("close", "DOUBLE", False),
    ), ("trade_date", "ticker"))
    def crash(point):
        if point == sys.argv[2]:
            os._exit(91)
    previous = store.current_publication("prices", "2026-09-30")
    store.publish(schema, pd.DataFrame({
        "trade_date": [date(2026, 9, 30)], "ticker": ["000001.SZ"], "close": [11.],
    }), partition="2026-09-30", idempotency_key="v2", lineage={"raw_batch_id": "recorded"},
        expected_previous=previous, fault=crash)
    raise RuntimeError("fault injection point was not reached")


if __name__ == "__main__":
    main()
