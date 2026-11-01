"""Storage contracts for the isolated extension publication domain.

System B business fields retain the established production types. Publication
identity, input snapshots and producer metadata live in the lake publication
record, rather than fabricated legacy production-run rows.
"""

from .fields import ASSET_ID, TICKER, TRADE_DATE
from .schema import ColumnSpec, TableSchema, SYSTEM_B_STATE_OBSERVATION
from .system_b import SYSTEM_B_STATE_OUTPUT_COLUMNS


_STATE_COLUMNS = {column.name: column for column in SYSTEM_B_STATE_OBSERVATION.columns}
LAKE_SYSTEM_B_STATE = TableSchema(
    name="system_b_state",
    columns=tuple(_STATE_COLUMNS[name] for name in SYSTEM_B_STATE_OUTPUT_COLUMNS),
    primary_key=SYSTEM_B_STATE_OBSERVATION.primary_key,
)

LAKE_ROLLING_MEAN = TableSchema(
    name="rolling_mean",
    columns=(
        ColumnSpec(TICKER, "VARCHAR", nullable=False),
        ColumnSpec(TRADE_DATE, "DATE", nullable=False),
        ColumnSpec("mean_close", "DOUBLE"),
    ),
    primary_key=(TICKER, TRADE_DATE),
)

PLUGIN_SCHEMAS = {
    LAKE_SYSTEM_B_STATE.name: LAKE_SYSTEM_B_STATE,
    LAKE_ROLLING_MEAN.name: LAKE_ROLLING_MEAN,
}
