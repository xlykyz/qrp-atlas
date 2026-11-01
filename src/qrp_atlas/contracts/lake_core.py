"""Core lake dataset contracts; reuse the established standard table schemas.

The new domain stores complete daily factor snapshots, using the existing
factor field/key contract. It does not claim to recover historical knowledge
time from a current provider response.
"""

from dataclasses import dataclass

from .schema import TableSchema, get_table
from .mappings import TUSHARE_STOCK_BASIC


@dataclass(frozen=True)
class CoreDataset:
    name: str
    endpoint: str
    required_provider_fields: tuple[str, ...]
    provider_key: tuple[str, ...]
    date_field: str | None = "trade_date"
    allow_empty: bool = False

    @property
    def schema(self) -> TableSchema:
        return get_table(self.name)


CORE_DATASETS = {
    item.name: item for item in (
        CoreDataset("trading_calendar", "trade_cal", ("cal_date", "is_open"),
                    ("cal_date",), date_field="cal_date"),
        CoreDataset("stock_info", "stock_basic", tuple(TUSHARE_STOCK_BASIC),
                    ("ts_code",), date_field=None),
        CoreDataset("daily_market_snapshot", "daily",
                    ("ts_code", "trade_date", "open", "high", "low", "close",
                     "pre_close", "pct_chg", "vol", "amount"),
                    ("ts_code", "trade_date")),
        CoreDataset("daily_basic", "daily_basic", ("ts_code", "trade_date", "close"),
                    ("ts_code", "trade_date")),
        CoreDataset("adj_factor_changes", "adj_factor",
                    ("ts_code", "trade_date", "adj_factor"), ("ts_code", "trade_date")),
        CoreDataset("suspend_d", "suspend_d",
                    ("ts_code", "trade_date", "suspend_type"),
                    ("ts_code", "trade_date", "suspend_type"), allow_empty=True),
    )
}

STOCK_BASIC_PARTITIONS = tuple(
    (status, exchange)
    for status in ("L", "D", "P", "G")
    for exchange in ("SSE", "SZSE", "BSE")
)
