"""Explicit core daily-reader switch; the legacy default is unchanged."""
from contextlib import contextmanager

from qrp_atlas.config.settings import get_settings


@contextmanager
def daily_connection(legacy_factory):
    settings = get_settings()
    if settings.lake.daily_source == "legacy":
        con = legacy_factory()
        try:
            yield con
        finally:
            con.close()
    else:
        from .cli import configured_store
        from .store import DataPortal
        portal = DataPortal(configured_store(settings,"core",read_only=True), settings.lake.daily_snapshot)
        with portal.sql_connection(("daily_market_snapshot","adj_factor_changes","trading_calendar")) as con:
            yield con
