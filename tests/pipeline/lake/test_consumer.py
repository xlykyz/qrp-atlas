from dataclasses import replace
from datetime import date

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from qrp_atlas.api.routes import daily
from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import get_table
from qrp_atlas.pipeline.lake import consumer
from qrp_atlas.pipeline.lake.store import DuckLakeStore, LakeLocation


def test_existing_daily_api_uses_pinned_lake_without_legacy_connection(tmp_path, monkeypatch):
    catalog = str(tmp_path / "catalog.ducklake")
    store = DuckLakeStore(LakeLocation(catalog, tmp_path))
    store.initialize()
    final = None
    for name, values in [
        ("daily_market_snapshot", {"trade_date":date(2026,9,30), "ticker":"000001.SZ", "name":"test", "open":10., "high":11., "low":9., "close":10., "pre_close":9.5}),
        ("adj_factor_changes", {"trade_date":date(2026,9,30), "ticker":"000001.SZ", "adj_factor":1.}),
        ("trading_calendar", {"trade_date":date(2026,9,30), "is_open":True, "year":2026,"month":9,"quarter":3}),
    ]:
        schema = get_table(name)
        frame = pd.DataFrame([{col:values.get(col) for col in schema.column_names()}])
        final = store.publish(schema,frame,partition="2026-09-30",idempotency_key="fixture",lineage={})
    settings = AppSettings.load(environ={"QRP_LAKE_ROOT":str(tmp_path),"QRP_LAKE_CORE_READ_CATALOG":catalog,
        "QRP_LAKE_DAILY_SOURCE":"lake","QRP_LAKE_DAILY_SNAPSHOT":str(final.snapshot_id)},project_root=tmp_path/"repo")
    monkeypatch.setattr(consumer,"get_settings",lambda:settings)
    monkeypatch.setattr(daily,"get_db",lambda: (_ for _ in ()).throw(AssertionError("legacy connection used")))
    app = FastAPI()
    app.include_router(daily.router)
    response = TestClient(app).get("/api/daily",params={"ticker":"000001.SZ"})
    assert response.status_code == 200, response.text
    assert response.json()[0]["close"] == 10
