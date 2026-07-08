"""/api/daily 复权口径测试。

通过临时 DuckDB + monkeypatch daily 模块内的 get_db + FastAPI TestClient 验证：
- raw / qfq / hfq 三种口径的 OHLC 调整公式
- volume/amount 在三种口径下保持一致
- 缺少 adj_factor_changes 表时 qfq 不报错并退化为原值
- 非法 adjustment 触发 FastAPI 422 校验错误
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from qrp_atlas.api.routes import daily as daily_route
from qrp_atlas.api.server import app
from tests.conftest import make_fake_get_db


TICKER = "000001.SZ"


@pytest.fixture
def client_with_adj(sample_db_path: Path, monkeypatch) -> TestClient:
    """指向带 adj_factor_changes 表的临时 DuckDB 的 TestClient。"""
    monkeypatch.setattr(
        daily_route, "get_db", make_fake_get_db(sample_db_path)
    )
    with TestClient(app) as c:
        yield c


@pytest.fixture
def client_without_adj(sample_db_path_without_adj: Path, monkeypatch) -> TestClient:
    """指向没有 adj_factor_changes 表的临时 DuckDB 的 TestClient。"""
    monkeypatch.setattr(
        daily_route, "get_db", make_fake_get_db(sample_db_path_without_adj)
    )
    with TestClient(app) as c:
        yield c


def _fetch(client: TestClient, adjustment: str, *, ticker: str = TICKER) -> list[dict]:
    resp = client.get(
        "/api/daily", params={"ticker": ticker, "adjustment": adjustment}
    )
    assert resp.status_code == 200, resp.text
    rows = resp.json()
    assert isinstance(rows, list)
    return rows


# ────────────────────────────────────────────────────────────
# 1. raw 模式
# ────────────────────────────────────────────────────────────
def test_raw_mode_preserves_prices(client_with_adj: TestClient):
    rows = _fetch(client_with_adj, "raw")
    assert len(rows) == 3
    assert [r["close"] for r in rows] == [10.0, 11.0, 12.0]
    assert [r["open"] for r in rows] == [10.0, 11.0, 12.0]
    assert [r["high"] for r in rows] == [11.0, 12.0, 13.0]
    assert [r["low"] for r in rows] == [9.0, 10.0, 11.0]
    assert [r["pre_close"] for r in rows] == [9.8, 10.0, 11.0]
    assert [r["volume"] for r in rows] == [1000, 1200, 1300]
    assert [r["amount"] for r in rows] == [10000.0, 12000.0, 13000.0]
    # adj_factor 字段在 raw 模式下也返回原始值
    assert [r["adj_factor"] for r in rows] == [1.0, 2.0, 4.0]


# ────────────────────────────────────────────────────────────
# 2. qfq 模式：每行 price * row.adj_factor / latest_adj_factor(=4)
# ────────────────────────────────────────────────────────────
def test_qfq_mode_uses_latest_factor(client_with_adj: TestClient):
    rows = _fetch(client_with_adj, "qfq")
    assert len(rows) == 3
    # 公式：price * row.adj_factor / 4
    # close: 10*1/4=2.5, 11*2/4=5.5, 12*4/4=12
    assert [r["close"] for r in rows] == [2.5, 5.5, 12.0]
    # open: 10*1/4=2.5, 11*2/4=5.5, 12*4/4=12
    assert [r["open"] for r in rows] == [2.5, 5.5, 12.0]
    # high: 11*1/4=2.75, 12*2/4=6.0, 13*4/4=13.0
    assert [r["high"] for r in rows] == [2.75, 6.0, 13.0]
    # low: 9*1/4=2.25, 10*2/4=5.0, 11*4/4=11.0
    assert [r["low"] for r in rows] == [2.25, 5.0, 11.0]
    # pre_close: 9.8*1/4=2.45, 10*2/4=5.0, 11*4/4=11.0
    assert [r["pre_close"] for r in rows] == [2.45, 5.0, 11.0]


# ────────────────────────────────────────────────────────────
# 3. hfq 模式：first_adj_factor = 1 作为分母
# ────────────────────────────────────────────────────────────
def test_hfq_mode_uses_first_factor(client_with_adj: TestClient):
    rows = _fetch(client_with_adj, "hfq")
    assert len(rows) == 3
    # close: 10*1/1=10, 11*2/1=22, 12*4/1=48
    assert [r["close"] for r in rows] == [10.0, 22.0, 48.0]
    # open: 10, 22, 48
    assert [r["open"] for r in rows] == [10.0, 22.0, 48.0]
    # high: 11, 24, 52
    assert [r["high"] for r in rows] == [11.0, 24.0, 52.0]
    # low: 9, 20, 44
    assert [r["low"] for r in rows] == [9.0, 20.0, 44.0]
    # pre_close: 9.8, 20, 44
    assert [r["pre_close"] for r in rows] == [9.8, 20.0, 44.0]


# ────────────────────────────────────────────────────────────
# 4. 成交量成交额不复权：raw/qfq/hfq 下应一致
# ────────────────────────────────────────────────────────────
def test_volume_amount_identical_across_adjustments(client_with_adj: TestClient):
    raw_rows = _fetch(client_with_adj, "raw")
    qfq_rows = _fetch(client_with_adj, "qfq")
    hfq_rows = _fetch(client_with_adj, "hfq")

    raw_vol = [r["volume"] for r in raw_rows]
    raw_amt = [r["amount"] for r in raw_rows]
    assert [r["volume"] for r in qfq_rows] == raw_vol
    assert [r["volume"] for r in hfq_rows] == raw_vol
    assert [r["amount"] for r in qfq_rows] == raw_amt
    assert [r["amount"] for r in hfq_rows] == raw_amt


# ────────────────────────────────────────────────────────────
# 5. 无 adj_factor_changes 表：qfq 不报错，价格不调整
# ────────────────────────────────────────────────────────────
def test_qfq_without_adj_factor_table_degrades_to_raw(
    client_without_adj: TestClient,
):
    rows = _fetch(client_without_adj, "qfq")
    assert len(rows) == 3
    # 价格保持原值，不调整
    assert [r["close"] for r in rows] == [10.0, 11.0, 12.0]
    assert [r["open"] for r in rows] == [10.0, 11.0, 12.0]
    # adj_factor 应为 None（SQL 中 SELECT NULL AS adj_factor）
    assert all(r["adj_factor"] is None for r in rows)


def test_hfq_without_adj_factor_table_degrades_to_raw(
    client_without_adj: TestClient,
):
    rows = _fetch(client_without_adj, "hfq")
    assert len(rows) == 3
    assert [r["close"] for r in rows] == [10.0, 11.0, 12.0]
    assert all(r["adj_factor"] is None for r in rows)


def test_raw_without_adj_factor_table_returns_none_adj_factor(
    client_without_adj: TestClient,
):
    rows = _fetch(client_without_adj, "raw")
    assert len(rows) == 3
    assert all(r["adj_factor"] is None for r in rows)


# ────────────────────────────────────────────────────────────
# 6. 非法 adjustment 触发 FastAPI 参数校验错误（HTTP 422）
# ────────────────────────────────────────────────────────────
def test_invalid_adjustment_returns_422(client_with_adj: TestClient):
    resp = client_with_adj.get(
        "/api/daily", params={"ticker": TICKER, "adjustment": "bad"}
    )
    assert resp.status_code == 422
