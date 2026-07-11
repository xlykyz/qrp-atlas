"""/api/backtest/* 路由测试。

使用真实 mock fixtures（tests/fixtures/backtest_runs/sample_run_001/），
不 mock loader，验证端到端读取 JSON 的完整路径。
"""

from __future__ import annotations

import pytest

from qrp_atlas.api.server import app
from tests.api.asgi_client import ASGITestClient


SAMPLE_RUN = "sample_run_001"


@pytest.fixture
def client() -> ASGITestClient:
    return ASGITestClient(app)


# ────────────────────────────────────────────────────────────
# 1. runs 列表
# ────────────────────────────────────────────────────────────
def test_list_runs_returns_sample(client: ASGITestClient):
    resp = client.get("/api/backtest/runs")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    run_ids = [r["run_id"] for r in data]
    assert SAMPLE_RUN in run_ids


def test_list_runs_meta_fields(client: ASGITestClient):
    resp = client.get("/api/backtest/runs")
    sample = next(r for r in resp.json() if r["run_id"] == SAMPLE_RUN)
    for field in (
        "run_id",
        "name",
        "strategy_name",
        "universe",
        "start_date",
        "end_date",
        "created_at",
        "status",
    ):
        assert field in sample


# ────────────────────────────────────────────────────────────
# 2. 单 run 元信息
# ────────────────────────────────────────────────────────────
def test_get_run_meta(client: ASGITestClient):
    resp = client.get(f"/api/backtest/runs/{SAMPLE_RUN}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["run_id"] == SAMPLE_RUN
    assert data["strategy_name"] == "limit_up_trend_v01"
    assert data["universe"] == "A_SHARE"
    assert data["status"] == "success"


# ────────────────────────────────────────────────────────────
# 3. summary
# ────────────────────────────────────────────────────────────
def test_get_summary(client: ASGITestClient):
    resp = client.get(f"/api/backtest/runs/{SAMPLE_RUN}/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["run_id"] == SAMPLE_RUN
    assert data["trade_count"] == 128
    assert data["skipped_count"] == 14
    assert data["total_return_pct"] == 42.5
    assert data["max_drawdown_pct"] == -8.7
    assert data["win_rate_pct"] == 46.2
    assert data["profit_loss_ratio"] == 1.58
    assert data["avg_holding_days"] == 3.4
    for field in (
        "total_return_pct",
        "annual_return_pct",
        "max_drawdown_pct",
        "win_rate_pct",
        "profit_loss_ratio",
        "trade_count",
        "avg_holding_days",
        "max_trade_loss_pct",
        "max_trade_profit_pct",
        "skipped_count",
    ):
        assert field in data


# ────────────────────────────────────────────────────────────
# 4. equity
# ────────────────────────────────────────────────────────────
def test_get_equity(client: ASGITestClient):
    resp = client.get(f"/api/backtest/runs/{SAMPLE_RUN}/equity")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 20
    for point in data:
        assert "date" in point
        assert "equity" in point
        assert "drawdown_pct" in point
        assert isinstance(point["equity"], (int, float))
        assert isinstance(point["drawdown_pct"], (int, float))


# ────────────────────────────────────────────────────────────
# 5. trades
# ────────────────────────────────────────────────────────────
def test_get_trades(client: ASGITestClient):
    resp = client.get(f"/api/backtest/runs/{SAMPLE_RUN}/trades")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 10
    expected_fields = {
        "trade_id",
        "asset_id",
        "signal_date",
        "entry_date",
        "entry_price",
        "exit_date",
        "exit_price",
        "holding_days",
        "return_pct",
        "mae_pct",
        "mfe_pct",
        "exit_reason",
        "status",
    }
    for trade in data:
        assert expected_fields.issubset(trade.keys()), (
            f"missing fields: {expected_fields - set(trade.keys())}"
        )
    statuses = {t["status"] for t in data}
    assert "CLOSED" in statuses


# ────────────────────────────────────────────────────────────
# 6. skipped
# ────────────────────────────────────────────────────────────
def test_get_skipped(client: ASGITestClient):
    resp = client.get(f"/api/backtest/runs/{SAMPLE_RUN}/skipped")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 3
    for skip in data:
        assert "reason" in skip
        assert "asset_id" in skip
        assert "signal_date" in skip


# ────────────────────────────────────────────────────────────
# 7. config
# ────────────────────────────────────────────────────────────
def test_get_config(client: ASGITestClient):
    resp = client.get(f"/api/backtest/runs/{SAMPLE_RUN}/config")
    assert resp.status_code == 200
    data = resp.json()
    assert data["run_id"] == SAMPLE_RUN
    assert isinstance(data["config"], dict)
    assert "entry" in data["config"]
    assert "exit" in data["config"]
    assert "position" in data["config"]
    assert "cost" in data["config"]


# ────────────────────────────────────────────────────────────
# 8. 不存在 run_id 返回 404
# ────────────────────────────────────────────────────────────
def test_run_not_found_returns_404(client: ASGITestClient):
    resp = client.get("/api/backtest/runs/nonexistent_run_999")
    assert resp.status_code == 404


def test_summary_not_found_returns_404(client: ASGITestClient):
    resp = client.get("/api/backtest/runs/nonexistent_run_999/summary")
    assert resp.status_code == 404


def test_invalid_run_id_returns_422(client: ASGITestClient):
    """run_id 含路径分隔符应被白名单拒绝。"""
    resp = client.get("/api/backtest/runs/..%2F..%2Fetc")
    # FastAPI 路径参数不会解码 %2F，但即使能传进来也会被 _validate_run_id 拒绝
    # 422 或 404 都可接受，只要不是 200 即可
    assert resp.status_code in (404, 422)
