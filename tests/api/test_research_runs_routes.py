"""/api/research/* 研究区路由测试。

覆盖验收标准：
1. 列出 run，且不含 latest 软链造成的重复项（R4）
2. 运行详情各数据区块全部可加载，无 404（R5/R6）
3. 对比页可返回对比结果（R5）
4. 新增 run 无需重启即可见（R8：loader 每次 iterdir）
5. 归属校验放行（R3 方案 A：不做 owner 过滤）
6. 研究区目录只读（R1：不提供写接口）
7. 错误语义与空目录（R7）
"""

from __future__ import annotations

import json
import shutil

import pytest

from qrp_atlas.api.server import app
from qrp_atlas.backtest.results.loader import BacktestRunsLoader
from qrp_atlas.backtest.results.service import set_research_loader_for_tests
from qrp_atlas.config.paths import BACKTEST_FIXTURE_RUNS_DIR
from tests.api.asgi_client import ASGITestClient

FULL_RUN = "run_full_001"
NO_META_RUN = "run_no_meta"
OTHER_OWNER = "11111111-2222-3333-4444-555555555555"


def _write(path, payload) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _build_run_dir(root, run_id: str, *, owner: str | None = None) -> None:
    """基于 fixture 造一个完整 run 目录（含 17 个契约文件）。"""
    src = BACKTEST_FIXTURE_RUNS_DIR / "sample_run_001"
    dst = root / run_id
    shutil.copytree(src, dst)

    meta = json.loads((dst / "run_meta.json").read_text(encoding="utf-8"))
    meta["run_id"] = run_id
    if owner is not None:
        meta["owner_user_id"] = owner
    _write(dst / "run_meta.json", meta)

    # summary.json 内也带 run_id，需同步改，否则返回旧值
    summary = json.loads((dst / "summary.json").read_text(encoding="utf-8"))
    summary["run_id"] = run_id
    _write(dst / "summary.json", summary)

    # 补齐 17 文件契约中 fixture 未覆盖的产物
    _write(dst / "costs.json", {"commission": 1.0, "total_cost": 2.0})
    _write(dst / "diagnostics.json", {"result_package_version": "1.1", "trade_count": 3})
    _write(dst / "orders.json", [{"order_id": "O1"}])
    _write(dst / "fills.json", [{"fill_id": "F1"}])
    _write(dst / "snapshots.json", [{"trade_date": "2024-01-01"}])
    _write(dst / "daily_returns.json", [{"date": "2024-01-01", "daily_return": 0.01}])
    _write(dst / "rolling_performance.json", [{"date": "2024-01-01", "return_w20": 0.02}])
    _write(dst / "benchmark.json", {"benchmark_id": "000001.SH", "points": [], "summary": {}})
    _write(dst / "exposures.json", {"available": False, "reason": "test"})
    _write(dst / "reproducibility.json", {"snapshot_hash": "abc", "locked_to_run_snapshot": True})


@pytest.fixture
def research_root(tmp_path):
    """造一个研究区根：完整 run + 缺 meta 的 run + latest 软链 + 干扰文件。"""
    root = tmp_path / "results"
    root.mkdir()
    _build_run_dir(root, FULL_RUN, owner=OTHER_OWNER)
    # 缺 run_meta.json 的 run：列表应跳过，不报错（R7）
    (root / NO_META_RUN).mkdir()
    _write(root / NO_META_RUN / "summary.json", {"run_id": NO_META_RUN})
    # latest 软链：指向 FULL_RUN，不应被当成独立 run（R4）
    (root / "latest").symlink_to(FULL_RUN)
    # 非目录文件：应被忽略
    (root / ".gitkeep").write_text("")
    return root


@pytest.fixture
def client(research_root):
    set_research_loader_for_tests(BacktestRunsLoader(root=research_root))
    try:
        yield ASGITestClient(app)
    finally:
        set_research_loader_for_tests(None)


# ────────────────────────────────────────────────────────────
# 1. runs 列表 + R4（latest 不重复）
# ────────────────────────────────────────────────────────────
def test_list_runs_excludes_symlink_and_missing_meta(client):
    resp = client.get("/api/research/runs")
    assert resp.status_code == 200
    ids = [r["run_id"] for r in resp.json()]
    assert FULL_RUN in ids
    assert "latest" not in ids, "latest 软链不应作为独立 run 出现"
    assert NO_META_RUN not in ids, "缺 run_meta.json 的 run 应被跳过"
    assert len(ids) == len(set(ids)), "run_id 不应重复"


def test_list_runs_meta_fields(client):
    resp = client.get("/api/research/runs")
    sample = next(r for r in resp.json() if r["run_id"] == FULL_RUN)
    for field in (
        "run_id", "name", "strategy_name", "universe",
        "start_date", "end_date", "created_at", "status",
    ):
        assert field in sample


# ────────────────────────────────────────────────────────────
# 2. R3 方案 A：不做 owner 过滤（run 属主是别的 UUID 也可读）
# ────────────────────────────────────────────────────────────
def test_no_owner_filtering(client):
    resp = client.get(f"/api/research/runs/{FULL_RUN}")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == FULL_RUN


def test_list_includes_other_owner_run(client):
    resp = client.get("/api/research/runs")
    assert FULL_RUN in [r["run_id"] for r in resp.json()]


# ────────────────────────────────────────────────────────────
# 3. R5 全量产物端点
# ────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "suffix",
    [
        "", "/summary", "/equity", "/trades", "/skipped", "/config",
        "/costs", "/diagnostics", "/orders", "/fills", "/snapshots",
        "/daily-returns", "/rolling", "/benchmark", "/exposures", "/reproducibility",
    ],
)
def test_all_artifact_endpoints_ok(client, suffix):
    resp = client.get(f"/api/research/runs/{FULL_RUN}{suffix}")
    assert resp.status_code == 200, f"{suffix} -> {resp.status_code}"


def test_summary_payload(client):
    data = client.get(f"/api/research/runs/{FULL_RUN}/summary").json()
    assert data["run_id"] == FULL_RUN
    assert data["trade_count"] == 128


def test_equity_payload(client):
    data = client.get(f"/api/research/runs/{FULL_RUN}/equity").json()
    assert isinstance(data, list) and data
    for p in data:
        assert {"date", "equity", "drawdown_pct"} <= set(p)


def test_config_payload(client):
    data = client.get(f"/api/research/runs/{FULL_RUN}/config").json()
    assert data["run_id"] == FULL_RUN
    assert isinstance(data["config"], dict)


# ────────────────────────────────────────────────────────────
# 4. compare
# ────────────────────────────────────────────────────────────
def test_compare_returns_both_runs(client):
    resp = client.get(f"/api/research/compare?run_ids={FULL_RUN}&run_ids={FULL_RUN}")
    assert resp.status_code == 200
    body = resp.json()
    assert {"runs", "summaries", "configs", "missing"} <= set(body)
    assert len(body["runs"]) == 2
    assert body["missing"] == []


def test_compare_missing_run_reported(client):
    body = client.get(f"/api/research/compare?run_ids={FULL_RUN}&run_ids=nope").json()
    assert body["missing"] == ["nope"]
    assert len(body["runs"]) == 1


def test_compare_requires_run_ids(client):
    assert client.get("/api/research/compare").status_code == 400


def test_compare_rejects_more_than_10(client):
    q = "&".join(f"run_ids=r{i}" for i in range(11))
    assert client.get(f"/api/research/compare?{q}").status_code == 400


# ────────────────────────────────────────────────────────────
# 5. R7 错误语义
# ────────────────────────────────────────────────────────────
def test_run_not_found_detail(client):
    resp = client.get("/api/research/runs/nonexistent_run_999")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "backtest run not found: nonexistent_run_999"


def test_result_file_missing_detail(client):
    """缺 run_meta.json 的 run：直接取 meta 应 404 且 detail 带文件名。"""
    resp = client.get(f"/api/research/runs/{NO_META_RUN}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "result file missing: run_meta.json"


def test_symlink_run_id_not_addressable(client):
    """latest 是软链，不应能当 run_id 访问。"""
    resp = client.get("/api/research/runs/latest")
    assert resp.status_code == 404


# ────────────────────────────────────────────────────────────
# 6. R7 空/缺失根目录 → 200 []
# ────────────────────────────────────────────────────────────
def test_missing_root_returns_empty(tmp_path):
    set_research_loader_for_tests(BacktestRunsLoader(root=tmp_path / "does_not_exist"))
    try:
        resp = ASGITestClient(app).get("/api/research/runs")
        assert resp.status_code == 200
        assert resp.json() == []
    finally:
        set_research_loader_for_tests(None)


def test_empty_root_returns_empty(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    set_research_loader_for_tests(BacktestRunsLoader(root=empty))
    try:
        resp = ASGITestClient(app).get("/api/research/runs")
        assert resp.status_code == 200
        assert resp.json() == []
    finally:
        set_research_loader_for_tests(None)


# ────────────────────────────────────────────────────────────
# 7. R8 新 run 即时可见（无缓存）
# ────────────────────────────────────────────────────────────
def test_new_run_visible_without_restart(client, research_root):
    before = client.get("/api/research/runs").json()
    _build_run_dir(research_root, "run_new_002")
    after = client.get("/api/research/runs").json()
    assert "run_new_002" in [r["run_id"] for r in after]
    assert len(after) == len(before) + 1


# ────────────────────────────────────────────────────────────
# 8. R1 只读：不存在写接口
# ────────────────────────────────────────────────────────────
def test_no_write_endpoints(client):
    for method in ("put", "delete", "patch"):
        resp = client.request(method, "/api/research/runs")
        assert resp.status_code in (404, 405)