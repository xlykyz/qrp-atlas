"""Release-level acceptance checks spanning auth, results, compare, and replay."""

from __future__ import annotations

from uuid import UUID

from qrp_atlas.api.server import app
from qrp_atlas.auth.context import UserContext
from qrp_atlas.auth.dependencies import get_current_user
from qrp_atlas.backtest.product.service import (
    BacktestProductService,
    execute_validated_task,
    reset_product_service_for_tests,
)
from qrp_atlas.backtest.product.task_store import BacktestTaskStore
from qrp_atlas.backtest.results.loader import BacktestRunsLoader
from qrp_atlas.backtest.results.service import set_loader_for_tests
from tests.api.asgi_client import ASGITestClient
from tests.backtest.test_results_product_e2e_seal import (
    _classic_request,
    _make_classic_db,
)


def _user(user_id: str, username: str) -> UserContext:
    return UserContext(
        user_id=UUID(user_id),
        username=username,
        display_name=username,
    )


def test_multi_user_history_compare_and_replay_are_owner_isolated(tmp_path):
    user_a = _user("11111111-1111-4111-8111-111111111111", "owner-a")
    user_b = _user("22222222-2222-4222-8222-222222222222", "owner-b")
    current = {"user": user_a}
    db_path = _make_classic_db(tmp_path)
    runs_dir = tmp_path / "runs"
    tasks_dir = tmp_path / "tasks"

    execute_validated_task(
        _classic_request(),
        run_id="acceptance_owner_a",
        runs_dir=runs_dir,
        db_path=db_path,
        owner_user_id=str(user_a.user_id),
    )
    execute_validated_task(
        _classic_request(),
        run_id="acceptance_owner_b",
        runs_dir=runs_dir,
        db_path=db_path,
        owner_user_id=str(user_b.user_id),
    )

    service = BacktestProductService(
        task_store=BacktestTaskStore(tasks_dir),
        runs_dir=runs_dir,
        db_path=db_path,
        execute_inline=True,
    )
    reset_product_service_for_tests(service)
    set_loader_for_tests(BacktestRunsLoader(runs_dir))
    app.dependency_overrides[get_current_user] = lambda: current["user"]
    client = ASGITestClient(app)
    try:
        listed = client.get("/api/backtest/runs")
        assert listed.status_code == 200
        assert {row["run_id"] for row in listed.json()} == {"acceptance_owner_a"}

        history = client.get("/api/backtest/runs/acceptance_owner_a/config")
        assert history.status_code == 200
        assert history.json()["config"]["strategy_code"] == "dual_sma_trend"
        assert client.get("/api/backtest/runs/acceptance_owner_b/summary").status_code == 404

        replay = client.post(
            "/api/backtest/runs/acceptance_owner_a/replay",
            json={"new_run_id": "acceptance_owner_a_replay"},
        )
        assert replay.status_code == 200, replay.text
        assert replay.json()["match"]["all_business"] is True

        compared = client.post(
            "/api/backtest/compare",
            json={"run_ids": ["acceptance_owner_a", "acceptance_owner_a_replay"]},
        )
        assert compared.status_code == 200
        assert {row["run_id"] for row in compared.json()["runs"]} == {
            "acceptance_owner_a",
            "acceptance_owner_a_replay",
        }
        assert client.post(
            "/api/backtest/compare",
            json={"run_ids": ["acceptance_owner_a", "acceptance_owner_b"]},
        ).status_code == 404

        created = client.post(
            "/api/backtest/tasks",
            json=_classic_request().model_dump(mode="json"),
        )
        assert created.status_code == 200, created.text
        task_id = created.json()["task"]["task_id"]
        current["user"] = user_b
        assert client.get("/api/backtest/tasks").json() == []
        assert client.get(f"/api/backtest/tasks/{task_id}").status_code == 404
        assert client.get("/api/backtest/runs/acceptance_owner_a/summary").status_code == 404
    finally:
        app.dependency_overrides.pop(get_current_user, None)
        set_loader_for_tests(None)
        reset_product_service_for_tests(None)
