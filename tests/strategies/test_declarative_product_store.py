"""07-D declarative strategy product store and product-path tests."""

from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.backtest.product import (
    BacktestProductService,
    BacktestTaskStore,
    CreateBacktestTaskRequest,
    list_strategy_catalog,
    get_strategy_catalog_item,
    validate_create_request,
)
from qrp_atlas.backtest.product.schemas import (
    BacktestCostConfigDTO,
    BacktestExecutionConfigDTO,
    BacktestPositionConfigDTO,
)
from qrp_atlas.strategies.declarative.store import (
    DeclarativeStoreError,
    DeclarativeStrategyStore,
    deterministic_json,
    reset_declarative_store_for_tests,
    validate_declarative_payload,
)


def _definition(**overrides):
    payload = {
        "code": "demo_decl_trend",
        "name": "Demo Declarative",
        "version": "1.0.0",
        "description": "test",
        "strategy_type": "declarative",
        "required_fields": ["trade_date", "ticker", "close"],
        "required_indicators": [],
        "parameters": {
            "threshold": {
                "type": "number",
                "required": False,
                "default": 10.0,
            }
        },
        "entry": {
            "left": {"source_type": "field", "code": "close"},
            "operator": "gt",
            "right": {"source_type": "parameter", "code": "threshold"},
        },
        "exit": {
            "left": {"source_type": "field", "code": "close"},
            "operator": "lt",
            "right": {"source_type": "literal", "value": 9.0},
        },
    }
    payload.update(overrides)
    return payload


def _concurrent_create(root: str, queue) -> None:
    store = DeclarativeStrategyStore(Path(root))
    try:
        store.create(_definition(), owner_user_id="user-a")
        queue.put("created")
    except DeclarativeStoreError:
        queue.put("exists")


def test_rejects_illegal_operator_and_eval_tokens(tmp_path: Path):
    store = DeclarativeStrategyStore(tmp_path)
    with pytest.raises(DeclarativeStoreError):
        store.validate(
            _definition(
                entry={
                    "left": {"source_type": "field", "code": "close"},
                    "operator": "python",
                    "right": {"source_type": "literal", "value": 1},
                }
            )
        )
    with pytest.raises(DeclarativeStoreError):
        store.validate(_definition(description="evil eval(os.system('x'))"))


def test_validate_rejects_static_type_mismatches(tmp_path: Path):
    store = DeclarativeStrategyStore(tmp_path)
    with pytest.raises(DeclarativeStoreError, match="incompatible operand types"):
        store.validate(
            _definition(
                parameters={
                    "threshold": {"type": "string", "required": True},
                },
                entry={
                    "left": {"source_type": "parameter", "code": "threshold"},
                    "operator": "eq",
                    "right": {"source_type": "literal", "value": 1},
                },
            )
        )
    with pytest.raises(DeclarativeStoreError, match="does not support boolean"):
        store.validate(
            _definition(
                parameters={
                    "threshold": {"type": "boolean", "required": True},
                },
                entry={
                    "left": {"source_type": "parameter", "code": "threshold"},
                    "operator": "gt",
                    "right": {"source_type": "literal", "value": False},
                },
            )
        )


def test_version_immutability_and_owner_isolation(tmp_path: Path):
    store = DeclarativeStrategyStore(tmp_path)
    r1 = store.create(_definition(), owner_user_id="user-a")
    assert r1.version == "1.0.0"
    with pytest.raises(DeclarativeStoreError, match="already exists"):
        store.create(_definition(), owner_user_id="user-a")
    r2 = store.create_new_version(
        "demo_decl_trend",
        _definition(version="1.0.1", name="v2"),
        owner_user_id="user-a",
    )
    assert r2.version == "1.0.1"
    old = store.get("demo_decl_trend", "1.0.0", owner_user_id="user-a")
    assert old.name == "Demo Declarative"
    # owner isolation
    with pytest.raises(DeclarativeStoreError):
        store.get("demo_decl_trend", "1.0.0", owner_user_id="user-b")


def test_declarative_code_cannot_conflict_with_builtin(tmp_path: Path):
    store = DeclarativeStrategyStore(tmp_path)
    with pytest.raises(DeclarativeStoreError, match="conflicts with builtin"):
        store.create(
            _definition(code="dual_sma_trend"),
            owner_user_id="user-a",
        )


def test_semver_latest_uses_numeric_order(tmp_path: Path):
    store = DeclarativeStrategyStore(tmp_path)
    reset_declarative_store_for_tests(store)
    try:
        store.create(_definition(version="1.9.0"), owner_user_id="user-a")
        store.create(_definition(version="1.10.0"), owner_user_id="user-a")
        latest = get_strategy_catalog_item(
            "demo_decl_trend", owner_user_id="user-a"
        )
        assert latest.version == "1.10.0"
    finally:
        reset_declarative_store_for_tests(None)


def test_cross_process_create_is_locked_and_atomic(tmp_path: Path):
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(target=_concurrent_create, args=(str(tmp_path), queue))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    assert sorted(queue.get(timeout=2) for _ in processes) == ["created", "exists"]
    payload = json.loads(
        (tmp_path / "user-a" / "demo_decl_trend@1.0.0.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["definition"]["name"] == "Demo Declarative"
    assert not list((tmp_path / "user-a").glob("*.tmp"))


def test_deterministic_serialization():
    a = validate_declarative_payload(_definition()).to_dict()
    b = validate_declarative_payload(_definition()).to_dict()
    assert deterministic_json(a) == deterministic_json(b)


def test_no_eval_exec_in_declarative_source():
    root = Path("src/qrp_atlas/strategies/declarative")
    for path in root.rglob("*.py"):
        if path.name == "store.py":
            # store may mention banned tokens as denial list strings only
            continue
        text = path.read_text(encoding="utf-8")
        assert "eval(" not in text
        assert "exec(" not in text
    # evaluator/models must remain free of runtime eval/exec
    for name in ("evaluator.py", "models.py"):
        text = (root / name).read_text(encoding="utf-8")
        assert "eval(" not in text
        assert "exec(" not in text


def _price_db(tmp_path: Path) -> Path:
    db = tmp_path / "decl.duckdb"
    con = duckdb.connect(str(db))
    con.execute(
        """
        CREATE TABLE daily_market_snapshot (
            trade_date DATE, ticker VARCHAR, name VARCHAR,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, amount DOUBLE, turnover DOUBLE,
            market_cap DOUBLE, float_cap DOUBLE,
            is_st BOOLEAN, is_limit_up BOOLEAN, is_limit_down BOOLEAN
        )
        """
    )
    con.execute(
        "CREATE TABLE suspend_d (trade_date DATE, ticker VARCHAR, suspend_timing VARCHAR, suspend_type VARCHAR, created_at TIMESTAMP)"
    )
    dates = pd.bdate_range("2024-01-02", periods=30)
    rows = []
    for i, d in enumerate(dates):
        close = 8 + i * 0.3
        rows.append(
            (
                d.date().isoformat(),
                "600519.SH",
                "Moutai",
                close - 0.05,
                close + 0.1,
                close - 0.1,
                close,
                1000.0,
                1000.0 * close,
                0.01,
                1e10,
                5e9,
                False,
                False,
                False,
            )
        )
    con.executemany(
        "INSERT INTO daily_market_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    con.close()
    return db


def test_product_catalog_and_task_for_declarative(tmp_path: Path):
    store = DeclarativeStrategyStore(tmp_path / "decl")
    reset_declarative_store_for_tests(store)
    try:
        store.create(_definition(), owner_user_id="local-user")
        codes = {(i.code, i.version) for i in list_strategy_catalog(product_only=True)}
        assert ("demo_decl_trend", "1.0.0") in codes

        req = CreateBacktestTaskRequest(
            name="decl-run",
            strategy_code="demo_decl_trend",
            strategy_version="1.0.0",
            strategy_params={"threshold": 10.0},
            universe_mode="tickers",
            tickers=["600519.SH"],
            start_date="2024-01-10",
            end_date="2024-02-05",
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000, max_positions=1, max_weight_per_symbol=1.0
            ),
            cost=BacktestCostConfigDTO(),
            execution=BacktestExecutionConfigDTO(entry_timing="next_open"),
        )
        validate_create_request(req)
        service = BacktestProductService(
            task_store=BacktestTaskStore(tmp_path / "tasks"),
            runs_dir=tmp_path / "runs",
            db_path=_price_db(tmp_path),
            execute_inline=True,
        )
        task = service.create_task(req).task
        assert task.status == "succeeded", task.error_message
        # referenced lock
        rec = store.get("demo_decl_trend", "1.0.0", owner_user_id="local-user")
        assert rec.referenced_by_runs is True
        with pytest.raises(DeclarativeStoreError):
            store.create(_definition(), owner_user_id="local-user", allow_overwrite=True)
    finally:
        reset_declarative_store_for_tests(None)


def test_referenced_version_cannot_be_overwritten_even_with_flag(tmp_path: Path):
    store = DeclarativeStrategyStore(tmp_path)
    store.create(_definition(), owner_user_id="user-a")
    store.mark_referenced("demo_decl_trend", "1.0.0", owner_user_id="user-a")
    with pytest.raises(DeclarativeStoreError, match="strictly immutable"):
        store.create(
            _definition(name="mutated"),
            owner_user_id="user-a",
            allow_overwrite=True,
        )
    old = store.get("demo_decl_trend", "1.0.0", owner_user_id="user-a")
    assert old.name == "Demo Declarative"
    assert old.referenced_by_runs is True


def test_unreferenced_version_cannot_be_overwritten_with_legacy_flag(tmp_path: Path):
    store = DeclarativeStrategyStore(tmp_path)
    store.create(_definition(), owner_user_id="user-a")
    with pytest.raises(DeclarativeStoreError, match="already exists"):
        store.create(_definition(name="mutated"), owner_user_id="user-a")
    with pytest.raises(DeclarativeStoreError, match="strictly immutable"):
        store.create(
            _definition(name="mutated"),
            owner_user_id="user-a",
            allow_overwrite=True,
        )
    unchanged = store.get("demo_decl_trend", "1.0.0", owner_user_id="user-a")
    assert unchanged.name == "Demo Declarative"


def test_product_run_locks_definition_snapshot(tmp_path: Path):
    store = DeclarativeStrategyStore(tmp_path / "decl")
    reset_declarative_store_for_tests(store)
    try:
        store.create(_definition(), owner_user_id="local-user")
        req = CreateBacktestTaskRequest(
            name="decl-snapshot",
            strategy_code="demo_decl_trend",
            strategy_version="1.0.0",
            strategy_params={"threshold": 10.0},
            universe_mode="tickers",
            tickers=["600519.SH"],
            start_date="2024-01-10",
            end_date="2024-02-05",
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000, max_positions=1, max_weight_per_symbol=1.0
            ),
            cost=BacktestCostConfigDTO(),
            execution=BacktestExecutionConfigDTO(entry_timing="next_open"),
        )
        service = BacktestProductService(
            task_store=BacktestTaskStore(tmp_path / "tasks"),
            runs_dir=tmp_path / "runs",
            db_path=_price_db(tmp_path),
            execute_inline=True,
        )
        task = service.create_task(req).task
        assert task.status == "succeeded", task.error_message
        import json

        config = json.loads((tmp_path / "runs" / task.run_id / "config.json").read_text(encoding="utf-8"))
        assert config["strategy_code"] == "demo_decl_trend"
        assert config["strategy_version"] == "1.0.0"
        assert config["strategy_definition_snapshot"]["code"] == "demo_decl_trend"
        assert config["declarative_strategy_snapshot"]["definition"]["version"] == "1.0.0"
        # mutate current store definition via new version only; old run stays locked
        store.create_new_version(
            "demo_decl_trend",
            _definition(version="1.0.1", name="later"),
            owner_user_id="local-user",
        )
        config2 = json.loads((tmp_path / "runs" / task.run_id / "config.json").read_text(encoding="utf-8"))
        assert config2["strategy_definition_snapshot"]["name"] == "Demo Declarative"
        item = next(
            i
            for i in list_strategy_catalog(product_only=True)
            if i.code == "demo_decl_trend" and i.version == "1.0.0"
        )
        assert item.strategy_type == "declarative"
    finally:
        reset_declarative_store_for_tests(None)


def test_owner_isolates_catalog_tasks_and_run_results(tmp_path: Path):
    store = DeclarativeStrategyStore(tmp_path / "decl")
    reset_declarative_store_for_tests(store)
    try:
        store.create(_definition(name="owner-a"), owner_user_id="user-a")
        store.create(_definition(name="owner-b"), owner_user_id="user-b")
        assert [
            item.name
            for item in list_strategy_catalog(
                product_only=True, owner_user_id="user-a"
            )
            if item.code == "demo_decl_trend"
        ] == ["owner-a"]
        assert [
            item.name
            for item in list_strategy_catalog(
                product_only=True, owner_user_id="user-b"
            )
            if item.code == "demo_decl_trend"
        ] == ["owner-b"]

        service = BacktestProductService(
            task_store=BacktestTaskStore(tmp_path / "tasks"),
            runs_dir=tmp_path / "runs",
            db_path=_price_db(tmp_path),
            execute_inline=True,
        )
        response = service.create_task(
            CreateBacktestTaskRequest(
                name="owner-a-run",
                strategy_code="demo_decl_trend",
                strategy_version="1.0.0",
                strategy_params={"threshold": 10.0},
                tickers=["600519.SH"],
                start_date="2024-01-10",
                end_date="2024-02-05",
            ),
            owner_user_id="user-a",
        )
        task = response.task
        assert task.status == "succeeded", task.error_message
        assert service.list_tasks(owner_user_id="user-a")[0].task_id == task.task_id
        assert service.list_tasks(owner_user_id="user-b") == []
        with pytest.raises(KeyError):
            service.get_task(task.task_id, owner_user_id="user-b")
        run_meta = json.loads(
            (tmp_path / "runs" / task.run_id / "run_meta.json").read_text(
                encoding="utf-8"
            )
        )
        assert run_meta["owner_user_id"] == "user-a"
    finally:
        reset_declarative_store_for_tests(None)


def test_mark_referenced_failure_removes_committed_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    store = DeclarativeStrategyStore(tmp_path / "decl")
    reset_declarative_store_for_tests(store)
    try:
        store.create(_definition(), owner_user_id="local-user")

        def _fail_mark(*_args, **_kwargs):
            raise DeclarativeStoreError("lock write failed")

        monkeypatch.setattr(store, "mark_referenced", _fail_mark)
        service = BacktestProductService(
            task_store=BacktestTaskStore(tmp_path / "tasks"),
            runs_dir=tmp_path / "runs",
            db_path=_price_db(tmp_path),
            execute_inline=True,
        )
        task = service.create_task(
            CreateBacktestTaskRequest(
                strategy_code="demo_decl_trend",
                strategy_version="1.0.0",
                strategy_params={"threshold": 10.0},
                tickers=["600519.SH"],
                start_date="2024-01-10",
                end_date="2024-02-05",
            )
        ).task
        assert task.status == "failed"
        assert "failed to lock declarative strategy reference" in (task.error_message or "")
        assert not list((tmp_path / "runs").glob("run_*"))
    finally:
        reset_declarative_store_for_tests(None)


def test_rejects_duplicate_indicator_aliases():
    payload = _definition(
        required_indicators=["sma"],
        indicator_requests=[
            {"code": "sma", "parameters": {"window": 5}, "alias": "x"},
            {"code": "sma", "parameters": {"window": 10}, "alias": "x"},
        ],
    )
    with pytest.raises(DeclarativeStoreError):
        validate_declarative_payload(payload)


def test_indicator_request_accepts_strategy_parameter_binding():
    payload = _definition(
        parameters={
            "threshold": {"type": "number", "default": 10.0},
            "lookback": {"type": "integer", "default": 20},
        },
        indicator_requests=[
            {
                "code": "sma",
                "parameters": {"window": {"parameter": "lookback"}},
                "alias": "trend_sma",
            }
        ],
    )
    normalized = validate_declarative_payload(payload).to_dict()
    assert normalized["indicator_requests"][0]["parameters"]["window"] == {
        "parameter": "lookback"
    }
