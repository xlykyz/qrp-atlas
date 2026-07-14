"""07-D declarative strategy product store and product-path tests."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.backtest.product import (
    BacktestProductService,
    BacktestTaskStore,
    CreateBacktestTaskRequest,
    list_strategy_catalog,
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
