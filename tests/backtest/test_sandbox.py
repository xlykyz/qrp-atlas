"""``qrp_atlas.backtest.sandbox`` 结构测试。

这些测试不需要真实 ``quant.db``：行情由合成 PriceFrame 注入，只验证沙盒的
数据门面、策略 Adapter、隔离调度与“不落库”边界。真实行情验收必须在服务器
同步后补做。
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from qrp_atlas.api.server import app
from qrp_atlas.backtest import sandbox as sandbox_module
from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio.engine import PortfolioBacktestEngine
from qrp_atlas.backtest.portfolio.models import (
    PortfolioBacktestConfig,
    PortfolioExecutionRule,
)
from qrp_atlas.backtest.sandbox import (
    BENCHMARK_SUMMARY_KEYS,
    DEFAULT_TIMEOUT_SEC,
    SandboxMarketData,
    _engine_panel,
    execute_sandbox_code,
    recompute_benchmark,
    run_strategy,
)
from qrp_atlas.config.settings import get_settings
from tests.api.asgi_client import ASGITestClient

STOCK_A = "000001.SZ"
STOCK_B = "600519.SH"
INDEX = "000001.SH"
INDEX_B = "399001.SZ"
DAYS = 40


def _rows(
    asset_id: str,
    asset_name: str,
    asset_type: str,
    base: float,
    *,
    days: int = DAYS,
    amount_base: float = 100_000_000.0,
) -> list[dict[str, Any]]:
    """生成确定性上涨行情：每日 +0.5%，成交额每日递增。"""

    rows: list[dict[str, Any]] = []
    start = date(2024, 1, 2)
    for index in range(days):
        price = round(base * (1.0 + 0.005 * index), 4)
        rows.append(
            {
                "trade_date": (start + timedelta(days=index)).isoformat(),
                "asset_id": asset_id,
                "asset_name": asset_name,
                "asset_type": asset_type,
                "open": price,
                "high": round(price * 1.01, 4),
                "low": round(price * 0.99, 4),
                "close": price,
                "volume": 1_000_000.0,
                "amount": amount_base * (1.0 + 0.01 * index),
                "is_suspended": False,
            }
        )
    return rows


def _stocks(days: int = DAYS) -> pd.DataFrame:
    return pd.DataFrame(
        _rows(STOCK_A, "平安银行", "stock", 10.0, days=days)
        + _rows(STOCK_B, "贵州茅台", "stock", 20.0, days=days)
    )


def _indices(days: int = DAYS) -> pd.DataFrame:
    return pd.DataFrame(
        _rows(INDEX, "上证综指", "index", 3000.0, days=days, amount_base=0.0)
    )


def _indices_two(days: int = DAYS) -> pd.DataFrame:
    """两个基准：INDEX 每日 +0.5%，INDEX_B 持平，便于区分基准字段变化。"""

    flat = _rows(INDEX_B, "深证成指", "index", 10000.0, days=days, amount_base=0.0)
    for row in flat:
        row["close"] = 10000.0
    return pd.DataFrame(
        _rows(INDEX, "上证综指", "index", 3000.0, days=days, amount_base=0.0) + flat
    )


def _index_loader(indices: pd.DataFrame):
    """构造可按 code 过滤的指数加载器，模拟只读查库。"""

    def load(codes=None) -> pd.DataFrame:
        if not codes:
            return indices
        wanted = {str(code) for code in codes}
        return indices[indices["asset_id"].isin(wanted)].reset_index(drop=True)

    return load


def _fake_load_index_prices(
    *,
    con=None,
    db_path=None,
    codes=None,
    start_date=None,
    end_date=None,
    limit=None,
) -> pd.DataFrame:
    frame = _indices_two()
    if codes:
        wanted = {str(code) for code in codes}
        frame = frame[frame["asset_id"].isin(wanted)]
    return frame.reset_index(drop=True)


def _loader(stocks: pd.DataFrame | None = None, indices: pd.DataFrame | None = None):
    stocks_frame = _stocks() if stocks is None else stocks
    indices_frame = _indices() if indices is None else indices

    def load() -> tuple[pd.DataFrame, pd.DataFrame]:
        return stocks_frame, indices_frame

    return load


def _run(code: str, *, stocks=None, indices=None, benchmark_id: str | None = None):
    notes: list[str] = []
    outcome = run_strategy(
        code=code,
        start_date="2024-01-02",
        end_date="2024-02-28",
        initial_cash=1_000_000.0,
        benchmark_id=benchmark_id,
        price_loader=_loader(stocks, indices),
        notes=notes,
    )
    outcome["notes"] = notes
    return outcome


HOLD_A = """
def initialize(context):
    print("沙盒初始化完成")

def handle_bar(context, market_data):
    return {"%s": 1.0}
""" % STOCK_A


# ────────────────────────────────────────────────────────────
# 1. 行情门面
# ────────────────────────────────────────────────────────────
def test_money_alias_maps_to_amount():
    market_data = SandboxMarketData(_stocks())
    last = market_data.trading_dates[-1]
    market_data.current_date = last

    history = market_data.get_history([STOCK_A], field="money", bars=5)

    assert list(history.columns) == [STOCK_A]
    assert len(history) == 5
    expected = _stocks().loc[
        (_stocks()["asset_id"] == STOCK_A)
        & (_stocks()["trade_date"] == last.strftime("%Y-%m-%d")),
        "amount",
    ].iloc[0]
    assert history[STOCK_A].iloc[-1] == pytest.approx(expected)


def test_get_history_never_exposes_future_bars():
    market_data = SandboxMarketData(_stocks())
    dates = market_data.trading_dates
    market_data.current_date = dates[9]

    history = market_data.get_history([STOCK_A, STOCK_B], field="close", bars=100)

    assert len(history) == 10
    assert history.index[-1] == dates[9]


def test_unknown_field_is_rejected():
    market_data = SandboxMarketData(_stocks())
    market_data.current_date = market_data.trading_dates[-1]

    with pytest.raises(ValueError, match="不支持字段"):
        market_data.get_history([STOCK_A], field="not_a_field", bars=5)


def test_active_universe_excludes_indices_and_returns_stocks():
    market_data = SandboxMarketData(
        pd.concat([_stocks(), _indices()], ignore_index=True)
    )

    universe = market_data.get_active_universe(market_data.trading_dates[0])

    assert sorted(universe) == sorted([STOCK_A, STOCK_B])
    assert INDEX not in universe


def test_combined_panel_makes_index_tradable():
    outcome = _run(
        """
def handle_bar(context, market_data):
    print("index bars:", len(market_data.get_history(["%s"], bars=10)))
    return {"%s": 1.0}
""" % (INDEX, INDEX),
        benchmark_id=INDEX,
    )

    summary = outcome["summary"]
    assert summary["skipped_count"] == 0
    assert summary["final_equity"] > 1_000_000.0
    assert summary["benchmark_id"] == INDEX


# ────────────────────────────────────────────────────────────
# 2. 策略 Adapter
# ────────────────────────────────────────────────────────────
def test_strategy_runs_on_real_prices_and_returns_summary():
    outcome = _run(HOLD_A)
    summary = outcome["summary"]

    assert len(outcome["equity_points"]) == DAYS
    assert summary["final_equity"] > 1_000_000.0
    assert isinstance(summary["total_return_pct"], float)
    assert summary["run_id"] == "sandbox_unpersisted"


def test_zero_weight_snapshot_liquidates_holding():
    outcome = _run(
        """
def handle_bar(context, market_data):
    if len(market_data.get_history(["%s"], bars=100)) < 5:
        return {"%s": 1.0}
    return {}
""" % (STOCK_A, STOCK_A)
    )

    summary = outcome["summary"]
    points = outcome["equity_points"]
    assert summary["trade_count"] >= 1
    assert points[-1]["equity"] == pytest.approx(points[-2]["equity"])


def test_unknown_asset_is_reported_not_faked():
    outcome = _run(
        """
def handle_bar(context, market_data):
    return {"999999.SZ": 1.0}
"""
    )

    summary = outcome["summary"]
    assert summary["skipped_count"] >= 1
    assert summary["final_equity"] == pytest.approx(1_000_000.0)
    assert any("缺行情" in line for line in outcome["notes"])


def test_invalid_handle_bar_return_type_raises():
    with pytest.raises(TypeError, match="必须返回"):
        _run(
            """
def handle_bar(context, market_data):
    return ["%s"]
""" % STOCK_A
        )


def test_missing_handle_bar_raises():
    with pytest.raises(ValueError, match="handle_bar"):
        _run("def initialize(context):\n    pass\n")


def test_engine_panel_trim_preserves_engine_results():
    """裁剪到策略引用过的标的后，引擎结算结果必须与全量面板完全一致。"""

    filler = pd.DataFrame(
        [
            row
            for index in range(20)
            for row in _rows(f"{900000 + index}.SZ", f"填充{index}", "stock", 5.0)
        ]
    )
    wide = pd.concat([_stocks(), filler], ignore_index=True)
    dates = pd.DatetimeIndex(sorted(wide["trade_date"].unique()))
    targets = pd.DataFrame(
        [
            {"trade_date": day.strftime("%Y-%m-%d"), "asset_id": STOCK_A, "target_weight": 1.0}
            for day in dates
        ]
    )
    config = PortfolioBacktestConfig(
        name="sandbox_run",
        initial_cash=1_000_000.0,
        max_positions=100,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0003, stamp_tax_rate=0.001, slippage_bps=2.0),
        execution=PortfolioExecutionRule(),
    )
    engine = PortfolioBacktestEngine()
    full = engine.run(price_df=wide, target_weights_df=targets, config=config)

    trimmed_panel = _engine_panel(wide, {STOCK_A}, dates, log=[])
    assert len(trimmed_panel) < len(wide)

    trimmed = engine.run(
        price_df=trimmed_panel, target_weights_df=targets, config=config
    )
    assert trimmed.summary == full.summary
    assert trimmed.equity_curve == full.equity_curve


# ────────────────────────────────────────────────────────────
# 3. 隔离调度（不需要 quant.db）
# ────────────────────────────────────────────────────────────
def test_worker_captures_print_and_traceback():
    outcome = execute_sandbox_code(
        {
            "code": "print('Hello QRP Sandbox')\n",
            "start_date": "2024-01-02",
            "end_date": "2024-02-28",
            "initial_cash": 1_000_000.0,
            "benchmark_id": None,
        },
        timeout_sec=30,
    )

    assert outcome["success"] is False
    assert "Hello QRP Sandbox" in outcome["logs"]
    assert "handle_bar" in (outcome["error_message"] or "")
    assert outcome["summary"] is None


def test_worker_reports_syntax_error():
    outcome = execute_sandbox_code(
        {
            "code": "def handle_bar(context, market_data)\n    return {}\n",
            "start_date": "2024-01-02",
            "end_date": "2024-02-28",
            "initial_cash": 1_000_000.0,
            "benchmark_id": None,
        },
        timeout_sec=30,
    )

    assert outcome["success"] is False
    assert "SyntaxError" in (outcome["error_message"] or "")


def test_worker_kills_infinite_loop():
    outcome = execute_sandbox_code(
        {
            "code": "while True:\n    pass\n",
            "start_date": "2024-01-02",
            "end_date": "2024-02-28",
            "initial_cash": 1_000_000.0,
            "benchmark_id": None,
        },
        timeout_sec=3,
    )

    assert outcome["success"] is False
    assert "TimeoutError" in (outcome["error_message"] or "")
    assert any("沙盒超时" in line for line in outcome["logs"])
    assert outcome["duration_ms"] == 3_000


def test_http_contract_shape_is_unchanged_for_errors():
    client = ASGITestClient(app)
    response = client.post(
        "/api/custom-strategies/sandbox-run",
        json={
            "code": "def handle_bar(context, market_data)\n    return {}\n",
            "start_date": "2024-01-02",
            "end_date": "2024-02-28",
            "initial_cash": 1_000_000.0,
            "benchmark_id": "000985.XSHG",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is False
    assert set(body) == {
        "success",
        "summary",
        "equity_points",
        "series",
        "logs",
        "error_message",
        "duration_ms",
    }


# ────────────────────────────────────────────────────────────
# 4. 不产生持久化
# ────────────────────────────────────────────────────────────
def _listing(root: Path) -> list[str]:
    if not root.exists():
        return []
    return sorted(str(path.relative_to(root)) for path in root.rglob("*"))


def test_sandbox_run_creates_no_persisted_run():
    runs_dir = get_settings().paths.backtest_runs_dir
    before = _listing(runs_dir)

    _run(HOLD_A)

    assert _listing(runs_dir) == before


def test_worker_process_is_daemonic():
    """沙盒子进程必须是 daemon，API 退出时才不会残留孤儿进程。"""

    outcome = execute_sandbox_code(
        {
            "code": (
                "import multiprocessing\n"
                "print('daemon=', multiprocessing.current_process().daemon)\n"
            ),
            "start_date": "2024-01-02",
            "end_date": "2024-02-28",
            "initial_cash": 1_000_000.0,
            "benchmark_id": None,
        },
        timeout_sec=30,
    )

    assert outcome["success"] is False
    assert any("daemon= True" in line for line in outcome["logs"])


def test_timeout_default_is_thirty_minutes():
    assert DEFAULT_TIMEOUT_SEC == 1800


# ────────────────────────────────────────────────────────────
# 5. 基准切换：只重算后处理，不重跑策略
# ────────────────────────────────────────────────────────────
def test_recompute_benchmark_matches_full_run():
    """重算出的 9 个基准字段必须与整跑策略时完全一致。"""

    outcome = _run(HOLD_A, benchmark_id=INDEX)
    summary = outcome["summary"]

    recomputed = recompute_benchmark(
        equity_points=outcome["equity_points"],
        benchmark_id=INDEX,
        index_loader=_index_loader(_indices_two()),
    )

    assert recomputed["benchmark_id"] == INDEX
    assert recomputed["logs"] == []
    for key in BENCHMARK_SUMMARY_KEYS:
        assert recomputed[key] == summary[key]
    assert recomputed["series"] == outcome["series"]


def test_benchmark_series_aligns_dates_and_scales_to_pct():
    """series 与 equity_points 日期一一对应，且为 ×100 的百分数口径。"""

    outcome = _run(HOLD_A, benchmark_id=INDEX)
    recomputed = recompute_benchmark(
        equity_points=outcome["equity_points"],
        benchmark_id=INDEX,
        index_loader=_index_loader(_indices_two()),
    )

    series = recomputed["series"]
    assert [row["date"] for row in series] == [
        point["date"] for point in outcome["equity_points"]
    ]
    assert series[0] == {
        "date": outcome["equity_points"][0]["date"],
        "benchmark_cumulative_return_pct": 0.0,
        "portfolio_cumulative_return_pct": 0.0,
        "excess_percentage_point_pct": 0.0,
    }
    assert series[-1]["benchmark_cumulative_return_pct"] == pytest.approx(
        recomputed["benchmark_total_return_pct"]
    )


def test_benchmark_series_keeps_gaps_null():
    """基准缺口日期对应字段如实为 null，不跨缺口填充。"""

    indices = _indices()
    gap_date = str(indices["trade_date"].unique()[10])
    gapped = indices[indices["trade_date"] != gap_date].reset_index(drop=True)

    outcome = _run(HOLD_A)  # 先取无基准的净值曲线
    recomputed = recompute_benchmark(
        equity_points=outcome["equity_points"],
        benchmark_id=INDEX,
        index_loader=_index_loader(gapped),
    )

    by_date = {row["date"]: row for row in recomputed["series"]}
    assert by_date[gap_date]["benchmark_cumulative_return_pct"] is None
    assert by_date[gap_date]["excess_percentage_point_pct"] is None
    # 组合累计收益与基准无关，缺口日仍如实给出
    assert by_date[gap_date]["portfolio_cumulative_return_pct"] is not None


def test_run_series_is_empty_without_benchmark():
    outcome = _run(HOLD_A)
    assert outcome["series"] == []


def test_recompute_benchmark_missing_index_lists_available():
    outcome = _run(HOLD_A, benchmark_id=INDEX)

    recomputed = recompute_benchmark(
        equity_points=outcome["equity_points"],
        benchmark_id="999999.SH",
        index_loader=_index_loader(_indices_two()),
    )

    assert recomputed["benchmark_id"] == "999999.SH"
    assert all(recomputed[key] is None for key in BENCHMARK_SUMMARY_KEYS)
    joined = " ".join(recomputed["logs"])
    assert "不存在" in joined
    assert INDEX in joined and INDEX_B in joined


def test_recompute_benchmark_changes_only_benchmark_fields():
    """换基准只改基准/超额字段，组合收益口径与净值曲线不受影响。"""

    outcome = _run(HOLD_A)  # 无基准
    assert outcome["summary"]["benchmark_total_return_pct"] is None
    points = outcome["equity_points"]

    first = recompute_benchmark(
        equity_points=points, benchmark_id=INDEX, index_loader=_index_loader(_indices_two())
    )
    second = recompute_benchmark(
        equity_points=points, benchmark_id=INDEX_B, index_loader=_index_loader(_indices_two())
    )

    assert set(first) == {"benchmark_id", *BENCHMARK_SUMMARY_KEYS, "series", "logs"}
    assert first["portfolio_total_return_pct"] == second["portfolio_total_return_pct"]
    assert first["benchmark_total_return_pct"] != second["benchmark_total_return_pct"]
    assert outcome["equity_points"] == points


def test_recompute_benchmark_empty_points_raises():
    with pytest.raises(ValueError, match="不能为空"):
        recompute_benchmark(
            equity_points=[],
            benchmark_id=INDEX,
            index_loader=_index_loader(_indices()),
        )


def test_recompute_benchmark_creates_no_persisted_run():
    runs_dir = get_settings().paths.backtest_runs_dir
    before = _listing(runs_dir)

    outcome = _run(HOLD_A)
    recompute_benchmark(
        equity_points=outcome["equity_points"],
        benchmark_id=INDEX,
        index_loader=_index_loader(_indices()),
    )

    assert _listing(runs_dir) == before


def test_http_sandbox_benchmark_recomputes(monkeypatch):
    monkeypatch.setattr(sandbox_module, "load_index_prices", _fake_load_index_prices)
    outcome = _run(HOLD_A, benchmark_id=INDEX)

    client = ASGITestClient(app)
    response = client.post(
        "/api/custom-strategies/sandbox-benchmark",
        json={"equity_points": outcome["equity_points"], "benchmark_id": INDEX},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["benchmark_id"] == INDEX
    for key in BENCHMARK_SUMMARY_KEYS:
        assert body[key] == outcome["summary"][key]
    assert body["series"] == outcome["series"]


def test_http_sandbox_benchmark_missing_index_returns_nulls(monkeypatch):
    monkeypatch.setattr(sandbox_module, "load_index_prices", _fake_load_index_prices)
    outcome = _run(HOLD_A)

    client = ASGITestClient(app)
    response = client.post(
        "/api/custom-strategies/sandbox-benchmark",
        json={"equity_points": outcome["equity_points"], "benchmark_id": "999999.SH"},
    )

    assert response.status_code == 200
    body = response.json()
    assert all(body[key] is None for key in BENCHMARK_SUMMARY_KEYS)
    assert body["series"] == []
    assert "不存在" in " ".join(body["logs"])


def test_http_sandbox_benchmark_rejects_empty_points():
    client = ASGITestClient(app)
    response = client.post(
        "/api/custom-strategies/sandbox-benchmark",
        json={"equity_points": [], "benchmark_id": INDEX},
    )

    assert response.status_code == 400
    assert "不能为空" in response.json()["detail"]
