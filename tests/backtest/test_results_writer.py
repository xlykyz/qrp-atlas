from pathlib import Path

import pandas as pd
import pytest

from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import (
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioExecutionRule,
)
from qrp_atlas.backtest.results import (
    BacktestRunMeta,
    BacktestRunsLoader,
    BacktestSummary,
    BacktestTrade,
    BacktestRunWriter,
    EquityPoint,
)


def _portfolio_result():
    price_df = pd.DataFrame(
        [
            ("2024-01-02", "A", "A股", "stock", 10, 10, 10, 10),
            ("2024-01-03", "A", "A股", "stock", 12, 12, 12, 12),
        ],
        columns=[
            "trade_date",
            "asset_id",
            "asset_name",
            "asset_type",
            "open",
            "high",
            "low",
            "close",
        ],
    )
    targets = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "target_weight": 0.5},
            {"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.0},
        ]
    )
    config = PortfolioBacktestConfig(
        name="writer_test",
        initial_cash=10_000.0,
        max_positions=1,
        max_weight_per_asset=1.0,
        cost=CostRule(
            commission_rate=0.00025,
            stamp_tax_rate=0.0005,
            slippage_bps=0,
        ),
        execution=PortfolioExecutionRule(minimum_commission=5.0),
    )
    return PortfolioBacktestEngine().run(price_df, targets, config)


def test_writer_creates_existing_api_contract_and_audit_files(tmp_path: Path):
    result = _portfolio_result()
    run_dir = BacktestRunWriter(tmp_path).write_portfolio_run(
        result,
        run_id="portfolio_001",
        strategy_name="target_weight_test",
        universe="fixture",
        created_at="2026-07-13T00:00:00+00:00",
    )

    assert {path.name for path in run_dir.iterdir()} == {
        "run_meta.json",
        "summary.json",
        "equity.json",
        "trades.json",
        "skipped.json",
        "config.json",
        "orders.json",
        "fills.json",
        "snapshots.json",
    }

    loader = BacktestRunsLoader(tmp_path)
    meta = BacktestRunMeta.model_validate(loader.load_run_meta("portfolio_001"))
    summary = BacktestSummary.model_validate(loader.load_summary("portfolio_001"))
    equity = [
        EquityPoint.model_validate(point)
        for point in loader.load_equity("portfolio_001")
    ]
    trades = [
        BacktestTrade.model_validate(trade)
        for trade in loader.load_trades("portfolio_001")
    ]

    assert meta.run_id == "portfolio_001"
    assert meta.start_date == "2024-01-02"
    assert meta.end_date == "2024-01-03"
    assert summary.trade_count == 1
    assert summary.total_return_pct == pytest.approx(9.87)
    assert summary.max_trade_loss_pct is None
    assert summary.max_trade_profit_pct > 0
    assert len(equity) == 2
    assert equity[-1].equity == pytest.approx(1.0987)
    assert len(trades) == 1
    assert trades[0].status == "closed"
    assert trades[0].return_pct > 0


def test_writer_rejects_invalid_or_existing_run_id(tmp_path: Path):
    result = _portfolio_result()
    writer = BacktestRunWriter(tmp_path)

    with pytest.raises(ValueError, match="invalid run_id"):
        writer.write_portfolio_run(
            result,
            run_id="../invalid",
            strategy_name="test",
            universe="fixture",
        )

    writer.write_portfolio_run(
        result,
        run_id="existing",
        strategy_name="test",
        universe="fixture",
    )
    with pytest.raises(FileExistsError, match="already exists"):
        writer.write_portfolio_run(
            result,
            run_id="existing",
            strategy_name="test",
            universe="fixture",
        )
