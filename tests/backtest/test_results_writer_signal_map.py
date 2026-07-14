from pathlib import Path

import pandas as pd

from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio import (
    PortfolioBacktestConfig,
    PortfolioBacktestEngine,
    PortfolioExecutionRule,
)
from qrp_atlas.backtest.results import BacktestRunsLoader, BacktestRunWriter, BacktestSummary, BacktestTrade


def test_writer_preserves_signal_date_and_merges_extra_skips(tmp_path: Path):
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
    # Engine executes on 2024-01-03, but signal was 2024-01-02.
    targets = pd.DataFrame(
        [
            {"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.5},
            {"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.0},
        ]
    )
    # Need two execution dates for buy then sell; keep simple buy-only open trade.
    targets = pd.DataFrame(
        [{"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.5}]
    )
    config = PortfolioBacktestConfig(
        name="signal_map_test",
        initial_cash=10_000.0,
        max_positions=1,
        max_weight_per_asset=1.0,
        cost=CostRule(commission_rate=0.0, stamp_tax_rate=0.0, slippage_bps=0),
        execution=PortfolioExecutionRule(minimum_commission=0.0),
    )
    result = PortfolioBacktestEngine().run(price_df, targets, config)
    run_dir = BacktestRunWriter(tmp_path).write_portfolio_run(
        result,
        run_id="signal_map_001",
        strategy_name="demo",
        universe="A",
        execution_signal_map={("2024-01-03", "A"): "2024-01-02"},
        extra_skipped=[
            {
                "asset_id": None,
                "signal_date": "2024-01-03",
                "reason": "NO_EXECUTION_DATE_IN_RANGE",
                "detail": "end of range",
            }
        ],
    )

    loader = BacktestRunsLoader(tmp_path)
    trades = [BacktestTrade.model_validate(t) for t in loader.load_trades("signal_map_001")]
    skipped = loader.load_skipped("signal_map_001")
    summary = BacktestSummary.model_validate(loader.load_summary("signal_map_001"))

    assert trades
    assert trades[0].signal_date == "2024-01-02"
    assert trades[0].entry_date == "2024-01-03"
    assert any(item["reason"] == "NO_EXECUTION_DATE_IN_RANGE" for item in skipped)
    assert summary.skipped_count >= 1
    assert (run_dir / "skipped.json").exists()
