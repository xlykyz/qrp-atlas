"""
smoke.py - 最小可运行回测示例

使用手工构造的 DataFrame 跑通完整回测流程，不依赖真实数据库。

运行:
    python -m qrp_atlas.backtest.examples.smoke
"""

import pandas as pd

from qrp_atlas.backtest.engine import BacktestEngine
from qrp_atlas.backtest.models import (
    BacktestConfig,
    CostRule,
    EntryRule,
    ExitRule,
    PositionRule,
)


def build_price_df() -> pd.DataFrame:
    """构造 6 个交易日的股票行情（000001.SZ 平安银行）。"""
    rows = [
        ("2024-01-01", "000001.SZ", "平安银行", 10.0, 10.5, 9.8, 10.2),
        ("2024-01-02", "000001.SZ", "平安银行", 10.2, 10.8, 10.0, 10.5),
        ("2024-01-03", "000001.SZ", "平安银行", 10.5, 11.0, 10.3, 10.8),
        ("2024-01-04", "000001.SZ", "平安银行", 10.8, 11.0, 10.5, 10.6),
        ("2024-01-05", "000001.SZ", "平安银行", 10.6, 10.9, 10.4, 10.7),
        ("2024-01-06", "000001.SZ", "平安银行", 10.7, 11.2, 10.6, 11.0),
    ]
    return pd.DataFrame(
        rows,
        columns=["trade_date", "asset_id", "asset_name", "open", "high", "low", "close"],
    ).assign(asset_type="stock")


def build_signals_df() -> pd.DataFrame:
    """构造 1 条 long 信号。"""
    return pd.DataFrame(
        [
            {
                "signal_date": "2024-01-01",
                "asset_id": "000001.SZ",
                "direction": "long",
                "signal_name": "manual_test",
                "score": 1.0,
                "weight": 1.0,
            }
        ]
    )


def build_config() -> BacktestConfig:
    return BacktestConfig(
        name="manual_hold_5_bars",
        entry=EntryRule(timing="signal_close", price_field="close"),
        exit=ExitRule(type="hold_n_bars", bars=5, price_field="close"),
        position=PositionRule(
            initial_cash=1_000_000,
            position_pct=1.0,
            max_positions=999_999,
            allow_overlap=True,
            compound=False,
        ),
        cost=CostRule(
            commission_rate=0.00025,
            stamp_tax_rate=0.0005,
            slippage_bps=0,
        ),
    )


def main() -> None:
    price_df = build_price_df()
    signals_df = build_signals_df()
    config = build_config()

    engine = BacktestEngine()
    result = engine.run(price_df=price_df, signals_df=signals_df, config=config)

    print("=" * 60)
    print(f"config: {config.name}")
    print("-" * 60)
    print("summary:")
    for k, v in result.summary.items():
        print(f"  {k}: {v}")
    print("-" * 60)
    print(f"trades ({len(result.trades)}):")
    for t in result.trades:
        print(f"  {t}")
    print("-" * 60)
    print(f"skipped ({len(result.skipped)}):")
    for s in result.skipped:
        print(f"  {s}")
    print("=" * 60)


if __name__ == "__main__":
    main()
