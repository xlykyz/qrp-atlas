"""Persist portfolio backtests using the existing result-directory contract."""

from __future__ import annotations

import json
import re
import shutil
from collections import defaultdict, deque
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from qrp_atlas.config.paths import BACKTEST_RUNS_DIR

from ..portfolio.models import ORDER_REJECTED, PortfolioBacktestResult

_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_RESULT_FILENAMES = (
    "run_meta.json",
    "summary.json",
    "equity.json",
    "trades.json",
    "skipped.json",
    "config.json",
    "orders.json",
    "fills.json",
    "snapshots.json",
)


def _validate_run_id(run_id: str) -> str:
    if not run_id or not _RUN_ID_PATTERN.match(run_id):
        raise ValueError(f"invalid run_id: {run_id!r}")
    return run_id


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def portfolio_fills_to_trades(
    result: PortfolioBacktestResult,
) -> list[dict[str, Any]]:
    """Pair buy and sell fills FIFO into the existing BacktestTrade contract."""

    lots: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    trades: list[dict[str, Any]] = []
    trade_seq = 1

    for fill in sorted(result.fills, key=lambda item: (item.trade_date, item.fill_id)):
        if fill.side == "BUY":
            lots[fill.asset_id].append(
                {
                    "entry_date": fill.trade_date,
                    "entry_price": fill.execution_price,
                    "remaining_quantity": fill.quantity,
                    "buy_fee_per_share": fill.commission / fill.quantity,
                }
            )
            continue
        if fill.side != "SELL":
            raise ValueError(f"unsupported portfolio fill side: {fill.side!r}")

        remaining = fill.quantity
        sell_fee_per_share = (fill.commission + fill.stamp_tax) / fill.quantity
        queue = lots[fill.asset_id]
        while remaining > 0:
            if not queue:
                raise ValueError(
                    f"sell fill {fill.fill_id} exceeds FIFO inventory for {fill.asset_id}"
                )
            lot = queue[0]
            matched = min(remaining, lot["remaining_quantity"])
            entry_cost = matched * (
                lot["entry_price"] + lot["buy_fee_per_share"]
            )
            exit_proceeds = matched * (
                fill.execution_price - sell_fee_per_share
            )
            return_pct = (exit_proceeds / entry_cost - 1.0) * 100.0
            holding_days = (
                pd.Timestamp(fill.trade_date) - pd.Timestamp(lot["entry_date"])
            ).days
            trades.append(
                {
                    "trade_id": f"T{trade_seq:08d}",
                    "asset_id": fill.asset_id,
                    "signal_date": lot["entry_date"],
                    "entry_date": lot["entry_date"],
                    "entry_price": lot["entry_price"],
                    "exit_date": fill.trade_date,
                    "exit_price": fill.execution_price,
                    "holding_days": holding_days,
                    "return_pct": return_pct,
                    "mae_pct": None,
                    "mfe_pct": None,
                    "exit_reason": "target_rebalance",
                    "status": "closed",
                }
            )
            trade_seq += 1
            remaining -= matched
            lot["remaining_quantity"] -= matched
            if lot["remaining_quantity"] == 0:
                queue.popleft()

    for asset_id in sorted(lots):
        for lot in lots[asset_id]:
            if lot["remaining_quantity"] <= 0:
                continue
            trades.append(
                {
                    "trade_id": f"T{trade_seq:08d}",
                    "asset_id": asset_id,
                    "signal_date": lot["entry_date"],
                    "entry_date": lot["entry_date"],
                    "entry_price": lot["entry_price"],
                    "exit_date": None,
                    "exit_price": None,
                    "holding_days": None,
                    "return_pct": None,
                    "mae_pct": None,
                    "mfe_pct": None,
                    "exit_reason": None,
                    "status": "open",
                }
            )
            trade_seq += 1

    return trades


def _annual_return_pct(result: PortfolioBacktestResult) -> float | None:
    if len(result.snapshots) < 2:
        return None
    start = pd.Timestamp(result.snapshots[0].trade_date)
    end = pd.Timestamp(result.snapshots[-1].trade_date)
    days = (end - start).days
    total_return = float(result.summary["total_return"])
    if days <= 0 or total_return <= -1.0:
        return None
    return ((1.0 + total_return) ** (365.0 / days) - 1.0) * 100.0


def _summary_payload(
    run_id: str,
    result: PortfolioBacktestResult,
    trades: list[dict[str, Any]],
) -> dict[str, Any]:
    closed = [trade for trade in trades if trade["status"] == "closed"]
    returns = [float(trade["return_pct"]) for trade in closed]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    holdings = [
        int(trade["holding_days"])
        for trade in closed
        if trade["holding_days"] is not None
    ]
    avg_win = sum(wins) / len(wins) if wins else None
    avg_loss = sum(losses) / len(losses) if losses else None
    profit_loss_ratio = (
        avg_win / abs(avg_loss)
        if avg_win is not None and avg_loss not in (None, 0)
        else None
    )
    return {
        "run_id": run_id,
        "total_return_pct": float(result.summary["total_return_pct"]),
        "annual_return_pct": _annual_return_pct(result),
        "max_drawdown_pct": float(result.summary["max_drawdown_pct"]),
        "win_rate_pct": (len(wins) / len(closed) * 100.0) if closed else None,
        "profit_loss_ratio": profit_loss_ratio,
        "trade_count": len(closed),
        "avg_holding_days": (sum(holdings) / len(holdings)) if holdings else None,
        "max_trade_loss_pct": min(losses) if losses else None,
        "max_trade_profit_pct": max(wins) if wins else None,
        "skipped_count": int(result.summary["skipped_count"]),
        "turnover": float(result.summary["turnover"]),
        "commission": float(result.summary["commission"]),
        "stamp_tax": float(result.summary["stamp_tax"]),
        "slippage_cost": float(result.summary["slippage_cost"]),
        "total_cost": float(result.summary["total_cost"]),
        "final_equity": float(result.summary["final_equity"]),
    }


def _skipped_payload(result: PortfolioBacktestResult) -> list[dict[str, Any]]:
    return [
        {
            "asset_id": order.asset_id,
            "signal_date": order.trade_date,
            "reason": order.reason or "ORDER_REJECTED",
            "detail": (
                f"{order.side} target_weight={order.target_weight} "
                f"requested_quantity={order.requested_quantity}"
            ),
        }
        for order in result.orders
        if order.status == ORDER_REJECTED
    ]


class BacktestRunWriter:
    """Write portfolio output through a temporary result directory."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = Path(root) if root is not None else BACKTEST_RUNS_DIR

    def write_portfolio_run(
        self,
        result: PortfolioBacktestResult,
        *,
        run_id: str,
        strategy_name: str,
        universe: str,
        name: str | None = None,
        created_at: str | None = None,
        overwrite: bool = False,
    ) -> Path:
        _validate_run_id(run_id)
        run_dir = self.root / run_id
        if run_dir.exists() and not overwrite:
            raise FileExistsError(f"backtest run already exists: {run_id}")

        self.root.mkdir(parents=True, exist_ok=True)
        temp_dir = self.root / f".{run_id}.tmp"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir()

        try:
            trades = portfolio_fills_to_trades(result)
            start_date = result.snapshots[0].trade_date if result.snapshots else ""
            end_date = result.snapshots[-1].trade_date if result.snapshots else ""
            meta = {
                "run_id": run_id,
                "name": name or result.config.name,
                "strategy_name": strategy_name,
                "universe": universe,
                "start_date": start_date,
                "end_date": end_date,
                "created_at": created_at
                or datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "status": "completed",
            }
            payloads = {
                "run_meta.json": meta,
                "summary.json": _summary_payload(run_id, result, trades),
                "equity.json": list(result.equity_curve),
                "trades.json": trades,
                "skipped.json": _skipped_payload(result),
                "config.json": asdict(result.config),
                "orders.json": [order.to_dict() for order in result.orders],
                "fills.json": [fill.to_dict() for fill in result.fills],
                "snapshots.json": [snapshot.to_dict() for snapshot in result.snapshots],
            }
            for filename in _RESULT_FILENAMES:
                _write_json(temp_dir / filename, payloads[filename])

            if run_dir.exists():
                shutil.rmtree(run_dir)
            temp_dir.replace(run_dir)
        except Exception:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            raise

        return run_dir


__all__ = ["BacktestRunWriter", "portfolio_fills_to_trades"]
