export interface BacktestRun {
  run_id: string;
  name: string;
  strategy_name: string;
  universe: string;
  start_date: string;
  end_date: string;
  created_at: string;
  status: string;
}

export interface BacktestSummary {
  run_id: string;
  total_return_pct: number | null;
  annual_return_pct: number | null;
  max_drawdown_pct: number | null;
  sharpe?: number | null;
  sortino?: number | null;
  calmar?: number | null;
  win_rate_pct: number | null;
  profit_loss_ratio: number | null;
  trade_count: number;
  avg_holding_days: number | null;
  max_trade_loss_pct: number | null;
  max_trade_profit_pct: number | null;
  skipped_count: number;
  turnover?: number | null;
  commission?: number | null;
  stamp_tax?: number | null;
  slippage_cost?: number | null;
  total_cost?: number | null;
  final_equity?: number | null;
}

export interface EquityPoint {
  date: string;
  equity: number;
  drawdown_pct: number;
}

export interface BacktestTrade {
  trade_id: string;
  asset_id: string;
  signal_date: string;
  entry_date: string;
  entry_price: number;
  exit_date: string | null;
  exit_price: number | null;
  holding_days: number | null;
  return_pct: number | null;
  mae_pct: number | null;
  mfe_pct: number | null;
  exit_reason: string | null;
  status: string;
}

export interface SkippedTrade {
  asset_id: string | null;
  signal_date: string | null;
  reason: string;
  detail: string | null;
}

export interface BacktestConfigSnapshot {
  run_id: string;
  config: Record<string, unknown>;
}
