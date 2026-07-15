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
  benchmark_id?: string | null;
  benchmark_total_return_pct?: number | null;
  excess_total_return_pct?: number | null;
  benchmark_sharpe?: number | null;
  excess_sharpe?: number | null;
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


export interface DailyReturnPoint {
  date: string;
  daily_return: number | null;
  equity?: number | null;
}

export interface RollingPerformancePoint {
  date: string;
  equity?: number | null;
  drawdown?: number | null;
  [key: string]: number | string | null | undefined;
}

export interface BenchmarkPoint {
  date: string;
  benchmark_level?: number | null;
  benchmark_return?: number | null;
  benchmark_cumulative_return?: number | null;
  portfolio_return?: number | null;
  excess_return?: number | null;
}

export interface BenchmarkArtifact {
  benchmark_id?: string | null;
  points: BenchmarkPoint[];
  summary?: Record<string, number | null | undefined>;
  diagnostics?: string[];
}

export interface ExposureArtifact {
  available: boolean;
  reason?: string | null;
  industry?: Array<Record<string, unknown>>;
  market_cap?: Array<Record<string, unknown>>;
  note?: string | null;
}

export interface ReproducibilityArtifact {
  locked_to_run_snapshot?: boolean;
  snapshot_hash?: string | null;
  strategy_code?: string | null;
  strategy_version?: string | null;
  benchmark_id?: string | null;
  note?: string | null;
  [key: string]: unknown;
}
