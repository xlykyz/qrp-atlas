/**
 * Backtest task / job types used by the workflow shell.
 * Distinct from result-view types in ./backtest.ts.
 */

import type { StrategyParamValues } from './strategy';

/** Product status enum used by backend task API. */
export type BacktestTaskStatus = 'pending' | 'running' | 'succeeded' | 'failed';

export type EntryTiming = 'next_open' | 'same_close' | 'next_close';

/** Product universe modes. `index_components` is PIT historical membership. */
export type UniverseMode = 'preset' | 'tickers' | 'index_components';

export interface BacktestCostConfig {
  commission_rate: number;
  stamp_tax_rate: number;
  slippage_bps: number;
}

export interface BacktestPositionConfig {
  initial_cash: number;
  max_positions: number;
  /** Max weight per ticker, 0–1. */
  max_weight_per_symbol: number;
}

export interface BacktestExecutionConfig {
  entry_timing: EntryTiming;
}

/**
 * API request DTO for creating a backtest task.
 * Keep page form models separate and map via form-to-dto helpers.
 */
export interface CreateBacktestTaskRequest {
  name?: string;
  strategy_code: string;
  strategy_version: string;
  strategy_params: StrategyParamValues;
  universe_mode: UniverseMode;
  universe_preset?: string;
  /** Required when universe_mode is index_components. */
  index_code?: string;
  tickers?: string[];
  start_date: string;
  end_date: string;
  position: BacktestPositionConfig;
  cost: BacktestCostConfig;
  execution: BacktestExecutionConfig;
}

export interface BacktestTask {
  task_id: string;
  run_id: string | null;
  name: string;
  strategy_code: string;
  strategy_version: string;
  strategy_params: StrategyParamValues;
  universe_mode: UniverseMode;
  universe_preset: string | null;
  index_code?: string | null;
  tickers: string[];
  start_date: string;
  end_date: string;
  position: BacktestPositionConfig;
  cost: BacktestCostConfig;
  execution: BacktestExecutionConfig;
  status: BacktestTaskStatus;
  error_message: string | null;
  created_at: string;
  updated_at: string;
  /** True when results come from local mock fixtures, not a real engine run. */
  is_mock: boolean;
  /** Optional backend request snapshot. */
  request_snapshot?: Record<string, unknown>;
}

export interface CreateBacktestTaskResponse {
  task: BacktestTask;
}
