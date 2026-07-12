/**
 * Embedded backtest result fixtures for mock adapters.
 * Derived from tests/fixtures/backtest_runs/sample_run_001 and variants
 * for multi-run comparison demos. Pages must not import this module.
 */

import type {
  BacktestConfigSnapshot,
  BacktestRun,
  BacktestSummary,
  BacktestTrade,
  EquityPoint,
  SkippedTrade,
} from '@/types/backtest';

export interface MockRunBundle {
  meta: BacktestRun;
  summary: BacktestSummary;
  equity: EquityPoint[];
  trades: BacktestTrade[];
  skipped: SkippedTrade[];
  config: BacktestConfigSnapshot;
}

const BASE_EQUITY: EquityPoint[] = [
  { date: '2024-01-02', equity: 1.0, drawdown_pct: 0.0 },
  { date: '2024-01-03', equity: 1.028, drawdown_pct: 0.0 },
  { date: '2024-01-04', equity: 1.015, drawdown_pct: -1.26 },
  { date: '2024-01-05', equity: 1.042, drawdown_pct: 0.0 },
  { date: '2024-01-08', equity: 1.06, drawdown_pct: 0.0 },
  { date: '2024-01-09', equity: 1.078, drawdown_pct: 0.0 },
  { date: '2024-01-10', equity: 1.095, drawdown_pct: 0.0 },
  { date: '2024-01-11', equity: 1.082, drawdown_pct: -1.19 },
  { date: '2024-01-12', equity: 1.065, drawdown_pct: -2.74 },
  { date: '2024-01-15', equity: 1.048, drawdown_pct: -4.29 },
  { date: '2024-01-16', equity: 1.025, drawdown_pct: -6.39 },
  { date: '2024-01-17', equity: 1.0, drawdown_pct: -8.68 },
  { date: '2024-01-18', equity: 1.022, drawdown_pct: -6.67 },
  { date: '2024-01-19', equity: 1.045, drawdown_pct: -4.57 },
  { date: '2024-01-22', equity: 1.068, drawdown_pct: -2.47 },
  { date: '2024-01-23', equity: 1.085, drawdown_pct: -0.91 },
  { date: '2024-01-24', equity: 1.105, drawdown_pct: 0.0 },
  { date: '2024-01-25', equity: 1.125, drawdown_pct: 0.0 },
  { date: '2024-01-26', equity: 1.145, drawdown_pct: 0.0 },
  { date: '2024-01-29', equity: 1.165, drawdown_pct: 0.0 },
  { date: '2024-01-30', equity: 1.185, drawdown_pct: 0.0 },
  { date: '2024-01-31', equity: 1.205, drawdown_pct: 0.0 },
  { date: '2024-02-01', equity: 1.228, drawdown_pct: 0.0 },
  { date: '2024-02-02', equity: 1.25, drawdown_pct: 0.0 },
  { date: '2024-02-05', equity: 1.275, drawdown_pct: 0.0 },
  { date: '2024-02-06', equity: 1.298, drawdown_pct: 0.0 },
  { date: '2024-02-07', equity: 1.322, drawdown_pct: 0.0 },
  { date: '2024-02-08', equity: 1.345, drawdown_pct: 0.0 },
  { date: '2024-02-20', equity: 1.372, drawdown_pct: 0.0 },
  { date: '2024-02-21', equity: 1.425, drawdown_pct: 0.0 },
];

const BASE_TRADES: BacktestTrade[] = [
  {
    trade_id: 'T0001',
    asset_id: '000001.SZ',
    signal_date: '2024-01-08',
    entry_date: '2024-01-08',
    entry_price: 10.12,
    exit_date: '2024-01-12',
    exit_price: 10.84,
    holding_days: 4,
    return_pct: 7.11,
    mae_pct: -2.35,
    mfe_pct: 9.44,
    exit_reason: 'CLOSE_BELOW_MA5_2D',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0002',
    asset_id: '600519.SH',
    signal_date: '2024-01-09',
    entry_date: '2024-01-09',
    entry_price: 1685.2,
    exit_date: '2024-01-15',
    exit_price: 1720.5,
    holding_days: 5,
    return_pct: 2.1,
    mae_pct: -1.85,
    mfe_pct: 3.45,
    exit_reason: 'TIME_EXIT',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0003',
    asset_id: '000858.SZ',
    signal_date: '2024-01-10',
    entry_date: '2024-01-10',
    entry_price: 145.3,
    exit_date: '2024-01-12',
    exit_price: 142.8,
    holding_days: 2,
    return_pct: -1.72,
    mae_pct: -3.5,
    mfe_pct: 0.85,
    exit_reason: 'STOP_LOSS_3PCT',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0004',
    asset_id: '002594.SZ',
    signal_date: '2024-01-11',
    entry_date: '2024-01-11',
    entry_price: 215.6,
    exit_date: '2024-01-18',
    exit_price: 226.4,
    holding_days: 6,
    return_pct: 5.01,
    mae_pct: -1.2,
    mfe_pct: 6.32,
    exit_reason: 'TAKE_PROFIT_5PCT',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0005',
    asset_id: '600036.SH',
    signal_date: '2024-01-12',
    entry_date: '2024-01-12',
    entry_price: 35.2,
    exit_date: '2024-01-19',
    exit_price: 36.85,
    holding_days: 5,
    return_pct: 4.69,
    mae_pct: -0.85,
    mfe_pct: 5.2,
    exit_reason: 'TIME_EXIT',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0006',
    asset_id: '601318.SH',
    signal_date: '2024-01-15',
    entry_date: '2024-01-15',
    entry_price: 48.5,
    exit_date: '2024-01-17',
    exit_price: 47.2,
    holding_days: 2,
    return_pct: -2.68,
    mae_pct: -3.8,
    mfe_pct: 0.45,
    exit_reason: 'STOP_LOSS_3PCT',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0007',
    asset_id: '000333.SZ',
    signal_date: '2024-01-16',
    entry_date: '2024-01-16',
    entry_price: 78.9,
    exit_date: '2024-01-23',
    exit_price: 82.5,
    holding_days: 5,
    return_pct: 4.56,
    mae_pct: -1.1,
    mfe_pct: 5.8,
    exit_reason: 'TAKE_PROFIT_5PCT',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0008',
    asset_id: '002415.SZ',
    signal_date: '2024-01-17',
    entry_date: '2024-01-17',
    entry_price: 32.4,
    exit_date: '2024-01-22',
    exit_price: 33.85,
    holding_days: 3,
    return_pct: 4.48,
    mae_pct: -0.95,
    mfe_pct: 5.1,
    exit_reason: 'TIME_EXIT',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0009',
    asset_id: '600276.SH',
    signal_date: '2024-01-18',
    entry_date: '2024-01-18',
    entry_price: 45.8,
    exit_date: '2024-01-25',
    exit_price: 47.2,
    holding_days: 5,
    return_pct: 3.06,
    mae_pct: -1.4,
    mfe_pct: 4.25,
    exit_reason: 'TIME_EXIT',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0010',
    asset_id: '300750.SZ',
    signal_date: '2024-01-19',
    entry_date: '2024-01-19',
    entry_price: 168.5,
    exit_date: '2024-01-26',
    exit_price: 175.3,
    holding_days: 5,
    return_pct: 4.04,
    mae_pct: -1.55,
    mfe_pct: 5.45,
    exit_reason: 'TAKE_PROFIT_5PCT',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0011',
    asset_id: '601012.SH',
    signal_date: '2024-01-22',
    entry_date: '2024-01-22',
    entry_price: 22.1,
    exit_date: '2024-01-29',
    exit_price: 21.45,
    holding_days: 5,
    return_pct: -2.94,
    mae_pct: -4.1,
    mfe_pct: 0.8,
    exit_reason: 'STOP_LOSS_3PCT',
    status: 'CLOSED',
  },
  {
    trade_id: 'T0012',
    asset_id: '002241.SZ',
    signal_date: '2024-01-23',
    entry_date: '2024-01-23',
    entry_price: 18.65,
    exit_date: null,
    exit_price: null,
    holding_days: null,
    return_pct: null,
    mae_pct: null,
    mfe_pct: null,
    exit_reason: null,
    status: 'OPEN',
  },
];

const BASE_SKIPPED: SkippedTrade[] = [
  {
    asset_id: '000001.SZ',
    signal_date: '2024-01-05',
    reason: 'INVALID_PRICE',
    detail: 'non-finite high/low in MAE/MFE window',
  },
  {
    asset_id: '600519.SH',
    signal_date: '2024-01-08',
    reason: 'MISSING_BAR',
    detail: 'no price bar on entry date 2024-01-08',
  },
  {
    asset_id: '000858.SZ',
    signal_date: '2024-01-09',
    reason: 'OVERLAP_BLOCKED',
    detail: 'existing open position on 000858.SZ',
  },
  {
    asset_id: '002594.SZ',
    signal_date: '2024-01-10',
    reason: 'INVALID_PRICE',
    detail: 'negative open price after adjustment',
  },
];

function scaleEquity(points: EquityPoint[], factor: number, ddBoost = 0): EquityPoint[] {
  let peak = 1;
  return points.map((p, i) => {
    const equity = Number((1 + (p.equity - 1) * factor).toFixed(4));
    peak = Math.max(peak, equity);
    const rawDd = peak > 0 ? ((equity - peak) / peak) * 100 : 0;
    const drawdown_pct = Number((rawDd + (i % 5 === 0 ? ddBoost : 0)).toFixed(2));
    return { date: p.date, equity, drawdown_pct: Math.min(0, drawdown_pct) };
  });
}

function cloneTrades(scaleReturn: number, prefix: string): BacktestTrade[] {
  return BASE_TRADES.map((t, idx) => ({
    ...t,
    trade_id: `${prefix}${String(idx + 1).padStart(4, '0')}`,
    return_pct:
      t.return_pct == null ? null : Number((t.return_pct * scaleReturn).toFixed(2)),
  }));
}

export const MOCK_RUN_BUNDLES: Record<string, MockRunBundle> = {
  sample_run_001: {
    meta: {
      run_id: 'sample_run_001',
      name: 'Sample Backtest Run',
      strategy_name: 'system_b_basic',
      universe: 'A_SHARE',
      start_date: '2024-01-01',
      end_date: '2024-12-31',
      created_at: '2026-07-09T15:30:00',
      status: 'success',
    },
    summary: {
      run_id: 'sample_run_001',
      total_return_pct: 42.5,
      annual_return_pct: 12.3,
      max_drawdown_pct: -8.7,
      win_rate_pct: 46.2,
      profit_loss_ratio: 1.58,
      trade_count: 128,
      avg_holding_days: 3.4,
      max_trade_loss_pct: -9.8,
      max_trade_profit_pct: 31.2,
      skipped_count: 14,
    },
    equity: BASE_EQUITY,
    trades: BASE_TRADES,
    skipped: BASE_SKIPPED,
    config: {
      run_id: 'sample_run_001',
      config: {
        name: 'Sample Backtest Run',
        strategy_code: 'system_b_basic',
        strategy_version: '1.0.0',
        strategy_params: { min_hold_bars: 1, enable_reentry: true },
        universe: 'A_SHARE',
        start_date: '2024-01-01',
        end_date: '2024-12-31',
        entry: { timing: 'next_open', price_field: 'open' },
        position: {
          initial_cash: 1000000,
          max_positions: 10,
          max_weight_per_symbol: 0.1,
        },
        cost: {
          commission_rate: 0.00025,
          stamp_tax_rate: 0.0005,
          slippage_bps: 5,
        },
      },
    },
  },
  sample_run_002: {
    meta: {
      run_id: 'sample_run_002',
      name: 'Dual SMA Demo',
      strategy_name: 'dual_sma_trend',
      universe: 'CUSTOM:000001.SZ,600519.SH,000858.SZ',
      start_date: '2024-01-01',
      end_date: '2024-06-30',
      created_at: '2026-07-10T10:00:00',
      status: 'success',
    },
    summary: {
      run_id: 'sample_run_002',
      total_return_pct: 18.6,
      annual_return_pct: 8.4,
      max_drawdown_pct: -12.1,
      win_rate_pct: 42.0,
      profit_loss_ratio: 1.32,
      trade_count: 64,
      avg_holding_days: 8.2,
      max_trade_loss_pct: -11.5,
      max_trade_profit_pct: 22.4,
      skipped_count: 6,
    },
    equity: scaleEquity(BASE_EQUITY, 0.55, -0.4),
    trades: cloneTrades(0.7, 'S2'),
    skipped: BASE_SKIPPED.slice(0, 2),
    config: {
      run_id: 'sample_run_002',
      config: {
        name: 'Dual SMA Demo',
        strategy_code: 'dual_sma_trend',
        strategy_version: '1.0.0',
        strategy_params: {
          fast_window: 10,
          slow_window: 30,
          exit_on_cross_down: true,
        },
        universe: 'CUSTOM',
        tickers: ['000001.SZ', '600519.SH', '000858.SZ'],
        start_date: '2024-01-01',
        end_date: '2024-06-30',
        entry: { timing: 'next_open', price_field: 'open' },
        position: {
          initial_cash: 1000000,
          max_positions: 5,
          max_weight_per_symbol: 0.2,
        },
        cost: {
          commission_rate: 0.0003,
          stamp_tax_rate: 0.0005,
          slippage_bps: 8,
        },
      },
    },
  },
  sample_run_003: {
    meta: {
      run_id: 'sample_run_003',
      name: 'Donchian Breakout Demo',
      strategy_name: 'donchian_breakout',
      universe: 'CSI300',
      start_date: '2024-03-01',
      end_date: '2024-12-31',
      created_at: '2026-07-11T09:15:00',
      status: 'success',
    },
    summary: {
      run_id: 'sample_run_003',
      total_return_pct: 27.3,
      annual_return_pct: 10.1,
      max_drawdown_pct: -15.4,
      win_rate_pct: 39.5,
      profit_loss_ratio: 1.71,
      trade_count: 41,
      avg_holding_days: 12.6,
      max_trade_loss_pct: -14.2,
      max_trade_profit_pct: 38.0,
      skipped_count: 9,
    },
    equity: scaleEquity(BASE_EQUITY, 0.72, -0.8),
    trades: cloneTrades(0.85, 'S3'),
    skipped: BASE_SKIPPED.slice(1),
    config: {
      run_id: 'sample_run_003',
      config: {
        name: 'Donchian Breakout Demo',
        strategy_code: 'donchian_breakout',
        strategy_version: '1.0.0',
        strategy_params: {
          entry_window: 20,
          exit_window: 10,
          max_hold_bars: 40,
        },
        universe: 'CSI300',
        start_date: '2024-03-01',
        end_date: '2024-12-31',
        entry: { timing: 'next_open', price_field: 'open' },
        position: {
          initial_cash: 2000000,
          max_positions: 8,
          max_weight_per_symbol: 0.15,
        },
        cost: {
          commission_rate: 0.00025,
          stamp_tax_rate: 0.0005,
          slippage_bps: 10,
        },
      },
    },
  },
  sample_run_failed: {
    meta: {
      run_id: 'sample_run_failed',
      name: 'Failed Demo Run',
      strategy_name: 'rolling_zscore_mean_reversion',
      universe: 'A_SHARE',
      start_date: '2024-01-01',
      end_date: '2024-03-01',
      created_at: '2026-07-11T11:00:00',
      status: 'failed',
    },
    summary: {
      run_id: 'sample_run_failed',
      total_return_pct: null,
      annual_return_pct: null,
      max_drawdown_pct: null,
      win_rate_pct: null,
      profit_loss_ratio: null,
      trade_count: 0,
      avg_holding_days: null,
      max_trade_loss_pct: null,
      max_trade_profit_pct: null,
      skipped_count: 0,
    },
    equity: [],
    trades: [],
    skipped: [],
    config: {
      run_id: 'sample_run_failed',
      config: {
        name: 'Failed Demo Run',
        strategy_code: 'rolling_zscore_mean_reversion',
        strategy_version: '1.0.0',
        strategy_params: {
          lookback: 20,
          entry_zscore: -1.5,
          exit_zscore: 0,
          stop_loss_pct: 5,
        },
        universe: 'A_SHARE',
        start_date: '2024-01-01',
        end_date: '2024-03-01',
        error: 'MOCK_ENGINE_ERROR: insufficient bars for lookback window',
      },
    },
  },
};

/** Maps strategy code to a preset successful fixture run for demo completion. */
export const STRATEGY_RESULT_MAP: Record<string, string> = {
  system_b_basic: 'sample_run_001',
  dual_sma_trend: 'sample_run_002',
  donchian_breakout: 'sample_run_003',
  rolling_zscore_mean_reversion: 'sample_run_001',
};

export function listMockRuns(): BacktestRun[] {
  return Object.values(MOCK_RUN_BUNDLES).map((b) => b.meta);
}

export function getMockRunBundle(runId: string): MockRunBundle | undefined {
  return MOCK_RUN_BUNDLES[runId];
}
