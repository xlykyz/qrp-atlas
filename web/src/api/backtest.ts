import type {
  BacktestConfigSnapshot,
  BacktestRun,
  BacktestSummary,
  BacktestTrade,
  EquityPoint,
  SkippedTrade,
} from '../types/backtest';
import { request } from './client';

export function getBacktestRuns(): Promise<BacktestRun[]> {
  return request('/api/backtest/runs');
}

export function getBacktestRun(runId: string): Promise<BacktestRun> {
  return request(`/api/backtest/runs/${encodeURIComponent(runId)}`);
}

export function getBacktestSummary(runId: string): Promise<BacktestSummary> {
  return request(`/api/backtest/runs/${encodeURIComponent(runId)}/summary`);
}

export function getBacktestEquity(runId: string): Promise<EquityPoint[]> {
  return request(`/api/backtest/runs/${encodeURIComponent(runId)}/equity`);
}

export function getBacktestTrades(runId: string): Promise<BacktestTrade[]> {
  return request(`/api/backtest/runs/${encodeURIComponent(runId)}/trades`);
}

export function getBacktestSkipped(runId: string): Promise<SkippedTrade[]> {
  return request(`/api/backtest/runs/${encodeURIComponent(runId)}/skipped`);
}

export function getBacktestConfig(runId: string): Promise<BacktestConfigSnapshot> {
  return request(`/api/backtest/runs/${encodeURIComponent(runId)}/config`);
}
