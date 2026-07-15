import type {
  BacktestConfigSnapshot,
  BacktestRun,
  BacktestSummary,
  BacktestTrade,
  BenchmarkArtifact,
  EquityPoint,
  ExposureArtifact,
  ReproducibilityArtifact,
  RollingPerformancePoint,
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


export function getBacktestRolling(runId: string): Promise<RollingPerformancePoint[]> {
  return request(`/api/backtest/runs/${encodeURIComponent(runId)}/rolling`);
}

export async function getBacktestBenchmark(runId: string): Promise<BenchmarkArtifact> {
  const data = await request<BenchmarkArtifact | null>(`/api/backtest/runs/${encodeURIComponent(runId)}/benchmark`);
  return data ?? { benchmark_id: null, points: [], summary: {}, diagnostics: ['benchmark_missing'] };
}

export async function getBacktestExposures(runId: string): Promise<ExposureArtifact> {
  const data = await request<ExposureArtifact | null>(`/api/backtest/runs/${encodeURIComponent(runId)}/exposures`);
  return data ?? { available: false, reason: 'missing_artifact', industry: [], market_cap: [] };
}

export async function getBacktestReproducibility(runId: string): Promise<ReproducibilityArtifact> {
  const data = await request<ReproducibilityArtifact | null>(`/api/backtest/runs/${encodeURIComponent(runId)}/reproducibility`);
  return data ?? { locked_to_run_snapshot: false, note: 'missing_artifact' };
}
