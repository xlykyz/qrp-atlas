/**
 * BacktestResultApi facade used by workflow compare / optional offline analysis.
 * Existing pages may continue using ./backtest.ts (HTTP). This facade is
 * switchable between mock fixtures and HTTP via VITE_BACKTEST_RESULT_SOURCE.
 */

import { getBacktestResultApi } from './adapters';

export function listResultRuns() {
  return getBacktestResultApi().listRuns();
}

export function getResultRun(runId: string) {
  return getBacktestResultApi().getRun(runId);
}

export function getResultSummary(runId: string) {
  return getBacktestResultApi().getSummary(runId);
}

export function getResultEquity(runId: string) {
  return getBacktestResultApi().getEquity(runId);
}

export function getResultTrades(runId: string) {
  return getBacktestResultApi().getTrades(runId);
}

export function getResultSkipped(runId: string) {
  return getBacktestResultApi().getSkipped(runId);
}

export function getResultConfig(runId: string) {
  return getBacktestResultApi().getConfig(runId);
}
