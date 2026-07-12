import type { BacktestResultApi } from './types';
import {
  getBacktestConfig,
  getBacktestEquity,
  getBacktestRun,
  getBacktestRuns,
  getBacktestSkipped,
  getBacktestSummary,
  getBacktestTrades,
} from '../backtest';

/** HTTP adapter wrapping the existing result endpoints. */
export function createHttpBacktestResultApi(): BacktestResultApi {
  return {
    listRuns: () => getBacktestRuns(),
    getRun: (runId) => getBacktestRun(runId),
    getSummary: (runId) => getBacktestSummary(runId),
    getEquity: (runId) => getBacktestEquity(runId),
    getTrades: (runId) => getBacktestTrades(runId),
    getSkipped: (runId) => getBacktestSkipped(runId),
    getConfig: (runId) => getBacktestConfig(runId),
  };
}
