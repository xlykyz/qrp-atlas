import type { BacktestResultApi } from './types';
import { AdapterError } from './types';
import { getMockRunBundle, listMockRuns } from '../mock/fixtures';

function delay<T>(value: T, ms = 100): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), ms));
}

function requireBundle(runId: string) {
  const bundle = getMockRunBundle(runId);
  if (!bundle) throw new AdapterError(404, `backtest run not found: ${runId}`);
  return bundle;
}

export function createMockBacktestResultApi(): BacktestResultApi {
  return {
    async listRuns() {
      return delay(listMockRuns());
    },
    async getRun(runId) {
      return delay({ ...requireBundle(runId).meta });
    },
    async getSummary(runId) {
      return delay({ ...requireBundle(runId).summary });
    },
    async getEquity(runId) {
      return delay([...requireBundle(runId).equity]);
    },
    async getTrades(runId) {
      return delay(requireBundle(runId).trades.map((t) => ({ ...t })));
    },
    async getSkipped(runId) {
      return delay(requireBundle(runId).skipped.map((s) => ({ ...s })));
    },
    async getConfig(runId) {
      const bundle = requireBundle(runId);
      return delay({
        run_id: bundle.config.run_id,
        config: { ...bundle.config.config },
      });
    },
  };
}
