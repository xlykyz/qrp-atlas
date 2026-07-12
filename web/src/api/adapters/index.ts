import { createMockStrategyCatalogApi } from './strategy-catalog.mock';
import { createMockBacktestTaskApi } from './backtest-task.mock';
import { createMockBacktestResultApi } from './backtest-result.mock';
import { createHttpBacktestResultApi } from './backtest-result.http';
import { getResultSource, isMockCatalogAndTasksEnabled } from './mode';
import type {
  BacktestResultApi,
  BacktestTaskApi,
  StrategyCatalogApi,
} from './types';

export type { StrategyCatalogApi, BacktestTaskApi, BacktestResultApi } from './types';
export { AdapterError } from './types';

let strategyCatalogApi: StrategyCatalogApi | null = null;
let backtestTaskApi: BacktestTaskApi | null = null;
let backtestResultApi: BacktestResultApi | null = null;

export function getStrategyCatalogApi(): StrategyCatalogApi {
  if (!strategyCatalogApi) {
    // Future: swap to HTTP adapter when strategy catalog API is ready.
    strategyCatalogApi = isMockCatalogAndTasksEnabled()
      ? createMockStrategyCatalogApi()
      : createMockStrategyCatalogApi();
  }
  return strategyCatalogApi;
}

export function getBacktestTaskApi(): BacktestTaskApi {
  if (!backtestTaskApi) {
    backtestTaskApi = createMockBacktestTaskApi();
  }
  return backtestTaskApi;
}

export function getBacktestResultApi(): BacktestResultApi {
  if (!backtestResultApi) {
    backtestResultApi =
      getResultSource() === 'http'
        ? createHttpBacktestResultApi()
        : createMockBacktestResultApi();
  }
  return backtestResultApi;
}

/** Reset singletons (useful in tests / HMR). */
export function resetAdapters() {
  strategyCatalogApi = null;
  backtestTaskApi = null;
  backtestResultApi = null;
}

