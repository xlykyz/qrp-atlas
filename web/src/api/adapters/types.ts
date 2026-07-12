/**
 * API adapter contracts for strategy catalog, backtest tasks, and results.
 * Page code should depend on these interfaces via the facade modules,
 * not on mock data or HTTP details.
 */

import type {
  BacktestConfigSnapshot,
  BacktestRun,
  BacktestSummary,
  BacktestTrade,
  EquityPoint,
  SkippedTrade,
} from '@/types/backtest';
import type {
  BacktestTask,
  CreateBacktestTaskRequest,
  CreateBacktestTaskResponse,
} from '@/types/backtest-task';
import type { StrategyCatalogItem } from '@/types/strategy';

export interface StrategyCatalogApi {
  listStrategies(): Promise<StrategyCatalogItem[]>;
  getStrategy(code: string, version?: string): Promise<StrategyCatalogItem>;
}

export interface BacktestTaskApi {
  createTask(request: CreateBacktestTaskRequest): Promise<CreateBacktestTaskResponse>;
  listTasks(): Promise<BacktestTask[]>;
  getTask(taskId: string): Promise<BacktestTask>;
}

export interface BacktestResultApi {
  listRuns(): Promise<BacktestRun[]>;
  getRun(runId: string): Promise<BacktestRun>;
  getSummary(runId: string): Promise<BacktestSummary>;
  getEquity(runId: string): Promise<EquityPoint[]>;
  getTrades(runId: string): Promise<BacktestTrade[]>;
  getSkipped(runId: string): Promise<SkippedTrade[]>;
  getConfig(runId: string): Promise<BacktestConfigSnapshot>;
}

export class AdapterError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'AdapterError';
    this.status = status;
  }
}
