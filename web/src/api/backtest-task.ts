/**
 * BacktestTaskApi facade for create / poll / list.
 */

import { getBacktestTaskApi } from './adapters';
import type { CreateBacktestTaskRequest } from '@/types/backtest-task';

export function createBacktestTask(request: CreateBacktestTaskRequest) {
  return getBacktestTaskApi().createTask(request);
}

export function listBacktestTasks() {
  return getBacktestTaskApi().listTasks();
}

export function getBacktestTask(taskId: string) {
  return getBacktestTaskApi().getTask(taskId);
}
