import type { BacktestTaskApi } from './types';
import { AdapterError } from './types';
import {
  mockCreateTask,
  mockGetTask,
  mockListTasks,
} from '../mock/task-store';

function delay<T>(value: T, ms = 80): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), ms));
}

export function createMockBacktestTaskApi(): BacktestTaskApi {
  return {
    async createTask(request) {
      if (!request.strategy_code) {
        throw new AdapterError(400, 'strategy_code is required');
      }
      if (!request.start_date || !request.end_date) {
        throw new AdapterError(400, 'start_date and end_date are required');
      }
      if (request.start_date > request.end_date) {
        throw new AdapterError(400, 'start_date must be <= end_date');
      }
      if (request.universe_mode === 'tickers' && (!request.tickers || request.tickers.length === 0)) {
        throw new AdapterError(400, 'tickers required when universe_mode is tickers');
      }
      const task = mockCreateTask(request);
      return delay({ task });
    },
    async listTasks() {
      return delay(mockListTasks().map((t) => ({ ...t })));
    },
    async getTask(taskId) {
      const task = mockGetTask(taskId);
      if (!task) throw new AdapterError(404, `task not found: ${taskId}`);
      return delay({ ...task });
    },
  };
}
