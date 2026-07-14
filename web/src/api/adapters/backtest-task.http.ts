import type { BacktestTaskApi } from './types';
import { AdapterError } from './types';
import type {
  BacktestTask,
  CreateBacktestTaskRequest,
  CreateBacktestTaskResponse,
} from '@/types/backtest-task';
import { ApiError, request as httpRequest } from '../client';

function mapError(err: unknown): never {
  if (err instanceof ApiError) {
    throw new AdapterError(err.status, err.message);
  }
  throw err;
}

/** Map backend product statuses onto the UI status union. */
function normalizeTask(task: BacktestTask): BacktestTask {
  const status = String(task.status);
  let mapped = task.status;
  if (status === 'pending' || status === 'queued') mapped = 'pending';
  else if (status === 'running') mapped = 'running';
  else if (status === 'succeeded' || status === 'success' || status === 'completed') {
    mapped = 'succeeded';
  } else if (status === 'failed') mapped = 'failed';
  return {
    ...task,
    status: mapped,
    is_mock: Boolean(task.is_mock),
  };
}

export function createHttpBacktestTaskApi(): BacktestTaskApi {
  return {
    async createTask(payload: CreateBacktestTaskRequest) {
      try {
        const response = await httpRequest<CreateBacktestTaskResponse>('/api/backtest/tasks', {
          method: 'POST',
          body: payload,
        });
        return { task: normalizeTask(response.task) };
      } catch (err) {
        mapError(err);
      }
    },
    async listTasks() {
      try {
        const tasks = await httpRequest<BacktestTask[]>('/api/backtest/tasks');
        return tasks.map(normalizeTask);
      } catch (err) {
        mapError(err);
      }
    },
    async getTask(taskId: string) {
      try {
        const task = await httpRequest<BacktestTask>(
          `/api/backtest/tasks/${encodeURIComponent(taskId)}`,
        );
        return normalizeTask(task);
      } catch (err) {
        mapError(err);
      }
    },
  };
}
