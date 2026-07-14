/**
 * In-memory mock task store with simulated status transitions.
 * Used only by mock adapters — never imported by page components.
 */

import type {
  BacktestTask,
  BacktestTaskStatus,
  CreateBacktestTaskRequest,
} from '@/types/backtest-task';
import { STRATEGY_RESULT_MAP } from './fixtures';

const tasks = new Map<string, BacktestTask>();
const timers = new Map<string, ReturnType<typeof setTimeout>[]>();

let seq = 1;

function nowIso(): string {
  return new Date().toISOString();
}

function makeIds(): { task_id: string; run_id: string } {
  const n = seq++;
  const stamp = Date.now().toString(36);
  return {
    task_id: `task_${stamp}_${n}`,
    run_id: `run_${stamp}_${n}`,
  };
}

function clearTimers(taskId: string) {
  const list = timers.get(taskId) ?? [];
  list.forEach((t) => clearTimeout(t));
  timers.delete(taskId);
}

function scheduleStatus(
  taskId: string,
  status: BacktestTaskStatus,
  delayMs: number,
  patch?: Partial<BacktestTask>,
) {
  const handle = setTimeout(() => {
    const current = tasks.get(taskId);
    if (!current) return;
    // Do not advance terminal states.
    if (current.status === 'succeeded' || current.status === 'failed') return;
    tasks.set(taskId, {
      ...current,
      ...patch,
      status,
      updated_at: nowIso(),
    });
  }, delayMs);
  const list = timers.get(taskId) ?? [];
  list.push(handle);
  timers.set(taskId, list);
}

/**
 * Force a deterministic failure for demo: name contains "fail" (case-insensitive)
 * or strategy rolling_zscore with lookback > 100.
 */
function shouldFail(req: CreateBacktestTaskRequest): string | null {
  if ((req.name ?? '').toLowerCase().includes('fail')) {
    return '模拟失败：任务名称包含 fail（演示错误态）';
  }
  const lookback = req.strategy_params.lookback;
  if (
    req.strategy_code === 'rolling_zscore_mean_reversion' &&
    typeof lookback === 'number' &&
    lookback > 100
  ) {
    return 'MOCK_ENGINE_ERROR: insufficient bars for lookback window';
  }
  return null;
}

export function mockListTasks(): BacktestTask[] {
  return Array.from(tasks.values()).sort((a, b) =>
    a.created_at < b.created_at ? 1 : -1,
  );
}

export function mockGetTask(taskId: string): BacktestTask | undefined {
  return tasks.get(taskId);
}

export function mockCreateTask(req: CreateBacktestTaskRequest): BacktestTask {
  const { task_id, run_id } = makeIds();
  const created_at = nowIso();
  const failReason = shouldFail(req);
  const fixtureRunId = STRATEGY_RESULT_MAP[req.strategy_code] ?? 'sample_run_001';

  const task: BacktestTask = {
    task_id,
    run_id: null,
    name:
      req.name?.trim() ||
      `${req.strategy_code}@${req.strategy_version} ${req.start_date}~${req.end_date}`,
    strategy_code: req.strategy_code,
    strategy_version: req.strategy_version,
    strategy_params: { ...req.strategy_params },
    universe_mode: req.universe_mode,
    universe_preset: req.universe_preset ?? null,
    tickers: req.tickers ? [...req.tickers] : [],
    start_date: req.start_date,
    end_date: req.end_date,
    position: { ...req.position },
    cost: { ...req.cost },
    execution: { ...req.execution },
    status: 'pending',
    error_message: null,
    created_at,
    updated_at: created_at,
    is_mock: true,
  };

  tasks.set(task_id, task);

  // Simulate queue → running → terminal
  scheduleStatus(task_id, 'running', 600);

  if (failReason) {
    scheduleStatus(task_id, 'failed', 1800, {
      error_message: failReason,
      run_id: null,
    });
  } else {
    // Attach a stable fixture run_id so results can be opened.
    // Use fixture id for known strategies so summary/equity match presets;
    // also register a virtual alias under generated run_id pointing to same fixture via mapping.
    scheduleStatus(task_id, 'succeeded', 2200, {
      run_id: fixtureRunId,
      error_message: null,
    });
  }

  // Keep generated run_id unused for now (fixture linkage is clearer for demos).
  void run_id;

  return { ...task };
}

/** Test helper / HMR reset */
export function mockResetTasks() {
  Array.from(tasks.keys()).forEach(clearTimers);
  tasks.clear();
  seq = 1;
}
