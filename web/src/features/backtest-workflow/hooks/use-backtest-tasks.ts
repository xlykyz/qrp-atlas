import { useCallback, useEffect, useRef, useState } from 'react';
import {
  createBacktestTask,
  getBacktestTask,
  listBacktestTasks,
} from '@/api/backtest-task';
import type { BacktestTask, CreateBacktestTaskRequest } from '@/types/backtest-task';

const POLL_MS = 500;

export function useBacktestTasks() {
  const [tasks, setTasks] = useState<BacktestTask[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const reload = useCallback(async () => {
    try {
      const list = await listBacktestTasks();
      setTasks(list);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : '加载任务列表失败');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void reload();
  }, [reload]);

  // Poll while any task is non-terminal
  useEffect(() => {
    const hasActive = tasks.some(
      (t) => t.status === 'pending' || t.status === 'running',
    );

    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }

    if (!hasActive) return;

    pollRef.current = setInterval(() => {
      void (async () => {
        try {
          const list = await listBacktestTasks();
          setTasks(list);
        } catch {
          // keep last known list on transient poll errors
        }
      })();
    }, POLL_MS);

    return () => {
      if (pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    };
  }, [tasks]);

  const createTask = useCallback(async (request: CreateBacktestTaskRequest) => {
    setSubmitting(true);
    setSubmitError(null);
    try {
      const { task } = await createBacktestTask(request);
      setTasks((prev) => {
        const rest = prev.filter((t) => t.task_id !== task.task_id);
        return [task, ...rest];
      });
      // Immediate refresh to sync store
      const fresh = await getBacktestTask(task.task_id);
      setTasks((prev) => {
        const rest = prev.filter((t) => t.task_id !== fresh.task_id);
        return [fresh, ...rest];
      });
      return fresh;
    } catch (err) {
      const msg = err instanceof Error ? err.message : '创建任务失败';
      setSubmitError(msg);
      throw err;
    } finally {
      setSubmitting(false);
    }
  }, []);

  return {
    tasks,
    loading,
    error,
    submitting,
    submitError,
    createTask,
    reload,
  };
}
