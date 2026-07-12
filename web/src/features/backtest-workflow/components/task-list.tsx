import { Link } from 'react-router-dom';
import { Button, buttonVariants } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import type { BacktestTask } from '@/types/backtest-task';
import { taskStatusClass, taskStatusLabel } from '../lib/labels';

interface Props {
  tasks: BacktestTask[];
  loading?: boolean;
  error?: string | null;
  selectedTaskId?: string | null;
  onSelect?: (task: BacktestTask) => void;
  onOpenResult?: (runId: string) => void;
  onAddToCompare?: (runId: string) => void;
}

export function TaskList({
  tasks,
  loading,
  error,
  selectedTaskId,
  onSelect,
  onOpenResult,
  onAddToCompare,
}: Props) {
  if (loading) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 3 }).map((_, i) => (
          <div
            key={i}
            className="h-16 animate-pulse rounded-lg border border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-slate-900/50"
          />
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-600 dark:border-red-800 dark:bg-red-950/30 dark:text-red-400">
        {error}
      </div>
    );
  }

  if (tasks.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-slate-300 p-6 text-center text-sm text-slate-500 dark:border-slate-700 dark:text-slate-400">
        尚未创建回测任务。配置策略后点击「创建模拟回测」。
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {tasks.map((task) => {
        const active = task.task_id === selectedTaskId;
        return (
          <div
            key={task.task_id}
            className={`rounded-lg border p-3 transition-colors ${
              active
                ? 'border-blue-400 bg-blue-50/60 dark:border-blue-500 dark:bg-blue-950/30'
                : 'border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900/40'
            }`}
          >
            <div className="flex flex-wrap items-start justify-between gap-2">
              <button
                type="button"
                className="min-w-0 flex-1 text-left"
                onClick={() => onSelect?.(task)}
              >
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-medium text-slate-900 dark:text-white">{task.name}</span>
                  <span
                    className={`rounded-full px-2 py-0.5 text-[11px] font-medium ${taskStatusClass(task.status)}`}
                  >
                    {taskStatusLabel(task.status)}
                  </span>
                  {task.is_mock ? (
                    <span className="rounded bg-amber-100 px-1.5 py-0.5 text-[10px] text-amber-800 dark:bg-amber-950 dark:text-amber-300">
                      MOCK
                    </span>
                  ) : null}
                </div>
                <p className="mt-1 font-mono text-[11px] text-slate-500 dark:text-slate-400">
                  {task.task_id}
                  {task.run_id ? ` · run ${task.run_id}` : ''}
                </p>
                <p className="mt-0.5 text-xs text-slate-500 dark:text-slate-400">
                  {task.strategy_code}@{task.strategy_version} · {task.start_date} → {task.end_date}
                </p>
              </button>

              <div className="flex flex-wrap gap-1.5">
                {task.status === 'success' && task.run_id ? (
                  <>
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => onOpenResult?.(task.run_id!)}
                    >
                      打开结果
                    </Button>
                    <Button
                      size="sm"
                      variant="secondary"
                      onClick={() => onAddToCompare?.(task.run_id!)}
                    >
                      加入对比
                    </Button>
                    <Link
                      to={`/backtest?runId=${encodeURIComponent(task.run_id)}&source=mock`}
                      className={cn(buttonVariants({ size: 'sm', variant: 'ghost' }))}
                    >
                      分析页
                    </Link>
                  </>
                ) : null}
              </div>
            </div>

            {task.status === 'failed' && task.error_message ? (
              <div className="mt-2 rounded border border-red-200 bg-red-50 px-2 py-1.5 text-xs text-red-600 dark:border-red-900 dark:bg-red-950/40 dark:text-red-400">
                {task.error_message}
              </div>
            ) : null}

            {(task.status === 'queued' || task.status === 'running') && (
              <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-200 dark:bg-slate-800">
                <div
                  className={`h-full rounded-full bg-blue-500 transition-all ${
                    task.status === 'queued' ? 'w-1/4 animate-pulse' : 'w-2/3 animate-pulse'
                  }`}
                />
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
