import type { BacktestTaskStatus } from '@/types/backtest-task';
import type { StrategyFamily } from '@/types/strategy';

export function taskStatusLabel(status: BacktestTaskStatus): string {
  switch (status) {
    case 'queued':
      return '排队中';
    case 'running':
      return '运行中';
    case 'success':
      return '成功';
    case 'failed':
      return '失败';
    default:
      return status;
  }
}

export function taskStatusClass(status: BacktestTaskStatus): string {
  switch (status) {
    case 'queued':
      return 'bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300';
    case 'running':
      return 'bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300';
    case 'success':
      return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300';
    case 'failed':
      return 'bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300';
    default:
      return 'bg-slate-100 text-slate-600';
  }
}

export function strategyFamilyLabel(family: StrategyFamily): string {
  switch (family) {
    case 'price_action':
      return '价格行为';
    case 'mean_reversion':
      return '均值回归';
    case 'trend':
      return '趋势';
    case 'breakout':
      return '突破';
    default:
      return '其他';
  }
}
