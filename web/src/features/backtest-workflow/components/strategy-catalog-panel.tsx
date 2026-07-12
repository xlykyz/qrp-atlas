import type { StrategyCatalogItem } from '@/types/strategy';
import { strategyFamilyLabel } from '../lib/labels';
import { cn } from '@/lib/utils';

interface Props {
  strategies: StrategyCatalogItem[];
  selectedCode: string | null;
  loading?: boolean;
  error?: string | null;
  onSelect: (strategy: StrategyCatalogItem) => void;
}

export function StrategyCatalogPanel({
  strategies,
  selectedCode,
  loading,
  error,
  onSelect,
}: Props) {
  if (loading) {
    return (
      <div className="grid gap-3 sm:grid-cols-2">
        {Array.from({ length: 4 }).map((_, i) => (
          <div
            key={i}
            className="h-28 animate-pulse rounded-lg border border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-slate-900/50"
          />
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-lg border border-red-300 bg-red-50 p-4 text-sm text-red-600 dark:border-red-800 dark:bg-red-950/30 dark:text-red-400">
        {error}
      </div>
    );
  }

  if (strategies.length === 0) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-6 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
        暂无可用策略
      </div>
    );
  }

  return (
    <div className="grid gap-3 sm:grid-cols-2">
      {strategies.map((s) => {
        const active = s.code === selectedCode;
        return (
          <button
            key={`${s.code}@${s.version}`}
            type="button"
            onClick={() => onSelect(s)}
            className={cn(
              'rounded-lg border p-4 text-left transition-colors',
              active
                ? 'border-blue-500 bg-blue-50 ring-1 ring-blue-500 dark:border-blue-400 dark:bg-blue-950/40'
                : 'border-slate-200 bg-white hover:border-slate-300 dark:border-slate-800 dark:bg-slate-900/40 dark:hover:border-slate-700',
            )}
          >
            <div className="mb-1 flex items-center justify-between gap-2">
              <span className="font-medium text-slate-900 dark:text-white">{s.name}</span>
              <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[11px] text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                {strategyFamilyLabel(s.family)}
              </span>
            </div>
            <p className="mb-2 font-mono text-xs text-slate-500 dark:text-slate-400">
              {s.code} · v{s.version}
            </p>
            <p className="line-clamp-2 text-xs text-slate-600 dark:text-slate-300">
              {s.description}
            </p>
          </button>
        );
      })}
    </div>
  );
}
