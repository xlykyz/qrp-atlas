import { useMemo } from 'react';
import type { BacktestTrade } from '@/types/backtest';

type Stats = {
  count: number;
  min: number;
  max: number;
  avg: number;
  buckets: { label: string; count: number }[];
};

function buildStats(values: number[], bucketCount = 8): Stats | null {
  const finite = values.filter((v) => Number.isFinite(v));
  if (finite.length === 0) return null;

  const min = Math.min(...finite);
  const max = Math.max(...finite);
  const avg = finite.reduce((s, v) => s + v, 0) / finite.length;

  if (min === max) {
    return {
      count: finite.length,
      min,
      max,
      avg,
      buckets: [{ label: formatNum(min), count: finite.length }],
    };
  }

  const step = (max - min) / bucketCount;
  const buckets = Array.from({ length: bucketCount }, (_, i) => {
    const start = min + step * i;
    const end = i === bucketCount - 1 ? max : start + step;
    return {
      label: `${formatNum(start)}~${formatNum(end)}`,
      count: 0,
    };
  });

  for (const v of finite) {
    const idx = Math.min(bucketCount - 1, Math.floor((v - min) / step));
    buckets[idx].count += 1;
  }

  return { count: finite.length, min, max, avg, buckets };
}

function formatNum(v: number): string {
  if (Math.abs(v) >= 100) return v.toFixed(0);
  if (Math.abs(v) >= 1) return v.toFixed(2);
  return v.toFixed(2);
}

interface DistributionCardProps {
  title: string;
  stats: Stats | null;
  unit?: string;
}

function DistributionCard({ title, stats, unit }: DistributionCardProps) {
  if (!stats) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-900/50">
        <p className="mb-2 text-sm font-medium text-slate-700 dark:text-slate-200">{title}</p>
        <p className="text-xs text-slate-400">无可用数据</p>
      </div>
    );
  }

  const maxCount = Math.max(...stats.buckets.map((b) => b.count), 1);

  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-900/50">
      <p className="mb-3 text-sm font-medium text-slate-700 dark:text-slate-200">
        {title}
        {unit ? <span className="ml-1 text-xs text-slate-400">({unit})</span> : null}
      </p>
      <div className="mb-3 grid grid-cols-4 gap-2 text-xs">
        <div>
          <span className="text-slate-400">样本</span>
          <p className="font-mono text-slate-700 dark:text-slate-200">{stats.count}</p>
        </div>
        <div>
          <span className="text-slate-400">最小</span>
          <p className="font-mono text-slate-700 dark:text-slate-200">{formatNum(stats.min)}</p>
        </div>
        <div>
          <span className="text-slate-400">平均</span>
          <p className="font-mono text-slate-700 dark:text-slate-200">{formatNum(stats.avg)}</p>
        </div>
        <div>
          <span className="text-slate-400">最大</span>
          <p className="font-mono text-slate-700 dark:text-slate-200">{formatNum(stats.max)}</p>
        </div>
      </div>
      <div className="grid gap-1">
        {stats.buckets.map((b, i) => {
          const width = `${Math.max(2, (b.count / maxCount) * 100)}%`;
          return (
            <div
              key={`${b.label}-${i}`}
              className="grid grid-cols-[minmax(120px,200px)_1fr_32px] items-center gap-2 text-xs"
            >
              <span className="truncate font-mono text-slate-500 dark:text-slate-400" title={b.label}>
                {b.label}
              </span>
              <div className="h-4 rounded bg-slate-200 dark:bg-slate-800">
                <div className="h-4 rounded bg-blue-500/70" style={{ width }} />
              </div>
              <span className="text-right font-mono text-slate-600 dark:text-slate-300">{b.count}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

interface Props {
  trades: BacktestTrade[] | null;
  loading?: boolean;
  error?: string | null;
}

export function RiskDistribution({ trades, loading, error }: Props) {
  const returnStats = useMemo(() => {
    if (!trades) return null;
    return buildStats(trades.map((t) => t.return_pct).filter((v): v is number => v != null));
  }, [trades]);

  const maeStats = useMemo(() => {
    if (!trades) return null;
    return buildStats(trades.map((t) => t.mae_pct).filter((v): v is number => v != null));
  }, [trades]);

  const holdingStats = useMemo(() => {
    if (!trades) return null;
    return buildStats(trades.map((t) => t.holding_days).filter((v): v is number => v != null));
  }, [trades]);

  if (loading) {
    return (
      <div className="grid gap-3 lg:grid-cols-3">
        {Array.from({ length: 3 }).map((_, i) => (
          <div
            key={i}
            className="rounded-lg border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-900/50"
          >
            <div className="h-4 w-24 animate-pulse rounded bg-slate-200 dark:bg-slate-700" />
            <div className="mt-3 h-32 animate-pulse rounded bg-slate-200 dark:bg-slate-700" />
          </div>
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

  if (!trades || trades.length === 0) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-6 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
        暂无 trades 数据用于分布计算
      </div>
    );
  }

  return (
    <div className="grid gap-3 lg:grid-cols-3">
      <DistributionCard title="收益率分布" stats={returnStats} unit="%" />
      <DistributionCard title="MAE 分布" stats={maeStats} unit="%" />
      <DistributionCard title="持有天数分布" stats={holdingStats} />
    </div>
  );
}
