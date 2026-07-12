import type { StrategyCatalogItem } from '@/types/strategy';
import { strategyFamilyLabel } from '../lib/labels';

interface Props {
  strategy: StrategyCatalogItem | null;
}

export function StrategyDetailCard({ strategy }: Props) {
  if (!strategy) {
    return (
      <div className="rounded-lg border border-dashed border-slate-300 p-6 text-sm text-slate-500 dark:border-slate-700 dark:text-slate-400">
        从左侧目录选择一个内置策略，查看说明、版本与适用范围。
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-900/50">
      <div className="mb-3 flex flex-wrap items-start justify-between gap-2">
        <div>
          <h3 className="text-base font-semibold text-slate-900 dark:text-white">
            {strategy.name}
          </h3>
          <p className="mt-0.5 font-mono text-xs text-slate-500 dark:text-slate-400">
            {strategy.code} @ {strategy.version} · {strategy.strategy_type}
          </p>
        </div>
        <span className="rounded-full bg-white px-2 py-0.5 text-xs text-slate-600 shadow-sm dark:bg-slate-800 dark:text-slate-300">
          {strategyFamilyLabel(strategy.family)}
        </span>
      </div>

      <p className="mb-3 text-sm leading-relaxed text-slate-700 dark:text-slate-200">
        {strategy.description}
      </p>

      <dl className="grid gap-2 text-xs sm:grid-cols-2">
        <div>
          <dt className="text-slate-500 dark:text-slate-400">适用范围</dt>
          <dd className="mt-0.5 text-slate-800 dark:text-slate-200">{strategy.scope}</dd>
        </div>
        <div>
          <dt className="text-slate-500 dark:text-slate-400">所需字段</dt>
          <dd className="mt-0.5 font-mono text-slate-800 dark:text-slate-200">
            {strategy.required_fields.join(', ') || '—'}
          </dd>
        </div>
        <div className="sm:col-span-2">
          <dt className="text-slate-500 dark:text-slate-400">所需指标</dt>
          <dd className="mt-0.5 font-mono text-slate-800 dark:text-slate-200">
            {strategy.required_indicators.join(', ') || '—'}
          </dd>
        </div>
      </dl>
    </div>
  );
}
