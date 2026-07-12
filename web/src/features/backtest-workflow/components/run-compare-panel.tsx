import { formatPct } from '@/shared/lib/format';
import type { CompareRunData } from '../hooks/use-run-compare';
import type { ConfigDiffRow } from '../lib/compare';
import type { BacktestRun } from '@/types/backtest';

interface Props {
  availableRuns: BacktestRun[];
  runsLoading?: boolean;
  runsError?: string | null;
  selectedRunIds: string[];
  orderedDetails: CompareRunData[];
  metricRows: Array<{
    key: string;
    label: string;
    format: 'pct' | 'ratio' | 'int' | 'days';
    values: Array<number | null>;
  }>;
  configDiff: ConfigDiffRow[];
  onToggleRun: (runId: string) => void;
}

function formatMetric(
  value: number | null,
  format: 'pct' | 'ratio' | 'int' | 'days',
): string {
  if (value == null || Number.isNaN(value)) return '—';
  if (format === 'pct') return formatPct(value);
  if (format === 'ratio') return value.toFixed(2);
  if (format === 'days') return value.toFixed(1);
  return String(Math.trunc(value));
}

export function RunComparePanel({
  availableRuns,
  runsLoading,
  runsError,
  selectedRunIds,
  orderedDetails,
  metricRows,
  configDiff,
  onToggleRun,
}: Props) {
  if (runsLoading) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-6 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
        加载可对比 run…
      </div>
    );
  }

  if (runsError) {
    return (
      <div className="rounded-lg border border-red-300 bg-red-50 p-4 text-sm text-red-600 dark:border-red-800 dark:bg-red-950/30 dark:text-red-400">
        {runsError}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div>
        <p className="mb-2 text-xs text-slate-500 dark:text-slate-400">
          选择 2–4 个成功 run 进行指标与配置对比（当前为 mock 结果，不伪造 fixture 中不存在的组合能力）。
        </p>
        <div className="flex flex-wrap gap-2">
          {availableRuns.length === 0 ? (
            <span className="text-sm text-slate-500">暂无可对比 run</span>
          ) : (
            availableRuns.map((run) => {
              const checked = selectedRunIds.includes(run.run_id);
              return (
                <label
                  key={run.run_id}
                  className={`flex cursor-pointer items-center gap-2 rounded-lg border px-3 py-2 text-xs transition-colors ${
                    checked
                      ? 'border-blue-400 bg-blue-50 dark:border-blue-500 dark:bg-blue-950/40'
                      : 'border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900/40'
                  }`}
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => onToggleRun(run.run_id)}
                  />
                  <span>
                    <span className="font-medium text-slate-800 dark:text-slate-100">
                      {run.name}
                    </span>
                    <span className="mt-0.5 block font-mono text-[10px] text-slate-500">
                      {run.run_id} · {run.strategy_name}
                    </span>
                  </span>
                </label>
              );
            })
          )}
        </div>
      </div>

      {selectedRunIds.length === 0 ? (
        <div className="rounded-lg border border-dashed border-slate-300 p-6 text-center text-sm text-slate-500 dark:border-slate-700 dark:text-slate-400">
          请选择至少一个 run 开始对比
        </div>
      ) : (
        <>
          {orderedDetails.some((d) => d.loading) ? (
            <div className="text-sm text-slate-500">加载对比数据…</div>
          ) : null}
          {orderedDetails.some((d) => d.error) ? (
            <div className="rounded border border-red-200 bg-red-50 p-2 text-xs text-red-600 dark:border-red-900 dark:bg-red-950/30 dark:text-red-400">
              {orderedDetails
                .filter((d) => d.error)
                .map((d) => `${d.runId}: ${d.error}`)
                .join('；')}
            </div>
          ) : null}

          <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-xs text-slate-500 dark:bg-slate-900/80 dark:text-slate-400">
                <tr>
                  <th className="sticky left-0 bg-slate-50 px-3 py-2 font-medium dark:bg-slate-900/80">
                    指标
                  </th>
                  {orderedDetails.map((d) => (
                    <th key={d.runId} className="px-3 py-2 font-medium">
                      <div>{d.meta?.name ?? d.runId}</div>
                      <div className="font-mono text-[10px] font-normal opacity-70">{d.runId}</div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {metricRows.map((row) => (
                  <tr
                    key={row.key}
                    className="border-t border-slate-100 dark:border-slate-800"
                  >
                    <td className="sticky left-0 bg-white px-3 py-2 text-slate-600 dark:bg-slate-950 dark:text-slate-300">
                      {row.label}
                    </td>
                    {row.values.map((v, i) => (
                      <td
                        key={`${row.key}-${i}`}
                        className="px-3 py-2 font-medium text-slate-900 dark:text-white"
                      >
                        {formatMetric(v, row.format)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div>
            <h4 className="mb-2 text-sm font-medium text-slate-700 dark:text-slate-200">
              配置差异
            </h4>
            <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
              <table className="min-w-full text-xs">
                <thead className="bg-slate-50 text-left text-slate-500 dark:bg-slate-900/80 dark:text-slate-400">
                  <tr>
                    <th className="sticky left-0 bg-slate-50 px-3 py-2 dark:bg-slate-900/80">路径</th>
                    {orderedDetails.map((d) => (
                      <th key={`cfg-${d.runId}`} className="px-3 py-2">
                        {d.runId}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {configDiff.length === 0 ? (
                    <tr>
                      <td
                        className="px-3 py-4 text-slate-500"
                        colSpan={orderedDetails.length + 1}
                      >
                        暂无配置
                      </td>
                    </tr>
                  ) : (
                    configDiff.map((row) => (
                      <tr
                        key={row.path}
                        className={`border-t border-slate-100 dark:border-slate-800 ${
                          row.differs ? 'bg-amber-50/60 dark:bg-amber-950/20' : ''
                        }`}
                      >
                        <td className="sticky left-0 bg-inherit px-3 py-1.5 font-mono text-slate-600 dark:text-slate-300">
                          {row.path}
                        </td>
                        {row.values.map((v, i) => (
                          <td
                            key={`${row.path}-${i}`}
                            className="px-3 py-1.5 font-mono text-slate-800 dark:text-slate-100"
                          >
                            {v ?? '—'}
                          </td>
                        ))}
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
            <p className="mt-1 text-[11px] text-slate-400">高亮行表示各 run 配置值不一致。</p>
          </div>
        </>
      )}
    </div>
  );
}
