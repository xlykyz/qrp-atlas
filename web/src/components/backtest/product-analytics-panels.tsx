import type {
  BenchmarkArtifact,
  ExposureArtifact,
  ReproducibilityArtifact,
  RollingPerformancePoint,
} from '@/types/backtest';
import { Card, CardContent } from '@/components/ui/card';

function Empty({ text }: { text: string }) {
  return (
    <div className="rounded-lg border border-dashed border-slate-300 p-4 text-sm text-slate-500 dark:border-slate-700 dark:text-slate-400">
      {text}
    </div>
  );
}

export function BenchmarkPanel({
  data,
  loading,
  error,
}: {
  data: BenchmarkArtifact | null;
  loading?: boolean;
  error?: string | null;
}) {
  if (loading) return <Empty text="加载 benchmark…" />;
  if (error) return <Empty text={`Benchmark 加载失败：${error}`} />;
  if (!data || !data.points?.length) return <Empty text="无 benchmark 结果（未配置或数据缺失）" />;
  const points = data.points;
  const hasAny = points.some((p) => p.benchmark_level != null);
  if (!hasAny) {
    return (
      <Empty
        text={`Benchmark 缺失：${(data.diagnostics || []).join('; ') || '无可用序列'}`}
      />
    );
  }
  return (
    <Card className="bg-slate-50 dark:bg-slate-900/50">
      <CardContent className="space-y-2 p-4 text-sm">
        <div className="font-medium">Benchmark / Excess</div>
        <div className="text-xs text-slate-500">
          id: {data.benchmark_id || '—'}；观测点 {points.length}
        </div>
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
          <div>基准累计收益: {fmt(data.summary?.benchmark_total_return_pct)}%</div>
          <div>超额累计: {fmt(data.summary?.excess_total_return_pct)}%</div>
          <div>基准 Sharpe: {fmt(data.summary?.benchmark_sharpe)}</div>
          <div>超额 Sharpe: {fmt(data.summary?.excess_sharpe)}</div>
        </div>
        <div className="max-h-48 overflow-auto rounded border border-slate-200 dark:border-slate-700">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-slate-100 dark:bg-slate-800">
                <th className="p-1 text-left">date</th>
                <th className="p-1 text-right">bench ret</th>
                <th className="p-1 text-right">excess</th>
                <th className="p-1 text-right">cum bench</th>
              </tr>
            </thead>
            <tbody>
              {points.slice(-30).map((p) => (
                <tr key={p.date} className="border-t border-slate-100 dark:border-slate-800">
                  <td className="p-1">{p.date}</td>
                  <td className="p-1 text-right">{fmt(p.benchmark_return)}</td>
                  <td className="p-1 text-right">{fmt(p.excess_return)}</td>
                  <td className="p-1 text-right">{fmt(p.benchmark_cumulative_return)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {data.diagnostics?.length ? (
          <div className="text-xs text-amber-700 dark:text-amber-300">
            diagnostics: {data.diagnostics.join(' | ')}
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}

export function RollingPanel({
  data,
  loading,
  error,
}: {
  data: RollingPerformancePoint[] | null;
  loading?: boolean;
  error?: string | null;
}) {
  if (loading) return <Empty text="加载滚动表现…" />;
  if (error) return <Empty text={`滚动表现加载失败：${error}`} />;
  if (!data?.length) return <Empty text="无滚动表现数据" />;
  return (
    <Card className="bg-slate-50 dark:bg-slate-900/50">
      <CardContent className="space-y-2 p-4 text-sm">
        <div className="font-medium">滚动表现（含 equity=0 日期）</div>
        <div className="max-h-56 overflow-auto rounded border border-slate-200 dark:border-slate-700">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-slate-100 dark:bg-slate-800">
                <th className="p-1 text-left">date</th>
                <th className="p-1 text-right">equity</th>
                <th className="p-1 text-right">drawdown</th>
                <th className="p-1 text-right">ret_w20</th>
                <th className="p-1 text-right">sharpe_w20</th>
              </tr>
            </thead>
            <tbody>
              {data.slice(-40).map((p) => (
                <tr key={p.date} className="border-t border-slate-100 dark:border-slate-800">
                  <td className="p-1">{p.date}</td>
                  <td className="p-1 text-right">{fmt(p.equity as number | null)}</td>
                  <td className="p-1 text-right">{fmt(p.drawdown as number | null)}</td>
                  <td className="p-1 text-right">{fmt((p as any).return_w20)}</td>
                  <td className="p-1 text-right">{fmt((p as any).sharpe_w20)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}

export function ExposuresPanel({
  data,
  loading,
  error,
}: {
  data: ExposureArtifact | null;
  loading?: boolean;
  error?: string | null;
}) {
  if (loading) return <Empty text="加载暴露…" />;
  if (error) return <Empty text={`暴露加载失败：${error}`} />;
  if (!data || !data.available) {
    return <Empty text={data?.reason || '当前 run 无暴露结果（非横截面或空仓）'} />;
  }
  return (
    <Card className="bg-slate-50 dark:bg-slate-900/50">
      <CardContent className="space-y-2 p-4 text-sm">
        <div className="font-medium">行业 / 市值暴露</div>
        {data.note ? <div className="text-xs text-slate-500">{data.note}</div> : null}
        <div className="grid gap-3 lg:grid-cols-2">
          <div>
            <div className="mb-1 text-xs text-slate-500">industry rows: {data.industry?.length || 0}</div>
            <pre className="max-h-40 overflow-auto rounded bg-white p-2 text-xs dark:bg-slate-950">
              {JSON.stringify(data.industry?.slice(-10) || [], null, 2)}
            </pre>
          </div>
          <div>
            <div className="mb-1 text-xs text-slate-500">market_cap / concentration rows: {data.market_cap?.length || 0}</div>
            <pre className="max-h-40 overflow-auto rounded bg-white p-2 text-xs dark:bg-slate-950">
              {JSON.stringify(data.market_cap?.slice(-10) || [], null, 2)}
            </pre>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

export function ReproPanel({
  data,
  loading,
  error,
}: {
  data: ReproducibilityArtifact | null;
  loading?: boolean;
  error?: string | null;
}) {
  if (loading) return <Empty text="加载复现快照…" />;
  if (error) return <Empty text={`复现快照加载失败：${error}`} />;
  if (!data) return <Empty text="无复现快照" />;
  return (
    <Card className="bg-slate-50 dark:bg-slate-900/50">
      <CardContent className="space-y-2 p-4 text-sm">
        <div className="font-medium">复现快照</div>
        <div>locked: {String(data.locked_to_run_snapshot)}</div>
        <div className="break-all text-xs">hash: {data.snapshot_hash || '—'}</div>
        <div className="text-xs">
          strategy: {data.strategy_code}@{data.strategy_version} / benchmark: {data.benchmark_id || '—'}
        </div>
        <pre className="max-h-48 overflow-auto rounded bg-white p-2 text-xs dark:bg-slate-950">
          {JSON.stringify(data, null, 2)}
        </pre>
      </CardContent>
    </Card>
  );
}

function fmt(v: number | null | undefined) {
  if (v == null || Number.isNaN(Number(v))) return '—';
  return Number(v).toFixed(4);
}
