interface Props {
  config: Record<string, unknown> | null;
  loading?: boolean;
  error?: string | null;
}

export function ConfigSnapshot({ config, loading, error }: Props) {
  if (loading) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-900/50">
        <div className="h-4 w-24 animate-pulse rounded bg-slate-200 dark:bg-slate-700" />
        <div className="mt-3 h-40 animate-pulse rounded bg-slate-200 dark:bg-slate-700" />
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

  if (!config || Object.keys(config).length === 0) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-6 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
        暂无配置数据
      </div>
    );
  }

  const json = JSON.stringify(config, null, 2);

  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-slate-900/50">
      <div className="border-b border-slate-200 px-4 py-2 text-xs font-medium text-slate-500 dark:border-slate-800 dark:text-slate-400">
        配置快照（只读）
      </div>
      <pre className="overflow-auto p-4 font-mono text-xs text-slate-800 dark:text-slate-200" style={{ maxHeight: 480 }}>
        {json}
      </pre>
    </div>
  );
}
