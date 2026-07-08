import { useMemo, useState } from 'react';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import type { SkippedTrade } from '@/types/backtest';

interface Props {
  rows: SkippedTrade[] | null;
  loading?: boolean;
  error?: string | null;
}

export function SkippedTable({ rows, loading, error }: Props) {
  const [reasonFilter, setReasonFilter] = useState<string>('ALL');

  const reasons = useMemo(() => {
    const set = new Set<string>();
    (rows ?? []).forEach((r) => {
      if (r.reason) set.add(r.reason);
    });
    return Array.from(set).sort();
  }, [rows]);

  const filtered = useMemo(() => {
    if (!rows) return [];
    if (reasonFilter === 'ALL') return rows;
    return rows.filter((r) => r.reason === reasonFilter);
  }, [rows, reasonFilter]);

  if (loading) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-6 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
        加载 skipped 记录中...
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

  if (!rows || rows.length === 0) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-6 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
        无 skipped 记录
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex items-end gap-3">
        <label className="flex flex-col gap-1 text-xs text-slate-500 dark:text-slate-400">
          原因筛选
          <Select value={reasonFilter} onValueChange={(v) => setReasonFilter(v ?? 'ALL')}>
            <SelectTrigger className="w-56">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">全部</SelectItem>
              {reasons.map((r) => (
                <SelectItem key={r} value={r}>
                  {r}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
        <span className="text-xs text-slate-500 dark:text-slate-400">
          共 {filtered.length} 条
        </span>
      </div>

      <div className="rounded-lg border border-slate-200 dark:border-slate-800">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-32">资产</TableHead>
              <TableHead className="w-32">信号日期</TableHead>
              <TableHead className="w-48">原因</TableHead>
              <TableHead>详情</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filtered.length === 0 ? (
              <TableRow>
                <TableCell colSpan={4} className="text-center text-sm text-slate-500 dark:text-slate-400">
                  筛选后无记录
                </TableCell>
              </TableRow>
            ) : (
              filtered.map((row, idx) => (
                <TableRow key={`${row.asset_id ?? ''}-${row.signal_date ?? ''}-${idx}`}>
                  <TableCell className="font-mono text-xs">{row.asset_id ?? '—'}</TableCell>
                  <TableCell className="font-mono text-xs text-slate-500 dark:text-slate-400">
                    {row.signal_date ?? '—'}
                  </TableCell>
                  <TableCell className="font-mono text-xs">{row.reason}</TableCell>
                  <TableCell className="text-xs text-slate-700 dark:text-slate-200">
                    {row.detail ?? '—'}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
