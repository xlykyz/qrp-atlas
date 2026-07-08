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
import { formatPct, pctColor } from '@/shared/lib/format';
import { sortRows, type SortDir } from '@/shared/lib/sort';
import type { BacktestTrade } from '@/types/backtest';

type SortKey =
  | 'return_pct'
  | 'mae_pct'
  | 'mfe_pct'
  | 'holding_days'
  | 'entry_date'
  | 'exit_date';

const COLUMNS: { key: keyof BacktestTrade; label: string; sortable?: boolean }[] = [
  { key: 'trade_id', label: '交易ID' },
  { key: 'asset_id', label: '资产' },
  { key: 'signal_date', label: '信号日' },
  { key: 'entry_date', label: '入场日' },
  { key: 'entry_price', label: '入场价' },
  { key: 'exit_date', label: '出场日', sortable: true },
  { key: 'exit_price', label: '出场价' },
  { key: 'holding_days', label: '持有天', sortable: true },
  { key: 'return_pct', label: '收益%', sortable: true },
  { key: 'mae_pct', label: 'MAE%', sortable: true },
  { key: 'mfe_pct', label: 'MFE%', sortable: true },
  { key: 'exit_reason', label: '退出原因' },
  { key: 'status', label: '状态' },
];

interface Props {
  rows: BacktestTrade[] | null;
  loading?: boolean;
  error?: string | null;
}

function formatPrice(value: number | null): string {
  if (value == null || Number.isNaN(value)) return '—';
  return value.toFixed(2);
}

function formatDaysInt(value: number | null): string {
  if (value == null || Number.isNaN(value)) return '—';
  return String(Math.trunc(value));
}

export function TradesTable({ rows, loading, error }: Props) {
  const [keyword, setKeyword] = useState('');
  const [statusFilter, setStatusFilter] = useState<string>('ALL');
  const [reasonFilter, setReasonFilter] = useState<string>('ALL');
  const [sortKey, setSortKey] = useState<SortKey | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>('desc');

  const exitReasons = useMemo(() => {
    const set = new Set<string>();
    (rows ?? []).forEach((r) => {
      if (r.exit_reason) set.add(r.exit_reason);
    });
    return Array.from(set).sort();
  }, [rows]);

  const filtered = useMemo(() => {
    if (!rows) return [];
    let result = rows;
    const kw = keyword.trim().toLowerCase();
    if (kw) {
      result = result.filter((r) => r.asset_id.toLowerCase().includes(kw));
    }
    if (statusFilter !== 'ALL') {
      result = result.filter((r) => r.status === statusFilter);
    }
    if (reasonFilter !== 'ALL') {
      result = result.filter((r) => r.exit_reason === reasonFilter);
    }
    if (sortKey) {
      result = sortRows(result, sortKey, sortDir);
    }
    return result;
  }, [rows, keyword, statusFilter, reasonFilter, sortKey, sortDir]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((prev) => (prev === 'desc' ? 'asc' : 'desc'));
    } else {
      setSortKey(key);
      setSortDir('desc');
    }
  }

  if (loading) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-6 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
        加载交易明细中...
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
        暂无交易明细
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-1 text-xs text-slate-500 dark:text-slate-400">
          资产搜索
          <input
            value={keyword}
            onChange={(e) => setKeyword(e.target.value)}
            placeholder="如 000001.SZ"
            className="h-9 w-56 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-900 outline-none focus:border-slate-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white"
          />
        </label>
        <label className="flex flex-col gap-1 text-xs text-slate-500 dark:text-slate-400">
          状态
          <Select value={statusFilter} onValueChange={(v) => setStatusFilter(v ?? 'ALL')}>
            <SelectTrigger className="w-32">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">全部</SelectItem>
              <SelectItem value="CLOSED">CLOSED</SelectItem>
              <SelectItem value="OPEN">OPEN</SelectItem>
            </SelectContent>
          </Select>
        </label>
        <label className="flex flex-col gap-1 text-xs text-slate-500 dark:text-slate-400">
          退出原因
          <Select value={reasonFilter} onValueChange={(v) => setReasonFilter(v ?? 'ALL')}>
            <SelectTrigger className="w-56">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">全部</SelectItem>
              {exitReasons.map((r) => (
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
        <div className="w-full overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                {COLUMNS.map((col) => {
                  const isSortable = col.sortable;
                  const isActive = isSortable && sortKey === col.key;
                  return (
                    <TableHead
                      key={col.key}
                      className={
                        isSortable
                          ? 'cursor-pointer select-none whitespace-nowrap'
                          : 'whitespace-nowrap'
                      }
                      onClick={() => isSortable && toggleSort(col.key as SortKey)}
                    >
                      {col.label}
                      {isActive ? (sortDir === 'desc' ? ' ↓' : ' ↑') : ''}
                    </TableHead>
                  );
                })}
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.length === 0 ? (
                <TableRow>
                  <TableCell
                    colSpan={COLUMNS.length}
                    className="text-center text-sm text-slate-500 dark:text-slate-400"
                  >
                    筛选后无记录
                  </TableCell>
                </TableRow>
              ) : (
                filtered.map((row) => (
                  <TableRow key={row.trade_id}>
                    <TableCell className="whitespace-nowrap font-mono text-xs text-slate-500 dark:text-slate-400">
                      {row.trade_id}
                    </TableCell>
                    <TableCell className="whitespace-nowrap font-mono text-xs">
                      {row.asset_id}
                    </TableCell>
                    <TableCell className="whitespace-nowrap font-mono text-xs text-slate-500 dark:text-slate-400">
                      {row.signal_date}
                    </TableCell>
                    <TableCell className="whitespace-nowrap font-mono text-xs">
                      {row.entry_date}
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-right font-mono text-xs">
                      {formatPrice(row.entry_price)}
                    </TableCell>
                    <TableCell className="whitespace-nowrap font-mono text-xs">
                      {row.exit_date ?? '—'}
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-right font-mono text-xs">
                      {formatPrice(row.exit_price)}
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-right font-mono text-xs">
                      {formatDaysInt(row.holding_days)}
                    </TableCell>
                    <TableCell className={`whitespace-nowrap text-right font-mono text-xs ${pctColor(row.return_pct)}`}>
                      {formatPct(row.return_pct)}
                    </TableCell>
                    <TableCell className={`whitespace-nowrap text-right font-mono text-xs ${pctColor(row.mae_pct)}`}>
                      {formatPct(row.mae_pct)}
                    </TableCell>
                    <TableCell className={`whitespace-nowrap text-right font-mono text-xs ${pctColor(row.mfe_pct)}`}>
                      {formatPct(row.mfe_pct)}
                    </TableCell>
                    <TableCell className="whitespace-nowrap font-mono text-xs text-slate-700 dark:text-slate-200">
                      {row.exit_reason ?? '—'}
                    </TableCell>
                    <TableCell className="whitespace-nowrap">
                      <span
                        className={`rounded px-1.5 py-0.5 text-xs ${
                          row.status === 'OPEN'
                            ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300'
                            : 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300'
                        }`}
                      >
                        {row.status}
                      </span>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  );
}
