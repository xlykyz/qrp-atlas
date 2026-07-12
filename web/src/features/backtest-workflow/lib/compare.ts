import type { BacktestConfigSnapshot, BacktestSummary } from '@/types/backtest';

export type CompareMetricKey =
  | 'total_return_pct'
  | 'annual_return_pct'
  | 'max_drawdown_pct'
  | 'win_rate_pct'
  | 'profit_loss_ratio'
  | 'trade_count'
  | 'avg_holding_days'
  | 'max_trade_loss_pct'
  | 'max_trade_profit_pct'
  | 'skipped_count';

export const COMPARE_METRICS: {
  key: CompareMetricKey;
  label: string;
  format: 'pct' | 'ratio' | 'int' | 'days';
}[] = [
  { key: 'total_return_pct', label: '累计收益', format: 'pct' },
  { key: 'annual_return_pct', label: '年化收益', format: 'pct' },
  { key: 'max_drawdown_pct', label: '最大回撤', format: 'pct' },
  { key: 'win_rate_pct', label: '胜率', format: 'pct' },
  { key: 'profit_loss_ratio', label: '盈亏比', format: 'ratio' },
  { key: 'trade_count', label: '交易笔数', format: 'int' },
  { key: 'avg_holding_days', label: '平均持有天数', format: 'days' },
  { key: 'max_trade_loss_pct', label: '单笔最大亏损', format: 'pct' },
  { key: 'max_trade_profit_pct', label: '单笔最大盈利', format: 'pct' },
  { key: 'skipped_count', label: '跳过信号数', format: 'int' },
];

export interface ConfigDiffRow {
  path: string;
  values: Array<string | null>;
  differs: boolean;
}

function flattenConfig(
  value: unknown,
  prefix = '',
  out: Record<string, string> = {},
): Record<string, string> {
  if (value === null || value === undefined) {
    out[prefix || '(root)'] = 'null';
    return out;
  }
  if (Array.isArray(value)) {
    out[prefix || '(root)'] = JSON.stringify(value);
    return out;
  }
  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj);
    if (keys.length === 0) {
      out[prefix || '(root)'] = '{}';
      return out;
    }
    for (const k of keys.sort()) {
      const next = prefix ? `${prefix}.${k}` : k;
      flattenConfig(obj[k], next, out);
    }
    return out;
  }
  out[prefix || '(root)'] = String(value);
  return out;
}

export function buildConfigDiff(
  configs: Array<BacktestConfigSnapshot | null>,
): ConfigDiffRow[] {
  const maps = configs.map((c) => (c ? flattenConfig(c.config) : {}));
  const allKeys = new Set<string>();
  maps.forEach((m) => Object.keys(m).forEach((k) => allKeys.add(k)));

  return Array.from(allKeys)
    .sort()
    .map((path) => {
      const values = maps.map((m) => (path in m ? m[path] : null));
      const present = values.filter((v) => v != null);
      const differs =
        present.length === 0
          ? false
          : present.some((v) => v !== present[0]) ||
            values.some((v) => v == null);
      return { path, values, differs };
    });
}

export function pickSummaryValue(
  summary: BacktestSummary | null,
  key: CompareMetricKey,
): number | null {
  if (!summary) return null;
  const v = summary[key];
  return typeof v === 'number' ? v : v == null ? null : Number(v);
}
