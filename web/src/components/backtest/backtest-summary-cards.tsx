import { Card, CardContent } from '@/components/ui/card';
import { formatPct } from '@/shared/lib/format';
import type { BacktestSummary } from '@/types/backtest';

type Numberish = number | null | undefined;

function formatInt(value: Numberish): string {
  if (value == null || Number.isNaN(value)) return '—';
  return Math.trunc(value).toLocaleString();
}

function formatRatio(value: Numberish, decimals = 2): string {
  if (value == null || Number.isNaN(value)) return '—';
  return value.toFixed(decimals);
}

function formatDays(value: Numberish): string {
  if (value == null || Number.isNaN(value)) return '—';
  return value.toFixed(1);
}

type CardDef = {
  label: string;
  value: string;
  hint?: string;
  tone?: 'positive' | 'negative' | 'neutral';
};

function toneClass(tone?: CardDef['tone']): string {
  if (tone === 'positive') return 'text-red-600 dark:text-red-400';
  if (tone === 'negative') return 'text-green-600 dark:text-green-400';
  return 'text-slate-900 dark:text-white';
}

interface Props {
  summary: BacktestSummary | null;
  loading?: boolean;
}

export function BacktestSummaryCards({ summary, loading }: Props) {
  if (loading) {
    return (
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
        {Array.from({ length: 10 }).map((_, i) => (
          <Card key={i} className="bg-slate-50 dark:bg-slate-900/50">
            <CardContent className="p-4">
              <div className="mb-2 h-3 w-16 animate-pulse rounded bg-slate-200 dark:bg-slate-700" />
              <div className="h-6 w-20 animate-pulse rounded bg-slate-200 dark:bg-slate-700" />
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  if (!summary) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-6 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
        暂无汇总数据
      </div>
    );
  }

  const cards: CardDef[] = [
    {
      label: '累计收益',
      value: formatPct(summary.total_return_pct),
      tone: summary.total_return_pct == null ? 'neutral' : summary.total_return_pct >= 0 ? 'positive' : 'negative',
    },
    {
      label: '年化收益',
      value: formatPct(summary.annual_return_pct),
      tone: summary.annual_return_pct == null ? 'neutral' : summary.annual_return_pct >= 0 ? 'positive' : 'negative',
    },
    {
      label: '最大回撤',
      value: formatPct(summary.max_drawdown_pct),
      tone: 'negative',
    },
    {
      label: 'Sharpe',
      value: formatRatio(summary.sharpe ?? null),
    },
    {
      label: 'Sortino',
      value: formatRatio(summary.sortino ?? null),
    },
    {
      label: 'Calmar',
      value: formatRatio(summary.calmar ?? null),
    },
    {
      label: '胜率',
      value: formatPct(summary.win_rate_pct),
    },
    {
      label: '盈亏比',
      value: formatRatio(summary.profit_loss_ratio),
    },
    {
      label: '交易笔数',
      value: formatInt(summary.trade_count),
    },
    {
      label: '换手',
      value: formatRatio(summary.turnover ?? null, 4),
    },
    {
      label: '总费用',
      value: formatRatio(summary.total_cost ?? null, 2),
      hint: summary.commission != null
        ? `佣金 ${summary.commission} / 印花税 ${summary.stamp_tax} / 滑点 ${summary.slippage_cost}`
        : undefined,
    },
    {
      label: '平均持有天数',
      value: formatDays(summary.avg_holding_days),
    },
    {
      label: '单笔最大亏损',
      value: formatPct(summary.max_trade_loss_pct),
      tone: 'negative',
    },
    {
      label: '单笔最大盈利',
      value: formatPct(summary.max_trade_profit_pct),
      tone: 'positive',
    },
    {
      label: '跳过信号数',
      value: formatInt(summary.skipped_count),
    },
  ];

  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
      {cards.map((card) => (
        <Card key={card.label} className="bg-slate-50 dark:bg-slate-900/50">
          <CardContent className="p-4">
            <p className="mb-1 text-xs text-slate-500 dark:text-slate-400">{card.label}</p>
            <p className={`text-xl font-semibold ${toneClass(card.tone)}`}>{card.value}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
