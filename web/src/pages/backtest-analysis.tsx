import { useEffect, useState } from 'react';
import { useOutletContext } from 'react-router-dom';
import { BacktestRunSelector } from '@/components/backtest/backtest-run-selector';
import { BacktestSummaryCards } from '@/components/backtest/backtest-summary-cards';
import { ConfigSnapshot } from '@/components/backtest/config-snapshot';
import { DrawdownChart } from '@/components/backtest/drawdown-chart';
import { EquityCurveChart } from '@/components/backtest/equity-curve-chart';
import { RiskDistribution } from '@/components/backtest/risk-distribution';
import { SkippedTable } from '@/components/backtest/skipped-table';
import { TradesTable } from '@/components/backtest/trades-table';
import {
  getBacktestConfig,
  getBacktestEquity,
  getBacktestRuns,
  getBacktestSkipped,
  getBacktestSummary,
  getBacktestTrades,
} from '@/api/backtest';
import type {
  BacktestConfigSnapshot,
  BacktestRun,
  BacktestSummary,
  BacktestTrade,
  EquityPoint,
  SkippedTrade,
} from '@/types/backtest';

type Tab = 'trades' | 'risk' | 'skipped' | 'config';

type ResultState<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

function initialState<T>(): ResultState<T> {
  return { data: null, loading: false, error: null };
}

export default function BacktestAnalysis() {
  const { setPageTitle, setHeaderControls } = useOutletContext<{
    setPageTitle: (t: string) => void;
    setHeaderControls: (c: React.ReactNode | null) => void;
  }>();

  const [runs, setRuns] = useState<BacktestRun[]>([]);
  const [runsLoading, setRunsLoading] = useState(true);
  const [runsError, setRunsError] = useState<string | null>(null);

  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<Tab>('trades');

  const [summary, setSummary] = useState<ResultState<BacktestSummary>>(initialState());
  const [equity, setEquity] = useState<ResultState<EquityPoint[]>>(initialState());
  const [trades, setTrades] = useState<ResultState<BacktestTrade[]>>(initialState());
  const [skipped, setSkipped] = useState<ResultState<SkippedTrade[]>>(initialState());
  const [config, setConfig] = useState<ResultState<BacktestConfigSnapshot>>(initialState());

  // ── Page title ──
  useEffect(() => {
    setPageTitle('回测分析');
    return () => {
      setPageTitle('');
      setHeaderControls(null);
    };
  }, [setPageTitle, setHeaderControls]);

  // ── Load runs ──
  useEffect(() => {
    let cancelled = false;
    setRunsLoading(true);
    setRunsError(null);

    getBacktestRuns()
      .then((list) => {
        if (cancelled) return;
        setRuns(list);
        setRunsLoading(false);
      })
      .catch((err) => {
        if (cancelled) return;
        setRunsError(err instanceof Error ? err.message : '加载 runs 失败');
        setRunsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  // ── Auto-select when only one run or first run ──
  useEffect(() => {
    if (selectedRunId) return;
    if (runs.length === 0) return;
    setSelectedRunId(runs[0].run_id);
  }, [runs, selectedRunId]);

  // ── Header controls: run selector ──
  useEffect(() => {
    setHeaderControls(
      runsLoading ? (
        <span className="text-sm text-slate-400">加载 runs...</span>
      ) : (
        <BacktestRunSelector
          runs={runs}
          selectedRunId={selectedRunId}
          onSelect={setSelectedRunId}
        />
      ),
    );
    return () => {
      setHeaderControls(null);
    };
  }, [runs, runsLoading, selectedRunId, setHeaderControls]);

  // ── Load all detail data when run changes ──
  useEffect(() => {
    if (!selectedRunId) return;
    let cancelled = false;

    const fetchOne = async <T,>(
      fetcher: () => Promise<T>,
      setter: (s: ResultState<T>) => void,
    ): Promise<void> => {
      setter({ data: null, loading: true, error: null });
      try {
        const data = await fetcher();
        if (!cancelled) setter({ data, loading: false, error: null });
      } catch (err) {
        if (!cancelled) {
          setter({
            data: null,
            loading: false,
            error: err instanceof Error ? err.message : '加载失败',
          });
        }
      }
    };

    Promise.all([
      fetchOne(() => getBacktestSummary(selectedRunId), setSummary),
      fetchOne(() => getBacktestEquity(selectedRunId), setEquity),
      fetchOne(() => getBacktestTrades(selectedRunId), setTrades),
      fetchOne(() => getBacktestSkipped(selectedRunId), setSkipped),
      fetchOne(() => getBacktestConfig(selectedRunId), setConfig),
    ]);

    return () => {
      cancelled = true;
    };
  }, [selectedRunId]);

  // ── Tabs config ──
  const tabs: { key: Tab; label: string }[] = [
    { key: 'trades', label: '交易明细' },
    { key: 'risk', label: '风险分布' },
    { key: 'skipped', label: 'Skipped 记录' },
    { key: 'config', label: '配置快照' },
  ];

  return (
    <div className="space-y-6">
      {runsError && (
        <div className="rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-600 dark:border-red-800 dark:bg-red-950/30 dark:text-red-400">
          加载 runs 列表失败：{runsError}
        </div>
      )}

      {!runsError && !runsLoading && runs.length === 0 && (
        <div className="rounded-lg border border-slate-200 bg-slate-50 p-6 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
          暂无可查看的回测 run
        </div>
      )}

      {selectedRunId && (
        <>
          <section>
            <h2 className="mb-3 text-sm font-medium text-slate-700 dark:text-slate-200">
              总览指标
            </h2>
            <BacktestSummaryCards summary={summary.data} loading={summary.loading} />
          </section>

          <section>
            <EquityCurveChart
              data={equity.data}
              loading={equity.loading}
              error={equity.error}
            />
          </section>

          <section>
            <DrawdownChart
              data={equity.data}
              loading={equity.loading}
              error={equity.error}
            />
          </section>

          <section>
            <div className="mb-3 flex items-center gap-1 rounded-md border border-slate-200 bg-white p-1 dark:border-slate-800 dark:bg-slate-900">
              {tabs.map((t) => (
                <button
                  key={t.key}
                  type="button"
                  onClick={() => setActiveTab(t.key)}
                  className={`rounded px-3 py-1.5 text-sm transition-colors ${
                    activeTab === t.key
                      ? 'bg-slate-200 text-slate-900 dark:bg-slate-700 dark:text-white'
                      : 'text-slate-500 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'
                  }`}
                >
                  {t.label}
                </button>
              ))}
            </div>

            {activeTab === 'trades' && (
              <TradesTable
                rows={trades.data}
                loading={trades.loading}
                error={trades.error}
              />
            )}
            {activeTab === 'risk' && (
              <RiskDistribution
                trades={trades.data}
                loading={trades.loading}
                error={trades.error}
              />
            )}
            {activeTab === 'skipped' && (
              <SkippedTable
                rows={skipped.data}
                loading={skipped.loading}
                error={skipped.error}
              />
            )}
            {activeTab === 'config' && (
              <ConfigSnapshot
                config={config.data?.config ?? null}
                loading={config.loading}
                error={config.error}
              />
            )}
          </section>
        </>
      )}
    </div>
  );
}
