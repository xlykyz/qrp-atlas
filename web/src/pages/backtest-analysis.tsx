import { useEffect, useMemo, useState } from 'react';
import { Link, useOutletContext, useSearchParams } from 'react-router-dom';
import { BacktestRunSelector } from '@/components/backtest/backtest-run-selector';
import { BacktestSummaryCards } from '@/components/backtest/backtest-summary-cards';
import { ConfigSnapshot } from '@/components/backtest/config-snapshot';
import { DrawdownChart } from '@/components/backtest/drawdown-chart';
import { EquityCurveChart } from '@/components/backtest/equity-curve-chart';
import { RiskDistribution } from '@/components/backtest/risk-distribution';
import { SkippedTable } from '@/components/backtest/skipped-table';
import { TradesTable } from '@/components/backtest/trades-table';
import {
  BenchmarkPanel,
  ExposuresPanel,
  ReproPanel,
  RollingPanel,
} from '@/components/backtest/product-analytics-panels';
import {
  getBacktestBenchmark,
  getBacktestConfig,
  getBacktestEquity,
  getBacktestExposures,
  getBacktestReproducibility,
  getBacktestRolling,
  getBacktestRuns,
  getBacktestSkipped,
  getBacktestSummary,
  getBacktestTrades,
} from '@/api/backtest';
import {
  getResultConfig,
  getResultEquity,
  getResultSkipped,
  getResultSummary,
  getResultTrades,
  listResultRuns,
} from '@/api/backtest-result';
import { buttonVariants } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import type {
  BacktestConfigSnapshot,
  BacktestRun,
  BacktestSummary,
  BacktestTrade,
  BenchmarkArtifact,
  EquityPoint,
  ExposureArtifact,
  ReproducibilityArtifact,
  RollingPerformancePoint,
  SkippedTrade,
} from '@/types/backtest';

type Tab = 'trades' | 'risk' | 'skipped' | 'config' | 'analytics';

type ResultState<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

function initialState<T>(): ResultState<T> {
  return { data: null, loading: false, error: null };
}

type DataSource = 'http' | 'mock';

export default function BacktestAnalysis() {
  const { setPageTitle, setHeaderControls } = useOutletContext<{
    setPageTitle: (t: string) => void;
    setHeaderControls: (c: React.ReactNode | null) => void;
  }>();
  const [searchParams] = useSearchParams();
  const queryRunId = searchParams.get('runId');
  const querySource = (searchParams.get('source') === 'mock' ? 'mock' : 'http') as DataSource;

  const [source, setSource] = useState<DataSource>(querySource);
  const [runs, setRuns] = useState<BacktestRun[]>([]);
  const [runsLoading, setRunsLoading] = useState(true);
  const [runsError, setRunsError] = useState<string | null>(null);

  const [selectedRunId, setSelectedRunId] = useState<string | null>(queryRunId);
  const [activeTab, setActiveTab] = useState<Tab>('trades');

  const [summary, setSummary] = useState<ResultState<BacktestSummary>>(initialState());
  const [equity, setEquity] = useState<ResultState<EquityPoint[]>>(initialState());
  const [trades, setTrades] = useState<ResultState<BacktestTrade[]>>(initialState());
  const [skipped, setSkipped] = useState<ResultState<SkippedTrade[]>>(initialState());
  const [config, setConfig] = useState<ResultState<BacktestConfigSnapshot>>(initialState());
  const [rolling, setRolling] = useState<ResultState<RollingPerformancePoint[]>>(initialState());
  const [benchmark, setBenchmark] = useState<ResultState<BenchmarkArtifact>>(initialState());
  const [exposures, setExposures] = useState<ResultState<ExposureArtifact>>(initialState());
  const [repro, setRepro] = useState<ResultState<ReproducibilityArtifact>>(initialState());

  useEffect(() => {
    setPageTitle('回测分析');
    return () => {
      setPageTitle('');
      setHeaderControls(null);
    };
  }, [setPageTitle, setHeaderControls]);

  // Sync source/run from URL when navigating from workflow
  useEffect(() => {
    if (querySource) setSource(querySource);
    if (queryRunId) setSelectedRunId(queryRunId);
  }, [queryRunId, querySource]);

  useEffect(() => {
    let cancelled = false;
    setRunsLoading(true);
    setRunsError(null);

    const loader = source === 'mock' ? listResultRuns : getBacktestRuns;

    loader()
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
  }, [source]);

  useEffect(() => {
    if (selectedRunId) return;
    if (runs.length === 0) return;
    setSelectedRunId(runs[0].run_id);
  }, [runs, selectedRunId]);

  const sourceLabel = useMemo(
    () => (source === 'mock' ? 'Mock fixtures' : 'HTTP API'),
    [source],
  );

  useEffect(() => {
    setHeaderControls(
      <div className="flex flex-wrap items-center gap-2">
        <Link
          to="/backtest/workflow"
          className={cn(buttonVariants({ variant: 'outline', size: 'sm' }))}
        >
          策略回测工作流
        </Link>
        <select
          className="h-8 rounded-md border border-slate-200 bg-white px-2 text-xs dark:border-slate-700 dark:bg-slate-900"
          value={source}
          onChange={(e) => {
            setSource(e.target.value as DataSource);
            setSelectedRunId(null);
          }}
          title="结果数据源"
        >
          <option value="http">HTTP API</option>
          <option value="mock">Mock fixtures</option>
        </select>
        <span className="hidden text-[11px] text-slate-400 sm:inline">{sourceLabel}</span>
        {runsLoading ? (
          <span className="text-sm text-slate-400">加载 runs...</span>
        ) : (
          <BacktestRunSelector
            runs={runs}
            selectedRunId={selectedRunId}
            onSelect={setSelectedRunId}
          />
        )}
      </div>,
    );
    return () => {
      setHeaderControls(null);
    };
  }, [runs, runsLoading, selectedRunId, setHeaderControls, source, sourceLabel]);

  useEffect(() => {
    if (!selectedRunId) return;
    let cancelled = false;

    const api =
      source === 'mock'
        ? {
            summary: () => getResultSummary(selectedRunId),
            equity: () => getResultEquity(selectedRunId),
            trades: () => getResultTrades(selectedRunId),
            skipped: () => getResultSkipped(selectedRunId),
            config: () => getResultConfig(selectedRunId),
          }
        : {
            summary: () => getBacktestSummary(selectedRunId),
            equity: () => getBacktestEquity(selectedRunId),
            trades: () => getBacktestTrades(selectedRunId),
            skipped: () => getBacktestSkipped(selectedRunId),
            config: () => getBacktestConfig(selectedRunId),
            rolling: () => getBacktestRolling(selectedRunId),
            benchmark: () => getBacktestBenchmark(selectedRunId),
            exposures: () => getBacktestExposures(selectedRunId),
            repro: () => getBacktestReproducibility(selectedRunId),
          };

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

    const jobs = [
      fetchOne(api.summary, setSummary),
      fetchOne(api.equity, setEquity),
      fetchOne(api.trades, setTrades),
      fetchOne(api.skipped, setSkipped),
      fetchOne(api.config, setConfig),
    ];
    if (source === 'http') {
      jobs.push(
        fetchOne(api.rolling!, setRolling),
        fetchOne(api.benchmark!, setBenchmark),
        fetchOne(api.exposures!, setExposures),
        fetchOne(api.repro!, setRepro),
      );
    } else {
      setRolling({ data: null, loading: false, error: null });
      setBenchmark({ data: null, loading: false, error: null });
      setExposures({ data: null, loading: false, error: null });
      setRepro({ data: null, loading: false, error: null });
    }
    Promise.all(jobs);

    return () => {
      cancelled = true;
    };
  }, [selectedRunId, source]);

  const tabs: { key: Tab; label: string }[] = [
    { key: 'trades', label: '交易明细' },
    { key: 'risk', label: '风险分布' },
    { key: 'skipped', label: 'Skipped 记录' },
    { key: 'config', label: '配置快照' },
    { key: 'analytics', label: '分析扩展' },
  ];

  return (
    <div className="space-y-6">
      {runsError && (
        <div className="rounded-lg border border-red-300 bg-red-50 p-3 text-sm text-red-600 dark:border-red-800 dark:bg-red-950/30 dark:text-red-400">
          加载 runs 列表失败：{runsError}
          {source === 'http' ? (
            <span className="mt-1 block text-xs">
              可切换右上角数据源为 Mock fixtures，或确认后端
              /api/backtest 可用。
            </span>
          ) : null}
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
            {activeTab === 'analytics' && (
              <div className="space-y-4">
                <BenchmarkPanel data={benchmark.data} loading={benchmark.loading} error={benchmark.error} />
                <RollingPanel data={rolling.data} loading={rolling.loading} error={rolling.error} />
                <ExposuresPanel data={exposures.data} loading={exposures.loading} error={exposures.error} />
                <ReproPanel data={repro.data} loading={repro.loading} error={repro.error} />
              </div>
            )}
          </section>
        </>
      )}
    </div>
  );
}
