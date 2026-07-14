import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  getResultConfig,
  getResultSummary,
  listResultRuns,
} from '@/api/backtest-result';
import type { BacktestConfigSnapshot, BacktestRun, BacktestSummary } from '@/types/backtest';
import {
  buildConfigDiff,
  COMPARE_METRICS,
  pickSummaryValue,
  type CompareMetricKey,
} from '../lib/compare';

export interface CompareRunData {
  runId: string;
  meta: BacktestRun | null;
  summary: BacktestSummary | null;
  config: BacktestConfigSnapshot | null;
  error: string | null;
  loading: boolean;
}

export function useRunCompare(initialSelected: string[] = []) {
  const [availableRuns, setAvailableRuns] = useState<BacktestRun[]>([]);
  const [runsLoading, setRunsLoading] = useState(true);
  const [runsError, setRunsError] = useState<string | null>(null);
  const [selectedRunIds, setSelectedRunIds] = useState<string[]>(initialSelected);
  const [details, setDetails] = useState<Record<string, CompareRunData>>({});

  useEffect(() => {
    let cancelled = false;
    setRunsLoading(true);
    listResultRuns()
      .then((list) => {
        if (cancelled) return;
        setAvailableRuns(list.filter((r) => ['success', 'succeeded', 'completed'].includes(String(r.status))));
        setRunsLoading(false);
      })
      .catch((err) => {
        if (cancelled) return;
        setRunsError(err instanceof Error ? err.message : '加载 run 列表失败');
        setRunsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Load detail for selected runs
  useEffect(() => {
    let cancelled = false;

    selectedRunIds.forEach((runId) => {
      const existing = details[runId];
      if (existing && !existing.loading && existing.summary) return;

      setDetails((prev) => ({
        ...prev,
        [runId]: {
          runId,
          meta: availableRuns.find((r) => r.run_id === runId) ?? prev[runId]?.meta ?? null,
          summary: prev[runId]?.summary ?? null,
          config: prev[runId]?.config ?? null,
          error: null,
          loading: true,
        },
      }));

      Promise.all([getResultSummary(runId), getResultConfig(runId)])
        .then(([summary, config]) => {
          if (cancelled) return;
          setDetails((prev) => ({
            ...prev,
            [runId]: {
              runId,
              meta: availableRuns.find((r) => r.run_id === runId) ?? prev[runId]?.meta ?? null,
              summary,
              config,
              error: null,
              loading: false,
            },
          }));
        })
        .catch((err) => {
          if (cancelled) return;
          setDetails((prev) => ({
            ...prev,
            [runId]: {
              runId,
              meta: prev[runId]?.meta ?? null,
              summary: null,
              config: null,
              error: err instanceof Error ? err.message : '加载失败',
              loading: false,
            },
          }));
        });
    });

    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- only re-fetch when selection changes
  }, [selectedRunIds, availableRuns]);

  const toggleRun = useCallback((runId: string) => {
    setSelectedRunIds((prev) => {
      if (prev.includes(runId)) return prev.filter((id) => id !== runId);
      if (prev.length >= 4) return prev;
      return [...prev, runId];
    });
  }, []);

  const setRuns = useCallback((ids: string[]) => {
    setSelectedRunIds(ids.slice(0, 4));
  }, []);

  const orderedDetails = useMemo(
    () => selectedRunIds.map((id) => details[id]).filter(Boolean) as CompareRunData[],
    [selectedRunIds, details],
  );

  const metricRows = useMemo(() => {
    return COMPARE_METRICS.map((m) => ({
      ...m,
      values: orderedDetails.map((d) => pickSummaryValue(d.summary, m.key as CompareMetricKey)),
    }));
  }, [orderedDetails]);

  const configDiff = useMemo(
    () => buildConfigDiff(orderedDetails.map((d) => d.config)),
    [orderedDetails],
  );

  return {
    availableRuns,
    runsLoading,
    runsError,
    selectedRunIds,
    toggleRun,
    setRuns,
    orderedDetails,
    metricRows,
    configDiff,
  };
}
