import { useEffect, useRef } from 'react';
import {
  createChart,
  LineSeries,
  type IChartApi,
  type ISeriesApi,
  type Time,
} from 'lightweight-charts';
import type { EquityPoint } from '@/types/backtest';

interface Props {
  data: EquityPoint[] | null;
  loading?: boolean;
  error?: string | null;
}

const CHART_HEIGHT = 240;

export function DrawdownChart({ data, loading, error }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Line'> | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      height: CHART_HEIGHT,
      width: containerRef.current.clientWidth,
      layout: {
        background: { color: 'transparent' },
        textColor: '#94a3b8',
      },
      grid: {
        vertLines: { color: 'rgba(148, 163, 184, 0.15)' },
        horzLines: { color: 'rgba(148, 163, 184, 0.15)' },
      },
      rightPriceScale: {
        borderColor: 'rgba(148, 163, 184, 0.3)',
      },
      timeScale: {
        borderColor: 'rgba(148, 163, 184, 0.3)',
        timeVisible: false,
      },
      crosshair: {
        mode: 0,
      },
    });
    const series = chart.addSeries(LineSeries, {
      color: '#dc2626',
      lineWidth: 2,
    });
    chartRef.current = chart;
    seriesRef.current = series;

    const ro = new ResizeObserver(() => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: containerRef.current.clientWidth });
      }
    });
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!seriesRef.current) return;
    if (!data || data.length === 0) {
      seriesRef.current.setData([]);
      return;
    }
    const points = data.map((p) => ({
      time: p.date as Time,
      value: p.drawdown_pct,
    }));
    seriesRef.current.setData(points);
    chartRef.current?.timeScale().fitContent();
  }, [data]);

  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-900/50">
      <p className="mb-3 text-sm font-medium text-slate-700 dark:text-slate-200">回撤曲线</p>
      <div className="relative" style={{ height: CHART_HEIGHT }}>
        <div ref={containerRef} style={{ height: '100%', width: '100%' }} />
        {error ? (
          <div className="absolute inset-0 flex items-center justify-center bg-slate-50 text-sm text-red-600 dark:bg-slate-900/50 dark:text-red-400">
            {error}
          </div>
        ) : loading ? (
          <div className="absolute inset-0 flex items-center justify-center bg-slate-50 text-sm text-slate-500 dark:bg-slate-900/50 dark:text-slate-400">
            加载回撤数据...
          </div>
        ) : !data || data.length === 0 ? (
          <div className="absolute inset-0 flex items-center justify-center bg-slate-50 text-sm text-slate-500 dark:bg-slate-900/50 dark:text-slate-400">
            暂无回撤数据
          </div>
        ) : null}
      </div>
    </div>
  );
}
