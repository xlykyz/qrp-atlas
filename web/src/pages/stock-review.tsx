import { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { getDailyByDate, getDailyByTicker } from '@/api/daily'
import type { DailyRow } from '@/types'
import {
  createChart,
  ColorType,
  CrosshairMode,
  LineStyle,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type LineData,
  type HistogramData,
  type Time,
} from 'lightweight-charts'

// ── helpers ──

function formatDate(ymd: string): string {
  if (ymd.length === 8) {
    return `${ymd.slice(0, 4)}-${ymd.slice(4, 6)}-${ymd.slice(6, 8)}`
  }
  return ymd
}

function toChartTime(ymd: string): string {
  return `${ymd.slice(0, 4)}-${ymd.slice(4, 6)}-${ymd.slice(6, 8)}`
}

function formatPct(v: number | null | undefined): string {
  if (v == null) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(2)}%`
}

function formatVolume(v: number | null | undefined): string {
  if (v == null) return '—'
  if (v >= 1e8) return `${(v / 1e8).toFixed(2)}亿`
  if (v >= 1e4) return `${(v / 1e4).toFixed(0)}万`
  return v.toLocaleString()
}

function pctColor(v: number | null | undefined): string {
  if (v == null) return 'text-gray-400'
  if (v > 0) return 'text-red-500'
  if (v < 0) return 'text-green-500'
  return 'text-gray-400'
}

function formatAmount(v: number | null | undefined): string {
  if (v == null) return '—'
  return `${(v / 1e8).toFixed(2)}亿`
}

function calcMA(
  data: { time: Time; close: number }[],
  period: number,
): LineData[] {
  return data
    .map((d, i) => {
      if (i < period - 1) return null
      let sum = 0
      for (let j = i - period + 1; j <= i; j++) {
        sum += data[j].close
      }
      return { time: d.time, value: sum / period } as LineData
    })
    .filter((d): d is LineData => d !== null)
}

const MA_CONFIGS = [
  { key: 'ma5', label: 'MA5', period: 5, color: '#3b82f6' },
  { key: 'ma10', label: 'MA10', period: 10, color: '#10b981' },
  { key: 'ma20', label: 'MA20', period: 20, color: '#f97316' },
  { key: 'ma60', label: 'MA60', period: 60, color: '#a855f7' },
  { key: 'ma120', label: 'MA120', period: 120, color: '#ef4444' },
] as const

type MAKey = (typeof MA_CONFIGS)[number]['key']

// ── chart theme constants ──

const BG_COLOR = '#0f172a'
const TEXT_COLOR = '#94a3b8'
const GRID_COLOR = '#1e293b'
const BORDER_COLOR = '#334155'
const UP_COLOR = '#22c55e'
const DOWN_COLOR = '#ef4444'
const WICK_UP_COLOR = '#22c55e'
const WICK_DOWN_COLOR = '#ef4444'

// ── component ──

export default function StockReview() {
  // ── state: search ──

  const [searchQuery, setSearchQuery] = useState('')
  const [searching, setSearching] = useState(false)
  const [searchResults, setSearchResults] = useState<DailyRow[]>([])
  const [selectedStock, setSelectedStock] = useState<{
    ticker: string
    name: string
  } | null>(null)

  // ── state: date range ──

  const now = useMemo(() => new Date(), [])
  const todayYMD = useMemo(
    () =>
      `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}`,
    [now],
  )
  const defaultStart = useMemo(() => {
    const d = new Date(now)
    d.setDate(d.getDate() - 90)
    return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`
  }, [now])

  const [startDate, setStartDate] = useState(defaultStart)
  const [endDate, setEndDate] = useState(todayYMD)

  // ── state: data ──

  const [chartData, setChartData] = useState<DailyRow[]>([])
  const [dataLoading, setDataLoading] = useState(false)
  const [dataError, setDataError] = useState<string | null>(null)

  // ── state: MA visibility ──

  const [visibleMAs, setVisibleMAs] = useState<Set<MAKey>>(
    new Set(['ma5', 'ma20', 'ma60']),
  )

  // ── refs ──

  const mainChartRef = useRef<HTMLDivElement>(null)
  const volumeChartRef = useRef<HTMLDivElement>(null)
  const pctChartRef = useRef<HTMLDivElement>(null)

  const mainChart = useRef<IChartApi | null>(null)
  const volumeChart = useRef<IChartApi | null>(null)
  const pctChart = useRef<IChartApi | null>(null)

  const candleSeries = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const volumeSeries = useRef<ISeriesApi<'Histogram'> | null>(null)
  const pctSeries = useRef<ISeriesApi<'Line'> | null>(null)

  const maSeries = useRef<Map<MAKey, ISeriesApi<'Line'>>>(new Map())

  const isSyncing = useRef(false)

  // ── search logic ──

  const handleSearch = useCallback(async () => {
    const q = searchQuery.trim()
    if (!q) return

    setSearching(true)
    setSearchResults([])
    try {
      // Fetch latest trading day's data
      const rows = await getDailyByDate(todayYMD)
      const lowerQ = q.toLowerCase()
      const matched = rows.filter(
        (r) =>
          r.ticker.toLowerCase().includes(lowerQ) ||
          (r.name && r.name.toLowerCase().includes(lowerQ)),
      )
      setSearchResults(matched.slice(0, 20))
    } catch (err) {
      // Fallback: try a few recent dates
      try {
        const fallbackDate = new Date(now)
        fallbackDate.setDate(fallbackDate.getDate() - 1)
        const fd = `${fallbackDate.getFullYear()}${String(fallbackDate.getMonth() + 1).padStart(2, '0')}${String(fallbackDate.getDate()).padStart(2, '0')}`
        const rows = await getDailyByDate(fd)
        const lowerQ = q.toLowerCase()
        const matched = rows.filter(
          (r) =>
            r.ticker.toLowerCase().includes(lowerQ) ||
            (r.name && r.name.toLowerCase().includes(lowerQ)),
        )
        setSearchResults(matched.slice(0, 20))
      } catch {
        setSearchResults([])
      }
    } finally {
      setSearching(false)
    }
  }, [searchQuery, todayYMD, now])

  const handleSelectStock = useCallback(
    (ticker: string, name: string) => {
      setSelectedStock({ ticker, name })
      setSearchResults([])
      setSearchQuery(`${ticker} - ${name}`)
    },
    [],
  )

  // ── fetch chart data ──

  useEffect(() => {
    if (!selectedStock) return

    let cancelled = false

    async function fetchData() {
      setDataLoading(true)
      setDataError(null)
      try {
        const rows = await getDailyByTicker(
          selectedStock.ticker,
          startDate,
          endDate,
        )
        if (!cancelled) {
          // Sort ascending by trade_date for charting
          rows.sort((a, b) => a.trade_date.localeCompare(b.trade_date))
          setChartData(rows)
        }
      } catch (err) {
        if (!cancelled) {
          setDataError(
            err instanceof Error ? err.message : '数据加载失败',
          )
          setChartData([])
        }
      } finally {
        if (!cancelled) setDataLoading(false)
      }
    }

    fetchData()
    return () => {
      cancelled = true
    }
  }, [selectedStock, startDate, endDate])

  // ── chart creation and data population ──

  const chartReady = useMemo(
    () =>
      chartData.length > 0 &&
      !dataLoading &&
      !dataError &&
      selectedStock !== null,
    [chartData.length, dataLoading, dataError, selectedStock],
  )

  // Prepare chart data
  const candleData = useMemo((): CandlestickData[] => {
    return chartData
      .filter((r) => r.open != null && r.high != null && r.low != null && r.close != null)
      .map((r) => ({
        time: toChartTime(r.trade_date) as Time,
        open: r.open!,
        high: r.high!,
        low: r.low!,
        close: r.close!,
      }))
  }, [chartData])

  const closeData = useMemo(() => {
    return chartData
      .filter((r) => r.close != null)
      .map((r) => ({
        time: toChartTime(r.trade_date) as Time,
        close: r.close!,
      }))
  }, [chartData])

  const volumeData = useMemo((): HistogramData[] => {
    return chartData
      .filter((r) => r.volume != null && r.open != null && r.close != null)
      .map((r) => ({
        time: toChartTime(r.trade_date) as Time,
        value: r.volume!,
        color: r.close! >= r.open! ? UP_COLOR : DOWN_COLOR,
      }))
  }, [chartData])

  const pctData = useMemo((): LineData[] => {
    return chartData
      .filter((r) => r.pct_change != null)
      .map((r) => ({
        time: toChartTime(r.trade_date) as Time,
        value: r.pct_change!,
      }))
  }, [chartData])

  // MA data per period
  const maDataMap = useMemo(() => {
    const map = new Map<MAKey, LineData[]>()
    for (const cfg of MA_CONFIGS) {
      map.set(cfg.key, calcMA(closeData, cfg.period))
    }
    return map
  }, [closeData])

  // Rebuild charts when data changes
  const buildCharts = useCallback(() => {
    if (
      !mainChartRef.current ||
      !volumeChartRef.current ||
      !pctChartRef.current ||
      candleData.length === 0
    ) {
      return
    }

    // Destroy existing charts
    mainChart.current?.remove()
    volumeChart.current?.remove()
    pctChart.current?.remove()
    maSeries.current.clear()

    const containerWidth = mainChartRef.current.clientWidth

    // ── Main chart: K-line + MA ──
    const main = createChart(mainChartRef.current, {
      width: containerWidth,
      height: 400,
      layout: {
        background: { type: ColorType.Solid, color: BG_COLOR },
        textColor: TEXT_COLOR,
      },
      grid: {
        vertLines: { color: GRID_COLOR },
        horzLines: { color: GRID_COLOR },
      },
      borderColor: BORDER_COLOR,
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          color: '#6366f1',
          width: 1,
          style: LineStyle.Dashed,
          labelBackgroundColor: '#6366f1',
        },
        horzLine: {
          color: '#6366f1',
          width: 1,
          style: LineStyle.Dashed,
          labelBackgroundColor: '#6366f1',
        },
      },
      timeScale: {
        borderColor: BORDER_COLOR,
        timeVisible: false,
        secondsVisible: false,
      },
      rightPriceScale: {
        borderColor: BORDER_COLOR,
      },
    })

    const candles = main.addCandlestickSeries({
      upColor: UP_COLOR,
      downColor: DOWN_COLOR,
      borderUpColor: WICK_UP_COLOR,
      borderDownColor: WICK_DOWN_COLOR,
      wickUpColor: WICK_UP_COLOR,
      wickDownColor: WICK_DOWN_COLOR,
    })
    candles.setData(candleData)
    candleSeries.current = candles

    // Add MA lines
    for (const cfg of MA_CONFIGS) {
      const data = maDataMap.get(cfg.key) ?? []
      if (data.length === 0) continue
      const series = main.addLineSeries({
        color: cfg.color,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: true,
        title: cfg.label,
        visible: visibleMAs.has(cfg.key),
      })
      series.setData(data)
      maSeries.current.set(cfg.key, series)
    }

    mainChart.current = main

    // ── Volume chart ──
    const vol = createChart(volumeChartRef.current, {
      width: containerWidth,
      height: 120,
      layout: {
        background: { type: ColorType.Solid, color: BG_COLOR },
        textColor: TEXT_COLOR,
      },
      grid: {
        vertLines: { color: GRID_COLOR },
        horzLines: { color: GRID_COLOR },
      },
      borderColor: BORDER_COLOR,
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          color: '#6366f1',
          width: 1,
          style: LineStyle.Dashed,
          labelBackgroundColor: '#6366f1',
        },
        horzLine: {
          color: '#6366f1',
          width: 1,
          style: LineStyle.Dashed,
          labelBackgroundColor: '#6366f1',
        },
      },
      timeScale: {
        borderColor: BORDER_COLOR,
        visible: false,
      },
      rightPriceScale: {
        borderColor: BORDER_COLOR,
      },
    })

    const hist = vol.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'right',
    })
    hist.setData(volumeData)
    volumeSeries.current = hist
    volumeChart.current = vol

    // ── Pct change chart ──
    const pct = createChart(pctChartRef.current, {
      width: containerWidth,
      height: 80,
      layout: {
        background: { type: ColorType.Solid, color: BG_COLOR },
        textColor: TEXT_COLOR,
      },
      grid: {
        vertLines: { color: GRID_COLOR },
        horzLines: { color: GRID_COLOR },
      },
      borderColor: BORDER_COLOR,
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          color: '#6366f1',
          width: 1,
          style: LineStyle.Dashed,
          labelBackgroundColor: '#6366f1',
        },
        horzLine: {
          color: '#6366f1',
          width: 1,
          style: LineStyle.Dashed,
          labelBackgroundColor: '#6366f1',
        },
      },
      timeScale: {
        borderColor: BORDER_COLOR,
        visible: false,
      },
      rightPriceScale: {
        borderColor: BORDER_COLOR,
      },
    })

    const pctLine = pct.addLineSeries({
      color: '#f59e0b',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: true,
    })
    pctLine.setData(pctData)
    pctSeries.current = pctLine
    pctChart.current = pct

    // ── Crosshair sync: sync visible range between charts ──
    const syncCharts = (source: IChartApi) => {
      if (isSyncing.current) return
      const range = source.timeScale().getVisibleLogicalRange()
      if (!range) return
      isSyncing.current = true
      const targets = [main, vol, pct].filter((c) => c !== source)
      for (const c of targets) {
        c.timeScale().setVisibleLogicalRange(range)
      }
      isSyncing.current = false
    }

    main.timeScale().subscribeVisibleLogicalRangeChange(() => syncCharts(main))
    vol.timeScale().subscribeVisibleLogicalRangeChange(() => syncCharts(vol))
    pct.timeScale().subscribeVisibleLogicalRangeChange(() => syncCharts(pct))

    // Fit content
    main.timeScale().fitContent()
  }, [candleData, closeData, volumeData, pctData, maDataMap, visibleMAs])

  // Build/rebuild charts when data changes
  useEffect(() => {
    buildCharts()

    return () => {
      mainChart.current?.remove()
      volumeChart.current?.remove()
      pctChart.current?.remove()
      mainChart.current = null
      volumeChart.current = null
      pctChart.current = null
      maSeries.current.clear()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [candleData, closeData, volumeData, pctData, maDataMap])

  // Update MA visibility without rebuilding
  useEffect(() => {
    for (const cfg of MA_CONFIGS) {
      const series = maSeries.current.get(cfg.key)
      if (series) {
        series.applyOptions({ visible: visibleMAs.has(cfg.key) })
      }
    }
  }, [visibleMAs])

  // Resize observer for chart containers
  useEffect(() => {
    const container = mainChartRef.current
    if (!container) return

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width } = entry.contentRect
        const w = Math.floor(width)
        mainChart.current?.resize(w, 400)
        volumeChart.current?.resize(w, 120)
        pctChart.current?.resize(w, 80)
      }
    })
    observer.observe(container)

    return () => observer.disconnect()
  }, [])

  // Fallback: rebuild on initial mount when container size is ready
  useEffect(() => {
    if (!chartReady) return
    const timer = setTimeout(() => {
      buildCharts()
    }, 100)
    return () => clearTimeout(timer)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [chartReady])

  // ── MA toggle ──

  const toggleMA = useCallback((key: MAKey) => {
    setVisibleMAs((prev) => {
      const next = new Set(prev)
      if (next.has(key)) {
        next.delete(key)
      } else {
        next.add(key)
      }
      return next
    })
  }, [])

  // ── info panel data ──

  const latestRow = useMemo(
    () => (chartData.length > 0 ? chartData[chartData.length - 1] : null),
    [chartData],
  )

  const highInRange = useMemo(
    () =>
      chartData.reduce(
        (max, r) => (r.high != null && r.high > max ? r.high : max),
        -Infinity,
      ),
    [chartData],
  )
  const lowInRange = useMemo(
    () =>
      chartData.reduce(
        (min, r) => (r.low != null && r.low < min ? r.low : min),
        Infinity,
      ),
    [chartData],
  )

  // ── render ──

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-bold">个股复盘</h1>

      {/* ── Search bar ── */}
      <Card className="border-slate-800 bg-slate-900/50">
        <CardContent className="p-4">
          <div className="flex items-end gap-3">
            <div className="flex-1 relative">
              <Input
                placeholder="输入股票代码或名称..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value)
                  setSearchResults([])
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') handleSearch()
                }}
                className="border-slate-700 bg-slate-800 text-white placeholder:text-slate-500"
              />
              {/* Search dropdown */}
              {searchResults.length > 0 && (
                <div className="absolute z-50 mt-1 w-full rounded-lg border border-slate-700 bg-slate-800 shadow-lg max-h-60 overflow-y-auto">
                  {searchResults.map((r) => (
                    <button
                      key={r.ticker}
                      className="w-full px-3 py-2 text-left text-sm text-gray-200 hover:bg-slate-700 transition-colors flex items-center gap-2"
                      onClick={() =>
                        handleSelectStock(r.ticker, r.name ?? r.ticker)
                      }
                    >
                      <span className="font-mono text-xs text-slate-400">
                        {r.ticker}
                      </span>
                      <span>{r.name ?? '—'}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
            <Button
              onClick={handleSearch}
              disabled={searching || !searchQuery.trim()}
              className="bg-slate-700 hover:bg-slate-600 text-white"
            >
              {searching ? '搜索中...' : '搜索'}
            </Button>
          </div>

          {/* Selected stock display */}
          {selectedStock && (
            <div className="mt-3 text-sm text-slate-300">
              已选择：<span className="font-mono text-white">{selectedStock.ticker}</span>
              {' - '}
              <span className="text-white">{selectedStock.name}</span>
            </div>
          )}
        </CardContent>
      </Card>

      {/* ── Date range ── */}
      {selectedStock && (
        <Card className="border-slate-800 bg-slate-900/50">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <label className="text-sm text-slate-400">开始日期</label>
              <Input
                type="date"
                value={formatDate(startDate)}
                onChange={(e) => {
                  const v = e.target.value.replace(/-/g, '')
                  setStartDate(v)
                }}
                className="w-40 border-slate-700 bg-slate-800 text-white"
              />
              <label className="text-sm text-slate-400">结束日期</label>
              <Input
                type="date"
                value={formatDate(endDate)}
                onChange={(e) => {
                  const v = e.target.value.replace(/-/g, '')
                  setEndDate(v)
                }}
                className="w-40 border-slate-700 bg-slate-800 text-white"
              />
            </div>
          </CardContent>
        </Card>
      )}

      {/* ── Initial state ── */}
      {!selectedStock && (
        <div className="flex items-center justify-center py-20">
          <p className="text-lg text-slate-400">
            请搜索并选择一只股票
          </p>
        </div>
      )}

      {/* ── Data loading ── */}
      {selectedStock && dataLoading && (
        <div className="flex items-center justify-center py-20">
          <p className="text-lg text-slate-400">正在加载数据...</p>
        </div>
      )}

      {/* ── Error state ── */}
      {selectedStock && dataError && !dataLoading && (
        <div className="flex flex-col items-center justify-center py-20 gap-4">
          <p className="text-red-400">{dataError}</p>
          <p className="text-sm text-slate-500">
            请确认后端服务是否运行，或调整日期范围重试
          </p>
        </div>
      )}

      {/* ── Empty data ── */}
      {selectedStock &&
        !dataLoading &&
        !dataError &&
        chartData.length === 0 && (
          <div className="flex items-center justify-center py-20">
            <p className="text-lg text-slate-400">
              未找到该股票的数据
            </p>
          </div>
        )}

      {/* ── Charts area ── */}
      {chartReady && (
        <>
          {/* MA toggle buttons */}
          <div className="flex items-center gap-2 flex-wrap">
            {MA_CONFIGS.map((cfg) => {
              const active = visibleMAs.has(cfg.key)
              return (
                <button
                  key={cfg.key}
                  onClick={() => toggleMA(cfg.key)}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors border ${
                    active
                      ? 'bg-slate-700 text-white border-slate-600'
                      : 'bg-transparent text-slate-500 border-slate-700 hover:text-slate-300 hover:border-slate-500'
                  }`}
                  style={
                    active
                      ? { borderColor: cfg.color, color: cfg.color }
                      : undefined
                  }
                >
                  {cfg.label}
                </button>
              )
            })}
          </div>

          {/* Info panel */}
          {latestRow && (
            <Card className="border-slate-800 bg-slate-900/50">
              <CardContent className="p-4">
                <div className="flex items-center gap-6 flex-wrap">
                  <div>
                    <span className="text-sm text-slate-400">最新价</span>
                    <span className="ml-2 text-3xl font-bold text-white">
                      {latestRow.close?.toFixed(2) ?? '—'}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-400">涨跌幅</span>
                    <span
                      className={`ml-2 text-xl font-semibold ${pctColor(latestRow.pct_change)}`}
                    >
                      {formatPct(latestRow.pct_change)}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-400">区间最高</span>
                    <span className="ml-2 text-lg font-medium text-white">
                      {highInRange !== -Infinity
                        ? highInRange.toFixed(2)
                        : '—'}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-400">区间最低</span>
                    <span className="ml-2 text-lg font-medium text-white">
                      {lowInRange !== Infinity
                        ? lowInRange.toFixed(2)
                        : '—'}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-400">成交量</span>
                    <span className="ml-2 text-lg font-medium text-white">
                      {formatVolume(latestRow.volume)}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-400">成交额</span>
                    <span className="ml-2 text-lg font-medium text-white">
                      {formatAmount(latestRow.amount)}
                    </span>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Chart containers */}
          <div className="space-y-0">
            <div
              ref={mainChartRef}
              className="w-full rounded-t-lg border border-slate-800 bg-slate-900/50 overflow-hidden"
              style={{ height: 400 }}
            />
            <div
              ref={volumeChartRef}
              className="w-full border-l border-r border-slate-800 bg-slate-900/50 overflow-hidden"
              style={{ height: 120 }}
            />
            <div
              ref={pctChartRef}
              className="w-full rounded-b-lg border border-slate-800 bg-slate-900/50 overflow-hidden"
              style={{ height: 80 }}
            />
          </div>
        </>
      )}
    </div>
  )
}
