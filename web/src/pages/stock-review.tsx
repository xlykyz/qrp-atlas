import { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { getDailyByDate, getDailyByTicker, type AdjustmentMode } from '@/api/daily'
import { getPhase, createPhase } from '@/api/phase'
import { getTrades, createTrade, updateTrade } from '@/api/trades'
import { getStockList, type StockInfo } from '@/api/stock'
import { pinyin as pinyinPro } from 'pinyin-pro'
import Slider from 'rc-slider'
import 'rc-slider/assets/index.css'
import type { DailyRow, PhaseRecord, PhaseWrite, TradeRecord, TradeWrite, TradePatch } from '@/types'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  createChart,
  CandlestickSeries,
  LineSeries,
  HistogramSeries,
  ColorType,
  CrosshairMode,
  LineStyle,
  createSeriesMarkers,
  type IChartApi,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  type CandlestickData,
  type LineData,
  type HistogramData,
  type Time,
  type SeriesMarker,
  type IPriceLine,
  type MouseEventParams,
  type SeriesType,
  type LogicalRange,
  type PriceFormat,
} from 'lightweight-charts'

// ── helpers ──

function formatDate(ymd: string): string {
  if (ymd.length === 8) {
    return `${ymd.slice(0, 4)}-${ymd.slice(4, 6)}-${ymd.slice(6, 8)}`
  }
  return ymd
}

function toChartTime(ymd: string): string {
  if (ymd.includes('-')) return ymd // already YYYY-MM-DD
  return `${ymd.slice(0, 4)}-${ymd.slice(4, 6)}-${ymd.slice(6, 8)}`
}

function normalizeChartTime(time: Time | null): string | null {
  if (time == null) return null
  if (typeof time === 'string') return time
  if (typeof time === 'number') {
    return new Date(time * 1000).toISOString().slice(0, 10)
  }
  return `${time.year}-${String(time.month).padStart(2, '0')}-${String(time.day).padStart(2, '0')}`
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

type MAKey = string
type SubIndicatorKey = 'volume' | 'amount' | 'pct_change' | 'turnover' | 'rsi' | 'macd'
type SubChartSingleSeries = ISeriesApi<'Histogram'> | ISeriesApi<'Line'>
type SubChartSeries = ISeriesApi<SeriesType>
type SubChartSeriesBundle = {
  single: SubChartSingleSeries | null
  macdLine: ISeriesApi<'Line'> | null
  signalLine: ISeriesApi<'Line'> | null
  histogram: ISeriesApi<'Histogram'> | null
}

const SUB_INDICATOR_OPTIONS: { value: SubIndicatorKey; label: string }[] = [
  { value: 'volume', label: '成交量' },
  { value: 'amount', label: '成交额' },
  { value: 'pct_change', label: '涨跌幅' },
  { value: 'turnover', label: '换手率' },
  { value: 'rsi', label: 'RSI' },
  { value: 'macd', label: 'MACD' },
]

function getSubIndicatorLabel(indicator: SubIndicatorKey): string {
  return SUB_INDICATOR_OPTIONS.find((opt) => opt.value === indicator)?.label ?? indicator
}

function clearAllSeries(chart: IChartApi, seriesRefs: (SubChartSeries | null)[]) {
  for (const s of seriesRefs) {
    if (s) {
      try { chart.removeSeries(s) } catch {
        // Series may already be detached during chart rebuild.
      }
    }
  }
}

function truncateAxisLabel(label: string): string {
  return label.length > 8 ? label.slice(0, 7) + '...' : label
}

function formatCompactAxisNumber(value: number): string {
  const abs = Math.abs(value)
  if (abs >= 1e8) return (value / 1e8).toFixed(2) + '亿'
  if (abs >= 1e4) return (value / 1e4).toFixed(1) + '万'
  if (abs >= 1000) return value.toFixed(0)
  if (abs >= 100) return value.toFixed(1)
  return value.toFixed(2)
}

function boundedSubIndicatorPriceFormat(indicator: SubIndicatorKey): PriceFormat {
  if (indicator === 'volume') {
    return {
      type: 'custom',
      formatter: (price) => truncateAxisLabel(formatCompactAxisNumber(price)),
      minMove: 1,
    }
  }
  if (indicator === 'amount') {
    return {
      type: 'custom',
      formatter: (price) => price.toFixed(2) + '亿',
      minMove: 0.01,
    }
  }
  if (indicator === 'pct_change' || indicator === 'turnover') {
    return {
      type: 'custom',
      formatter: (price) => price.toFixed(2) + '%',
      minMove: 0.01,
    }
  }
  return {
    type: 'custom',
    formatter: (price) => truncateAxisLabel(price.toFixed(2)),
    minMove: 0.01,
  }
}
function getSubChartCrosshairSeries(
  single: SubChartSingleSeries | null,
  macdLine: ISeriesApi<'Line'> | null,
  signalLine: ISeriesApi<'Line'> | null,
  histogram: ISeriesApi<'Histogram'> | null,
): SubChartSeries | null {
  return (single ?? histogram ?? macdLine ?? signalLine) as SubChartSeries | null
}

function syncVisibleLogicalRangeFromMain(
  main: IChartApi | null,
  sub1: IChartApi | null,
  sub2: IChartApi | null,
) {
  const range = main?.timeScale().getVisibleLogicalRange()
  if (!range) return
  sub1?.timeScale().setVisibleLogicalRange(range)
  sub2?.timeScale().setVisibleLogicalRange(range)
}

function setMainVisibleRangeAndSyncSubCharts(
  main: IChartApi | null,
  sub1: IChartApi | null,
  sub2: IChartApi | null,
  range: { from: Time; to: Time },
) {
  main?.timeScale().setVisibleRange(range)
  syncVisibleLogicalRangeFromMain(main, sub1, sub2)
}

function setAllVisibleLogicalRange(
  main: IChartApi | null,
  sub1: IChartApi | null,
  sub2: IChartApi | null,
  range: LogicalRange,
) {
  main?.timeScale().setVisibleLogicalRange(range)
  sub1?.timeScale().setVisibleLogicalRange(range)
  sub2?.timeScale().setVisibleLogicalRange(range)
}
function rebuildSubChart(chart: IChartApi, indicator: SubIndicatorKey): SubChartSeriesBundle {
  if (indicator === 'macd') {
    return {
      single: null,
      histogram: chart.addSeries(HistogramSeries, {
        color: '#22c55e',
        priceLineVisible: false,
        lastValueVisible: false,
        priceFormat: boundedSubIndicatorPriceFormat('macd'),
      }),
      macdLine: chart.addSeries(LineSeries, {
        color: '#3b82f6', lineWidth: 2, priceLineVisible: false,
        lastValueVisible: false, crosshairMarkerVisible: true,
        priceFormat: boundedSubIndicatorPriceFormat('macd'),
      }),
      signalLine: chart.addSeries(LineSeries, {
        color: '#f59e0b', lineWidth: 2, priceLineVisible: false,
        lastValueVisible: false, crosshairMarkerVisible: true,
        priceFormat: boundedSubIndicatorPriceFormat('macd'),
      }),
    }
  }

  if (indicator === 'volume' || indicator === 'amount') {
    return {
      single: chart.addSeries(HistogramSeries, {
        priceLineVisible: false,
        lastValueVisible: false,
        priceFormat: boundedSubIndicatorPriceFormat(indicator),
      }),
      macdLine: null,
      signalLine: null,
      histogram: null,
    }
  }

  const color = indicator === 'rsi' ? '#a855f7' : '#f59e0b'
  return {
    single: chart.addSeries(LineSeries, {
      color, lineWidth: 2, priceLineVisible: false,
      lastValueVisible: false, crosshairMarkerVisible: true,
      priceFormat: boundedSubIndicatorPriceFormat(indicator),
    }),
    macdLine: null,
    signalLine: null,
    histogram: null,
  }
}
function calcRSI(
  data: { time: Time; close: number }[],
  period = 14,
): LineData[] {
  const results: LineData[] = []
  let gains = 0, losses = 0
  for (let i = 1; i < data.length; i++) {
    const diff = data[i].close - data[i - 1].close
    if (i <= period) {
      gains += Math.max(diff, 0)
      losses += Math.max(-diff, 0)
      if (i === period) {
        const avgGain = gains / period
        const avgLoss = losses / period
        const rs = avgLoss === 0 ? 100 : avgGain / avgLoss
        results.push({ time: data[i].time, value: 100 - 100 / (1 + rs) })
      }
    } else {
      gains = (gains * (period - 1)) / period + Math.max(diff, 0)
      losses = (losses * (period - 1)) / period + Math.max(-diff, 0)
      const rs = losses === 0 ? 100 : gains / losses
      results.push({ time: data[i].time, value: 100 - 100 / (1 + rs) })
    }
  }
  return results
}

function calcEMA(data: number[], period: number): number[] {
  const multiplier = 2 / (period + 1)
  const ema: number[] = [data[0]]
  for (let i = 1; i < data.length; i++) {
    ema.push((data[i] - ema[i - 1]) * multiplier + ema[i - 1])
  }
  return ema
}

function calcMACD(data: { time: Time; close: number }[]): {
  macdLine: LineData[]
  signalLine: LineData[]
  histogram: HistogramData[]
} {
  const closes = data.map((d) => d.close)
  const ema12 = calcEMA(closes, 12)
  const ema26 = calcEMA(closes, 26)
  const macdLineArr = ema12.map((v, i) => v - ema26[i])
  const signalLineArr = calcEMA(macdLineArr, 9)
  return {
    macdLine: data.map((d, i) => ({ time: d.time, value: macdLineArr[i] })),
    signalLine: data.map((d, i) => ({ time: d.time, value: signalLineArr[i] })),
    histogram: data.map((d, i) => ({
      time: d.time,
      value: macdLineArr[i] - signalLineArr[i],
      color: macdLineArr[i] - signalLineArr[i] >= 0 ? '#ef4444' : '#22c55e',
    })),
  }
}

// ── chart theme constants ──

const BG_COLOR = '#0f172a'
const TEXT_COLOR = '#94a3b8'
const GRID_COLOR = '#1e293b'
const UP_COLOR = '#22c55e'
const DOWN_COLOR = '#ef4444'
const WICK_UP_COLOR = '#22c55e'
const WICK_DOWN_COLOR = '#ef4444'
const ADJUSTMENT_STORAGE_KEY = 'stock-review-adjustment-mode'
const ADJUSTMENT_OPTIONS: { value: AdjustmentMode; label: string }[] = [
  { value: 'raw', label: '除权' },
  { value: 'qfq', label: '前复权' },
  { value: 'hfq', label: '后复权' },
]

function readStoredAdjustmentMode(): AdjustmentMode {
  if (typeof window === 'undefined') return 'raw'
  const value = window.localStorage.getItem(ADJUSTMENT_STORAGE_KEY)
  return value === 'qfq' || value === 'hfq' || value === 'raw' ? value : 'raw'
}

function isChartControlTarget(target: EventTarget | null): boolean {
  return target instanceof Element && target.closest('[data-chart-control="true"]') !== null
}
const SUB_CHART_PRICE_SCALE_WIDTH = 60

// ── slider dark-theme override styles ──
const sliderStyles = `
.rc-slider-track {
  background-color: #6366f1 !important;
}
.rc-slider-rail {
  background-color: #334155 !important;
}
.rc-slider-handle {
  border-color: #6366f1 !important;
  background-color: #6366f1 !important;
  opacity: 1 !important;
}
.rc-slider-handle:hover {
  border-color: #818cf8 !important;
}
.rc-slider-handle-dragging {
  border-color: #818cf8 !important;
  box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.3) !important;
}
@keyframes toastFade {
  0% { opacity: 0; transform: translateX(-50%) translateY(-8px); }
  15% { opacity: 1; transform: translateX(-50%) translateY(0); }
  75% { opacity: 1; transform: translateX(-50%) translateY(0); }
  100% { opacity: 0; transform: translateX(-50%) translateY(-8px); }
}
`

// ── component ──

export default function StockReview() {
  const [searchParams] = useSearchParams()

  // ── state: search ──

  const [searchQuery, setSearchQuery] = useState('')
  const [searching, setSearching] = useState(false)
  const [searchResults, setSearchResults] = useState<StockInfo[]>([])
  const [stockList, setStockList] = useState<StockInfo[]>([])
  const [, setStockListError] = useState(false)
  const [selectedStock, setSelectedStock] = useState<{
    ticker: string
    name: string
  } | null>(null)

  // ── state: data ──

  const [chartData, setChartData] = useState<DailyRow[]>([])
  const [chartDataTicker, setChartDataTicker] = useState<string | null>(null)
  const [dataLoading, setDataLoading] = useState(false)
  const [dataError, setDataError] = useState<string | null>(null)
  const [adjustmentMode, setAdjustmentMode] = useState<AdjustmentMode>(() => readStoredAdjustmentMode())

  // ── state: MA visibility ──

  const DEFAULT_MA_COLORS = ['#8b5cf6', '#ec4899', '#14b8a6', '#f97316', '#06b6d4', '#84cc16', '#d946ef', '#0ea5e9']
  const [customMAs, setCustomMAs] = useState<{ period: number; color: string }[]>(() => {
    try {
      const saved = localStorage.getItem('stock-review-custom-mas')
      return saved ? JSON.parse(saved) : []
    } catch { return [] }
  })
  const [isAddingMA, setIsAddingMA] = useState(false)
  const [toastMsg, setToastMsg] = useState<string | null>(null)

  // ── state: recent searches ──
  const [recentSearches, setRecentSearches] = useState<{ ticker: string; name: string }[]>(() => {
    try {
      const saved = localStorage.getItem('stock-review-recent-searches')
      return saved ? JSON.parse(saved) : []
    } catch { return [] }
  })
  const [crosshairData, setCrosshairData] = useState<{
    time: string
    open: number
    close: number
    volume: number | null
    amount: number | null
    turnover: number | null
    maValues: { label: string; value: number; color: string }[]
    distFromHigh: number | null
    distFromLow: number | null
    sub1Value: { label: string; value: number | null } | null
    sub2Value: { label: string; value: number | null } | null
  } | null>(null)

  useEffect(() => {
    if (toastMsg) {
      const timer = setTimeout(() => setToastMsg(null), 2500)
      return () => clearTimeout(timer)
    }
  }, [toastMsg])

  const [visibleMAs, setVisibleMAs] = useState<Set<string>>(
    new Set(['ma5', 'ma20', 'ma60']),
  )

  // ── Persist custom MAs to localStorage ──
  useEffect(() => {
    localStorage.setItem('stock-review-custom-mas', JSON.stringify(customMAs))
  }, [customMAs])

  // ── Persist recent searches to localStorage ──
  useEffect(() => {
    localStorage.setItem('stock-review-recent-searches', JSON.stringify(recentSearches))
  }, [recentSearches])

  // ── Persist adjustment mode to localStorage ──
  useEffect(() => {
    localStorage.setItem(ADJUSTMENT_STORAGE_KEY, adjustmentMode)
  }, [adjustmentMode])

  // ── state: sub-chart indicators ──
  const [subChart1Indicator, setSubChart1Indicator] = useState<SubIndicatorKey>('volume')
  const [subChart2Indicator, setSubChart2Indicator] = useState<SubIndicatorKey>('pct_change')
  const [chartsReady, setChartsReady] = useState(false)

  // ── state: phase notes ──
  const [, setPhaseRecord] = useState<PhaseRecord | null>(null)
  const [phase, setPhase] = useState<string>('')
  const [M1_core, setM1_core] = useState(false)
  const [M2_front, setM2_front] = useState(false)
  const [M3_identifiable, setM3_identifiable] = useState(false)
  const [V_triggered, setV_triggered] = useState(false)
  const [phaseNotes, setPhaseNotes] = useState('')
  const [phaseSaving, setPhaseSaving] = useState(false)
  const [phaseToast, setPhaseToast] = useState<string | null>(null)

  // ── state: trades ──
  const [allTrades, setAllTrades] = useState<TradeRecord[]>([])
  const [showNewTradeForm, setShowNewTradeForm] = useState(false)
  const [expandedTradeId, setExpandedTradeId] = useState<string | null>(null)
  const [editingTradeId, setEditingTradeId] = useState<string | null>(null)
  const [newEntryDate, setNewEntryDate] = useState('')
  const [newEntryPrice, setNewEntryPrice] = useState('')
  const [newPathType, setNewPathType] = useState('')
  const [newPositionPct, setNewPositionPct] = useState('')
  const [newTradeNotes, setNewTradeNotes] = useState('')
  const [editHalfSellTrigger, setEditHalfSellTrigger] = useState('')
  const [editHalfSellDate, setEditHalfSellDate] = useState('')
  const [editHalfSellPrice, setEditHalfSellPrice] = useState('')
  const [editExitDate, setEditExitDate] = useState('')
  const [editExitPrice, setEditExitPrice] = useState('')
  const [editNotes, setEditNotes] = useState('')

  // ── refs ──

  const mainChart = useRef<IChartApi | null>(null)
  const volumeChart = useRef<IChartApi | null>(null)
  const pctChart = useRef<IChartApi | null>(null)
  const mainChartContainerRef = useRef<HTMLDivElement | null>(null)
  const mainChartWheelRef = useRef<HTMLDivElement | null>(null)

  const candleSeries = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const volumeSeries = useRef<SubChartSingleSeries | null>(null)
  const pctSeries = useRef<SubChartSingleSeries | null>(null)

  const subChart1MACDLine = useRef<ISeriesApi<'Line'> | null>(null)
  const subChart1SignalLine = useRef<ISeriesApi<'Line'> | null>(null)
  const subChart1Histogram = useRef<ISeriesApi<'Histogram'> | null>(null)
  const subChart2MACDLine = useRef<ISeriesApi<'Line'> | null>(null)
  const subChart2SignalLine = useRef<ISeriesApi<'Line'> | null>(null)
  const subChart2Histogram = useRef<ISeriesApi<'Histogram'> | null>(null)

  const maSeries = useRef<Map<string, ISeriesApi<'Line'>>>(new Map())
  const chartDataRef = useRef<DailyRow[]>([])
  const maConfigRef = useRef<{ key: string; label: string; color: string }[]>([])
  const visibleMAsRef = useRef<Set<string>>(new Set())
  const subChart1IndicatorRef = useRef<SubIndicatorKey>(subChart1Indicator)
  const subChart2IndicatorRef = useRef<SubIndicatorKey>(subChart2Indicator)
  const rsiDataRef = useRef<LineData[]>([])
  const macdDataRef = useRef<ReturnType<typeof calcMACD> | null>(null)

  const isSyncing = useRef(false)
  const crosshairSynced = useRef(false)
  const resizeObserverRef = useRef<ResizeObserver | null>(null)

  const initialFillDone = useRef(false)

  const hasAutoLoaded = useRef(false)

  // ── refs for chart overlays (interval analysis) ──
  const markersPlugin = useRef<ISeriesMarkersPluginApi<Time> | null>(null)
  const highPriceLine = useRef<IPriceLine | null>(null)
  const lowPriceLine = useRef<IPriceLine | null>(null)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const sliderStatsRef = useRef<any>(null)

  const selectionStartX = useRef<number | null>(null)
  const [selectionBox, setSelectionBox] = useState<{
    left: number
    width: number
  } | null>(null)

  // ── TASK 2: URL param auto-load ──

  useEffect(() => {
    if (hasAutoLoaded.current) return
    const tickerParam = searchParams.get('ticker')
    const nameParam = searchParams.get('name')
    if (tickerParam) {
      const name = nameParam || tickerParam
      setSelectedStock({ ticker: tickerParam, name })
      setSearchQuery(`${tickerParam} - ${name}`)
      hasAutoLoaded.current = true
    }
  }, [searchParams])

  // ── TASK 5: Fetch stock list on mount ──

  useEffect(() => {
    let cancelled = false
    async function fetchStockList() {
      try {
        const list = await getStockList()
        if (!cancelled) {
          setStockList(list)
          setStockListError(false)
        }
      } catch {
        if (!cancelled) {
          setStockListError(true)
        }
      }
    }
    fetchStockList()
    return () => { cancelled = true }
  }, [])

  // ── TASK 5: Real-time local filtering for search suggestions ──

  const handleSearchInputChange = useCallback((value: string) => {
    setSearchQuery(value)
    const q = value.trim()
    if (!q || stockList.length === 0) {
      setSearchResults([])
      return
    }

    const lowerQ = q.toLowerCase()
    const matched = stockList.filter((s) => {
      // a) ticker code match
      if (s.ticker.toLowerCase().includes(lowerQ)) return true
      // b) name match
      if (s.name && s.name.toLowerCase().includes(lowerQ)) return true
      // c) pinyin initials match
      if (s.name) {
        try {
          const initials = pinyinPro(s.name, { pattern: 'first', toneType: 'none' })
          const compact = initials.replace(/\s+/g, '')
          if (compact && compact.toLowerCase().includes(lowerQ)) return true
        } catch {
          // ignore pinyin errors
        }
      }
      return false
    })

    setSearchResults(matched.slice(0, 20))
  }, [stockList])

  // ── search logic (fallback degraded mode) ──

  const handleSearch = useCallback(async () => {
    const q = searchQuery.trim()
    if (!q) return

    const todayYMD = (() => {
      const d = new Date()
      return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`
    })()

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
      setSearchResults(
        matched.slice(0, 20).map((r) => ({ ticker: r.ticker, name: r.name ?? r.ticker })),
      )
    } catch {
      // Fallback: try a few recent dates
      try {
        const fallbackDate = new Date()
        fallbackDate.setDate(fallbackDate.getDate() - 1)
        const fd = `${fallbackDate.getFullYear()}${String(fallbackDate.getMonth() + 1).padStart(2, '0')}${String(fallbackDate.getDate()).padStart(2, '0')}`
        const rows = await getDailyByDate(fd)
        const lowerQ = q.toLowerCase()
        const matched = rows.filter(
          (r) =>
            r.ticker.toLowerCase().includes(lowerQ) ||
            (r.name && r.name.toLowerCase().includes(lowerQ)),
        )
        setSearchResults(
          matched.slice(0, 20).map((r) => ({ ticker: r.ticker, name: r.name ?? r.ticker })),
        )
      } catch {
        setSearchResults([])
      }
    } finally {
      setSearching(false)
    }
  }, [searchQuery])

  const handleSelectStock = useCallback(
    (ticker: string, name: string) => {
      setSelectedStock({ ticker, name })
      setSearchResults([])
      setSearchQuery(`${ticker} - ${name}`)
      setRecentSearches((prev) => {
        const next = prev.filter((s) => s.ticker !== ticker)
        return [{ ticker, name }, ...next].slice(0, 5)
      })
    },
    [],
  )

  // ── 换股票时清理图表 ref，避免 DOM 重挂载后 ref callback 因旧 ref 跳过创建 ──
  useEffect(() => {
    if (!selectedStock) return

    // 先销毁图表实例
    resizeObserverRef.current?.disconnect()
    resizeObserverRef.current = null
    mainChart.current?.remove()
    volumeChart.current?.remove()
    pctChart.current?.remove()
    mainChart.current = null
    volumeChart.current = null
    pctChart.current = null

    // 清理系列 ref
    candleSeries.current = null
    volumeSeries.current = null
    pctSeries.current = null
    subChart1MACDLine.current = null
    subChart1SignalLine.current = null
    subChart1Histogram.current = null
    subChart2MACDLine.current = null
    subChart2SignalLine.current = null
    subChart2Histogram.current = null
    maSeries.current.clear()

    // 重置状态 ref
    crosshairSynced.current = false
    setChartsReady(false)
    initialFillDone.current = false
    mainChartContainerRef.current = null
    selectionStartX.current = null
    chartDataRef.current = []
    setChartData([])
    setChartDataTicker(null)
    markersPlugin.current = null
    highPriceLine.current = null
    lowPriceLine.current = null
    setSelectionBox(null)
  }, [selectedStock])

  // ── fetch chart data ──

  useEffect(() => {
    if (!selectedStock) return
    const selectedTicker = selectedStock.ticker

    let cancelled = false

    async function fetchData() {
      setDataLoading(true)
      setDataError(null)
      try {
        const rows = await getDailyByTicker(
          selectedTicker,
          undefined,
          undefined,
          adjustmentMode,
        )
        if (!cancelled) {
          // Sort ascending by trade_date for charting
          rows.sort((a, b) => a.trade_date.localeCompare(b.trade_date))
          setChartData(rows)
          setChartDataTicker(selectedTicker)
          chartDataRef.current = rows
        }
      } catch (err) {
        if (!cancelled) {
          setDataError(
            err instanceof Error ? err.message : '数据加载失败',
          )
          setChartData([])
          setChartDataTicker(null)
          chartDataRef.current = []
        }
      } finally {
        if (!cancelled) setDataLoading(false)
      }
    }

    fetchData()
    return () => {
      cancelled = true
    }
  }, [selectedStock, adjustmentMode])

  // ── chart creation and data population ──

  const chartReady = useMemo(
    () =>
      chartData.length > 0 &&
      chartDataTicker === selectedStock?.ticker &&
      !dataError &&
      selectedStock !== null,
    [chartData.length, chartDataTicker, dataError, selectedStock],
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

  const amountData = useMemo((): HistogramData[] => {
    return chartData
      .filter((r) => r.amount != null && r.open != null && r.close != null)
      .map((r) => ({
        time: toChartTime(r.trade_date) as Time,
        value: r.amount! / 1e8,
        color: r.close! >= r.open! ? UP_COLOR : DOWN_COLOR,
      }))
  }, [chartData])

  const turnoverData = useMemo((): LineData[] => {
    return chartData
      .filter((r) => r.turnover != null)
      .map((r) => ({
        time: toChartTime(r.trade_date) as Time,
        value: r.turnover!,
      }))
  }, [chartData])

  const rsiData = useMemo((): LineData[] => {
    return calcRSI(closeData)
  }, [closeData])

  const macdData = useMemo(() => {
    return calcMACD(closeData)
  }, [closeData])

  // Merge built-in and custom MA configs
  const allMAConfigs = useMemo(() => {
    const builtin = MA_CONFIGS.map(cfg => ({ ...cfg, label: cfg.label }))
    const custom = customMAs.map((m) => ({
      key: `custom_${m.period}`,
      label: `MA${m.period}`,
      period: m.period,
      color: m.color,
    }))
    return [...builtin, ...custom]
  }, [customMAs])

  // MA data per period
  const maDataMap = useMemo(() => {
    const map = new Map<MAKey, LineData[]>()
    for (const cfg of allMAConfigs) {
      map.set(cfg.key, calcMA(closeData, cfg.period))
    }
    return map
  }, [closeData, allMAConfigs])

  // Sync refs used by chart callbacks
  useEffect(() => {
    maConfigRef.current = allMAConfigs.map(c => ({ key: c.key, label: c.label, color: c.color }))
  }, [allMAConfigs])

  useEffect(() => {
    subChart1IndicatorRef.current = subChart1Indicator
    subChart2IndicatorRef.current = subChart2Indicator
  }, [subChart1Indicator, subChart2Indicator])

  useEffect(() => {
    rsiDataRef.current = rsiData
    macdDataRef.current = macdData
  }, [macdData, rsiData])

  // TASK 6: Date list for slider
  const allDates = useMemo(() => {
    return chartData.map((r) => toChartTime(r.trade_date))
  }, [chartData])

  const [sliderValue, setSliderValue] = useState<[number, number]>([
    Math.max(0, allDates.length - 90),
    allDates.length - 1,
  ])

  const applyVisibleDateRange = useCallback(
    (startIdx: number, endIdx: number) => {
      if (allDates.length === 0) return
      const safeStart = Math.max(0, Math.min(startIdx, allDates.length - 1))
      const safeEnd = Math.max(0, Math.min(endIdx, allDates.length - 1))
      const [fromIdx, toIdx] = safeStart <= safeEnd
        ? [safeStart, safeEnd]
        : [safeEnd, safeStart]
      setSliderValue([fromIdx, toIdx])
      const range = {
        from: allDates[fromIdx] as Time,
        to: allDates[toIdx] as Time,
      }
      setMainVisibleRangeAndSyncSubCharts(mainChart.current, volumeChart.current, pctChart.current, range)
    },
    [allDates],
  )

  const getSubIndicatorValue = useCallback((indicator: SubIndicatorKey, timeStr: string, row: DailyRow | undefined): number | null => {
    if (indicator === 'volume') return row?.volume ?? null
    if (indicator === 'amount') return row?.amount != null ? row.amount / 1e8 : null
    if (indicator === 'pct_change') return row?.pct_change ?? null
    if (indicator === 'turnover') return row?.turnover ?? null
    if (indicator === 'rsi') {
      return rsiDataRef.current.find((d) => normalizeChartTime(d.time) === timeStr)?.value ?? null
    }
    const macdPoint = macdDataRef.current?.macdLine.find((d) => normalizeChartTime(d.time) === timeStr)
    return macdPoint?.value ?? null
  }, [])

  const setSubChartData = useCallback((indicator: SubIndicatorKey, refs: SubChartSeriesBundle) => {
    if (indicator === 'volume' && refs.single && volumeData.length > 0) {
      refs.single.setData(volumeData)
    } else if (indicator === 'amount' && refs.single && amountData.length > 0) {
      refs.single.setData(amountData)
    } else if (indicator === 'pct_change' && refs.single && pctData.length > 0) {
      refs.single.setData(pctData)
    } else if (indicator === 'turnover' && refs.single && turnoverData.length > 0) {
      refs.single.setData(turnoverData)
    } else if (indicator === 'rsi' && refs.single && rsiData.length > 0) {
      refs.single.setData(rsiData)
    } else if (indicator === 'macd') {
      if (refs.macdLine && macdData.macdLine.length > 0) refs.macdLine.setData(macdData.macdLine)
      if (refs.signalLine && macdData.signalLine.length > 0) refs.signalLine.setData(macdData.signalLine)
      if (refs.histogram && macdData.histogram.length > 0) refs.histogram.setData(macdData.histogram)
    }
  }, [amountData, macdData, pctData, rsiData, turnoverData, volumeData])

  // ── Effect: Create/remove MA series dynamically when allMAConfigs changes ──
  useEffect(() => {
    const main = mainChart.current
    if (!main || !candleSeries.current) return

    // 创建缺失的 MA series（例如新增的自定义MA）
    for (const cfg of allMAConfigs) {
      if (!maSeries.current.has(cfg.key)) {
        const s = main.addSeries(LineSeries, {
          color: cfg.color, lineWidth: 1, priceLineVisible: false,
          lastValueVisible: false,
          visible: visibleMAs.has(cfg.key),
        })
        maSeries.current.set(cfg.key, s)
      }
    }

    // 清理已被删除的 MA series（例如删除的自定义MA）
    for (const [key, series] of maSeries.current.entries()) {
      if (!allMAConfigs.some(cfg => cfg.key === key)) {
        main.removeSeries(series)
        maSeries.current.delete(key)
      }
    }
  }, [allMAConfigs, visibleMAs])

  // ── Effect: Rebuild sub-chart series + set data ──
  // Always recreate sub-chart series on indicator changes to avoid stale series after rapid switches.
  useEffect(() => {
    if (!candleSeries.current || candleData.length === 0) return

    const savedLogicalRange = mainChart.current?.timeScale().getVisibleLogicalRange()

    const chart1 = volumeChart.current
    if (chart1) {
      clearAllSeries(chart1, [
        volumeSeries.current,
        subChart1MACDLine.current,
        subChart1SignalLine.current,
        subChart1Histogram.current,
      ])
      const rebuilt = rebuildSubChart(chart1, subChart1Indicator)
      volumeSeries.current = rebuilt.single
      subChart1MACDLine.current = rebuilt.macdLine
      subChart1SignalLine.current = rebuilt.signalLine
      subChart1Histogram.current = rebuilt.histogram
      setSubChartData(subChart1Indicator, rebuilt)
    }

    const chart2 = pctChart.current
    if (chart2) {
      clearAllSeries(chart2, [
        pctSeries.current,
        subChart2MACDLine.current,
        subChart2SignalLine.current,
        subChart2Histogram.current,
      ])
      const rebuilt = rebuildSubChart(chart2, subChart2Indicator)
      pctSeries.current = rebuilt.single
      subChart2MACDLine.current = rebuilt.macdLine
      subChart2SignalLine.current = rebuilt.signalLine
      subChart2Histogram.current = rebuilt.histogram
      setSubChartData(subChart2Indicator, rebuilt)
    }

    candleSeries.current.setData(candleData)
    for (const cfg of allMAConfigs) {
      const series = maSeries.current.get(cfg.key)
      const data = maDataMap.get(cfg.key) ?? []
      if (series && data.length > 0) {
        series.setData(data)
      }
    }

    if (savedLogicalRange && initialFillDone.current) {
      setAllVisibleLogicalRange(mainChart.current, volumeChart.current, pctChart.current, savedLogicalRange)
    }

    if (candleData.length > 0 && !initialFillDone.current) {
      initialFillDone.current = true
      const endIdx = allDates.length - 1
      const startIdx = Math.max(0, allDates.length - 90)
      setSliderValue([startIdx, endIdx])
      const range = {
        from: allDates[startIdx] as Time,
        to: allDates[endIdx] as Time,
      }
      setMainVisibleRangeAndSyncSubCharts(mainChart.current, volumeChart.current, pctChart.current, range)
    }
  }, [allDates, allMAConfigs, candleData, maDataMap, setSubChartData, subChart1Indicator, subChart2Indicator])
  // ── Effect: 三图 crosshair + 时间轴同步（在所有图表 ref 就绪后统一注册）──
  useEffect(() => {
    const m = mainChart.current
    const v = volumeChart.current
    const p = pctChart.current
    if (!chartsReady || !m || !v || !p) return

    const charts = [m, v, p]
    const unsubs: (() => void)[] = []

    charts.forEach((source) => {
      const handler = () => {
        if (isSyncing.current) return
        const range = source.timeScale().getVisibleLogicalRange()
        if (!range) return
        isSyncing.current = true
        charts.filter((chart) => chart !== source).forEach((chart) => chart.timeScale().setVisibleLogicalRange(range))
        isSyncing.current = false
      }
      source.timeScale().subscribeVisibleLogicalRangeChange(handler)
      unsubs.push(() => source.timeScale().unsubscribeVisibleLogicalRangeChange(handler))
    })

    const syncCrosshair = (param: MouseEventParams) => {
      const vSeries = getSubChartCrosshairSeries(
        volumeSeries.current,
        subChart1MACDLine.current,
        subChart1SignalLine.current,
        subChart1Histogram.current,
      )
      const pSeries = getSubChartCrosshairSeries(
        pctSeries.current,
        subChart2MACDLine.current,
        subChart2SignalLine.current,
        subChart2Histogram.current,
      )
      if (!param.time || !vSeries || !pSeries) {
        v.clearCrosshairPosition()
        p.clearCrosshairPosition()
        return
      }
      const timeStr = normalizeChartTime(param.time as Time)
      const row = timeStr ? chartDataRef.current.find((r) => toChartTime(r.trade_date) === timeStr) : undefined
      const vPrice = timeStr ? getSubIndicatorValue(subChart1IndicatorRef.current, timeStr, row) : null
      const pPrice = timeStr ? getSubIndicatorValue(subChart2IndicatorRef.current, timeStr, row) : null
      v.setCrosshairPosition(vPrice ?? 0, param.time as Time, vSeries)
      p.setCrosshairPosition(pPrice ?? 0, param.time as Time, pSeries)
    }
    m.subscribeCrosshairMove(syncCrosshair)
    unsubs.push(() => m.unsubscribeCrosshairMove(syncCrosshair))

    crosshairSynced.current = true
    return () => {
      unsubs.forEach((fn) => fn())
      crosshairSynced.current = false
    }
  }, [chartsReady, getSubIndicatorValue])
  // ── Effect: Cleanup on unmount only ──
  useEffect(() => {
    return () => {
      resizeObserverRef.current?.disconnect()
      resizeObserverRef.current = null
      mainChart.current?.remove()
      volumeChart.current?.remove()
      pctChart.current?.remove()
      mainChart.current = null
      volumeChart.current = null
      pctChart.current = null
      candleSeries.current = null
      volumeSeries.current = null
      pctSeries.current = null
      subChart1MACDLine.current = null
      subChart1SignalLine.current = null
      subChart1Histogram.current = null
      subChart2MACDLine.current = null
      subChart2SignalLine.current = null
      subChart2Histogram.current = null
      maSeries.current.clear()
      crosshairSynced.current = false
      setChartsReady(false)
      markersPlugin.current = null
      highPriceLine.current = null
      lowPriceLine.current = null
    }
  }, [])

  // ── Sync visibleMAs to ref for crosshair callback ──
  useEffect(() => {
    visibleMAsRef.current = visibleMAs
  }, [visibleMAs])

  // ── Effect D: MA visibility toggle ──
  useEffect(() => {
    for (const cfg of allMAConfigs) {
      const series = maSeries.current.get(cfg.key)
      if (series) {
        series.applyOptions({ visible: visibleMAs.has(cfg.key) })
      }
    }
  }, [visibleMAs, allMAConfigs])

  // ── MA toggle ──

  const toggleMA = useCallback((key: string) => {
    setVisibleMAs((prev) => {
      if (prev.has(key)) {
        // 关闭 — 始终允许
        const next = new Set(prev)
        next.delete(key)
        return next
      }
      // 开启 — 检查是否超出3条限制
      if (prev.size >= 3) {
        setTimeout(() => setToastMsg('最多同时显示3条均线'), 0)
        return prev
      }
      const next = new Set(prev)
      next.add(key)
      return next
    })
  }, [])

  // ── current date for phase ──
  const currentDate = useMemo(() => {
    if (chartData.length > 0) return chartData[chartData.length - 1].trade_date
    return ''
  }, [chartData])

  // ── fetch phase for current date ──
  useEffect(() => {
    if (!currentDate) return
    let cancelled = false
    async function fetchPhase() {
      try {
        const records = await getPhase(currentDate)
        if (!cancelled && records.length > 0) {
          const r = records[0]
          setPhaseRecord(r)
          setPhase(r.phase ?? '')
          setM1_core(r.M1_core ?? false)
          setM2_front(r.M2_front ?? false)
          setM3_identifiable(r.M3_identifiable ?? false)
          setV_triggered(r.V_triggered ?? false)
          setPhaseNotes(r.notes ?? '')
        } else if (!cancelled) {
          setPhaseRecord(null)
          setPhase('')
          setM1_core(false)
          setM2_front(false)
          setM3_identifiable(false)
          setV_triggered(false)
          setPhaseNotes('')
        }
      } catch {
        if (!cancelled) setPhaseRecord(null)
      }
    }
    fetchPhase()
    return () => { cancelled = true }
  }, [currentDate])

  // ── fetch all trades ──
  useEffect(() => {
    let cancelled = false
    async function fetchTrades() {
      try {
        const records = await getTrades()
        if (!cancelled) setAllTrades(records)
      } catch { /* ignore */ }
    }
    fetchTrades()
    return () => { cancelled = true }
  }, [])

  // ── filtered trades for current stock ──
  const stockTrades = useMemo(() => {
    if (!selectedStock) return []
    return allTrades.filter((t) => t.ticker === selectedStock.ticker)
  }, [allTrades, selectedStock])

  // ── handle save phase ──
  const handleSavePhase = useCallback(async () => {
    if (!currentDate) return
    setPhaseSaving(true)
    try {
      const data: PhaseWrite = {
        trade_date: currentDate,
        phase: phase || null,
        M1_core,
        M2_front,
        M3_identifiable,
        V_triggered,
        notes: phaseNotes || null,
      }
      await createPhase(data)
      setPhaseToast('判读已保存')
      setTimeout(() => setPhaseToast(null), 2000)
    } catch {
      setPhaseToast('保存失败')
      setTimeout(() => setPhaseToast(null), 2000)
    } finally {
      setPhaseSaving(false)
    }
  }, [currentDate, phase, M1_core, M2_front, M3_identifiable, V_triggered, phaseNotes])

  // ── handle create trade ──
  const handleCreateTrade = useCallback(async () => {
    if (!selectedStock) return
    try {
      const data: TradeWrite = {
        ticker: selectedStock.ticker,
        entry_date: newEntryDate || currentDate || null,
        entry_price: newEntryPrice ? parseFloat(newEntryPrice) : null,
        path_type: newPathType || null,
        position_pct: newPositionPct ? parseFloat(newPositionPct) : null,
        notes: newTradeNotes || null,
      }
      await createTrade(data)
      const records = await getTrades()
      setAllTrades(records)
      setShowNewTradeForm(false)
      setNewEntryDate('')
      setNewEntryPrice('')
      setNewPathType('')
      setNewPositionPct('')
      setNewTradeNotes('')
    } catch { /* ignore */ }
  }, [selectedStock, currentDate, newEntryDate, newEntryPrice, newPathType, newPositionPct, newTradeNotes])

  // ── handle edit trade ──
  const handleEditTrade = useCallback(async (tradeId: string) => {
    try {
      const patch: TradePatch = {
        half_sell_trigger: editHalfSellTrigger ? parseFloat(editHalfSellTrigger) : null,
        half_sell_date: editHalfSellDate || null,
        half_sell_price: editHalfSellPrice ? parseFloat(editHalfSellPrice) : null,
        exit_date: editExitDate || null,
        exit_price: editExitPrice ? parseFloat(editExitPrice) : null,
        notes: editNotes || null,
      }
      await updateTrade(tradeId, patch)
      const records = await getTrades()
      setAllTrades(records)
      setEditingTradeId(null)
    } catch { /* ignore */ }
  }, [editHalfSellTrigger, editHalfSellDate, editHalfSellPrice, editExitDate, editExitPrice, editNotes])

  const openEditForm = useCallback((trade: TradeRecord) => {
    setEditingTradeId(trade.trade_id)
    setEditHalfSellTrigger(trade.half_sell_trigger?.toString() ?? '')
    setEditHalfSellDate(trade.half_sell_date ?? '')
    setEditHalfSellPrice(trade.half_sell_price?.toString() ?? '')
    setEditExitDate(trade.exit_date ?? '')
    setEditExitPrice(trade.exit_price?.toString() ?? '')
    setEditNotes(trade.notes ?? '')
  }, [])

  const calcPnl = useCallback((trade: TradeRecord): string | null => {
    if (trade.entry_price == null || trade.exit_price == null) return null
    const pnl = ((trade.exit_price - trade.entry_price) / trade.entry_price) * 100
    return pnl.toFixed(2)
  }, [])

  // ── info panel data (from slider range) ──

  const sliderStats = useMemo(() => {
    if (chartData.length === 0 || sliderValue[0] >= chartData.length || sliderValue[1] >= chartData.length) {
      return null
    }

    const startIdx = Math.max(0, sliderValue[0])
    const endIdx = Math.min(chartData.length - 1, sliderValue[1])
    const sliced = chartData.slice(startIdx, endIdx + 1)
    if (sliced.length === 0) return null

    const lastRow = sliced[sliced.length - 1]
    const firstRow = sliced[0]

    // 区间最高
    const highInRange = sliced.reduce(
      (max, r) => (r.high != null && r.high > max ? r.high : max),
      -Infinity,
    )
    // 区间最低
    const lowInRange = sliced.reduce(
      (min, r) => (r.low != null && r.low < min ? r.low : min),
      Infinity,
    )

    // 涨跌幅: 最后一根 K 线相对前一根的涨跌幅
    let pctChange: number | null = null
    if (endIdx > 0) {
      const prevRow = chartData[endIdx - 1]
      if (lastRow.close != null && prevRow.close != null && prevRow.close !== 0) {
        pctChange = ((lastRow.close - prevRow.close) / prevRow.close) * 100
      }
    }

    // 区间涨幅 = (区间末根K线收盘价 - 区间首根K线开盘价) / 区间首根K线开盘价 × 100%
    let rangeGain: number | null = null
    if (firstRow.open != null && lastRow.close != null && firstRow.open !== 0) {
      rangeGain = ((lastRow.close - firstRow.open) / firstRow.open) * 100
    }

    // 振幅 = (区间最高价 - 区间最低价) / 区间首根K线开盘价 × 100%
    let rangeAmplitude: number | null = null
    if (highInRange !== -Infinity && lowInRange !== Infinity && firstRow.open != null && firstRow.open !== 0) {
      rangeAmplitude = ((highInRange - lowInRange) / firstRow.open) * 100
    }

    // 均价(VWAP) = 区间所有K线成交额之和 / 区间所有K线成交量之和
    let vwap: number | null = null
    let totalAmount = 0, totalVolume = 0
    let hasAmountVolume = false
    for (const r of sliced) {
      if (r.amount != null && r.volume != null) {
        totalAmount += r.amount
        totalVolume += r.volume
        hasAmountVolume = true
      }
    }
    if (hasAmountVolume && totalVolume !== 0) {
      vwap = totalAmount / totalVolume
    }

    // 量比 = 区间末根K线成交量 / (区间总成交量 / 区间K线根数)
    let volumeRatio: number | null = null
    let maxVolumeIdx = -1
    let maxVolumeValue = 0
    let maxVolumeRow: DailyRow | null = null
    for (const r of sliced) {
      if (r.volume != null && r.volume > maxVolumeValue) {
        maxVolumeValue = r.volume
        maxVolumeIdx = sliced.indexOf(r)
        maxVolumeRow = r
      }
    }
    if (lastRow.volume != null && totalVolume !== 0 && sliced.length > 0) {
      volumeRatio = lastRow.volume / (totalVolume / sliced.length)
    }

    // 价格位置 = (最新价 - 区间最低) / (区间最高 - 区间最低) × 100%
    let pricePosition: number | null = null
    if (highInRange !== -Infinity && lowInRange !== Infinity && lastRow.close != null && highInRange !== lowInRange) {
      pricePosition = ((lastRow.close - lowInRange) / (highInRange - lowInRange)) * 100
    }

    const result = {
      lastRow,
      high: highInRange !== -Infinity ? highInRange : null,
      low: lowInRange !== Infinity ? lowInRange : null,
      pctChange,
      rangeGain,
      rangeAmplitude,
      vwap,
      volumeRatio,
      pricePosition,
      maxVolumeIdx: maxVolumeIdx >= 0 ? startIdx + maxVolumeIdx : -1,
      maxVolumeValue,
      maxVolumeRow,
    }
    return result
  }, [chartData, sliderValue])

  // Sync sliderStats to ref for crosshair callback
  useEffect(() => {
    sliderStatsRef.current = sliderStats
  }, [sliderStats])

  // ── Effect: K线图 overlay 标记（区间高低点虚线 / 首尾标记 / 最大量标记）──
  useEffect(() => {
    if (!sliderStats || !candleSeries.current) return

    const cs = candleSeries.current
    const stats = sliderStats

    // 高低点价格线
    if (highPriceLine.current) {
      try { cs.removePriceLine(highPriceLine.current) } catch { /* ignore */ }
    }
    if (lowPriceLine.current) {
      try { cs.removePriceLine(lowPriceLine.current) } catch { /* ignore */ }
    }
    if (stats.high != null) {
      highPriceLine.current = cs.createPriceLine({
        price: stats.high,
        color: '#f97316',
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: false,
      })
    }
    if (stats.low != null) {
      lowPriceLine.current = cs.createPriceLine({
        price: stats.low,
        color: '#22c55e',
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: false,
      })
      }

      // 首尾 K 线 + 最大量标记
    if (markersPlugin.current && allDates.length > 0) {
      const markers: SeriesMarker<Time>[] = []
      const [s, e] = sliderValue
      if (s >= 0 && s < allDates.length) {
        markers.push({
          time: allDates[s] as Time,
          position: 'belowBar',
          color: '#22c55e',
          shape: 'arrowUp',
          id: 'first-candle',
        })
      }
      if (e >= 0 && e < allDates.length && e !== s) {
        markers.push({
          time: allDates[e] as Time,
          position: 'aboveBar',
          color: '#ef4444',
          shape: 'arrowDown',
          id: 'last-candle',
        })
      }
      if (stats.maxVolumeIdx >= 0 && stats.maxVolumeIdx < allDates.length) {
        markers.push({
          time: allDates[stats.maxVolumeIdx] as Time,
          position: 'aboveBar',
          color: '#f59e0b',
          shape: 'circle',
          id: 'max-volume',
        })
      }
      markersPlugin.current.setMarkers(markers)
    }

    return () => {
      if (highPriceLine.current) {
        try { cs.removePriceLine(highPriceLine.current) } catch { /* ignore */ }
        highPriceLine.current = null
      }
      if (lowPriceLine.current) {
        try { cs.removePriceLine(lowPriceLine.current) } catch { /* ignore */ }
        lowPriceLine.current = null
      }
    }
  }, [sliderStats, allDates, sliderValue])

  // ── TASK 6: Time range slider handler ──

  const handleSliderChange = useCallback(
    (value: number | number[]) => {
      if (!Array.isArray(value) || value.length < 2) return
      const [startIdx, endIdx] = value
      applyVisibleDateRange(startIdx, endIdx)
    },
    [applyVisibleDateRange],
  )

  const getChartX = useCallback((clientX: number) => {
    const el = mainChartContainerRef.current
    if (!el) return null
    const rect = el.getBoundingClientRect()
    return Math.max(0, Math.min(clientX - rect.left, rect.width))
  }, [])

  const updateSelectionBox = useCallback((fromX: number, toX: number) => {
    setSelectionBox({
      left: Math.min(fromX, toX),
      width: Math.abs(toX - fromX),
    })
  }, [])

  const handleChartPointerDown = useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      if (event.button !== 0 || allDates.length === 0) return
      const x = getChartX(event.clientX)
      if (x == null) return
      event.currentTarget.setPointerCapture(event.pointerId)
      selectionStartX.current = x
      updateSelectionBox(x, x)
    },
    [allDates.length, getChartX, updateSelectionBox],
  )

  const handleChartPointerMove = useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      const startX = selectionStartX.current
      if (startX == null) return
      const x = getChartX(event.clientX)
      if (x == null) return
      updateSelectionBox(startX, x)
    },
    [getChartX, updateSelectionBox],
  )

  const finishChartSelection = useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      const startX = selectionStartX.current
      if (startX == null) return
      selectionStartX.current = null
      setSelectionBox(null)

      const endX = getChartX(event.clientX)
      if (endX == null || Math.abs(endX - startX) < 8 || allDates.length === 0) return

      const timeScale = mainChart.current?.timeScale()
      const fromTime = normalizeChartTime(timeScale?.coordinateToTime(Math.min(startX, endX)) ?? null)
      const toTime = normalizeChartTime(timeScale?.coordinateToTime(Math.max(startX, endX)) ?? null)
      if (!fromTime || !toTime) return

      const fromIdx = allDates.indexOf(fromTime)
      const toIdx = allDates.indexOf(toTime)
      if (fromIdx < 0 || toIdx < 0 || fromIdx === toIdx) return

      applyVisibleDateRange(fromIdx, toIdx)
    },
    [allDates, applyVisibleDateRange, getChartX],
  )

  const zoomChartWindow = useCallback(
    (deltaY: number) => {
      if (deltaY === 0 || allDates.length <= 1) return

      const [currentStart, currentEnd] = sliderValue
      const currentSpan = currentEnd - currentStart + 1
      const nextSpan = deltaY > 0
        ? Math.ceil(currentSpan * 1.2)
        : Math.floor(currentSpan / 1.2)
      const clampedSpan = Math.max(5, Math.min(allDates.length, nextSpan))
      if (clampedSpan === currentSpan) return

      const nextEnd = currentEnd
      const nextStart = Math.max(0, nextEnd - clampedSpan + 1)

      applyVisibleDateRange(nextStart, nextEnd)
    },
    [allDates.length, applyVisibleDateRange, sliderValue],
  )

  const handleChartWheel = useCallback(
    (event: React.WheelEvent<HTMLDivElement>) => {
      if (isChartControlTarget(event.target)) return
      event.preventDefault()
      event.stopPropagation()
      zoomChartWindow(event.deltaY)
    },
    [zoomChartWindow],
  )

  useEffect(() => {
    const el = mainChartWheelRef.current
    if (!el) return

    const handleNativeWheel = (event: WheelEvent) => {
      if (isChartControlTarget(event.target)) return
      event.preventDefault()
      event.stopPropagation()
      zoomChartWindow(event.deltaY)
    }

    el.addEventListener('wheel', handleNativeWheel, { passive: false })
    return () => {
      el.removeEventListener('wheel', handleNativeWheel)
    }
  }, [zoomChartWindow])
  // ── render ──

  return (
    <div className="space-y-4">
      <style>{sliderStyles}</style>
      <h1 className="text-2xl font-bold">个股复盘</h1>

      {/* ── Search bar ── */}
      <Card className="border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50">
        <CardContent className="p-4">
          <div className="flex items-end gap-3">
            <div className="max-w-sm relative">
              <Input
                placeholder="输入股票代码或名称..."
                value={searchQuery}
                onChange={(e) => {
                  handleSearchInputChange(e.target.value)
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    if (searchResults.length > 0) {
                      const first = searchResults[0]
                      handleSelectStock(first.ticker, first.name)
                    } else {
                      handleSearch()
                    }
                  }
                }}
                className="border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white placeholder:text-slate-400 dark:placeholder:text-slate-500"
              />
              {/* Search dropdown */}
              {(searchQuery.trim() === '' ? recentSearches.length > 0 : searchResults.length > 0) && (
                <div
                  ref={(el) => {
                    if (!el) return
                    const rect = el.parentElement!.getBoundingClientRect()
                    el.style.left = `${rect.left}px`
                    el.style.width = `${rect.width}px`
                    el.style.top = `${rect.bottom}px`
                  }}
                  className="fixed z-[9999] mt-0 rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 shadow-lg max-h-60 overflow-y-auto"
                >
                  {searchQuery.trim() === '' ? (
                    recentSearches.map((r) => (
                      <div
                        key={r.ticker}
                        className="flex items-center gap-2 px-3 py-2 text-sm text-slate-700 dark:text-gray-200 hover:bg-slate-100 dark:hover:bg-slate-700 transition-colors"
                      >
                        <button
                          className="flex items-center gap-2 flex-1 text-left"
                          onClick={() => handleSelectStock(r.ticker, r.name)}
                        >
                          <span className="font-mono text-xs text-slate-400 dark:text-slate-400">
                            {r.ticker}
                          </span>
                          <span>{r.name}</span>
                        </button>
                        <button
                          className="text-xs text-slate-400 hover:text-red-400 ml-1 shrink-0"
                          onClick={(e) => {
                            e.stopPropagation()
                            setRecentSearches((prev) => prev.filter((s) => s.ticker !== r.ticker))
                          }}
                        >
                          ✕
                        </button>
                      </div>
                    ))
                  ) : (
                    searchResults.map((r) => (
                      <button
                        key={r.ticker}
                        className="w-full px-3 py-2 text-left text-sm text-slate-700 dark:text-gray-200 hover:bg-slate-100 dark:hover:bg-slate-700 transition-colors flex items-center gap-2"
                        onClick={() =>
                          handleSelectStock(r.ticker, r.name)
                        }
                      >
                        <span className="font-mono text-xs text-slate-400 dark:text-slate-400">
                          {r.ticker}
                        </span>
                        <span>{r.name}</span>
                      </button>
                    ))
                  )}
                </div>
              )}
            </div>
            <Button
              onClick={() => {
                if (searchResults.length > 0) {
                  const first = searchResults[0]
                  handleSelectStock(first.ticker, first.name)
                } else {
                  handleSearch()
                }
              }}
              disabled={searching || !searchQuery.trim()}
              className="bg-slate-200 dark:bg-slate-700 hover:bg-slate-300 dark:hover:bg-slate-600 text-slate-800 dark:text-white"
            >
              {searching ? '搜索中...' : '搜索'}
            </Button>
          </div>

          {/* Selected stock display */}
          {selectedStock && (
            <div className="mt-3 text-sm text-slate-500 dark:text-slate-300">
              已选择：<span className="font-mono text-slate-800 dark:text-white">{selectedStock.ticker}</span>
              {' - '}
              <span className="text-slate-800 dark:text-white">{selectedStock.name}</span>
            </div>
          )}
        </CardContent>
      </Card>

      {/* ── Initial state ── */}
      {!selectedStock && (
        <div className="flex items-center justify-center py-20">
          <p className="text-lg text-slate-500 dark:text-slate-400">
            请搜索并选择一只股票
          </p>
        </div>
      )}

      {/* ── Data loading ── */}
      {selectedStock && dataLoading && chartData.length === 0 && (
        <div className="flex items-center justify-center py-20">
          <p className="text-lg text-slate-500 dark:text-slate-400">正在加载数据...</p>
        </div>
      )}

      {/* ── Error state ── */}
      {selectedStock && dataError && !dataLoading && (
        <div className="flex flex-col items-center justify-center py-20 gap-4">
          <p className="text-red-400">{dataError}</p>
          <p className="text-sm text-slate-500 dark:text-slate-500">
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
            <p className="text-lg text-slate-500 dark:text-slate-400">
              未找到该股票的数据
            </p>
          </div>
        )}

      {/* ── Charts area ── */}
      {chartReady && (
        <div className="relative">
          {toastMsg && (
            <div
              className="absolute top-0 left-1/2 -translate-x-1/2 z-50 px-4 py-2 rounded-lg bg-slate-800/90 text-slate-200 text-sm border border-slate-700 shadow-lg transition-opacity duration-500 pointer-events-none"
              style={{ animation: 'toastFade 2.5s ease-in-out forwards' }}
            >
              {toastMsg}
            </div>
          )}
          {/* Crosshair tooltip */}
          {crosshairData && (
            <div className="absolute top-2 right-2 z-10 bg-slate-900/85 backdrop-blur-sm border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-200 space-y-1 min-w-[160px] shadow-lg pointer-events-none">
              <div className="text-slate-400 font-medium mb-1">{crosshairData.time}</div>
              <div className="flex justify-between gap-3">
                <span>开 <span className="text-slate-200 font-mono">{crosshairData.open.toFixed(2)}</span></span>
                <span>收 <span className={`font-mono ${crosshairData.close >= crosshairData.open ? 'text-red-400' : 'text-green-400'}`}>{crosshairData.close.toFixed(2)}</span></span>
              </div>
              <div className="flex justify-between gap-3">
                <span>量 <span className="text-slate-200 font-mono">{formatVolume(crosshairData.volume)}</span></span>
                <span>额 <span className="text-slate-200 font-mono">{formatAmount(crosshairData.amount)}</span></span>
              </div>
              <div>
                <span>换手 <span className="text-slate-200 font-mono">{crosshairData.turnover != null ? `${crosshairData.turnover.toFixed(2)}%` : '—'}</span></span>
              </div>
              {crosshairData.maValues.length > 0 && (
                <div className="border-t border-slate-700 pt-1 mt-1 flex flex-wrap gap-x-2 gap-y-0.5">
                  {crosshairData.maValues.map(m => (
                    <span key={m.label} style={{ color: m.color }} className="font-mono">
                      {m.label} {m.value.toFixed(2)}
                    </span>
                  ))}
                </div>
              )}
              <div className="border-t border-slate-700 pt-1 mt-1">
                <span className="text-red-400">
                  距高点 {crosshairData.distFromHigh != null ? `${crosshairData.distFromHigh.toFixed(2)}%` : '—'}
                </span>
                <span className="text-green-400 ml-3">
                  距低点 {crosshairData.distFromLow != null ? `+${crosshairData.distFromLow.toFixed(2)}%` : '—'}
                </span>
              </div>
              {crosshairData.sub1Value && (
                <div>
                  {crosshairData.sub1Value.label}
                  <span className="font-mono ml-1">
                    {crosshairData.sub1Value.value?.toFixed(2) ?? '—'}
                  </span>
                </div>
              )}
              {crosshairData.sub2Value && (
                <div>
                  {crosshairData.sub2Value.label}
                  <span className="font-mono ml-1">
                    {crosshairData.sub2Value.value?.toFixed(2) ?? '—'}
                  </span>
                </div>
              )}
            </div>
          )}
          {/* MA toggle buttons */}
          <div className="flex items-center gap-2 flex-wrap">
            {allMAConfigs.map((cfg) => {
              const active = visibleMAs.has(cfg.key)
              const isCustom = cfg.key.startsWith('custom_')
              return (
                <div key={cfg.key} className="flex items-center gap-0.5">
                  <button
                    onClick={() => toggleMA(cfg.key)}
                    className={`px-3 py-1 rounded-full text-xs font-medium transition-colors border ${
                      active
                        ? 'bg-slate-200 dark:bg-slate-700 text-slate-800 dark:text-white border-slate-300 dark:border-slate-600'
                        : 'bg-transparent text-slate-500 border-slate-300 dark:border-slate-700 hover:text-slate-700 dark:hover:text-slate-300 hover:border-slate-400 dark:hover:border-slate-500'
                    }`}
                    style={
                      active
                        ? { borderColor: cfg.color, color: cfg.color }
                        : undefined
                    }
                  >
                    {cfg.label}
                  </button>
                  {isCustom && (
                    <button
                      className="text-xs text-slate-500 hover:text-red-400 ml-0.5"
                      onClick={(e) => {
                        e.stopPropagation()
                        setCustomMAs(prev => prev.filter(m => `custom_${m.period}` !== cfg.key))
                        setVisibleMAs(prev => { const n = new Set(prev); n.delete(cfg.key); return n })
                      }}
                    >
                      ✕
                    </button>
                  )}
                </div>
              )
            })}
            {customMAs.length < 3 && (
              <>
                {isAddingMA ? (
                  <input
                    type="text"
                    inputMode="numeric"
                    placeholder="周期"
                    id="custom-ma-input"
                    className="px-3 py-1 rounded-full text-xs font-medium border bg-transparent border-slate-300 dark:border-slate-700 text-slate-500 dark:text-slate-400 w-14"
                    autoFocus
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        const input = e.currentTarget
                        const val = parseInt(input.value)
                        if (!isNaN(val) && val >= 3 && val <= 999 && !customMAs.some(m => m.period === val)) {
                          const color = DEFAULT_MA_COLORS[customMAs.length % DEFAULT_MA_COLORS.length]
                          setCustomMAs(prev => [...prev, { period: val, color }])
                        }
                        input.value = ''
                        setIsAddingMA(false)
                      }
                      if (e.key === 'Escape') {
                        e.currentTarget.value = ''
                        setIsAddingMA(false)
                      }
                    }}
                    onBlur={(e) => {
                      const val = parseInt(e.currentTarget.value)
                      if (!isNaN(val) && val >= 3 && val <= 999 && !customMAs.some(m => m.period === val)) {
                        const color = DEFAULT_MA_COLORS[customMAs.length % DEFAULT_MA_COLORS.length]
                        setCustomMAs(prev => [...prev, { period: val, color }])
                      }
                      e.currentTarget.value = ''
                      setIsAddingMA(false)
                    }}
                  />
                ) : (
                  <button
                    className="px-3 py-1 rounded-full text-xs font-medium border border-dashed border-indigo-500/40 text-indigo-400/70 hover:text-indigo-300 hover:border-indigo-400/60 bg-transparent transition-colors"
                    onClick={() => setIsAddingMA(true)}
                  >
                    + 自定义MA
                  </button>
                )}
              </>
            )}
          </div>

          {/* Info panel */}
          {sliderStats && (
            <Card className="border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50">
              <CardContent className="p-4">
                <div className="flex items-center gap-6 flex-wrap">
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">个股</span>
                    <span className="ml-2 text-xl font-semibold text-slate-900 dark:text-white">
                      {selectedStock?.name ?? '—'}
                    </span>
                    <span className="ml-2 text-sm text-slate-500 dark:text-slate-400">
                      {selectedStock?.ticker ?? ''}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">最新价</span>
                    <span className="ml-2 text-3xl font-bold text-slate-900 dark:text-white">
                      {sliderStats.lastRow.close?.toFixed(2) ?? '—'}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">涨跌幅</span>
                    <span
                      className={`ml-2 text-xl font-semibold ${pctColor(sliderStats.pctChange)}`}
                    >
                      {formatPct(sliderStats.pctChange)}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">区间最高</span>
                    <span className="ml-2 text-lg font-medium text-slate-900 dark:text-white">
                      {sliderStats.high != null
                        ? sliderStats.high.toFixed(2)
                        : '—'}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">区间最低</span>
                    <span className="ml-2 text-lg font-medium text-slate-900 dark:text-white">
                      {sliderStats.low != null
                        ? sliderStats.low.toFixed(2)
                        : '—'}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">成交量</span>
                    <span className="ml-2 text-lg font-medium text-slate-900 dark:text-white">
                      {formatVolume(sliderStats.lastRow.volume)}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">成交额</span>
                    <span className="ml-2 text-lg font-medium text-slate-900 dark:text-white">
                      {formatAmount(sliderStats.lastRow.amount)}
                    </span>
                  </div>
                </div>
                {/* 进阶指标第二行 */}
                <div className="border-t border-slate-200 dark:border-slate-700 pt-3 mt-3 flex items-center gap-6 flex-wrap">
                  {/* 区间涨幅 */}
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">区间涨幅</span>
                    <span
                      className={`ml-2 text-lg font-medium ${pctColor(sliderStats.rangeGain)}`}
                    >
                      {sliderStats.rangeGain != null ? formatPct(sliderStats.rangeGain) : '—'}
                    </span>
                  </div>
                  {/* 振幅 */}
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">振幅</span>
                    <span className="ml-2 text-lg font-medium text-slate-900 dark:text-white">
                      {sliderStats.rangeAmplitude != null ? `${sliderStats.rangeAmplitude.toFixed(2)}%` : '—'}
                    </span>
                  </div>
                  {/* 均价 */}
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">均价</span>
                    <span className="ml-2 text-lg font-medium text-slate-900 dark:text-white">
                      {sliderStats.vwap != null ? sliderStats.vwap.toFixed(2) : '—'}
                    </span>
                  </div>
                  {/* 量比 */}
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">量比</span>
                    {sliderStats.volumeRatio != null && (
                      <span
                        className={`ml-2 text-lg font-medium ${
                          sliderStats.volumeRatio >= 2.0
                            ? 'text-red-500'
                            : sliderStats.volumeRatio < 0.5
                              ? 'text-orange-500'
                              : 'text-slate-900 dark:text-white'
                        }`}
                      >
                        {sliderStats.volumeRatio.toFixed(2)}
                        {sliderStats.volumeRatio >= 2.0
                          ? ' 放量'
                          : sliderStats.volumeRatio < 0.5
                            ? ' 缩量'
                            : ''}
                      </span>
                    )}
                    {sliderStats.volumeRatio == null && (
                      <span className="ml-2 text-lg font-medium text-slate-400">—</span>
                    )}
                  </div>
                  {/* 价格位置 */}
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">价格位置</span>
                    {sliderStats.pricePosition != null && (
                      <div className="ml-2 flex items-center gap-2">
                        <span className={`text-lg font-medium ${
                          sliderStats.pricePosition < 30
                            ? 'text-green-500'
                            : sliderStats.pricePosition > 70
                              ? 'text-red-500'
                              : 'text-slate-900 dark:text-white'
                        }`}>
                          {sliderStats.pricePosition.toFixed(1)}%
                        </span>
                        <div className="w-6 h-1.5 rounded-full bg-slate-200 dark:bg-slate-700 overflow-hidden">
                          <div
                            className="h-full rounded-full"
                            style={{
                              width: `${Math.max(0, Math.min(100, sliderStats.pricePosition))}%`,
                              backgroundColor:
                                sliderStats.pricePosition < 30
                                  ? '#22c55e'
                                  : sliderStats.pricePosition > 70
                                    ? '#ef4444'
                                    : '#94a3b8',
                            }}
                          />
                        </div>
                      </div>
                    )}
                    {sliderStats.pricePosition == null && (
                      <span className="ml-2 text-lg font-medium text-slate-400">—</span>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Chart containers */}
          <div className="space-y-0">
            {/* Main K-line chart */}
            <div
              ref={mainChartWheelRef}
              className="relative cursor-crosshair select-none touch-none overscroll-contain"
              onPointerDown={handleChartPointerDown}
              onPointerMove={handleChartPointerMove}
              onPointerUp={finishChartSelection}
              onPointerCancel={finishChartSelection}
              onWheel={handleChartWheel}
            >
              <div
                data-chart-control="true"
                className="absolute top-3 right-[72px] z-30 flex items-center gap-2 rounded-lg border border-slate-600/70 bg-slate-950/85 px-2.5 py-1.5 text-xs text-slate-300 shadow-lg backdrop-blur-sm"
                onPointerDown={(event) => event.stopPropagation()}
                onPointerMove={(event) => event.stopPropagation()}
                onPointerUp={(event) => event.stopPropagation()}
                onPointerCancel={(event) => event.stopPropagation()}
                onWheel={(event) => {
                  event.preventDefault()
                  event.stopPropagation()
                }}
              >
                <label htmlFor="stock-review-adjustment" className="text-slate-400">
                  复权
                </label>
                <select
                  id="stock-review-adjustment"
                  value={adjustmentMode}
                  onChange={(event) => setAdjustmentMode(event.target.value as AdjustmentMode)}
                  className="h-7 rounded-md border border-slate-700 bg-slate-900 px-2 text-xs text-slate-100 outline-none hover:border-slate-500 focus:border-indigo-400"
                >
                  {ADJUSTMENT_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </div>
              <div
                ref={(el) => {
                  mainChartContainerRef.current = el
                  if (!el || candleSeries.current) return
                const w = el.clientWidth || 800
                const main = createChart(el, {
                  width: w, height: el.clientHeight || 1600,
                  layout: { background: { type: ColorType.Solid, color: BG_COLOR }, textColor: TEXT_COLOR, attributionLogo: false },
                  grid: { vertLines: { color: GRID_COLOR }, horzLines: { color: GRID_COLOR } },
                  crosshair: {
                    mode: CrosshairMode.Normal,
                    vertLine: { color: '#6366f1', width: 1, style: LineStyle.Dashed, labelBackgroundColor: '#6366f1' },
                    horzLine: { color: '#6366f1', width: 1, style: LineStyle.Dashed, labelBackgroundColor: '#6366f1' },
                  },
                  handleScroll: { pressedMouseMove: false, vertTouchDrag: false, horzTouchDrag: false },
                  handleScale: { mouseWheel: false, pinch: false, axisPressedMouseMove: false },
                  timeScale: { timeVisible: false, secondsVisible: false, tickMarkFormatter: (time: Time) => {
                    const date = typeof time === 'string' ? new Date(time) : new Date((time as number) * 1000)
                    const month = date.getMonth() + 1
                    const day = date.getDate()
                    return `${month}/${day}`
                  } },
                  rightPriceScale: { minimumWidth: SUB_CHART_PRICE_SCALE_WIDTH, entireTextOnly: true },
                })
                const candles = main.addSeries(CandlestickSeries, {
                  upColor: UP_COLOR, downColor: DOWN_COLOR,
                  borderUpColor: WICK_UP_COLOR, borderDownColor: WICK_DOWN_COLOR,
                  wickUpColor: WICK_UP_COLOR, wickDownColor: WICK_DOWN_COLOR,
                  priceLineVisible: false, lastValueVisible: false,
                })
                const maMap = new Map<MAKey, ISeriesApi<'Line'>>()
                for (const cfg of allMAConfigs) {
                  const s = main.addSeries(LineSeries, {
                    color: cfg.color, lineWidth: 1, priceLineVisible: false,
                    lastValueVisible: false,
                    visible: visibleMAs.has(cfg.key),
                  })
                  maMap.set(cfg.key, s)
                }
                mainChart.current = main
                candleSeries.current = candles
                maSeries.current = maMap

                // Crosshair tooltip
                main.subscribeCrosshairMove((param) => {
                  if (!param.time || !param.point) {
                    setCrosshairData(null)
                    return
                  }
                  const cd = param.seriesData.get(candles) as CandlestickData | undefined
                  if (!cd) { setCrosshairData(null); return }

                  const timeStr = typeof param.time === 'string'
                    ? param.time
                    : new Date((param.time as number) * 1000).toISOString().slice(0, 10)

                  const row = chartDataRef.current.find(r => toChartTime(r.trade_date) === timeStr)

                  const maValues: { label: string; value: number; color: string }[] = []
                  const visible = visibleMAsRef.current
                  for (const [key, series] of maSeries.current.entries()) {
                    if (!visible.has(key)) continue
                    const ld = param.seriesData.get(series) as LineData | undefined
                    if (ld !== undefined) {
                      const cfg = maConfigRef.current.find(c => c.key === key)
                      if (cfg) maValues.push({ label: cfg.label, value: ld.value, color: cfg.color })
                    }
                  }

                  const stats = sliderStatsRef.current
                  let distFromHigh: number | null = null
                  let distFromLow: number | null = null
                  if (stats && cd.close != null) {
                    if (stats.high != null && stats.high !== 0) {
                      distFromHigh = ((cd.close - stats.high) / stats.high) * 100
                    }
                    if (stats.low != null && stats.low !== 0) {
                      distFromLow = ((cd.close - stats.low) / stats.low) * 100
                    }
                  }

                  const sub1Indicator = subChart1IndicatorRef.current
                  const sub2Indicator = subChart2IndicatorRef.current

                  setCrosshairData({
                    time: timeStr,
                    open: cd.open,
                    close: cd.close,
                    volume: row?.volume ?? null,
                    amount: row?.amount ?? null,
                    turnover: row?.turnover ?? null,
                    maValues,
                    distFromHigh,
                    distFromLow,
                    sub1Value: {
                      label: getSubIndicatorLabel(sub1Indicator),
                      value: getSubIndicatorValue(sub1Indicator, timeStr, row),
                    },
                    sub2Value: {
                      label: getSubIndicatorLabel(sub2Indicator),
                      value: getSubIndicatorValue(sub2Indicator, timeStr, row),
                    },
                  })
                })

                // ResizeObserver — observe main chart container
                const ro = new ResizeObserver((entries) => {
                  for (const entry of entries) {
                    const { width, height } = entry.contentRect
                    const cw = Math.floor(width)
                    const ch = Math.floor(height)
                    mainChart.current?.resize(cw, ch)
                    volumeChart.current?.resize(cw, 120)
                    pctChart.current?.resize(cw, 120)
                  }
                })
                ro.observe(el)
                resizeObserverRef.current = ro

                // Series markers plugin
                markersPlugin.current = createSeriesMarkers(candles, [])
                if (mainChart.current && volumeChart.current && pctChart.current) {
                  setChartsReady(true)
                }
              }}
                className="w-full rounded-t-lg border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50 overflow-hidden"
                style={{ height: 800 }}
              />
              {selectionBox && (
                <div
                  className="pointer-events-none absolute top-0 bottom-0 z-20 border border-indigo-300/90 bg-indigo-400/20"
                  style={{ left: selectionBox.left, width: selectionBox.width }}
                />
              )}
            </div>

            {/* Volume chart */}
            <div className="relative">
              <select
                value={subChart1Indicator}
                onChange={(e) => setSubChart1Indicator(e.target.value as SubIndicatorKey)}
                className="absolute top-1 left-1 z-10 text-xs bg-slate-900/80 border border-slate-700 rounded px-1.5 py-0.5 text-slate-300 backdrop-blur-sm"
              >
                {SUB_INDICATOR_OPTIONS.map((opt) => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </select>
              <div
                ref={(el) => {
                  if (!el || volumeChart.current) return
                  const w = el.clientWidth || 800
                  const vol = createChart(el, {
                    width: w, height: 120,
                    layout: { background: { type: ColorType.Solid, color: BG_COLOR }, textColor: TEXT_COLOR, attributionLogo: false },
                    grid: { vertLines: { color: GRID_COLOR }, horzLines: { color: GRID_COLOR } },
                    crosshair: {
                      mode: CrosshairMode.Normal,
                      vertLine: { color: '#6366f1', width: 1, style: LineStyle.Dashed, labelBackgroundColor: '#6366f1' },
                      horzLine: { color: '#6366f1', width: 1, style: LineStyle.Dashed, labelBackgroundColor: '#6366f1' },
                    },
                    handleScroll: { pressedMouseMove: false, vertTouchDrag: false, horzTouchDrag: false },
                    handleScale: { mouseWheel: false, pinch: false, axisPressedMouseMove: false },
                    timeScale: { visible: false },
                    rightPriceScale: { minimumWidth: SUB_CHART_PRICE_SCALE_WIDTH, scaleMargins: { top: 0.1, bottom: 0.1 }, entireTextOnly: true },
                  })
                  const hist = vol.addSeries(HistogramSeries, {
                    priceFormat: boundedSubIndicatorPriceFormat('volume'),
                    priceLineVisible: false,
                    lastValueVisible: false,
                  })
                  volumeChart.current = vol
                  volumeSeries.current = hist
                  if (mainChart.current && volumeChart.current && pctChart.current) {
                    setChartsReady(true)
                  }
                }}
                className="w-full border-l border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50 overflow-hidden"
                style={{ height: 120 }}
              />
            </div>

            {/* Pct change chart */}
            <div className="relative">
              <select
                value={subChart2Indicator}
                onChange={(e) => setSubChart2Indicator(e.target.value as SubIndicatorKey)}
                className="absolute top-1 left-1 z-10 text-xs bg-slate-900/80 border border-slate-700 rounded px-1.5 py-0.5 text-slate-300 backdrop-blur-sm"
              >
                {SUB_INDICATOR_OPTIONS.map((opt) => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </select>
              <div
                ref={(el) => {
                  if (!el || pctChart.current) return
                  const w = el.clientWidth || 800
                  const pct = createChart(el, {
                    width: w, height: 120,
                    layout: { background: { type: ColorType.Solid, color: BG_COLOR }, textColor: TEXT_COLOR, attributionLogo: false },
                    grid: { vertLines: { color: GRID_COLOR }, horzLines: { color: GRID_COLOR } },
                    crosshair: {
                      mode: CrosshairMode.Normal,
                      vertLine: { color: '#6366f1', width: 1, style: LineStyle.Dashed, labelBackgroundColor: '#6366f1' },
                      horzLine: { color: '#6366f1', width: 1, style: LineStyle.Dashed, labelBackgroundColor: '#6366f1' },
                    },
                    handleScroll: { pressedMouseMove: false, vertTouchDrag: false, horzTouchDrag: false },
                    handleScale: { mouseWheel: false, pinch: false, axisPressedMouseMove: false },
                    timeScale: { visible: false },
                    rightPriceScale: { minimumWidth: SUB_CHART_PRICE_SCALE_WIDTH, entireTextOnly: true },
                  })
                  const pctLine = pct.addSeries(LineSeries, {
                    color: '#f59e0b', lineWidth: 2, priceLineVisible: false,
                    lastValueVisible: false, crosshairMarkerVisible: true,
                  })
                  pctChart.current = pct
                  pctSeries.current = pctLine
                  if (mainChart.current && volumeChart.current && pctChart.current) {
                    setChartsReady(true)
                  }
                }}
                className="w-full border-l border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50 overflow-hidden"
                style={{ height: 120 }}
              />
            </div>
            {/* TASK 6: Time range slider */}
            {allDates.length > 0 && (
              <div className="w-full rounded-b-lg border border-t-0 border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50 px-4 pt-3 pb-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs text-slate-400">
                    {allDates[Math.max(0, allDates.length - 365)]}
                  </span>
                  <span className="text-xs text-slate-500">时间范围</span>
                  <span className="text-xs text-slate-400">
                    {allDates[allDates.length - 1]}
                  </span>
                </div>
                <Slider
                  range
                  min={0}
                  max={allDates.length - 1}
                  value={sliderValue}
                  onChange={handleSliderChange}
                />
                <div className="flex items-center justify-between mt-1">
                  <span className="text-[11px] text-slate-500" id="slider-start-label">
                    {allDates[sliderValue[0]]}
                  </span>
                  <span className="text-[11px] text-slate-500" id="slider-end-label">
                    {allDates[sliderValue[1]]}
                  </span>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── 市场判读笔记 ── */}
      {selectedStock && chartReady && (
        <Card className="border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50">
          <CardHeader>
            <CardTitle className="text-lg text-slate-900 dark:text-white">市场判读</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Phase select */}
            <div className="flex items-center gap-3">
              <label className="text-sm text-slate-500 dark:text-slate-400 whitespace-nowrap">市场阶段</label>
              <Select value={phase} onValueChange={(value) => setPhase(value ?? '')}>
                <SelectTrigger className="w-40 border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white">
                  <SelectValue placeholder="选择阶段" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="上升期">上升期</SelectItem>
                  <SelectItem value="震荡期">震荡期</SelectItem>
                  <SelectItem value="下降期">下降期</SelectItem>
                  <SelectItem value="混沌期">混沌期</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {/* Checkboxes */}
            <div className="flex items-center gap-6 flex-wrap">
              <label className="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 cursor-pointer select-none">
                <input type="checkbox" checked={M1_core} onChange={(e) => setM1_core(e.target.checked)} className="accent-red-500" />
                <span className="w-2.5 h-2.5 rounded-full bg-red-500 inline-block shrink-0" />
                M1（核心板块形成）
              </label>
              <label className="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 cursor-pointer select-none">
                <input type="checkbox" checked={M2_front} onChange={(e) => setM2_front(e.target.checked)} className="accent-orange-500" />
                <span className="w-2.5 h-2.5 rounded-full bg-orange-500 inline-block shrink-0" />
                M2（前排标的）
              </label>
              <label className="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 cursor-pointer select-none">
                <input type="checkbox" checked={M3_identifiable} onChange={(e) => setM3_identifiable(e.target.checked)} className="accent-yellow-500" />
                <span className="w-2.5 h-2.5 rounded-full bg-yellow-500 inline-block shrink-0" />
                M3（可识别性）
              </label>
              <label className="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 cursor-pointer select-none">
                <input type="checkbox" checked={V_triggered} onChange={(e) => setV_triggered(e.target.checked)} className="accent-green-500" />
                <span className="w-2.5 h-2.5 rounded-full bg-green-500 inline-block shrink-0" />
                V（启动确认）
              </label>
            </div>
            {/* Notes textarea */}
            <textarea
              value={phaseNotes}
              onChange={(e) => setPhaseNotes(e.target.value)}
              placeholder="输入判读笔记..."
              rows={3}
              className="w-full rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white placeholder:text-slate-400 dark:placeholder:text-slate-500 p-2 text-sm resize-y focus:outline-none focus:border-slate-400 dark:focus:border-slate-500"
            />
            {/* Save + toast */}
            <div className="flex items-center gap-3">
              <Button
                onClick={handleSavePhase}
                disabled={phaseSaving}
                className="bg-blue-600 hover:bg-blue-500 text-white"
              >
                {phaseSaving ? '保存中...' : '保存判读'}
              </Button>
              {phaseToast && (
                <div className="px-3 py-1.5 rounded bg-slate-100 dark:bg-slate-700 text-sm text-slate-800 dark:text-white animate-pulse">
                  {phaseToast}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* ── 交易记录 ── */}
      {selectedStock && (
        <Card className="border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50">
          <CardHeader>
            <CardTitle className="text-lg text-slate-900 dark:text-white">交易记录</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {/* New trade button */}
            <Button
              onClick={() => {
                setShowNewTradeForm(!showNewTradeForm)
                if (!showNewTradeForm) {
                  setNewEntryDate(currentDate ?? '')
                }
              }}
              className="bg-slate-200 dark:bg-slate-700 hover:bg-slate-300 dark:hover:bg-slate-600 text-slate-800 dark:text-white"
            >
              {showNewTradeForm ? '取消' : '新建交易'}
            </Button>

            {/* New trade inline form */}
            {showNewTradeForm && (
              <div className="border border-slate-200 dark:border-slate-700 rounded-lg p-3 space-y-3 bg-slate-50 dark:bg-slate-800/50">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">入场日期</label>
                    <Input
                      type="date"
                      value={newEntryDate ? formatDate(newEntryDate) : formatDate(currentDate || '')}
                      onChange={(e) => setNewEntryDate(e.target.value.replace(/-/g, ''))}
                      className="border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm"
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">入场价</label>
                    <Input
                      type="number"
                      step="0.001"
                      value={newEntryPrice}
                      onChange={(e) => setNewEntryPrice(e.target.value)}
                      placeholder="0.00"
                      className="border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm"
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">路径类型</label>
                    <Select value={newPathType} onValueChange={(value) => setNewPathType(value ?? '')}>
                      <SelectTrigger className="w-full border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm">
                        <SelectValue placeholder="选择路径" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="主升">主升</SelectItem>
                        <SelectItem value="轮动">轮动</SelectItem>
                        <SelectItem value="反抽">反抽</SelectItem>
                        <SelectItem value="潜伏">潜伏</SelectItem>
                        <SelectItem value="打板">打板</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">仓位占比 (%)</label>
                    <Input
                      type="number"
                      min="0"
                      max="100"
                      value={newPositionPct}
                      onChange={(e) => setNewPositionPct(e.target.value)}
                      placeholder="0-100"
                      className="border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm"
                    />
                  </div>
                </div>
                <div>
                  <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">备注</label>
                  <textarea
                    value={newTradeNotes}
                    onChange={(e) => setNewTradeNotes(e.target.value)}
                    rows={2}
                    placeholder="备注..."
                    className="w-full rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white placeholder:text-slate-400 dark:placeholder:text-slate-500 p-2 text-sm resize-y focus:outline-none focus:border-slate-400 dark:focus:border-slate-500"
                  />
                </div>
                <div className="flex gap-2">
                  <Button onClick={handleCreateTrade} className="bg-blue-600 hover:bg-blue-500 text-white">
                    提交
                  </Button>
                  <Button onClick={() => setShowNewTradeForm(false)} className="bg-slate-200 dark:bg-slate-600 hover:bg-slate-300 dark:hover:bg-slate-500 text-slate-800 dark:text-white">
                    取消
                  </Button>
                </div>
              </div>
            )}

            {/* Trade list */}
            {stockTrades.length === 0 ? (
              <p className="text-sm text-slate-500 dark:text-slate-500 py-4 text-center">暂无交易记录</p>
            ) : (
              <div className="space-y-2">
                {stockTrades.map((trade) => {
                  const isExpanded = expandedTradeId === trade.trade_id
                  const isEditing = editingTradeId === trade.trade_id
                  const pnl = calcPnl(trade)
                  return (
                    <div key={trade.trade_id} className="border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
                      {/* Summary row */}
                      <button
                        className="w-full flex items-center justify-between px-3 py-2 text-sm text-left hover:bg-slate-100 dark:hover:bg-slate-800/50 transition-colors"
                        onClick={() => setExpandedTradeId(isExpanded ? null : trade.trade_id)}
                      >
                        <div className="flex items-center gap-3">
                          <span className="font-mono text-xs text-slate-400 dark:text-slate-400">{trade.ticker}</span>
                          <span className="text-slate-600 dark:text-slate-300">{trade.entry_date ? formatDate(trade.entry_date) : '—'}</span>
                        </div>
                        <div className="flex items-center gap-4">
                          <span className="text-slate-600 dark:text-slate-300">
                            入场: {trade.entry_price?.toFixed(2) ?? '—'}
                          </span>
                          <span className="text-slate-600 dark:text-slate-300">
                            出场: {trade.exit_price?.toFixed(2) ?? '—'}
                          </span>
                          {pnl !== null && (
                            <span className={`font-mono text-sm ${parseFloat(pnl) >= 0 ? 'text-red-500' : 'text-green-500'}`}>
                              {parseFloat(pnl) >= 0 ? '+' : ''}{pnl}%
                            </span>
                          )}
                          <span className="text-slate-400 dark:text-slate-500">{isExpanded ? '▲' : '▼'}</span>
                        </div>
                      </button>

                      {/* Expanded detail */}
                      {isExpanded && !isEditing && (
                        <div className="border-t border-slate-200 dark:border-slate-700 px-3 py-2 space-y-2 bg-slate-50 dark:bg-slate-800/30">
                          <div className="grid grid-cols-3 gap-3 text-sm">
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">入场日期</span>
                              <p className="text-slate-700 dark:text-slate-300">{trade.entry_date ? formatDate(trade.entry_date) : '—'}</p>
                            </div>
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">入场价</span>
                              <p className="text-slate-700 dark:text-slate-300">{trade.entry_price?.toFixed(3) ?? '—'}</p>
                            </div>
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">路径类型</span>
                              <p className="text-slate-700 dark:text-slate-300">{trade.path_type ?? '—'}</p>
                            </div>
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">仓位占比</span>
                              <p className="text-slate-700 dark:text-slate-300">{trade.position_pct != null ? `${trade.position_pct}%` : '—'}</p>
                            </div>
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">半卖触发价</span>
                              <p className="text-slate-700 dark:text-slate-300">{trade.half_sell_trigger?.toFixed(3) ?? '—'}</p>
                            </div>
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">半卖日期</span>
                              <p className="text-slate-700 dark:text-slate-300">{trade.half_sell_date ? formatDate(trade.half_sell_date) : '—'}</p>
                            </div>
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">半卖价</span>
                              <p className="text-slate-700 dark:text-slate-300">{trade.half_sell_price?.toFixed(3) ?? '—'}</p>
                            </div>
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">出场日期</span>
                              <p className="text-slate-700 dark:text-slate-300">{trade.exit_date ? formatDate(trade.exit_date) : '—'}</p>
                            </div>
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">出场价</span>
                              <p className="text-slate-700 dark:text-slate-300">{trade.exit_price?.toFixed(3) ?? '—'}</p>
                            </div>
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">盈亏</span>
                              <p className={pnl !== null ? (parseFloat(pnl) >= 0 ? 'text-red-500' : 'text-green-500') : 'text-slate-700 dark:text-slate-300'}>
                                {pnl !== null ? `${parseFloat(pnl) >= 0 ? '+' : ''}${pnl}%` : '—'}
                              </p>
                            </div>
                          </div>
                          {trade.notes && (
                            <div>
                              <span className="text-slate-500 dark:text-slate-400 text-xs">备注</span>
                              <p className="text-slate-700 dark:text-slate-300 text-sm">{trade.notes}</p>
                            </div>
                          )}
                          <Button
                            onClick={() => openEditForm(trade)}
                            className="bg-slate-200 dark:bg-slate-600 hover:bg-slate-300 dark:hover:bg-slate-500 text-slate-800 dark:text-white text-xs px-3 py-1"
                          >
                            编辑
                          </Button>
                        </div>
                      )}

                      {/* Edit form */}
                      {isExpanded && isEditing && (
                        <div className="border-t border-slate-200 dark:border-slate-700 px-3 py-2 space-y-3 bg-slate-50 dark:bg-slate-800/30">
                          <div className="grid grid-cols-2 gap-3">
                            <div>
                              <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">半卖触发价</label>
                              <Input
                                type="number"
                                step="0.001"
                                value={editHalfSellTrigger}
                                onChange={(e) => setEditHalfSellTrigger(e.target.value)}
                                className="border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm"
                              />
                            </div>
                            <div>
                              <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">半卖日期</label>
                              <Input
                                type="date"
                                value={editHalfSellDate ? formatDate(editHalfSellDate) : ''}
                                onChange={(e) => setEditHalfSellDate(e.target.value.replace(/-/g, ''))}
                                className="border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm"
                              />
                            </div>
                            <div>
                              <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">半卖价</label>
                              <Input
                                type="number"
                                step="0.001"
                                value={editHalfSellPrice}
                                onChange={(e) => setEditHalfSellPrice(e.target.value)}
                                className="border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm"
                              />
                            </div>
                            <div>
                              <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">出场日期</label>
                              <Input
                                type="date"
                                value={editExitDate ? formatDate(editExitDate) : ''}
                                onChange={(e) => setEditExitDate(e.target.value.replace(/-/g, ''))}
                                className="border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm"
                              />
                            </div>
                            <div>
                              <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">出场价</label>
                              <Input
                                type="number"
                                step="0.001"
                                value={editExitPrice}
                                onChange={(e) => setEditExitPrice(e.target.value)}
                                className="border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm"
                              />
                            </div>
                          </div>
                          <div>
                            <label className="block text-xs text-slate-500 dark:text-slate-400 mb-1">备注</label>
                            <textarea
                              value={editNotes}
                              onChange={(e) => setEditNotes(e.target.value)}
                              rows={2}
                              className="w-full rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white placeholder:text-slate-400 dark:placeholder:text-slate-500 p-2 text-sm resize-y focus:outline-none focus:border-slate-400 dark:focus:border-slate-500"
                            />
                          </div>
                          <div className="flex gap-2">
                            <Button onClick={() => handleEditTrade(trade.trade_id)} className="bg-blue-600 hover:bg-blue-500 text-white">
                              保存
                            </Button>
                            <Button onClick={() => setEditingTradeId(null)} className="bg-slate-200 dark:bg-slate-600 hover:bg-slate-300 dark:hover:bg-slate-500 text-slate-800 dark:text-white">
                              取消
                            </Button>
                          </div>
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}
