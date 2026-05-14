import { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { getDailyByDate, getDailyByTicker } from '@/api/daily'
import { getPhase, createPhase } from '@/api/phase'
import { getTrades, createTrade, updateTrade } from '@/api/trades'
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

  // ── state: phase notes ──
  const [phaseRecord, setPhaseRecord] = useState<PhaseRecord | null>(null)
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

  // ── current date for phase ──
  const currentDate = useMemo(() => {
    if (chartData.length > 0) return chartData[chartData.length - 1].trade_date
    return endDate
  }, [chartData, endDate])

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
      <Card className="border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50">
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
                className="border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white placeholder:text-slate-400 dark:placeholder:text-slate-500"
              />
              {/* Search dropdown */}
              {searchResults.length > 0 && (
                <div className="absolute z-50 mt-1 w-full rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 shadow-lg max-h-60 overflow-y-auto">
                  {searchResults.map((r) => (
                    <button
                      key={r.ticker}
                      className="w-full px-3 py-2 text-left text-sm text-slate-700 dark:text-gray-200 hover:bg-slate-100 dark:hover:bg-slate-700 transition-colors flex items-center gap-2"
                      onClick={() =>
                        handleSelectStock(r.ticker, r.name ?? r.ticker)
                      }
                    >
                      <span className="font-mono text-xs text-slate-400 dark:text-slate-400">
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

      {/* ── Date range ── */}
      {selectedStock && (
        <Card className="border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50">
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <label className="text-sm text-slate-500 dark:text-slate-400">开始日期</label>
              <Input
                type="date"
                value={formatDate(startDate)}
                onChange={(e) => {
                  const v = e.target.value.replace(/-/g, '')
                  setStartDate(v)
                }}
                className="w-40 border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white"
              />
              <label className="text-sm text-slate-500 dark:text-slate-400">结束日期</label>
              <Input
                type="date"
                value={formatDate(endDate)}
                onChange={(e) => {
                  const v = e.target.value.replace(/-/g, '')
                  setEndDate(v)
                }}
                className="w-40 border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-900 dark:text-white"
              />
            </div>
          </CardContent>
        </Card>
      )}

      {/* ── Initial state ── */}
      {!selectedStock && (
        <div className="flex items-center justify-center py-20">
          <p className="text-lg text-slate-500 dark:text-slate-400">
            请搜索并选择一只股票
          </p>
        </div>
      )}

      {/* ── Data loading ── */}
      {selectedStock && dataLoading && (
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
              )
            })}
          </div>

          {/* Info panel */}
          {latestRow && (
            <Card className="border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50">
              <CardContent className="p-4">
                <div className="flex items-center gap-6 flex-wrap">
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">最新价</span>
                    <span className="ml-2 text-3xl font-bold text-slate-900 dark:text-white">
                      {latestRow.close?.toFixed(2) ?? '—'}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">涨跌幅</span>
                    <span
                      className={`ml-2 text-xl font-semibold ${pctColor(latestRow.pct_change)}`}
                    >
                      {formatPct(latestRow.pct_change)}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">区间最高</span>
                    <span className="ml-2 text-lg font-medium text-slate-900 dark:text-white">
                      {highInRange !== -Infinity
                        ? highInRange.toFixed(2)
                        : '—'}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">区间最低</span>
                    <span className="ml-2 text-lg font-medium text-slate-900 dark:text-white">
                      {lowInRange !== Infinity
                        ? lowInRange.toFixed(2)
                        : '—'}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">成交量</span>
                    <span className="ml-2 text-lg font-medium text-slate-900 dark:text-white">
                      {formatVolume(latestRow.volume)}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-slate-500 dark:text-slate-400">成交额</span>
                    <span className="ml-2 text-lg font-medium text-slate-900 dark:text-white">
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
              className="w-full rounded-t-lg border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50 overflow-hidden"
              style={{ height: 400 }}
            />
            <div
              ref={volumeChartRef}
              className="w-full border-l border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50 overflow-hidden"
              style={{ height: 120 }}
            />
            <div
              ref={pctChartRef}
              className="w-full rounded-b-lg border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900/50 overflow-hidden"
              style={{ height: 80 }}
            />
          </div>
        </>
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
              <Select value={phase} onValueChange={setPhase}>
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
                      value={newEntryDate ? formatDate(newEntryDate) : formatDate(currentDate || endDate)}
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
                    <Select value={newPathType} onValueChange={setNewPathType}>
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
