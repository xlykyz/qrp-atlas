import { useState, useEffect, useMemo, useCallback } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { getDailyByDate, getDailyByDateRange } from '@/api/daily'
import type { DailyRow } from '@/types'

// ── helpers ──

function formatDate(ymd: string): string {
  if (ymd.length === 8) {
    return `${ymd.slice(0, 4)}-${ymd.slice(4, 6)}-${ymd.slice(6, 8)}`
  }
  return ymd
}

function formatPct(v: number | null): string {
  if (v == null) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(2)}%`
}

function formatAmount(v: number | null): string {
  if (v == null) return '—'
  return `${(v / 1e8).toFixed(2)}亿`
}

function pctColor(v: number | null): string {
  if (v == null) return 'text-gray-400'
  if (v > 0) return 'text-red-500'
  if (v < 0) return 'text-green-500'
  return 'text-gray-400'
}

// ── component ──

export default function Overview() {
  const [selectedDate, setSelectedDate] = useState<string>('')
  const [availableDates, setAvailableDates] = useState<string[]>([])
  const [data, setData] = useState<DailyRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [isDateChanging, setIsDateChanging] = useState(false)

  // Build today and reference dates
  const now = useMemo(() => new Date(), [])
  const todayYMD = useMemo(
    () =>
      `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}`,
    [now],
  )

  // ── Step 1: Initial load — discover available dates ──

  useEffect(() => {
    let cancelled = false

    async function initLoad() {
      setLoading(true)
      setError(null)
      try {
        const past = new Date(now)
        past.setDate(past.getDate() - 60)
        const startDate =
          `${past.getFullYear()}${String(past.getMonth() + 1).padStart(2, '0')}${String(past.getDate()).padStart(2, '0')}`

        // Fetch a broad range to discover which dates have data
        const rangeData = await getDailyByDateRange(startDate, todayYMD, 2000)
        if (cancelled) return

        const dates = [...new Set(rangeData.map((r) => r.trade_date))].sort().reverse()
        const top20 = dates.slice(0, 20)

        if (top20.length > 0) {
          setAvailableDates(top20)
          const latest = top20[0]
          setSelectedDate(latest)
          // Use data already in the range response
          setData(rangeData.filter((r) => r.trade_date === latest))
        } else {
          // Fallback: try fetching today directly
          try {
            const todayData = await getDailyByDate(todayYMD)
            if (cancelled) return
            if (todayData.length > 0) {
              setData(todayData)
              setAvailableDates([todayYMD])
              setSelectedDate(todayYMD)
            }
          } catch {
            if (cancelled) return
            // Still no data — stays empty
          }
        }
      } catch (err) {
        if (cancelled) return
        setError(err instanceof Error ? err.message : '数据加载失败')
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    initLoad()
    return () => { cancelled = true }
  }, [now, todayYMD])

  // ── Step 2: Fetch data when selectedDate changes (after initial load) ──

  useEffect(() => {
    if (loading || !selectedDate) return
    // If availableDates was just populated, the first date's data is already in `data`
    // For subsequent date changes, fetch fresh data
    const isFirstDate = availableDates.length > 0 && selectedDate === availableDates[0]
    if (isFirstDate && data.length > 0) return

    let cancelled = false

    async function fetchByDate() {
      setIsDateChanging(true)
      setError(null)
      try {
        const rows = await getDailyByDate(selectedDate)
        if (!cancelled) setData(rows)
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : '数据加载失败')
          setData([])
        }
      } finally {
        if (!cancelled) setIsDateChanging(false)
      }
    }

    fetchByDate()
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedDate])

  // ── KPIs ──

  const kpis = useMemo(() => {
    const total = data.length
    const upCount = data.filter((r) => (r.pct_change ?? 0) > 0).length
    const downCount = data.filter((r) => (r.pct_change ?? 0) < 0).length
    const limitUpCount = data.filter((r) => r.is_limit_up).length
    return {
      total,
      upCount,
      upRatio: total > 0 ? (upCount / total) * 100 : 0,
      downCount,
      downRatio: total > 0 ? (downCount / total) * 100 : 0,
      limitUpCount,
    }
  }, [data])

  // ── Sorted data ──

  const sortedData = useMemo(
    () => [...data].sort((a, b) => (b.pct_change ?? 0) - (a.pct_change ?? 0)),
    [data],
  )

  const isBusy = loading || isDateChanging

  // ── Retry ──

  const handleRetry = useCallback(() => {
    setError(null)
    setLoading(true)
    setData([])
    setAvailableDates([])
    setSelectedDate('')

    const past = new Date()
    past.setDate(past.getDate() - 60)
    const startDate =
      `${past.getFullYear()}${String(past.getMonth() + 1).padStart(2, '0')}${String(past.getDate()).padStart(2, '0')}`

    getDailyByDateRange(startDate, todayYMD, 2000)
      .then((rangeData) => {
        const dates = [...new Set(rangeData.map((r) => r.trade_date))].sort().reverse()
        const top20 = dates.slice(0, 20)
        if (top20.length > 0) {
          setAvailableDates(top20)
          const latest = top20[0]
          setSelectedDate(latest)
          setData(rangeData.filter((r) => r.trade_date === latest))
        }
        setLoading(false)
      })
      .catch((err) => {
        setError(err instanceof Error ? err.message : '数据加载失败')
        setLoading(false)
      })
  }, [todayYMD])

  // ── Loading state ──

  if (loading) {
    return (
      <div className="bg-slate-950 text-white min-h-screen p-6">
        <div className="flex items-center justify-center h-64">
          <p className="text-lg text-gray-400">正在加载数据...</p>
        </div>
      </div>
    )
  }

  // ── Error state (no data at all) ──

  if (error && data.length === 0 && !isDateChanging) {
    return (
      <div className="bg-slate-950 text-white min-h-screen p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <p className="text-lg text-red-400">
              数据加载失败，请确认后端服务是否运行
            </p>
            <p className="mt-2 text-sm text-gray-500">{error}</p>
            <Button onClick={handleRetry} className="mt-4" variant="outline">
              重试
            </Button>
          </div>
        </div>
      </div>
    )
  }

  // ── Empty state ──

  const isEmpty = !isBusy && data.length === 0

  if (isEmpty && availableDates.length === 0) {
    return (
      <div className="bg-slate-950 text-white min-h-screen p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <p className="text-lg text-gray-400">当前日期无数据</p>
            <Button onClick={handleRetry} className="mt-4" variant="outline">
              重试
            </Button>
          </div>
        </div>
      </div>
    )
  }

  // ── Main content ──

  return (
    <div className="bg-slate-950 text-white min-h-screen p-6">
      {/* Header + Date selector */}
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">今日概览</h1>
        <Select
          value={selectedDate}
          onValueChange={(v) => setSelectedDate(v)}
          disabled={isBusy}
        >
          <SelectTrigger className="w-40">
            <SelectValue placeholder="选择日期" />
          </SelectTrigger>
          <SelectContent>
            {availableDates.map((d) => (
              <SelectItem key={d} value={d}>
                {formatDate(d)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Loading overlay during date switch */}
      {isDateChanging && (
        <div className="flex items-center justify-center py-8">
          <p className="text-gray-400">正在加载数据...</p>
        </div>
      )}

      {/* Error banner (non-fatal, data still present) */}
      {error && (
        <div className="mb-4 rounded-lg border border-red-800 bg-red-950/30 p-3 text-sm text-red-400">
          {error}
          <button
            className="ml-3 underline hover:text-red-300"
            onClick={handleRetry}
          >
            重试
          </button>
        </div>
      )}

      {/* KPI cards */}
      <div className="grid grid-cols-4 gap-4 mb-6">
        <Card className="bg-slate-900 border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-gray-400 mb-1">股票总数</p>
            <p className="text-2xl font-bold">{kpis.total.toLocaleString()}</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-900 border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-gray-400 mb-1">上涨</p>
            <p className="text-2xl font-bold text-red-500">
              {kpis.upCount.toLocaleString()}
              <span className="text-sm ml-1 font-normal">
                ({kpis.upRatio.toFixed(1)}%)
              </span>
            </p>
          </CardContent>
        </Card>

        <Card className="bg-slate-900 border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-gray-400 mb-1">下跌</p>
            <p className="text-2xl font-bold text-green-500">
              {kpis.downCount.toLocaleString()}
              <span className="text-sm ml-1 font-normal">
                ({kpis.downRatio.toFixed(1)}%)
              </span>
            </p>
          </CardContent>
        </Card>

        <Card className="bg-slate-900 border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-gray-400 mb-1">涨停</p>
            <p className="text-2xl font-bold">
              🚀 {kpis.limitUpCount.toLocaleString()}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Empty state inside table area */}
      {isEmpty && (
        <div className="flex items-center justify-center py-16">
          <p className="text-gray-400">当前日期无数据</p>
        </div>
      )}

      {/* Data table */}
      {!isEmpty && (
        <div className="rounded-lg border border-slate-800 bg-slate-900/50">
          <div className="max-h-[calc(100vh-280px)] overflow-y-auto">
            <Table>
              <TableHeader className="sticky top-0 bg-slate-900 z-10">
                <TableRow className="border-slate-800">
                  <TableHead className="text-gray-400 font-medium">代码</TableHead>
                  <TableHead className="text-gray-400 font-medium">名称</TableHead>
                  <TableHead className="text-gray-400 font-medium text-right">收盘价</TableHead>
                  <TableHead className="text-gray-400 font-medium text-right">涨跌幅</TableHead>
                  <TableHead className="text-gray-400 font-medium text-right">成交额</TableHead>
                  <TableHead className="text-gray-400 font-medium text-right">换手率</TableHead>
                  <TableHead className="text-gray-400 font-medium text-center">涨停</TableHead>
                  <TableHead className="text-gray-400 font-medium text-center">跌停</TableHead>
                  <TableHead className="text-gray-400 font-medium text-center">ST</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedData.map((row, idx) => (
                  <TableRow
                    key={`${row.ticker}-${idx}`}
                    className="border-slate-800 hover:bg-slate-800/50"
                  >
                    <TableCell className="font-mono text-xs text-gray-300">
                      {row.ticker}
                    </TableCell>
                    <TableCell className="text-gray-200">
                      {row.name ?? '—'}
                    </TableCell>
                    <TableCell className="text-right font-mono text-gray-200">
                      {row.close != null ? row.close.toFixed(2) : '—'}
                    </TableCell>
                    <TableCell
                      className={`text-right font-mono ${pctColor(row.pct_change)}`}
                    >
                      {formatPct(row.pct_change)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-gray-200">
                      {formatAmount(row.amount)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-gray-200">
                      {row.turnover != null ? `${row.turnover.toFixed(2)}%` : '—'}
                    </TableCell>
                    <TableCell className="text-center text-lg">
                      {row.is_limit_up ? '🚀' : ''}
                    </TableCell>
                    <TableCell className="text-center text-lg">
                      {row.is_limit_down ? '📉' : ''}
                    </TableCell>
                    <TableCell className="text-center text-lg">
                      {row.is_st ? '⚠️' : ''}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </div>
      )}
    </div>
  )
}
