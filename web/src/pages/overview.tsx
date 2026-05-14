import { useState, useEffect, useMemo, useCallback } from 'react'
import { useOutletContext } from 'react-router-dom'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { DatePicker } from '@/components/date-picker'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { getDailyByDate, getDailyByDateRange, getDailyDates } from '@/api/daily'
import type { DailyRow } from '@/types'

// ── helpers ──

function formatPct(v: number | null): string {
  if (v == null) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(2)}%`
}

function formatAmount(v: number | null): string {
  if (v == null) return '—'
  // tushare amount 单位是千元，转为亿元
  return `${(v / 100000).toFixed(2)}亿`
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

  // Board filter state
  const [boardFilters, setBoardFilters] = useState<Record<string, boolean>>({
    '上证主板': true,
    '深证主板': true,
    '创业板': true,
    '科创板': true,
    '北交所': false,
    'ST': false,
  })

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

        // Fetch trade dates from trading_calendar
        const dateList = await getDailyDates(startDate, todayYMD, 20)
        if (cancelled) return

        if (dateList.length > 0) {
          setAvailableDates(dateList)
          const latest = dateList[0]
          setSelectedDate(latest)
          // 用最新日期获取完整数据
          const todayData = await getDailyByDate(latest)
          if (cancelled) return
          setData(todayData)
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

  // ── Board filters ──

  const boardOptions = ['上证主板', '深证主板', '创业板', '科创板', '北交所', 'ST']

  const filteredData = useMemo(() => {
    return data.filter(row => {
      // ST 特殊处理
      const stOk = boardFilters['ST'] || !row.is_st
      const boardOk = boardFilters[row.board ?? ''] ?? false
      return stOk && boardOk
    })
  }, [data, boardFilters])

  // ── KPIs ──

  const kpis = useMemo(() => {
    const total = filteredData.length
    const upCount = filteredData.filter((r) => (r.pct_change ?? 0) > 0).length
    const downCount = filteredData.filter((r) => (r.pct_change ?? 0) < 0).length
    const limitUpCount = filteredData.filter((r) => r.is_limit_up).length
    const limitDownCount = filteredData.filter((r) => r.is_limit_down).length
    return {
      total,
      upCount,
      upRatio: total > 0 ? (upCount / total) * 100 : 0,
      downCount,
      downRatio: total > 0 ? (downCount / total) * 100 : 0,
      limitUpCount,
      limitDownCount,
    }
  }, [filteredData])

  // ── Sort state ──

  type SortKey = 'ticker' | 'name' | 'close' | 'pct_change' | 'pct_5d' | 'pct_10d' | 'pct_20d' | 'amount' | 'turnover'
  type SortDir = 'desc' | 'asc'

  const [sortKey, setSortKey] = useState<SortKey>('pct_change')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  function handleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir(prev => (prev === 'desc' ? 'asc' : 'desc'))
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const sortedData = useMemo(() => {
    return [...filteredData].sort((a, b) => {
      const aVal = (a as any)[sortKey] ?? 0
      const bVal = (b as any)[sortKey] ?? 0
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'desc' ? bVal.localeCompare(aVal) : aVal.localeCompare(bVal)
      }
      return sortDir === 'desc' ? (bVal as number) - (aVal as number) : (aVal as number) - (bVal as number)
    })
  }, [filteredData, sortKey, sortDir])

  const isBusy = loading || isDateChanging

  // ── Register header title + controls ──

  const { setPageTitle, setHeaderControls } = useOutletContext<{
    setPageTitle: (t: string) => void
    setHeaderControls: (c: React.ReactNode | null) => void
  }>()

  useEffect(() => {
    setPageTitle('今日概览')
    setHeaderControls(
      <DatePicker
        value={selectedDate}
        onChange={(v) => setSelectedDate(v)}
        availableDates={availableDates}
        disabled={isBusy}
      />,
    )
    return () => {
      setPageTitle('')
      setHeaderControls(null)
    }
  }, [selectedDate, availableDates, isBusy, setPageTitle, setHeaderControls])

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

    getDailyDates(startDate, todayYMD, 20)
      .then((dateList) => {
        if (dateList.length > 0) {
          setAvailableDates(dateList)
          const latest = dateList[0]
          setSelectedDate(latest)
          return getDailyByDate(latest)
        }
        return []
      })
      .then((todayData) => {
        if (todayData) setData(todayData)
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
      <div className="min-h-screen p-6">
        <div className="flex items-center justify-center h-64">
          <p className="text-lg text-slate-500 dark:text-gray-400">正在加载数据...</p>
        </div>
      </div>
    )
  }

  // ── Error state (no data at all) ──

  if (error && data.length === 0 && !isDateChanging) {
    return (
      <div className="min-h-screen p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <p className="text-lg text-red-400">
              数据加载失败，请确认后端服务是否运行
            </p>
            <p className="mt-2 text-sm text-slate-500 dark:text-gray-500">{error}</p>
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
      <div className="min-h-screen p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <p className="text-lg text-slate-500 dark:text-gray-400">当前日期无数据</p>
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
    <div className="min-h-screen p-6">
      {/* Loading overlay during date switch */}
      {isDateChanging && (
        <div className="flex items-center justify-center py-8">
          <p className="text-slate-500 dark:text-gray-400">正在加载数据...</p>
        </div>
      )}

      {/* Error banner (non-fatal, data still present) */}
      {error && (
        <div className="mb-4 rounded-lg border border-red-300 dark:border-red-800 bg-red-50 dark:bg-red-950/30 p-3 text-sm text-red-600 dark:text-red-400">
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
      <div className="grid grid-cols-5 gap-4 mb-6">
        <Card className="bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-1">股票总数</p>
            <p className="text-2xl font-bold text-slate-900 dark:text-white">{kpis.total.toLocaleString()}</p>
          </CardContent>
        </Card>

        <Card className="bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-1">上涨</p>
            <p className="text-2xl font-bold text-red-500">
              {kpis.upCount.toLocaleString()}
              <span className="text-sm ml-1 font-normal">
                ({kpis.upRatio.toFixed(1)}%)
              </span>
            </p>
          </CardContent>
        </Card>

        <Card className="bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-1">下跌</p>
            <p className="text-2xl font-bold text-green-500">
              {kpis.downCount.toLocaleString()}
              <span className="text-sm ml-1 font-normal">
                ({kpis.downRatio.toFixed(1)}%)
              </span>
            </p>
          </CardContent>
        </Card>

        <Card className="bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-1">涨停 🚀</p>
            <p className="text-2xl font-bold text-slate-900 dark:text-white">
              {kpis.limitUpCount.toLocaleString()}
            </p>
          </CardContent>
        </Card>

        <Card className="bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-1">跌停 📉</p>
            <p className="text-2xl font-bold text-slate-900 dark:text-white">
              {kpis.limitDownCount.toLocaleString()}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Board filters */}
      <div className="flex items-center gap-2 mb-4 flex-wrap">
        {boardOptions.map(board => (
          <label
            key={board}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm cursor-pointer transition-colors ${
              boardFilters[board]
                ? 'bg-slate-200 dark:bg-slate-700 text-slate-900 dark:text-white'
                : 'bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400 hover:bg-slate-200 dark:hover:bg-slate-700'
            }`}
          >
            <input
              type="checkbox"
              checked={boardFilters[board]}
              onChange={() => setBoardFilters(prev => ({ ...prev, [board]: !prev[board] }))}
              className="sr-only"
            />
            {board}
          </label>
        ))}
      </div>

      {/* Empty state inside table area */}
      {isEmpty && (
        <div className="flex items-center justify-center py-16">
          <p className="text-slate-500 dark:text-gray-400">当前日期无数据</p>
        </div>
      )}

      {/* Data table */}
      {!isEmpty && (
        <div className="rounded-lg border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900/50">
          <div className="max-h-[calc(100vh-280px)] overflow-y-auto">
            <Table>
              <TableHeader className="sticky top-0 bg-white dark:bg-slate-900 z-10">
                <TableRow className="border-slate-200 dark:border-slate-800">
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('ticker')}
                  >
                    代码 {sortKey === 'ticker' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('name')}
                  >
                    名称 {sortKey === 'name' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('close')}
                  >
                    收盘价 {sortKey === 'close' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('pct_change')}
                  >
                    涨跌幅 {sortKey === 'pct_change' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('pct_5d')}
                  >
                    5日 {sortKey === 'pct_5d' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('pct_10d')}
                  >
                    10日 {sortKey === 'pct_10d' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('pct_20d')}
                  >
                    20日 {sortKey === 'pct_20d' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('amount')}
                  >
                    成交额 {sortKey === 'amount' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('turnover')}
                  >
                    换手率 {sortKey === 'turnover' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead className="text-slate-500 dark:text-gray-400 font-medium text-center">涨跌停</TableHead>
                  <TableHead className="text-slate-500 dark:text-gray-400 font-medium text-center">ST</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedData.map((row, idx) => (
                  <TableRow
                    key={`${row.ticker}-${idx}`}
                    className="border-slate-200 dark:border-slate-800 hover:bg-slate-100 dark:hover:bg-slate-800/50"
                  >
                    <TableCell className="font-mono text-xs text-slate-600 dark:text-gray-300">
                      {row.ticker}
                    </TableCell>
                    <TableCell className="text-slate-800 dark:text-gray-200">
                      {row.name ?? '—'}
                    </TableCell>
                    <TableCell className="text-right font-mono text-slate-700 dark:text-gray-200">
                      {row.close != null ? row.close.toFixed(2) : '—'}
                    </TableCell>
                    <TableCell
                      className={`text-right font-mono ${pctColor(row.pct_change)}`}
                    >
                      {formatPct(row.pct_change)}
                    </TableCell>
                    <TableCell
                      className={`text-right font-mono ${pctColor(row.pct_5d)}`}
                    >
                      {formatPct(row.pct_5d)}
                    </TableCell>
                    <TableCell
                      className={`text-right font-mono ${pctColor(row.pct_10d)}`}
                    >
                      {formatPct(row.pct_10d)}
                    </TableCell>
                    <TableCell
                      className={`text-right font-mono ${pctColor(row.pct_20d)}`}
                    >
                      {formatPct(row.pct_20d)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-slate-700 dark:text-gray-200">
                      {formatAmount(row.amount)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-slate-700 dark:text-gray-200">
                      {row.turnover != null ? `${row.turnover.toFixed(2)}%` : '—'}
                    </TableCell>
                    <TableCell className="text-center text-lg">
                      {row.is_limit_up ? '🚀' : row.is_limit_down ? '📉' : ''}
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
