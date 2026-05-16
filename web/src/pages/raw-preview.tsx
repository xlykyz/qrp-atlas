import { useState, useEffect, useMemo, useCallback } from 'react'
import { useNavigate, useOutletContext } from 'react-router-dom'
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
import { getDailyByDate } from '@/api/daily'
import type { DailyRow } from '@/types'

// ── helpers ──

function formatPct(v: number | null): string {
  if (v == null) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(2)}%`
}

function formatNum(v: number | null, decimals = 2): string {
  if (v == null) return '—'
  return v.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
}

function pctColor(v: number | null): string {
  if (v == null) return 'text-gray-400'
  if (v > 0) return 'text-red-500'
  if (v < 0) return 'text-green-500'
  return 'text-gray-400'
}

// ── component ──

export default function RawPreview() {
  const navigate = useNavigate()
  const [selectedDate, setSelectedDate] = useState<string>('')
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

  // ── Initial load — fetch today's data directly ──

  useEffect(() => {
    let cancelled = false

    async function initLoad() {
      setLoading(true)
      setError(null)
      try {
        const todayData = await getDailyByDate(todayYMD)
        if (cancelled) return
        if (todayData.length > 0) {
          setSelectedDate(todayYMD)
          setData(todayData)
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
  }, [todayYMD])

  // ── Step 2: Fetch data when selectedDate changes ──

  useEffect(() => {
    if (loading || !selectedDate) return
    // Initial load already fetched data for today, skip redundant fetch
    if (selectedDate === todayYMD && data.length > 0) return

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

  // ── Total count KPI ──

  const totalCount = data.length

  // ── Sort state ──

  type SortKey = 'ticker' | 'name' | 'open' | 'high' | 'low' | 'close' | 'pct_change' | 'pre_close' | 'volume' | 'amount' | 'turnover' | 'market_cap' | 'float_cap'
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
    return [...data].sort((a, b) => {
      const aVal = (a as any)[sortKey] ?? 0
      const bVal = (b as any)[sortKey] ?? 0
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'desc' ? bVal.localeCompare(aVal) : aVal.localeCompare(bVal)
      }
      return sortDir === 'desc' ? (bVal as number) - (aVal as number) : (aVal as number) - (bVal as number)
    })
  }, [data, sortKey, sortDir])

  const isBusy = loading || isDateChanging

  // ── Register header title + controls ──

  const { setPageTitle, setHeaderControls } = useOutletContext<{
    setPageTitle: (t: string) => void
    setHeaderControls: (c: React.ReactNode | null) => void
  }>()

  useEffect(() => {
    setPageTitle('数据库预览')
    setHeaderControls(
      <DatePicker
        value={selectedDate}
        onChange={(v) => setSelectedDate(v)}
        disabled={isBusy}
      />,
    )
    return () => {
      setPageTitle('')
      setHeaderControls(null)
    }
  }, [selectedDate, isBusy, setPageTitle, setHeaderControls])

  // ── Retry ──

  const handleRetry = useCallback(() => {
    setError(null)
    setLoading(true)
    setData([])
    setSelectedDate('')

    getDailyByDate(todayYMD)
      .then((todayData) => {
        if (todayData.length > 0) {
          setSelectedDate(todayYMD)
          setData(todayData)
        }
        setLoading(false)
      })
      .catch((err) => {
        setError(err instanceof Error ? err.message : '数据加载失败')
        setLoading(false)
      })
  }, [todayYMD])

  // ── Row click navigation ──

  const handleRowClick = useCallback(
    (ticker: string) => {
      navigate(`/stock?ticker=${ticker}`)
    },
    [navigate],
  )

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

  if (isEmpty) {
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

      {/* Total count KPI */}
      <div className="mb-6">
        <Card className="inline-block bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-1">股票总数</p>
            <p className="text-2xl font-bold text-slate-900 dark:text-white">{totalCount.toLocaleString()}</p>
          </CardContent>
        </Card>
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
                  <TableHead className="text-slate-500 dark:text-gray-400 font-medium text-xs">
                    trade_date
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('ticker')}
                  >
                    ticker {sortKey === 'ticker' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('name')}
                  >
                    name {sortKey === 'name' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('open')}
                  >
                    open {sortKey === 'open' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('high')}
                  >
                    high {sortKey === 'high' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('low')}
                  >
                    low {sortKey === 'low' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('close')}
                  >
                    close {sortKey === 'close' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('pct_change')}
                  >
                    pct_change(%) {sortKey === 'pct_change' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('pre_close')}
                  >
                    pre_close {sortKey === 'pre_close' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('volume')}
                  >
                    volume(股) {sortKey === 'volume' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('amount')}
                  >
                    amount(元) {sortKey === 'amount' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('turnover')}
                  >
                    turnover(%) {sortKey === 'turnover' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('market_cap')}
                  >
                    market_cap(元) {sortKey === 'market_cap' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead
                    className="text-slate-500 dark:text-gray-400 font-medium text-right text-xs cursor-pointer hover:text-slate-900 dark:hover:text-white"
                    onClick={() => handleSort('float_cap')}
                  >
                    float_cap(元) {sortKey === 'float_cap' ? (sortDir === 'desc' ? '↓' : '↑') : ''}
                  </TableHead>
                  <TableHead className="text-slate-500 dark:text-gray-400 font-medium text-xs text-center">is_st</TableHead>
                  <TableHead className="text-slate-500 dark:text-gray-400 font-medium text-xs text-center">is_limit_up</TableHead>
                  <TableHead className="text-slate-500 dark:text-gray-400 font-medium text-xs text-center">is_limit_down</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedData.map((row, idx) => (
                  <TableRow
                    key={`${row.ticker}-${idx}`}
                    className="border-slate-200 dark:border-slate-800 hover:bg-slate-100 dark:hover:bg-slate-800/50 cursor-pointer"
                    onClick={() => handleRowClick(row.ticker)}
                  >
                    <TableCell className="font-mono text-xs text-slate-600 dark:text-gray-300">
                      {row.trade_date ?? '—'}
                    </TableCell>
                    <TableCell className="font-mono text-xs text-slate-600 dark:text-gray-300">
                      {row.ticker}
                    </TableCell>
                    <TableCell className="text-slate-800 dark:text-gray-200 text-xs">
                      {row.name ?? '—'}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-slate-700 dark:text-gray-200">
                      {row.open != null ? row.open.toFixed(2) : '—'}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-slate-700 dark:text-gray-200">
                      {row.high != null ? row.high.toFixed(2) : '—'}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-slate-700 dark:text-gray-200">
                      {row.low != null ? row.low.toFixed(2) : '—'}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-slate-700 dark:text-gray-200">
                      {row.close != null ? row.close.toFixed(2) : '—'}
                    </TableCell>
                    <TableCell
                      className={`text-right font-mono text-xs ${pctColor(row.pct_change)}`}
                    >
                      {formatPct(row.pct_change)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-slate-700 dark:text-gray-200">
                      {row.pre_close != null ? row.pre_close.toFixed(2) : '—'}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-slate-700 dark:text-gray-200">
                      {formatNum(row.volume, 0)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-slate-700 dark:text-gray-200">
                      {formatNum(row.amount, 0)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-slate-700 dark:text-gray-200">
                      {row.turnover != null ? `${row.turnover.toFixed(2)}%` : '—'}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-slate-700 dark:text-gray-200">
                      {formatNum(row.market_cap, 0)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-slate-700 dark:text-gray-200">
                      {formatNum(row.float_cap, 0)}
                    </TableCell>
                    <TableCell className="text-center text-xs">
                      {row.is_st ? '⚠️' : ''}
                    </TableCell>
                    <TableCell className="text-center text-xs">
                      {row.is_limit_up ? '🚀' : ''}
                    </TableCell>
                    <TableCell className="text-center text-xs">
                      {row.is_limit_down ? '📉' : ''}
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
