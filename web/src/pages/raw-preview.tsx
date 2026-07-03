import { useState, useEffect, useMemo, useCallback } from 'react'
import { useNavigate, useOutletContext } from 'react-router-dom'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { listTables, queryTable } from '@/api/tables'
import type { ColumnInfo } from '@/api/tables'

// ── helpers ──

type NumericStats = {
  count: number
  min: number
  max: number
  avg: number
  buckets: { label: string; count: number }[]
}

function formatCell(val: unknown): string {
  if (val == null) return '—'
  if (typeof val === 'boolean') return val ? '✓' : ''
  if (typeof val === 'number') {
    if (Number.isInteger(val) && Math.abs(val) > 1e6) return val.toLocaleString()
    if (Math.abs(val) < 10) return val.toFixed(2)
    return val.toLocaleString(undefined, { maximumFractionDigits: 2 })
  }
  return String(val)
}

function formatNumber(val: number): string {
  if (Math.abs(val) >= 1e8) return `${(val / 1e8).toFixed(2)}亿`
  if (Math.abs(val) >= 1e4) return `${(val / 1e4).toFixed(2)}万`
  if (Math.abs(val) < 10) return val.toFixed(2)
  return val.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

function isNumeric(val: unknown): boolean {
  return typeof val === 'number'
}

function isNumericType(type: string): boolean {
  return ['DOUBLE', 'BIGINT', 'INTEGER', 'DECIMAL', 'FLOAT', 'INT'].includes(type.toUpperCase())
}

function valueMatches(row: Record<string, unknown>, keyword: string): boolean {
  const q = keyword.trim().toLowerCase()
  if (!q) return true
  return Object.values(row).some((value) => String(value ?? '').toLowerCase().includes(q))
}

function buildNumericStats(rows: Record<string, unknown>[], columnName: string): NumericStats | null {
  const values = rows
    .map((row) => row[columnName])
    .filter((value): value is number => typeof value === 'number' && Number.isFinite(value))

  if (values.length === 0) return null

  const min = Math.min(...values)
  const max = Math.max(...values)
  const avg = values.reduce((sum, value) => sum + value, 0) / values.length

  if (min === max) {
    return {
      count: values.length,
      min,
      max,
      avg,
      buckets: [{ label: formatNumber(min), count: values.length }],
    }
  }

  const bucketCount = 10
  const step = (max - min) / bucketCount
  const buckets = Array.from({ length: bucketCount }, (_, index) => {
    const start = min + step * index
    const end = index === bucketCount - 1 ? max : start + step
    return {
      label: `${formatNumber(start)}~${formatNumber(end)}`,
      count: 0,
    }
  })

  for (const value of values) {
    const index = Math.min(bucketCount - 1, Math.floor((value - min) / step))
    buckets[index].count += 1
  }

  return { count: values.length, min, max, avg, buckets }
}

/** 根据列类型和内容样本估算列宽 */
function estimateColumnWidth(col: ColumnInfo, sampleValues: unknown[]): number {
  const type = col.type.toUpperCase()
  const name = col.name

  if (type === 'BOOLEAN') return 56
  if (type === 'DATE') return 90
  if (['TIME', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE'].includes(type)) return 140

  if (/_(?:code|id)$/i.test(name) || name === 'ticker' || name === 'index_code') return 100

  if (type === 'BIGINT' || type === 'INTEGER') return 100
  if (isNumericType(type)) return 100

  let maxLen = name.length
  for (const v of sampleValues) {
    const s = formatCell(v)
    if (s.length > maxLen) maxLen = s.length
  }

  const px = Math.min(maxLen * 7.5 + 24, 300)
  return Math.max(px, 90)
}

// ── component ──

export default function RawPreview() {
  const navigate = useNavigate()

  // Table list
  const [tables, setTables] = useState<string[]>([])
  const [selectedTable, setSelectedTable] = useState<string>('daily_market_snapshot')
  const [tablesLoading, setTablesLoading] = useState(true)

  // Table data
  const [columns, setColumns] = useState<ColumnInfo[]>([])
  const [data, setData] = useState<Record<string, unknown>[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [reloadToken, setReloadToken] = useState(0)

  // Query state
  const [page, setPage] = useState(1)
  const [pageInput, setPageInput] = useState('1')
  const [pageSize, setPageSize] = useState(500)
  const [keyword, setKeyword] = useState('')
  const [selectedNumericColumn, setSelectedNumericColumn] = useState<string>('')

  // Sort state
  const [sortKey, setSortKey] = useState<string>('')
  const [sortDir, setSortDir] = useState<'desc' | 'asc'>('desc')

  const offset = (page - 1) * pageSize
  const totalPages = Math.max(1, Math.ceil(total / pageSize))

  function clampPage(value: number): number {
    if (!Number.isFinite(value)) return page
    return Math.min(totalPages, Math.max(1, Math.trunc(value)))
  }

  function handlePageJump() {
    const nextPage = clampPage(Number(pageInput))
    setPage(nextPage)
    setPageInput(String(nextPage))
  }

  // ── Derived metadata ──

  const numericColumns = useMemo(
    () => columns.filter((col) => isNumericType(col.type)),
    [columns],
  )

  const columnTypeCounts = useMemo(() => {
    return columns.reduce<Record<string, number>>((acc, col) => {
      const key = col.type.toUpperCase()
      acc[key] = (acc[key] ?? 0) + 1
      return acc
    }, {})
  }, [columns])

  const filteredData = useMemo(() => {
    if (!keyword.trim()) return data
    return data.filter((row) => valueMatches(row, keyword))
  }, [data, keyword])

  const numericStats = useMemo(() => {
    if (!selectedNumericColumn) return null
    return buildNumericStats(filteredData, selectedNumericColumn)
  }, [filteredData, selectedNumericColumn])

  // ── Column widths ──

  const columnWidths = useMemo(() => {
    if (columns.length === 0 || filteredData.length === 0) return {}
    const widths: Record<string, number> = {}
    for (const col of columns) {
      const sampleValues = filteredData.slice(0, 500).map((r) => r[col.name])
      widths[col.name] = estimateColumnWidth(col, sampleValues)
    }
    return widths
  }, [columns, filteredData])

  // ── Table total width (number) ──

  const tableWidth = useMemo(() => {
    if (columns.length === 0) return 0
    return columns.reduce((acc, col) => acc + (columnWidths[col.name] ?? 120), 0)
  }, [columns, columnWidths])

  // ── Column left offsets for sticky positioning ──

  const columnLeftOffsets = useMemo(() => {
    if (columns.length === 0) return {}
    const offsets: Record<string, number> = {}
    let acc = 0
    for (const col of columns) {
      offsets[col.name] = acc
      acc += columnWidths[col.name] ?? 120
    }
    return offsets
  }, [columns, columnWidths])

  // ── Load table list on mount ──

  useEffect(() => {
    let cancelled = false
    setTablesLoading(true)

    listTables()
      .then((list) => {
        if (cancelled) return
        setTables(list)
        setTablesLoading(false)
      })
      .catch((err) => {
        if (cancelled) return
        setError(err instanceof Error ? err.message : '加载表列表失败')
        setTablesLoading(false)
      })

    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    setPageInput(String(page))
  }, [page])

  // ── Reset page when query shape changes ──

  useEffect(() => {
    setPage(1)
    setKeyword('')
  }, [selectedTable, pageSize])

  // ── Fetch data when selectedTable/page changes ──

  useEffect(() => {
    if (!selectedTable) return
    let cancelled = false

    async function loadData() {
      setLoading(true)
      setError(null)
      setSortKey('')
      setSortDir('desc')
      try {
        const result = await queryTable(selectedTable, pageSize, offset)
        if (cancelled) return
        setColumns(result.columns)
        setData(result.rows)
        setTotal(result.total)
      } catch (err) {
        if (cancelled) return
        setError(err instanceof Error ? err.message : '数据加载失败')
        setData([])
        setColumns([])
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    loadData()
    return () => { cancelled = true }
  }, [selectedTable, pageSize, offset, reloadToken])

  useEffect(() => {
    if (selectedNumericColumn && numericColumns.some((col) => col.name === selectedNumericColumn)) return
    setSelectedNumericColumn(numericColumns[0]?.name ?? '')
  }, [numericColumns, selectedNumericColumn])

  // ── Sort ──

  function handleSort(key: string) {
    if (sortKey === key) {
      setSortDir(prev => (prev === 'desc' ? 'asc' : 'desc'))
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const sortedData = useMemo(() => {
    if (!sortKey) return filteredData
    return [...filteredData].sort((a, b) => {
      const aVal = a[sortKey]
      const bVal = b[sortKey]
      const aNum = (aVal ?? 0) as number
      const bNum = (bVal ?? 0) as number

      if (isNumeric(aVal) && isNumeric(bVal)) {
        return sortDir === 'desc' ? bNum - aNum : aNum - bNum
      }

      const aStr = String(aVal ?? '')
      const bStr = String(bVal ?? '')
      return sortDir === 'desc' ? bStr.localeCompare(aStr, 'zh') : aStr.localeCompare(bStr, 'zh')
    })
  }, [filteredData, sortKey, sortDir])

  const isBusy = loading || tablesLoading
  const shownStart = total === 0 ? 0 : offset + 1
  const shownEnd = Math.min(offset + data.length, total)

  // ── Register header controls ──

  const { setPageTitle, setHeaderControls } = useOutletContext<{
    setPageTitle: (t: string) => void
    setHeaderControls: (c: React.ReactNode | null) => void
  }>()

  useEffect(() => {
    setPageTitle('数据库预览')
    setHeaderControls(
      tables.length > 0 ? (
        <Select
          value={selectedTable}
          onValueChange={(v) => setSelectedTable(v ?? selectedTable)}
          disabled={isBusy}
        >
          <SelectTrigger className="w-64">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {tables.map((t) => (
              <SelectItem key={t} value={t}>
                {t}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      ) : null,
    )
    return () => {
      setPageTitle('')
      setHeaderControls(null)
    }
  }, [selectedTable, tables, isBusy, setPageTitle, setHeaderControls])

  // ── Row click ──

  const handleRowClick = useCallback(
    (ticker: string) => {
      navigate(`/stock?ticker=${ticker}`)
    },
    [navigate],
  )

  const isSnapshotTable = selectedTable === 'daily_market_snapshot'

  // ── Loading state ──

  if (tablesLoading) {
    return (
      <div className="min-h-screen p-6">
        <div className="flex items-center justify-center h-64">
          <p className="text-lg text-slate-500 dark:text-gray-400">加载表列表...</p>
        </div>
      </div>
    )
  }

  if (loading && data.length === 0) {
    return (
      <div className="min-h-screen p-6">
        <div className="flex items-center justify-center h-64">
          <p className="text-lg text-slate-500 dark:text-gray-400">正在加载数据...</p>
        </div>
      </div>
    )
  }

  // ── Error state ──

  if (error && data.length === 0 && !loading) {
    return (
      <div className="min-h-screen p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <p className="text-lg text-red-400">{error}</p>
            <Button onClick={() => setReloadToken((v) => v + 1)} className="mt-4" variant="outline">
              重试
            </Button>
          </div>
        </div>
      </div>
    )
  }

  const isEmpty = !isBusy && data.length === 0

  if (isEmpty) {
    return (
      <div className="min-h-screen p-6">
        <div className="flex items-center justify-center h-64">
          <p className="text-lg text-slate-500 dark:text-gray-400">当前表无数据</p>
        </div>
      </div>
    )
  }

  // ── Determine which columns are sticky (first 2) ──

  const stickyCols = columns.slice(0, 2).map((c) => c.name)

  /** Style for sticky header cells */
  function headerStickyStyle(colName: string): React.CSSProperties {
    const w = columnWidths[colName]
    const left = columnLeftOffsets[colName] ?? 0
    return {
      width: w,
      minWidth: w,
      maxWidth: w,
      left,
      top: 0,
      position: 'sticky',
    }
  }

  /** Style for sticky body cells */
  function bodyStickyStyle(colName: string): React.CSSProperties {
    const w = columnWidths[colName]
    const left = columnLeftOffsets[colName] ?? 0
    return {
      width: w,
      minWidth: w,
      maxWidth: w,
      left,
      position: 'sticky',
    }
  }

  /** Style for non-sticky header cells */
  function headerNormalStyle(colName: string): React.CSSProperties | undefined {
    const w = columnWidths[colName]
    return w ? { width: w, minWidth: w, maxWidth: w, top: 0 } : { top: 0 }
  }

  /** Style for non-sticky body cells */
  function normalStyle(colName: string): React.CSSProperties | undefined {
    const w = columnWidths[colName]
    return w ? { width: w, minWidth: w, maxWidth: w } : undefined
  }

  // ── Main content ──

  return (
    <div className="w-full min-w-0 max-w-full overflow-hidden min-h-screen p-6">
      <div className="mb-4 flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-1 text-xs text-slate-500 dark:text-gray-400">
          当前页搜索
          <input
            value={keyword}
            onChange={(e) => setKeyword(e.target.value)}
            placeholder="代码、名称、日期或任意值"
            className="h-9 w-72 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-900 outline-none focus:border-slate-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white"
          />
        </label>
        <label className="flex flex-col gap-1 text-xs text-slate-500 dark:text-gray-400">
          每页行数
          <Select value={String(pageSize)} onValueChange={(v) => setPageSize(Number(v))} disabled={isBusy}>
            <SelectTrigger className="w-28">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {[100, 200, 500, 1000].map((size) => (
                <SelectItem key={size} value={String(size)}>{size}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
        <label className="flex flex-col gap-1 text-xs text-slate-500 dark:text-gray-400">
          数值分布
          <Select value={selectedNumericColumn} onValueChange={(v) => setSelectedNumericColumn(v ?? '')} disabled={numericColumns.length === 0}>
            <SelectTrigger className="w-48">
              <SelectValue placeholder="无数值列" />
            </SelectTrigger>
            <SelectContent>
              {numericColumns.map((col) => (
                <SelectItem key={col.name} value={col.name}>{col.name}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
        <Button variant="outline" onClick={() => setReloadToken((v) => v + 1)} disabled={isBusy}>
          刷新
        </Button>
      </div>

      <div className="mb-6 grid gap-3 lg:grid-cols-[repeat(3,minmax(0,220px))_minmax(320px,1fr)]">
        <Card className="bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-1">总行数</p>
            <p className="text-2xl font-bold text-slate-900 dark:text-white">{total.toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card className="bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-1">当前窗口</p>
            <p className="text-xl font-semibold text-slate-900 dark:text-white">{shownStart.toLocaleString()}-{shownEnd.toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card className="bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-1">过滤结果</p>
            <p className="text-xl font-semibold text-slate-900 dark:text-white">{sortedData.length.toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card className="bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-2">列类型</p>
            <div className="flex flex-wrap gap-2">
              {Object.entries(columnTypeCounts).map(([type, count]) => (
                <span key={type} className="rounded border border-slate-200 bg-white px-2 py-1 text-xs text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300">
                  {type} x {count}
                </span>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {numericStats && (
        <div className="mb-6 rounded-lg border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-900/50">
          <div className="mb-3 grid gap-3 text-sm md:grid-cols-4">
            <div><span className="text-slate-500 dark:text-slate-400">样本</span><p className="font-mono text-slate-900 dark:text-white">{numericStats.count.toLocaleString()}</p></div>
            <div><span className="text-slate-500 dark:text-slate-400">最小</span><p className="font-mono text-slate-900 dark:text-white">{formatNumber(numericStats.min)}</p></div>
            <div><span className="text-slate-500 dark:text-slate-400">平均</span><p className="font-mono text-slate-900 dark:text-white">{formatNumber(numericStats.avg)}</p></div>
            <div><span className="text-slate-500 dark:text-slate-400">最大</span><p className="font-mono text-slate-900 dark:text-white">{formatNumber(numericStats.max)}</p></div>
          </div>
          <div className="grid gap-1">
            {numericStats.buckets.map((bucket) => {
              const maxCount = Math.max(...numericStats.buckets.map((b) => b.count), 1)
              const width = `${Math.max(2, (bucket.count / maxCount) * 100)}%`
              return (
                <div key={bucket.label} className="grid grid-cols-[minmax(120px,220px)_1fr_56px] items-center gap-2 text-xs">
                  <span className="truncate font-mono text-slate-500 dark:text-slate-400" title={bucket.label}>{bucket.label}</span>
                  <div className="h-5 rounded bg-slate-200 dark:bg-slate-800">
                    <div className="h-5 rounded bg-blue-500/70" style={{ width }} />
                  </div>
                  <span className="text-right font-mono text-slate-600 dark:text-slate-300">{bucket.count}</span>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Error banner */}
      {error && (
        <div className="mb-4 rounded-lg border border-red-300 dark:border-red-800 bg-red-50 dark:bg-red-950/30 p-3 text-sm text-red-600 dark:text-red-400">
          {error}
        </div>
      )}

      {/* Data table */}
      {!isEmpty && (
        <div className="rounded-lg border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900/50 w-full min-w-0 max-w-full overflow-hidden">
          <div
            className="w-full min-w-0 max-w-full overflow-x-auto overflow-y-auto"
            style={{ maxHeight: 'calc(100vh - 390px)' }}
          >
            <div style={{ width: tableWidth, minWidth: '100%' }}>
              <table className="w-full table-fixed caption-bottom text-sm">
                <thead>
                  <tr className="border-b border-slate-200 dark:border-slate-800">
                    {columns.map((col, idx) => {
                      const isSticky = stickyCols.includes(col.name)
                      return (
                        <th
                          key={col.name}
                          style={isSticky ? headerStickyStyle(col.name) : headerNormalStyle(col.name)}
                          className={[
                            'h-10 px-2 text-left align-middle',
                            'sticky top-0 bg-white dark:bg-slate-900',
                            'text-slate-500 dark:text-gray-400 font-medium text-xs',
                            'cursor-pointer hover:text-slate-900 dark:hover:text-white select-none truncate',
                            isNumericType(col.type) ? 'text-right' : '',
                            isSticky ? [
                              idx === 0 ? 'z-50' : 'z-40',
                              idx === 0 ? 'shadow-[1px_0_0_0_rgba(0,0,0,0.08)] dark:shadow-[1px_0_0_0_rgba(255,255,255,0.06)]' : '',
                            ].join(' ') : 'z-30',
                          ].join(' ')}
                          onClick={() => handleSort(col.name)}
                          title={`${col.name} (${col.type})`}
                        >
                          {col.name}
                          {sortKey === col.name ? (sortDir === 'desc' ? ' ↓' : ' ↑') : ''}
                        </th>
                      )
                    })}
                  </tr>
                </thead>
                <tbody>
                  {sortedData.map((row, idx) => (
                    <tr
                      key={`row-${offset + idx}`}
                      className={[
                        'border-b border-slate-200 dark:border-slate-800',
                        'hover:bg-slate-100 dark:hover:bg-slate-800/50',
                        isSnapshotTable ? 'cursor-pointer' : '',
                      ].join(' ')}
                      onClick={() => {
                        if (isSnapshotTable && row.ticker) {
                          handleRowClick(row.ticker as string)
                        }
                      }}
                    >
                      {columns.map((col, colIdx) => {
                        const val = row[col.name]
                        const isSticky = stickyCols.includes(col.name)

                        return (
                          <td
                            key={col.name}
                            style={isSticky ? bodyStickyStyle(col.name) : normalStyle(col.name)}
                            className={[
                              'p-2 align-middle',
                              'font-mono text-xs truncate',
                              isNumericType(col.type) ? 'text-right' : '',
                              col.type === 'DATE' ? 'text-slate-500 dark:text-gray-400' : '',
                              col.type === 'BOOLEAN' ? 'text-center' : 'text-slate-700 dark:text-gray-200',
                              isSticky ? [
                                colIdx === 0 ? 'z-20 bg-slate-50 dark:bg-slate-900' : 'z-10 bg-slate-50 dark:bg-slate-900',
                                colIdx === 0 ? 'shadow-[1px_0_0_0_rgba(0,0,0,0.08)] dark:shadow-[1px_0_0_0_rgba(255,255,255,0.06)]' : '',
                              ].join(' ') : '',
                            ].join(' ')}
                            title={formatCell(val)}
                          >
                            {formatCell(val)}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
          <div className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-200 px-4 py-3 text-sm dark:border-slate-800">
            <span className="text-slate-500 dark:text-slate-400">
              第 {page}/{totalPages} 页，当前载入 {data.length.toLocaleString()} 行
            </span>
            <div className="flex flex-wrap items-center gap-2">
              <Button variant="outline" size="sm" disabled={page <= 1 || isBusy} onClick={() => setPage((p) => Math.max(1, p - 1))}>
                上一页
              </Button>
              <div className="flex items-center gap-1 text-slate-500 dark:text-slate-400">
                <span>跳到</span>
                <input
                  value={pageInput}
                  onChange={(e) => setPageInput(e.target.value.replace(/\D/g, ''))}
                  onBlur={handlePageJump}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handlePageJump()
                  }}
                  disabled={isBusy}
                  className="h-7 w-16 rounded border border-slate-200 bg-white px-2 text-center font-mono text-sm text-slate-900 outline-none focus:border-slate-400 disabled:opacity-60 dark:border-slate-700 dark:bg-slate-900 dark:text-white"
                />
                <span>页</span>
              </div>
              <Button variant="outline" size="sm" disabled={isBusy} onClick={handlePageJump}>
                跳转
              </Button>
              <Button variant="outline" size="sm" disabled={page >= totalPages || isBusy} onClick={() => setPage((p) => Math.min(totalPages, p + 1))}>
                下一页
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}