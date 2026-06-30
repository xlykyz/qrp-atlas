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

function isNumeric(val: unknown): boolean {
  return typeof val === 'number'
}

function isNumericType(type: string): boolean {
  return ['DOUBLE', 'BIGINT', 'INTEGER', 'DECIMAL', 'FLOAT', 'INT'].includes(type.toUpperCase())
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

  // Sort state
  const [sortKey, setSortKey] = useState<string>('')
  const [sortDir, setSortDir] = useState<'desc' | 'asc'>('desc')

  // ── Column widths ──

  const columnWidths = useMemo(() => {
    if (columns.length === 0 || data.length === 0) return {}
    const widths: Record<string, number> = {}
    for (const col of columns) {
      const sampleValues = data.slice(0, 500).map((r) => r[col.name])
      widths[col.name] = estimateColumnWidth(col, sampleValues)
    }
    return widths
  }, [columns, data])

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

  // ── Fetch data when selectedTable changes ──

  useEffect(() => {
    if (!selectedTable) return
    let cancelled = false

    async function loadData() {
      setLoading(true)
      setError(null)
      setSortKey('')
      setSortDir('desc')
      try {
        const result = await queryTable(selectedTable, 500)
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
  }, [selectedTable])

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
    if (!sortKey) return data
    return [...data].sort((a, b) => {
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
  }, [data, sortKey, sortDir])

  const isBusy = loading || tablesLoading

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

  if (loading) {
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
            <Button onClick={() => setSelectedTable(selectedTable)} className="mt-4" variant="outline">
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
      {/* Total count KPI */}
      <div className="mb-6 flex items-center gap-4">
        <Card className="inline-block bg-slate-100 dark:bg-slate-900 border-slate-200 dark:border-slate-800">
          <CardContent className="p-4">
            <p className="text-sm text-slate-500 dark:text-gray-400 mb-1">总行数</p>
            <p className="text-2xl font-bold text-slate-900 dark:text-white">{total.toLocaleString()}</p>
          </CardContent>
        </Card>
        {loading && <p className="text-sm text-slate-500 dark:text-gray-400">加载中...</p>}
      </div>

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
            style={{ maxHeight: 'calc(100vh - 280px)' }}
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
                          title={col.name}
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
                      key={`row-${idx}`}
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
        </div>
      )}
    </div>
  )
}
