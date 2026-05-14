import { useState, useEffect, useMemo, useCallback } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { getPhaseByDateRange } from '@/api/phase'
import { getTrades } from '@/api/trades'
import type { PhaseRecord, TradeRecord } from '@/types'

// ── helpers ──

function fmtDate(ymd: string): string {
  if (ymd.length === 8) return `${ymd.slice(0, 4)}-${ymd.slice(4, 6)}-${ymd.slice(6, 8)}`
  return ymd
}

function toApiDate(v: string): string {
  return v.replace(/-/g, '')
}

function toInputDate(ymd: string): string {
  if (ymd.length === 8) return `${ymd.slice(0, 4)}-${ymd.slice(4, 6)}-${ymd.slice(6, 8)}`
  return ymd
}

function todayStr(): string {
  const d = new Date()
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`
}

function daysAgoStr(n: number): string {
  const d = new Date()
  d.setDate(d.getDate() - n)
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`
}

const PHASE_LABELS: Record<string, { label: string; emoji: string; border: string }> = {
  '上升期': { label: '上升期', emoji: '🟢', border: 'border-l-green-500' },
  '震荡期': { label: '震荡期', emoji: '🟡', border: 'border-l-yellow-500' },
  '下降期': { label: '下降期', emoji: '🔴', border: 'border-l-red-500' },
  '混沌期': { label: '混沌期', emoji: '⚪', border: 'border-l-gray-400' },
}

function phaseMeta(phase: string | null | undefined) {
  return PHASE_LABELS[phase ?? ''] ?? { label: phase ?? '未标记', emoji: '📅', border: 'border-l-blue-500' }
}

function indicatorColor(active: boolean | null | undefined): string {
  if (active) return 'bg-green-500 shadow-[0_0_6px_rgba(34,197,94,0.5)]'
  return 'bg-gray-600'
}

const PATH_TYPE_MAP: Record<string, string> = {
  '主升': '主升',
  '轮动': '轮动',
  '反抽': '反抽',
  '潜伏': '潜伏',
  '打板': '打板',
}

function pathLabel(v: string | null | undefined): string {
  return PATH_TYPE_MAP[v ?? ''] ?? v ?? '—'
}

function calcPL(entry?: number | null, exit?: number | null): { pct: number | null; color: string; label: string } {
  if (entry == null) return { pct: null, color: 'text-gray-400', label: '—' }
  if (exit == null) return { pct: null, color: 'text-yellow-500', label: '⏳ 持仓中' }
  const pct = ((exit - entry) / entry) * 100
  const color = pct > 0 ? 'text-red-500' : pct < 0 ? 'text-green-500' : 'text-gray-400'
  const sign = pct > 0 ? '+' : ''
  return { pct, color, label: `${sign}${pct.toFixed(2)}%` }
}

// ── tab button style ──

function tabBtn(active: boolean) {
  return active
    ? 'rounded-lg bg-primary text-primary-foreground px-4 py-1.5 text-sm font-medium'
    : 'rounded-lg px-4 py-1.5 text-sm font-medium text-muted-foreground hover:bg-muted hover:text-foreground'
}

// ── component ──

export default function ReviewLogs() {
  const todayApi = useMemo(() => todayStr(), [])
  const defaultStartApi = useMemo(() => daysAgoStr(30), [])

  // Date inputs (YYYY-MM-DD for native date input)
  const [startDateInput, setStartDateInput] = useState(() => toInputDate(defaultStartApi))
  const [endDateInput, setEndDateInput] = useState(() => toInputDate(todayApi))
  const apiStartDate = useMemo(() => toApiDate(startDateInput), [startDateInput])
  const apiEndDate = useMemo(() => toApiDate(endDateInput), [endDateInput])

  // Tab
  const [activeTab, setActiveTab] = useState<'phase' | 'trade'>('phase')

  // Phase tab state
  const [phaseData, setPhaseData] = useState<PhaseRecord[]>([])
  const [phaseLoading, setPhaseLoading] = useState(true)
  const [phaseLoaded, setPhaseLoaded] = useState(false)
  const [phaseError, setPhaseError] = useState<string | null>(null)

  // Trade tab state
  const [tradeData, setTradeData] = useState<TradeRecord[]>([])
  const [tradeLoading, setTradeLoading] = useState(true)
  const [tradeLoaded, setTradeLoaded] = useState(false)
  const [tradeError, setTradeError] = useState<string | null>(null)

  // Expand state
  const [expandedNotes, setExpandedNotes] = useState<Set<string>>(new Set())
  const [expandedTrade, setExpandedTrade] = useState<Set<string>>(new Set())

  const toggleNote = useCallback((key: string) => {
    setExpandedNotes(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }, [])

  const toggleTradeRow = useCallback((key: string) => {
    setExpandedTrade(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }, [])

  // ── fetch phase ──

  const fetchPhase = useCallback(async () => {
    setPhaseLoading(true)
    setPhaseError(null)
    try {
      const data = await getPhaseByDateRange(apiStartDate, apiEndDate)
      setPhaseData(data)
      setPhaseLoaded(true)
    } catch (err) {
      setPhaseError(err instanceof Error ? err.message : '加载判读记录失败')
    } finally {
      setPhaseLoading(false)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiStartDate, apiEndDate])

  // ── fetch trade ──

  const fetchTrade = useCallback(async () => {
    setTradeLoading(true)
    setTradeError(null)
    try {
      const data = await getTrades()
      setTradeData(data)
      setTradeLoaded(true)
    } catch (err) {
      setTradeError(err instanceof Error ? err.message : '加载交易记录失败')
    } finally {
      setTradeLoading(false)
    }
  }, [])

  // Initial phase fetch
  useEffect(() => {
    fetchPhase()
  }, [fetchPhase])

  // Fetch trade on first switch to trade tab
  useEffect(() => {
    if (activeTab === 'trade' && !tradeLoaded) {
      fetchTrade()
    }
  }, [activeTab, tradeLoaded, fetchTrade])

  // Re-fetch phase when date range changes (if already loaded)
  useEffect(() => {
    if (phaseLoaded) {
      fetchPhase()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiStartDate, apiEndDate])

  // Refresh
  const handleRefresh = useCallback(() => {
    // Reset trade so it re-fetches
    setTradeLoaded(false)
    setPhaseLoaded(false)
    setTradeData([])
    fetchPhase()
    if (activeTab === 'trade') {
      fetchTrade()
    }
  }, [fetchPhase, fetchTrade, activeTab])

  // ── derived data ──

  const groupedPhases = useMemo(() => {
    const map = new Map<string, PhaseRecord[]>()
    for (const r of phaseData) {
      const d = r.trade_date
      if (!map.has(d)) map.set(d, [])
      map.get(d)!.push(r)
    }
    return Array.from(map.entries()).sort(([a], [b]) => b.localeCompare(a))
  }, [phaseData])

  const filteredTrades = useMemo(() => {
    return tradeData
      .filter(t => {
        if (!t.entry_date) return false
        return t.entry_date >= apiStartDate && t.entry_date <= apiEndDate
      })
      .sort((a, b) => (b.entry_date ?? '').localeCompare(a.entry_date ?? ''))
  }, [tradeData, apiStartDate, apiEndDate])

  // ── render ──

  return (
    <div className="mx-auto max-w-4xl">
      {/* Header */}
      <h1 className="mb-6 text-2xl font-bold">复盘日志</h1>

      {/* Filter bar */}
      <Card className="mb-6 border-border/60">
        <CardContent className="flex flex-wrap items-end gap-4 p-4">
          <div className="flex flex-col gap-1">
            <label className="text-xs text-muted-foreground">开始日期</label>
            <Input
              type="date"
              value={startDateInput}
              onChange={e => setStartDateInput(e.target.value)}
              className="h-8 w-40"
            />
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-xs text-muted-foreground">结束日期</label>
            <Input
              type="date"
              value={endDateInput}
              onChange={e => setEndDateInput(e.target.value)}
              className="h-8 w-40"
            />
          </div>

          <div className="ml-auto flex items-center gap-2">
            {/* Tab toggle */}
            <button
              className={tabBtn(activeTab === 'phase')}
              onClick={() => setActiveTab('phase')}
            >
              判读记录
            </button>
            <button
              className={tabBtn(activeTab === 'trade')}
              onClick={() => setActiveTab('trade')}
            >
              交易记录
            </button>
            <Button variant="outline" size="sm" onClick={handleRefresh} className="ml-2">
              刷新
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Error banners */}
      {activeTab === 'phase' && phaseError && (
        <div className="mb-4 rounded-lg border border-red-800 bg-red-950/30 p-3 text-sm text-red-400">
          {phaseError}
          <button className="ml-3 underline hover:text-red-300" onClick={fetchPhase}>
            重试
          </button>
        </div>
      )}
      {activeTab === 'trade' && tradeError && (
        <div className="mb-4 rounded-lg border border-red-800 bg-red-950/30 p-3 text-sm text-red-400">
          {tradeError}
          <button className="ml-3 underline hover:text-red-300" onClick={fetchTrade}>
            重试
          </button>
        </div>
      )}

      {/* ── Phase Tab ── */}
      {activeTab === 'phase' && (
        <>
          {phaseLoading && (
            <div className="flex items-center justify-center py-16">
              <p className="text-muted-foreground">正在加载判读记录...</p>
            </div>
          )}

          {!phaseLoading && phaseError && phaseData.length === 0 && (
            <div className="flex items-center justify-center py-16">
              <div className="text-center">
                <p className="text-muted-foreground">加载失败，请重试</p>
              </div>
            </div>
          )}

          {!phaseLoading && !phaseError && phaseData.length === 0 && (
            <div className="flex items-center justify-center py-16">
              <p className="text-muted-foreground">暂无判读记录</p>
            </div>
          )}

          {!phaseLoading && phaseData.length > 0 && (
            <div className="space-y-8">
              {groupedPhases.map(([date, records]) => (
                <div key={date}>
                  {/* Date heading */}
                  <div className="mb-3 flex items-center gap-2">
                    <span className="text-lg">📅</span>
                    <h2 className="text-lg font-semibold">{fmtDate(date)}</h2>
                    <span className="text-xs text-muted-foreground">
                      {records.length} 条记录
                    </span>
                  </div>

                  {/* Timeline cards */}
                  <div className="space-y-3">
                    {records.map((r, idx) => {
                      const meta = phaseMeta(r.phase)
                      const noteKey = `${date}-${idx}`
                      const isExpanded = expandedNotes.has(noteKey)
                      return (
                        <Card
                          key={noteKey}
                          className={`border-l-4 ${meta.border} border-border/60`}
                        >
                          <CardContent className="p-4">
                            {/* Phase label */}
                            <div className="mb-2 flex items-center gap-2">
                              <span className="text-base">{meta.emoji}</span>
                              <span className="text-sm font-medium">{meta.label}</span>
                            </div>

                            {/* M1/M2/M3/V indicators */}
                            <div className="mb-2 flex items-center gap-3 text-xs">
                              <span className="flex items-center gap-1">
                                <span className={`inline-block h-2.5 w-2.5 rounded-full ${indicatorColor(r.M1_core)}`} />
                                M1
                              </span>
                              <span className="flex items-center gap-1">
                                <span className={`inline-block h-2.5 w-2.5 rounded-full ${indicatorColor(r.M2_front)}`} />
                                M2
                              </span>
                              <span className="flex items-center gap-1">
                                <span className={`inline-block h-2.5 w-2.5 rounded-full ${indicatorColor(r.M3_identifiable)}`} />
                                M3
                              </span>
                              <span className="flex items-center gap-1">
                                <span className={`inline-block h-2.5 w-2.5 rounded-full ${indicatorColor(r.V_triggered)}`} />
                                V
                              </span>
                            </div>

                            {/* Notes */}
                            {r.notes && (
                              <div>
                                <p
                                  className={`text-sm text-muted-foreground ${
                                    isExpanded ? '' : 'line-clamp-3'
                                  }`}
                                >
                                  {r.notes}
                                </p>
                                {r.notes.length > 120 && (
                                  <button
                                    className="mt-1 text-xs text-blue-400 hover:text-blue-300"
                                    onClick={() => toggleNote(noteKey)}
                                  >
                                    {isExpanded ? '收起' : '展开全文'}
                                  </button>
                                )}
                              </div>
                            )}
                          </CardContent>
                        </Card>
                      )
                    })}
                  </div>
                </div>
              ))}
            </div>
          )}
        </>
      )}

      {/* ── Trade Tab ── */}
      {activeTab === 'trade' && (
        <>
          {tradeLoading && (
            <div className="flex items-center justify-center py-16">
              <p className="text-muted-foreground">正在加载交易记录...</p>
            </div>
          )}

          {!tradeLoading && tradeError && tradeData.length === 0 && (
            <div className="flex items-center justify-center py-16">
              <div className="text-center">
                <p className="text-muted-foreground">加载失败，请重试</p>
              </div>
            </div>
          )}

          {!tradeLoading && !tradeError && filteredTrades.length === 0 && (
            <div className="flex items-center justify-center py-16">
              <p className="text-muted-foreground">
                {tradeLoaded ? '暂无交易记录' : '正在加载交易记录...'}
              </p>
            </div>
          )}

          {!tradeLoading && tradeLoaded && filteredTrades.length > 0 && (
            <div className="space-y-2">
              {filteredTrades.map(t => {
                const pl = calcPL(t.entry_price, t.exit_price)
                const isExpanded = expandedTrade.has(t.trade_id)
                return (
                  <Card
                    key={t.trade_id}
                    className="cursor-pointer border-border/60 transition-colors hover:border-border"
                    onClick={() => toggleTradeRow(t.trade_id)}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-center justify-between">
                        {/* Left: ticker + dates */}
                        <div className="flex-1">
                          <div className="flex items-center gap-2">
                            <span className="font-mono text-base font-semibold">
                              {t.ticker ?? '—'}
                            </span>
                            {t.path_type && (
                              <span className="rounded-md bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">
                                {pathLabel(t.path_type)}
                              </span>
                            )}
                          </div>
                          <div className="mt-0.5 flex items-center gap-3 text-xs text-muted-foreground">
                            <span>入 {t.entry_date ? fmtDate(t.entry_date) : '—'}</span>
                            <span>出 {t.exit_date ? fmtDate(t.exit_date) : '—'}</span>
                          </div>
                        </div>

                        {/* Right: P&L */}
                        <div className={`text-right font-mono text-sm font-medium ${pl.color}`}>
                          {pl.label}
                        </div>
                      </div>

                      {/* Expanded detail */}
                      {isExpanded && (
                        <div className="mt-3 border-t border-border/50 pt-3 text-xs text-muted-foreground">
                          <div className="grid grid-cols-2 gap-x-6 gap-y-1.5">
                            <div>
                              入场价：
                              <span className="text-foreground">
                                {t.entry_price != null ? t.entry_price.toFixed(2) : '—'}
                              </span>
                            </div>
                            <div>
                              出场价：
                              <span className="text-foreground">
                                {t.exit_price != null ? t.exit_price.toFixed(2) : '—'}
                              </span>
                            </div>
                            <div>
                              半卖触发价：
                              <span className="text-foreground">
                                {t.half_sell_trigger != null ? t.half_sell_trigger.toFixed(2) : '—'}
                              </span>
                            </div>
                            <div>
                              半卖日期：
                              <span className="text-foreground">
                                {t.half_sell_date ? fmtDate(t.half_sell_date) : '—'}
                              </span>
                            </div>
                            <div>
                              半卖价：
                              <span className="text-foreground">
                                {t.half_sell_price != null ? t.half_sell_price.toFixed(2) : '—'}
                              </span>
                            </div>
                            <div>
                              仓位占比：
                              <span className="text-foreground">
                                {t.position_pct != null ? `${(t.position_pct * 100).toFixed(1)}%` : '—'}
                              </span>
                            </div>
                          </div>
                          {t.notes && (
                            <div className="mt-2">
                              <span>备注：</span>
                              <span>{t.notes}</span>
                            </div>
                          )}
                        </div>
                      )}
                    </CardContent>
                  </Card>
                )
              })}
            </div>
          )}
        </>
      )}
    </div>
  )
}
