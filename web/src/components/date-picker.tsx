import { useState, useEffect } from 'react'
import { format } from 'date-fns'
import { Calendar } from '@/components/ui/calendar'
import { Button } from '@/components/ui/button'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { getDailyDates } from '@/api/daily'

/** 统一将 YYYYMMDD 转为 YYYY-MM-DD */
function normalizeDateInput(s: string): string {
  if (/^\d{8}$/.test(s)) {
    return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`
  }
  return s
}

/** 将 Date 转为 YYYY-MM-DD 字符串用于比对 */
function toDateStr(d: Date): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, '0')
  const day = String(d.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

interface DatePickerProps {
  value: string  // YYYY-MM-DD 或 YYYYMMDD
  onChange: (date: string) => void
  disabled?: boolean
}

export function DatePicker({ value, onChange, disabled }: DatePickerProps) {
  const normalized = value ? normalizeDateInput(value) : ''
  const selectedDate = normalized ? new Date(normalized + 'T00:00:00') : undefined

  // 加载交易日列表
  const [tradingDays, setTradingDays] = useState<Set<string> | null>(null)

  useEffect(() => {
    getDailyDates(undefined, undefined, 5000)
      .then(dates => setTradingDays(new Set(dates)))
      .catch(() => {
        // 如果接口失败，fallback 为空 Set，只做未来日期限制
        setTradingDays(new Set())
      })
  }, [])

  function isDisabled(date: Date): boolean {
    // 未来日期不可选
    if (date > new Date()) return true
    // 交易日列表加载中 → 只禁未来日期（宽松 fallback）
    if (!tradingDays) return date > new Date()
    // 非交易日不可选
    return !tradingDays.has(toDateStr(date))
  }

  return (
    <Popover>
      <PopoverTrigger render={<Button variant="outline" className="w-44 justify-start text-left font-normal" disabled={disabled} />}>
        {value ? format(selectedDate!, 'yyyy-MM-dd') : <span>选择日期</span>}
      </PopoverTrigger>
      <PopoverContent className="w-auto p-0" align="start">
        <Calendar
          mode="single"
          selected={selectedDate}
          onSelect={(date) => {
            if (date) onChange(format(date, 'yyyy-MM-dd'))
          }}
          disabled={isDisabled}
        />
      </PopoverContent>
    </Popover>
  )
}
