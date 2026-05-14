import { format } from 'date-fns'
import { zhCN } from 'date-fns/locale'
import { Calendar } from '@/components/ui/calendar'
import { Button } from '@/components/ui/button'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'

interface DatePickerProps {
  value: string  // YYYY-MM-DD
  onChange: (date: string) => void
  availableDates: string[]  // 可选的交易日列表 YYYY-MM-DD
  disabled?: boolean
}

export function DatePicker({ value, onChange, availableDates, disabled }: DatePickerProps) {
  const selectedDate = value ? new Date(value + 'T00:00:00') : undefined
  const today = new Date()
  today.setHours(23, 59, 59, 999)

  // 判断某日期是否可选（是交易日且不是未来日期）
  function isDateDisabled(date: Date) {
    const dateStr = format(date, 'yyyy-MM-dd')
    if (date > today) return true  // 未来日期不可选
    return !availableDates.includes(dateStr)  // 非交易日不可选
  }

  return (
    <Popover>
      <PopoverTrigger
        render={
          <Button
            variant="outline"
            className="w-44 justify-start text-left font-normal"
            disabled={disabled}
          />
        }
      >
        {value ? format(selectedDate!, 'yyyy-MM-dd') : <span>选择日期</span>}
      </PopoverTrigger>
      <PopoverContent className="w-auto p-0" align="start">
        <Calendar
          mode="single"
          selected={selectedDate}
          onSelect={(date) => {
            if (date) onChange(format(date, 'yyyy-MM-dd'))
          }}
          disabled={isDateDisabled}
          locale={zhCN}
          initialFocus
        />
      </PopoverContent>
    </Popover>
  )
}
