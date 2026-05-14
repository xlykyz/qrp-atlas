import { format } from 'date-fns'
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
  disabled?: boolean
}

export function DatePicker({ value, onChange, disabled }: DatePickerProps) {
  const selectedDate = value ? new Date(value + 'T00:00:00') : undefined

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
          disabled={(date) => date > new Date()}
          initialFocus
        />
      </PopoverContent>
    </Popover>
  )
}
