import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import type { BacktestRun } from '@/types/backtest';

interface Props {
  runs: BacktestRun[];
  selectedRunId: string | null;
  onSelect: (runId: string) => void;
  disabled?: boolean;
}

export function BacktestRunSelector({ runs, selectedRunId, onSelect, disabled }: Props) {
  if (runs.length === 0) {
    return null;
  }

  return (
    <Select
      value={selectedRunId ?? undefined}
      onValueChange={(v) => v && onSelect(v)}
      disabled={disabled || runs.length === 0}
    >
      <SelectTrigger className="w-72">
        <SelectValue placeholder="选择回测 run" />
      </SelectTrigger>
      <SelectContent>
        {runs.map((run) => (
          <SelectItem key={run.run_id} value={run.run_id}>
            {run.run_id} · {run.name} ({run.strategy_name})
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
