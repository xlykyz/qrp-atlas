import type {
  CreateBacktestTaskRequest,
  EntryTiming,
  UniverseMode,
} from '@/types/backtest-task';
import type { ParameterSchema, StrategyParamValues } from '@/types/strategy';

/** Page-level form model — deliberately separate from API DTO. */
export interface BacktestWorkflowFormState {
  taskName: string;
  strategyCode: string;
  strategyVersion: string;
  strategyParams: StrategyParamValues;
  universeMode: UniverseMode;
  universePreset: string;
  tickersText: string;
  startDate: string;
  endDate: string;
  initialCash: number;
  maxPositions: number;
  maxWeightPerSymbol: number;
  entryTiming: EntryTiming;
  commissionRate: number;
  stampTaxRate: number;
  slippageBps: number;
}

export const UNIVERSE_PRESETS = [
  { value: 'A_SHARE', label: '全 A（演示）' },
  { value: 'CSI300', label: '沪深300（演示）' },
  { value: 'CSI500', label: '中证500（演示）' },
  { value: 'CUSTOM', label: '自定义列表' },
] as const;

export const ENTRY_TIMING_OPTIONS: { value: EntryTiming; label: string }[] = [
  { value: 'next_open', label: '次日开盘' },
  { value: 'same_close', label: '当日收盘（同 bar，非严格 PIT）' },
  { value: 'next_close', label: '次日收盘' },
];

export function defaultParamsFromSchema(schema: ParameterSchema): StrategyParamValues {
  const values: StrategyParamValues = {};
  for (const [key, spec] of Object.entries(schema)) {
    if (spec.default !== undefined) {
      values[key] = spec.default;
    } else if (spec.type === 'boolean') {
      values[key] = false;
    } else if (spec.type === 'string') {
      values[key] = '';
    } else {
      values[key] = null;
    }
  }
  return values;
}

export function createDefaultFormState(): BacktestWorkflowFormState {
  return {
    taskName: '',
    strategyCode: '',
    strategyVersion: '',
    strategyParams: {},
    universeMode: 'tickers',
    universePreset: 'CUSTOM',
    tickersText: '000001.SZ, 600519.SH',
    startDate: '2024-01-01',
    endDate: '2024-12-31',
    initialCash: 1_000_000,
    maxPositions: 10,
    maxWeightPerSymbol: 0.1,
    entryTiming: 'next_open',
    commissionRate: 0.00025,
    stampTaxRate: 0.0005,
    slippageBps: 5,
  };
}

export function parseTickers(text: string): string[] {
  return text
    .split(/[\s,;，；\n]+/)
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
}

/**
 * Map UI form state → CreateBacktestTaskRequest DTO.
 * This is the only place form fields should be coupled to API shape.
 */
export function formStateToCreateRequest(
  form: BacktestWorkflowFormState,
): CreateBacktestTaskRequest {
  const tickers = parseTickers(form.tickersText);
  const universeMode: UniverseMode =
    form.universePreset === 'CUSTOM' || form.universeMode === 'tickers'
      ? 'tickers'
      : 'preset';

  return {
    name: form.taskName.trim() || undefined,
    strategy_code: form.strategyCode,
    strategy_version: form.strategyVersion,
    strategy_params: { ...form.strategyParams },
    universe_mode: universeMode,
    universe_preset: universeMode === 'preset' ? form.universePreset : undefined,
    tickers: universeMode === 'tickers' ? tickers : undefined,
    start_date: form.startDate,
    end_date: form.endDate,
    position: {
      initial_cash: form.initialCash,
      max_positions: form.maxPositions,
      max_weight_per_symbol: form.maxWeightPerSymbol,
    },
    cost: {
      commission_rate: form.commissionRate,
      stamp_tax_rate: form.stampTaxRate,
      slippage_bps: form.slippageBps,
    },
    execution: {
      entry_timing: form.entryTiming,
    },
  };
}
