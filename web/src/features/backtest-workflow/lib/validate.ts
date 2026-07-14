import type { BacktestWorkflowFormState } from './form-model';
import {
  isCrossSectionalStrategy,
  isEventStrategy,
  parseTickers,
} from './form-model';
import type { ParameterSchema, StrategyParamValues } from '@/types/strategy';

export type FieldErrors = Record<string, string>;

function isInteger(n: number): boolean {
  return Number.isFinite(n) && Math.floor(n) === n;
}

export function validateStrategyParams(
  schema: ParameterSchema,
  values: StrategyParamValues,
): FieldErrors {
  const errors: FieldErrors = {};
  for (const [key, spec] of Object.entries(schema)) {
    const label = spec.label || key;
    const value = values[key];
    if (spec.required && (value === null || value === undefined || value === '')) {
      errors[`param.${key}`] = `${label}为必填项`;
      continue;
    }
    if (value === null || value === undefined || value === '') continue;
    if (spec.type === 'number' || spec.type === 'integer') {
      const num = typeof value === 'number' ? value : Number(value);
      if (!Number.isFinite(num)) {
        errors[`param.${key}`] = `${label}必须是数字`;
        continue;
      }
      if (spec.type === 'integer' && !isInteger(num)) {
        errors[`param.${key}`] = `${label}必须是整数`;
      }
      if (spec.minimum != null && num < spec.minimum) {
        errors[`param.${key}`] = `${label}不能小于 ${spec.minimum}`;
      }
      if (spec.maximum != null && num > spec.maximum) {
        errors[`param.${key}`] = `${label}不能大于 ${spec.maximum}`;
      }
    }
  }

  // Cross-field: dual SMA fast < slow
  if (
    typeof values.fast_window === 'number' &&
    typeof values.slow_window === 'number' &&
    values.fast_window >= values.slow_window
  ) {
    errors['param.slow_window'] = '慢线窗口必须大于快线窗口';
  }

  return errors;
}

export function validateWorkflowForm(
  form: BacktestWorkflowFormState,
  schema: ParameterSchema | null,
): FieldErrors {
  const errors: FieldErrors = {};
  const crossSectional = isCrossSectionalStrategy(form.strategyCode);
  const eventStrategy = isEventStrategy(form.strategyCode);

  if (!form.strategyCode) {
    errors.strategyCode = '请选择策略';
  }
  if (!form.startDate) errors.startDate = '请选择开始日期';
  if (!form.endDate) errors.endDate = '请选择结束日期';
  if (form.startDate && form.endDate && form.startDate > form.endDate) {
    errors.endDate = '结束日期不能早于开始日期';
  }

  if (crossSectional) {
    if (!form.indexCode.trim()) {
      errors.indexCode = '请选择或填写指数代码';
    }
    if (form.entryTiming !== 'next_open') {
      errors.entryTiming = '横截面动量仅支持次日开盘成交';
    }
    const topN = form.strategyParams.top_n;
    if (typeof topN === 'number' && topN > form.maxPositions) {
      errors['param.top_n'] = `Top N 不能大于最大持仓数（${form.maxPositions}）`;
    }
  } else if (eventStrategy) {
    const tickers = parseTickers(form.tickersText);
    if (tickers.length === 0) {
      errors.tickersText = '请至少输入一只股票代码（事件过滤股票池）';
    }
    if (form.entryTiming !== 'next_open') {
      errors.entryTiming = '事件策略仅支持 available_trade_date 开盘入场（next_open 语义）';
    }
  } else {
    const universeMode =
      form.universePreset === 'CUSTOM' || form.universeMode === 'tickers'
        ? 'tickers'
        : 'preset';
    if (universeMode === 'tickers') {
      const tickers = parseTickers(form.tickersText);
      if (tickers.length === 0) {
        errors.tickersText = '请至少输入一只股票代码';
      }
    }
  }

  if (!(form.initialCash > 0)) {
    errors.initialCash = '初始资金必须大于 0';
  }
  if (!(form.maxPositions >= 1)) {
    errors.maxPositions = '最大持仓数至少为 1';
  }
  if (!(form.maxWeightPerSymbol > 0 && form.maxWeightPerSymbol <= 1)) {
    errors.maxWeightPerSymbol = '单票仓位上限需在 0–1 之间';
  }
  if (form.commissionRate < 0) errors.commissionRate = '手续费率不能为负';
  if (form.stampTaxRate < 0) errors.stampTaxRate = '印花税率不能为负';
  if (form.slippageBps < 0) errors.slippageBps = '滑点不能为负';

  if (schema) {
    Object.assign(errors, validateStrategyParams(schema, form.strategyParams));
  }

  return errors;
}
