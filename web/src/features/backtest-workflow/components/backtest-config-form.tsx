import { Input } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import type { BacktestWorkflowFormState } from '../lib/form-model';
import {
  ENTRY_TIMING_OPTIONS,
  INDEX_CODE_OPTIONS,
  UNIVERSE_PRESETS,
  isCrossSectionalStrategy,
} from '../lib/form-model';

interface Props {
  form: BacktestWorkflowFormState;
  errors?: Record<string, string>;
  disabled?: boolean;
  onChange: (patch: Partial<BacktestWorkflowFormState>) => void;
}

function FieldError({ message }: { message?: string }) {
  if (!message) return null;
  return <p className="mt-1 text-xs text-red-500">{message}</p>;
}

export function BacktestConfigForm({ form, errors = {}, disabled, onChange }: Props) {
  const crossSectional = isCrossSectionalStrategy(form.strategyCode);
  const isCustomUniverse =
    !crossSectional && (form.universePreset === 'CUSTOM' || form.universeMode === 'tickers');

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-2">
        <div className="sm:col-span-2">
          <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
            任务名称（可选）
          </label>
          <Input
            disabled={disabled}
            placeholder="留空则自动生成；名称含 fail 可演示失败态"
            value={form.taskName}
            onChange={(e) => onChange({ taskName: e.target.value })}
          />
        </div>

        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
            开始日期
          </label>
          <Input
            type="date"
            disabled={disabled}
            value={form.startDate}
            onChange={(e) => onChange({ startDate: e.target.value })}
          />
          <FieldError message={errors.startDate} />
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
            结束日期
          </label>
          <Input
            type="date"
            disabled={disabled}
            value={form.endDate}
            onChange={(e) => onChange({ endDate: e.target.value })}
          />
          <FieldError message={errors.endDate} />
        </div>

        {crossSectional ? (
          <div className="sm:col-span-2">
            <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
              历史指数成分股票池
            </label>
            <Select
              value={form.indexCode}
              onValueChange={(v) => {
                if (!v) return;
                onChange({
                  indexCode: v,
                  universeMode: 'index_components',
                });
              }}
              disabled={disabled}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder="选择指数" />
              </SelectTrigger>
              <SelectContent>
                {INDEX_CODE_OPTIONS.map((p) => (
                  <SelectItem key={p.value} value={p.value}>
                    {p.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
              股票池按历史指数成分构建，不使用当前成分回看过去。每个调仓信号日使用该日
              as-of 可见的成分记录。
            </p>
            <div className="mt-2">
              <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
                指数代码（可手填）
              </label>
              <Input
                disabled={disabled}
                placeholder="000300.SH"
                value={form.indexCode}
                onChange={(e) =>
                  onChange({
                    indexCode: e.target.value.toUpperCase(),
                    universeMode: 'index_components',
                  })
                }
              />
              <FieldError message={errors.indexCode} />
            </div>
          </div>
        ) : (
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
              股票池
            </label>
            <Select
              value={form.universePreset}
              onValueChange={(v) => {
                if (!v) return;
                onChange({
                  universePreset: v,
                  universeMode: v === 'CUSTOM' ? 'tickers' : 'preset',
                });
              }}
              disabled={disabled}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder="选择股票池" />
              </SelectTrigger>
              <SelectContent>
                {UNIVERSE_PRESETS.map((p) => (
                  <SelectItem key={p.value} value={p.value}>
                    {p.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        )}

        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
            入场执行时点
          </label>
          <Select
            value={crossSectional ? 'next_open' : form.entryTiming}
            onValueChange={(v) =>
              v && onChange({ entryTiming: v as BacktestWorkflowFormState['entryTiming'] })
            }
            disabled={disabled || crossSectional}
          >
            <SelectTrigger className="w-full">
              <SelectValue placeholder="入场时点" />
            </SelectTrigger>
            <SelectContent>
              {(crossSectional
                ? ENTRY_TIMING_OPTIONS.filter((o) => o.value === 'next_open')
                : ENTRY_TIMING_OPTIONS
              ).map((o) => (
                <SelectItem key={o.value} value={o.value}>
                  {o.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {crossSectional ? (
            <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
              横截面动量固定为信号日收盘后决策，下一合法交易日开盘成交。
            </p>
          ) : null}
          <FieldError message={errors.entryTiming} />
        </div>

        {isCustomUniverse ? (
          <div className="sm:col-span-2">
            <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
              股票列表（逗号/空格分隔）
            </label>
            <Input
              disabled={disabled}
              placeholder="000001.SZ, 600519.SH"
              value={form.tickersText}
              onChange={(e) => onChange({ tickersText: e.target.value })}
            />
            <FieldError message={errors.tickersText} />
          </div>
        ) : null}
      </div>

      <div className="grid gap-3 sm:grid-cols-3">
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
            初始资金
          </label>
          <Input
            type="number"
            min={1}
            disabled={disabled}
            value={form.initialCash}
            onChange={(e) => onChange({ initialCash: Number(e.target.value) })}
          />
          <FieldError message={errors.initialCash} />
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
            最大持仓数
          </label>
          <Input
            type="number"
            min={1}
            step={1}
            disabled={disabled}
            value={form.maxPositions}
            onChange={(e) => onChange({ maxPositions: Number(e.target.value) })}
          />
          <FieldError message={errors.maxPositions} />
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
            单票仓位上限 (0–1)
          </label>
          <Input
            type="number"
            min={0.01}
            max={1}
            step={0.01}
            disabled={disabled}
            value={form.maxWeightPerSymbol}
            onChange={(e) => onChange({ maxWeightPerSymbol: Number(e.target.value) })}
          />
          <FieldError message={errors.maxWeightPerSymbol} />
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-3">
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
            手续费率
          </label>
          <Input
            type="number"
            min={0}
            step={0.00001}
            disabled={disabled}
            value={form.commissionRate}
            onChange={(e) => onChange({ commissionRate: Number(e.target.value) })}
          />
          <FieldError message={errors.commissionRate} />
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
            印花税率
          </label>
          <Input
            type="number"
            min={0}
            step={0.0001}
            disabled={disabled}
            value={form.stampTaxRate}
            onChange={(e) => onChange({ stampTaxRate: Number(e.target.value) })}
          />
          <FieldError message={errors.stampTaxRate} />
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
            滑点 (bps)
          </label>
          <Input
            type="number"
            min={0}
            step={1}
            disabled={disabled}
            value={form.slippageBps}
            onChange={(e) => onChange({ slippageBps: Number(e.target.value) })}
          />
          <FieldError message={errors.slippageBps} />
        </div>
      </div>
    </div>
  );
}
