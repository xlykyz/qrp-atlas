import { Input } from '@/components/ui/input';
import type { ParameterSchema, StrategyParamValues } from '@/types/strategy';

interface Props {
  schema: ParameterSchema;
  values: StrategyParamValues;
  errors?: Record<string, string>;
  disabled?: boolean;
  onChange: (next: StrategyParamValues) => void;
}

export function SchemaParamForm({ schema, values, errors = {}, disabled, onChange }: Props) {
  const entries = Object.entries(schema);

  if (entries.length === 0) {
    return (
      <div className="rounded-lg border border-slate-200 bg-slate-50 p-4 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/50 dark:text-slate-400">
        该策略无需额外参数。
      </div>
    );
  }

  function setValue(key: string, value: number | string | boolean | null) {
    onChange({ ...values, [key]: value });
  }

  return (
    <div className="grid gap-3 sm:grid-cols-2">
      {entries.map(([key, spec]) => {
        const label = spec.label ?? key;
        const err = errors[`param.${key}`];
        const value = values[key];

        if (spec.type === 'boolean') {
          return (
            <label
              key={key}
              className="flex items-start gap-2 rounded-lg border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900/40"
            >
              <input
                type="checkbox"
                className="mt-0.5"
                checked={Boolean(value)}
                disabled={disabled}
                onChange={(e) => setValue(key, e.target.checked)}
              />
              <span>
                <span className="block text-sm font-medium text-slate-800 dark:text-slate-100">
                  {label}
                  {spec.required ? <span className="text-red-500"> *</span> : null}
                </span>
                {spec.description ? (
                  <span className="mt-0.5 block text-xs text-slate-500 dark:text-slate-400">
                    {spec.description}
                  </span>
                ) : null}
                {err ? <span className="mt-1 block text-xs text-red-500">{err}</span> : null}
              </span>
            </label>
          );
        }

        return (
          <div key={key} className="space-y-1">
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-300">
              {label}
              {spec.required ? <span className="text-red-500"> *</span> : null}
            </label>
            <Input
              type={spec.type === 'string' ? 'text' : 'number'}
              step={spec.type === 'integer' ? 1 : 'any'}
              min={spec.minimum ?? undefined}
              max={spec.maximum ?? undefined}
              disabled={disabled}
              value={value == null ? '' : String(value)}
              aria-invalid={Boolean(err)}
              onChange={(e) => {
                const raw = e.target.value;
                if (raw === '') {
                  setValue(key, null);
                  return;
                }
                if (spec.type === 'string') {
                  setValue(key, raw);
                  return;
                }
                const num = Number(raw);
                setValue(key, Number.isNaN(num) ? null : num);
              }}
            />
            <div className="flex flex-wrap gap-x-2 text-[11px] text-slate-400">
              {spec.minimum != null || spec.maximum != null ? (
                <span>
                  范围: {spec.minimum ?? '−∞'} ~ {spec.maximum ?? '+∞'}
                </span>
              ) : null}
              {spec.default !== undefined && spec.default !== null ? (
                <span>默认: {String(spec.default)}</span>
              ) : null}
            </div>
            {spec.description ? (
              <p className="text-xs text-slate-500 dark:text-slate-400">{spec.description}</p>
            ) : null}
            {err ? <p className="text-xs text-red-500">{err}</p> : null}
          </div>
        );
      })}
    </div>
  );
}
