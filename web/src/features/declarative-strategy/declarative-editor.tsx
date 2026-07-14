import { useMemo, useState } from 'react';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import {
  createDeclarativeStrategy,
  createDeclarativeVersion,
  validateDeclarativeStrategy,
} from '@/api/declarative-strategies';

type CmpOp = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte';

interface SimpleRule {
  field: string;
  op: CmpOp;
  value: number;
}

const OPS: CmpOp[] = ['eq', 'ne', 'gt', 'gte', 'lt', 'lte'];

function ruleToCondition(rule: SimpleRule) {
  return {
    left: { source_type: 'field', code: rule.field },
    operator: rule.op,
    right: { source_type: 'literal', value: rule.value },
  };
}

export function DeclarativeEditorPanel() {
  const [code, setCode] = useState('my_declarative_trend');
  const [name, setName] = useState('我的声明式策略');
  const [version, setVersion] = useState('1.0.0');
  const [description, setDescription] = useState('受控规则编辑器生成的声明式策略');
  const [entry, setEntry] = useState<SimpleRule>({ field: 'close', op: 'gt', value: 10 });
  const [exitRule, setExitRule] = useState<SimpleRule>({ field: 'close', op: 'lt', value: 9 });
  const [holdEnabled, setHoldEnabled] = useState(false);
  const [holdRule, setHoldRule] = useState<SimpleRule>({ field: 'close', op: 'gt', value: 8 });
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const definition = useMemo(() => {
    const fields = new Set([entry.field, exitRule.field, 'close']);
    if (holdEnabled) fields.add(holdRule.field);
    const payload: Record<string, unknown> = {
      code,
      name,
      version,
      description,
      strategy_type: 'declarative',
      required_fields: Array.from(fields),
      required_indicators: [],
      indicator_requests: [],
      parameters: {},
      entry: ruleToCondition(entry),
      exit: ruleToCondition(exitRule),
    };
    if (holdEnabled) {
      payload.hold = ruleToCondition(holdRule);
    }
    return payload;
  }, [code, name, version, description, entry, exitRule, holdEnabled, holdRule]);

  async function onValidate() {
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const res = await validateDeclarativeStrategy(definition);
      setMessage(`校验通过：${String(res.code)}@${String(res.version)}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : '校验失败');
    } finally {
      setBusy(false);
    }
  }

  async function onSave(asNewVersion = false) {
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const res = asNewVersion
        ? await createDeclarativeVersion(code, definition)
        : await createDeclarativeStrategy(definition);
      setMessage(`已保存 ${res.code}@${res.version}（状态 ${res.status}）`);
    } catch (e) {
      setError(e instanceof Error ? e.message : '保存失败');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-4 rounded-lg border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900/40">
      <div>
        <h3 className="text-sm font-medium text-slate-800 dark:text-slate-100">声明式策略编辑器</h3>
        <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
          仅允许白名单比较与字段引用。禁止任意代码、eval/exec 或自由表达式文本。
        </p>
      </div>

      <div className="grid gap-3 sm:grid-cols-2">
        <div>
          <label className="mb-1 block text-xs">code</label>
          <Input value={code} onChange={(e) => setCode(e.target.value)} disabled={busy} />
        </div>
        <div>
          <label className="mb-1 block text-xs">version</label>
          <Input value={version} onChange={(e) => setVersion(e.target.value)} disabled={busy} />
        </div>
        <div className="sm:col-span-2">
          <label className="mb-1 block text-xs">name</label>
          <Input value={name} onChange={(e) => setName(e.target.value)} disabled={busy} />
        </div>
        <div className="sm:col-span-2">
          <label className="mb-1 block text-xs">description</label>
          <Input
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            disabled={busy}
          />
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-2">
        <RuleEditor title="ENTER 条件" rule={entry} onChange={setEntry} disabled={busy} />
        <RuleEditor title="EXIT 条件" rule={exitRule} onChange={setExitRule} disabled={busy} />
      </div>

      <div className="space-y-2 rounded-md border border-slate-200 p-3 dark:border-slate-700">
        <label className="flex items-center gap-2 text-xs text-slate-700 dark:text-slate-200">
          <input
            type="checkbox"
            checked={holdEnabled}
            disabled={busy}
            onChange={(e) => setHoldEnabled(e.target.checked)}
          />
          启用可选 HOLD 条件（不满足则退出）
        </label>
        {holdEnabled ? (
          <RuleEditor title="HOLD 条件" rule={holdRule} onChange={setHoldRule} disabled={busy} />
        ) : null}
      </div>

      <div className="rounded-md bg-slate-50 p-3 text-xs text-slate-600 dark:bg-slate-950/50 dark:text-slate-300">
        <div className="mb-1 font-medium">只读定义预览（结构化 JSON，不可执行文本）</div>
        <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-all">
          {JSON.stringify(definition, null, 2)}
        </pre>
      </div>

      <div className="flex flex-wrap gap-2">
        <Button type="button" disabled={busy} onClick={onValidate}>
          静态校验
        </Button>
        <Button type="button" disabled={busy} onClick={() => onSave(false)}>
          保存新策略
        </Button>
        <Button type="button" disabled={busy} variant="secondary" onClick={() => onSave(true)}>
          保存为新版本
        </Button>
      </div>

      {message ? <p className="text-xs text-emerald-600 dark:text-emerald-400">{message}</p> : null}
      {error ? <p className="text-xs text-red-600 dark:text-red-400">{error}</p> : null}
    </div>
  );
}

function RuleEditor({
  title,
  rule,
  onChange,
  disabled,
}: {
  title: string;
  rule: SimpleRule;
  onChange: (rule: SimpleRule) => void;
  disabled?: boolean;
}) {
  return (
    <div className="rounded-md border border-slate-200 p-3 dark:border-slate-700">
      <div className="mb-2 text-xs font-medium text-slate-700 dark:text-slate-200">{title}</div>
      <div className="grid gap-2">
        <Input
          disabled={disabled}
          value={rule.field}
          onChange={(e) => onChange({ ...rule, field: e.target.value.trim() })}
          placeholder="字段，如 close"
        />
        <select
          className="h-9 rounded-md border border-slate-200 bg-white px-2 text-sm dark:border-slate-700 dark:bg-slate-900"
          disabled={disabled}
          value={rule.op}
          onChange={(e) => onChange({ ...rule, op: e.target.value as CmpOp })}
        >
          {OPS.map((op) => (
            <option key={op} value={op}>
              {op}
            </option>
          ))}
        </select>
        <Input
          type="number"
          disabled={disabled}
          value={rule.value}
          onChange={(e) => onChange({ ...rule, value: Number(e.target.value) })}
        />
      </div>
    </div>
  );
}
