import { useEffect, useMemo, useState } from 'react';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import {
  createDeclarativeStrategy,
  createDeclarativeVersion,
  listDeclarativeStrategies,
  type DeclarativeStrategyRecord,
  validateDeclarativeStrategy,
} from '@/api/declarative-strategies';

type CmpOp = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte';
type SourceType = 'field' | 'indicator' | 'parameter' | 'literal';
type GroupOp = 'all' | 'any' | 'not';
type ParameterType = 'number' | 'integer' | 'string' | 'boolean';

type ReferenceDraft = {
  sourceType: SourceType;
  code: string;
  value: string;
  literalType: ParameterType;
};

type ConditionDraft =
  | { id: string; kind: 'comparison'; left: ReferenceDraft; operator: CmpOp; right: ReferenceDraft }
  | { id: string; kind: 'group'; operator: GroupOp; children: ConditionDraft[] };

type ParameterDraft = {
  id: string;
  code: string;
  type: ParameterType;
  required: boolean;
  defaultValue: string;
};

type IndicatorDraft = {
  id: string;
  code: string;
  alias: string;
  parameterName: string;
  parameterSource: 'literal' | 'parameter';
  parameterValue: string;
};

const OPS: CmpOp[] = ['eq', 'ne', 'gt', 'gte', 'lt', 'lte'];
const uid = () => Math.random().toString(36).slice(2, 10);

function reference(sourceType: SourceType, code = '', value = ''): ReferenceDraft {
  return { sourceType, code, value, literalType: 'number' };
}

function comparison(field: string, operator: CmpOp, value: string): ConditionDraft {
  return {
    id: uid(),
    kind: 'comparison',
    left: reference('field', field),
    operator,
    right: reference('literal', '', value),
  };
}

function parseScalar(value: string, type: ParameterType): unknown {
  if (type === 'boolean') return value === 'true';
  if (type === 'number') return Number(value);
  if (type === 'integer') return Math.trunc(Number(value));
  return value;
}

function referencePayload(item: ReferenceDraft): Record<string, unknown> {
  if (item.sourceType === 'literal') {
    return { source_type: 'literal', value: parseScalar(item.value, item.literalType) };
  }
  return { source_type: item.sourceType, code: item.code.trim() };
}

function conditionPayload(condition: ConditionDraft): Record<string, unknown> {
  if (condition.kind === 'comparison') {
    return {
      left: referencePayload(condition.left),
      operator: condition.operator,
      right: referencePayload(condition.right),
    };
  }
  if (condition.operator === 'not') {
    return { not: conditionPayload(condition.children[0]) };
  }
  return { [condition.operator]: condition.children.map(conditionPayload) };
}

function collectReferences(condition: ConditionDraft, fields: Set<string>) {
  if (condition.kind === 'group') {
    condition.children.forEach((child) => collectReferences(child, fields));
    return;
  }
  for (const item of [condition.left, condition.right]) {
    if (item.sourceType === 'field' && item.code.trim()) fields.add(item.code.trim());
  }
}

export function DeclarativeEditorPanel() {
  const [code, setCode] = useState('my_declarative_trend');
  const [name, setName] = useState('我的声明式策略');
  const [version, setVersion] = useState('1.0.0');
  const [description, setDescription] = useState('受控规则编辑器生成的声明式策略');
  const [entry, setEntry] = useState<ConditionDraft>(comparison('close', 'gt', '10'));
  const [exitRule, setExitRule] = useState<ConditionDraft>(comparison('close', 'lt', '9'));
  const [holdEnabled, setHoldEnabled] = useState(false);
  const [holdRule, setHoldRule] = useState<ConditionDraft>(comparison('close', 'gt', '8'));
  const [parameters, setParameters] = useState<ParameterDraft[]>([]);
  const [indicators, setIndicators] = useState<IndicatorDraft[]>([]);
  const [history, setHistory] = useState<DeclarativeStrategyRecord[]>([]);
  const [selectedHistory, setSelectedHistory] = useState<DeclarativeStrategyRecord | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function reloadHistory() {
    try {
      const rows = await listDeclarativeStrategies(true);
      setHistory(rows);
      if (selectedHistory) {
        setSelectedHistory(
          rows.find(
            (row) => row.code === selectedHistory.code && row.version === selectedHistory.version,
          ) ?? null,
        );
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : '版本列表加载失败');
    }
  }

  useEffect(() => {
    void reloadHistory();
  }, []);

  const definition = useMemo(() => {
    const fields = new Set<string>(['trade_date', 'ticker']);
    collectReferences(entry, fields);
    collectReferences(exitRule, fields);
    if (holdEnabled) collectReferences(holdRule, fields);

    const parameterSchema = Object.fromEntries(
      parameters
        .filter((item) => item.code.trim())
        .map((item) => [
          item.code.trim(),
          {
            type: item.type,
            required: item.required,
            ...(!item.required && item.defaultValue !== ''
              ? { default: parseScalar(item.defaultValue, item.type) }
              : {}),
          },
        ]),
    );
    const indicatorRequests = indicators
      .filter((item) => item.code.trim())
      .map((item) => ({
        code: item.code.trim(),
        alias: item.alias.trim() || undefined,
        parameters:
          item.parameterName.trim() === ''
            ? {}
            : {
                [item.parameterName.trim()]:
                  item.parameterSource === 'parameter'
                    ? { parameter: item.parameterValue.trim() }
                    : Number(item.parameterValue),
              },
        output_fields: {},
      }));

    const payload: Record<string, unknown> = {
      code: code.trim(),
      name: name.trim(),
      version: version.trim(),
      description,
      strategy_type: 'declarative',
      required_fields: Array.from(fields),
      required_indicators: [],
      indicator_requests: indicatorRequests,
      parameters: parameterSchema,
      entry: conditionPayload(entry),
      exit: conditionPayload(exitRule),
    };
    if (holdEnabled) payload.hold = conditionPayload(holdRule);
    return payload;
  }, [code, name, version, description, entry, exitRule, holdEnabled, holdRule, parameters, indicators]);

  async function runAction(action: () => Promise<unknown>, success: (result: any) => string) {
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const result = await action();
      setMessage(success(result));
      await reloadHistory();
    } catch (e) {
      setError(e instanceof Error ? e.message : '操作失败');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-4 rounded-lg border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900/40">
      <div>
        <h3 className="text-sm font-medium text-slate-800 dark:text-slate-100">声明式策略编辑器</h3>
        <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
          仅生成受控 JSON 条件树。版本创建后内容不可修改；历史版本只能浏览。
        </p>
      </div>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(260px,1fr)]">
        <div className="space-y-4">
          <div className="grid gap-3 sm:grid-cols-2">
            <LabeledInput label="code" value={code} onChange={setCode} disabled={busy} />
            <LabeledInput label="version（语义化版本）" value={version} onChange={setVersion} disabled={busy} />
            <div className="sm:col-span-2">
              <LabeledInput label="name" value={name} onChange={setName} disabled={busy} />
            </div>
            <div className="sm:col-span-2">
              <LabeledInput label="description" value={description} onChange={setDescription} disabled={busy} />
            </div>
          </div>

          <ParameterEditor items={parameters} onChange={setParameters} disabled={busy} />
          <IndicatorEditor items={indicators} onChange={setIndicators} disabled={busy} />

          <ConditionEditor title="ENTER 条件" value={entry} onChange={setEntry} disabled={busy} />
          <ConditionEditor title="EXIT 条件" value={exitRule} onChange={setExitRule} disabled={busy} />
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={holdEnabled}
                onChange={(event) => setHoldEnabled(event.target.checked)}
                disabled={busy}
              />
              启用 HOLD 条件（不满足时退出）
            </label>
            {holdEnabled ? (
              <ConditionEditor title="HOLD 条件" value={holdRule} onChange={setHoldRule} disabled={busy} />
            ) : null}
          </div>

          <div className="rounded-md bg-slate-50 p-3 text-xs text-slate-600 dark:bg-slate-950/50 dark:text-slate-300">
            <div className="mb-1 font-medium">待保存定义预览</div>
            <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-all">{JSON.stringify(definition, null, 2)}</pre>
          </div>

          <div className="flex flex-wrap gap-2">
            <Button
              type="button"
              disabled={busy}
              onClick={() =>
                runAction(
                  () => validateDeclarativeStrategy(definition),
                  (result) => `校验通过：${String(result.code)}@${String(result.version)}`,
                )
              }
            >
              静态校验
            </Button>
            <Button
              type="button"
              disabled={busy}
              onClick={() =>
                runAction(
                  () => createDeclarativeStrategy(definition),
                  (result) => `已保存 ${result.code}@${result.version}`,
                )
              }
            >
              保存新策略
            </Button>
            <Button
              type="button"
              disabled={busy}
              variant="secondary"
              onClick={() =>
                runAction(
                  () => createDeclarativeVersion(code, definition),
                  (result) => `已创建不可变版本 ${result.code}@${result.version}`,
                )
              }
            >
              保存为新版本
            </Button>
          </div>
        </div>

        <div className="space-y-3 rounded-md border border-slate-200 p-3 dark:border-slate-700">
          <div className="flex items-center justify-between gap-2">
            <div className="text-sm font-medium">版本浏览</div>
            <Button type="button" variant="secondary" disabled={busy} onClick={() => void reloadHistory()}>
              刷新
            </Button>
          </div>
          <div className="max-h-56 space-y-1 overflow-auto">
            {history.length === 0 ? <p className="text-xs text-slate-500">暂无版本</p> : null}
            {history.map((record) => (
              <button
                key={`${record.code}@${record.version}`}
                type="button"
                className={`block w-full rounded px-2 py-1.5 text-left text-xs ${
                  selectedHistory?.code === record.code && selectedHistory.version === record.version
                    ? 'bg-blue-50 text-blue-700 dark:bg-blue-950/40 dark:text-blue-300'
                    : 'hover:bg-slate-50 dark:hover:bg-slate-800'
                }`}
                onClick={() => setSelectedHistory(record)}
              >
                <span className="font-medium">{record.code}@{record.version}</span>
                <span className="ml-2 text-slate-500">{record.status}</span>
              </button>
            ))}
          </div>
          <div className="rounded bg-slate-50 p-2 text-xs dark:bg-slate-950/50">
            <div className="mb-1 font-medium">历史版本只读定义</div>
            <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-all">
              {selectedHistory ? JSON.stringify(selectedHistory.definition, null, 2) : '选择一个历史版本查看'}
            </pre>
          </div>
        </div>
      </div>

      {message ? <p className="text-xs text-emerald-600 dark:text-emerald-400">{message}</p> : null}
      {error ? <p className="text-xs text-red-600 dark:text-red-400">{error}</p> : null}
    </div>
  );
}

function LabeledInput({
  label,
  value,
  onChange,
  disabled,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
}) {
  return (
    <div>
      <label className="mb-1 block text-xs">{label}</label>
      <Input value={value} onChange={(event) => onChange(event.target.value)} disabled={disabled} />
    </div>
  );
}

function ParameterEditor({
  items,
  onChange,
  disabled,
}: {
  items: ParameterDraft[];
  onChange: (items: ParameterDraft[]) => void;
  disabled?: boolean;
}) {
  return (
    <section className="space-y-2 rounded-md border border-slate-200 p-3 dark:border-slate-700">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium">策略参数（可在条件和指标请求中引用）</div>
        <Button
          type="button"
          variant="secondary"
          disabled={disabled}
          onClick={() =>
            onChange([...items, { id: uid(), code: '', type: 'number', required: false, defaultValue: '10' }])
          }
        >
          添加参数
        </Button>
      </div>
      {items.map((item) => (
        <div key={item.id} className="grid gap-2 sm:grid-cols-5">
          <Input value={item.code} placeholder="参数 code" onChange={(e) => onChange(items.map((row) => row.id === item.id ? { ...row, code: e.target.value } : row))} />
          <select className="h-9 rounded-md border bg-transparent px-2 text-sm" value={item.type} onChange={(e) => onChange(items.map((row) => row.id === item.id ? { ...row, type: e.target.value as ParameterType } : row))}>
            {['number', 'integer', 'string', 'boolean'].map((type) => <option key={type}>{type}</option>)}
          </select>
          <Input value={item.defaultValue} placeholder="默认值" disabled={item.required} onChange={(e) => onChange(items.map((row) => row.id === item.id ? { ...row, defaultValue: e.target.value } : row))} />
          <label className="flex items-center gap-1 text-xs"><input type="checkbox" checked={item.required} onChange={(e) => onChange(items.map((row) => row.id === item.id ? { ...row, required: e.target.checked } : row))} />必填</label>
          <Button type="button" variant="secondary" onClick={() => onChange(items.filter((row) => row.id !== item.id))}>删除</Button>
        </div>
      ))}
    </section>
  );
}

function IndicatorEditor({ items, onChange, disabled }: { items: IndicatorDraft[]; onChange: (items: IndicatorDraft[]) => void; disabled?: boolean }) {
  return (
    <section className="space-y-2 rounded-md border border-slate-200 p-3 dark:border-slate-700">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium">指标请求</div>
        <Button type="button" variant="secondary" disabled={disabled} onClick={() => onChange([...items, { id: uid(), code: 'sma', alias: '', parameterName: 'window', parameterSource: 'literal', parameterValue: '20' }])}>添加指标</Button>
      </div>
      {items.map((item) => (
        <div key={item.id} className="grid gap-2 sm:grid-cols-6">
          <Input value={item.code} placeholder="指标 code" onChange={(e) => onChange(items.map((row) => row.id === item.id ? { ...row, code: e.target.value } : row))} />
          <Input value={item.alias} placeholder="alias" onChange={(e) => onChange(items.map((row) => row.id === item.id ? { ...row, alias: e.target.value } : row))} />
          <Input value={item.parameterName} placeholder="参数名" onChange={(e) => onChange(items.map((row) => row.id === item.id ? { ...row, parameterName: e.target.value } : row))} />
          <select className="h-9 rounded-md border bg-transparent px-2 text-sm" value={item.parameterSource} onChange={(e) => onChange(items.map((row) => row.id === item.id ? { ...row, parameterSource: e.target.value as 'literal' | 'parameter' } : row))}><option value="literal">固定值</option><option value="parameter">策略参数引用</option></select>
          <Input value={item.parameterValue} placeholder={item.parameterSource === 'parameter' ? '参数 code' : '值'} onChange={(e) => onChange(items.map((row) => row.id === item.id ? { ...row, parameterValue: e.target.value } : row))} />
          <Button type="button" variant="secondary" onClick={() => onChange(items.filter((row) => row.id !== item.id))}>删除</Button>
        </div>
      ))}
    </section>
  );
}

function ConditionEditor({ title, value, onChange, disabled }: { title: string; value: ConditionDraft; onChange: (value: ConditionDraft) => void; disabled?: boolean }) {
  return (
    <section className="space-y-2 rounded-md border border-slate-200 p-3 dark:border-slate-700">
      <div className="text-xs font-medium">{title}</div>
      <ConditionNodeEditor value={value} onChange={onChange} disabled={disabled} />
    </section>
  );
}

function ConditionNodeEditor({ value, onChange, disabled }: { value: ConditionDraft; onChange: (value: ConditionDraft) => void; disabled?: boolean }) {
  if (value.kind === 'group') {
    return (
      <div className="space-y-2 rounded border border-dashed border-slate-300 p-2 dark:border-slate-600">
        <div className="flex flex-wrap gap-2">
          <select className="h-9 rounded-md border bg-transparent px-2 text-sm" value={value.operator} disabled={disabled} onChange={(e) => {
            const operator = e.target.value as GroupOp;
            const children = operator === 'not' ? [value.children[0] ?? comparison('close', 'gt', '0')] : value.children;
            onChange({ ...value, operator, children });
          }}><option value="all">all</option><option value="any">any</option><option value="not">not</option></select>
          {value.operator !== 'not' ? <Button type="button" variant="secondary" onClick={() => onChange({ ...value, children: [...value.children, comparison('close', 'gt', '0')] })}>添加子条件</Button> : null}
          <Button type="button" variant="secondary" onClick={() => onChange(comparison('close', 'gt', '0'))}>改为比较</Button>
        </div>
        {value.children.map((child, index) => (
          <div key={child.id} className="flex gap-2">
            <div className="min-w-0 flex-1"><ConditionNodeEditor value={child} onChange={(next) => onChange({ ...value, children: value.children.map((row, childIndex) => childIndex === index ? next : row) })} disabled={disabled} /></div>
            {value.operator !== 'not' && value.children.length > 1 ? <Button type="button" variant="secondary" onClick={() => onChange({ ...value, children: value.children.filter((_, childIndex) => childIndex !== index) })}>删除</Button> : null}
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="grid gap-2 rounded border border-dashed border-slate-300 p-2 sm:grid-cols-[1fr_auto_1fr_auto] dark:border-slate-600">
      <ReferenceEditor value={value.left} onChange={(left) => onChange({ ...value, left })} disabled={disabled} />
      <select className="h-9 rounded-md border bg-transparent px-2 text-sm" value={value.operator} disabled={disabled} onChange={(e) => onChange({ ...value, operator: e.target.value as CmpOp })}>{OPS.map((op) => <option key={op}>{op}</option>)}</select>
      <ReferenceEditor value={value.right} onChange={(right) => onChange({ ...value, right })} disabled={disabled} />
      <Button type="button" variant="secondary" onClick={() => onChange({ id: uid(), kind: 'group', operator: 'all', children: [value, comparison('close', 'gt', '0')] })}>组合</Button>
    </div>
  );
}

function ReferenceEditor({ value, onChange, disabled }: { value: ReferenceDraft; onChange: (value: ReferenceDraft) => void; disabled?: boolean }) {
  return (
    <div className="grid gap-1 sm:grid-cols-2">
      <select className="h-9 rounded-md border bg-transparent px-2 text-sm" value={value.sourceType} disabled={disabled} onChange={(e) => onChange({ ...value, sourceType: e.target.value as SourceType })}><option value="field">字段</option><option value="indicator">指标输出</option><option value="parameter">策略参数</option><option value="literal">常量</option></select>
      {value.sourceType === 'literal' ? (
        <div className="grid grid-cols-[90px_1fr] gap-1">
          <select className="h-9 rounded-md border bg-transparent px-1 text-xs" value={value.literalType} onChange={(e) => onChange({ ...value, literalType: e.target.value as ParameterType })}><option value="number">number</option><option value="integer">integer</option><option value="string">string</option><option value="boolean">boolean</option></select>
          {value.literalType === 'boolean' ? <select className="h-9 rounded-md border bg-transparent px-2 text-sm" value={value.value} onChange={(e) => onChange({ ...value, value: e.target.value })}><option value="true">true</option><option value="false">false</option></select> : <Input value={value.value} onChange={(e) => onChange({ ...value, value: e.target.value })} />}
        </div>
      ) : <Input value={value.code} placeholder="引用 code" onChange={(e) => onChange({ ...value, code: e.target.value })} />}
    </div>
  );
}
