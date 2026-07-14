import { useEffect, useMemo, useState } from 'react';
import { Link, useNavigate, useOutletContext } from 'react-router-dom';
import { Button, buttonVariants } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import type { StrategyCatalogItem } from '@/types/strategy';
import { StrategyCatalogPanel } from '@/features/backtest-workflow/components/strategy-catalog-panel';
import { StrategyDetailCard } from '@/features/backtest-workflow/components/strategy-detail-card';
import { SchemaParamForm } from '@/features/backtest-workflow/components/schema-param-form';
import { BacktestConfigForm } from '@/features/backtest-workflow/components/backtest-config-form';
import { TaskList } from '@/features/backtest-workflow/components/task-list';
import { RunComparePanel } from '@/features/backtest-workflow/components/run-compare-panel';
import { useStrategyCatalog } from '@/features/backtest-workflow/hooks/use-strategy-catalog';
import { useBacktestTasks } from '@/features/backtest-workflow/hooks/use-backtest-tasks';
import { useRunCompare } from '@/features/backtest-workflow/hooks/use-run-compare';
import {
  createDefaultFormState,
  defaultParamsFromSchema,
  formStateToCreateRequest,
  type BacktestWorkflowFormState,
} from '@/features/backtest-workflow/lib/form-model';
import { validateWorkflowForm } from '@/features/backtest-workflow/lib/validate';
import { getResultSource, getWorkflowSource } from '@/api/adapters/mode';

type Step = 'configure' | 'tasks' | 'compare';

export default function BacktestWorkflowPage() {
  const { setPageTitle, setHeaderControls } = useOutletContext<{
    setPageTitle: (t: string) => void;
    setHeaderControls: (c: React.ReactNode | null) => void;
  }>();
  const navigate = useNavigate();

  const { strategies, loading: catalogLoading, error: catalogError } = useStrategyCatalog();
  const {
    tasks,
    loading: tasksLoading,
    error: tasksError,
    submitting,
    submitError,
    createTask,
  } = useBacktestTasks();

  const [form, setForm] = useState<BacktestWorkflowFormState>(createDefaultFormState);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const [step, setStep] = useState<Step>('configure');
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);

  const workflowSource = getWorkflowSource();
  const resultSource = getResultSource();
  const compare = useRunCompare();
  const isMockMode = workflowSource === 'mock';

  useEffect(() => {
    setPageTitle('策略回测工作流');
    setHeaderControls(
      <div className="flex items-center gap-2">
        <Link
          to={`/backtest?source=${resultSource}`}
          className={cn(buttonVariants({ variant: 'outline', size: 'sm' }))}
        >
          回测分析
        </Link>
      </div>,
    );
    return () => {
      setPageTitle('');
      setHeaderControls(null);
    };
  }, [setPageTitle, setHeaderControls, resultSource]);

  const selectedStrategy: StrategyCatalogItem | null = useMemo(() => {
    if (!form.strategyCode) return null;
    return (
      strategies.find(
        (s) => s.code === form.strategyCode && s.version === form.strategyVersion,
      ) ??
      strategies.find((s) => s.code === form.strategyCode) ??
      null
    );
  }, [strategies, form.strategyCode, form.strategyVersion]);

  function patchForm(patch: Partial<BacktestWorkflowFormState>) {
    setForm((prev) => ({ ...prev, ...patch }));
  }

  function handleSelectStrategy(strategy: StrategyCatalogItem) {
    setForm((prev) => ({
      ...prev,
      strategyCode: strategy.code,
      strategyVersion: strategy.version,
      strategyParams: defaultParamsFromSchema(strategy.parameter_schema),
    }));
    setFieldErrors((prev) => {
      const next = { ...prev };
      delete next.strategyCode;
      Object.keys(next)
        .filter((k) => k.startsWith('param.'))
        .forEach((k) => delete next[k]);
      return next;
    });
  }

  async function handleSubmit() {
    const schema = selectedStrategy?.parameter_schema ?? null;
    const errors = validateWorkflowForm(form, schema);
    setFieldErrors(errors);
    if (Object.keys(errors).length > 0) return;

    const dto = formStateToCreateRequest(form);
    try {
      const task = await createTask(dto);
      setSelectedTaskId(task.task_id);
      setStep('tasks');
    } catch {
      // submitError already set in hook
    }
  }

  function openResult(runId: string) {
    navigate(`/backtest?runId=${encodeURIComponent(runId)}&source=${isMockMode ? 'mock' : 'http'}`);
  }

  function addToCompare(runId: string) {
    if (!compare.selectedRunIds.includes(runId)) {
      compare.setRuns([...compare.selectedRunIds, runId].slice(0, 4));
    }
    setStep('compare');
  }

  const steps: { key: Step; label: string }[] = [
    { key: 'configure', label: '1. 配置' },
    { key: 'tasks', label: '2. 任务' },
    { key: 'compare', label: '3. 对比' },
  ];

  return (
    <div className="space-y-6">
      <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-xs text-amber-900 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-200">
        {isMockMode ? (
          <>
            当前为<strong>Mock 模式</strong>：策略目录与任务状态由 mock adapter
            驱动，成功任务绑定本地 fixture run，不执行真实策略算法。
          </>
        ) : (
          <>
            当前为<strong>真实 HTTP 模式</strong>：策略目录、任务创建/状态与结果
            均来自后端产品 API。任务与结果会持久化，刷新后仍可再次打开。
          </>
        )}
      </div>

      <div className="flex flex-wrap gap-1 rounded-md border border-slate-200 bg-white p-1 dark:border-slate-800 dark:bg-slate-900">
        {steps.map((s) => (
          <button
            key={s.key}
            type="button"
            onClick={() => setStep(s.key)}
            className={`rounded px-3 py-1.5 text-sm transition-colors ${
              step === s.key
                ? 'bg-slate-200 text-slate-900 dark:bg-slate-700 dark:text-white'
                : 'text-slate-500 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'
            }`}
          >
            {s.label}
          </button>
        ))}
      </div>

      {step === 'configure' && (
        <div className="space-y-6">
          <section className="grid gap-4 lg:grid-cols-2">
            <div>
              <h2 className="mb-3 text-sm font-medium text-slate-700 dark:text-slate-200">
                策略目录
              </h2>
              <StrategyCatalogPanel
                strategies={strategies}
                selectedCode={form.strategyCode || null}
                loading={catalogLoading}
                error={catalogError}
                onSelect={handleSelectStrategy}
              />
              {fieldErrors.strategyCode ? (
                <p className="mt-2 text-xs text-red-500">{fieldErrors.strategyCode}</p>
              ) : null}
            </div>
            <div>
              <h2 className="mb-3 text-sm font-medium text-slate-700 dark:text-slate-200">
                策略说明
              </h2>
              <StrategyDetailCard strategy={selectedStrategy} />
            </div>
          </section>

          <section>
            <h2 className="mb-3 text-sm font-medium text-slate-700 dark:text-slate-200">
              策略参数
            </h2>
            {selectedStrategy ? (
              <SchemaParamForm
                schema={selectedStrategy.parameter_schema}
                values={form.strategyParams}
                errors={fieldErrors}
                disabled={submitting}
                onChange={(strategyParams) => patchForm({ strategyParams })}
              />
            ) : (
              <div className="rounded-lg border border-dashed border-slate-300 p-4 text-sm text-slate-500 dark:border-slate-700 dark:text-slate-400">
                选择策略后将根据 parameter schema 动态生成参数表单。
              </div>
            )}
          </section>

          <section>
            <h2 className="mb-3 text-sm font-medium text-slate-700 dark:text-slate-200">
              回测任务配置
            </h2>
            <div className="rounded-lg border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900/40">
              <BacktestConfigForm
                form={form}
                errors={fieldErrors}
                disabled={submitting}
                onChange={patchForm}
              />
            </div>
          </section>

          <div className="flex flex-wrap items-center gap-3">
            <Button onClick={() => void handleSubmit()} disabled={submitting}>
              {submitting ? '提交中…' : '创建回测任务'}
            </Button>
            <Button
              variant="outline"
              disabled={!selectedStrategy}
              onClick={() => {
                if (!selectedStrategy) return;
                setForm((prev) => ({
                  ...prev,
                  strategyParams: defaultParamsFromSchema(selectedStrategy.parameter_schema),
                }));
              }}
            >
              重置策略参数
            </Button>
            {submitError ? (
              <span className="text-sm text-red-500">{submitError}</span>
            ) : null}
          </div>
        </div>
      )}

      {step === 'tasks' && (
        <section>
          <div className="mb-3 flex items-center justify-between gap-2">
            <h2 className="text-sm font-medium text-slate-700 dark:text-slate-200">
              回测任务
            </h2>
            <Button size="sm" variant="outline" onClick={() => setStep('configure')}>
              修改配置再运行
            </Button>
          </div>
          <TaskList
            tasks={tasks}
            loading={tasksLoading}
            error={tasksError}
            selectedTaskId={selectedTaskId}
            onSelect={(t) => setSelectedTaskId(t.task_id)}
            onOpenResult={openResult}
            onAddToCompare={addToCompare}
          />
        </section>
      )}

      {step === 'compare' && (
        <section>
          <h2 className="mb-3 text-sm font-medium text-slate-700 dark:text-slate-200">
            Run 对比
          </h2>
          <RunComparePanel
            availableRuns={compare.availableRuns}
            runsLoading={compare.runsLoading}
            runsError={compare.runsError}
            selectedRunIds={compare.selectedRunIds}
            orderedDetails={compare.orderedDetails}
            metricRows={compare.metricRows}
            configDiff={compare.configDiff}
            onToggleRun={compare.toggleRun}
          />
        </section>
      )}
    </div>
  );
}
