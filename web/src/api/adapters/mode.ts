/**
 * Adapter mode switch for strategy catalog, tasks, and results.
 *
 * Defaults:
 * - VITE_BACKTEST_WORKFLOW_SOURCE=http  → real product APIs
 * - VITE_BACKTEST_WORKFLOW_SOURCE=mock  → local mock adapters
 * - VITE_BACKTEST_RESULT_SOURCE can still override results independently
 */

export type AdapterSource = 'mock' | 'http';

function normalize(value: string | undefined): AdapterSource | null {
  const raw = value?.toLowerCase().trim();
  if (raw === 'http' || raw === 'mock') return raw;
  return null;
}

/** Shared workflow source for catalog + tasks (+ default for results). */
export function getWorkflowSource(): AdapterSource {
  return (
    normalize(import.meta.env.VITE_BACKTEST_WORKFLOW_SOURCE as string | undefined) ??
    // Backward-compatible fallback: prefer result source when set, else real HTTP.
    normalize(import.meta.env.VITE_BACKTEST_RESULT_SOURCE as string | undefined) ??
    'http'
  );
}

export function getResultSource(): AdapterSource {
  return (
    normalize(import.meta.env.VITE_BACKTEST_RESULT_SOURCE as string | undefined) ??
    getWorkflowSource()
  );
}

export function isMockCatalogAndTasksEnabled(): boolean {
  return getWorkflowSource() === 'mock';
}
