/**
 * Adapter mode switch.
 *
 * Workflow shell currently defaults to mock for strategy catalog + tasks
 * because those backend APIs are not yet available.
 * Result API uses mock fixtures so the demo works offline; set
 * VITE_BACKTEST_RESULT_SOURCE=http to prefer the live result endpoints.
 */

export type ResultSource = 'mock' | 'http';

export function getResultSource(): ResultSource {
  const raw = (import.meta.env.VITE_BACKTEST_RESULT_SOURCE as string | undefined)?.toLowerCase();
  if (raw === 'http') return 'http';
  return 'mock';
}

/** Strategy catalog and task APIs have no stable backend yet — always mock for now. */
export function isMockCatalogAndTasksEnabled(): boolean {
  return true;
}

