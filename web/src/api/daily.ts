import { normalizeDate } from '../shared/lib/date';
import type { DailyRow } from '../types';
import { request } from './client';

export type AdjustmentMode = 'raw' | 'qfq' | 'hfq';

const ADJUSTMENT_STORAGE_KEY = 'stock-review-adjustment-mode';
const ADJUSTMENT_CONTROL_ID = 'stock-review-adjustment-control';
let adjustmentControlObserver: MutationObserver | null = null;

function getStoredAdjustmentMode(): AdjustmentMode {
  if (typeof window === 'undefined') return 'raw';
  const value = window.localStorage.getItem(ADJUSTMENT_STORAGE_KEY);
  return value === 'qfq' || value === 'hfq' || value === 'raw' ? value : 'raw';
}

function findStockMainChartHost(): HTMLElement | null {
  if (typeof document === 'undefined') return null;

  const candidates = Array.from(document.querySelectorAll<HTMLElement>('div'));
  return candidates.find((el) => {
    const classes = el.classList;
    const looksLikeMainChart =
      classes.contains('rounded-t-lg') &&
      classes.contains('overflow-hidden') &&
      classes.contains('w-full');
    const hasMainChartHeight = el.style.height === '800px' || el.clientHeight >= 700;
    return looksLikeMainChart && hasMainChartHeight;
  }) ?? null;
}

function createAdjustmentControl(): HTMLDivElement {
  const container = document.createElement('div');
  container.id = ADJUSTMENT_CONTROL_ID;
  container.style.position = 'absolute';
  container.style.top = '12px';
  container.style.right = '72px';
  container.style.zIndex = '30';
  container.style.display = 'flex';
  container.style.alignItems = 'center';
  container.style.gap = '8px';
  container.style.padding = '6px 10px';
  container.style.border = '1px solid rgba(100, 116, 139, 0.55)';
  container.style.borderRadius = '9999px';
  container.style.background = 'rgba(15, 23, 42, 0.82)';
  container.style.boxShadow = '0 10px 30px rgba(15, 23, 42, 0.28)';
  container.style.backdropFilter = 'blur(10px)';
  container.style.color = '#cbd5e1';
  container.style.fontSize = '12px';
  container.style.pointerEvents = 'auto';

  const stopChartGesture = (event: Event) => {
    event.stopPropagation();
  };
  container.addEventListener('pointerdown', stopChartGesture);
  container.addEventListener('pointermove', stopChartGesture);
  container.addEventListener('pointerup', stopChartGesture);
  container.addEventListener('wheel', stopChartGesture, { passive: true });

  const label = document.createElement('span');
  label.textContent = '复权';
  label.style.color = '#94a3b8';

  const select = document.createElement('select');
  select.value = getStoredAdjustmentMode();
  select.style.border = '1px solid rgba(71, 85, 105, 0.85)';
  select.style.borderRadius = '9999px';
  select.style.background = '#0f172a';
  select.style.color = '#e2e8f0';
  select.style.padding = '3px 8px';
  select.style.outline = 'none';

  const options: Array<[AdjustmentMode, string]> = [
    ['raw', '除权'],
    ['qfq', '前复权'],
    ['hfq', '后复权'],
  ];
  for (const [value, text] of options) {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = text;
    select.appendChild(option);
  }

  select.addEventListener('change', () => {
    const next = select.value as AdjustmentMode;
    window.localStorage.setItem(ADJUSTMENT_STORAGE_KEY, next);
    window.location.reload();
  });

  container.appendChild(label);
  container.appendChild(select);
  return container;
}

function attachStockAdjustmentControl(): boolean {
  const host = findStockMainChartHost();
  if (!host) return false;

  if (!host.style.position) {
    host.style.position = 'relative';
  }

  const existing = document.getElementById(ADJUSTMENT_CONTROL_ID) as HTMLDivElement | null;
  const control = existing ?? createAdjustmentControl();
  if (control.parentElement !== host) {
    host.appendChild(control);
  }
  return true;
}

function mountStockAdjustmentControl(): void {
  if (typeof window === 'undefined' || typeof document === 'undefined') return;
  if (!window.location.pathname.startsWith('/stock')) return;
  if (attachStockAdjustmentControl()) return;
  if (adjustmentControlObserver) return;

  adjustmentControlObserver = new MutationObserver(() => {
    if (attachStockAdjustmentControl()) {
      adjustmentControlObserver?.disconnect();
      adjustmentControlObserver = null;
    }
  });
  adjustmentControlObserver.observe(document.body, { childList: true, subtree: true });
}

export function getDailyByDate(date: string): Promise<DailyRow[]> {
  return request('/api/daily', {
    query: { date: normalizeDate(date) },
  });
}

export function getDailyByTicker(
  ticker: string,
  startDate?: string,
  endDate?: string,
): Promise<DailyRow[]> {
  mountStockAdjustmentControl();
  return request('/api/daily', {
    query: {
      ticker,
      start_date: startDate ? normalizeDate(startDate) : undefined,
      end_date: endDate ? normalizeDate(endDate) : undefined,
      adjustment: getStoredAdjustmentMode(),
    },
  });
}

export function getDailyByDateRange(
  startDate: string,
  endDate?: string,
  limit?: number,
): Promise<DailyRow[]> {
  return request('/api/daily', {
    query: {
      start_date: normalizeDate(startDate),
      end_date: endDate ? normalizeDate(endDate) : undefined,
      limit,
    },
  });
}

export function getDailyDates(
  startDate?: string,
  endDate?: string,
  limit?: number,
): Promise<string[]> {
  return request('/api/daily/dates', {
    query: {
      start_date: startDate ? normalizeDate(startDate) : undefined,
      end_date: endDate ? normalizeDate(endDate) : undefined,
      limit,
    },
  });
}
