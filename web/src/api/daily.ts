import { normalizeDate } from '../shared/lib/date';
import type { DailyRow } from '../types';
import { request } from './client';

export type AdjustmentMode = 'raw' | 'qfq' | 'hfq';

export function getDailyByDate(date: string): Promise<DailyRow[]> {
  return request('/api/daily', {
    query: { date: normalizeDate(date) },
  });
}

export function getDailyByTicker(
  ticker: string,
  startDate?: string,
  endDate?: string,
  adjustment: AdjustmentMode = 'raw',
): Promise<DailyRow[]> {
  return request('/api/daily', {
    query: {
      ticker,
      start_date: startDate ? normalizeDate(startDate) : undefined,
      end_date: endDate ? normalizeDate(endDate) : undefined,
      adjustment,
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
