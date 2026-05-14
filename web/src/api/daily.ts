import type { DailyRow } from '../types';
import { request } from './client';

/** 将 YYYYMMDD 格式统一转为 YYYY-MM-DD，已是 YYYY-MM-DD 则原样返回 */
function normalizeDate(date: string): string {
  if (/^\d{8}$/.test(date)) {
    const m = date.match(/^(\d{4})(\d{2})(\d{2})$/);
    if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  }
  return date;
}

export function getDailyByDate(date: string): Promise<DailyRow[]> {
  return request(`/api/daily?date=${normalizeDate(date)}`);
}

export function getDailyByTicker(
  ticker: string,
  startDate?: string,
  endDate?: string,
): Promise<DailyRow[]> {
  const params = new URLSearchParams({ ticker });
  if (startDate) params.set('start_date', normalizeDate(startDate));
  if (endDate) params.set('end_date', normalizeDate(endDate));
  return request(`/api/daily?${params.toString()}`);
}

export function getDailyByDateRange(
  startDate: string,
  endDate?: string,
  limit?: number,
): Promise<DailyRow[]> {
  const params = new URLSearchParams({ start_date: normalizeDate(startDate) });
  if (endDate) params.set('end_date', normalizeDate(endDate));
  if (limit) params.set('limit', String(limit));
  return request(`/api/daily?${params.toString()}`);
}
