import type { DailyRow } from '../types';
import { request } from './client';

export function getDailyByDate(date: string): Promise<DailyRow[]> {
  return request(`/api/daily?date=${date}`);
}

export function getDailyByTicker(
  ticker: string,
  startDate?: string,
  endDate?: string,
): Promise<DailyRow[]> {
  const params = new URLSearchParams({ ticker });
  if (startDate) params.set('start_date', startDate);
  if (endDate) params.set('end_date', endDate);
  return request(`/api/daily?${params.toString()}`);
}

export function getDailyByDateRange(
  startDate: string,
  endDate?: string,
  limit?: number,
): Promise<DailyRow[]> {
  const params = new URLSearchParams({ start_date: startDate });
  if (endDate) params.set('end_date', endDate);
  if (limit) params.set('limit', String(limit));
  return request(`/api/daily?${params.toString()}`);
}
