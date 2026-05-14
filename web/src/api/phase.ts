import type { PhaseRecord, PhaseWrite } from '../types';
import { request } from './client';

export function getPhase(date?: string): Promise<PhaseRecord[]> {
  if (date) {
    return request(`/api/phase?date=${date}`);
  }
  return request('/api/phase');
}

export function getPhaseByDateRange(
  startDate: string,
  endDate?: string,
): Promise<PhaseRecord[]> {
  const params = new URLSearchParams({ start_date: startDate });
  if (endDate) params.set('end_date', endDate);
  return request(`/api/phase?${params.toString()}`);
}

export function createPhase(data: PhaseWrite): Promise<PhaseRecord> {
  return request('/api/phase', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}
