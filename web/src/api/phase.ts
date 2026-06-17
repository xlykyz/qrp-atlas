import { normalizeDate } from '../shared/lib/date';
import type { PhaseRecord, PhaseWrite } from '../types';
import { request } from './client';

export function getPhase(date?: string): Promise<PhaseRecord[]> {
  return request('/api/phase', {
    query: {
      date: date ? normalizeDate(date) : undefined,
    },
  });
}

export function getPhaseByDateRange(
  startDate: string,
  endDate?: string,
): Promise<PhaseRecord[]> {
  return request('/api/phase', {
    query: {
      start_date: normalizeDate(startDate),
      end_date: endDate ? normalizeDate(endDate) : undefined,
    },
  });
}

export function createPhase(data: PhaseWrite): Promise<PhaseRecord> {
  return request('/api/phase', {
    method: 'POST',
    body: data,
  });
}
