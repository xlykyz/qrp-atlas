import { request } from './client';

export interface DeclarativeStrategyRecord {
  code: string;
  version: string;
  owner_user_id: string;
  name: string;
  description: string;
  status: string;
  definition: Record<string, unknown>;
  created_at: string;
  archived_at?: string | null;
  referenced_by_runs?: boolean;
}

export function validateDeclarativeStrategy(definition: Record<string, unknown>) {
  return request<Record<string, unknown>>('/api/declarative-strategies/validate', {
    method: 'POST',
    body: { definition },
  });
}

export function createDeclarativeStrategy(definition: Record<string, unknown>) {
  return request<DeclarativeStrategyRecord>('/api/declarative-strategies', {
    method: 'POST',
    body: { definition },
  });
}

export function listDeclarativeStrategies(includeArchived = false) {
  return request<DeclarativeStrategyRecord[]>('/api/declarative-strategies', {
    query: { include_archived: includeArchived },
  });
}

export function getDeclarativeStrategy(code: string, version: string) {
  return request<DeclarativeStrategyRecord>(`/api/declarative-strategies/${code}/${version}`);
}

export function createDeclarativeVersion(code: string, definition: Record<string, unknown>) {
  return request<DeclarativeStrategyRecord>(`/api/declarative-strategies/${code}/versions`, {
    method: 'POST',
    body: { definition },
  });
}

export function setDeclarativeStatus(code: string, version: string, status: string) {
  return request<DeclarativeStrategyRecord>(
    `/api/declarative-strategies/${code}/${version}/status`,
    {
      method: 'POST',
      body: { status },
    },
  );
}
