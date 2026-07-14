import type { StrategyCatalogApi } from './types';
import { AdapterError } from './types';
import type { StrategyCatalogItem } from '@/types/strategy';
import { ApiError, request as httpRequest } from '../client';

function mapError(err: unknown): never {
  if (err instanceof ApiError) {
    throw new AdapterError(err.status, err.message);
  }
  throw err;
}

export function createHttpStrategyCatalogApi(): StrategyCatalogApi {
  return {
    async listStrategies() {
      try {
        return await httpRequest<StrategyCatalogItem[]>('/api/strategies');
      } catch (err) {
        mapError(err);
      }
    },
    async getStrategy(code, version) {
      try {
        return await httpRequest<StrategyCatalogItem>(
          `/api/strategies/${encodeURIComponent(code)}`,
          { query: version ? { version } : undefined },
        );
      } catch (err) {
        mapError(err);
      }
    },
  };
}
