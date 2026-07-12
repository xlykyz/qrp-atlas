import type { StrategyCatalogApi } from './types';
import { AdapterError } from './types';
import { getMockStrategy, MOCK_STRATEGIES } from '../mock/strategies';

function delay<T>(value: T, ms = 120): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), ms));
}

export function createMockStrategyCatalogApi(): StrategyCatalogApi {
  return {
    async listStrategies() {
      return delay(MOCK_STRATEGIES.map((s) => ({ ...s, parameter_schema: { ...s.parameter_schema } })));
    },
    async getStrategy(code, version) {
      const item = getMockStrategy(code, version);
      if (!item) {
        throw new AdapterError(404, `strategy not found: ${code}${version ? `@${version}` : ''}`);
      }
      return delay({ ...item, parameter_schema: { ...item.parameter_schema } });
    },
  };
}
