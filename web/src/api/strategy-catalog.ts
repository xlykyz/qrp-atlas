/**
 * StrategyCatalogApi facade.
 * Pages/hooks import from here — never from mock modules.
 */

import { getStrategyCatalogApi } from './adapters';

export function listStrategies() {
  return getStrategyCatalogApi().listStrategies();
}

export function getStrategy(code: string, version?: string) {
  return getStrategyCatalogApi().getStrategy(code, version);
}
