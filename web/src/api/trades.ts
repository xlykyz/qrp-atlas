import type { TradeRecord, TradeWrite, TradePatch } from '../types';
import { request } from './client';

export function getTrades(tradeId?: string): Promise<TradeRecord[]> {
  if (tradeId) {
    return request(`/api/trades?trade_id=${tradeId}`);
  }
  return request('/api/trades');
}

export function createTrade(data: TradeWrite): Promise<TradeRecord> {
  return request('/api/trades', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export function updateTrade(
  tradeId: string,
  data: TradePatch,
): Promise<TradeRecord> {
  return request(`/api/trades/${tradeId}`, {
    method: 'PATCH',
    body: JSON.stringify(data),
  });
}
