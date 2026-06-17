import type { TradePatch, TradeRecord, TradeWrite } from '../types';
import { request } from './client';

export function getTrades(tradeId?: string): Promise<TradeRecord[]> {
  return request('/api/trades', {
    query: {
      trade_id: tradeId,
    },
  });
}

export function createTrade(data: TradeWrite): Promise<TradeRecord> {
  return request('/api/trades', {
    method: 'POST',
    body: data,
  });
}

export function updateTrade(
  tradeId: string,
  data: TradePatch,
): Promise<TradeRecord> {
  return request(`/api/trades/${tradeId}`, {
    method: 'PATCH',
    body: data,
  });
}
