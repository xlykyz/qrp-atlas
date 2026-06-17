import type { RequestOptions } from './client';
import { request } from './client';

export interface StockInfo {
  ticker: string;
  name: string;
}

let cachedStockList: StockInfo[] | null = null;
let stockListPromise: Promise<StockInfo[]> | null = null;

export function getStockList(options?: Pick<RequestOptions, 'signal'>): Promise<StockInfo[]> {
  if (cachedStockList) return Promise.resolve(cachedStockList);
  if (stockListPromise) return stockListPromise;

  stockListPromise = request<StockInfo[]>('/api/stock/list', options).then((data) => {
    cachedStockList = data;
    stockListPromise = null;
    return data;
  }).catch((err) => {
    stockListPromise = null;
    throw err;
  });

  return stockListPromise;
}
