import { request } from './client'
import type { ColumnInfo } from './tables'

export interface SqlQueryResult {
  columns: ColumnInfo[]
  rows: Record<string, unknown>[]
  total: number
  limit?: number
  truncated?: boolean
}

export function querySql(sql: string): Promise<SqlQueryResult> {
  return request('/api/dev/sql', {
    method: 'POST',
    body: { sql },
  })
}