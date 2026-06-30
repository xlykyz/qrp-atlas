import { request } from './client'

export interface ColumnInfo {
  name: string
  type: string
}

export interface TableQueryResult {
  columns: ColumnInfo[]
  rows: Record<string, unknown>[]
  total: number
  limit: number
  offset: number
}

export function listTables(): Promise<string[]> {
  return request('/api/tables')
}

export function getTableSchema(tableName: string): Promise<ColumnInfo[]> {
  return request(`/api/tables/${tableName}/schema`)
}

export function queryTable(
  tableName: string,
  limit = 200,
  offset = 0,
): Promise<TableQueryResult> {
  return request(`/api/tables/${tableName}`, {
    query: { limit, offset },
  })
}
