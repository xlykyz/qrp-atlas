export type SortDir = 'asc' | 'desc';

type Comparable = string | number | boolean | Date | null | undefined;

function toComparable(value: unknown): Comparable {
  if (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    value instanceof Date ||
    value == null
  ) {
    return value;
  }
  return String(value);
}

export function compare(a: unknown, b: unknown, dir: SortDir = 'asc'): number {
  const left = toComparable(a);
  const right = toComparable(b);
  const multiplier = dir === 'desc' ? -1 : 1;

  if (left == null && right == null) return 0;
  if (left == null) return 1;
  if (right == null) return -1;

  if (typeof left === 'number' && typeof right === 'number') {
    return (left - right) * multiplier;
  }

  if (left instanceof Date || right instanceof Date) {
    const leftTime = left instanceof Date ? left.getTime() : new Date(String(left)).getTime();
    const rightTime = right instanceof Date ? right.getTime() : new Date(String(right)).getTime();
    return (leftTime - rightTime) * multiplier;
  }

  if (typeof left === 'boolean' && typeof right === 'boolean') {
    return (Number(left) - Number(right)) * multiplier;
  }

  return String(left).localeCompare(String(right)) * multiplier;
}

export function sortRows<T>(
  rows: readonly T[],
  getValue: keyof T | ((row: T) => unknown),
  dir: SortDir = 'asc',
): T[] {
  const read = typeof getValue === 'function'
    ? getValue
    : (row: T) => row[getValue];

  return [...rows].sort((a, b) => compare(read(a), read(b), dir));
}
