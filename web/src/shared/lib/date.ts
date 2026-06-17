const COMPACT_DATE_RE = /^(\d{4})(\d{2})(\d{2})$/;
const DASHED_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

export function normalizeDate(date: string): string {
  const value = date.trim();
  const match = value.match(COMPACT_DATE_RE);
  if (match) return `${match[1]}-${match[2]}-${match[3]}`;
  return value;
}

export function toCompactDate(date: string): string {
  const value = normalizeDate(date);
  if (DASHED_DATE_RE.test(value)) return value.replaceAll('-', '');
  return value;
}

export function isDateLike(date: string): boolean {
  const value = date.trim();
  return COMPACT_DATE_RE.test(value) || DASHED_DATE_RE.test(value);
}
