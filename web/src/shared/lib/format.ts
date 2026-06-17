type NullableNumber = number | null | undefined;

const EMPTY = '—';

export function formatPct(value: NullableNumber, decimals = 2): string {
  if (value == null || Number.isNaN(value)) return EMPTY;
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(decimals)}%`;
}

export function pctColor(value: NullableNumber): string {
  if (value == null || Number.isNaN(value)) return 'text-gray-400';
  if (value > 0) return 'text-red-500';
  if (value < 0) return 'text-green-500';
  return 'text-gray-400';
}

export function formatAmount(value: NullableNumber, decimals = 2): string {
  if (value == null || Number.isNaN(value)) return EMPTY;
  return `${(value / 1e8).toFixed(decimals)}亿`;
}

export function formatVolume(value: NullableNumber): string {
  if (value == null || Number.isNaN(value)) return EMPTY;
  if (value >= 1e8) return `${(value / 1e8).toFixed(2)}亿`;
  if (value >= 1e4) return `${(value / 1e4).toFixed(0)}万`;
  return value.toLocaleString();
}

export function formatNum(value: NullableNumber, decimals = 2): string {
  if (value == null || Number.isNaN(value)) return EMPTY;
  return value.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}
