export interface DailyRow {
  trade_date: string;
  ticker: string;
  name?: string | null;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  close?: number | null;
  pct_change?: number | null;
  pre_close?: number | null;
  volume?: number | null;
  amount?: number | null;
  turnover?: number | null;
  market_cap?: number | null;
  float_cap?: number | null;
  is_st?: boolean | null;
  is_limit_up?: boolean | null;
  is_limit_down?: boolean | null;
  created_at?: string | null;
  board?: string | null;
}

export interface PhaseRecord {
  trade_date: string;
  phase?: string | null;
  M1_core?: boolean | null;
  M2_front?: boolean | null;
  M3_identifiable?: boolean | null;
  V_triggered?: boolean | null;
  notes?: string | null;
  created_at?: string | null;
}

export interface PhaseWrite {
  trade_date: string;
  phase?: string | null;
  M1_core?: boolean | null;
  M2_front?: boolean | null;
  M3_identifiable?: boolean | null;
  V_triggered?: boolean | null;
  notes?: string | null;
}

export interface TradeRecord {
  trade_id: string;
  ticker?: string | null;
  entry_date?: string | null;
  entry_price?: number | null;
  path_type?: string | null;
  half_sell_trigger?: number | null;
  half_sell_date?: string | null;
  half_sell_price?: number | null;
  exit_date?: string | null;
  exit_price?: number | null;
  position_pct?: number | null;
  notes?: string | null;
}

export interface TradeWrite {
  ticker?: string | null;
  entry_date?: string | null;
  entry_price?: number | null;
  path_type?: string | null;
  half_sell_trigger?: number | null;
  half_sell_date?: string | null;
  half_sell_price?: number | null;
  exit_date?: string | null;
  exit_price?: number | null;
  position_pct?: number | null;
  notes?: string | null;
}

export interface TradePatch {
  exit_date?: string | null;
  exit_price?: number | null;
  half_sell_date?: string | null;
  half_sell_price?: number | null;
  notes?: string | null;
}
