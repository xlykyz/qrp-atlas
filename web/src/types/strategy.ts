/**
 * Strategy catalog and parameter schema types for the backtest workflow shell.
 * Keep these UI/API-facing types independent of page form state.
 */

export type ParamValueType = 'number' | 'integer' | 'boolean' | 'string';

export interface ParameterSpec {
  type: ParamValueType;
  required?: boolean;
  default?: number | string | boolean | null;
  minimum?: number | null;
  maximum?: number | null;
  description?: string;
  /** Display label; falls back to param key when omitted. */
  label?: string;
  /** Optional discrete choices for select-style fields. */
  enum?: Array<string | number | boolean>;
}

export type ParameterSchema = Record<string, ParameterSpec>;

export type StrategyFamily =
  | 'price_action'
  | 'mean_reversion'
  | 'trend'
  | 'breakout'
  | 'cross_sectional'
  | 'other';

export interface StrategyCatalogItem {
  code: string;
  name: string;
  version: string;
  family: StrategyFamily;
  description: string;
  /** Human-readable applicability notes. */
  scope: string;
  strategy_type: 'builtin' | 'declarative';
  required_fields: string[];
  required_indicators: string[];
  parameter_schema: ParameterSchema;
  /** Product capability metadata from backend catalog. */
  product_supported?: boolean;
  requires_historical_universe?: boolean;
  supported_universe_modes?: Array<'tickers' | 'preset' | 'index_components'>;
  supported_entry_timings?: Array<'next_open' | 'same_close' | 'next_close'>;
  requires_portfolio_config?: boolean;
}

export type StrategyParamValues = Record<string, number | string | boolean | null>;
