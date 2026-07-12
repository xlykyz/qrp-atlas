import type { StrategyCatalogItem } from '@/types/strategy';

/**
 * Mock strategy catalog for frontend workflow shell.
 * These are catalog/config objects only — not executable strategy code.
 */
export const MOCK_STRATEGIES: StrategyCatalogItem[] = [
  {
    code: 'system_b_basic',
    name: 'System B Basic',
    version: '1.0.0',
    family: 'trend',
    description:
      '基于预计算 System B 趋势状态的最小策略。入场依赖趋势确认，出场依赖退出触发，不在策略内重算指标。',
    scope: '单标的趋势跟踪；需要 System B 状态字段；适合验证策略/回测边界。',
    strategy_type: 'builtin',
    required_fields: ['trade_date', 'ticker'],
    required_indicators: ['system_b_trend_valid', 'system_b_exit_triggered'],
    parameter_schema: {
      min_hold_bars: {
        type: 'integer',
        required: false,
        default: 1,
        minimum: 0,
        maximum: 60,
        label: '最短持有天数',
        description: '入场后至少持有的交易日数（模拟参数，便于前端演示）。',
      },
      enable_reentry: {
        type: 'boolean',
        required: false,
        default: true,
        label: '允许再入场',
        description: '同一标的出场后是否允许再次 ENTER。',
      },
    },
  },
  {
    code: 'dual_sma_trend',
    name: '双均线趋势',
    version: '1.0.0',
    family: 'trend',
    description:
      '快慢均线交叉趋势策略。快线上穿慢线做多，下穿或跌破慢线退出。指标窗口参数化。',
    scope: '价格行为趋势类；适合中长期单标的或多标的并行演示。',
    strategy_type: 'builtin',
    required_fields: ['trade_date', 'ticker', 'close'],
    required_indicators: ['sma'],
    parameter_schema: {
      fast_window: {
        type: 'integer',
        required: true,
        default: 10,
        minimum: 2,
        maximum: 120,
        label: '快线窗口',
        description: '短期均线窗口（交易日）。',
      },
      slow_window: {
        type: 'integer',
        required: true,
        default: 30,
        minimum: 5,
        maximum: 250,
        label: '慢线窗口',
        description: '长期均线窗口，必须大于快线。',
      },
      exit_on_cross_down: {
        type: 'boolean',
        required: false,
        default: true,
        label: '死叉退出',
        description: '快线下穿慢线时是否立即 EXIT。',
      },
    },
  },
  {
    code: 'donchian_breakout',
    name: '唐奇安通道突破',
    version: '1.0.0',
    family: 'breakout',
    description:
      '价格突破 N 日最高价入场，跌破 M 日最低价或持有期满出场。通道计算排除当前 bar，避免未来函数。',
    scope: '突破类价格行为；适合趋势启动段；需注意假突破与成本。',
    strategy_type: 'builtin',
    required_fields: ['trade_date', 'ticker', 'high', 'low', 'close'],
    required_indicators: ['donchian_high', 'donchian_low'],
    parameter_schema: {
      entry_window: {
        type: 'integer',
        required: true,
        default: 20,
        minimum: 5,
        maximum: 120,
        label: '入场通道窗口',
        description: '突破所用最高价回看窗口。',
      },
      exit_window: {
        type: 'integer',
        required: true,
        default: 10,
        minimum: 2,
        maximum: 60,
        label: '出场通道窗口',
        description: '跌破所用最低价回看窗口。',
      },
      max_hold_bars: {
        type: 'integer',
        required: false,
        default: 40,
        minimum: 1,
        maximum: 250,
        label: '最长持有天数',
        description: '强制时间出场上限。',
      },
    },
  },
  {
    code: 'rolling_zscore_mean_reversion',
    name: '滚动 Z-Score 均值回归',
    version: '1.0.0',
    family: 'mean_reversion',
    description:
      '价格相对滚动均值的 z-score 超卖时做多，回归至均值或触及止损/止盈后退出。',
    scope: '均值回归；更适合震荡市；需配合最大持仓与成本约束。',
    strategy_type: 'builtin',
    required_fields: ['trade_date', 'ticker', 'close'],
    required_indicators: ['rolling_zscore'],
    parameter_schema: {
      lookback: {
        type: 'integer',
        required: true,
        default: 20,
        minimum: 5,
        maximum: 120,
        label: '回看窗口',
        description: '滚动均值与标准差窗口。',
      },
      entry_zscore: {
        type: 'number',
        required: true,
        default: -1.5,
        minimum: -5,
        maximum: 0,
        label: '入场 Z-Score',
        description: 'z-score 低于该阈值时 ENTER（做多）。',
      },
      exit_zscore: {
        type: 'number',
        required: true,
        default: 0,
        minimum: -2,
        maximum: 3,
        label: '出场 Z-Score',
        description: 'z-score 回升至该阈值时 EXIT。',
      },
      stop_loss_pct: {
        type: 'number',
        required: false,
        default: 5,
        minimum: 0.5,
        maximum: 30,
        label: '止损 (%)',
        description: '单笔最大亏损百分比。',
      },
    },
  },
];

export function getMockStrategy(code: string, version?: string): StrategyCatalogItem | undefined {
  const matches = MOCK_STRATEGIES.filter((s) => s.code === code);
  if (matches.length === 0) return undefined;
  if (version) return matches.find((s) => s.version === version);
  return matches[matches.length - 1];
}
