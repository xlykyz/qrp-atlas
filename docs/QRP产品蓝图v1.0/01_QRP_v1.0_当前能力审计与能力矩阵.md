# QRP v1.0 当前能力审计与能力矩阵

> 初始审计基线：2026-07-12。
> 任务 08 发布验收校准：2026-07-15，对照 main `69f31e41daebcb04d75af9ec71191ddf06e60ebb`。
> 数据事实：用户已确认本地数据库与 `contracts` 严格一致。

## 一、审计结论

QRP 后端研究闭环、四类产品主链与发布级结果能力已经完成，当前进入最终发布验收：

```text
contracts → indicators → strategies → backtest runtime / portfolio / product → results → api / web
```

### 已成熟能力

- A 股日线、估值、市值、涨跌停、停牌、指数、财务三表、历史行业、历史指数成分与 PIT as_of 查询；
- 参数化时间序列指标、横截面算子/因子/中性化、事件指标、市场残差指标；
- 经典单标的策略、横截面 long-only 策略、`event_drift_basic`、`market_residual_mean_reversion`；
- 组合共享资金、目标权重、A 股现实成交与真实净值；
- 代表性残差 walk-forward / 成本压力 / 参数敏感性 / 滚动表现与 OOS 结果包；
- 经典、横截面、事件与声明式四类真实回测产品链；
- 标准结果、benchmark/excess、realized exposures、targets、reproducibility、compare 与 replay；
- owner 隔离的策略目录、任务和结果访问；
- 回测 workflow、分析页与声明式策略受控编辑器。

### 当前剩余工作

1. **08**：执行四类真实产品链、多用户隔离、历史读取、compare/replay 与完整构建测试；
2. 输出最终验收报告、已知边界和发布候选结论；
3. 等待独立 PR 最终复验，复验前不合并、不打正式版本标签。

**本文件不宣告 v1.0 发布完成。**

## 二、任务完成总表

| 任务 | 状态 | 关键证据 |
|---|---|---|
| 01 参数化指标与经典策略 | ✅ | `IndicatorRequest`、经典策略与动态 runtime |
| 02 组合账户与 A 股现实回测 | ✅ | `PortfolioBacktestEngine`、T+1/涨跌停/停牌/整数手 |
| 03 PIT 数据与查询 | ✅ | 财务/行业/指数 as_of 查询 |
| 04 横截面与多因子 | ✅ | 04-A～04-E 全部完成 |
| 05 事件驱动研究链路 | ✅ | `earnings_forecast_event` + EventFrame + 事件指标 + `event_drift_basic` + `run_event_drift_portfolio_backtest` |
| 06 残差与稳健性 | ✅ | 市场/行业残差研究 + residual robustness 包 |
| 07-A 真实回测产品主链 | ✅ | product task 状态机 + 经典策略产品路径 |
| 07-B1 横截面动量产品闭环 | ✅ | product catalog/service + 前端 workflow |
| 07-B2 事件产品闭环 | ✅ | PR #29 已合并，事件产品真实闭环完成 |
| 07-C 标准结果与分析封板 | ✅ | PR #30 已合并，标准结果与复现审计封板 |
| 07-D 声明式策略产品 | ✅ | PR #31 已合并，owner/版本/前端能力封板 |
| 08 总体验收与发布 | 🔄 | `release/v1.0-acceptance` 独立验收中 |

## 三、现有模块真实状态

### 3.1 contracts / 数据层

已实现并可用于回测/研究：

- 日线行情、daily basic、复权、股票信息、交易日历；
- 指数日线、涨跌停、停牌；
- 财务三表、财务指标、历史行业、历史指数成分（PIT）；
- **v1.0 第一类标准事件表：`earnings_forecast_event`**（非泛化 `corporate_event`）。

PIT 语义已落地：

- `announcement_date` / `published_at` / `available_trade_date` / revision / source / ingested；
- as_of 查询拒绝未来数据；修订保留历史版本。

明确不是 v1.0 当前验收对象：

- 泛化 `corporate_event` 统一事件主表（可延后到 v1.1 扩展更多事件类型）；
- 无风险利率、股指期货、期权、借券与融资成本。

### 3.2 indicators

已实现：

- 参数化 SMA / return / Donchian / rolling 统计 / 经典技术指标族；
- 横截面 rank / winsorize / z-score、正式因子、行业市值中性化；
- 业绩预告事件指标：`profit_change_midpoint`、`event_age`、`event_window` 等；
- 市场残差 / residual z-score 等相对价值指标。

当前限制：

- 指标版本快照仍需在 07-C 产品结果中统一封板；
- 目录 API 已部分接入，产品展示可继续完善。

### 3.3 strategies

已实现：

- 标准 `StrategyDefinition / StrategyDecision` 与版本化注册表；
- 经典价格行为策略与扩展技术策略；
- `cross_sectional_momentum_long_only` / `multifactor_long_only`；
- `event_drift_basic`；
- `market_residual_mean_reversion`；
- 受限声明式策略求值底座（`strategies/declarative`）。

产品目录当前正式暴露：

- 经典产品策略（07-A）；
- `cross_sectional_momentum_long_only`（07-B1）。

尚未产品化：

- `event_drift_basic`（07-B2）；
- 用户声明式策略 CRUD / 版本持久化 / 前端编辑器（07-D）。

### 3.4 backtest runtime / portfolio / research

已实现：

- 动态 ENTER/HOLD/EXIT 与旧信号回测兼容；
- 组合共享现金、目标权重、A 股现实约束；
- 横截面组合回测；
- 事件组合回测 `run_event_drift_portfolio_backtest`；
- 市场/行业残差研究；
- 残差稳健性：train/validation/test、walk-forward、成本压力、参数敏感性、滚动表现、OOS 保存。

口径修正：

- v1.0 **要求代表性残差策略**具备完整稳健性验证；
- **全策略通用稳健性/优化框架延后 v1.1**，不再作为旧策略全部接入的发布门禁。

### 3.5 product / results / api / web

已实现：

- product task 状态机、标准结果包写入、任务与结果 API；
- 经典与横截面产品路径；
- 前端 workflow：策略目录、参数表单、任务创建、状态查询、结果打开、基础 run 比较；
- 分析结果页：净值、回撤、交易、skipped、配置快照等基础展示。

待 07-C 封板：

- 统一发布级 summary（Sharpe/Sortino/Calmar、胜率、盈亏比、MAE/MFE 等）；
- 产品级 benchmark 与 excess；
- 滚动表现进入标准产品页；
- 完整策略/指标/universe/PIT/execution 快照与复现审计；
- 暴露、成本拆分、多 run 比较的稳定消费契约。

## 四、能力矩阵

| 能力 | 数据 | 指标 | 策略 | 回测/研究 | 产品/前端 | 状态 | 备注 |
|---|---|---|---|---|---|---|---|
| 估值、市值、换手、涨跌停、停牌 | 已有 | 已有 | 已用 | 已接入撮合 | 可读 | ✅ | 任务 02 |
| 通用时间序列指标 | 已有 | 参数化已完成 | 经典策略已用 | 动态回测可用 | 目录部分接入 | ✅ | 任务 01 |
| 趋势/动量/突破/均值回归 | 已有 | 已完成 | 已完成 | 动态/组合可用 | 07-A 产品路径 | ✅ | 任务 01/07-A |
| 横截面排序与调仓 | 历史股票池已有 | rank/因子/中性化完成 | CS 策略完成 | 组合回测完成 | 07-B1 产品闭环 | ✅ | 任务 04/07-B1 |
| 多因子 long-only | 财务/行业/指数 as_of 已有 | 正式因子完成 | 已实现 | 研究闭环完成 | 研究完成；产品主推动量 | ✅ | 任务 04 |
| 事件驱动 | `earnings_forecast_event` 已有 | 事件指标完成 | `event_drift_basic` 完成 | 组合回测完成 | 产品入口与标准结果完成 | ✅ | 任务 05 / 07-B2 / 07-C |
| 残差/相对价值研究 | 指数与行业历史已有 | beta/残差完成 | 代表策略完成 | 研究+稳健性完成 | 研究级结果包 | ✅ | 任务 06；非产品主链 |
| 完整配对多腿交易 | 不足 | 缺 | 缺 | 缺 short/多腿 | 缺 | ➡ | 不阻塞 v1.0 |
| 监督学习复杂模型 | 部分特征 | 缺通用训练 | 无 | 缺统一框架 | 缺 | ➡ | 不阻塞 v1.0 |
| 强化学习 / 高频 | 不足 | 无 | 无 | 无 | 无 | ➡ | 排除 |
| 组合资金曲线 | 足够 | n/a | 目标权重完成 | 真实 equity 完成 | 可读 | ✅ | 任务 02/07-A |
| A 股现实成交约束 | 字段已有 | n/a | n/a | 已实现 | 拒单可审计 | ✅ | 任务 02 |
| 成本与换手分析 | 已有 | n/a | n/a | summary 已有 | 可读，归因可加深 | ✅ | 基础完成；07-C 统一 |
| 代表性残差稳健性 | 交易日已有 | n/a | residual 策略 | walk-forward 等完成 | 研究包 | ✅ | 任务 06-B |
| 全策略通用稳健性框架 | n/a | n/a | n/a | 未做成通用产品 | 未做 | ➡ | 延后 v1.1 |
| 策略 API 与编辑器 | n/a | 参数化指标请求 | 内置 + 声明式 | 统一运行器 | 受控编辑器与版本浏览 | ✅ | 07-D 已合并 |
| 结果可复现与发布分析 | 数据指纹 | 指标请求快照 | 策略定义快照 | 标准结果 + replay | 分析页与 compare | ✅ | 07-C 已合并 |

## 五、策略族能力矩阵

| 策略族 | 当前基础 | 缺口 | v1.0 代表策略 |
|---|---|---|---|
| 时间序列趋势/动量 | 指标、策略、组合、产品与标准结果已完成 | 非阻塞增强见 v1.1 | `time_series_momentum`、`dual_sma_trend`、`donchian_breakout` |
| 均值回归 | rolling z-score、状态机与产品结果已完成 | 非阻塞增强见 v1.1 | `rolling_zscore_mean_reversion` |
| QRP 专属系统 | System B 基础策略 | 完整规则系统化非阻塞 | `system_b_basic` |
| 横截面动量 | 研究、产品闭环与 realized exposure 已完成 | 非阻塞增强见 v1.1 | `cross_sectional_momentum_long_only` |
| 多因子 | 研究闭环已完成 | 非当前产品主推路径 | `multifactor_long_only` |
| 事件驱动 | 数据/指标/策略/研究与产品回测已完成 | 非阻塞扩展更多事件源 | `event_drift_basic` |
| 相对价值 | 市场/行业残差与稳健性已完成 | 完整多腿执行 | `market_residual_mean_reversion` |
| 机器学习 | 边界保留 | 复杂内置模型 | 不作为发布阻塞 |
| RL/HFT | 无 | 基础设施不足 | 不进入 v1.0 |

## 六、优先级结论

### 已关闭的 P0 研究/底座项

- 真实组合资金曲线与 A 股约束；
- PIT 财务/行业/指数与事件第一类标准链路；
- 横截面研究到策略；
- 残差研究与代表性稳健性；
- 经典与横截面真实产品主链。

### 当前 P0 产品剩余

- 08 总体验收与发布候选；
- 独立 PR 最终复验与人工发布授权。

### 不作为 v1.0 发布阻塞

- short / 借券 / 保证金 / 多腿配对；
- 协整完整交易、机器学习复杂模型；
- 分布式回测、实盘自动交易；
- 泛化 `corporate_event` 统一主表与更多事件源；
- 全策略通用稳健性优化框架（v1.1）。
