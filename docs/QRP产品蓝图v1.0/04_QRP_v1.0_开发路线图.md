# QRP v1.0 开发路线图

> 任务 08 发布验收校准：2026-07-15，对照 main `69f31e41daebcb04d75af9ec71191ddf06e60ebb`。

## 一、路线原则

1. 架构不再重构，只补能力；
2. 数据的 point-in-time 正确性优先于策略数量；
3. 先单标的、再组合；
4. 先 long-only A 股可交易能力，再考虑 short 和多腿；
5. 每个阶段必须形成端到端测试；
6. API 和前端在稳定接口形成后并行推进；
7. 每完成一阶段同步更新能力矩阵；
8. 不把 short、多腿、借券、协整、机器学习、分布式回测和实盘列为 v1.0 发布阻塞。

## 二、阶段总览

| 阶段 | 目标 | 状态 | 主要模块 |
|---|---|---|---|
| 0 | 架构与产品蓝图封版 / 状态校准 | ✅ 任务 00 校准中 | docs |
| 1 | 参数化指标与经典单标的策略 | ✅ 已完成 | indicators / strategies / backtest |
| 2 | 组合账户、A 股约束与真实资金曲线 | ✅ 已完成 | backtest / results |
| 3 | point-in-time 数据底座与查询服务 | ✅ 已完成 | contracts / pipeline / data / backtest |
| 4 | 横截面与多因子 | ✅ 已完成 | indicators / strategies / backtest / results |
| 5 | 事件驱动研究链路 | ✅ 已完成 | contracts / pipeline / indicators / strategies / backtest |
| 6 | 残差相对价值与代表性稳健性验证 | ✅ 已完成 | indicators / strategies / backtest / results |
| 7-A | 真实回测产品主链 | ✅ 已完成 | api / web / backtest/product |
| 7-B1 | 横截面动量产品闭环 | ✅ 已完成 | api / web / backtest/product |
| 7-B2 | 事件驱动真实产品闭环 | ✅ 已完成 | api / web / backtest/product |
| 7-C | 标准结果与分析产品封板 | ✅ 已完成 | results / api / web |
| 7-D | 声明式策略产品化 | ✅ 已完成 | strategies / api / web |
| 8 | v1.0 总体验收与发布候选 | 🔄 当前执行 | 全部 |

## 三、已完成阶段摘要

### 阶段 1～2

- 参数化指标与经典策略；
- 组合共享资金、A 股现实成交、真实净值。

### 阶段 3

- 财务/行业/指数 PIT 数据与 as_of 查询完成；
- 结构化事件不由阶段 3 承担。

### 阶段 4

- 04-A～04-E 全部完成：算子、因子、中性化、策略、IC/分组/暴露研究闭环。

### 阶段 5（研究完成，产品入口已由 07-B2 封板）

交付事实：

- 第一类标准事件：`earnings_forecast_event`（**不是** 以泛化 `corporate_event` 为唯一验收对象）；
- `query_earnings_forecast_as_of` / `to_earnings_forecast_event_frame`；
- 事件指标；
- `event_drift_basic`；
- `run_event_drift_portfolio_backtest`。

退出条件（研究侧）已满足：

- 盘后公告不能同日成交；
- 修订可追溯且不污染历史 as_of；
- 端到端研究回测通过。

产品化路径已由 07-B2 完成并合入 main。

### 阶段 6

交付事实：

- 市场残差 / 行业残差研究；
- `market_residual_mean_reversion`；
- train/validation/test、walk-forward、成本压力、参数敏感性、滚动表现、OOS 保存。

口径修正：

- v1.0 要求**代表性残差策略**具备完整稳健性验证；
- “旧策略均可使用通用稳健性框架”调整为 **v1.1** 目标，不阻塞发布。

## 四、阶段 7-B2～7-D 完成事实与阶段 8

### 阶段 7-B2：事件驱动真实产品闭环

目标：

```text
前端选择 event_drift_basic
→ 配置事件过滤和持有期
→ 创建真实回测任务
→ PIT 读取 earnings_forecast_event
→ EventFrame
→ run_event_drift_portfolio_backtest
→ 标准产品结果包
→ 前端查看、刷新重开、run 对比
```

必须复用既有查询、策略、runner、writer 与任务状态机；不得二次计算 `available_trade_date`，不得二次 `next_open` 偏移。

### 阶段 7-C：标准结果与分析产品封板

目标：

- 统一经典 / 横截面 / 事件标准产品 run 的结果契约；
- 补齐 summary、benchmark、滚动表现、暴露、快照与复现审计；
- API 与前端分析页只消费标准产品结果，不依赖研究目录。

### 阶段 7-D：声明式策略产品化

目标：

- 白名单规则编辑、静态校验、版本化保存；
- 与内置策略共享 `StrategyDecision` 与产品回测链；
- 禁止 eval/exec/任意代码。

### 阶段 8：总体验收与发布候选

前置：07-B2、07-C、07-D 全部合并。

交付：

- 架构/PIT/成交约束审计；
- 四条正式演示（经典 / 横截面 / 事件 / 声明式）；
- 全量测试与构建；
- 文档、release notes 与未合并 PR 等待人工授权。

未经明确授权：不打 tag、不创建 GitHub Release、不宣布正式发布、不进入 v1.1 开发。

## 五、并行与边界

- Windows 端经典因子、估值与交易活跃度因子扩充任务不得被本路线图干扰；
- 不新增顶级模块；
- 不复制已有事件、残差、横截面或组合回测逻辑；
- 研究增强项不得回写成发布门禁。
