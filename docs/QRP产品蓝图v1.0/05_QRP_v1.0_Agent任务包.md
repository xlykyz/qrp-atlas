# QRP v1.0 Agent 任务包

> 任务 00 状态校准：2026-07-15。以下任务状态以 main 真实代码为准。

## 一、使用规则

1. 一次只做一个任务或一个明确子任务；
2. 不新增顶级模块，不重构已封版架构；
3. 优先复用现有 contracts、indicators、strategies、backtest、results、api、web；
4. 不复制已有事件、残差、横截面或组合回测逻辑；
5. 每项任务补充测试，并运行完整 pytest、compileall、git diff check 和前端 build；
6. 禁止 `--ignore`、`-k`、新增 skip/skipif 绕过失败；
7. 禁止 rebase 和 force push；
8. PR 保持未合并，等待云端验收；
9. 完成后更新能力矩阵与验收清单。

## 二、任务状态总表

| 任务 | 状态 | 说明 |
|---|---|---|
| 00 蓝图状态校准 | ✅ | 文档状态已与 main 对齐 |
| 01 参数化指标与经典策略 | ✅ | 已完成 |
| 02 组合账户与 A 股现实回测 | ✅ | 已完成 |
| 03 PIT 数据与查询 | ✅ | 已完成 |
| 04 横截面与多因子 | ✅ | 04-A～04-E 已完成 |
| 05 事件研究链路 | ✅ | `earnings_forecast_event` + `event_drift_basic` 研究闭环完成 |
| 06 残差与稳健性 | ✅ | 市场/行业残差与代表性稳健性完成 |
| 07-A 真实回测产品主链 | ✅ | 已完成 |
| 07-B1 横截面动量产品闭环 | ✅ | 已完成 |
| 07-B2 事件产品闭环 | ⛔ | **当前剩余** |
| 07-C 标准结果与分析封板 | ⛔ | **当前剩余** |
| 07-D 声明式策略产品 | ⛔ | **当前剩余** |
| 08 总体验收与发布 | ⛔ | 前置 07-B2/C/D |

## 三、已完成任务（摘要，不再重做）

### 任务 01～04

见对应任务文档 07～15。

### 任务 05

- 第一类标准事件：`earnings_forecast_event + EventFrame`；
- 不以泛化 `corporate_event` 为当前唯一验收对象；
- 事件指标、`event_drift_basic`、`run_event_drift_portfolio_backtest` 已完成。

### 任务 06

- 市场/行业残差研究与代表策略；
- walk-forward、成本压力、参数敏感性、滚动表现、OOS 保存；
- **验收口径**：代表性残差策略具备完整稳健性验证；
- **延后 v1.1**：全策略通用稳健性/优化框架。

### 任务 07-A / 07-B1

- 真实 product task 主链；
- 经典策略与 `cross_sectional_momentum_long_only` 产品闭环。

## 四、剩余任务包

### 任务 07-B2：事件驱动真实产品闭环

建议分支：`feature/event-product-loop`

目标路径：

```text
前端选择 event_drift_basic
→ 配置事件过滤和持有期
→ 创建真实回测任务
→ query_earnings_forecast_as_of
→ to_earnings_forecast_event_frame
→ run_event_drift_portfolio_backtest
→ BacktestRunWriter 标准结果包
→ 前端查看 / 刷新重开 / run 对比
```

核心约束：

- 复用现有 product/API/task 边界；
- 保持 `announcement_date → strictly next open available_trade_date → open 入场`；
- 产品层不得重算 `available_trade_date`，不得二次 `next_open`；
- 参数：`hold_days`、`min_profit_change_midpoint`、日期、资金、持仓上限、权重上限、成本；
- 不做新事件源、新闻 NLP、新表、事件参数优化、实盘。

### 任务 07-C：标准结果与分析产品封板

建议分支：`feature/backtest-results-product-completion`

目标：

- 统一经典 / 横截面 / 事件标准产品 run 契约；
- 补齐 summary、benchmark、滚动、暴露、快照、复现与多 run 比较；
- 研究专用 residual robustness artifact 可保留独立格式，但标准产品页不得依赖研究目录。

### 任务 07-D：声明式策略编辑、版本与持久化

建议分支：`feature/declarative-strategy-product`

目标：

- 白名单规则（比较、and/or/not、crossing、连续满足、参数/指标引用、常量）；
- 静态校验、不可变版本、owner 语义；
- 目录合并内置与用户策略；
- 前端受控规则编辑器；
- 严格禁止 eval/exec/任意 Python/任意导入。

### 任务 08：总体验收与发布候选

建议分支：`release/v1.0-acceptance`

前置：07-B2、07-C、07-D 全部合并。

交付：

- 架构、PIT、成交现实约束审计；
- 三条正式演示；
- 全量 pytest / compileall / diff check / web build / lint；
- README、蓝图、release notes；
- PR 未合并，等待人工授权；不打 tag、不宣布发布。

## 五、明确非阻塞项

以下未完成不阻塞 v1.0：

- short / 多腿 / 借券 / 保证金；
- 协整完整交易；
- 机器学习复杂模型；
- 分布式回测；
- 实盘自动交易；
- 泛化 `corporate_event` 统一主表；
- 全策略通用稳健性优化框架。
