# Task05 System B 新增仓授权策略设计书 v0.1

## 1. 产品身份

```text
semantic_owner = SYSTEM_B
delivery_mode = BUILTIN
capability_type = STRATEGY
runtime = QRP_COMMON
persistence = QRP_COMMON
```

Task05 是 System B 的业务语义量化实现，不升级为 QRP Core 自身的通用交易规则。它可以作为 QRP bundled built-in strategy 随项目开发、测试和发布；未来外部策略应复用同一套 Strategy Registry / Runtime / Result 基础设施。

## 2. 目标

Task05 只回答：

> 指定交易日，System B 是否允许新增仓？

输出是 strategy-level authorization，不是个股 ENTER/EXIT，不接管已有持仓，不生成目标仓位。

```text
prepared judgment context
→ System B authorization strategy
→ new_position_authorized
→ reason_codes
```

## 3. 已冻结业务语义

来源：`xlykyz/MyTradingSystem` 当前锁定规则基线。

- 阶段 A：不允许新增仓；
- 阶段 B：允许新增仓；
- 阶段 C：不允许新增仓；
- V 规则触发：撤销新增仓授权；
- 授权变化不影响已有持仓；
- 授权变化不触发平仓；
- 单一交易日内授权结果冻结；
- 主线判断不得等同于 M1—M6 任一单项。

## 4. 未冻结语义与 fail-closed 边界

当前规则尚未给出可直接编码的、完整且唯一的 `M1—M6 / 节点 / 周期 / 题材结构 → A/B/C` 自动分类公式。

因此 Task05 v0.1 **不得**：

- 自行发明 M1—M6 权重；
- 用 M4/M5/M6 任一单项直接确认阶段 B；
- 暗置经验阈值；
- 通过空值、默认值或推断自动补出主线判断；
- 为完成 Task05 而提前实现 Task06 评分逻辑。

Task05 v0.1 接收已经解析好的 `market_phase` 作为 prepared strategy input。若阶段无法解析，应显式为 `UNRESOLVED`，并 fail-closed：`new_position_authorized = false`。

后续若 MyTradingSystem 冻结自动阶段分类规则，可以新增/替换 System B judgment component；Task05 的 authorization contract 不需要变化。

## 5. QRP Core 最小扩展

现有 Strategy Framework 默认只支持 asset-level 输入：`validate_strategy_input()` 强制 `(ticker, trade_date)`，`StrategyRunResult` 也只有 asset-level `StrategyDecision`。

Task05 需要补两个通用、薄且向后兼容的能力。

### 5.1 Strategy input scope

新增：

```text
StrategyInputScope.ASSET   # 默认，保持现有行为
StrategyInputScope.MARKET  # 一行一个 trade_date，不要求 ticker
```

`StrategyDefinition.input_scope` 默认 `ASSET`。

校验规则：

- `ASSET`：identity key = `(ticker, trade_date)`；
- `MARKET`：identity key = `(trade_date)`；
- 两类都必须确定性排序；
- 不复制第二套 Strategy Registry / Runtime。

### 5.2 typed authorization result

新增通用：

```text
StrategyAuthorization
```

建议字段：

```text
trade_date
strategy_code
strategy_version
authorization_type
is_authorized
reason_codes
evidence
```

`authorization_type` 当前使用：

```text
NEW_POSITION
```

`StrategyRunResult` 新增：

```text
authorizations: tuple[StrategyAuthorization, ...] = ()
```

保持既有 `decisions` 字段和所有现有策略行为不变。

禁止使用虚拟 ticker（如 `MARKET` / `__SYSTEM__`）伪装 market-level 结果。

## 6. System B v0.1 实现

新增 built-in strategy：

```text
code = system_b_authorization
version = 1.0.0
input_scope = MARKET
```

prepared input 每个交易日至少包含：

```text
trade_date
phase
v_triggered
```

其中 System B phase 允许：

```text
A
B
C
UNRESOLVED
```

确定性规则：

```text
v_triggered = true
→ authorized = false
→ reason = V_RULE_REVOKED

phase = B and v_triggered = false
→ authorized = true
→ reason = PHASE_B_AUTHORIZED

phase = A
→ authorized = false
→ reason = PHASE_A_NOT_AUTHORIZED

phase = C
→ authorized = false
→ reason = PHASE_C_NOT_AUTHORIZED

phase = UNRESOLVED
→ authorized = false
→ reason = PHASE_UNRESOLVED
```

若输入 phase 非法、V 非布尔、trade_date 重复或缺失，显式报 `StrategyValidationError`，不得静默修复。

## 7. evidence

每条 authorization 至少保存：

```text
market_phase
v_triggered
semantic_owner = SYSTEM_B
delivery_mode = BUILTIN
```

规则版本由稳定的 strategy code/version 表达；输入快照、rule_version_set、parameter_set 等运行级审计上下文继续由 QRP common runtime/result 层承载，不为 Task05 单独建设 repository 或数据库表。

## 8. 明确不做

Task05 不实现：

- A/B/C 自动分类公式；
- M1—M6 composite score；
- eligibility / hard veto；
- score / rank / M1—M3 result；
- ENTER / HOLD / EXIT；
- portfolio target；
- 1/8 仓位与 6 只上限；
- 持仓退出；
- execution / order plan；
- Task05 专用 pipeline；
- Task05 专用数据库 repository。

旧 `system_b_basic` 仅是早期 strategy/backtest boundary 验证策略。本任务不得把其 `trend_valid → ENTER` 行为当成 System B 当前正式交易语义；除非存在明确兼容性问题，不在 Task05 顺手重写或删除它。

## 9. 测试与退出条件

至少覆盖：

1. MARKET scope 不要求 ticker，ASSET scope 行为完全不变；
2. MARKET scope 同日重复输入拒绝；
3. A/B/C/UNRESOLVED 四阶段授权结果；
4. V 规则对 B 阶段撤权；
5. 授权结果不产生任何 ENTER/HOLD/EXIT decision；
6. 空输入返回空 authorization；
7. 非法 phase / 非布尔 V / 非法日期 fail-closed；
8. 相同输入 + strategy version 的 `to_dict()` 完全确定；
9. 默认 Registry 可发现 `system_b_authorization@1.0.0`；
10. 现有 strategy 全量回归零破坏。

Task05 完成定义：

```text
System B market-level authorization
可通过 QRP 标准 Strategy Registry / Runtime 执行
+ typed result 可序列化、解释、确定性重放
+ 未冻结的主线分类规则没有被 Agent 推断进入生产
```
