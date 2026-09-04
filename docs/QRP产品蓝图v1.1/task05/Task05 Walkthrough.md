# Task05 Walkthrough - System B 新增仓授权策略实现

在 `feature/v1.1-task05-authorization` 分支上完成了 Task05 System B market-level 新增仓授权策略实现及 Review 修正。

## 变更概述

1. **Strategy Framework 最小通用扩展** (`strategies/models.py`)
   - 新增 `StrategyInputScope` 枚举（`ASSET`, `MARKET`）；
   - `StrategyDefinition` 增加 `input_scope: StrategyInputScope = StrategyInputScope.ASSET` 字段（向后兼容所有既有策略）并在 `to_dict()` 中稳定序列化；
   - 新增 `@dataclass(frozen=True) StrategyAuthorization` 类型化授权结果，包含 `trade_date`, `strategy_code`, `strategy_version`, `authorization_type`, `is_authorized`, `reason_codes`, `evidence` 及 `to_dict()` 序列化；
   - `StrategyRunResult` 增加 `authorizations: tuple[StrategyAuthorization, ...] = ()`，保持既有 `decisions` 和其它字段不变。

2. **Scope-Aware 校验机制与规范化顺序修正** (`strategies/validation.py`)
   - `validate_definition` 校验 `input_scope` 必须为有效 `StrategyInputScope`；
   - `validate_strategy_input` 严格执行**先规范化 identity，再进行 duplicate check** 的确定性流转：
     - `MARKET`: `trade_date canonicalize (format="mixed") → duplicate(trade_date) → sort`；
       - identity 仅要求 `trade_date`，不要求 `ticker`；
       - 等价日期格式（如 `2024-01-01` 与 `2024/01/01`）先规范化为标准 ISO 日期，并在同日重复检查中被严格拒绝；
       - mergesort 按 `[trade_date]` 稳定排序；
     - `ASSET`: `trade_date canonicalize + ticker canonicalize → duplicate(ticker, trade_date) → sort`；
       - 严格维持原有全部 `(ticker, trade_date)` 校验、等价日期去重与 `[ticker, trade_date]` 排序行为。

3. **Contracts 字段复用（零重复定义）** (`contracts/fields.py`, `contracts/__init__.py`)
   - 统一复用既有 contracts SSOT `V_TRIGGERED`（wire value `"V_triggered"`），不新增 `V_TRIGGERED_LOWER` 重复契约常量，严格保持现有字段 SSOT 纯洁性。

4. **System B Authorization Strategy** (`strategies/builtin/system_b_authorization.py`)
   - 实现 `SystemBAuthorizationStrategy`：
     - `code = "system_b_authorization"`, `version = "1.0.0"`, `input_scope = MARKET`；
     - 输入字段严格要求 `(trade_date, phase, V_triggered)`（通过 `V_TRIGGERED` 契约常量）；
     - 校验 `phase` 必须在 `{"A", "B", "C", "UNRESOLVED"}`；
     - 校验 `V_triggered` 必须为严格布尔类型（拒绝 int 0/1、字符串、None 等）；
     - 优先级逻辑：V 规则触发（`V_triggered=True`）高于阶段 B 授权；
     - 阶段规则及 reason codes：
       - `V_triggered = True` -> `is_authorized = False`, `reason = V_RULE_REVOKED`
       - `phase = "B"` (`V_triggered = False`) -> `is_authorized = True`, `reason = PHASE_B_AUTHORIZED`
       - `phase = "A"` (`V_triggered = False`) -> `is_authorized = False`, `reason = PHASE_A_NOT_AUTHORIZED`
       - `phase = "C"` (`V_triggered = False`) -> `is_authorized = False`, `reason = PHASE_C_NOT_AUTHORIZED`
       - `phase = "UNRESOLVED"` (`V_triggered = False`) -> `is_authorized = False`, `reason = PHASE_UNRESOLVED`
     - Evidence 包含：`market_phase`, `V_TRIGGERED`, `semantic_owner="SYSTEM_B"`, `delivery_mode="BUILTIN"`, `capability_type="STRATEGY"`；
     - 绝对不生成任何 `StrategyDecision` (`decisions=()`)。

5. **Registry & 导出** (`strategies/registry.py`, `strategies/__init__.py`, `strategies/builtin/__init__.py`)
   - 在默认策略注册表中注册 `SystemBAuthorizationStrategy`；
   - 导出 `StrategyInputScope`, `StrategyAuthorization`, `SystemBAuthorizationStrategy`。

6. **测试用例** (`tests/strategies/test_system_b_authorization.py`, `tests/strategies/test_registry.py`)
   - 新增 28 个单元测试，覆盖 MARKET scope 校验、各阶段授权判断、V 规则撤权优先级、规范化前后重复拦截（`2024-01-01` 与 `2024/01/01`）、非法输入 fail-closed、无 decisions 约束、确定性重放及 Registry 发现等；
   - 更新 Registry 固定策略清单断言。

## 验证结果

- **Targeted Tests**:
  ```pwsh
  pytest tests/strategies/test_system_b_authorization.py tests/strategies/test_registry.py tests/strategies/test_system_b_basic.py -v
  ```
  结果：38 passed in 0.18s

- **Strategy Module Tests**:
  ```pwsh
  pytest tests/strategies/ -v
  ```
  结果：163 passed in 5.87s

- **Full Project Tests**:
  ```pwsh
  pytest
  ```
  结果：1375 passed, 3 skipped in 256.59s (0:04:16)，全仓测试 0 回归。

## Commit 记录

- **Base Feature Commit**: `ada9c0c691a149fc850edd1fc7924c0bec7f9540` (`feat(strategies): implement Task05 System B new position authorization strategy`)
- **Review Fix Commit**: `6af921e33d027e8a9f6e6d1933bf9cbf39df5b12` (`fix(strategies): canonicalize identity before duplicate check and reuse V_TRIGGERED`)
