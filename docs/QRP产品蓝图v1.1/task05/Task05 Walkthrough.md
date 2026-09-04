# Task05 Walkthrough - System B 新增仓授权策略实现

在 `feature/v1.1-task05-authorization` 分支上完成了 Task05 System B market-level 新增仓授权策略实现。

## 变更概述

1. **Strategy Framework 最小通用扩展** (`strategies/models.py`)
   - 新增 `StrategyInputScope` 枚举（`ASSET`, `MARKET`）；
   - `StrategyDefinition` 增加 `input_scope: StrategyInputScope = StrategyInputScope.ASSET` 字段（向后兼容所有既有策略）并在 `to_dict()` 中稳定序列化；
   - 新增 `@dataclass(frozen=True) StrategyAuthorization` 类型化授权结果，包含 `trade_date`, `strategy_code`, `strategy_version`, `authorization_type`, `is_authorized`, `reason_codes`, `evidence` 及 `to_dict()` 序列化；
   - `StrategyRunResult` 增加 `authorizations: tuple[StrategyAuthorization, ...] = ()`，保持既有 `decisions` 和其它字段不变。

2. **Scope-Aware 校验机制** (`strategies/validation.py`)
   - `validate_definition` 校验 `input_scope` 必须为有效 `StrategyInputScope`；
   - `validate_strategy_input` 根据 `input_scope` 进行针对性处理：
     - `MARKET` 模式下 identity 仅要求 `trade_date`，不要求 `ticker`；
     - `MARKET` 模式下检查并拒绝同一 `trade_date` 重复输入；
     - `MARKET` 模式按 `[trade_date]` mergesort 稳定排序；
     - `ASSET` 模式维持原本全部 `(ticker, trade_date)` 校验与排序行为。

3. **Contracts 字段扩充** (`contracts/fields.py`, `contracts/__init__.py`)
   - 新增 `V_TRIGGERED_LOWER = "v_triggered"` 常量，使得 `known_contract_fields()` 自动纳入 `"v_triggered"`，并加入 `BOOLEAN_FIELDS`。

4. **System B Authorization Strategy** (`strategies/builtin/system_b_authorization.py`)
   - 实现 `SystemBAuthorizationStrategy`：
     - `code = "system_b_authorization"`, `version = "1.0.0"`, `input_scope = MARKET`；
     - 输入字段严格要求 `(trade_date, phase, v_triggered)`；
     - 校验 `phase` 必须在 `{"A", "B", "C", "UNRESOLVED"}`；
     - 校验 `v_triggered` 必须为严格布尔类型（拒绝 int 0/1、字符串、None 等）；
     - 优先级逻辑：V 规则触发（`v_triggered=True`）高于阶段 B 授权；
     - 阶段规则及 reason codes：
       - `v_triggered = True` -> `is_authorized = False`, `reason = V_RULE_REVOKED`
       - `phase = "B"` (`v_triggered = False`) -> `is_authorized = True`, `reason = PHASE_B_AUTHORIZED`
       - `phase = "A"` (`v_triggered = False`) -> `is_authorized = False`, `reason = PHASE_A_NOT_AUTHORIZED`
       - `phase = "C"` (`v_triggered = False`) -> `is_authorized = False`, `reason = PHASE_C_NOT_AUTHORIZED`
       - `phase = "UNRESOLVED"` (`v_triggered = False`) -> `is_authorized = False`, `reason = PHASE_UNRESOLVED`
     - Evidence 包含：`market_phase`, `v_triggered`, `semantic_owner="SYSTEM_B"`, `delivery_mode="BUILTIN"`, `capability_type="STRATEGY"`；
     - 绝对不生成任何 `StrategyDecision` (`decisions=()`)。

5. **Registry & 导出** (`strategies/registry.py`, `strategies/__init__.py`, `strategies/builtin/__init__.py`)
   - 在默认策略注册表中注册 `SystemBAuthorizationStrategy`；
   - 导出 `StrategyInputScope`, `StrategyAuthorization`, `SystemBAuthorizationStrategy`。

6. **测试用例** (`tests/strategies/test_system_b_authorization.py`, `tests/strategies/test_registry.py`)
   - 新增 26 个单元测试，覆盖 MARKET scope 校验、各阶段授权判断、V 规则撤权优先级、非法输入 fail-closed、无 decisions 约束、确定性重放及 Registry 发现等；
   - 更新 Registry 固定策略清单断言。

## 验证结果

- **Targeted Tests**:
  ```pwsh
  pytest tests/strategies/test_system_b_authorization.py tests/strategies/test_registry.py tests/strategies/test_system_b_basic.py -v
  ```
  结果：36 passed in 0.19s

- **Strategy Module Tests**:
  ```pwsh
  pytest tests/strategies/ -v
  ```
  结果：161 passed in 5.26s

- **Full Project Tests**:
  ```pwsh
  pytest
  ```
  结果：1373 passed, 3 skipped in 240.08s (0:04:00)，全仓测试 0 回归。

## Commit 信息

- **Commit SHA**: `ada9c0c691a149fc850edd1fc7924c0bec7f9540`
- **Commit Message**: `feat(strategies): implement Task05 System B new position authorization strategy`
