# Episode Segment 统计粒度细化工作包

## 1. 工作包定位

本工作包负责 **System B 行情轮次（Episode）内部走势与收益结构的统计粒度细化（Append-only Episode Refinement）**。

* **核心定位**：纯下游确定性派生（Observation / Research），不改变任何现有状态机、Episode 起止规则、股票池与策略交易规则；
* **架构层级**：位于 `system_b_episode_observation`（每日观察）下游，派生并持久化 `system_b_episode_segment`；
* **核心数学闭包**：
  $$\prod_{k=1}^n (1 + \text{segment\_return}_k) = 1 + \text{episode\_return}$$
* **强一致性 Invariant**：
  $$\text{Count}(\text{segment\_state} = \text{'ACTIVE'}) = \text{ma5\_reentry\_count} + 1$$

---

## 2. 文档清单

1. [`01_System_B_Episode_统计粒度细化设计.md`](01_System_B_Episode_统计粒度细化设计.md)：
   * 完整的架构设计、Anchor 机制、数学闭合证明、测试用例定义与 Implementation Notes。
2. [`02_Implementation_Plan_实施方案.md`](02_Implementation_Plan_实施方案.md)：
   * 开工前工程实施方案、影响面分析与修改清单。
3. [`03_Walkthrough_验收与交付报告.md`](03_Walkthrough_验收与交付报告.md)：
   * 全量测试通过证据链（82 项测试 100% 通过）与交付说明。
4. 相关基础规范与报告追溯：
   * [`../06_SystemB行情轮次规格与运行.md`](../06_SystemB行情轮次规格与运行.md)
   * [`../07_SystemB行情轮次验收报告_20260727.md`](../07_SystemB行情轮次验收报告_20260727.md)

---

## 3. 代码落位一览

* **契约与 Schema**：
  * [`src/qrp_atlas/contracts/system_b.py`](../../../src/qrp_atlas/contracts/system_b.py)
  * [`src/qrp_atlas/contracts/schema.py`](../../../src/qrp_atlas/contracts/schema.py)
  * [`deploy/duckdb/002_system_b_episode.sql`](../../../deploy/duckdb/002_system_b_episode.sql)
* **指标派生算法**：
  * [`src/qrp_atlas/indicators/system_b/segment.py`](../../../src/qrp_atlas/indicators/system_b/segment.py)
* **生产 Pipeline 与级联 Audit**：
  * [`src/qrp_atlas/pipeline/system_b_episode/service.py`](../../../src/qrp_atlas/pipeline/system_b_episode/service.py)
* **自动化测试**：
  * [`tests/indicators/test_system_b_segment.py`](../../../tests/indicators/test_system_b_segment.py)（12 项指标单测）
  * [`tests/pipeline/system_b_episode/test_production.py`](../../../tests/pipeline/system_b_episode/test_production.py)（生产事务与 Audit 检验）
