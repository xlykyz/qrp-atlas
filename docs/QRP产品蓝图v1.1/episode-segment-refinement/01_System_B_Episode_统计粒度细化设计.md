# System B Episode 统计粒度细化设计

**文档状态**：Design v1.0  
**目标版本**：QRP-ATLAS v1.1  
**适用模块**：System B / Episode / Indicators  
**设计性质**：统计粒度增强，不涉及交易规则变更  
**核心原则**：Append-only downstream derivation

---

## 1. 背景

当前 System B 已经形成三级事实链路中的前两级：

```text
System B State Observation
        ↓
System B Episode
        ↓
System B Episode Observation
```

其中：

- `system_b_state_observation` 记录逐交易日的 BASE / CANDIDATE / ACTIVE 状态；
- `system_b_episode` 定义完整的 System B 行情轮次；
- `system_b_episode_observation` 记录 Episode 存续期间逐交易日的状态、收益、峰值和回撤等事实。

当前 Episode 的统计粒度仍然偏粗。

一个完整 Episode 内部实际上可能经历：

```text
ACTIVE
→ NON_ACTIVE
→ ACTIVE
→ NON_ACTIVE
→ ACTIVE
→ ...
→ Episode End
```

即一个 MA10 级别的大行情轮次中，可能存在多轮基于 MA5 状态形成的上涨冲刺。

当前 `ma5_reentry_count` 已经能够识别后续 `CANDIDATE -> ACTIVE` 的重新进入次数，但这些 ACTIVE 阶段尚未被正式建模为独立统计实体。

因此需要在 Episode 与 Episode Observation 之间增加一层：

> **Episode Segment**

用于无损描述一个 Episode 内部由多个连续 ACTIVE / NON_ACTIVE 区间组成的路径。

---

# 2. 本次升级目标

本次升级解决的是：

> **System B Episode 内部收益和走势结构的进一步拆解。**

核心希望回答：

1. 一个 Episode 内有多少轮 ACTIVE 冲刺；
2. 每轮 ACTIVE 持续多久；
3. 每轮 ACTIVE 获得多少收益；
4. Episode 总收益主要来自 ACTIVE 还是 NON_ACTIVE；
5. 第 1、2、3……轮 ACTIVE 的收益质量是否存在显著差异；
6. ACTIVE 之间的 NON_ACTIVE 阶段产生多少回撤或收益侵蚀；
7. 一个完整 Episode 的收益能否被 Segment 完整、无损地复原。

本次升级**不改变 System B 的任何状态判断、Episode 判断和交易规则**。

---

# 3. 设计边界

## 3.1 核心约束

本次升级必须满足：

> **完全基于现有 Episode 产物进行下游派生。**

依赖关系固定为：

```text
system_b_state_observation
        ↓
calculate_system_b_episodes()
        ↓
system_b_episode
system_b_episode_observation
        ↓
calculate_system_b_episode_segments()
        ↓
system_b_episode_segment
```

新增 Segment 模块不得重新读取：

- 原始行情；
- MA5；
- MA10；
- System B 状态机输入；
- 股票池；
- Strategy；
- Portfolio。

Segment 不拥有任何独立判断 System B 状态的能力。

---

## 3.2 明确禁止修改的业务语义

本次升级不得改变：

```text
BASE / CANDIDATE / ACTIVE 定义
CANDIDATE -> ACTIVE 确认逻辑
Episode start
Episode confirmed
Episode end
Episode return
Peak return
Drawdown from peak
MA5 reentry count
System B pool
Strategy
Portfolio
```

尤其不得修改：

```text
episode_start_date
episode_confirmed_date
episode_end_date
episode_id
episode_no
ma5_reentry_count
```

现有：

```text
system_b_episode
system_b_episode_observation
```

在升级前后必须保持业务结果完全一致。

---

# 4. Episode 现有收益口径

当前 Episode 在：

```text
CANDIDATE -> ACTIVE
```

发生时被确认。

例如：

```text
T0  CANDIDATE
T1  ACTIVE       ← confirmed
T2  ACTIVE
T3  ACTIVE
...
TN  Episode End
```

Episode 的起点是：

```text
episode_start_date = T0
start_close = T0 close
```

确认点为：

```text
episode_confirmed_date = T1
```

因此 Episode 收益：

```text
episode_return(t)
=
close(t) / episode_start_close - 1
```

完整 Episode 最终收益：

```text
episode_return
=
episode_end_close / episode_start_close - 1
```

因此：

> `T0 -> T1` 即进入 ACTIVE 当天产生的涨跌，属于 Episode 收益的一部分。

这是 Segment 收益拆解必须严格继承的收益边界。

---

# 5. 新统计层级

升级后的结构：

```text
L0  system_b_state_observation
        │
        │ 每日 System B 原子状态
        ↓
L1  system_b_episode
        │
        │ MA10 级行情轮次
        ├────────────────────┐
        ↓                    ↓
L1-Daily                L2
episode_observation     episode_segment
                             │
                             ├── ACTIVE
                             │      └── ACTIVE Sprint
                             │
                             └── NON_ACTIVE
```

其中：

### Episode

继续表示完整的大行情轮次。

### Episode Observation

继续作为 Episode 内的逐交易日事实 SSOT。

### Episode Segment

新增。

表示 Episode 内：

> **连续具有相同 ACTIVE / NON_ACTIVE 分类的交易日区间。**

### ACTIVE Sprint

不是另一套基础表。

定义为：

```text
segment_state = ACTIVE
```

的 Episode Segment。

因此：

> **Sprint 是 ACTIVE Segment 的业务/研究名称，而 Segment 才是正式统计实体。**

---

# 6. 为什么采用 Segment，而不是只建立 Sprint

如果只建立：

```text
system_b_episode_sprint
```

则只能看到：

```text
Sprint 1
Sprint 2
Sprint 3
```

中间的 NON_ACTIVE 区间会消失。

例如：

```text
ACTIVE
ACTIVE
CANDIDATE
BASE
CANDIDATE
ACTIVE
ACTIVE
```

只记录 Sprint 会变成：

```text
Sprint 1
Sprint 2
```

但无法直接知道：

- 两轮冲刺相隔多久；
- 中间跌了多少；
- 是否发生深度回撤；
- 下一轮 ACTIVE 是快速恢复还是长期整理；
- Episode 利润有多少被 NON_ACTIVE 阶段侵蚀。

采用 Segment 后：

```text
SEG_001 ACTIVE
SEG_002 NON_ACTIVE
SEG_003 ACTIVE
```

整个 Episode 路径被完整保存。

因此 canonical 数据层采用：

```text
system_b_episode_segment
```

而：

```text
ACTIVE Sprint
```

作为 `ACTIVE Segment` 的语义别名。

---

# 7. Segment 划分规则

## 7.1 状态标准化

Segment 不重新解释状态。

直接使用：

```text
system_b_episode_observation.trend_state
```

并归一化：

```text
ACTIVE
    → ACTIVE

BASE
CANDIDATE
    → NON_ACTIVE
```

即：

```python
segment_state = (
    "ACTIVE"
    if trend_state == "ACTIVE"
    else "NON_ACTIVE"
)
```

---

## 7.2 分段条件

对于同一个 `episode_id`：

按：

```text
trade_date ASC
```

排序。

当：

```text
segment_state(t)
!=
segment_state(t-1)
```

时开启新的 Segment。

因此：

```text
ACTIVE
ACTIVE
ACTIVE
NON_ACTIVE
NON_ACTIVE
ACTIVE
ACTIVE
```

产生：

```text
SEG_001 ACTIVE
SEG_002 NON_ACTIVE
SEG_003 ACTIVE
```

---

## 7.3 不按 BASE / CANDIDATE 再拆分

第一阶段不将：

```text
BASE
CANDIDATE
```

进一步拆成不同 Segment。

例如：

```text
CANDIDATE
CANDIDATE
BASE
BASE
CANDIDATE
```

统一视为：

```text
NON_ACTIVE
```

一个 Segment。

原因是本阶段研究问题首先是：

> **Episode 收益究竟发生在 ACTIVE 还是 NON_ACTIVE。**

BASE / CANDIDATE 的细粒度信息仍完整保存在：

```text
system_b_episode_observation
```

因此没有信息损失。

未来如有研究需要，可以继续从 observation 派生。

---

# 8. Segment ID

使用确定性 ID：

```text
{episode_id}_SEG_{segment_no:03d}
```

例如：

```text
603221.SH_EP_0102_SEG_001
603221.SH_EP_0102_SEG_002
603221.SH_EP_0102_SEG_003
```

不建议直接使用：

```text
..._S01
..._S02
```

因为并非所有 Segment 都是 Sprint。

Sprint 序号独立记录：

```text
active_sprint_no
```

例如：

| segment_id | segment_state | active_sprint_no |
|---|---|---:|
| SEG_001 | ACTIVE | 1 |
| SEG_002 | NON_ACTIVE | NULL |
| SEG_003 | ACTIVE | 2 |
| SEG_004 | NON_ACTIVE | NULL |
| SEG_005 | ACTIVE | 3 |

---

# 9. Segment 数据模型

新增：

```text
system_b_episode_segment
```

建议字段如下。

## 9.1 Identity

```text
segment_id
episode_id
asset_id
segment_no
segment_state
active_sprint_no
```

### segment_no

Episode 内从 1 开始：

```text
1, 2, 3, ...
```

### segment_state

枚举：

```text
ACTIVE
NON_ACTIVE
```

### active_sprint_no

仅 ACTIVE Segment 非空：

```text
1
2
3
...
```

NON_ACTIVE：

```text
NULL
```

---

## 9.2 时间边界

```text
anchor_date
start_date
end_date
trading_days
```

### start_date

Segment 第一条 Episode Observation 日期。

### end_date

Segment 最后一条 Episode Observation 日期。

### trading_days

Segment 包含的 Episode Observation 数量。

注意：

> trading_days 是实际交易观察数量，而不是自然日数量。

---

# 10. 收益边界设计

这是本设计最关键的部分。

Segment 不能简单定义为：

```text
end_close / start_close - 1
```

否则每个 Segment 第一天的收益都会被遗漏。

---

## 10.1 Destination-day attribution

每日 close-to-close 收益归属于：

> **收益终点当天所属状态。**

例如：

```text
T0 NON_ACTIVE close = 10
T1 ACTIVE     close = 11
T2 ACTIVE     close = 12
```

T0 → T1 的 +10%：

属于 T1 的 ACTIVE。

因此 ACTIVE Segment 收益必须从：

```text
T0 close
```

开始计算，而不是从 T1 close 开始。

---

# 11. Anchor 定义

每个 Segment 增加：

```text
anchor_date
anchor_close
```

Segment 收益定义为：

```text
segment_return
=
end_close / anchor_close - 1
```

---

## 11.1 后续 Segment

对于 Segment 2 及以后：

```text
anchor_date
=
上一 Segment 的 end_date

anchor_close
=
上一 Segment 的 end_close
```

因此收益边界连续。

---

## 11.2 第一个 Segment

第一个 Segment 是特殊情况。

当前 Episode 实现：

```text
episode_start_date = confirmed 前一个实际交易日
```

但 `system_b_episode_observation` 的第一条记录已经是：

```text
episode_confirmed_date
```

即 ACTIVE 当天。

同时当前 `system_b_episode` 并没有持久化：

```text
episode_start_close
```

因此不能简单通过 observation 内：

```text
lag(close)
```

获得 Episode 起点价格。

---

# 12. Episode Start Close 的恢复

为了坚持：

> **Segment 完全基于 Episode，不重新读取 State Observation 或行情。**

第一个 Segment 的 anchor 直接从现有 Episode Observation 精确恢复。

首条 observation 已有：

```text
close
episode_return
```

而：

```text
episode_return
=
close / episode_start_close - 1
```

因此：

```text
episode_start_close
=
close / (1 + episode_return)
```

所以第一个 Segment：

```text
anchor_date
=
episode_start_date

anchor_close
=
first_observation.close
/
(1 + first_observation.episode_return)
```

这样不需要：

- 修改现有 Episode contract；
- 给 Episode 新增 start_close；
- 回读 state observation；
- 回读行情；
- 重新计算任何指标。

从而保持真正的：

> **Episode-only downstream derivation**

---

# 13. Segment 收益闭合

完整 Episode：

```text
anchor
 ↓
SEG_001
 ↓
SEG_002
 ↓
SEG_003
 ↓
...
 ↓
latest / episode_end
```

定义：

```text
R1 = SEG_001 return
R2 = SEG_002 return
...
Rn = SEG_n return
```

必须满足：

```text
(1 + R1)
× (1 + R2)
× ...
× (1 + Rn)

=

1 + Episode Return
```

即：

```text
Π(1 + segment_return)
=
1 + episode_return
```

这是本次设计最重要的数学 invariant。

对于：

### 已结束 Episode

右侧采用最终 Episode Observation：

```text
episode_end_date
```

对应的 `episode_return`。

### 尚未结束 Episode

采用最新 Episode Observation 的：

```text
episode_return
```

同样必须闭合。

---

# 14. Segment 价格与风险指标

建议第一版记录：

```text
anchor_close
start_close
end_close
segment_return
peak_close
peak_date
peak_return
max_drawdown
```

---

## 14.1 start_close

Segment 第一条 observation close。

注意：

```text
start_close != anchor_close
```

通常成立。

---

## 14.2 peak_return

相对 anchor：

```text
peak_return
=
peak_close / anchor_close - 1
```

---

## 14.3 max_drawdown

在 Segment 路径内部计算 running peak。

价格路径包括：

```text
anchor_close
+
Segment 内所有 close
```

定义：

```text
drawdown(t)
=
close(t) / running_peak(t) - 1
```

Segment：

```text
max_drawdown
=
min(drawdown)
```

---

# 15. Open Segment

对于未结束 Episode 的最后一个 Segment：

```text
is_open = true
```

其他：

```text
is_open = false
```

Episode 已结束时：

所有 Segment：

```text
is_open = false
```

这样可以明确区分：

```text
已经完成的 Sprint
```

与：

```text
当前仍在运行的 Sprint
```

---

# 16. ACTIVE Sprint

ACTIVE Sprint 无需单独建立基础表。

定义：

```sql
SELECT *
FROM system_b_episode_segment
WHERE segment_state = 'ACTIVE'
```

即可获得。

每个 ACTIVE Segment：

```text
active_sprint_no = 1, 2, 3...
```

因此可以直接研究：

```text
Sprint 1
Sprint 2
Sprint 3
Sprint 4+
```

---

# 17. 与 ma5_reentry_count 的关系

现有 Episode：

第一次：

```text
CANDIDATE -> ACTIVE
```

创建 Episode。

之后再次：

```text
CANDIDATE -> ACTIVE
```

增加：

```text
ma5_reentry_count += 1
```

因此在正常合法 Episode 中：

```text
ACTIVE Segment Count
=
ma5_reentry_count + 1
```

例如：

```text
ACTIVE
NON_ACTIVE
ACTIVE
NON_ACTIVE
ACTIVE
```

存在：

```text
3 个 ACTIVE Segment
```

对应：

```text
ma5_reentry_count = 2
```

这可以成为 Segment audit 的强 invariant。

若二者不一致，应视为：

```text
Segment derivation / Episode state path invariant violation
```

而不是自动修复。

---

# 18. 推荐 Schema

第一版建议：

```text
segment_id
episode_id
asset_id

segment_no
segment_state
active_sprint_no

anchor_date
start_date
end_date
trading_days

anchor_close
start_close
end_close

segment_return
peak_close
peak_date
peak_return
max_drawdown

is_open

source_episode_rule_version
segment_version

created_run_id
created_at
```

其中：

```text
segment_version
=
system_b_episode_segment@1.0.0
```

用于独立记录 Segment 派生算法版本。

Episode 的原始 rule version 单独保留为：

```text
source_episode_rule_version
```

这样未来 Segment 统计算法升级不会伪装成 System B 业务规则升级。

---

# 19. 第一阶段不增加 Episode 汇总字段

初版不建议直接给：

```text
system_b_episode
```

追加：

```text
active_sprint_count
active_return
non_active_return
...
```

原因：

1. 避免污染当前稳定 Episode contract；
2. 保持升级 append-only；
3. Segment 本身已经包含全部信息；
4. 聚合指标仍处于研究阶段。

第一阶段通过 SQL/View 派生即可。

例如：

```text
system_b_episode_segment_summary
```

可作为后续 view，而不是立即修改 canonical Episode 表。

---

# 20. Episode 级可派生统计

基于 Segment 可以得到：

```text
active_sprint_count

active_days
non_active_days
active_day_ratio

active_compound_return
non_active_compound_return

first_sprint_return
last_sprint_return

max_sprint_return
min_sprint_return
avg_sprint_return
median_sprint_return

avg_sprint_days
max_sprint_days

max_non_active_gap_days
max_non_active_drawdown
```

---

# 21. ACTIVE / NON_ACTIVE 收益拆分

需要注意：

普通百分比收益不是可加的。

不能直接认为：

```text
episode_return
=
active_return
+
non_active_return
```

正确关系是：

```text
1 + episode_return
=
(1 + active_compound_return)
×
(1 + non_active_compound_return)
```

其中：

```text
active_compound_return
=
所有 ACTIVE 日收益因子的乘积 - 1
```

```text
non_active_compound_return
=
所有 NON_ACTIVE 日收益因子的乘积 - 1
```

因此：

```text
active_return / episode_return
```

只能作为描述性指标，不能解释为严格的“收益贡献率”。

如果未来需要严格可加的收益贡献分析，可使用：

```text
log return
```

因为：

```text
log(1 + episode_return)

=

log(1 + active_return)
+
log(1 + non_active_return)
```

但这属于研究层，不进入本次 canonical Segment v1。

---

# 22. Raw Segment 不进行人工合并

第一版严格按真实状态变化分段。

例如：

```text
ACTIVE
ACTIVE
NON_ACTIVE
ACTIVE
ACTIVE
```

必须得到：

```text
SEG_001 ACTIVE
SEG_002 NON_ACTIVE
SEG_003 ACTIVE
```

即使 NON_ACTIVE 只有一天，也不自动合并为一轮 Sprint。

不得直接加入类似：

```text
NON_ACTIVE <= 1 天则合并
NON_ACTIVE <= 2 天则合并
回撤低于 3% 则合并
```

因为这些都已经属于：

> **研究假设，而非原始统计事实。**

未来如果研究证明有价值，可以额外建立：

```text
merged_sprint
```

并显式版本化其 merge rule。

Canonical Segment 永远保留原始状态路径。

---

# 23. 计算接口

新增：

```text
src/qrp_atlas/indicators/system_b/segment.py
```

建议接口：

```python
@dataclass(frozen=True)
class SystemBEpisodeSegmentResult:
    segments: pd.DataFrame


def calculate_system_b_episode_segments(
    episodes: pd.DataFrame,
    observations: pd.DataFrame,
) -> SystemBEpisodeSegmentResult:
    ...
```

输入严格限定：

```text
system_b_episode
system_b_episode_observation
```

不得接受：

```text
raw market data
system_b_state_observation
ma5 input
ma10 input
```

这样通过函数接口本身限制依赖方向。

---

# 24. 生产 Pipeline

现有：

```text
state observation
↓
calculate_system_b_episodes
↓
write episode
↓
write episode observation
↓
audit
↓
commit
```

扩展为：

```text
state observation
↓
calculate_system_b_episodes
↓
calculate_system_b_episode_segments
↓
write episode
↓
write episode observation
↓
write episode segment
↓
audit episode
↓
audit segment
↓
commit
```

仍然处于现有同一事务内。

---

# 25. Transaction 原则

Segment 必须与 Episode 一起：

```text
BEGIN
...
AUDIT
...
COMMIT
```

任何 Segment invariant 失败：

```text
ROLLBACK
```

不得出现：

```text
episode 已更新
segment 更新失败
```

这种半完成状态。

---

# 26. Replacement 顺序

现有 production 使用：

```text
transactional rule-version replacement
```

新增后继续保持。

建议删除顺序：

```text
DELETE segment
DELETE episode_observation
DELETE episode
```

写入顺序：

```text
INSERT episode
INSERT episode_observation
INSERT segment
```

最后统一 audit。

---

# 27. 核心 Invariants

## 27.1 Legacy Zero-Change

同一输入：

升级前后：

```text
system_b_episode
```

必须完全一致。

升级前后：

```text
system_b_episode_observation
```

必须完全一致。

Segment 升级不得改变任何已有业务字段。

---

## 27.2 Segment Coverage

每一条：

```text
system_b_episode_observation
```

必须且只能属于一个 Segment。

---

## 27.3 Segment Date Range

同一 Episode：

```text
Segment date range
```

不得重叠。

---

## 27.4 Segment Number

必须：

```text
1,2,3,...N
```

连续无缺口。

---

## 27.5 Adjacent State

相邻 Segment 必须：

```text
segment_state不同
```

不得存在：

```text
ACTIVE
ACTIVE
```

两个连续 Segment。

---

## 27.6 Trading Day Coverage

必须：

```text
Σ segment.trading_days
=
Episode Observation Row Count
```

---

## 27.7 Boundary Coverage

必须：

```text
first_segment.start_date
=
first_episode_observation.trade_date
```

```text
last_segment.end_date
=
last_episode_observation.trade_date
```

---

## 27.8 Episode Anchor

第一个 Segment：

```text
anchor_date
=
episode_start_date
```

---

## 27.9 Return Closure

必须在数值 tolerance 内：

```text
Π(1 + segment_return)
=
1 + latest_episode_return
```

建议 tolerance：

```text
1e-10
```

或依据当前 float64 体系统一定义。

---

## 27.10 ACTIVE Sprint Count

在合法 Episode 状态路径下：

```text
count(segment_state='ACTIVE')
=
ma5_reentry_count + 1
```

---

## 27.11 Determinism

对同一输入重复执行：

```text
segment_id
segment_no
segment_state
日期
统计值
```

必须完全稳定。

---

## 27.12 No Orphan Segment

任何 Segment 必须能够匹配：

```text
system_b_episode.episode_id
```

---

# 28. 测试场景

至少覆盖以下 case。

### Case 1：单轮 ACTIVE

```text
ACTIVE ACTIVE ACTIVE ...
```

验证：

```text
active_sprint_no = 1
```

---

### Case 2：一次 reentry

```text
ACTIVE
NON_ACTIVE
ACTIVE
```

验证：

```text
ACTIVE segment count = 2
ma5_reentry_count = 1
```

---

### Case 3：多次 reentry

```text
ACTIVE
NON_ACTIVE
ACTIVE
NON_ACTIVE
ACTIVE
```

验证 Sprint 1/2/3。

---

### Case 4：长时间 NON_ACTIVE 但 Episode 未结束

验证 NON_ACTIVE Segment 不会错误终止 Episode。

Segment 不拥有 Episode end 判断权。

---

### Case 5：Episode End

验证最后的 NON_ACTIVE Segment 正确包含：

```text
is_episode_end = true
```

对应交易日。

---

### Case 6：Open Episode

验证：

```text
last_segment.is_open = true
```

并使用最新 observation 收益做 return closure。

---

### Case 7：首个 ACTIVE 日大涨

例如：

```text
T0 = 10
T1 ACTIVE = 11
```

验证 Segment 1 收益包含：

```text
10 → 11
```

而不是从 11 开始。

---

### Case 8：共享 Episode Boundary

当前系统允许某些：

```text
new episode_start_date
=
previous episode_end_date
```

场景。

Segment 必须以 `episode_id` 为严格隔离边界，不得跨 Episode 合并。

---

### Case 9：交易日间存在自然日 Gap

周末、节假日不得被误算为 Segment gap trading days。

---

### Case 10：重复运行

同一数据重复 rebuild：

输出完全一致。

---

# 29. 回归测试

本次升级最重要的回归测试不是 Segment 本身，而是：

> **证明旧系统没有变化。**

建议增加 golden/snapshot regression：

```text
Before Upgrade:
episodes_before
observations_before

After Upgrade:
episodes_after
observations_after
```

验证：

```text
episodes_before == episodes_after

observations_before == observations_after
```

允许忽略：

```text
created_run_id
created_at
```

等运行元数据。

所有业务字段必须一致。

---

# 30. 代码变更范围

建议只涉及：

```text
src/qrp_atlas/contracts/system_b.py
src/qrp_atlas/contracts/schema.py

src/qrp_atlas/indicators/system_b/segment.py

src/qrp_atlas/indicators/system_b/__init__.py

src/qrp_atlas/pipeline/system_b_episode/service.py

tests/...
```

其中：

### 新增

```text
segment.py
segment contract
segment schema
segment tests
segment audits
```

### 最小修改

```text
service.py
```

只负责调用、持久化和 audit。

---

# 31. 明确不应修改的文件

原则上：

```text
state_machine_v2.py
episode.py
pools/*
strategies/*
portfolio/*
```

都不需要因为本次统计细化而修改业务逻辑。

尤其：

> 如果实现过程中发现必须修改 `state_machine_v2.py` 或 Episode 起止规则才能完成 Segment，则说明实现方向已经越界，应停止并重新审视设计。

---

# 32. 风险隔离

本次风险分成两类。

## 32.1 业务逻辑风险

理论上应接近零。

因为 Segment：

```text
只读 Episode
不反馈 Episode
```

不存在：

```text
Segment → State Machine
Segment → Episode
Segment → Strategy
```

反向依赖。

---

## 32.2 工程基础设施风险

仍需防范：

```text
schema 定义错误
contract registry 错误
SQL insert 列顺序错误
transaction 删除顺序错误
import 错误
audit SQL 错误
```

因此不能表述为：

> “绝对不可能引入 Episode 外 Bug。”

更准确的工程目标是：

> **业务行为影响面严格限制在 Episode 统计子系统；共享基础设施通过回归测试证明没有副作用。**

---

# 33. 规则治理边界

本次 Segment 是：

```text
已有事实的确定性派生
```

不增加：

```text
买入条件
卖出条件
禁止条件
评分
排序
仓位
```

因此暂时属于：

```text
Indicators / Observation / Research
```

不需要修改 MyTradingSystem 的业务规则。

---

# 34. 何时需要升级为正式交易规则

如果未来出现：

```text
第 3 个 ACTIVE Sprint 禁止开仓

Sprint 2 收益低于 X 则降分

NON_ACTIVE 超过 N 天则退出

Sprint 回撤达到 X 则减仓
```

这时 Segment 已经从：

```text
Observation
```

进入：

```text
Decision
```

必须走正式规则治理流程：

```text
MyTradingSystem
↓
规则裁定
↓
commit/hash 锁定
↓
QRP-ATLAS change proposal
↓
contract / implementation / tests
↓
新 rule_version_set
```

不能直接在 QRP-ATLAS 内把统计规律固化成策略规则。

---

# 35. 第一阶段研究目标

Segment v1 上线后第一批研究不需要复杂模型。

优先回答：

## Q1

```text
Episode 的收益是否主要发生在 ACTIVE？
```

统计：

```text
ACTIVE compound return
NON_ACTIVE compound return
ACTIVE trading day ratio
```

---

## Q2

```text
第几轮 ACTIVE Sprint 最有价值？
```

按：

```text
active_sprint_no
```

统计：

```text
样本数
平均收益
中位收益
胜率
平均持续交易日
最大回撤
```

---

## Q3

```text
Repeated reentry 是否存在收益衰减？
```

比较：

```text
Sprint 1
Sprint 2
Sprint 3
Sprint 4+
```

---

## Q4

```text
NON_ACTIVE 阶段是整理还是利润侵蚀？
```

统计：

```text
NON_ACTIVE return
duration
drawdown
next ACTIVE probability
```

这几项即可检验：

> System B Episode 内部是否存在“启动 → 主升 → 反复冲刺 → 衰竭”的生命周期结构。

---

# 36. 本次不做的内容

明确排除：

```text
不改变 Episode 定义
不改变 ACTIVE 定义
不改变 MA5 / MA10
不改变 Episode end
不修改历史 Episode ID
不修改现有 Episode return
不引入 Sprint merge rule
不引入交易策略
不引入评分
不引入阈值
不修改 Pool
不修改 Portfolio
```

---

# 37. 验收标准

本次升级完成必须同时满足：

### A. Legacy

```text
旧 Episode 结果零变化
旧 Episode Observation 结果零变化
```

### B. Isolation

```text
State Machine 零业务改动
Pool 零业务改动
Strategy 零业务改动
Portfolio 零业务改动
```

### C. Completeness

每个 Episode Observation：

```text
恰好映射到一个 Segment
```

### D. Mathematical Closure

```text
Π(1 + segment_return)
≈
1 + episode_return
```

### E. Reentry Closure

```text
ACTIVE Segment Count
=
ma5_reentry_count + 1
```

### F. Determinism

重复 rebuild：

```text
Segment identity 与统计结果稳定
```

### G. Transaction Safety

任何 Segment audit 失败：

```text
整个 Episode rebuild rollback
```

---

# 38. 最终架构结论

本次升级不是：

> System B 规则升级。

也不是：

> Episode 算法升级。

而是：

> **Episode 统计粒度升级。**

最终结构：

```text
System B State
      ↓
Episode
      ↓
Episode Observation
      ↓
Episode Segment
      ├── ACTIVE Segment
      │      └── ACTIVE Sprint
      │
      └── NON_ACTIVE Segment
```

设计原则总结为一句话：

> **Episode 保持原样，Segment 只对 Episode 已有事实进行确定性、无损、可复原的下游拆解。**

因此本次实现的核心工程约束可以正式命名为：

# Append-only Episode Refinement

即：

```text
旧输入不变
旧状态机不变
旧 Episode 算法不变
旧 Episode 表不变
旧 Observation 语义不变
旧策略不变

只新增：
Episode Segment Contract
Episode Segment Derivation
Episode Segment Persistence
Episode Segment Audit
Episode Segment Research
```

这是本次统计粒度细化的正式设计边界。

---

# 39. Implementation Notes

落地实现时必须严格遵守以下工程准则：

### 39.1 浮点数闭合公差（Return Closure Tolerance）
- **禁止浮点数严格相等（`==`）**。
- **比较对象**：比较收益乘积因子本身，而非简单收益率差值：
  ```python
  segment_factor = np.prod(1.0 + segment_returns)
  episode_factor = 1.0 + latest_episode_return

  is_closed = np.isclose(
      segment_factor,
      episode_factor,
      rtol=1e-10,
      atol=1e-12,
  )
  ```
- **可解释性审计输出**：Audit 校验未通过时，必须显式抛出结构化诊断信息：`episode_id`、`lhs (segment_factor)`、`rhs (episode_factor)`、`abs_error` 与 `rel_error`，便于快速定位与复现。

---

### 39.2 矢量化与 Episode 边界严格隔离
- **禁止全局跨 Episode 的 `shift` 操作**。
- 必须先进行稳定双键排序，再在 `episode_id` 分组内进行状态转移检测：
  ```python
  # 1. 稳定排序
  sorted_obs = observations.sort_values(
      ["episode_id", "trade_date"],
      kind="mergesort",
  )

  # 2. 组内 shift 隔离
  prev_state = sorted_obs.groupby("episode_id", sort=False)["segment_state"].shift()
  is_new_segment = prev_state.isna() | sorted_obs["segment_state"].ne(prev_state)

  # 3. 组内连续编号
  segment_no = is_new_segment.groupby(
      sorted_obs["episode_id"], sort=False
  ).cumsum()
  ```
- 杜绝将 `EP_0001` 尾部的 ACTIVE 与 `EP_0002` 首部的 ACTIVE 错误合并为同一个 Segment。

---

### 39.3 性能准则与 Benchmark
- 生产 Pipeline 本身按 `asset_batch` 批次处理，核心实现要求**杜绝 Python row-by-row 热循环、保持内存受控与矢量化吞吐**；
- 性能指标以生产环境的实际 benchmark 测试为准，不作为绝对秒级的硬性验收承诺。

---

### 39.4 API 消费层解耦（分阶段实施）
- **Phase 1（本次范围）**：聚焦于 Canonical 核心闭环：
  ```text
  Contract → Derivation → Persistence → Audit → Tests
  ```
- **Phase 2（后续扩展）**：当独立前端或研究看板产生实际消费需求时，在 `src/qrp_atlas/api/routes/system_b.py` 中扩展对应 DTO 与端点：
  ```http
  GET /api/v1/system-b/episodes/{episode_id}/segments
  ```
  不将 API 与前端联调纳入 Phase 1 的阻塞性验收范围。