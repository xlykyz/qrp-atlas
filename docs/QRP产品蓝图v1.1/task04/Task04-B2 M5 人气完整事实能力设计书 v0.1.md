# Task04-B / B2｜M5 人气完整事实能力设计书 v0.1

> 状态：业务算法已封板  
> 适用版本：QRP Atlas v1.1  
> 所属工作包：Task04-B｜M5 人气完整事实能力  
> 子任务定位：Task04-B1 建设人气榜数据底座；Task04-B2 完成 Theme 映射与 M5 Observation，作为 Task04-B 最终交付。  
> 业务依据：`MyTradingSystem/docs/09_M5人气与M6市场情绪规格.md` v0.1  
> 核心原则：最小化实现既有 M5 定义，不新增评分、权重、阈值、衰减或其他人气模型。

---

## 1. 目标

Task04-B 的最终目标是：

> 给定交易日 D，读取当天全部正式人气榜完整快照，将所有榜单记录映射到 D 日有效 Theme Membership，并为每个 Theme 产出确定性的日级 M5 人气事实。

最终正式指标：

```text
theme_member_count
theme_hot_stock_count
theme_hot_stock_ratio
theme_hot_list_appearance_count
theme_hot_source_count
```

M5 仍然只是题材级客观事实：

```text
M5 高
≠
Theme 自动成为主线
```

Task04-B 不负责主线判断。

---

## 2. B1 与 B2 的职责边界

### 2.1 Task04-B1｜Popularity Data Foundation

B1 负责：

```text
Tushare dc_hot
Tushare ths_hot
        ↓
Raw
        ↓
Clean
        ↓
逻辑 snapshot reconstruction
        ↓
完整 Top100 校验
        ↓
dc_hot / ths_hot canonical tables
```

B1 保存 Provider 在 D 日能够提供的全部完整日内 snapshot。

B1 不做：

```text
Theme mapping
M5 aggregation
M5 score
M5 threshold
主线判断
```

### 2.2 Task04-B2｜M5 Observation

B2 只负责：

```text
D 日全部完整 popularity snapshots
        ↓
统一股票身份
        ↓
D 日有效 Theme Membership
        ↓
Theme × Popularity Record Mapping
        ↓
M5 Daily Observation
```

B2 不重新定义榜单 snapshot，不修改 B1 数据。

---

## 3. 正式数据来源

M5 v0.1 当前正式读取两套 B1 canonical 数据：

```text
dc_hot
ths_hot
```

对应：

```text
EASTMONEY / POPULARITY
THS        / HOT_STOCK
```

当前算法只对这两个已正式接入来源负责。

未来新增淘股吧、其他社区或其他榜单，必须作为新的数据能力单独接入，不属于 B2 v0.1 的实现范围。

---

## 4. D 日榜单输入语义

### 4.1 使用 D 日全部完整 snapshot

M5 不选择：

```text
最后一个 snapshot
收盘 snapshot
固定时点 snapshot
```

而是使用 D 日两个正式来源中已经由 B1 重建并校验通过的全部完整 snapshot。

例如：

```text
EASTMONEY
snapshot_seq = 1
snapshot_seq = 2
...
snapshot_seq = 12

THS
snapshot_seq = 1
snapshot_seq = 2
...
snapshot_seq = 15
```

则上述全部 `12 × 100 + 15 × 100` 条榜单记录均进入 D 日 M5 映射输入。

### 4.2 不做日内去重

B2 不在输入阶段执行：

```text
DISTINCT ticker
DISTINCT source + ticker
取最后一次出现
只保留最佳排名
```

同一股票在不同 snapshot 中重复出现时，每一条都仍然是一条独立榜单记录。

该语义用于支持原 M5 指标 `theme_hot_list_appearance_count` 对“反复被榜单看见”的记录总数统计。

---

## 5. Theme Membership 语义

M5 使用：

```text
D 日有效 Theme Membership
```

而不是：

```text
M4 Effective Member
```

正式 invariant：

```text
M5 Theme Membership
≠
M4 Calculation Eligibility
```

因此：

- 上市不足 5 个实际交易日的股票仍可以参与 M5；
- 当日停牌股票仍可以参与 M5；
- 不使用 M4 的新股过滤；
- 不使用 M4 的停牌过滤；
- 不使用涨跌幅、行情完整性等 M4 计算资格条件。

原因是：

> M5 衡量市场关注，而不是价格指数计算资格。

Theme Membership 的 PIT 解析完全复用 QRP 已有 StockCollection / Theme Membership 能力。

B2 不新增自己的 Membership 规则。

必须满足：

> 不使用未来才确认的 Theme 归属回填历史 M5。

---

## 6. 股票身份映射

Popularity canonical 数据中的股票必须通过 QRP 既有统一股票身份能力与 Theme Membership 对齐。

B2 不新增：

```text
ticker mapping rule
代码补丁表
特殊股票映射逻辑
```

统一身份解析属于现有基础设施。

---

## 7. Mapping 规则

对于 D 日每一条榜单记录：

```text
trade_date
source
list_name
snapshot_seq
ticker
rank_position
...
```

查找该股票在 D 日所属的所有有效 Theme。

若股票 A 同时属于 Theme X、Theme Y、Theme Z，则一条榜单记录产生：

```text
X × A × 本条榜单记录
Y × A × 本条榜单记录
Z × A × 本条榜单记录
```

同一榜单记录允许映射到多个 Theme。

不进行 `1 / Theme数量` 之类的拆分或权重分配。

Theme 本身允许存在成员重叠。

---

## 8. theme_member_count

定义：

```text
theme_member_count(T,D)
=
D 日 Theme T 有效 Membership 中的去重股票数量
```

即：

```sql
COUNT(DISTINCT member_ticker)
```

它是 Theme 人气覆盖率的分母事实，不是 M4 的 `effective_member_count`。

---

## 9. theme_hot_stock_count

定义：

```text
theme_hot_stock_count(T,D)
=
D 日全部正式 popularity snapshots 中
至少出现过一次的 Theme T 去重成员数量
```

计算：

```sql
COUNT(DISTINCT ticker)
```

股票 A 即使在 DC 出现 10 次、THS 出现 8 次，在 `theme_hot_stock_count` 中仍只贡献 1。

该指标回答：

> 今天这个 Theme 内有多少只不同股票获得过榜单关注。

---

## 10. theme_hot_stock_ratio

定义：

```text
theme_hot_stock_ratio(T,D)
=
theme_hot_stock_count(T,D)
/
theme_member_count(T,D)
```

例如：

```text
Theme A
member_count = 10
hot_stock_count = 5
ratio = 0.50

Theme B
member_count = 100
hot_stock_count = 8
ratio = 0.08
```

该指标只保存客观比例事实。

本版不规定：

```text
多少比例算强
多少比例算弱
如何与其他指标联合评分
```

零分母规则：

```text
theme_member_count > 0
→ 正常计算

theme_member_count = 0
→ theme_hot_stock_ratio = NULL
```

不得使用 0 代替 NULL，因为“Theme 没有有效成员”与“Theme 有成员但无人上榜”是不同事实。

---

## 11. theme_hot_list_appearance_count

定义：

> Theme 成员在 D 日全部正式来源、全部正式榜单、全部完整 snapshot 中产生的榜单记录总数。

计算：

```sql
COUNT(*)
```

这里不做 ticker 去重。

例如股票 A：

```text
EASTMONEY 出现 10 个 snapshot
THS 出现 8 个 snapshot
```

则 A 为其所属 Theme 贡献 18 次 `theme_hot_list_appearance_count`。

若另一股票 B 又出现 5 次，则总 appearance_count = 23。

该指标回答：

> Theme 成员今天在人气榜单中被反复看到多少次。

---

## 12. theme_hot_source_count

定义：

```text
theme_hot_source_count(T,D)
=
D 日至少出现一只 Theme T 成员的去重 source 数量
```

计算：

```sql
COUNT(DISTINCT source)
```

当前正式来源只有：

```text
EASTMONEY
THS
```

因此当前自然取值为 0 / 1 / 2。

B2 不进一步产生 `consensus`、`cross_platform_score`、`source_strength` 等新指标。

---

## 13. 完整计算示例

假设 Theme T 的 D 日有效成员：

```text
A B C D E F G H I J
```

因此：

```text
theme_member_count = 10
```

D 日榜单事实：

```text
DC snapshot 1
A B C

DC snapshot 2
A B D

DC snapshot 3
A E F

THS snapshot 1
A B

THS snapshot 2
A G
```

则去重上榜成员为：

```text
A B C D E F G
```

因此：

```text
theme_hot_stock_count = 7
theme_hot_stock_ratio = 7 / 10 = 0.70
theme_hot_list_appearance_count = 3 + 3 + 3 + 2 + 2 = 13
theme_hot_source_count = 2
```

最终：

```text
theme_member_count                  = 10
theme_hot_stock_count               = 7
theme_hot_stock_ratio               = 0.70
theme_hot_list_appearance_count     = 13
theme_hot_source_count              = 2
```

---

## 14. 无人上榜 Theme 的输出

B2 应对 D 日所有正式有效 Theme 形成明确 Observation。

若 Theme 有成员：

```text
theme_member_count = 20
```

但当天没有任何成员进入任何正式榜单，则：

```text
theme_hot_stock_count = 0
theme_hot_stock_ratio = 0.0
theme_hot_list_appearance_count = 0
theme_hot_source_count = 0
```

不得通过“没有结果行”表达零人气。

这样可以明确区分：

```text
计算结果为 0
```

与：

```text
M5 当日没有成功计算
```

---

## 15. 上游数据缺失语义

若 D 日某个正式 M5 数据源缺失，或其 B1 数据未形成合法完整 snapshot：

```text
不得把该来源解释成 source_count = 0
```

因为：

```text
数据缺失
≠
市场没有关注
```

因此生产 M5 Observation 前，应确认当前正式数据来源的 D 日 canonical 输入可用。

输入不完整时：

```text
M5 D 日计算 fail closed
```

不得生成看似合法但实际缺少来源的正式 Observation。

---

## 16. 正式 Observation

建立：

```text
theme_m5_observation
```

业务核心字段：

```text
theme_id
trade_date

theme_member_count
theme_hot_stock_count
theme_hot_stock_ratio
theme_hot_list_appearance_count
theme_hot_source_count
```

技术版本、生产审计等字段沿用 Task04 已有统一规范，不为 M5 单独发明新的治理体系。

算法版本：

```text
theme_m5_observation@0.1.0
```

一条正式业务 Observation 的唯一语义：

```text
theme_id × trade_date
```

---

## 17. 生产计算流程

正式计算流程：

```text
Target Trade Date D
        ↓
读取 D 日 dc_hot 全部 snapshots
+
读取 D 日 ths_hot 全部 snapshots
        ↓
输入完整性检查
        ↓
统一股票身份
        ↓
读取 D 日 PIT Theme Membership
        ↓
形成 Theme × Popularity Record Mapping
        ↓
按 theme_id 聚合
        ↓
theme_member_count
theme_hot_stock_count
theme_hot_stock_ratio
theme_hot_list_appearance_count
theme_hot_source_count
        ↓
theme_m5_observation
```

生产计算应优先集合化处理。

禁止：

```text
逐 Theme 查询榜单
逐股票 N+1 Membership 查询
逐 Theme 单独打开数据库
```

---

## 18. 历史与 PIT 原则

M5 Observation 必须满足 Task04 通用 PIT 原则：

```text
观察日事实
只使用观察时点允许获得的信息
```

特别是 Theme Membership：

```text
不得使用未来才确认的成员关系
重新解释历史 Theme 归属
```

B1 的 `dc_hot`、`ths_hot` 仍然是 Provider mirror，可以按照 B1 自身规则重新同步。

M5 属于 QRP 派生事实，其生产、历史重建和审计沿用 Task04 已有统一事实治理机制，不在 B2 中重新发明另一套生命周期模型。

---

## 19. M5 v0.1 明确不做

Task04-B / B2 完成后仍然不实现：

```text
榜单排名权重
平台权重
THS hot 字段权重
Top10 权重
Top20 权重
最佳排名指标
snapshot 持续率
首次上榜时间
最后上榜时间
排名变化速度
多日累计
时间衰减
M5 score
M5 qualified
M5 强弱阈值
Theme size penalty
其他归一化模型
跨平台 consensus score
主线判断
交易授权
```

其中 `rank_position`、`hot`、`snapshot_seq`、`source_rank_time` 继续保留在 B1 canonical 数据中，以供未来研究使用，但 M5 v0.1 不消费这些字段形成额外指标。

---

## 20. Task04-B 最终能力边界

Task04-B 完成后，QRP 应具备：

```text
B1
完整可复现的人气榜 Provider Facts

+

B2
D 日全部榜单 snapshot
×
PIT Theme Membership
        ↓
完整 M5 Daily Observations
```

最终业务事实：

```text
theme_member_count
theme_hot_stock_count
theme_hot_stock_ratio
theme_hot_list_appearance_count
theme_hot_source_count
```

正式 invariant：

```text
全部 snapshot
≠
只取最后 snapshot

appearance
≠
去重股票数

Theme Membership
≠
M4 Effective Member

raw observation
≠
score

M5
≠
mainline decision
```

---

## 21. Task04-B / B2 完成判定

满足以下条件后，Task04-B 可以退出：

1. B1 的 `dc_hot`、`ths_hot` 正式成为 B2 输入；
2. D 日全部完整 snapshot 均参与 M5；
3. B2 正确使用 D 日 PIT Theme Membership；
4. 同一 popularity record 可正确映射至多个 Theme；
5. 正确计算：
   - `theme_member_count`
   - `theme_hot_stock_count`
   - `theme_hot_stock_ratio`
   - `theme_hot_list_appearance_count`
   - `theme_hot_source_count`
6. 无榜单命中的有效 Theme 仍生成明确的零值 Observation；
7. 上游正式来源缺失时 fail closed，不将缺失解释成零人气；
8. 历史读取不使用未来 Membership 信息；
9. 具备自动化测试、生产 Pipeline Contract、查询与审计所需的正式数据能力；
10. 未引入任何未经批准的权重、评分、阈值或额外 M5 指标。

至此：

```text
Task04-B1
Popularity Data Foundation
        +
Task04-B2
Theme Popularity Observation
        =
Task04-B
M5 人气完整事实能力
```
