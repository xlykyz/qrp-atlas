# Task04-B1｜M5 人气数据底座设计书

> 状态：设计已对齐  
> 适用版本：QRP Atlas v1.1  
> 所属工作包：Task04-B｜M5 人气完整事实能力  
> 本文范围：仅定义东方财富人气榜、同花顺热股榜的数据采集、Raw/Clean 分层及数据库持久化能力。  
> 不定义 M5 聚合、评分、阈值、Theme 映射或生产调度策略。

---

## 1. 设计目标

Task04-B1 建设两套稳定、可重复执行的人气榜数据底座：

```text
东方财富 A 股人气榜
同花顺热股榜
```

数据底座的职责是：

> 忠实获取 Tushare 当前提供的人气榜历史/当日数据，保存原始响应，经确定性清洗后形成 QRP canonical 数据，并原子写入 DuckDB。

该能力既服务于日常采集，也支持后续的：

```text
首次初始化
历史补跑
指定范围重跑
Provider 历史数据重新同步
```

但上述场景如何选择日期、何时触发、如何调度，不属于本数据 Pipeline 的业务语义。

Pipeline 只提供统一的数据获取和持久化方法。

---

## 2. 核心设计原则

### 2.1 Provider 数据是客观数据镜像

`dc_hot` 与 `ths_hot` 属于外部 Provider 客观数据，不属于 QRP 自维护的动态业务事实。

其基本语义为：

```text
Tushare 当前返回什么
QRP 就保存什么
```

QRP 只执行必要的：

```text
字段标准化
类型标准化
股票代码标准化
榜单来源辨识
榜单类别辨识
逻辑 snapshot 重建
质量校验
```

不得在数据底座层增加：

```text
M5 score
M5 qualified
平台权重
排名权重
24H/1H 等未经 Provider 明确证明的窗口定义
Theme 判断
主线判断
```

### 2.2 Raw means raw

Raw CSV 是 Tushare Provider 响应的原始镜像。

硬规则：

> Raw CSV 不允许增加、删除、改名、转换任何字段。

因此禁止在 Raw 层增加：

```text
provider
source
endpoint
requested_at
ingested_at
snapshot_id
snapshot_seq
```

也禁止执行：

```text
字段 rename
类型转换
ticker 标准化
排序
去重
时间解析
分类字段补充
```

允许的唯一操作是：

> 将同一 endpoint、同一批次中多个日期返回的同结构 DataFrame 进行纵向 concat。

数据来源、endpoint、目标日期范围、运行时间等批次信息，通过：

```text
文件名
目录
Pipeline run metadata
日志
```

表达，不写入 Raw CSV 内容。

### 2.3 Clean CSV 是数据库写入前的完整 canonical 数据集

所有 QRP 自己的标准化、辨识和派生字段均从 Clean 阶段开始。

基本链路固定为：

```text
Tushare
   ↓
逐日 Provider 请求
   ↓
Raw DataFrames
   ↓
concat
   ↓
Raw CSV
   ↓
Cleaning / Normalization
   ↓
Clean CSV
   ↓
一次 DuckDB Connection
   ↓
一次数据库事务
   ↓
Canonical Table
```

Clean CSV 应与最终 DuckDB 写入内容保持基本等价。

数据库写入阶段不再承担主要数据清洗职责。

---

## 3. 正式数据源

### 3.1 东方财富人气榜

正式 endpoint：

```text
Tushare dc_hot
```

固定 Provider 参数：

```python
pro.dc_hot(
    trade_date=D,
    market="A股市场",
    hot_type="人气榜",
    is_new="N",
)
```

固定语义：

```text
source    = EASTMONEY
list_name = POPULARITY
```

正式数据底座只采集：

```text
A股市场
+
人气榜
```

不采集：

```text
飙升榜
其他市场
```

若未来需要飙升榜，应单独定义新的数据能力，不与当前 M5 Base 数据混入。

### 3.2 同花顺热股榜

正式 endpoint：

```text
Tushare ths_hot
```

固定 Provider 参数：

```python
pro.ths_hot(
    trade_date=D,
    market="热股",
    is_new="N",
)
```

固定语义：

```text
source    = THS
list_name = HOT_STOCK
```

正式数据底座只采集：

```text
热股
```

ETF、可转债、期货、概念板块、行业板块、港股、美股等其他榜单不属于当前数据底座。

---

## 4. 为什么统一使用 `is_new=N`

正式数据底座必须保存 Tushare 当前能够返回的全部日内榜单快照，而不是只保存每日最终榜单。

因此两个接口统一：

```text
is_new = N
```

数据底座保留完整多时点事实，未来上层可以自由研究或选择：

```text
盘中 snapshot
收盘附近 snapshot
晚间最终 snapshot
排名变化
持续入榜时间
跨平台共振
THS hot 强度变化
```

数据底座本身不决定：

```text
哪一个 snapshot 才是 D 日正式 M5 输入
```

该问题属于后续 M5 Observation 层。

---

## 5. 数据分层

### 5.1 Raw Layer

两个 endpoint 分别形成独立 Raw Dataset。

示例：

```text
dc_hot_raw_<start>_<end>.csv
ths_hot_raw_<start>_<end>.csv
```

文件命名只用于运行和审计辨识，不属于数据内容。

#### DC Raw

字段完全以 Tushare 实际返回为准，例如：

```text
trade_date
data_type
ts_code
ts_name
rank
pct_change
current_price
rank_time
```

#### THS Raw

字段完全以 Tushare 实际返回为准，例如：

```text
trade_date
data_type
ts_code
ts_name
rank
pct_change
current_price
concept
rank_reason
hot
rank_time
```

若 Provider 后续字段发生变化，应由 schema validation 暴露，而不是 Raw 层自行兼容或吞掉变化。

---

## 6. Clean Layer

### 6.1 公共 canonical 字段

Clean 数据统一形成以下公共语义：

```text
trade_date

source
list_name

ticker
name

rank_position
pct_change
current_price

source_rank_time

snapshot_seq
snapshot_started_at
snapshot_completed_at
```

其中：

```text
source
list_name
```

属于 QRP 增加的最小辨识/分类字段。

它们只描述明确的数据来源和榜单身份，不添加任何业务判断。

### 6.2 DC 专属字段

DC 当前无额外正式业务字段。

Clean 表保留其 Provider 可提供的：

```text
pct_change
current_price
source_rank_time
```

不为了与 THS schema 对齐而制造：

```text
hot = NULL
concept = NULL
rank_reason = NULL
```

### 6.3 THS 专属字段

THS 额外保留：

```text
hot
concept
rank_reason
```

这些属于 Provider 客观字段。

其中：

```text
concept
```

仅表示同花顺自身的概念标签。

不得解释为：

```text
THS concept
=
QRP Theme
```

Theme 映射属于后续独立能力。

---

## 7. Snapshot 语义

### 7.1 `rank_time` 是 row-level Provider 时间

实际采样数据已经证明：

同一张 Top100 榜单中的不同股票，其 `rank_time` 可能存在秒级差异。

因此：

```text
source_rank_time
≠
snapshot primary key
```

不能简单按照完整 `rank_time` 分组榜单。

### 7.2 QRP 重建逻辑 Snapshot

Clean 阶段需要确定性重建逻辑榜单。

推荐规则：

对于：

```text
source × trade_date
```

按照每个：

```text
rank_position
```

的 `source_rank_time` 升序排列。

其第 N 次出现归入：

```text
snapshot_seq = N
```

随后将同一 `snapshot_seq` 下：

```text
rank_position = 1..100
```

组成一张完整逻辑榜单。

例如：

```text
snapshot_seq = 1
rank 1..100

snapshot_seq = 2
rank 1..100

...
```

由该 snapshot 内：

```text
MIN(source_rank_time)
```

得到：

```text
snapshot_started_at
```

由：

```text
MAX(source_rank_time)
```

得到：

```text
snapshot_completed_at
```

---

## 8. Snapshot 数据质量约束

每个被识别为完整 snapshot 的榜单必须满足：

```text
row_count = 100

distinct ticker = 100

distinct rank_position = 100

MIN(rank_position) = 1

MAX(rank_position) = 100

rank_position exactly covers 1..100
```

若某个逻辑 snapshot 无法满足完整 Top100：

```text
本次 Provider 数据视为不完整
Pipeline fail closed
```

不得静默删除缺失记录后继续写入。

同时要求同一交易日：

```text
snapshot[n].snapshot_completed_at
<
snapshot[n+1].snapshot_started_at
```

用于识别明显的 snapshot reconstruction 异常。

---

## 9. Canonical Tables

正式建立两张独立表：

```text
dc_hot
ths_hot
```

不建立单一万能：

```text
popularity_rank
```

原因：

1. 两个 Provider 原始字段不同；
2. THS 存在 `hot / concept / rank_reason`；
3. 两套数据生命周期独立；
4. 两条 Pipeline 可以独立采集、重跑和替换；
5. M5 上层统一读取比底层强行统一 schema 更自然。

---

## 10. 主键设计

推荐两个表统一采用：

```text
PRIMARY KEY (
    trade_date,
    snapshot_seq,
    rank_position
)
```

同时执行质量约束：

```text
UNIQUE (
    trade_date,
    snapshot_seq,
    ticker
)
```

不使用：

```text
trade_date + ticker
```

因为同一股票一天可以出现在多个 snapshot。

也不使用：

```text
trade_date + rank_time + ticker
```

因为 `rank_time` 是 Provider row-level timestamp，不是稳定的逻辑 snapshot 身份。

---

## 11. Pipeline 参数模型

数据 Pipeline 不区分：

```text
production
bootstrap
backfill
rerun
repair
```

这些不是数据采集方法。

Pipeline 只接受统一目标日期参数。

支持：

```text
trade_date
```

或：

```text
start_date
end_date
```

因此：

```text
单日运行
=
日期范围长度为 1 的特例
```

所有日期范围执行统一走 Batch Pipeline。

---

## 12. Batch Fetch 设计

Provider endpoint 本身按单日请求，因此范围能力内部执行：

```text
target date range
       ↓
date 1 → Provider
date 2 → Provider
date 3 → Provider
...
date N → Provider
       ↓
Raw DataFrames
       ↓
concat
       ↓
ONE Raw Dataset
```

例如：

```text
2026-08-01 ~ 2026-08-31
```

不是生成 31 个独立完整 Pipeline。

而是一个 batch：

```text
31 次 Provider date request
        ↓
1 个 Raw DataFrame
        ↓
1 个 Raw CSV
        ↓
1 个 Clean DataFrame
        ↓
1 个 Clean CSV
        ↓
1 次数据库事务
```

---

## 13. Empty Response 语义

单个目标日期 Provider 返回空响应时：

```text
不把 empty 自动解释为业务上的“当天没有榜单”
```

同时：

```text
不得因为 empty response 删除数据库中已有的该日数据
```

因此保留当前 QRP snapshot Pipeline 的 fail-closed 原则。

范围请求中存在空日期时：

```text
仅对本次成功获取并完成验证的日期执行 replacement
```

不使用简单：

```sql
DELETE
WHERE trade_date BETWEEN start_date AND end_date
```

去无条件删除整个自然日期区间。

---

## 14. Clean Batch

整个 Raw Dataset 完成采集后，再统一执行：

```text
schema validation
↓
field normalization
↓
ticker normalization
↓
numeric conversion
↓
rank validation
↓
source/list classification
↓
snapshot reconstruction
↓
snapshot integrity validation
↓
canonical ordering
↓
Clean DataFrame
↓
Clean CSV
```

Clean CSV 应代表本批次最终准备写入数据库的完整业务数据。

---

## 15. DuckDB 持久化

### 15.1 一次 Batch 只打开一次 DuckDB

禁止：

```text
for each date:
    open DuckDB
    delete
    insert
    close DuckDB
```

正式模式：

```text
完成整个日期范围 Provider Fetch
↓
完成整个 Raw CSV
↓
完成整个 Clean CSV
↓
打开 DuckDB 一次
↓
BEGIN TRANSACTION
↓
删除本次成功验证日期的旧数据
↓
一次性插入整个 Clean Batch
↓
COMMIT
↓
关闭 DuckDB
```

### 15.2 Transaction Boundary

数据库事务边界为：

> 一个 endpoint 的一个完整日期批次。

例如：

```text
dc_hot
2026-08-01 ~ 2026-08-31
```

为一个事务。

THS 是另一个独立事务。

任何数据库写入错误：

```text
ROLLBACK entire batch
```

不得留下部分日期已更新、部分日期未更新的半完成状态。

---

## 16. Replacement 语义

对于本次成功获取并验证的目标日期：

```text
DELETE existing target-date rows
+
INSERT current clean rows
```

即：

```text
Provider current response
→
QRP current canonical mirror
```

因此相同日期重复执行具有天然幂等性。

Pipeline 不关心本次调用属于：

```text
首次初始化
历史补跑
历史重跑
Provider修订重新同步
日常采集
```

其底层行为始终一致。

这些场景只表现为：

```text
调用时间不同
目标日期集合不同
```

---

## 17. 方法职责建议

实现层可按三层方法组织。

### 17.1 Provider Methods

```text
fetch_dc_hot(date)
fetch_ths_hot(date)
```

职责：

```text
构造固定 Provider 参数
调用 Tushare
返回未经修改的原始 DataFrame
```

### 17.2 Batch Methods

```text
fetch_dc_hot_range(start_date, end_date)
fetch_ths_hot_range(start_date, end_date)
```

职责：

```text
逐日请求
↓
Raw concat
↓
Raw CSV
↓
Clean
↓
Clean CSV
```

内部清洗可进一步拆分：

```text
clean_dc_hot_batch()
clean_ths_hot_batch()
```

### 17.3 Persistence Methods

```text
replace_dc_hot_batch()
replace_ths_hot_batch()
```

职责：

```text
一次 DuckDB connection
一次 transaction
目标日期 replacement
whole-batch insert
rollback on failure
```

---

## 18. 调度层边界

本设计不规定：

```text
每天几点运行
首次初始化跑多久历史
缺哪些日期
何时补跑
多久重新同步一次历史数据
是否自动检测缺口
```

调度层未来只负责决定：

```text
调用哪个 Pipeline
+
传入哪个 target date / date range
+
什么时候调用
```

数据 Pipeline 不通过 `mode` 参数理解调用目的。

因此不引入：

```text
mode=bootstrap
mode=backfill
mode=rerun
```

等额外业务参数。

---

## 19. 与 M5 的边界

Task04-B1 到此结束。

B1 输出：

```text
dc_hot
ths_hot
```

它们只是人气榜 canonical source facts。

后续 Task04-B 才进入：

```text
dc_hot / ths_hot
        ↓
Observation Time Selection
        ↓
PIT Theme Membership
        ↓
Theme Popularity Aggregation
        ↓
theme_hot_stock_count
theme_hot_list_appearance_count
theme_hot_source_count
```

B1 不决定：

```text
每日选择哪个 snapshot
平台是否等权
排名是否加权
hot 是否进入评分
多日累计/衰减
M5 threshold
```

---

## 20. 明确禁止项

本工作包禁止实现层自行增加：

```text
24H 标签
1H 标签
ranking_window 推断

M5 score
M5 qualified

rank weighting
source weighting
hot weighting

Theme mapping
Theme score

daily canonical snapshot selection

历史 finalized 机制

coverage ledger

bootstrap/backfill/rerun 专用运行模式
```

除非后续设计明确批准。

---

## 21. B1 验收条件

Task04-B1 完成至少满足：

1. 正式建立 `dc_hot` 与 `ths_hot` 两张 canonical 表；
2. DC 固定使用 `market='A股市场' + hot_type='人气榜' + is_new='N'`；
3. THS 固定使用 `market='热股' + is_new='N'`；
4. Raw CSV 与 Tushare 原始响应字段完全一致，零附加、零修改；
5. 日期范围多个 Provider response 被合并为一个 Raw Batch；
6. 整个 Raw Batch 一次完成 Clean；
7. Clean CSV 与最终数据库 canonical 数据结构一致；
8. 能确定性重建多时点 Top100 snapshot；
9. 每个完整 snapshot 通过 Top100 integrity validation；
10. 单日调用与日期范围调用使用相同能力；
11. 一个日期范围只打开一次 DuckDB；
12. 一个日期范围使用一个数据库事务完成 replacement；
13. 空 Provider response 不会删除已有历史数据；
14. 同一日期范围重复执行结果幂等；
15. DC、THS 独立运行、独立事务、互不覆盖；
16. 不实现任何 M5 聚合、评分、阈值或 Theme 判断；
17. 初始化、补跑、历史重跑等调用目的不进入 Pipeline 业务语义，由后续调度层决定。

---

## 22. Implementation Notes（非阻塞）

以下细化不改变当前设计语义，也不构成 B1 验收阻塞项：

- 若同一 `rank_position` 的 `source_rank_time` 完全相等，实现排序时应保持稳定；snapshot 分组逻辑仍按 Provider 原始出现次序确定，不由时间相等额外拆分。
- 可对 `snapshot_seq` 做连续性自洽校验，例如要求 `1, 2, ..., N` 无跳跃。该校验属于实现增强，不新增业务规则。

---

## 23. 最终数据链

```text
                 Tushare
                    │
          ┌─────────┴─────────┐
          │                   │
       dc_hot               ths_hot
          │                   │
          ▼                   ▼
   Raw Provider DF     Raw Provider DF
          │                   │
      date concat          date concat
          │                   │
          ▼                   ▼
      Raw CSV             Raw CSV
          │                   │
          ▼                   ▼
      Clean Batch         Clean Batch
          │                   │
          ▼                   ▼
      Clean CSV           Clean CSV
          │                   │
          ▼                   ▼
    one transaction     one transaction
          │                   │
          ▼                   ▼
       dc_hot               ths_hot
        table                table
          │                   │
          └─────────┬─────────┘
                    │
                    ▼
             Task04-B M5
             Observation Layer
```

---

## 24. 一句话定义

> **Task04-B1 建立的是 Tushare 东方财富 A 股人气榜与同花顺热股榜的忠实 canonical 数据镜像：Provider 原始响应按日期批量采集并原样保存 Raw CSV，经最小必要标准化和逻辑 snapshot 重建形成 Clean CSV，再以整个日期批次为单位一次连接、一次事务原子写入 DuckDB；调用属于日常采集、初始化、补跑还是历史重跑，由调度层决定，与数据 Pipeline 本身无关。**
