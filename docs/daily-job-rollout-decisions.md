# 日常 Job 分组处理决策单（临时）

> 状态：已按确认方案完成源码、测试、部署侧清单和生产 scheduler 重载；今日 08:00/08:10 新增基础资料 Job 已按目标日手动处理，`index_basic_update` 成功，`stock_basic_update` 因源数据质量失败且未写入，暂不重试。
>
> 规则：本文件记录的确认方案已执行到源码、测试、部署侧清单和 scheduler 重载；本次不执行历史回补、不执行手工初始化、不自动补跑遗漏的 08:00/08:10 业务 Job。

## 总原则

- 本次只建立日常更新，不做历史回补；历史回补另行安排。
- 基础资料按当前状态资料维护：成功拉取的新快照应完整替换旧状态；校验或写入失败时保留旧数据。
- 日频行情、事件和快照表按目标交易日替换，历史交易日保留。
- 逻辑相近的能力合并为一组确认，调度时间统一使用 `Asia/Shanghai`。

## 分组状态

| 组 | 能力范围 | 状态 |
| --- | --- | --- |
| 1 | 基础资料与 ETF 日常快照 | 已实施；今日 index 成功，stock 源数据质量失败未写入 |
| 2 | Tushare 日频事件快照 | 已实施，scheduler 已重载 |
| 3 | 行业成员与指数成分 | 已冻结，不建立日常 Job |
| 4 | PIT、基本面与业绩预告 | 已实施，scheduler 已重载 |
| 5 | System B 及其他未调度能力 | 已实施，scheduler 已重载 |

## 第 1 组：基础资料与 ETF 日常快照

### 已确认

- 建立四项日常更新：
  - `index_basic_update`
  - `stock_basic_update`
  - `etf_daily_update`
  - `etf_adj_factor_update`
- 仅建立日常更新，不做历史回补。
- 建议日常时间（工作日，`Asia/Shanghai`）：
  - `index_basic_update`：08:00
  - `stock_basic_update`：08:10
  - `etf_daily_update`：16:30
  - `etf_adj_factor_update`：16:45，依赖 `etf_daily_update`
- `index_basic_update` 必须在执行阶段改为 `FULL_REBUILD`：完整快照校验通过后，在一个事务内清空并重写 `index_basic`；空响应或失败不得清空旧状态。
- `stock_basic_update` 源码已经是 `FULL_REBUILD`，每次完整快照成功后替换 `stock_info`。
- ETF 两项是按目标交易日 `REPLACE_TARGET_DATE`，不删除历史日期；复权因子依赖 ETF 日行情。

### 实施结果

- `index_basic_update` 已改为完整快照校验通过后事务内 `FULL_REBUILD`；写入失败保留旧状态。
- 四项日常 Job 已加入并启用生产清单；部署侧清单通过校验。
- 源码 Contract、Job 定义、依赖图和临时数据库测试已通过；未执行首轮业务 Job。

## 第 2 组：Tushare 日频事件快照

### 待确认能力

- `limit_step_ingest`：连续涨停梯队快照，按目标日期或日期范围替换。
- `ths_daily_ingest`：同花顺板块指数日频快照，按目标日期或日期范围替换。
- `stk_high_shock_ingest`：股票严重异常波动公告快照，按目标日期或日期范围替换。

### 建议日常方案

- 三项都按工作日日更，只使用调度当天的目标交易日，不配置历史日期范围参数。
- 建议安排在收盘后错峰执行：
  - `limit_step_ingest`：16:40
  - `ths_daily_ingest`：16:50
  - `stk_high_shock_ingest`：17:00
- 三项均写入 `quant.db`，按目标日期完整替换；目标日确实无数据时保留空快照语义，已有目标数据遇到空响应时失败并保留旧数据。

### 已确认

- 三项全部纳入日常更新。
- 接受工作日 16:40、16:50、17:00 的错峰时间。

## 调度波次方案讨论（未定案）

用户提出将任务统一在 09:00（盘前）和 15:15（盘后）入队，再由队列逐项处理。本方案当前只作为讨论记录，不覆盖第 1、2 组已经确认的时间。

### 当前队列能力可以做到的部分

- 同一时刻可以创建多个到期 Job，服务会从队列中领取并执行。
- 当前服务配置最多 4 个 worker；正式 Contract 在同一服务进程内执行。
- 共同写入 `quant.db` 的 Job 都声明 `quant_db_writer`，运行库会阻止并发写入，因此同一波次最终会串行写库。
- 已声明的 Contract 依赖会自动形成阻塞和释放关系；但同一时刻、没有业务依赖的 Job 不保证固定的抢锁顺序。

### 当前语义下的限制

- 09:00 适合 `index_basic_update`、`stock_basic_update` 这类状态资料。三个 Tushare 日频事件 Contract 在没有日期参数时直接使用调度日，09:00 请求当天通常尚未形成完整数据，不能把成功的空响应当作当天已更新。
- 15:15 对市场行情和 ETF Contract 仍处于当前代码的 16:00 cutoff 之前，会解析为上一个交易日；如果目标是当天收盘数据，现行语义应从 16:00 之后开始，且还要考虑 provider 数据就绪时间。
- 如果需要严格的“逐项”顺序，不能只把 Job 设成同一时刻；应继续使用错峰时间，或新增明确的编排依赖。当前 Production Job 清单本身不承载任意 Job 间依赖，依赖来自 Contract 的业务声明。

### 当前判断

保留已确认的错峰方案更符合现有日期语义：盘前只安排确实可在盘前完成的状态资料，收盘数据从 16:00 之后按数据就绪和资源锁安排。09:00/15:15 波次方案若要采用，需要先单独确认每项能力的目标日期语义、数据就绪时间和严格顺序要求。

## 第 3 组：行业成员与指数成分

### 源码事实

- 来源时间：底层 PIT 数据管线于 2026-07-13 的 `95e7ca1` 引入；当前两条正式 Contract 于 2026-08-02 18:25 的 `95a80f0` 完成合同化。它们不是最近 12 小时新增的能力。
- 当前状态：源码 catalog 可发现，但没有 Production Job 实例，也没有生产日常调度；本次只是把它们纳入待确认范围。
- `industry_membership_ingest` 是 PIT 历史追加能力，必须指定股票代码或一个申万行业代码范围；`is_new=Y/N` 只是可选的 provider 过滤，不能替代业务范围。调度日期只是观察标签，provider 请求本身不按调度日期过滤。
- `index_component_ingest` 是 PIT 成分权重历史追加能力，必须指定指数代码、`start_date` 和 `end_date`；当前没有默认指数集合，也不能仅靠调度日期推导来源范围。
- 两项都使用 `APPEND` 和 `revision_id` 幂等追加，重复拉取不会覆盖历史版本。

### 用户决定

- 不需要这两项数据，冻结。
- 不建立日常 Job，不做历史回补，不启动数据生产。
- 源码 Contract、表结构和已有历史数据暂不删除；后续如需恢复，另行解冻。

## 第 4 组：PIT、基本面与业绩预告

### 能力事实

- `pit_backfill`：显式、可恢复的 PIT 历史批次编排，覆盖财务、行业和指数数据；本身是历史回补能力，不适合作为日常 Job。
- `fundamentals_ingest`：写入 `income_statement`、`balance_sheet`、`cashflow_statement`、`financial_indicator`，现以公告日模式按 `ann_date` 增量拉取，保留报告期和修订字段。
- `earnings_forecast_ingest`：写入 `earnings_forecast_event`，现以公告日模式按 `ann_date` 增量拉取；不把公告日误当作报告期。

### 已确认方案

- `pit_backfill` 保持手工历史回补，不建立日常 Job。
- `fundamentals_ingest` 建立日常更新，但不能把调度日期直接当作 `period`。增加 `ann_date` 日增量模式：用调度日生成公告日查询范围，四个接口按公告日拉取；从返回记录的 `end_date` 读取真实报告期，再将记录写入对应财务表，保留 `ann_date`、`f_ann_date`、可用时间和修订版本。
- 为处理 provider 延迟、补发和修订，公告日查询采用以基准日 `D` 为中心的三个自然日窗口：`D-1`、`D`、`D+1`。报告期滚动集合只能作为接口不支持全市场公告日过滤时的兜底方案，不能替代公告日增量。
- `earnings_forecast_ingest` 按调度日读取公告日增量，建立工作日日更 Job；写入仍为 PIT `APPEND`，不覆盖披露修订。

### 接口验证与实施结果

- 已使用部署侧同一套 Tushare API 配置进行只读验证，不调用 Job、不写数据库。以 `ann_date=20240315` 为样例，四个 VIP 接口均在不传 `ts_code` 时接受请求；限制返回 3 行时，均返回多只股票，并带有 `ann_date`、`end_date` 等关键字段。
- 因此四个 VIP 接口可以作为全市场公告日增量入口：`income_vip`、`balancesheet_vip`、`cashflow_vip`、`fina_indicator_vip`。
- 返回记录中的 `end_date` 才是实际报告期；`ann_date` 是本次查询条件，`f_ann_date` 可能与其不同，三者必须分开保留，不能用调度日覆盖。
- 已用同样方式验证 `forecast(ann_date=20240315)`：不传 `ts_code` 时返回多只股票，说明业绩预告底层也具备全市场公告日增量能力；返回记录可能包含多个 `end_date`，不能把公告日当作报告期。
- Contract 已自动从上海执行日计算 `D-1,D,D+1` 三个自然日，生产 Job 只固定 `mode=ann_date`，本次未执行历史回补。

### 用户确认

- `fundamentals_ingest` 和 `earnings_forecast_ingest` 均建立日更。
- 两项均采用基准日 `D-1`、`D`、`D+1` 三个自然日的公告日窗口。

## 第 5 组：System B 及其他未调度能力

### 能力事实

- System B 是一条有先后关系的计算链：`system_b_state_readiness`（只读前置检查）→ `system_b_state_daily`（写入 `quant.db` 状态表）→ `system_b_episode_rebuild`（重建独立 episode 库）→ `system_b_pool_height`、`system_b_pool_capacity`、`system_b_pool_recognition`（分别生成三个股票池）。
- `system_b_state_initialize` 只用于首次初始化、历史回补或恢复，属于手工操作，不应设置为日常 Job。
- `system_b_pool_completeness_daily` 只是计划中的只读检查：确认同一交易日的 HEIGHT、CAPACITY、RECOGNITION 三个池是否都有 `COMPLETED` 记录，不生成业务数据。当前没有正式执行入口；本轮不把未实现能力伪装成日常 Job。
- System B 的真实输入包括 `stock_info`、交易日历、日行情、复权因子和 `suspend_d`。当前生产已有 `suspend_d` 09:15 日更，但 System B 注册表中的 `suspend_d` 新鲜度检查仍需在首轮正式运行前完成实际验收；本次只完成合同和清单接入，未执行业务 Job。
- System B 生产清单现已加入 readiness、state、episode 和 HEIGHT/CAPACITY/RECOGNITION 五类日常 Job；独立 episode/pool 数据库路径已由生产 env 配置并由 AppSettings 解析确认。

### 实施结果

- 整条计算链已纳入工作日日更：readiness → state → episode → HEIGHT/CAPACITY/RECOGNITION；三个 pool 共享写锁，并依赖 episode Contract。
- 实际生产 schedule 为 18:30、18:40、18:50、19:00、19:10、19:20（Asia/Shanghai）；三个 pool 的错峰只作通常顺序，Contract 依赖仍是权威约束。
- `initialize` 保持手工；完整性检查等 PLANNED 能力本轮不建立。
- 首次 episode/pool 有效运行前仍需另行批准并执行历史初始化；本轮不执行该初始化，不能以清理 JobRun 或直接写库绕过前置。

### 用户确认与实施状态

- 已确认：System B 的 state、episode 和三个股票池全部建立工作日日更。
- 已确认：`system_b_state_initialize` 保持手工，`system_b_pool_completeness_daily` 本轮暂不建立。
- 已实施：6 个 System B 日常 Contract 已注册，生产清单已加入对应 Job；scheduler 已于 11:09 重载，后续时点按新清单执行。

## 实施门禁

全部五组已确认，以下步骤解除门禁；本轮仍不执行历史回补。

全部分组确认后按以下顺序执行；本次全部完成，08:00/08:10 补跑单独留待确认：

1. 更新 Contract 及对应测试。
2. 更新仓库内生产 Job 示例/定义和部署侧清单，保留变更前备份。
3. 在临时数据库、mock 外部接口和隔离运行库中测试。
4. 校验正式 Contract、Job 定义、依赖图、唯一 scheduler lease 和无运行中的目标 Job。
5. 部署并重载唯一生产调度服务，验证首轮计划和 Job 结果；本次已于 11:09 完成重载并通过健康检查，未执行历史回补、手工初始化或遗漏 Job 补跑。

## 实施验收记录

- 源码 Contract 校验：28 条通过。
- 生产 Job 清单校验：29 个启用 Job 通过。
- 全量测试：1191 passed，1 个既有 Python 3.14 依赖弃用警告；测试使用仓库外隔离临时目录，未写入生产数据库。
- 重载前生产健康检查：`HEALTHY`，`pending_runs=0`，`running_runs=0`，`last_error=null`。
- 变更前生产清单回滚备份：`/home/claire/apps/qrp-atlas/backups/20260805_daily-contract-rollout/pre-change-production-job-definitions.json`。
- 生产服务已切换到新进程 PID 545190；重载后 health 通过。今日目标日手动结果：`index_basic_update` 成功写入 12,456 行；`stock_basic_update` 因 `market` 空值触发 `STOCK_BASIC_API_PARTIAL`，`rows_written=0`，旧状态保持，未重试。其它新增链路等今日完整调度结束后统一评估。
