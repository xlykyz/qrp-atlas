# System B Pool 性能回归调查报告

> 状态：**已定位；修复未实施**（生产侧已做临时规避）
> 发现日期：2026-09-11 凌晨（episode 事故恢复的补跑验证过程中）
> 关联文档：[SystemB每日链事故根因分析](./SystemB每日链事故根因分析_20260911.md)（同一"重启激活"模式）
> 调查性质：只读排查 + 已授权的生产规避操作（停用 pool job）；修复代码未动

---

## 1. 现象

episode 补跑成功后（09-10 数据，SUCCESS），按短期方案顺序补跑 `system-b-pool-height-daily`，该任务**卡死**：

| 观测项 | 值 | 对照 |
| --- | --- | --- |
| 运行时长 | **> 27 分钟未完成**（最终被人工中止） | 历史三次稳定 **71–72 秒**（09-07/08/09） |
| 进程 CPU | 101%（单核满载） | 正常 |
| 内存 | 9.4 GB 且缓慢增长 | 正常量级（输入含 19.6M + 18.7M 行） |
| job 心跳 | 被拖慢至 **417 秒**未更新（正常为秒级） | GIL 被计算线程长期占用 |
| `hard_timeout`（1200s） | **超时后仍 RUNNING** | 协作式超时，executor 内无检查点则不触发 |
| 中止方式 | SIGTERM 无效 → SIGKILL → 系统自动标注 | `FAILED / stale heartbeat recovery` |

**已排除的假设**（均有实测证据）：JOIN 基数爆炸（JOIN 结果 = observation 行数 7,987,683，无放大）、库文件膨胀（主库 10.5 GiB、空闲块 11.7%）、swap/内存压力（`VmSwap=0`，可用内存 37 GB）、数据量异常（episode 858,673 / observation 7,987,683，均正常）、CPU 外部竞争（无其他重负载进程）。

## 2. 定位：对比测量（决定性证据）

| 加载路径 | 实现方式 | 实测耗时 |
| --- | --- | --- |
| **旧版**（`167b1af`，08-30 生产加载） | **纯 SQL JOIN**（DuckDB 原生执行） | **1.1 秒**（19,610,480 行） |
| **现行**（Task06 之后） | pandas 全量加载 → Python 处理 → `con.register` 回 DuckDB → JOIN | **超过 2 分钟未完成**（只读测量被超时终止；生产实测 27+ 分钟） |

## 3. 根因

`system_b_pools/service.py` 的 `_load_market_panel` 在 Task06 中被改写：输入加载从"纯 SQL 直接 JOIN"变为先调用
`system_b/market_series.py::load_canonical_market_series`（pandas 全量加载 + 处理 + 注册回库）。
该函数内有**两处 Python 级循环**：

1. `state_frame.groupby(["asset_id", "trade_date"])` → **约 18,687,257 个分组**（每组仅 1 行）**逐组 Python 迭代**；
2. `for row in frame.itertuples(index=False)` → **19,610,480 行逐行 Python 迭代**；

**合计约 3,800 万次 Python 级迭代**（而旧路径是一条 1.1 秒的 SQL）。

> 注：该改写的**目的正当**（源码注释：让 pool 的 OHLC 值来自与 System B state 相同的 target-normalised 复权序列），
> 问题仅在于**实现方式**引入了不可接受的性能成本。

## 4. 引入与生效时间线

| 时点 | 事件 |
| --- | --- |
| **09-06** | 提交 `8e0b72e`（"feat(task06): add asset relative ranking pipeline"）新增 `market_series.py`（399 行，旧代码中不存在，经 `merge-base --is-ancestor` 验证）并改写 pool 加载路径 |
| 08-30 → 09-10 | 生产运行旧代码（`167b1af` 时代），pool 每日 71 秒正常 |
| **09-10 05:22** | 生产重启加载 `d991880` → **首次包含 Task06** |
| 09-10 19:00 | 定时 pool-height **BLOCKED**（上游 episode FAILED）→ **新代码未真正运行过** |
| **09-11 凌晨** | episode 修复后补跑 pool-height → **新代码首次真实运行** → 卡死暴露 |

**与 episode 事故同一模式**：09-10 的重启一次性激活了多个"已合入但从未运行"的变更（segment refinement、Task06 pool 加载改写）。

## 5. 生产处置（已完成，2026-09-11 凌晨）

1. **停用 3 个 pool job**：`system-b-pool-height-daily`、`system-b-pool-capacity-daily`、`system-b-pool-recognition-daily` → `enabled: false`
   - 修改 `QRP_HOME/pipeline/` 下 `production-job-definitions.json` 与 `.candidate.json`（两份同步）；
   - 备份：`backups/production-job-definitions.pre-pool-disable-20260911-0419.json`；
   - 文件校验：`validate-job-definitions` 通过（29 条 = 26 enabled + 3 disabled）。
2. **重启 `qrp-atlas-jobs`** 使新清单生效（04:23:10 CST），验证：服务正常、心跳/游标推进、manifest 生效（改于 04:19 < 启动于 04:23）。
3. 效果：**今晚 19:00 起 pool 链不再调度**（避免无人值守时再次卡死）。

**未触碰**：其他 26 条 job（含 `zt-dt-pool-close`——东财涨跌停池，与 System B pool 无关）、其余调度配置、生产数据。

## 6. 影响与当前状态

| 面 | 状态 |
| --- | --- |
| pool 数据 | 停用期间不再更新；09-10 的 pool 数据未产出 |
| 下游依赖 | System B pool **无下游任务**，影响范围限于 pool 自身 |
| episode 链 | 正常（修复已上线，09-10 补跑成功） |
| 仓库镜像 | `deploy/pipeline/production-job-definitions.json` **尚未同步**本次停用（属治理分支工作，已暂停） |

## 7. 修复方向（候选，均未实施）

| 方案 | 说明 | 备注 |
| --- | --- | --- |
| a. **向量化**（推荐） | 将 `load_canonical_market_series` 的两处循环改为 pandas/DuckDB 向量运算（groupby→merge/map；itertuples→列运算） | 保留 Task06 的复权口径统一目的 |
| b. 回退加载路径 | pool 走回纯 SQL JOIN（旧路径 1.1 秒） | 会丢失"OHLC 与 state 同口径"的改进，需评估业务影响 |
| c. 缩小加载范围 | 只加载必要日期窗 | 现有注释说明需保留历史（供 shift/rolling），需先验证可行性 |
| d. 加固超时 | 在长循环中加入 `execution_control.check()` 检查点 | 防御性，避免"超时未生效"重现；与 a/b 可并行 |

## 8. 遗留事项

- [ ] pool 加载路径的性能修复（走 dev 分支 + PR 流程）；
- [ ] 修复上线后：恢复 3 个 pool job 的 `enabled: true`，并同步仓库镜像；
- [ ] 09-10 的 pool 数据补跑（修复后）；
- [ ] 本缺陷补入事故报告（作为独立于 episode 事务问题的第二个缺陷）；
- [ ] `hard_timeout` 对无检查点 executor 的失效问题（方案 d）另案处理。

## 9. 附录：复核方法（只读）

```bash
# ① 历史耗时对照（job 运行账本）
#    表 job_run：job_id='system-b-pool-height-daily'，比较 wall_duration_ms
# ② 旧/新加载路径的耗时对比（只读）
#    旧路径等价 SQL：SELECT COUNT(*) FROM daily_market_snapshot m
#                    LEFT JOIN daily_basic b ON b.trade_date=m.trade_date AND b.ticker=m.ticker
#                    WHERE m.trade_date <= DATE '2026-09-10';      -- 实测 1.1s
#    新路径：load_canonical_market_series(con, end_date)            -- 实测 >120s 未完成
# ③ 代码证据
#    git diff 167b1af..HEAD -- src/qrp_atlas/pipeline/system_b_pools/service.py
#    git log --oneline -- src/qrp_atlas/pipeline/system_b/market_series.py
```

---

*本调查在排除多个假设后经对比测量定位到加载路径；修复方案尚未实施，生产当前以"停用 pool job"临时规避。*
