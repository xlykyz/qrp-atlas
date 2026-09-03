# Task 04-B1 Walkthrough (M5 Popularity Data Foundation)

## 1. 概述与核心成果

Task 04-B1 按照权威设计书 `docs/QRP产品蓝图v1.1/task04/Task04-B1 M5 人气数据底座设计书.md`，完成了东方财富人气榜 (`dc_hot`) 与同花顺热股榜 (`ths_hot`) 两套正式 canonical 数据底座能力的落地，并通过了架构审查与收口。

### 核心设计与契约基准
1. **正式 Canonical 双榜契约**：
   - `dc_hot`：东方财富人气榜数据底座，主键 `(trade_date, snapshot_seq, rank_position)`。
   - `ths_hot`：同花顺热股榜数据底座，主键 `(trade_date, snapshot_seq, rank_position)`，额外承载 `hot`, `concept`, `rank_reason`。
   - 纳入 `contracts.schema.ALL_TABLES`，标准字段全面纳入 `PRICE_FIELDS` 与 `NUMERIC_FIELDS`。
2. **固定 Provider 调用参数**：
   - 东方财富：`client.dc_hot(trade_date=YYYYMMDD, market="A股市场", hot_type="人气榜", is_new="N")`
   - 同花顺：`client.ths_hot(trade_date=YYYYMMDD, market="热股", is_new="N")`
3. **Raw means raw 原样持久化**：
   - 逐日拉取后原样拼接为一个 Raw DataFrame，不得增删、重命名或做类型转换，原子写入 Raw CSV。
4. **Batch 内 Raw Provider Schema 一致性防御**：
   - 日期范围拉取时，首个 non-empty response 锁定本次 endpoint/batch 的原始列结构与顺序；
   - 后续日期若发生字段增加、遗漏或重排，立即 fail-closed 抛出 `PROVIDER_SCHEMA_DRIFT`（映射为 `API_PARTIAL`），禁止 `pd.concat` 静默吞掉字段漂移。
5. **多快照逻辑重建与秒级抖动平滑**：
   - 依据 `(rank_position, source_rank_time, _orig_idx)` 稳定时序切分重构 `snapshot_seq`；
   - 每个 snapshot 严格要求 `row_count=100`、`distinct ticker=100`、`distinct rank=100` 且连续覆盖 `1..100`；
   - 跨快照时序严格单调递增：`snapshot[n].snapshot_completed_at < snapshot[n+1].snapshot_started_at`；任一异常立即 fail-closed。
6. **日期范围作为单 Batch 原子持久化**：
   - N 次逐日请求 → 1 个 Raw Dataset → 1 个 Clean Dataset → 1 次 DuckDB 事务；
   - 严格落实 `api_requests = 实际请求天数`、`batches = 1` 的 PipelineMetrics 语义；
   - 单连接单事务原子替换，前置执行“空响应防破坏既有历史”检查。
7. **架构统一与无感迁移**：
   - 移除了冗余的 popularity 专用 migration 工具；
   - 深度复用 `contracts.init_database` 与统一主库迁移入口 `scripts/migrate_canonical_schema.py`；
   - Pipeline 保留现有 Tushare snapshot 体系的 `CREATE TABLE IF NOT EXISTS` 运行时自愈兜底机制。

---

## 2. 变更文件清单

### A. Contracts 契约层
- `src/qrp_atlas/contracts/fields.py`：新增人气榜字段常量并纳入 `PRICE_FIELDS` 与 `NUMERIC_FIELDS`。
- `src/qrp_atlas/contracts/schema.py`：声明 `DC_HOT` 与 `THS_HOT` 表契约，主键统一为 `(trade_date, snapshot_seq, rank_position)`，注册至 `ALL_TABLES`。
- `src/qrp_atlas/contracts/mappings.py`：定义 `TUSHARE_DC_HOT` 与 `TUSHARE_THS_HOT` 字段映射，注册至 `SOURCE_MAPPINGS`。
- `src/qrp_atlas/contracts/__init__.py`：统一导出所有新字段、表契约与映射。

### B. DDL 与支持实现
- `deploy/duckdb/005_popularity_dc_ths_hot.sql`：两张人气表的标准 DDL。
- `src/qrp_atlas/pipeline/popularity_support.py`：人气数据核心支持层，包含固定参数请求、Batch 级 Raw Schema 一致性防漂移、Raw means raw 原子保存、时序快照重建与质量验证、单事务原子替换及质量检查器。

### C. Pipeline 合约与编排注册
- `src/qrp_atlas/pipeline/dc_hot_contracts.py`：正式声明 `DC_HOT_INGEST` (`dc_hot_ingest`)，度量语义为 `batches=1`。
- `src/qrp_atlas/pipeline/ths_hot_contracts.py`：正式声明 `THS_HOT_INGEST` (`ths_hot_ingest`)，度量语义为 `batches=1`。
- `src/qrp_atlas/pipeline/contract_catalog.py`：将两个新契约模块注册到 `CONTRACT_MODULES`。

### D. 自动化测试套件
- `tests/pipeline/test_popularity_contracts.py`：16 个综合验收测试，完整覆盖单日、日期范围单 Batch 处理、固定参数、Raw 原样性、快照抖动重构、Top100 完整性保护、时序单调保护、空响应防篡改保护、幂等替换、事务回滚、Schema 跨日漂移拦截以及度量指标断言。
- `tests/contracts/test_schema_contracts.py`：增加对 `dc_hot` 与 `ths_hot` 的已知主键、DDL 可执行性断言。
- `tests/pipeline/test_pipeline_contract.py`、`test_market_data_contracts.py`、`test_irm_contracts.py`、`test_production_jobs.py`：同步维护正式契约注册总数（29 -> 31）与契约 ID 集合。

### E. 保留但备注的 Windows 开发环境兼容修复
- `src/qrp_atlas/orchestration/definitions.py`：使用 `PurePosixPath` 兼容 Windows 下 POSIX 绝对路径判定。
- `src/qrp_atlas/pipeline/system_b/service.py`：Windows 缺少 Unix `resource` 模块时允许安全 import，内存指标降级为 `0.0`。

---

## 3. 验证结果

所有测试均在本地 Windows 开发环境下实测通过：
1. **人气榜 Pipeline 专项测试**：
   - 命令：`pytest tests/pipeline/test_popularity_contracts.py -v`
   - 结果：`16 passed in 4.18s`。
2. **Contracts / Schema 相关测试**：
   - 命令：`pytest tests/contracts/ -v`
   - 结果：`30 passed in 0.64s`。
3. **正式 Pipeline Contract 校验**：
   - 命令：`pytest tests/pipeline/test_pipeline_contract.py tests/pipeline/test_market_data_contracts.py tests/pipeline/test_irm_contracts.py tests/pipeline/test_production_jobs.py -v`
   - 结果：`141 passed in 19.50s`。
4. **全仓全局回归测试**：
   - 命令：`pytest`
   - 结果：`1323 passed, 3 skipped in 247.88s`（零失败、零回归）。

---

## 4. 生产环境部署操作指引（仅备忘）

根据 `AGENTS.md` 边界约束，本机仅负责代码开发与本地验证。后续代码同步至 Linux 生产节点后：

1. **代码同步**：无新增第三方 pip 依赖。
2. **主库迁移**：
   利用既有正式主库迁移工具安全建表：
   ```bash
   python scripts/migrate_canonical_schema.py --env-file /etc/qrp-atlas/qrp-atlas.env --apply
   ```
3. **契约验证**：
   ```bash
   qrp-pipeline validate-contracts
   ```
   预期输出：`valid contracts: 31`。
