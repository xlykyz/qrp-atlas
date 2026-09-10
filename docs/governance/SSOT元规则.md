# QRP Atlas SSOT 元规则

> 文档性质：治理元规则（活文档）
> 目的：回答一个固定问题 —— **某一类事实发生冲突时，以什么为准？**
> 生效日期：2026-09-11
> 范围：只登记"哪个维度由谁负责"与冲突裁决顺序；**不复制任何业务内容、字段清单或规则值**
> 关联文档：[QRP治理体检台账](./QRP治理体检台账.md)（B-02）、[生产环境实测调研报告](./生产环境实测调研报告.md)

---

## 0. 本文解决什么问题

治理体检台账 B-02 记录了"至少四种'唯一事实来源'主张并存，且从未说明它们分属不同维度、也无优先级"。本文件按**维度**登记权威归属，消除"同一维度多个文件都自称权威"的歧义：

| 台账记录的并存主张 | 实际所属维度 | 在本文件中的归位 |
| --- | --- | --- |
| 业务规则 SSOT = `MyTradingSystem`（跨仓） | 维度 D7：System B 业务语义 | 权威（跨仓） |
| 数据定义 SSOT = `contracts/` 3 个核心文件 | 维度 D1：持久化 schema / 字段 / PK / PIT | 权威（本仓） |
| Dual SSOT = `quant.db` + `canonical/**` | 维度 D9：运行数据实例与恢复源 | 权威（数据实例层，非定义层） |
| Pipeline 业务语义 SSOT = 源码 `PipelineContract` | 维度 D2：Pipeline 业务 Contract | 权威（本仓） |

四条主张**分属四个不同维度，并不互相冲突**；冲突只发生在被误读为同一维度时。

---

## 1. 元规则（R1–R5）

**R1 单一权威**：每个事实维度有且只有一个权威。同名维度的第二份"权威声明"必须被降级为派生、示例或历史。

**R2 权威分层**：定义层（schema、Contract、规则）与实例层（某台机器上的实际数据、运行记录）必须分开。实例层是**事实**而非**定义**：生产库实际表结构是事实，`contracts/` 才是定义；两者不一致按缺陷处理，不改变权威归属。

**R3 派生必须显式**：任何从权威派生的文件（整理稿、盘点表、迁移 DDL、镜像副本）必须能从**名称或文档头部**看出其派生关系，不得自称"唯一权威"。

**R4 历史保留但标注**：过期文档不删除，但必须标明状态（现行 / 封版 / 历史 / 示例 / 派生），不得让读者误认为当前事实。

**R5 冲突裁决顺序**：
1. 同一维度内冲突 → 以该维度权威为准；
2. 定义与实例冲突 → 以权威定义为正确，实例按缺陷修复；
3. 文档相互冲突 → 现行 > 封版 > 历史；同层级时以更接近机器可读事实者为准；
4. 跨仓冲突（System B 业务规则）→ 回 `MyTradingSystem` 裁决，本仓不得自行裁决或默认补齐。

---

## 2. 维度登记表

| # | 事实维度 | 唯一权威 | 派生 / 镜像（非权威） | 历史 / 示例（不得当作现状） |
| --- | --- | --- | --- | --- |
| D1 | 持久化 schema / 字段 / 主键 / PIT 语义 | 本仓 `src/qrp_atlas/contracts/`（`fields.py` / `schema.py` / `mappings.py` / `conventions.py`） | `deploy/duckdb/*.sql`（迁移 DDL，由 `tests/contracts/test_schema_contracts.py` 校验与 contracts 字段一致）；`docs/ssot_data_model.md`（自动整理总览） | 早期 PIT 迁移脚本（`scripts/migrate_*.py`）、`docs/仓库历史设计稿` |
| D2 | Pipeline 业务 Contract（输入输出 / 依赖 / 幂等 / 事务 / 完成 / 质量 / 超时 / 重试） | 本仓源码 `PipelineContract`（`src/qrp_atlas/pipeline/**` 中 `register_pipeline()` 注册的契约对象） | `deploy/pipeline/pipeline-registry.json`（全量盘点与迁移状态；**只记录、不覆盖源码规则**） | 各 Task 设计书中的早期契约草稿 |
| D3 | Contract discovery / registration | 本仓 `src/qrp_atlas/pipeline/contract_catalog.py::CONTRACT_MODULES`（显式清单） + `pipeline/registry.py` | `qrp-atlas-jobs list-contracts` 输出（运行时视图） | 无 |
| D4 | production Job definition / schedule / enabled / 固定参数 | 生产运行：部署目录的 `pipeline/production-job-definitions.json`（由 systemd unit 传给 `qrp-atlas-jobs serve`）；本仓版本化镜像：`deploy/pipeline/production-job-definitions.json`（须与生产逐字节一致） | 运行时解析视图：`qrp-atlas-jobs list` / plan 输出 | `deploy/pipeline/*.example.json`（格式示例，全 disabled）；`deploy/pipeline/pipeline-definitions.shadow.json`（shadow Foundation，未启用、不被 scan/runner 使用） |
| D5 | Job runtime actual state（run / 结果 / 心跳 / 租约） | 运行实例：`job_runtime.sqlite3`（由 `QRP_JOB_RUNTIME_DB_PATH` 指定位，**不入 git**） | 无 | 无（仓库内不得存在运行状态副本） |
| D6 | 架构依赖规则 | 本仓 `src/qrp_atlas/AGENTS.md`（及模块级 AGENTS.md） | `tools/arch_check.py`（机械化检查 + baseline，见 `tools/arch_check_baseline.json`） | 台账/调研报告中的架构问题描述 |
| D7 | System B 业务语义 | 跨仓 `xlykyz/MyTradingSystem`（锁定 commit + document） | 本仓登记：`docs/QRP产品蓝图v1.1/task00/`（规则登记 / 工程映射 / 版本集与参数集） | 评分公式等未裁决项保持显式阻断，禁止推断 |
| D8 | 设计文档有效性关系 | 现行：`docs/QRP产品蓝图v1.1/`；已封版：`docs/QRP产品蓝图v1.0/`、`docs/核心架构v1.0/`（验收完成，后续功能不得破坏其模块职责与依赖方向） | 治理活文档：`docs/governance/` | MVP 阶段结构文档（`docs/architecture/`）、`docs/` 根目录一次性调研 |
| D9 | 运行数据实例与恢复源 | 运行实例：生产各 DuckDB 数据文件与运行目录（由 `QRP_*` 环境变量指定位，**不入 git**）；恢复源：部署侧 `canonical/**`（按部署流程维护） | 只读审计快照（如《生产环境实测调研报告》中的表清单与行数，仅代表观测时点） | 迁移前遗留的数据副本（如主库内停滞的 `irm_interaction_qa`，见实测报告 §4） |

---

## 3. repo 文件角色登记（`deploy/pipeline/`）

| 文件 | 角色 | 权威性 |
| --- | --- | --- |
| `production-job-definitions.json` | **生产调度实例的版本化镜像**（与生产部署目录文件逐字节一致） | 本仓内的 D4 权威副本 |
| `production-job-definitions.example.json` | 格式示例（全部 `enabled: false`） | 无；不得作为任何部署依据 |
| `pipeline-registry.json` | D2 的派生盘点视图（全量注册表与迁移状态） | 无；不得覆盖源码规则 |
| `pipeline-definitions.shadow.json` | 历史 shadow Foundation（未启用定义） | 无；不被 scan/runner 使用 |

文件级说明另见 [`deploy/pipeline/README.md`](../../deploy/pipeline/README.md)。

---

## 4. 冲突裁决流程

1. **定位维度**：先用第 2 节登记表确定冲突事实属于 D1–D8 中哪个维度；
2. **取权威**：按该维度的权威来源裁决；实例层冲突按缺陷处理（R2/R5.2）；
3. **修歧义源**：若冲突源于某文件自称权威或未标注状态，先修该文件的角色声明（R3/R4），再修内容；
4. **跨仓事项**：涉及 System B 业务规则，转 `MyTradingSystem` 裁决，本仓只登记结论与来源定位。

---

## 5. 维护约定

1. 新增事实维度或权威迁移时，更新第 2 节并注明日期与理由；
2. 权威文件自身变更（如 D4 镜像同步）不受本文约束，按对应流程执行（生产清单变更遵循运维手册流程）；
3. 本文**只登记归属，不登记内容**；任何字段清单、规则值、schedule 明细都应在各自权威文件中维护；
4. 发现"某文件自称唯一权威但不在本表"时，视为治理缺陷，先登记再裁决。
