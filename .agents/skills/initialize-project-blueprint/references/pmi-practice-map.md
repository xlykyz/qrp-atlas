# PMI/PMP 实践映射

## 用途

本文件用于在生成项目蓝图时选择合适的项目管理工件。它不是 PMP 考试教材，也不要求机械套用全部过程。

## 当前标准背景

PMI 已发布《PMBOK® Guide》第八版。该版本继续采用原则和绩效域思路，强调价值交付、适应性与责任，同时提供更可操作但非强制的过程指导。

本 Skill 采用“以价值和结果为中心、按项目情境裁剪”的方式，不把 PMBOK 当成固定瀑布模板。

## 蓝图成果与 PMP/PMI 工件对应

| 蓝图成果 | 项目管理对应概念 | 实际作用 |
|---|---|---|
| 项目章程与启动决策 | Project Charter | 授权项目、定义价值、角色、约束和高层范围 |
| 当前能力审计 | Current State Assessment | 确认真实起点，防止按假设规划 |
| 产品终点 | Product Scope / Project Scope Statement | 定义最终用户结果、能力和排除项 |
| 需求文档 | Requirements Documentation | 将业务目标转成可验证需求 |
| 需求追踪矩阵 | Requirements Traceability Matrix | 建立需求到交付、测试和验收的链路 |
| 架构与模块边界 | Solution / Technical Planning | 定义实现约束、责任和依赖 |
| 开发路线图 | Milestone Roadmap / Dependency Network | 表达逻辑顺序、里程碑和前置 |
| WBS | Work Breakdown Structure | 以交付物分解批准范围 |
| 工作包文档 | WBS Dictionary / Work Package Definition | 描述每个 WBS 元素的范围、输入、输出和验收 |
| 总体验收清单 | Quality Baseline / Acceptance Criteria | 定义 Done 和发布门禁 |
| 风险登记 | Risk Register | 记录风险、触发器、责任人和响应 |
| 范围基线 | Scope Baseline | 产品范围说明 + WBS + WBS 字典 |
| 变更控制 | Integrated Change Control / Configuration Control | 防止未经批准的范围漂移 |
| PR 审查 | Configuration and Quality Gate | 固化交付物版本、审查与批准证据 |

## 关键原则

### 价值优先

项目不是为了增加模块数量，而是为了交付可验证的业务价值。产品终点和验收场景必须从使用者结果出发。

### WBS 是“交付什么”，不是“怎么做”

WBS 应以名词性、交付物导向的结构组织总范围。活动、命令和编码步骤属于工作包之下，不应代替 WBS。

### 100% 规则

WBS 必须覆盖批准范围的全部交付物，包括项目管理、测试、迁移、文档、运维和发布工作；同层元素不应重复计算范围。

### 范围基线批准后受控变化

蓝图允许在批准前逐步细化；批准后，改变交付能力、工作包或验收门禁的事项必须走变更控制。

### 进度路线图不等于进度基线

只有当工作量、资源、依赖和工作日历已经估算，日期才可成为正式进度承诺。否则只表达顺序和里程碑。

### 需求必须可追踪

正式需求应能够追踪到工作包、实现、测试和验收证据。未形成证据链时，不应标记为完成。

### 按情境裁剪

个人项目可以一人兼任 Sponsor、Product Owner、Project Manager 和验收人，但应保留角色和批准记录。小项目可以合并文档，但不能丢失其管理逻辑。

## 对软件与 AI Agent 项目的补充裁剪

必须额外关注：

- 代码与文档真实状态不一致；
- Agent 可能过度扩展范围；
- 中间 MVP 被误认为最终产品；
- 生成代码缺乏验收和运行证据；
- 版本兼容、数据库迁移和回滚；
- 提示词、模型和外部 API 的不稳定性；
- 数据时间可用性和未来信息泄漏；
- 自动执行的权限边界、幂等和恢复；
- 多 Agent 并行导致依赖和分支冲突。

因此默认采用：

```text
独立蓝图 PR
→ 范围批准
→ 串行主线工作包
→ 每包独立验收
→ 统一发布候选验收
```

## 官方参考

- [PMBOK® Guide](https://www.pmi.org/standards/pmbok)
- [Practice Standard for Work Breakdown Structures](https://www.pmi.org/standards/work-breakdown-structures-third-edition)
- [PMI：WBS 概念与质量](https://www.pmi.org/learning/library/practice-standard-work-breakdown-structures-8063)
- [PMI：Requirements Management](https://www.pmi.org/learning/library/project-requirements-management-process-groups-6599)
- [PMI：Scope Management](https://www.pmi.org/learning/library/scope-management-9099)
