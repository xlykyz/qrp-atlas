# QRP v1.1 Agent 任务包

> 本文件规定所有本地、云端和第三方 Agent 执行 v1.1 主线任务时的统一规则。  
> 具体任务边界以对应任务文档为准；Agent 不得自行扩大、缩小或重新解释产品目标。  
> qrp-atlas v1.1 长期集成分支为 `develop/v1.1`。

## 一、总则

1. 一次只执行一个任务或一个已经批准的子任务；
2. 先读取本蓝图、仓库根/子目录 `AGENTS.md` 和目标任务文档；
3. 不把“可运行原型”当作任务完成；
4. 不为赶进度跳过 contracts、PIT、版本、owner、失败路径或审计；
5. 不复制已有指标、因子、目标权重、组合、结果、题材设计或认证逻辑；
6. 不把具体策略知识写入通用回测引擎；
7. 不把 QMT/xtquant 类型传播到 `execution`、策略或 contracts；
8. qrp-atlas 不 import xtquant，不直接管理 MiniQMT 生命周期；
9. qrp-trading 不实现策略、评分、目标组合或业务风险政策；
10. 不使用 `eval`、`exec`、任意代码上传或动态不受控导入；
11. 禁止 rebase、force push 和自行合并；
12. qrp-atlas 工作包 PR 目标必须是 `develop/v1.1`；
13. PR 保持未合并，等待独立验收与人工授权。

## 二、开工前必须完成

Agent 必须在任务开始时输出：

- 当前 `develop/v1.1` SHA；
- 当前 `main` SHA及二者关系；
- 工作分支；
- 任务编号；
- 计划修改模块/仓库；
- 明确不修改的范围；
- 前置任务是否完成；
- 读取到的相关 AGENTS 规则；
- 现有可复用实现；
- 契约、PIT、owner、兼容和安全风险；
- 涉及任务10-B时，qrp-trading当前commit/version和协议基线。

若前置任务未完成，应停止实现并报告阻塞，不得用临时实现绕过。

## 三、任务状态总表

| 任务 | qrp-atlas建议分支 | 其他仓库 | 状态 |
|---|---|---|---|
| 00 蓝图与规则治理封版 | `docs/v1.1-wp00-rule-governance` | 无 | 待执行 |
| 01 交易系统数据契约 | `feat/v1.1-wp01-contracts` | 无 | 待执行 |
| 02 历史全A与可交易性 | `feat/v1.1-wp02-universe` | 无 | 待执行 |
| 03 全市场状态与System B指标 | `feat/v1.1-wp03-system-b-state` | 无 | 待执行 |
| 04 市场判断与授权 | `feat/v1.1-wp04-market-authorization` | 无 | 待执行 |
| 05 动态产业—题材库 | `feat/v1.1-wp05-theme-library` | 无 | 待执行 |
| 06 M1—M3身份引擎 | `feat/v1.1-wp06-core-roles` | 无 | 待执行 |
| 07 System B正式策略 | `feat/v1.1-wp07-system-b-production` | 无 | 待执行 |
| 08 全策略稳健性 | `feat/v1.1-wp08-system-validation` | 无 | 待执行 |
| 09 每日运行产品 | `feat/v1.1-wp09-daily-product` | 无 | 待执行 |
| 10-A QRP执行编排核心 | `feat/v1.1-wp10a-execution-core` | 无 | 待执行 |
| 10-B MiniQMT网关协议与适配 | `feat/v1.1-wp10b-miniqmt-adapter` | qrp-trading独立功能分支 | 外部基础已存在，待正式工作包 |
| 11 最终验收 | `release/v1.1-acceptance` | qrp-trading锁定版本 | 待执行 |

## 四、统一实现流程

### 4.1 审计

- 阅读任务文档和相关源代码；
- 找到 contracts SSOT、正式查询和公开导出；
- 查找已有测试和兼容接口；
- 明确数据时点；
- 明确事实、决策、执行和评价边界；
- 对修改范围做静态依赖审计；
- 禁止把本地外部服务误判为 qrp-atlas 已实现模块；
- 任务10-B必须分别审计 qrp-atlas 与 qrp-trading。

### 4.2 设计

编码前形成实施说明：

```text
输入
输出
数据/时间语义
公共接口
持久化
版本策略
兼容策略
失败语义
测试矩阵
分支/PR目标
跨仓协议（适用时）
```

设计必须与任务文档一致，不得重新定义业务规则。

### 4.3 实现

- 先 contracts，再生产者和消费者；
- 先核心领域逻辑，再 API/前端；
- 先确定性实现，再 Agent 解释能力；
- 先 qrp-atlas execution core，再远程 MiniQMT gateway adapter；
- 先封板协议，再并行修改跨仓实现；
- 公共逻辑单一来源；
- 行为变化使用新版本；
- 结果保存完整输入和版本快照；
- qrp-trading生产下单使用窄协议，不使用通用RPC绕过OMS。

### 4.4 验证

每个任务按适用范围覆盖：

- 正常路径；
- 空输入；
- 缺列/非法类型；
- 边界值；
- 多标的隔离；
- 乱序输入；
- 重复键；
- 非有限值；
- PIT和未来数据；
- 修订；
- owner隔离；
- 重启和幂等；
- 部分成功；
- 上游缺失；
- 失败原因稳定；
- 兼容旧接口；
- 端到端真实链路；
- 协议版本和能力变化（适用时）；
- 三方对账（适用时）。

## 五、测试门禁

qrp-atlas运行逻辑任务必须执行：

```text
专项测试
python -m pytest
python -m compileall -q src tests
git diff --check
```

涉及前端时还必须执行：

```text
npm ci
npm run lint
npm run typecheck
npm test
npm run build
```

任务10-B还必须执行：

```text
qrp-trading 离线全量测试
qrp-atlas gateway adapter 专项与全量测试
Windows gateway ↔ Linux QRP 局域网集成测试
READONLY / TRADE_DISABLED / SIMULATION 场景
断线、重启、UNKNOWN和三方对账场景
```

若仓库脚本名称不同，使用现有正式命令并在报告中说明。

禁止：

- `--ignore`；
- `-k`排除失败；
- 新增skip/skipif绕过问题；
- 删除测试使结果变绿；
- 只报告专项测试而隐瞒全量失败；
- 把一次真实连接或一次成功下单当作完整验收。

纯文档任务可不运行pytest，但必须执行：

- 文件范围检查；
- 链接检查；
- `git diff --check`；
- 文档索引与编号一致性检查；
- 术语和模块边界一致性检查。

## 六、PIT与复现规则

所有历史能力必须明确：

- 事件发生时间；
- 发布时间；
- 市场可用时间；
- QRP观察/入库时间；
- 业务有效时间；
- 修订；
- 查询as_of；
- calculation version。

禁止：

- 用今天的题材、角色、ST、行业或指数成分回看过去；
- 用收盘后数据在同日收盘前决策；
- 用未来成交结果影响资格、评分和目标；
- 缺失时前填、后填或取邻近日期而不声明；
- 把当前canonical指针当历史权威来源。

## 七、规则与版本规则

- 任务00之前不实现争议规则；
- 已发布策略不可原地改写；
- 阈值变化进入参数集版本；
- 输入、状态、优先级和行为关系变化进入规则/策略新版本；
- 所有生产决策记录规则和参数版本；
- 新想法写入backlog或变更单，不插入当前任务；
- 研究结果不自动改变生产版本；
- qrp-trading协议语义变化必须升级协议版本并更新兼容矩阵。

## 八、执行安全规则

涉及 `execution` 或 qrp-trading 时：

- 默认使用paper、READONLY或仿真；
- 实盘能力必须在QRP与网关两层显式启用；
- 不得将真实交易作为单元测试副作用；
- 所有下单接口必须有`request_id`和`client_order_id`；
- 未知订单状态不得自动重下；
- 重启后先查询网关/券商再恢复；
- 任何人工操作进入审计；
- kill switch高于计划执行；
- 连接、账户、资金、协议或对账失败时fail closed；
- 不记录密码、API Key、完整账户号或敏感路径；
- qrp-atlas不得调用网关生命周期方法；
- 前端不得直接调用qrp-trading。

## 九、分支与PR规则

### 9.1 qrp-atlas

- 从最新`develop/v1.1`创建工作分支；
- 开发中按批准检查点普通merge最新`develop/v1.1`；
- 禁止rebase和force push；
- PR base固定为`develop/v1.1`；
- PR保持未合并；
- 完成后本地切回`develop/v1.1`并`pull --ff-only`；
- 不将工作包直接合入main；
- 最终仅`release/v1.1-acceptance`向main提版本PR。

### 9.2 qrp-trading

- 保持独立仓库和独立提交；
- 使用独立功能分支；
- 记录起始commit、最终commit、协议版本和测试结果；
- 不复制代码到qrp-atlas；
- 如暂无远程仓库，仍必须保留本地Git提交和可审计验收报告；
- 跨仓工作包在两个实现都完成后统一验收。

## 十、PR交付格式

PR描述至少包含：

```text
任务编号与目标
起始 develop/v1.1 SHA
main参考SHA
最终head SHA
修改文件与模块
明确未做范围
数据/PIT/版本语义
公共接口
迁移与兼容
测试结果
真实数据或仿真证据
已知边界
后续前置
跨仓commit/协议版本（适用时）
```

## 十一、验收失败处理

发现阻塞时：

- 明确列出阻塞和证据；
- 只做任务范围内必要修复；
- 不借返修扩大功能；
- 不把已知缺口改称非阻塞；
- 重新运行适用的完整门禁；
- 更新PR描述和验收报告；
- 跨仓协议问题先修契约，再分别修实现。

## 十二、完成定义

Agent的“完成”只表示：

- 对应任务文档的全部必须项已实现；
- 全部门禁通过；
- PR/本地仓库提交已创建并等待验收。

它不表示PR已合并，也不表示v1.1、MiniQMT或实盘已获得发布授权。
