# QRP v1.0 核心架构文档

> **文档状态：架构骨架封版**  
> **版本：v1.0**  
> **封版日期：2026-07-12**  
> **适用项目：qrp-atlas**

---

## 一、文档目的

本文档用于确定 QRP v1.0 的核心业务架构、模块职责、依赖方向与运行闭环。

本次封版的对象是：

- 核心模块的职责边界
- 模块之间的单向依赖关系
- 策略的统一定义
- 内置策略与前端自定义策略的统一运行模型
- 策略、指标与回测之间的连接方式
- QRP 作为可交付产品的最小业务闭环

本次封版不意味着冻结具体功能、策略数量、指标数量、数据库表数量或前端页面。后续新增能力必须在本架构内扩展，不得破坏已经确定的模块边界。

---

## 二、产品定义

QRP 是一个以标准数据契约为基础，通过指标层生成市场事实、通过策略层定义交易算法，并由回测系统完成数据准备、策略运行、成交模拟和结果分析的量化交易研究平台。

QRP v1.0 的核心工作流是：

```text
数据采集与入库
    ↓
标准数据契约
    ↓
复合指标计算
    ↓
策略定义与运行
    ↓
回测成交与绩效计算
    ↓
API 输出
    ↓
前端配置与分析
```

当用户能够完成以下完整操作时，QRP 即形成可交付产品闭环：

```text
选择或编写策略
→ 设置策略参数
→ 选择回测范围
→ 运行回测
→ 查看成交、收益、回撤和风险
→ 修改策略
→ 再次验证
```

---

## 三、核心设计原则

### 3.1 单向依赖

核心业务抽象从低到高依次为：

```text
contracts → indicators → strategies
```

含义是：

- `contracts` 是最底层基础模块
- `indicators` 可以依赖 `contracts`
- `strategies` 可以依赖 `indicators` 和 `contracts`
- 下层模块不得反向依赖上层模块
- `backtest`、`api` 等应用模块可以调用核心业务模块
- 核心业务模块不得依赖前端或 API

### 3.2 定义与运行分离

策略构建时只声明：

- 输入变量
- 算法
- 参数
- 入场规则
- 持有规则
- 退出规则
- 输出结构

策略本身不读取数据库。

真正的数据读取、指标准备和逐日运行发生在回测或其他运行环境中。

### 3.3 事实与决策分离

- `contracts` 定义数据
- `indicators` 描述事实
- `strategies` 作出决策
- `backtest` 模拟决策结果

任何模块不得越级承担相邻模块的职责。

### 3.4 内置策略与自定义策略统一

Python 内置策略与未来前端自定义策略可以采用不同的定义方式，但必须遵守同一套：

- 策略元数据
- 输入变量声明
- 参数定义
- 标准决策输出
- 版本机制
- 校验规则
- 回测调用接口

---

## 四、模块职责

## 4.1 `contracts`｜基础契约层

### 核心职责

定义项目中的标准数据语言，包括：

- 数据表
- 字段名
- 字段类型
- 主键
- 可空性
- 字段语义
- 标准 DataFrame 结构
- 数据源到标准字段的映射
- 模块间交换数据的基础结构

### 典型内容

```text
ticker
trade_date
open
high
low
close
amount
pct_chg
turnover_rate
```

### 边界

`contracts`：

- 不查询数据库
- 不计算指标
- 不生成交易决策
- 不模拟成交
- 不依赖其他业务模块

### 定位

> `contracts` 定义 QRP 中“数据是什么”。

---

## 4.2 `pipeline / data`｜数据生产与访问层

### 核心职责

负责：

- 外部数据获取
- 清洗
- 字段映射
- 契约校验
- 入库
- 标准数据库读取
- 为运行任务提供基础数据

### 依赖

```text
pipeline / data → contracts
```

### 边界

该层生产和读取数据，但不决定：

- 指标含义
- 策略行为
- 交易动作
- 回测结果

### 定位

> `pipeline / data` 负责“把标准数据准备好”。

---

## 4.3 `indicators`｜复合指标计算层

### 核心职责

基于数据库基础字段进行二次计算，形成可复用的客观指标和市场状态。

只要一个值需要经过计算才能得到，无论算法简单或复杂，都应统一进入 `indicators`。

### 典型指标

```text
ma5
close_above_ma5_days
close_below_ma5_days
system_b_trend_valid
system_b_exit_triggered
近 20 日涨停次数
市场宽度
市场风险状态
```

### 依赖

```text
indicators → contracts
```

### 边界

`indicators`：

- 可以引用标准字段
- 可以组合其他底层指标
- 不生成 ENTER、HOLD、EXIT
- 不处理持仓状态
- 不执行数据库写入
- 不模拟成交

### 定位

> `indicators` 回答“市场或标的已经发生了什么”。

---

## 4.4 `strategies`｜策略定义与决策层

### 策略定义

QRP 中的策略是：

> 具有明确输入、明确参数、明确算法、明确输出和稳定版本，可被统一运行并可对接回测的交易规则单元。

策略可以使用两类输入变量：

1. **基础字段**：由 `contracts` 定义，运行时直接从数据库数据中取得
2. **复合指标**：由 `indicators` 定义，运行时根据所需基础字段计算

### 策略构建阶段

策略只声明：

```text
需要哪些字段
需要哪些复合指标
参数是什么
条件如何组合
何时入场
何时持有
何时退出
输出什么决策
```

策略不请求数据库，也不负责主动准备指标。

### 标准输出

策略至少应能输出：

```text
ENTER
HOLD
EXIT
NO_ACTION
```

标准决策记录建议包含：

```text
trade_date
asset_id
action
direction
strategy_code
strategy_version
reason_code
score
weight
evidence
```

### 依赖

```text
strategies → indicators
strategies → contracts
```

### 边界

`strategies`：

- 不自行查询数据库
- 不重新实现已有指标
- 不模拟成交
- 不计算手续费和滑点
- 不生成收益曲线
- 不依赖 `backtest`

### 定位

> `strategies` 回答“面对这些数据和指标，应当做什么”。

---

## 4.5 `backtest`｜策略运行与交易模拟系统

`backtest` 在逻辑上分为两个部分。

### A. Backtest Runtime｜回测运行器

负责：

1. 读取策略定义
2. 解析策略所需基础字段
3. 根据 `contracts` 定位和校验数据
4. 从数据库加载回测区间数据
5. 识别策略所需复合指标
6. 调用 `indicators` 完成指标计算
7. 将完整输入交给策略
8. 按时间顺序运行策略
9. 将策略决策交给回测引擎

### B. Backtest Engine｜回测执行引擎

负责：

- 入场和退出成交
- 成交时点
- 成交价格
- 仓位
- 资金约束
- 手续费
- 印花税
- 滑点
- 持仓记录
- 收益
- MAE / MFE
- 资金曲线
- 回撤
- 汇总指标

### 依赖关系

```text
backtest runtime
├── contracts
├── indicators
├── strategies
├── data
└── backtest engine
```

底层回测引擎不应包含：

- 五日线
- 涨停
- 节点
- 题材
- 系统 A
- 系统 B
- 任何具体策略知识

### 定位

> 回测运行器负责“把策略运行起来”，回测引擎负责“模拟交易结果”。

---

## 4.6 `results`｜结果存储与查询层

### 核心职责

保存和读取：

- 回测任务信息
- 策略版本快照
- 参数快照
- 指标版本快照
- 交易记录
- 被跳过记录
- 资金曲线
- 回撤曲线
- 绩效汇总
- 配置快照

### 关键要求

任何回测结果都应能够追溯到：

```text
strategy_code
strategy_version
strategy_definition
strategy_parameters
indicator_versions
data_range
execution_config
cost_config
```

### 定位

> `results` 负责保证回测结果可查询、可复现、可比较。

---

## 4.7 `api`｜应用编排与能力输出层

### 核心职责

API 负责：

- 暴露指标列表
- 暴露策略列表
- 获取策略定义
- 校验自定义策略
- 创建回测任务
- 运行回测
- 查询回测结果
- 为前端提供统一 JSON 接口

API 可以组织调用多个业务模块，但不得承载指标算法或策略算法。

### 定位

> `api` 负责“把后端能力组织成产品接口”。

---

## 4.8 `frontend`｜产品交互层

### 核心职责

前端负责：

- 选择内置策略
- 编辑声明式自定义策略
- 选择字段与指标
- 设置参数
- 选择股票池和时间范围
- 启动回测
- 展示运行状态
- 展示交易记录和绩效结果
- 对比不同策略和参数结果

前端不直接执行 Python 策略，也不直接访问 DuckDB。

### 定位

> `frontend` 负责“让用户配置、运行和理解 QRP”。

---

## 五、基础字段与复合指标的区分

在代码和运行系统中，应明确区分两类策略输入。

| 类型 | 示例 | 定义位置 | 运行时来源 |
|---|---|---|---|
| 基础字段 `field` | close、amount、pct_chg | `contracts` | 数据库直接读取 |
| 复合指标 `indicator` | ma5、system_b_trend_valid | `indicators` | 根据基础字段计算 |

策略可以同时引用两类变量：

```json
{
  "required_fields": [
    "amount"
  ],
  "required_indicators": [
    "system_b_trend_valid"
  ]
}
```

前端可以统一把它们展示为“可用变量”，但后端必须保留来源类型：

```json
{
  "source_type": "field",
  "code": "amount"
}
```

```json
{
  "source_type": "indicator",
  "code": "system_b_trend_valid"
}
```

这样回测运行器才能确定该变量应当直接读取，还是调用指标模块计算。

---

## 六、策略运行模型

## 6.1 策略构建阶段

```text
用户或开发者
    ↓
定义策略算法和参数
    ↓
声明 required_fields
    ↓
声明 required_indicators
    ↓
声明入场、持有和退出规则
    ↓
保存 StrategyDefinition
```

该阶段不读取行情，不计算指标，不执行交易。

## 6.2 回测运行阶段

```text
读取 StrategyDefinition
    ↓
解析 required_fields
    ↓
读取数据库基础字段
    ↓
解析 required_indicators
    ↓
调用 indicators 计算
    ↓
构造策略输入
    ↓
逐日运行策略
    ↓
输出交易决策
    ↓
回测引擎模拟成交
    ↓
保存回测结果
```

---

## 七、内置策略与自定义策略

## 7.1 内置策略

内置策略由 Python 实现，适用于：

- 系统 A
- 系统 B
- 节点策略
- 复杂状态机
- 需要专门算法的策略
- 需要较强运行性能的策略

建议目录：

```text
src/qrp_atlas/strategies/builtin/
```

## 7.2 自定义策略

前端自定义策略应采用受约束的声明式结构，而不是执行任意 Python 代码。

建议支持：

- 字段选择
- 指标选择
- 参数定义
- 条件比较
- `all`
- `any`
- `not`
- 入场条件
- 退出条件
- 排序与评分
- 策略版本

建议目录：

```text
src/qrp_atlas/strategies/declarative/
```

## 7.3 统一接口

两种策略必须统一输出：

```text
StrategyDecisionFrame
```

并由同一个回测运行器执行：

```text
Python 内置策略 ──────┐
                     ├→ 统一策略接口 → Backtest Runtime
声明式自定义策略 ─────┘
```

---

## 八、核心依赖规则

### 允许

```text
pipeline → contracts
data → contracts
indicators → contracts
strategies → contracts
strategies → indicators
backtest runtime → contracts
backtest runtime → indicators
backtest runtime → strategies
backtest engine → backtest models
api → indicators / strategies / backtest / results
frontend → api
```

### 禁止

```text
contracts → indicators
contracts → strategies
contracts → backtest
indicators → strategies
indicators → backtest
strategies → backtest
strategies → api
strategies → frontend
backtest engine → 具体策略
frontend → DuckDB
frontend → Python 策略对象
```

---

## 九、推荐目录骨架

```text
src/qrp_atlas/
├── contracts/
├── pipeline/
├── data/
├── indicators/
├── strategies/
│   ├── definitions.py
│   ├── models.py
│   ├── protocol.py
│   ├── registry.py
│   ├── validation.py
│   ├── builtin/
│   └── declarative/
├── backtest/
│   ├── runtime/
│   ├── engine.py
│   ├── broker.py
│   ├── models.py
│   ├── metrics.py
│   └── results/
├── api/
└── ...
```

前端保持：

```text
web/
├── src/api/
├── src/pages/
├── src/components/
├── src/types/
└── ...
```

---

## 十、QRP v1.0 产品闭环

QRP v1.0 的业务闭环由以下能力构成：

### 数据底座

- 数据获取
- 数据清洗
- 标准契约
- 数据库存储
- 标准读取

### 研究底座

- 指标计算
- 指标注册
- 指标发现
- 指标复用

### 策略底座

- 策略定义
- 策略参数
- 策略版本
- 内置策略
- 声明式自定义策略
- 标准决策输出

### 验证底座

- 数据准备
- 指标准备
- 策略运行
- 成交模拟
- 仓位与成本
- 收益与风险
- 结果保存

### 产品界面

- 策略配置
- 回测控制
- 结果分析
- 策略迭代

只要上述主链路完成，QRP 即具备独立交付价值。实盘交易、AI 策略生成、分布式计算和复杂组合优化均属于 v1.0 之后的扩展能力。

---

## 十一、架构封版规则

本架构自 v1.0 起进入骨架封版状态。

后续开发必须遵守：

1. 新增数据库字段，先进入 `contracts`
2. 新增需要计算的市场变量，进入 `indicators`
3. 新增交易算法和规则，进入 `strategies`
4. 新增成交、费用、仓位和绩效能力，进入 `backtest`
5. 新增业务调用流程，进入应用服务或 API
6. 新增交互能力，进入前端
7. 不得在策略中直接查询数据库
8. 不得在策略中重复实现已有指标
9. 不得在指标中输出交易动作
10. 不得在通用回测引擎中写入具体策略知识
11. 内置策略与自定义策略必须遵守统一策略契约
12. 所有回测必须保存足够的版本与参数快照，以保证复现

允许调整：

- 模块内部文件划分
- 类名与函数名
- 数据结构实现细节
- 策略数量
- 指标数量
- 回测执行能力
- 前端页面和交互

不允许破坏：

- 核心职责边界
- 单向依赖关系
- 策略定义与运行分离
- 指标事实与策略决策分离
- 策略与回测执行分离
- 内置与自定义策略统一模型

---

## 十二、封版结论

QRP v1.0 的核心架构正式确定为：

```text
数据契约
→ 指标计算
→ 策略决策
→ 回测运行与成交模拟
→ 结果存储
→ API
→ 前端
```

其中：

```text
contracts 定义数据
pipeline / data 准备数据
indicators 计算事实
strategies 定义决策
backtest 验证决策
results 保存结果
api 组织能力
frontend 提供交互
```

该架构已经形成完整业务闭环。

当 `strategies`、`backtest runtime`、统一策略回测接口以及前端策略配置链路开发完成后，QRP 将由个人复盘与研究项目升级为可独立交付的量化交易研究产品。

---

**封版声明：**

> 自本文件生效起，QRP 后续功能均应在本架构骨架内演进。除非出现无法通过模块内部扩展解决的结构性证据，否则不再重新讨论核心模块的职责划分与依赖方向。
