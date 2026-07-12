# `src/qrp_atlas` Agent Rules

本文件适用于 `src/qrp_atlas` 及其所有子目录。仓库根目录的 `AGENTS.md` 仍然同时生效；如有冲突，以更具体的本文件为准。

## Contracts 的适用边界

`src/qrp_atlas/contracts/` 是数据进入系统时的唯一事实来源（SSOT）。它定义数据字段、表结构、数据源映射、类型/日期/ticker 约定和校验规则。

本项目按数据流向区分约束强度：

- `pipeline/` 是外部数据进入系统的通道，必须强制遵守 contracts。任何数据进入数据库前，都必须完成字段映射、schema 对齐、类型标准化和必要校验。
- `api/`、`backtest/`、`indicators/` 等模块是从数据库取数的数据流出/消费通道，优先参考和复用 contracts，但不要求所有内部变量、计算字段或 API 展示字段都直接使用 contracts 常量。
- 下游模块可以为了接口语义、计算便利或展示需求使用自己的 DTO、响应字段和派生字段；但访问数据库时仍必须以实际数据库 schema 为准，不能误读、篡改或隐式假设存储字段。

数据链路中的强约束边界是：

```text
数据源 -> 清洗 -> 标准化 -> 入库 -> API 查询 -> 可视化/回测
          [ contracts 强约束 ]       [ contracts 优先参考 ]
```

修改或新增 `pipeline/` 代码时，agent 必须先检查相关的 contracts 定义，再实现业务逻辑。修改其它模块时，应先检查 contracts 和数据库 schema，能复用时优先复用，但不因内部实现或对外展示需求而强行套用 contracts 的命名。

## 依赖方向与模块边界

依赖方向必须保持单向：

```text
contracts -> pipeline / indicators / backtest / api / frontend
```

- `indicators/` 可以 import `qrp_atlas.contracts`，用于复用字段常量、表结构和底层市场规则。
- `contracts/` 绝对不能 import `qrp_atlas.indicators`，也不能依赖任何下游业务模块。
- `indicators/` 是市场复合指标计算层，不是交易执行层；可以输出市场宽度、风险、趋势、系统状态等结构化结果，但不要直接写买入、卖出、下单逻辑。
- `pipeline/` 负责数据入库；`indicators/` 负责基于已有数据计算复合指标；`api/` 和 `web/` 负责展示与交互。不要把这些职责互相混入。

## Pipeline 必须遵守的约束

- pipeline 中的标准字段名必须从 `qrp_atlas.contracts` 导入，例如 `TICKER`、`TRADE_DATE`、`CLOSE`；禁止在 pipeline 中重复定义或散落硬编码标准字段名。
- 表名、列清单、主键、可空性和 DuckDB 类型必须以 `contracts/schema.py` 为准。新增或修改持久化数据结构时，先同步更新 schema，再更新调用方。
- 外部数据源字段必须通过 `contracts/mappings.py` 的映射处理；禁止在各 pipeline 中维护另一套隐式字段映射。
- 日期、ticker、交易所、板块、涨跌停等通用规则必须使用 `contracts/conventions.py` 提供的常量和函数；不要复制一份本地实现。
- DataFrame 进入数据库前，应使用 `validate_schema`、`align_to_schema`、`canonicalize` 或 `quick_validate` 等 contracts 校验/标准化函数。不要用静默丢列、随意改名或未经说明的类型转换掩盖契约不一致。
- 如果 contracts 中没有所需字段、表、映射或约定，应先补充 contracts，并同步更新 `contracts/__init__.py` 的公开导出（如适用），再修改使用方。
- 不要为了让 pipeline 通过而放宽、绕过或复制 contracts 校验；任何契约变更都必须明确说明影响范围，并补充或更新测试。

## 下游模块的建议

- API、回测和指标模块读取数据库时，建议优先使用 contracts 中的表名、字段名和约定，以减少拼写错误和语义漂移。
- 下游模块可以在边界处转换为自己的响应模型、领域模型或派生指标；这种转换应保持显式，不要把派生字段回写成标准存储字段而不更新 contracts。
- 如果下游代码发现数据库字段与 contracts 不一致，应优先报告或修复上游 pipeline/schema 问题；只有在明确的兼容场景下才增加适配逻辑，并注明兼容原因。

## 修改流程

1. 修改 pipeline 时，阅读任务相关的 `contracts/fields.py`、`schema.py`、`mappings.py`、`conventions.py`、`validate.py`。
2. 判断变更是“使用既有契约”还是“修改/扩展契约”。涉及数据流入或持久化结构时，后者先改 contracts，再改生产者和消费者。
3. 在 pipeline 入库边界执行字段、schema 和类型校验；对额外列、缺失列和类型转换采用显式策略。
4. 修改下游模块时，确认其查询使用的数据库字段与 schema 一致；对 API/回测/指标内部模型允许使用显式适配。
5. 运行与变更范围匹配的测试、lint 或类型检查，并在交付说明中记录未运行的检查。

## 变更范围控制

- 每次任务应尽量只修改与目标直接相关的文件。
- `indicators`、`pipeline`、`api`、`web`、`docs`、`scripts` 不要混在同一个无关提交里。
- 如果任务中途发现其它层的问题，优先报告；除非用户明确授权，否则不要顺手跨层修复。
- 修复测试失败时，优先修复真实契约或实现问题，不要通过放宽测试掩盖边界错误。

## 禁止事项

- 禁止在 pipeline 中创建与 contracts 重复的字段常量、表 schema、数据源映射或通用市场规则。
- 禁止 pipeline 直接依赖外部来源的原始列名作为入库接口。
- 禁止在没有迁移/兼容说明的情况下修改既有字段名、表名、主键或字段语义。
- 禁止下游模块把自己的展示字段、派生字段或内部模型反向当成数据库标准字段。
- 禁止 `contracts/` import `qrp_atlas.indicators` 或其它下游业务模块。
- 禁止把数据库、原始行情数据或生成数据的改动伪装成普通代码改动；如任务确实需要触碰这些内容，必须先说明原因并遵守仓库根目录规则。

## 常用导入示例

```python
from qrp_atlas.contracts import (
    CLOSE,
    TICKER,
    TRADE_DATE,
    DAILY_MARKET_SNAPSHOT,
    align_to_schema,
    quick_validate,
)
```
