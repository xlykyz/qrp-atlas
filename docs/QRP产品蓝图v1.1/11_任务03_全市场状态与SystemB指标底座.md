# 任务 03：全市场状态与 System B 指标底座

## 一、目标

把现有 System B 两个布尔字段扩展为完整、持仓无关、全市场逐日可复算的状态与事件体系，并补齐正式策略后续所需个股和市场客观指标。

## 二、前置

- 任务 01、02 已验收；
- 历史全 A 股票池和可交易性事实可用；
- 任务 00 已冻结等于 MA5、停牌、缺失值等规则语义。

## 三、System B 状态模型

状态集合以任务 00 批准结果为准，至少表达：

```text
INACTIVE
CONFIRMING
ACTIVE
WARNING
EXITED
```

状态与事件分离。事件至少包括：

```text
ENTRY_CONFIRMATION
FIRST_BREAK
RECOVERY
EXIT_CONFIRMATION
RESTART_CONFIRMATION
```

正式输出建议：

```text
system_b_state
system_b_active
system_b_entry_confirmed
system_b_first_break
system_b_recovered
system_b_exit_confirmed
system_b_restart_confirmed
state_started_at
state_age
above_ma5_streak
below_ma5_streak
ma5
close_to_ma5_pct
```

## 四、核心要求

- 状态不依赖账户是否持仓；
- 可对任意历史股票池逐日计算；
- 相同历史输入得到相同状态；
- 多标的严格隔离；
- 输入乱序后稳定排序；
- 不修改输入 DataFrame；
- warm-up 不产生虚假状态；
- 缺失和非有限值产生明确 unknown/diagnostic；
- 停牌日、无价格日和复牌日行为遵循规则基线；
- 输出 calculation version 和参数版本；
- 不产生 ENTER/HOLD/EXIT。

## 五、兼容层

保留：

```text
system_b_trend_valid
system_b_exit_triggered
calculate_system_b_basic_states*
detect_system_b_basic_state*
```

要求：

- 旧字段从新状态体系确定性派生或继续通过兼容实现保持原语义；
- `system_b_basic@1.0.0` 回测行为不变；
- 新正式策略不依赖旧布尔字段表达完整生命周期；
- 兼容导出有弃用/保留说明，不突然删除。

## 六、个股状态指标

统一复用已有指标并补齐生产元数据：

- 趋势斜率；
- 趋势 R²；
- 价格效率；
- 中期和短期动量；
- 距离高点；
- 波动和下行波动；
- 当前/最大回撤；
- 相对成交量；
- 成交额和换手；
- Amihud 流动性；
- 价格量相关；
- 涨停、连板、炸板、开板次数；
- 近 N 日涨停次数；
- 严重异常和可交易性 coverage。

每项明确：

```text
formula
inputs
window
direction
available_time
NaN semantics
calculation_version
coverage
```

## 七、相对强度指标

至少支持：

- 对主要指数；
- 对所属行业；
- 对所属题材/题材成员等权或指定基准；
- 滚动窗口收益差；
- 残差收益；
- 分位排名；
- 数据不足诊断。

题材相对强度在任务 05 数据可用后补齐正式输入，但指标接口和时点语义在本任务定义。

## 八、市场客观状态组件

扩展现有市场宽度和风险：

- 上涨/下跌家数和比例；
- 涨停、跌停、炸板；
- 创新高/新低；
- 大涨和大跌分布；
- 300/688 等不同涨跌幅制度下的大跌统计；
- 连板高度和容量；
- 市场成交额；
- 主要指数节点；
- 核心持续性和退潮组件；
- 数据 coverage。

这些组件只描述事实，不直接输出市场阶段或授权。

## 九、指标注册与服务

- 通过现有 IndicatorDefinition/Request 或正式兼容入口注册；
- 不建立平行 indicators 系统；
- 依赖关系由通用计算图解析；
- runtime 不按策略 code 硬编码；
- 目录 API 能展示名称、范围、频率、版本和说明；
- 批量全市场计算可分块，但结果必须确定性一致。

## 十、持久化边界

- 可重算指标原则上不作为不可替代事实；
- 每日生产需要持久化状态、事件、版本和输入指纹；
- 回测可按需重算并保存结果快照；
- 指标输出写入正式 observation/production artifact，而非修改基础行情表。

## 十一、测试

至少覆盖：

- 完整状态转移；
- 一次跌破后恢复；
- 两次跌破退出；
- 退出后重新确认；
- 等于 MA5；
- warm-up；
- 单条和不足历史；
- 停牌/缺失/复牌；
- 多 ticker 隔离；
- 乱序和重复键；
- NaN/inf/非法价格；
- 兼容字段一致性；
- 无未来数据；
- 版本和参数快照；
- 全市场规模和性能；
- 市场大跌统计的边界制度。

## 十二、禁止范围

- 不输出市场 A/B/C 阶段；
- 不输出题材 lifecycle；
- 不判定 M1—M3；
- 不做 eligible、score、rank；
- 不处理账户持仓；
- 不生成交易动作或目标权重；
- 不访问 QMT。

## 十三、验收

- System B 不再只能用两个布尔字段描述；
- 任意历史日全市场状态可复算；
- 新旧接口兼容；
- 事实、事件和诊断完整；
- 指标目录和运行器可通用消费；
- 专项、全量、PIT 和性能测试通过；
- PR 等待独立验收。