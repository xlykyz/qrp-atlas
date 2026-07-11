## Frontend Development Guidelines

前端开发目标：让 qrp-atlas 的 Web 界面长期可维护、可扩展、可重构。新增功能不能继续堆进页面文件，必须按职责拆分。

### 1. 核心原则

页面负责组装，组件负责展示，hooks 负责状态和副作用，api 层负责请求，lib 负责纯函数，types 负责类型。

新增或重构功能时，优先保持现有业务行为不变。除非任务明确要求，不要顺手改 UI、不改变 API 协议、不引入新的状态管理库、不重写无关模块。

### 2. 推荐结构

```txt
web/src/
  pages/                     # 路由页面，只做组装
  features/<feature>/         # 业务功能模块
    components/
    hooks/
    lib/
    types.ts
  shared/
    components/
    hooks/
    lib/
  api/                        # 后端请求封装
  types/                      # 跨模块共享类型
```

当前主要 feature：

```txt
stock-review
overview
raw-preview
review-logs
backtest
```

### 3. 页面文件约束

页面文件应保持轻量，主要负责：

* 读取路由参数
* 调用 feature hooks
* 组合 feature components
* 管理页面级布局

页面文件不应长期承载：

* 复杂数据请求
* 大量 useEffect
* 图表生命周期
* 表单提交细节
* 表格排序/筛选/分页细节
* 大量业务计算
* 重复工具函数

如果页面超过约 300 行，新增功能前应优先考虑拆分。

### 4. 复用规则

跨页面复用的能力放入 `shared/`。

某个业务功能专属的能力放入 `features/<feature>/`。

所有后端请求必须经过 `api/` 层，不要在页面或组件里直接写裸 `fetch`。

已有公共工具函数时必须复用，不要重复定义。例如格式化百分比、金额、成交量、日期、颜色 class 等。

### 5. 图表与大表格规则

图表逻辑必须和页面逻辑分离。图表组件负责渲染，hook 负责生命周期，lib 负责数据转换和指标计算。

大表格必须注意滚动边界、冻结表头/冻结列、分页或虚拟滚动。横向滚动应限制在表格容器内部，不能导致整个页面横向滚动。

### 6. 重构规则

重构必须小步进行，不要一次性重构整个页面。

推荐顺序：

1. 抽纯函数
2. 抽展示组件
3. 抽业务组件
4. 抽 hooks
5. 最后处理图表、表格等复杂生命周期逻辑

每次重构必须满足：

* 页面行为不变
* API 协议不变
* UI 风格不被顺手改动
* 不引入无关依赖
* 改动范围可解释、可回滚

### 7. 状态管理规则

当前阶段默认不引入 Redux、Zustand、MobX 等全局状态库。

优先使用：

* 组件内部 state
* 页面级 / feature 级 hook
* URL search params

只有当多个页面确实需要共享同一份状态时，才考虑全局状态方案。

### 8. 当前重点

`stock-review.tsx` 是当前最高优先级重构对象。目标不是重写页面，而是逐步拆出：

```txt
features/stock-review/components/
features/stock-review/hooks/
features/stock-review/lib/
features/stock-review/types.ts
```

每次只拆一个明确模块，拆完后确认页面行为不变。

### 9. 验收要求

前端修改完成后至少运行：

```bash
npm run build
```

如果项目配置了 lint 或 test，也应运行对应命令。

提交说明需要写清：

* 修改了哪个 feature
* 拆出了哪些组件 / hooks / lib
* 是否改变业务行为
* 验证命令和结果
