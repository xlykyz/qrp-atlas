# qrp-atlas web

`web/` 是 qrp-atlas 的 React + Vite 前端应用。它通过后端 API 完成复盘、策略配置、回测任务和结果分析，不直接访问 DuckDB，也不在浏览器中执行 Python 策略。

当前前端已经接入真实策略目录、回测任务和标准结果 API；mock adapter 仍保留用于离线界面开发和演示。前端产品体验仍在持续完善，不应仅凭后端 v1.0 验收状态视为已经达到正式发布质量。

## 本机前端开发

```bash
npm install
npm run dev
```

开发服务默认运行在 `http://localhost:3000`。

## API 地址

前端通过 `VITE_API_BASE_URL` 调用后端 API。首次本机开发时复制环境变量模板：

```bash
copy .env.example .env.local       # Windows PowerShell / cmd
# cp .env.example .env.local       # Linux / macOS
```

然后将 `.env.local` 指向目标后端，例如：

```bash
VITE_API_BASE_URL=http://192.168.x.x:8000
```

如果不设置 `VITE_API_BASE_URL`，前端默认请求当前页面主机的 `:8000` 端口。

## 真实与 mock 数据源

回测工作流统一通过 adapter 访问：

```text
pages / features → api facades → adapters (http | mock)
```

默认模式为真实 HTTP：

```bash
VITE_BACKTEST_WORKFLOW_SOURCE=http
```

- `http`：使用后端真实策略目录、任务、运行和结果接口；
- `mock`：使用 `src/api/mock/` 中的本地 fixture，不需要启动后端。

结果 adapter 默认继承工作流数据源，也可以单独覆盖：

```bash
VITE_BACKTEST_RESULT_SOURCE=http
```

离线开发示例：

```bash
VITE_BACKTEST_WORKFLOW_SOURCE=mock
VITE_BACKTEST_RESULT_SOURCE=mock
```

环境变量未设置时，工作流和结果均默认使用 `http`。

## 页面路由

| 路径 | 说明 |
| --- | --- |
| `/` | 今日概览 |
| `/stock` | 个股复盘 |
| `/logs` | 复盘日志 |
| `/raw` | 数据库预览 |
| `/backtest/workflow` | 策略目录、参数配置、回测任务与运行入口 |
| `/backtest` | 标准回测结果分析与运行对比 |

## 策略回测工作流

路径：`/backtest/workflow`

真实 HTTP 模式下，典型流程为：

1. 从后端策略目录读取可用策略和稳定版本；
2. 根据后端 schema 展示参数表单；
3. 配置股票池、日期、成本、执行方式和策略参数；
4. 创建持久化回测任务并读取任务状态；
5. 任务成功后使用真实 `run_id` 打开标准结果；
6. 从运行历史中选择多个 run 进行比较；
7. 在后端支持的范围内进行结果重放和复现核验。

可用策略类型由后端正式 catalog 决定，当前产品链已经覆盖经典时间序列、横截面、事件驱动和声明式策略。页面不得维护一份与后端注册表平行的策略真值清单。

## API 分层规则

```text
pages
  ↓
features/<feature>/hooks
  ↓
api facades
  ↓
adapters/http 或 adapters/mock
```

- 页面和组件不得直接写裸 `fetch`；
- 页面和 feature 不得直接 import `api/mock/*`；
- mock 与 HTTP adapter 应实现同一前端协议；
- API DTO 与表单状态应显式转换，不把页面内部字段直接当作后端契约；
- 前端不得自行模拟策略算法、成交规则、owner 过滤或回测指标；
- 认证用户和数据归属由后端可信上下文决定，前端隐藏不是权限控制。

## 当前产品边界

- 后端 v1.0 产品闭环已经验收，但前端仍需要继续改善信息架构、错误态、加载态、历史管理、结果解释和整体交互；
- 当前构建存在主 JavaScript chunk 偏大的已知边界，后续应按页面和业务模块逐步拆分；
- mock 模式只用于开发和演示，不是正式运行结果；
- 前端不包含实盘下单、QMT 接入或浏览器内策略执行。

## 常用命令

```bash
npm run dev
npm run lint
npm run build
npm run preview
```

前端运行逻辑变更完成后至少运行 `npm run build`；纯文档任务可以不运行构建，但应在交付说明中明确记录。