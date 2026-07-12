# qrp-atlas web

qrp-atlas 前端应用。这个目录可以作为独立的 Vite 项目开发，不需要在本机启动数据管道或写入数据库。

## 本机前端开发

```bash
npm install
npm run dev
```

开发服务默认运行在 `http://localhost:3000`。

## API 地址

前端通过 `VITE_API_BASE_URL` 调用后端 API。首次在本机开发时复制环境变量模板：

```bash
copy .env.example .env.local
```

然后把 `.env.local` 里的地址改成目标后端，例如：

```bash
VITE_API_BASE_URL=http://192.168.x.x:8000
```

如果不设置 `VITE_API_BASE_URL`，前端会默认请求当前页面主机的 `:8000` 端口。

可选：

```bash
# 工作流 / 对比结果数据源：mock（默认）或 http
VITE_BACKTEST_RESULT_SOURCE=mock
```

## 页面路由

| 路径 | 说明 |
|------|------|
| `/` | 今日概览 |
| `/stock` | 个股复盘 |
| `/logs` | 复盘日志 |
| `/raw` | 数据库预览 |
| `/backtest/workflow` | 策略配置与回测工作流（mock shell） |
| `/backtest` | 回测结果分析 |

### 策略回测工作流（任务 07 前端先行）

路径：`/backtest/workflow`

演示路径：

1. 在策略目录选择内置策略
2. 查看说明 / 版本，按 schema 配置参数
3. 设置股票池、日期、成本与执行规则
4. 创建模拟回测任务并观察 `queued → running → success/failed`
5. 成功后打开结果（分析页 mock 源）或加入 run 对比
6. 修改配置后再次运行

数据访问分层：

```text
pages / features  →  api/* facade  →  adapters (mock | http)
```

- `StrategyCatalogApi` / `BacktestTaskApi`：当前固定 mock（后端目录与任务 API 尚未就绪）
- `BacktestResultApi`：默认 mock fixtures；`VITE_BACKTEST_RESULT_SOURCE=http` 可切真实结果接口
- 页面组件不得直接 import `api/mock/*`

演示失败态：任务名称包含 `fail`，或 `rolling_zscore_mean_reversion` 且 `lookback > 100`。

## 常用命令

```bash
npm run dev
npm run lint
npm run build
npm run preview
```
