# qrp-atlas

个人交易复盘工具。每天收盘后自动拉行情，入库到本地数据库，打开网页就能看。

## 这工具干什么的

每天 15:15（交易日），自动跑数据管道：

1. 从新浪接口拉全市场日线行情（A股）
2. 清洗、补全（pre_close、涨跌幅、涨跌停标记）
3. 写入本地 DuckDB
4. 打开前端页面就能看当日数据、翻个股K线、写复盘笔记

数据管道、后端 API、前端页面，都在这一台机器上。

## 当前规模

- 日线数据：1,815 万行（1990-12-19 至今）
- 覆盖股票：5,858 只
- 交易日历：8,797 个交易日
- 每日新增：约 5,800 行

## 技术栈

| 层 | 用啥 |
|----|------|
| 数据存储 | DuckDB（本地文件，无需额外部署） |
| 后端 API | FastAPI（uvicorn，port 8000） |
| 前端 | React 19 + Vite + Tailwind CSS 4 + lightweight-charts v5 |
| 数据源 | 新浪接口（主），tushare 已过期 |
| 部署 | systemd 服务（qrp-atlas-api.service） |

## 项目结构

```
qrp-atlas/
├── src/qrp_atlas/          # Python 后端代码
│   ├── api/                # FastAPI 路由（daily、stock、phase、trades）
│   ├── config/             # 路径、数据源配置
│   ├── contracts/          # 字段名、表结构、源映射（SSOT）
│   └── pipeline/           # 数据管道
│       ├── daily_update/   # 日更行情（fetch→clean→enrich→load）
│       ├── cninfo/         # 机构调研数据
│       ├── research_report/  # 个股研报
│       └── research_industry/ # 行业研报
├── web/                    # 前端页面
│   ├── src/
│   │   ├── pages/          # 今日概览、个股复盘、复盘日志、原始数据
│   │   ├── components/     # 图表、表格、筛选器
│   │   └── api/            # 调用后端接口
│   └── dist/               # 构建产物
├── data/                   # 数据目录（gitignored）
│   ├── db/quant.db         # 运行数据库
│   ├── raw/                # 原始数据备份
│   └── canonical/          # 清洗后规范层（恢复用）
└── deploy/                 # systemd 服务配置
```

## 页面

| 页面 | 功能 |
|------|------|
| 今日概览 | 大盘 KPI、涨停/跌停池、按板块/概念筛选 |
| 个股复盘 | K 线图（可拖拽缩放）、自定义均线、复盘笔记编辑 |
| 复盘日志 | 按日期/股票/主题筛选历史复盘记录 |
| 原始数据 | 数据库表内容预览 |

## 启动方式

### API 后端（已配 systemd 自动启动）

```bash
# 手动启停
sudo systemctl start qrp-atlas-api
sudo systemctl stop qrp-atlas-api
```

### 前端开发

```bash
cd web
npm run dev     # 开发模式，port 3000
npm run build   # 构建到 dist/
npm run preview # 预览构建产物
```

### 手动跑日更管道

```bash
python -m qrp_atlas.pipeline.daily_update.run           # 最近交易日
python -m qrp_atlas.pipeline.daily_update.run --date 2026-06-01  # 指定日期
```

## 数据管道说明

```
新浪接口 ──→ raw CSV（原始备份）
                ↓
           canonical CSV（清洗后，可恢复用）
                ↓
            enrich + load_duckdb
                ↓
           quant.db（运行查询用）
```

- 数据用 `set -e` 脚本串行跑，一步失败就不会写库
- 写入是 DuckDB 事务性 upsert（以 trade_date + ticker 为主键）
- 写入前会做 `quick_validate()` 校验 schema

## cron 日更

交易日 15:15 自动触发，由 Claire（Hermes Agent）调用 `pipeline_daily_run.sh` 脚本执行，完成后在飞书群播报结果。

## 环境变量

`.env` 文件配置（不提交 git）：

- `TUSHARE_TOKEN` — tushare API key（当前已过期，走新浪接口兜底）

## 许可证

个人项目。
