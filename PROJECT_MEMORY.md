# PROJECT_MEMORY — 用户灵感、临时需求、碎片想法

> Agent 自行维护。格式：时间戳 + 记录内容。
> 建议大小 ≤2000 字符，Agent 智能精简旧记录。
> 重要程度：低，用于帮助回忆上下文，不替代决策。

2026-05-13: 后端 API MVP 交付
- 新增 src/qrp_atlas/api/ 层：FastAPI + DuckDB
- 架构：db.py(连接) → routes/{daily,phase,trades}.py → server.py(入口)
- systemd 服务：qrp-atlas-api.service，端口 8000，绑定 0.0.0.0，开机自启
- quant.db: 1.83GB, 1200万行 (2013-01-07 ~ 2026-02-27)
- 三个表: daily_market_snapshot(1200万行) / market_phase(空) / trade_execution(空)
- 前端未动，Streamlit 仍在 web/ 下待替换
- 工作流纪律：Claire PM → Claude Code 开发，Claire 不得擅自代工
