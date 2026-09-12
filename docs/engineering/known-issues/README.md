# Known Issues（待优化清单）

本目录以结构化 YAML 登记已确认但尚未修复的工程问题（pipeline 逻辑、runtime 行为等）。

## 文件命名

- 路径：`docs/engineering/known-issues/`
- 格式：`KI-<编号>-<status>.yaml`
- 状态枚举：`open` | `closed`
- 示例：`KI-001-open.yaml`、`KI-001-closed.yaml`

## 内容模板

````yaml
id: KI-001
date: 2026-09-12     # 建立 issue 的时间 (YYYY-MM-DD)
status: open         # 枚举：open | closed
# 仅记录：触发操作 -> 屏幕/日志/数据观测到的客观异常
symptom: |
  执行 `run_task --target-date 2026-09-12`，日志打印 "Skip: already processed"，但数据库对应日期的更新时间戳与数据均未更新。
# --- 以下选填 ---
mitigation: |
  手动追加 `--force-rerun` 参数触发执行。
root_cause: |
  幂等键仅包含 target-date，缺少 execution_id 区分重跑批次。
````

## Agent 执行规则

- **查阅活跃问题**：直接检索 `*-open.yaml` 文件名，严禁读取 `*-closed.yaml` 的内容。
- **新增登记**：查看当前最大编号累加，创建 `KI-XXX-open.yaml`，内容中 `status: open`，`date` 记录当天日期。
- **关闭问题**：将文件重命名为 `KI-XXX-closed.yaml`，并将内容中的 `status` 修改为 `closed`。