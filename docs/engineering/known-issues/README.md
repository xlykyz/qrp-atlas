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
title: 同一目标日失败后无法重跑。  # 必填：一句话说明该 Known Issue 是什么
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

- **查阅活跃问题**：默认只检索 `*-open.yaml`；仅在排查问题复发、历史原因或关联问题时，才查阅 `*-closed.yaml`。
- **新增登记**：查看当前最大编号累加，创建 `KI-XXX-open.yaml`，内容中 `status: open`，`date` 记录当天日期。
- **关闭问题**：同时完成两步——将文件重命名为 `KI-XXX-closed.yaml`，并将内容中的 `status` 修改为 `closed`。
- **一致性约束**：文件名中的 `status` 必须与 YAML 内 `status` 字段一致（`KI-XXX-open.yaml` ↔ `status: open`，`KI-XXX-closed.yaml` ↔ `status: closed`）；不一致视为无效记录。
