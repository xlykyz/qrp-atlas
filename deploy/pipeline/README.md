# `deploy/pipeline/` 文件角色说明

> 本目录包含 Pipeline 编排相关的**版本化文件**。各文件角色不同，权威性不同；
> 维度归属见 [`docs/governance/SSOT元规则.md`](../../docs/governance/SSOT元规则.md)（D2 / D4）。

---

## 1. 文件清单与角色

| 文件 | 角色 | 是否代表当前生产事实 |
| --- | --- | --- |
| `production-job-definitions.json` | **生产调度实例的版本化镜像**。生产机由 systemd unit 将部署目录中的同名文件传给 `qrp-atlas-jobs serve`；本仓库副本与该文件逐字节一致，作为版本控制与审计基线。 | **是**（镜像） |
| `production-job-definitions.example.json` | **格式示例**。演示 manifest 结构（`schema_version` + `jobs` 数组）；全部条目 `enabled: false`。 | 否 |
| `pipeline-registry.json` | **派生盘点视图**。记录 Pipeline 的全量盘点与迁移状态；对正式 Contract 只记录、不覆盖源码规则（源码 `PipelineContract` 才是业务语义权威）。 | 否 |
| `pipeline-definitions.shadow.json` | **历史 shadow Foundation**。全部条目 `enabled: false`，不被 scan 或 runner 使用。 | 否 |

**判断生产定义的唯一入口**：`production-job-definitions.json`。不需要比较本目录其他文件来判断"当前生产在跑什么"。

## 2. 生产定义的三级关系

```text
生产机（运行时事实）      部署目录 pipeline/production-job-definitions.json
                                  │  变更经运维流程；仓库镜像必须同步
仓库（版本化镜像/权威副本） deploy/pipeline/production-job-definitions.json
                                  │  格式示例（全 disabled，仅演示结构）
仓库（示例，非生产）       deploy/pipeline/production-job-definitions.example.json
```

- 运行时权威在生产机的部署目录文件（由 systemd 加载）；
- 仓库内 `production-job-definitions.json` 是它的版本化镜像，**不应与该文件产生差异**；出现差异即视为缺陷（镜像滞后或生产漂移），需按运维流程核对；
- 示例与 shadow 文件**永不**参与调度。

> **运行警示**：`qrp_atlas.pipeline.production_jobs.DEFAULT_PRODUCTION_JOBS_PATH` 指向仓库内这份**生产镜像**。`qrp-atlas-jobs serve` 未显式传入 `--production-jobs` 时，会直接加载本文件（29 条启用中的调度实例）。生产机由 systemd unit 显式传参（指向部署目录），因此不受仓库影响；但在开发机上**不要**从仓库根直接运行 `serve`，除非明确要在隔离环境中调度测试定义。

## 3. 变更流程（摘要）

1. 生产 schedule / enabled / 参数的变更走运维流程（部署目录与运维手册），本仓库不直接改动生产；
2. 变更落地后，将生产文件同步为仓库镜像（保持逐字节一致），并在提交信息中说明变更内容与时点；
3. 新增格式示例只能放入 `*.example.json`，且必须全部 `enabled: false`；
4. 不得把凭据、本机绝对路径写入本目录任何文件。

## 4. 校验

```bash
# 结构与 Contract 一致性（离线，不访问生产）
python -m pytest tests/pipeline/test_production_jobs.py -q
```

生产一致性（镜像 vs 运行实例）属运维核对项，不在离线测试中断言。
