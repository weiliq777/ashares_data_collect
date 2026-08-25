# Data Foundation 当前状态索引（2026-08-18）

本文是当前工作树的数据访问与数据基座状态索引，不替代设计契约。

## 当前有效基线

- Data Foundation Access Framework 的唯一实现契约：`dev_doc/data_access_kernel.md`
- 全局分层与迁移边界：`dev_doc/数据基座架构拆分方案.md`
- 代码防退化规则：`dev_doc/data_access_firewall.md`
- 当前任务清单：`dev_doc/agent_tasks.md`
- 当前运行手册：`dev_doc/project_completion_runbook.md`

## 已验证状态

- 全量测试：`884 passed, 1 skipped, 8 deselected`
- 数据访问契约扫描：`138 files; violations: 0`
- Stage 1 合同迁移已在目标 PostgreSQL 实例执行并完成幂等验证，记录见 `dev_doc/data_foundation_stage1_execution_20260818.md`。
- Stage 2 Mart 合同视图和网关事务边界已完成验证，记录见 `dev_doc/data_foundation_stage2_validation_20260818.md`。
- 现有 Raw、Standard、PIT、Feature、Research Snapshot 数据原则上复用；未授权进行全量重建或覆盖旧数据。

## 当前未完成边界

- 仍需按迁移清单逐批迁移未收口的旧访问点，并持续执行静态契约扫描、单元测试和集成测试。
- `public` 旧表向目标 schema 的物理迁移仍需独立设计双写、校验、回滚和切换窗口，不能用一次性大事务或直接 `ALTER TABLE ... SET SCHEMA` 冒险替代。
- 生产增量运行和后续研究/回测物化必须使用 Repository → Kernel 链路，并遵守 WorkScope、Checkpoint、幂等、超时回滚和分块预算约束。

## 文档使用规则

`dev_doc/过时/` 下的内容只用于历史追溯，不作为当前代码、数据库权限或数据状态的依据。代码与 `data_access_kernel.md` 冲突时，默认代码错误。
## 2026-08-18 继续审计后的状态覆盖

- 全量测试：`894 passed, 1 skipped, 8 deselected`。
- 契约扫描：141 个文件，`violations: 0`。
- wave3 的 WR/RSI/KDJ 已下沉至 `feature.technical_daily_v1`，Job 只读取 Feature 输出。
- 候选/回测规则已切换到 `foundation.*` + `backtest.app_execution_params`；策略消费必须命中 `app_strategy_version.approval_status='APPROVED'`。
- `research-v2.1` 已加入策略字段守卫，并与 Foundation v2.0 共享 PIT/Feature 输入契约；没有删除或重建旧 Snapshot。
- Mart 历史查询已切换到 `mart.v_price_feature`，视图只读且保留旧列顺序。
- 新增 `ops.foundation_table_migration` 作为物理迁移的注册、校验、切换、回滚控制面；当前没有执行任何 public→新 schema 的物理搬迁。

因此：访问框架与 Stage 2 的逻辑边界已完成并通过回归；生产库物理拆表仍是后续独立、可回滚的 Stage 3，不得把“控制面已就位”误报为“数据已搬迁”。
