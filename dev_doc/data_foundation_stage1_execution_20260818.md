# Data Foundation Stage 1 生产执行记录（2026-08-18）

本文记录 `sql/049_data_foundation_stage1_contracts.sql` 在本地生产 PostgreSQL（`ashares-postgres`）上的实际执行结果。设计基线仍是 `dev_doc/数据基座架构拆分方案.md`；本文不改变架构契约。

## 执行结果

- PostgreSQL 16.14 已完成 Docker 重启后的 WAL 自动恢复，当前 `pg_is_in_recovery()` 为 `false`，已接受连接。
- 049 已在管理员连接下成功提交，执行耗时约 2.69 秒；迁移连接设置了 120 秒语句超时、5 秒锁等待超时和 180 秒事务空闲超时。
- 迁移为增量、幂等 DDL/元数据迁移，没有移动、覆盖或删除已有 raw、standard、feature、snapshot 事实数据。
- 落库对象及行数：`contract.contract_version` 1、`contract.data_dictionary` 8、`backtest.app_strategy_version` 2、`backtest.app_execution_params` 6、`ops.trading_day_build` 0。

## 权限与约束探针

- `ashares_app` 可读取 contract 元数据，可追加写入 `ops.trading_day_build`。
- `ashares_app` 对 contract 元数据和应用参数表的 UPDATE/DELETE 被权限层拒绝。
- 管理员绕过权限探针时，contract 版本的 UPDATE 仍被数据库 append-only trigger 拒绝。
- `READY` 状态缺少 `completed_at` 时被 `ops.trading_day_build` trigger 拒绝。
- 所有写入探针均在事务中回滚，验证后 `ops.trading_day_build` 仍为 0 行。

## 风险边界

本次执行不代表数据基座全部重构完成。既有 raw/research 表的物理迁移、双读双写切换、完整数据字典、snapshot/contract 治理以及大批量 SET-based SQL 的运行时硬限额仍属于后续阶段。历史重建任务在完成大事务/存储压力防护前不应直接恢复运行。
