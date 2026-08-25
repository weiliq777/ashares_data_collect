# Data Foundation Stage 3：标准表物理迁移进度

更新时间：2026-08-19

本文是 Stage 3 物理迁移的当前记录。物理迁移采用“复制 → 行数与确定性哈希校验 → 自动 CUTOVER → 双写保留”的单一自动流程，不要求人工确认，不删除旧表，不执行全库重建。

## 已完成的 Pilot

| 源表 | 目标表 | 源行数 | 目标行数 | 校验 | 状态 | 运行时读取 |
|---|---|---:|---:|---|---|---|
| `public.trading_calendar` | `std.trading_calendar` | 3,666 | 3,666 | SHA-256 一致 | `CUTOVER` | `std.trading_calendar` |
| `public.stock_basic_info` | `std.stock_basic_info` | 5,883 | 5,883 | SHA-256 一致 | `CUTOVER` | `std.stock_basic_info` |

两张源表均保留，用于回滚校验和双写窗口。`stock_basic_info` 的采集写入已经同时写入旧表与 `std.stock_basic_info`；标准层 Repository 的读路径已全部切换到 `std.stock_basic_info`。

## 约束与验证

- 迁移入口：`SchemaMigrationRepository → DataAccessKernel → RebuildGateway`。
- 复制使用有界 `SqlPlan` 和事务预算；不把大表历史迁移放进单一长事务。
- 控制状态持久化在 `ops.foundation_table_migration`，校验失败不会进入 `CUTOVER`。
- 旧表不删除，生产程序不使用超级用户连接；DDL 由管理员执行，运行时使用 `ashares_app`。
- 每批代码完成后必须通过全量测试和 Data Access Contract scanner。
- `divid_factors` 的旧生产结构已通过 `sql/056_repair_divid_factors_schema.sql` 做加法修复，补齐 `interest`、`allot_price`、`dr`，不改写既有行。

## 下一阶段

下一阶段是大表的分区化迁移，优先评估 `daily_kline`、`adj_factor_daily`、`valuation_daily`。每张表必须先完成分区键、ChunkPlanner、Checkpoint、断点续跑和分批哈希校验，再自动切换；禁止一次性复制整张大表或用单一大事务完成迁移。

## 技术特征历史回填

`feature.technical_daily_v1` 已完成 2016-08-01 至 2026-08-14 的历史回填：

- 10,672,777 行，与 `public.daily_kline` 的覆盖行数一致；
- 2,438 个交易日、5,795 只股票；
- 10,541,522 行 `VALID`，131,255 行标记为 `INSUFFICIENT_HISTORY`；
- 所有 121 个月级范围和 105 个 7 天细分 Checkpoint 均完成，无未完成分块；
- 回填使用 `DataAccessKernel`、`ChunkPlanner`、批量 `VALUES` 写入、事务预算和 PostgreSQL Checkpoint，未执行单一大事务。

当前验证结果：全量测试 `903 passed, 1 skipped, 8 deselected`；契约扫描 `144 files, violations: 0`。
