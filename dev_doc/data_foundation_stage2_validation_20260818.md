# Data Foundation Stage 2 验证记录（2026-08-18）

## 本轮已完成

- 生产库只读目录检查确认 `feature.price_daily_v2`、`feature.valuation_daily_v1` 和 `research.stock_snapshot_daily` 均为大表；验证过程禁止精确全表 `COUNT(*)`，仅使用系统目录和近似统计，避免验证制造压力。
- 新增并执行 `sql/050_mart_contract_views.sql`，建立只读 `mart.v_price_feature`。该迁移只改变视图元数据，不复制、不更新、不删除 Feature/Snapshot 数据。
- `EXPLAIN` 验证按 `trade_date` 查询可使用 `price_daily_v2` 日期索引，并通过 valuation 主键连接；没有执行 `ANALYZE`，也没有启动大范围查询。
- `ashares_app` 对视图只有 SELECT 权限，INSERT/UPDATE/DELETE 均无权限；应用侧 bounded read 已成功执行。
- DatasetPolicy 新增 `mart.*` 与 `mart.v_price_feature` 的只读、必须携带 Scope 约束。
- 阶段测试：`884 passed, 1 skipped, 8 deselected`；数据访问契约扫描 138 个文件、0 违规。

## 访问内核边界修复

- `ReadGateway.query/scalar` 和 Repository 私有读桥接现在统一经过 `TransactionManager` 的独立短事务。
- 流式读取完成后提交；调用方提前中断或读取异常时回滚；在 Repository `run_atomic` 中不会产生子事务提交。
- 这避免 Snapshot 输入统计/内容哈希的长读事务跨越后续写入，符合 `data_access_kernel.md` §6 的事务边界要求。
- 新增行为测试覆盖独立读事务和 atomic 工作流，当前相关测试 67 个全部通过。
- Snapshot 输入统计/内容哈希已移到 `session_exclusive` 之前；物化锁区间不再包含源表统计和内容哈希扫描。

## 尚未完成的 Stage 2 项

1. `wave3.py` 仍在 Job 层计算 WR/RSI/KDJ，需要新增 Feature Repository 输出并让 Job 只负责筛选/通知。
2. 候选/回测消费路径仍读取旧的 `backtest.strategy_definition` 与 `backtest.market_rule_effective`；需要完成从 `app_strategy_version` / `app_execution_params` 的只读切换，并保留旧表只作为迁移证据，不能继续双写造成口径分叉。
3. Snapshot 去策略化和 `research-v2.1` 等价性验证尚未开始。

在上述两条消费路径完成前，不宣称 Stage 2 完成，也不启动历史重建或全量回填。
## 2026-08-18 后续执行覆盖

以上“尚未完成”是本记录创建时的状态，已由后续实现覆盖：wave3 Feature 下沉、规则新表只读切换、research-v2.1 策略字段守卫、Mart 视图消费和物理迁移控制面均已完成，并由全量测试 `894 passed, 1 skipped, 8 deselected` 与契约扫描 141 文件零违规验证。历史数据仍未重建；物理 schema 搬迁仍需单独经过双读/双写、哈希校验、人工批准和可回滚切换。
