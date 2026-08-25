# Data Foundation Stage 3 验收记录

更新时间：2026-08-19

## 本次完成

1. `public.stock_basic_info` → `std.stock_basic_info`：5,883/5,883 行，确定性 SHA-256 一致，自动 `CUTOVER`，运行时读取已切换，旧表保留并继续双写。
2. `feature.technical_daily_v1` 历史回填：2016-08-01 至 2026-08-14，共 10,672,777 行；覆盖 2,438 个交易日和 5,795 只股票。
3. 修复生产 `divid_factors` 旧表缺少 `dr` 等列的问题，采用加法 DDL，不删除、不重写历史行。
4. 批量写入从生产路径的逐行 `executemany` 改为有界 `execute_values`；读窗口、输出窗口和写事务均有上限，Checkpoint 支持断点续跑。

## 验收结果

- 技术特征：`VALID=10,541,522`，`INSUFFICIENT_HISTORY=131,255`；重复运行可幂等。
- Checkpoint：121 个月级范围和 105 个 7 天子块全部完成，未完成分块为 0。
- PostgreSQL：回填结束后无活动事务、无锁等待，服务健康。
- 全量测试：`903 passed, 1 skipped, 8 deselected`。
- Data Access Contract：扫描 144 个文件，`violations: 0`。

## 尚未完成

整体数据基座还没有宣称全部物理迁移完成。`daily_kline`、`adj_factor_daily`、`valuation_daily` 等大表仍保留在原表，下一阶段必须以分区、双写、分批校验和可回滚切换的方式逐表迁移；这不影响本次已完成的标准表切换和技术特征使用。
