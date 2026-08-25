# Data Foundation Stage 3 物理迁移 Pilot

更新时间：2026-08-18

## Pilot 范围

本次只选择小型 `trading_calendar`，没有触碰 `daily_kline`、Raw 大表或任何 FROZEN Snapshot。

| 项目 | 结果 |
|---|---|
| 源表 | `public.trading_calendar` |
| 目标表 | `std.trading_calendar` |
| 源行数 | 3,666 |
| 目标行数 | 3,666 |
| 内容哈希 | `6ce2775fb4de18f3c0d0beeea1b7a8b5191a1e0107bb07bcd1fde539984bb6` |
| 控制面状态 | `CUTOVER` |
| 源表 | 保留，未删除 |
| 读取路径 | 已切换到 `std.trading_calendar` |
| 双写路径 | 已启用并完成单行烟测 |

## 执行约束

- 目标表由 `sql/054_trading_calendar_pilot.sql` 建立。
- 数据复制由 `migrate_trading_calendar.py` 通过 `SchemaMigrationRepository → Kernel → RebuildGateway` 完成。
- 校验使用源/目标行数和按主键排序的 SHA-256 内容哈希。
- 没有执行 `ALTER TABLE ... SET SCHEMA`、DROP、TRUNCATE 或全库重建。
- 双写路径同时写入 `std` 和旧 `public`，计数不一致或任一侧失败会回滚。
- 内容校验通过后由自动化流程登记 `CUTOVER`，不依赖人工确认。

## 下一门禁

当前已进入 `CUTOVER`。双写继续保留，若发现差异，读取路径可以回退到 `public.trading_calendar`，源表作为回滚依据保留。
