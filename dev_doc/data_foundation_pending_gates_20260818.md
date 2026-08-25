# Data Foundation 待人工批准门禁

更新时间：2026-08-18

## 1. 策略版本审批

当前 `backtest.app_strategy_version` 中已有策略记录的状态是 `MIGRATED`，尚无 `APPROVED` 版本。

`MIGRATED` 只表示从旧 `backtest.strategy_definition` 迁移而来的审计证据，不代表已经完成策略审核。因此：

- 候选生成拒绝消费未批准策略；
- 正式回测拒绝创建 Run；
- 程序不会自动把 `MIGRATED` 改成 `APPROVED`；
- 人工审核通过后，必须显式发布一个带批准人、批准时间和证据哈希的 APPROVED 版本。

这是故意的 fail-closed 门禁，不是运行故障。

## 2. 技术指标历史数据

`feature.technical_daily_v1` 当前行数为 0，因为本轮只完成了代码、表结构、Policy、Repository 和增量写入路径，没有执行历史回填。

- 后续增量技术指标任务会写入新到达的数据；
- 若需要历史 wave3 结果，必须单独批准有边界的历史回填范围；
- 回填必须使用 Chunk、Checkpoint、超时回滚和幂等 UPSERT；
- 不允许借此触发全量历史重建或覆盖既有 Snapshot。

## 3. 当前结论

以上两项均已登记为人工门禁。代码已经完成保护，但在批准策略和批准回填范围之前，系统应继续拒绝对应的正式消费。

## 4. 物理迁移 Pilot

`trading_calendar` Pilot 已完成源表到 `std.trading_calendar` 的复制、哈希校验、单行双写烟测和自动切换，控制面为 `CUTOVER`。源表仍保留，双写继续运行，可在异常时回退读取路径。
