# Market Activity Persistence（市场活跃度持久化）

这份文档只描述后端保存模型；展示层另行实现。

## 数据链路

```text
std.daily_kline
      ↓ Repository → Kernel → ReadGateway
MarketActivityReport
      ↓ Repository → Kernel → WriteGateway
observation.market_activity_snapshot   交易日级父记录
      ├─ observation.market_activity_daily  每日市场汇总
      └─ observation.stock_activity_daily   每日股票明细
```

该域只写观察域新表，不改 Raw（原始层）、Standard（标准层）、PIT（时点层）、Feature（特征层）、Research Snapshot（研究快照）或 Backtest（回测）数据。供应商主力/超大单等估算型资金流不写入这三张表，未来应独立建设 Raw → Standard 数据域。

## 业务身份

每日构建使用一条父记录，当前 `activity_date_start` 和 `activity_date_end` 都等于目标交易日。父记录的业务身份为：

```text
activity_date_start
activity_date_end
methodology_hash
data_baseline_id
```

`methodology_hash` 会随窗口、Top-N 等计算参数变化；`data_baseline_id` 必须显式指定已验证数据基线。不同计算口径不能覆盖同一天旧结果。

## 保存内容

`observation.market_activity_snapshot` 保存生命周期、计算运行号、代码身份、输入哈希、质量报告哈希和行数统计。

`observation.market_activity_daily` 每天一行，保存全市场成交量/成交额、Top-N 集中度、输入行数、质量状态和结果哈希。

`observation.stock_activity_daily` 每个股票每天一行，保存成交量、成交额、市场占比、成交量/成交额排名、相对前置窗口均值的比值、历史观察条数、原因码和计算状态。

每条结果保存：

- `input_manifest_hash`：本次读取的 Standard 输入行及字段顺序哈希；
- `activity_hash`：方法、输入和计算结果的稳定哈希；
- `calculation_run_id`：运行审计身份；
- `quality_status` / `calculation_status`：完整性与历史窗口状态；
- `reason_codes`：例如 `INSUFFICIENT_LOOKBACK`，未知不填充为 0。

## 生命周期与幂等

```text
DRAFT → BUILDING → VALIDATED → FROZEN
                    └────────→ FAILED
```

同一业务身份重复运行时，FROZEN 或 VALIDATED 默认幂等跳过；DRAFT/FAILED 才继续构建。FROZEN、SUPERSEDED、CORRUPTED 由数据库触发器阻止子表新增、更新和删除。写入按 Gateway 分块，校验通过后才进入 VALIDATED，避免把长时间的大事务作为每日任务边界。

迁移文件为 `sql/105_market_activity_domain.sql`。本次只完成模型、Policy、Repository、Builder 和测试开发，未执行生产库迁移，也未启动历史回填。
