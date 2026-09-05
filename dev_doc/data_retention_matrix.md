# 数据表白名单与遗留保留矩阵

> 本文件是数据治理与清理顺序的执行契约，不授权直接删除数据。
> 代码访问边界仍以 `dev_doc/data_access_kernel.md`（数据访问内核设计契约）为最高约束；
> 实际运行状态以数据库中的 Manifest（清单）、Policy（策略）和质量报告为准。
>
> 盘点基准日：2026-09-05。所有“删除/归档”结论都必须重新执行引用审计后才能实施。

## 1. 当前正式白名单

以下对象属于当前生产数据链路，默认 `KEEP`（保留），不得作为遗留表清理：

| 层级 | 正式对象 | 用途 | 允许的消费者 |
| --- | --- | --- | --- |
| Raw（原始层） | `public.tushare_*_raw`、`public.tushare_*_raw_version` | 保存接口原始事实、版本、抓取时间和哈希 | Raw/Standard Repository（原始层/标准层仓储）、审计 |
| Standard（标准层） | `std.daily_kline`、`std.valuation_daily`、`std.adj_factor_daily` | 正式行情、估值、复权事实 | PIT/Feature Repository（时点层/特征层仓储） |
| Standard（标准层） | `std.stock_basic_info`、`std.trading_calendar`、`std.security_*`、`std.financial_*_standard` | 标准证券、日历、状态和财务事实 | PIT/Feature/Research Repository（时点层/特征层/研究仓储） |
| PIT（时点层） | `pit.universe_daily`、`pit.security_tradeability_daily`、`pit.financial_asof` | 还原指定日期可见信息和可交易状态 | Feature/Research/Backtest Repository（特征/研究/回测仓储） |
| Feature（特征层） | `feature.price_daily_v2`、`feature.valuation_daily_v1`、`feature.company_financial_v1/v2`、`feature.technical_daily_v1`、`feature.shareholder_return_v1` | 可复用、带版本的事实型特征 | Research/Observation/Backtest Repository（研究/观察/回测仓储） |
| Research（研究域） | `research.snapshot_manifest`、`research.stock_snapshot_daily*`、`research.snapshot_input` | 冻结研究快照及输入清单 | Backtest/Research Repository（回测/研究仓储） |
| Research（研究域） | `research.snapshot_lineage_bundle`、`research.lineage_source_record` | 当前 v2 紧凑血缘和来源字典 | 审计、复现、研究仓储 |
| Backtest（回测域） | `backtest.dataset_manifest` 及其冻结子表 | 冻结回测数据集和规则快照 | Backtest Engine（回测引擎） |
| Ops（控制面） | `ops.*` | Checkpoint（检查点）、Run Manifest（运行清单）、质量和发布记录 | Kernel/Repository（内核/仓储） |

其中 `feature.*` 的 `v1` 后缀不是清理依据。只有经过引用审计确认不再是正式输入，且存在替代版本并完成等价性验证后，才可以降级为兼容对象。

## 2. 只读兼容与回滚对象

以下对象默认 `RETAIN_READONLY`（只读保留），不参与新生产写入：

| 对象 | 当前角色 | 解除保留条件 |
| --- | --- | --- |
| `feature.price_daily_v1` | 旧价格特征兼容表；新 Research/Backtest（研究/回测）使用 `price_daily_v2` | 全部旧 Dataset/Run（数据集/运行）和人工脚本完成引用审计，并保留必要归档 |
| `research.snapshot_lineage` | v1 逐字段组血缘；当前 Foundation-v2 使用 v2 bundle（紧凑血缘包） | v2 bundle 全量等价校验、冻结系列引用审计、恢复演练和保留期完成 |
| `public._legacy_*` | Standard 物理切换后的回滚备份 | 新表稳定运行、恢复演练通过、保留期到期 |
| 旧 `research.snapshot_manifest` / `backtest.dataset_manifest` | 历史复现和审计记录 | 仅可按正式 retention（保留）规则归档；不得因不是当前版本而直接删除 |

特别规则：`SUPERSEDED`（正常替代）可以只读复现；`REVOKED/CORRUPTED`（撤销/污染）只能保留审计证据，不得作为新研究或回测输入。

## 3. 待审计候选，不代表可以删除

以下对象仅进入 `REVIEW`（待审计）队列：

| 类别 | 典型对象 | 必须先完成的检查 |
| --- | --- | --- |
| 测试/烟测表 | `feature.*_smoke_*` | 查找 DB 外键、代码、脚本和文档引用；确认不属于冻结清单 |
| 旧快照中的 DRAFT/BUILDING/FAILED 记录 | `research.snapshot_manifest` 子集 | 按 run_id、snapshot_id 和子表完整性判断，不能按状态单独删除 |
| 旧旁路业务表 | `public.fin_*`、`sector_stock`、`index_weight`、ETF/资金流表 | 确认是否仍由手动任务、Wave3（旧研究流程）或报告使用 |
| Raw 隔离/损坏表 | `public.*_raw_corrupt_*` | 确认恢复任务、重放任务和保留期限全部结束 |
| 旧控制面状态 | `ops.run_manifest` 中陈旧 `RUNNING`、历史 `FAILED` | 通过进程、数据库活动、心跳和原始错误审计后再做状态修复；不得物理删除运行记录 |

“未出现在当前 scheduler（调度器）配置”不等于“无用”。手动回补、恢复、审计和历史复现任务也属于有效消费者。

## 4. 禁止的清理方式

1. 禁止按表名包含 `v1`、`legacy`、`old` 或不在当前配置中作为删除条件。
2. 禁止用全表 `TRUNCATE` 或无边界 `DELETE` 清理历史数据。
3. 禁止在当前 FROZEN（冻结）快照、回测数据集或来源字典仍有引用时删除其输入或血缘。
4. 禁止通过公共旧别名重新访问已切换到 `std.*` 的正式表。
5. 任何物理清理必须先生成：引用清单、行数/哈希证据、恢复方案、保留期判断和小批量执行计划。

## 5. 清理执行顺序

```text
建立引用清单
  → 核对 FROZEN/VALIDATED（冻结/已验证）对象引用
  → 对候选表做行数、哈希和抽样对账
  → 归档而不是直接删除
  → 小批量、可恢复地清理
  → VACUUM/空间回收与恢复演练
```

在当前阶段，唯一可以直接推进的是代码入口修复、引用审计和回测数据集构建；旧血缘、回滚备份和 Raw 隔离表暂不物理删除。
