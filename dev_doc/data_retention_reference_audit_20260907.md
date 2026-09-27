# 数据保留与兼容引用审计

审计日期：2026-09-07
范围：生产 PostgreSQL、Repository/Job 真实代码、Research Snapshot（研究快照）、Backtest Dataset（回测数据集）和回测运行记录。
审计方式：只读查询与静态代码检索；本审计未删除、迁移或修改生产数据。

## 结论

当前不能直接清理所有带有 `v1`、`_legacy` 或 `corrupt` 后缀的对象。系统实际处于以下状态：

```text
当前正式链路：Foundation-v2 Snapshot
历史复现链路：v1 Snapshot + research.snapshot_lineage
兼容输入链路：company_financial_v1、price_daily_v1、valuation_daily_v1
恢复链路：*_raw_corrupt_20260811
回滚保留：public._legacy_*、research._legacy_*
```

## 快照和数据集实际引用

`research.snapshot_manifest` 当前存在两套快照系列：

| 血缘版本 | 状态 | 数量 | 日期范围 |
|---|---:|---:|---|
| v1 | FROZEN（冻结） | 3127 | 2021-08-02 ～ 2026-08-07 |
| v1 | VALIDATED（已验证） | 1215 | 2021-08-02 ～ 2026-08-06 |
| v1 | BUILDING/DRAFT（构建中/草稿） | 1212 | 历史未完成记录 |
| v2 | FROZEN（冻结） | 1226 | 2021-08-02 ～ 2026-08-21 |
| v2 | VALIDATED（已验证） | 1 | 2023-09-12 |
| v2 | BUILDING（构建中） | 709 | 历史未完成记录 |

当前数据集与快照的实际关系如下：

| 数据集 | 数据集状态 | 实际绑定的快照血缘 | 快照数 | 回测运行数 |
|---|---|---|---:|---:|
| `smoke-baseline` | VALIDATED | v1 | 3 | 0 |
| `smoke-baseline-v2` | FROZEN | v1 | 3 | 3 |
| `history-baseline-v13` | VALIDATED | v1 | 1215 | 0 |
| `history-baseline-v13-final` | FROZEN | v1 | 1215 | 2 |
| `smoke-batched-20260809` | VALIDATED | v1 | 3 | 0 |
| `foundation-v2-smoke-20260813` | FROZEN | v1 | 3 | 0 |
| `foundation-v2-full-20260821` | FROZEN | v2 | 1226 | 10 |

因此，`backtest-dataset-v2` 这个 schema/version 名称本身不能证明其绑定了 v2 快照；必须通过 `dataset_member.research_snapshot_id` 反查 `snapshot_manifest.lineage_storage_version`。

## 旧血缘表 `research.snapshot_lineage`

当前约 311,950,784 行、约 181GB。

它仍然有三类真实用途：

1. v1 冻结快照的历史复现；
2. `migrate_research_snapshot_lineage_v2.py` 将旧逐字段血缘转换为 v2 紧凑血缘包；
3. `research_watchlist_repository.py` 在 v2 血缘不存在时的兼容回退。

当前 v2 正式链路使用：

```text
research.snapshot_lineage_bundle
research.lineage_source_record
```

`snapshot_lineage_bundle` 是按月份分区的父表，父表本身显示为 0 物理页并不代表为空；抽样确认以下日期均有实际数据：

```text
2021-08-02：4477 行
2023-09-12：10550 行
2026-08-21：11100 行
```

结论：旧血缘可以规划归档，但当前不能删除。至少要先完成 v1 数据集/回测引用迁移或归档、历史复现演练、v1 与 v2 等价性验收。

## Feature（特征层）兼容表

### `feature.company_financial_v1`

不能清理。当前 `research_snapshot_repository.py` 的研究快照构建 SQL 仍直接从该表读取财务时点字段和历史财务摘要；质量检查和旧财务特征 Repository 也仍有引用。

`company_financial_v2` 已用于 Foundation-v2 的正式财务血缘契约，但不代表 v1 物理表已经脱离当前快照构建逻辑。

### `feature.price_daily_v1`

不是当前每日主生产链路，当前主链路使用 `feature.price_daily_v2`。但是：

- `tushare_data_quality.py` 仍检查该表；
- `feature_repository.py` 仍保留其显式重建入口；
- `tushare_phase2_incremental.py` 和 `tushare_derived_rebuild.py` 将其定义为只读历史兼容表。

因此它目前属于“可归档候选”，不是“可立即删除对象”。必须先移除或隔离质量检查和旧重建入口，并确认旧 Dataset/Run 不再依赖。

### `feature.valuation_daily_v1` 和 `feature.technical_daily_v1`

不能因后缀为 v1 就清理。当前研究快照和观察域仍使用它们作为输入或派生结果。它们不是单纯的旧兼容表。

## 回滚表和恢复副本

迁移台账 `ops.foundation_table_migration` 记录的切换时间：

| 对象 | 切换时间 |
|---|---|
| `std.trading_calendar`、`std.stock_basic_info` | 2026-08-18 |
| `research.stock_snapshot_daily_partitioned` | 2026-08-25 |
| `std.daily_kline`、`std.adj_factor_daily`、`std.valuation_daily` | 2026-08-25 |
| 财务 Standard（标准层）表 | 2026-08-25 |

当前所有迁移记录的 `rollback_at` 均为空，说明没有实际执行过回滚。

现存 `_legacy_*` 表属于 2026-08-18/25 迁移窗口产生的回滚保留对象。名称中的哈希不是时间戳，PostgreSQL 也不保存普通表的创建时间，因此不能从表名直接得到精确创建时刻。

另有：

```text
public.tushare_daily_raw_corrupt_20260811
public.tushare_daily_basic_raw_corrupt_20260811
```

这两张表不是普通回滚备份。`recover_daily_raw.py` 和 `recover_daily_basic_raw.py` 明确把它们作为恢复源，暂时不能删除。

## 当前清理建议

### 必须保留

- `research.snapshot_lineage_bundle` 及其分区；
- `research.lineage_source_record`；
- v1 冻结快照及其旧血缘；
- `feature.company_financial_v1`；
- `feature.valuation_daily_v1`、`feature.technical_daily_v1`；
- `*_raw_corrupt_20260811` 恢复源。

### 暂时只读保留

- `feature.price_daily_v1`；
- `public._legacy_*`；
- `research._legacy_stock_snapshot_daily_a079a5b1897c`。

### 可进入下一轮治理，但不能直接删除

- 没有回测运行引用的 smoke Dataset；
- v1 的 BUILDING/DRAFT 快照；
- v2 的 BUILDING 快照；
- 已无业务读取入口的 `price_daily_v1` 兼容路径。

下一轮应先建立“归档而非删除”的操作：将对象标记为 `ARCHIVE_CANDIDATE`，保存引用审计结果、恢复演练记录和保留截止日；通过保留期后再执行分批、可恢复的物理清理。

## 2026-09-07 未完成快照审计结果

审计时 Windows 进程、PostgreSQL 活动查询均未发现 Research Snapshot（研究快照）构建进程。

未完成快照分布：

| 血缘版本 | 状态 | 数量 | 主要时间信息 | 外部引用 |
|---|---|---:|---|---:|
| v1 | BUILDING | 610 | 2026-08-13 创建，最晚构建到 2026-08-13 | 0 |
| v1 | DRAFT | 602 | 2026-08-07 ～ 2026-08-13 创建 | 0 |
| v2 | BUILDING | 709 | 2026-09-06 创建，最晚构建到 2026-09-06 15:41 | 0 |

“外部引用”本轮检查了：

- `backtest.dataset_member`（回测数据集成员）；
- `backtest.signal`（回测信号）；
- `research.watchlist_member`（观察列表成员）；
- `research.evidence_reference`（证据引用）。

上述未完成快照在这些表中均没有引用。因此它们可以进入 `ARCHIVE_CANDIDATE`（归档候选）清单，但还不能直接改成 FAILED（失败）或删除：仍需保留其快照 ID、创建参数和可能存在的局部物化数据，供故障审计和恢复判断。

## 无回测运行引用的数据集

以下数据集当前没有 `backtest.run_manifest`（回测运行清单）引用，属于归档候选：

```text
smoke-baseline
history-baseline-v13
smoke-batched-20260809
foundation-v2-smoke-20260813
```

以下数据集已有成功或失败运行记录，暂不归档：

```text
smoke-baseline-v2
history-baseline-v13-final
foundation-v2-full-20260821
```

本轮只完成引用审计，没有修改状态、删除 Dataset（数据集）或删除 Snapshot（快照）。

## 归档候选登记结果

已创建并应用 `sql/100_retention_archive_registry_20260907.sql`，登记表为：

```text
ops.retention_archive_registry
```

当前登记结果：

| 对象 | 状态 | 数量 |
|---|---|---:|
| Snapshot | BUILDING | 1319 |
| Snapshot | DRAFT | 602 |
| Dataset | VALIDATED | 3 |
| Dataset | FROZEN | 1 |

所有记录当前均为 `CANDIDATE`（候选）状态。登记表保存来源状态、引用摘要、输入哈希/规则哈希和审计指纹；没有任何物理数据被删除或改写。

归档状态只能经过：

```text
CANDIDATE → APPROVED → ARCHIVED
                 │
                 └────→ RESTORED
```

本次没有执行 `APPROVED` 或 `ARCHIVED`，因此当前仍可完整恢复和继续审计。
