# Research 快照实施设计 v1

更新时间：2026-08-05

## 1. 目标与边界

Research 快照是 PIT/Feature 到 Research/Backtest 的不可变边界。第一版一个
`snapshot_id` 只对应一个 `research_cutoff_date`；快照生成器可以读取通过质量门禁的
PIT/Feature，Research 分析和 Backtest 只允许读取 `FROZEN` 快照。

本阶段只建立可复现基础，不自动生成正式快照、不接入自动交易。

## 2. 数据对象

迁移文件：`sql/024_create_research_snapshot.sql`

| 对象 | 用途 |
|---|---|
| `research.snapshot_manifest` | 快照身份、基线、代码、质量和状态 |
| `research.snapshot_input` | 本次实际使用的表切片、字段、行数、日期范围和内容哈希 |
| `research.stock_snapshot_daily` | 指定截止日的统一个股研究输入 |
| `research.snapshot_lineage` | 快照字段组到 PIT/Feature/Standard 记录的来源血缘 |

`stock_snapshot_daily.research_date` 必须等于 Manifest 的
`research_cutoff_date`，数据库触发器负责强制约束。

## 3. Manifest 契约

必须记录：

- `snapshot_id`：UUID，数据库主键且不可复用；
- `research_cutoff_date`：研究截止日；
- `data_baseline_tag`：必须指向 `tushare_data_release.status='validated'`；
- `schema_version`：Research 模型版本；
- `code_version`：Git commit，工作区有修改时追加 `+dirty`；
- `git_commit`、`git_dirty`、`diff_hash`、`code_archive_path`：代码可复现信息；
- `quality_report_hash`、`quality_report`：生成前质量门禁结果；
- `input_manifest_hash`：所有输入统计的汇总哈希；
- `status`：`DRAFT → BUILDING → VALIDATED → FROZEN`，失败可从 `FAILED` 重新构建。

`FROZEN` 必须同时具备质量哈希和输入哈希。工作区不干净时，必须保留
`diff_hash` 或代码归档路径。数据库触发器禁止非法状态跳转、删除 Manifest，
并禁止修改或删除 `FROZEN` 的所有子表记录。

## 4. 输入切片与哈希

生成器固定定义本次使用的表、筛选条件和字段集合，当前包括：

```text
pit.universe_daily
pit.security_tradeability_daily
feature.price_daily_v1
feature.valuation_daily_v1
pit.financial_asof
feature.shareholder_return_v1
stock_basic_info
std.security_lifecycle
```

`content_hash` 只计算本次切片和选定字段，不计算整张物理表。每个输入记录：

```text
selection_predicate
selected_columns
hash_algorithm = sha256
row_count
min_date / max_date
content_hash
```

行内容按固定主键排序、规范化日期/NULL/数值后逐行计算 SHA-256。
`input_manifest_hash` 对 schema、表名、筛选条件、字段集合、统计值和内容哈希
做规范化汇总，避免“同样数据但查询字段不同”仍被误认为同一快照。

## 5. 一致性生成流程

`data_collect/jobs/research_snapshot.py` 的 `build` 使用 PostgreSQL
`REPEATABLE READ` 事务：

```text
质量门禁
→ 读取并绑定 validated baseline
→ 创建/更新 BUILDING Manifest
→ 同一事务读取 PIT/Feature
→ 写 stock_snapshot_daily
→ 写 snapshot_lineage
→ 计算并写 snapshot_input
→ 计算 input_manifest_hash
→ 提交
```

如果增量任务在生成过程中提交新版本，快照事务仍只看到一致的已提交输入；
校验阶段会重新计算输入哈希，若输入在构建后发生变化，则校验失败，必须重新构建。

## 6. 血缘规则

每个快照个股固定记录以下字段组：

```text
universe
tradeability
price
valuation
financial
shareholder_return
lifecycle
instrument
```

每条血缘记录保存：

```text
source_table
source_record_key
source_record_version
source_payload_hash
```

当上游缺失时仍保存 `:missing` 占位键，并由快照质量状态标记缺失，不能伪造来源记录。

## 7. 命令

正式快照生成前，先确认基线和质量门禁：

```powershell
\.venv\Scripts\python.exe -m data_collect.jobs.research_snapshot create `
  --cutoff-date 2026-08-03 `
  --baseline-tag baseline_20260803
```

随后使用返回的 `snapshot_id`：

```powershell
\.venv\Scripts\python.exe -m data_collect.jobs.research_snapshot build --snapshot-id <id>
\.venv\Scripts\python.exe -m data_collect.jobs.research_snapshot validate --snapshot-id <id>
\.venv\Scripts\python.exe -m data_collect.jobs.research_snapshot freeze --snapshot-id <id>
```

只有 `FROZEN` 快照可以进入正式 Backtest。`VALIDATED` 仅用于检查和 Research 预览。

## 8. 验收要求

- 相同输入切片生成相同 `content_hash`；字段或筛选条件变化会改变哈希；
- 截止日、基线截止日和股票快照日期约束通过；
- 输入统计覆盖所有实际使用表；
- 每个快照个股具备 8 个血缘字段组；
- 质量门禁失败不能创建可用快照；
- `DRAFT/BUILDING/VALIDATED/FROZEN/FAILED` 状态流转符合规则；
- `FROZEN` 后 Manifest、输入统计、股票快照和血缘的更新/删除均失败；
- 正式 Backtest 接口拒绝非 `FROZEN` 快照。

