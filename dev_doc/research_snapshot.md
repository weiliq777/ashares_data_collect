# Research 快照实施设计 v1

更新时间：2026-08-08

当前运行基线：`baseline_20260806` 已通过质量门禁并保存完整 `source_manifest_hash`，状态为 `validated`；旧 `baseline_20260803` 仅保留为 `superseded` 审计记录。正式 `research-v1.3` 快照仍需基于该基线重新生成并完成 `build → validate → freeze`，不得复用旧版快照系列。

多日期生成使用 `data_collect.jobs.research_snapshot_series`：Manifest 采用批量 `execute_values`，股票快照、血缘和输入统计按每批 10 个截止日集合写入；每个 `snapshot_id` 仍保持单日粒度，`validate` 保留逐日审计语义。默认只完成 `build → validate`，正式 `freeze` 仍要求干净 Git 或显式代码归档。

## 1. 目标与边界

Research 快照是 PIT/Feature 到 Research/Backtest 的不可变边界。第一版一个
`snapshot_id` 只对应一个 `research_cutoff_date`；快照生成器可以读取通过质量门禁的
PIT/Feature，Research 分析和 Backtest 只允许读取 `FROZEN` 快照。

本设计已完成可复现基础实现；当前不自动生成新的正式快照、不接入自动交易。数据库中已有的旧
`research-v1` FROZEN 快照只读保留，新正式 Backtest 只能使用重新生成并验收通过的
`research-v1.3` 快照系列。

## 2. 数据对象

迁移文件：`sql/024_create_research_snapshot.sql`

| 对象 | 用途 |
|---|---|
| `research.snapshot_manifest` | 快照身份、基线、代码、质量和状态 |
| `research.snapshot_input` | 本次实际使用的表切片、字段、行数、日期范围和内容哈希 |
| `research.stock_snapshot_daily` | 指定截止日的统一个股研究输入，含 PIT `eligible_by_market` |
| `research.snapshot_lineage` | 快照字段组到 PIT/Feature/Standard 记录的来源血缘 |

`stock_snapshot_daily.research_date` 必须等于 Manifest 的
`research_cutoff_date`，数据库触发器负责强制约束。

## 3. Manifest 契约

必须记录：

- `snapshot_id`：UUID，数据库主键且不可复用；
- `research_cutoff_date`：研究截止日；
- `data_baseline_tag`：必须指向 `tushare_data_release.status='validated'`；
- `schema_version`：Research 模型版本；当前新建快照使用 `research-v1.3`，使用 `feature.price_daily_v2` 的时点总回报指标，并哈希 PIT `eligible_by_market` 及成熟度字段；旧版本 FROZEN 快照只读保留，不能回写升级；
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
feature.price_daily_v2
feature.valuation_daily_v1
pit.financial_asof
public.daily_kline
std.security_status_daily
feature.shareholder_return_v1
public.tushare_stock_basic_raw_version（受 validated baseline record_id 上限约束）
std.security_lifecycle
```

股票名称、市场和历史状态不能直接使用当前 `stock_basic_info` 倒灌到历史日期。
Snapshot 生成时以 `pit.universe_daily` 的上市状态和 `std.security_lifecycle` 的有效区间
为时点权威；`tushare_stock_basic_raw_version` 只提供基准内的代码、静态身份和上市/退市
日期字段，并将尚未到达研究日的日期裁剪为 NULL。

价格 Feature 使用 `feature.price_daily_v2` 的时点总回报坐标：只纳入
`std.dividend_event.implementation_status='实施'` 且 `ex_date <= trade_date` 的公司行为。
`feature.price_daily_v1` 仍保留为只读兼容表，不再作为新 Research/Backtest 的正式输入。
`price_daily_v2` 不依赖后来抓到的最终 `adj_factor`，因此不需要伪造历史 `fetched_at`；
Raw/Standard/PIT 数据仍全部保留并可追溯。

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

## 9. 重跑与正式冻结规则（2026-08-06）

`research.snapshot_manifest` 对 `(research_cutoff_date, data_baseline_tag, schema_version)`
建立唯一逻辑键。`research_snapshot_series.py` 重跑时复用 DRAFT/FAILED 快照，
不重建 VALIDATED/FROZEN 快照；并发发现 BUILDING 快照时直接失败，避免两个任务同时写入。

正式 `freeze` 会再次检查 validated 基线和质量报告哈希，避免 validate 之后基线降级或质量门禁变化仍被发布；只接受干净 Git 工作区，若确需脏工作区，必须提供已存在的代码归档路径。
`diff_hash` 只用于审计，不能单独替代可复现代码。

快照系列任务还会校验每个截止日属于 SSE 开市日；当前正式 Backtest 适配器只接受
`research-v1.3`（包含时点价格 Feature、`eligible_by_market` 及成熟度字段），已有旧 `research-v1`/`research-v1.1`/`research-v1.2` FROZEN 快照只读保留，
不得静默升级或作为正式 Dataset 输入。
## 2026-08-07 当前状态覆盖

Snapshot 基础代码此前生成过三份 `research-v1.1` FROZEN 日快照（2026-07-30、2026-07-31、2026-08-03），并物化到旧版三日烟测 Dataset；它们继续只读保留，不自动升级。当前正式生产目标是重新生成 `research-v1.3` 系列。

阶段三当前仍需补齐完整评价区间的逐 SSE 开市日 Snapshot 系列；三日 Snapshot 仅用于 Baseline 烟测，不能代表五年回测结果。Snapshot 生成器可以读取通过质量门禁的 PIT/Feature，但 Research/Backtest 只能读取 FROZEN 快照。

## 2026-08-08 数据库复核

实现默认版本已升级为 `research-v1.3`，并启用成熟度/PIT 检查、时点价格 Feature 及 Snapshot 输入边界锁。数据库当前没有 FROZEN `research-v1.3` 快照；已有 `research-v1.1` 记录为历史数据：690 条 FROZEN、1 条 DRAFT，`2021-08-02` 至 `2026-08-03` 的 SSE 开市日缺少 522 个，且 FROZEN 记录存在 dirty 代码/基线哈希缺口。它们只读保留，不能作为当前 Backtest Dataset v2 的正式输入。正式生产必须在干净代码版本上重新生成 `research-v1.3` 系列。

当前 `baseline_20260803` 的数据库记录虽然为 `validated`，但 `source_manifest_hash/source_manifest` 为空；现行代码会在 Snapshot 创建前拒绝该基线。这是保护性门禁，不应通过手工填空值绕过；必须在 Raw/Standard/PIT/Feature 稳定后发布带完整 Source Manifest 的新基线。

## 快照序列自动验收、续跑与冻结

统一入口为 `data_collect.jobs.research_snapshot_series_ops`。该入口复用现有
`ops.data_access_checkpoint`，不要求新增数据库迁移。

后台历史构建运行期间只允许执行 `monitor`。它只读取 `std.trading_calendar` 和
`research.snapshot_manifest` 两个小范围索引切片，不扫描股票快照、血缘或上游大表：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.research_snapshot_series_ops monitor `
  --start-date 2021-08-02 `
  --end-date 2026-08-07 `
  --baseline-tag foundation_v2_20260807 `
  --schema-version research-v2.0 `
  --expected-snapshots 1216 `
  --mode progress
```

历史进程确认结束后，续跑器先校验已有 `BUILDING`，再对 `DRAFT/FAILED` 执行小批次
`build → validate`，每批完成后写 checkpoint。该命令不提供 `--rebuild-building`，不会把
`BUILDING` 自动改回 `FAILED`：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.research_snapshot_series_ops resume `
  --start-date 2021-08-02 `
  --end-date 2026-08-07 `
  --baseline-tag foundation_v2_20260807 `
  --schema-version research-v2.0 `
  --expected-snapshots 1216 `
  --batch-size 2
```

收口器只有在日期完整且全部处于 `VALIDATED/FROZEN` 时才能启动。首次运行必须全部为
`VALIDATED`；中断续跑只接受按日期连续的 `FROZEN` 前缀。它先按小批次完成全系列行数、
输入 Manifest 哈希、血缘、PIT 和质量证据检查，在任何冻结写入前收齐全部验收结果；随后
每批冻结前再次复检，避免 `VALIDATED` 子表在全局预检后发生变化：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.research_snapshot_series_ops finalize `
  --start-date 2021-08-02 `
  --end-date 2026-08-07 `
  --baseline-tag foundation_v2_20260807 `
  --schema-version research-v2.0 `
  --expected-snapshots 1216 `
  --batch-size 10 `
  --code-archive-path <code-archive.zip>
```

`resume` 和 `finalize` 共用同一个序列级 PostgreSQL advisory lock，防止两个新操作入口
并发执行。旧版 `research_snapshot_series` 未持有该锁，因此仍必须先从操作系统和任务日志
确认旧历史进程已经退出；不得仅凭 BUILDING 时间较久就自动接管。
