# Tushare 历史库初始化与每日同步

## 当前架构

历史初始化和每日增量均遵循：

```text
Tushare → Raw 原始层 → Standard 标准层 → 质量检查
```

Raw 表不可修改或删除。行情接口每次抓取的版本保存在 `tushare_market_raw_version`；财务报表版本保存在 `tushare_financial_raw_version`。

## 历史初始化

历史任务：`data_collect.jobs.tushare_history_init`

默认只写 Raw：

```text
normalize_standard=False
```

`normalize_standard=True` 已被禁止。历史 Raw 完成后，必须独立运行 Raw→Standard 转换任务。

覆盖内容：

- `tushare_trade_cal_raw`：Tushare `trade_cal`，交易日历
- `tushare_daily_raw`：Tushare `daily`，原始日线
- `tushare_daily_basic_raw`：Tushare `daily_basic`，原始估值快照
- `tushare_adj_factor_raw`：Tushare `adj_factor`，原始复权因子
- `tushare_fina_indicator_raw`：Tushare `fina_indicator`，原始财务指标
- `tushare_income_raw`：Tushare `income`，利润表原始数据
- `tushare_balancesheet_raw`：Tushare `balancesheet`，资产负债表原始数据
- `tushare_cashflow_raw`：Tushare `cashflow`，现金流量表原始数据

历史 Raw 完成后执行：

```powershell
.venv\Scripts\python.exe -m data_collect.jobs.tushare_raw_to_standard --table all
.venv\Scripts\python.exe -m data_collect.jobs.tushare_financial_raw_version_backfill --dataset all
.venv\Scripts\python.exe -m data_collect.jobs.tushare_financial_raw_to_standard --dataset all
.venv\Scripts\python.exe -m data_collect.jobs.tushare_data_quality
```

## 每日增量

每日行情增量任务使用：

```text
data_collect.jobs.tushare_market_incremental_by_date
```

默认回补最近 5 个交易日：

```text
daily       → tushare_daily_raw       → daily_kline
daily_basic → tushare_daily_basic_raw → valuation_daily
adj_factor  → tushare_adj_factor_raw  → adj_factor_daily
```

重复运行允许捕获接口修正；Standard 按交易日重建，Raw 版本表按内容哈希幂等保存。正式任务不能使用 `--limit-stocks` 或 `--dry-run`。

该任务对 `daily`、`daily_basic`、`adj_factor` 都调用 `trade_date=YYYYMMDD`，每次运行每个数据集每个交易日只请求一次；最近 5 个交易日允许重复回补，以便捕获接口修正。Raw 仍通过版本表幂等保存，Standard 按日重建。正式任务示例：

```powershell
.venv\Scripts\python.exe -m data_collect.jobs.tushare_market_incremental_by_date --lookback-days 5 --batch-pause 1
```

`--limit-stocks` 和 `--dry-run` 仅用于小范围验证，不能用于正式全市场同步。

## 历史交易日缺口补采

历史初始化的旧 checkpoint 只能作为审计信息，不能单独证明 Raw 表确实存在该交易日数据。正式验收时必须用 SSE `trade_cal` 的开市日集合分别与以下两张 Raw 表做日期覆盖差集：

- `tushare_daily_raw`：Tushare `daily`，原始日线行情；
- `tushare_daily_basic_raw`：Tushare `daily_basic`，原始估值快照。

缺失日期使用独立任务补采，任务只追加 Raw，不删除、更新或覆盖已有原始记录：

```powershell
.venv\Scripts\python.exe -m data_collect.jobs.tushare_market_gap_repair `
  --start-date 20210802 --end-date 20260803 `
  --dataset all --batch-size 20 --pause 2
```

该任务不信任历史初始化的 `done` 状态，而是以 Raw 实际日期覆盖为准；成功条件是 Tushare 返回非空且数据库对应 Raw 表出现该日期记录。失败或空响应会保留为独立 checkpoint，便于续传和审计。Raw 补齐后必须重新转换对应 Standard，并重建依赖日线覆盖的状态、PIT 和 Feature。

2026-08-04 已完成本轮缺口修复：`daily`、`daily_basic`、`adj_factor` 均覆盖 1212/1212 个开市日；Standard/PIT/Feature 重建及质量审计通过。

历史兼容脚本 `tushare_daily_incremental.py` 和逐股票复权重试脚本仅用于历史遗留任务或按代码降级排障，不能作为正常每日任务：

```text
data_collect.jobs.tushare_adj_factor_incremental_retry
```

正常每日任务必须优先使用按交易日脚本；接口达到返回上限时由正式脚本自动按代码分片，并拒绝把截断结果标记为完成。

历史 Raw/Standard 验收后，使用 `tushare_release.py` 建立并验收基线。只有 `validated` 基线才能被任务自动读取：

```powershell
.venv\Scripts\python.exe -m data_collect.jobs.tushare_release create --tag baseline_20260731 --cutoff 20260731
.venv\Scripts\python.exe -m data_collect.jobs.tushare_release validate --tag baseline_20260731
```

阶段二低频/状态增量使用：

```powershell
.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_incremental --dataset all --lookback-days 5 --event-lookback-days 30
```

该任务的 `dividend` 按 `ann_date`/`imp_ann_date` 窗口查询，`stock_st`、`suspend_d`、`stk_limit` 按交易日查询，`namechange` 低频按日期窗口查询，`index_daily` 按指数和日期区间查询。

## 财务数据同步

财务数据不是每日全市场行情快照，应按公告/报告期定期补拉。三张财务报表分别转换为：

```text
tushare_income_raw       → financial_income_standard
tushare_balancesheet_raw → financial_balance_sheet_standard
tushare_cashflow_raw     → financial_cash_flow_standard
```

财务 Standard 必须保留报告期、公告日、报表口径和版本信息；PIT 使用 `f_ann_date`，为空时回退到 `ann_date`，不能只按 `end_date` 判断可见性。

## 重要边界

- 日线 Standard 的 `volume` 为股，来源 `vol` 为手，转换为 `vol × 100`。
- 日线 Standard 的 `amount` 为元，来源 `amount` 为千元，转换为 `amount × 1000`。
- Raw 保留 Tushare 原始单位和完整 JSON。
- 停牌或无成交产生的零 OHLC 必须在质量报告中标记，不能静默删除。
- 任务只入库事实数据，不直接生成买卖建议。
