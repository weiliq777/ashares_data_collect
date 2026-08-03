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

每日行情增量任务：

```text
data_collect.jobs.tushare_daily_incremental
```

默认回补最近 5 个交易日：

```text
daily       → tushare_daily_raw       → daily_kline
daily_basic → tushare_daily_basic_raw → valuation_daily
adj_factor  → tushare_adj_factor_raw  → adj_factor_daily
```

重复运行允许捕获接口修正；Standard 按交易日重建，Raw 版本表按内容哈希幂等保存。正式任务不能使用 `--limit-stocks` 或 `--dry-run`。

历史初始化中的复权因子仍按股票请求，便于按股票断点续传。日常增量不得复用这个逐股票逻辑，改用按交易日全市场批量请求：

```text
data_collect.jobs.tushare_market_incremental_by_date
```

该任务对 `daily`、`daily_basic`、`adj_factor` 都调用 `trade_date=YYYYMMDD`，每次运行每个数据集每个交易日只请求一次；最近 5 个交易日允许重复回补，以便捕获接口修正。Raw 仍通过版本表幂等保存，Standard 按日重建。正式任务示例：

```powershell
.venv\Scripts\python.exe -m data_collect.jobs.tushare_market_incremental_by_date --lookback-days 5 --batch-pause 1
```

`--limit-stocks` 和 `--dry-run` 仅用于小范围验证，不能用于正式全市场同步。

旧的逐股票增量重试任务仅用于处理历史遗留的逐股票任务：

```text
data_collect.jobs.tushare_adj_factor_incremental_retry
```

该任务按股票记录 checkpoint，失败后只重试未完成股票。新批量任务应优先用于后续每日增量。

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
