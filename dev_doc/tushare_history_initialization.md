# Tushare 历史库初始化与每日同步

## 设计结论

采用“两阶段”流程：首次按历史窗口初始化，之后每天只同步最新交易日。两者共用同一套标准表和 `ON CONFLICT DO NOTHING` 幂等写入逻辑。

历史任务：`data_collect.jobs.tushare_history_init`

覆盖内容：

- `trading_calendar`：Tushare `trade_cal`
- `daily_kline`：Tushare `daily`
- `valuation_daily`：Tushare `daily_basic`
- `adj_factor_daily`：Tushare `adj_factor`
- `stock_financial_indicator`：Tushare `fina_indicator`
- 三大财务报表原始 JSON：`tushare_income_raw`、`tushare_balancesheet_raw`、`tushare_cashflow_raw`
- 断点：`tushare_history_checkpoint`

## 初始化顺序

1. 执行 `sql/013_create_tushare_history.sql`。
2. 确认 `stock_basic_info` 已完成 Tushare 股票清单初始化。
3. 先初始化日线、估值、复权因子。
4. 再初始化财务指标和三大报表。
5. 运行覆盖率、重复键、日期连续性检查。

## 小范围验证

```powershell
$env:TUSHARE_API_KEY = "你的Token"
.venv\Scripts\python.exe -c "from data_collect.jobs.tushare_history_init import run; print(run(start_date='20260727', end_date='20260731', limit_stocks=3, max_days=2, include_financial=False))"
```

## 全量初始化

```powershell
$env:TUSHARE_API_KEY = "你的Token"
.venv\Scripts\python.exe -c "from data_collect.jobs.tushare_history_init import run; print(run(include_financial=False))"
.venv\Scripts\python.exe -c "from data_collect.jobs.tushare_history_init import run; print(run(include_financial=True))"
```

任务可重复执行。已完成的交易日或股票区间会由 `tushare_history_checkpoint` 跳过；失败项保留 `failed` 状态，修复后再次执行即可重试。

## 每日同步

历史库完成后使用现有管道：

```powershell
.venv\Scripts\python.exe run_job.py --pipeline tushare_daily --run-date 20260803
```

每日流程为：股票清单更新 -> 当日 `daily` -> 当日 `daily_basic`。财务数据按公告/报告期定期补拉，不应每天全市场重复拉取。

## 重要边界

- `daily` 和 `daily_basic` 的全市场五年数据量较大，应按交易日执行并保留限流。
- 财务接口按股票请求，耗时和积分消耗显著高于日线；建议单独运行、分批观察进度。
- `ann_date` 是信息可见时间，策略回测必须用它过滤，不能只按 `end_date`。
- 复权因子当前单独存储，后续计算前复权/后复权价格时使用，不能直接覆盖原始收盘价。
- 本任务只入库事实数据，不直接生成买卖建议。
