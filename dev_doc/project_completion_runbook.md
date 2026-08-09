# A股个人投资研究系统：完工与正式运行手册

更新时间：2026-08-07  
适用项目：`ashares_data_collect`

## 1. 当前结论

阶段三核心代码已经完成，当前环境全量测试为 `652 passed, 1 skipped, 5 deselected`。

当前尚未完成的是生产数据生成，而不是代码开发。正式历史回测还需要执行规则、快照、Dataset 和 Run 的生产门禁。

系统不连接 QMT/MiniQMT，不自动下单。Backtest 结果进入 Portfolio 后，必须经过人工审批，成交类型固定为 `PAPER_MANUAL`。

## 2. 全链路总览

```text
Tushare / 新闻源
        ↓
Raw → Standard → PIT → Feature
        ↓
FROZEN Research Snapshot → 观察池与证据引用
        ↓
FROZEN Backtest Dataset → Backtest Run
        ↓
信号 → 订单 → 成交/拒单 → 现金/持仓 → 结果审计
        ↓
Portfolio PENDING_APPROVAL → 人工批准 → PAPER_MANUAL
```

Research 生成器可以读取已通过质量门禁的 PIT/Feature；Research 分析和 Backtest 只能读取 `FROZEN` 快照或 Dataset，不能读取实时派生表。

## 2.1 核心计算与规则口径

### A. 数据单位与价格坐标

| 数据 | 计算口径 |
|---|---|
| Tushare `daily.vol` | 原始单位为手；物化为执行行情时 `volume_shares = vol × 100` |
| Tushare `daily.amount` | 原始单位为千元；物化为执行行情时 `amount_yuan = amount × 1000` |
| 成交价格 | 使用原始 OHLC，不使用复权价格模拟成交 |
| 指标价格 | `adj_close_base = raw_close × adj_factor`，仅用于均线、收益和趋势指标 |
| 财务比例 | 保留接口原始比例口径；不擅自乘除 100，除非 Standard 映射明确规定 |

执行行情的 OHLC、成交量、成交额必须来自同一份 Raw payload。若缺少有效来源、出现跨版本字段拼接、非有限值或非正价格，Dataset 验证失败或关闭交易能力。

### B. PIT 与版本选择

财务事实的可见日期为：

```text
effective_announce_date = COALESCE(f_ann_date, ann_date)
```

如果接口没有 `f_ann_date`，保持为空并回退 `ann_date`，不得伪造。决策日 `D` 只能使用：

```text
effective_announce_date <= D
```

并按 `effective_announce_date DESC, report_date DESC, record_version DESC, fetched_at DESC` 选择可见记录。修订记录不能只按报告期覆盖旧记录，必须保留 `record_version`、`source_record_key` 和 `payload_hash`。

指标口径：`or_yoy` 映射为营业收入同比 `revenue_yoy`，`tr_yoy` 单独保留为营业总收入同比，`q_sales_yoy` 为单季度营业收入同比。旧 `stock_financial_indicator` 只作兼容只读表。

### C. 价格 Feature 计算

按股票和交易日升序计算：

```text
return_1d  = adj_close[t] / adj_close[t-1]  - 1
return_20d = adj_close[t] / adj_close[t-20] - 1
return_60d = adj_close[t] / adj_close[t-60] - 1
MA20       = mean(adj_close[t-19 : t])
MA60       = mean(adj_close[t-59 : t])
MA120      = mean(adj_close[t-119 : t])
volatility_20d = sample_std(return_1d[t-19 : t])
amount_ma20    = mean(amount[t-19 : t])
volume_ratio_20d = volume[t] / mean(volume[t-19 : t])
```

窗口包含当日，历史不足时不会伪造完整指标；`available_observation_count` 用于质量判断。

### D. 估值 Feature 计算

PE/PB 只纳入有限、正值观测。在股票自身历史窗口内维护排序值：

```text
pe_pct = bisect_right(sorted_positive_pe, current_pe) / count_positive_pe
pb_pct = bisect_right(sorted_positive_pb, current_pb) / count_positive_pb
```

当前实现使用最近约 3 年和 5 年自然日窗口；缺少正 PE/PB 时分别为 NULL，并将 `is_pe_meaningful` 置为 false 或质量置为 WARNING。估值分位不是“便宜必涨”结论，只是可审计的筛选输入。

### E. 第一版 Value-Trend 策略硬过滤与评分

当前基准策略 `value_trend_v1` 的默认参数为：

```text
pe_percentile_max = 0.30
volume_ratio_min  = 1.00
stop_loss_pct     = 0.08
allow_st           = False
```

BUY 必须同时满足：

```text
已上市且当日研究范围有效
eligible_by_market != False
不是 ST（除非 allow_st=True）
pe_pct_5y <= 0.30 且 is_pe_meaningful=True
adj_close_base > MA20 > MA60
volume_ratio_20d >= 1.00，且该字段非 NULL
raw_close > 0，adj_close_base > 0
```

固定止损坐标：

```text
stop_loss_price_raw = raw_close × (1 - stop_loss_pct)
```

同日 BUY 的确定性评分为：

```text
value_score  = max(0, 1 - pe_pct_5y)
trend_score  = max(0, adj_close_base / MA20 - 1)
volume_score = max(0, min(volume_ratio_20d, 5) - 1) × 0.05
signal_score = round(value_score + trend_score + volume_score, 8)
```

SELL 规则为已有持仓且 `adj_close_base < MA20`。同日订单先处理 SELL，再按 `signal_score DESC, ts_code ASC` 处理 BUY；评分只决定资金不足时的顺序，不改变硬过滤条件。

说明：ROE、资产负债率、经营现金流、`revenue_yoy` 等财务特征已经进入 PIT/Feature 链路，但没有被当前 `value_trend_v1` 默认策略作为硬过滤条件。若启用“ROE 连续多年大于阈值”等规则，必须创建新的策略版本并把口径、缺失值处理和连续期数写入 Run Manifest，不能在旧策略版本中悄然改变。

### F. 仓位、费用与压力风险

费用规则必须从 Dataset 的有效日期规则读取，不能隐式使用 Python 默认值。基本公式为：

```text
commission(value) = max(value × commission_rate, min_commission)
buy_cost(value)   = commission(value) + value × transfer_fee_rate
sell_cost(value)  = commission(value)
                    + value × stamp_duty_sell_rate
                    + value × transfer_fee_rate
```

设 `equity_base` 为组合权益（缺省时使用可用现金），`risk_pct` 为单笔风险比例，`R` 为组合剩余风险预算，则：

```text
max_risk = min(equity_base × risk_pct, R)
entry_slippage = entry_price × entry_slippage_bps / 10000
exit_slippage  = stop_price × exit_slippage_bps / 10000
gap_stress     = stop_price × gap_stress_pct
pressure_buy   = entry_price + entry_slippage
pressure_sell  = max(price_tick, stop_price - exit_slippage)
stress_per_share = max(0, pressure_buy - pressure_sell) + gap_stress
theoretical_shares = max_risk / stress_per_share
planned_shares = floor(theoretical_shares / min_buy_lot) × min_buy_lot
```

然后逐手向下减少，直到同时满足：

```text
required_capital = shares × pressure_buy + buy_cost(shares × pressure_buy)
max_loss = shares × max(0, entry_price - stop_price)
            + shares × (entry_slippage + exit_slippage)
            + shares × gap_stress
            + buy_cost(shares × pressure_buy)
            + sell_cost(shares × pressure_sell)
required_capital <= available_cash
max_loss <= max_risk
```

不足一手、买入价不高于止损价、风险预算耗尽或费用后超限，直接拒绝，不产生订单成交。

### G. 成交、流动性与 A 股时序

```text
T 日收盘：读取 FROZEN Snapshot，生成信号和计划订单
T+1 开盘：读取原始 open，重新按实际价格计算仓位并尝试成交
成交后：创建 Fill，actual_shares = fill_shares
```

流动性容量为：

```text
volume_capacity_shares = floor(volume_shares × max_volume_participation_pct)
```

当前 v1 策略为整笔拒单：`requested_shares > volume_capacity_shares` 时不缩量成交，订单 `REJECTED` 且 `actual_shares=0`。成交量/成交额缺失、为 0、单位关系异常或行情不可交易，也拒绝成交。

买入必须满足可买、非涨停、开盘价高于止损价、没有已有持仓且未超过最大持仓数；卖出必须满足可卖、非跌停、持仓已过 T+1 可卖日期、可卖股数充足。现金结算日期超出 Dataset 时 fail-closed，不提前释放资金。

### H. 分红、送转与除权坐标

只纳入 `implementation_status='实施'` 且公告可见的公司行为。同一业务键只选择一个有效版本；`stk_div` 是送转合计，若同时有送股/转增分项，必须满足：

```text
stk_div = stk_bo_rate + stk_co_rate
```

有效送转率只取合计一次，不得把合计和分项相加。除权日前持仓按除权日前收盘价 `pre_close` 调整：

```text
coordinate_factor = (pre_close - cash_div) / (pre_close × (1 + stock_rate))
stock_factor      = 1 / (1 + stock_rate)
new_cost_price    = old_cost_price × stock_factor
new_stop_price    = old_stop_price × coordinate_factor
```

送转到账股数为：

```text
bonus_shares = floor(entitled_shares × stock_rate)
```

现金分红在 `pay_date` 入账；送转股在 `div_list_date`（缺失时按规则回退）到账。送转只属于除权日前持仓，不能分配给除权日后的新买入批次。第一版不模拟配股，不伪造零碎股现金补偿。

### I. 回测账本、哈希与压力测试

事件链固定为：

```text
signal → order → fill/reject/expire → cash ledger / position lot → daily valuation
```

`actual_shares` 只能来自 Fill；数据库生成 `backtest.order.order_audit_hash`，Portfolio 的 `source_order_hash` 必须与其一致。Dataset 输入哈希覆盖实际使用的快照、行情、日历、规则和公司行为切片，不覆盖整张可变物理表。

Monte Carlo 只对已完成交易的 R 倍数做压力测试，不替代历史回测：

```text
R = 单笔净损益 / 该笔风险预算
```

必须保存抽样方法（bootstrap 或 permutation）、随机种子、参数哈希和交易序列哈希，保证结果可复现。

现金与净值结算口径：

```text
BUY cash_change  = -(fill_price × fill_shares + buy_cost)
SELL cash_change =  (fill_price × fill_shares - sell_cost)
equity           = cash_balance + Σ(remaining_shares × raw_close)
unrealized_pnl   = Σ((raw_close - lot.cost_price) × lot.remaining_shares)
```

`cash_balance` 是账本余额，包含已成交但尚未可再次使用的卖出资金；`available_cash` 单独受 `cash_settlement_days` 约束，用于新订单资金检查。卖出资金在可用日期前不得用于新订单。期末假设清仓只记录 `END_OF_DATA_LIQUIDATION` 估值，不伪造 Fill，不改变真实成交统计。

## 3. 生产前通用准备

```powershell
cd F:\a_github\Finance_cn\ashares_data_collect
$env:TUSHARE_API_KEY = "<Tushare Token>"
docker compose up -d
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m compileall -q data_collect
git diff --check
```

正式冻结前必须满足：

```powershell
git status --porcelain
```

输出为空，或使用明确存在的代码归档；不能只依赖 dirty 工作区的 `diff_hash`。

## 4. 阶段一：历史数据底座

### 4.1 初始化行情 Raw

历史初始化按断点和批次执行，不能重复启动多个同类任务：

```powershell
.\.venv\Scripts\python.exe -c "from data_collect.jobs.tushare_history_init import run; print(run(include_financial=False, normalize_standard=False, batch_size=100, batch_pause=2.0))"
```

主要写入：`tushare_trade_cal_raw`、`tushare_daily_raw`、`tushare_daily_basic_raw`、`tushare_adj_factor_raw` 和 `tushare_history_checkpoint`。失败时根据 checkpoint 继续，不清理 Raw。

### 4.2 初始化财务 Raw

四类财务接口分开执行：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_financial_raw_init income --start-date 20210802 --end-date 20260803 --batch-size 100 --batch-pause 2
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_financial_raw_init balancesheet --start-date 20210802 --end-date 20260803 --batch-size 100 --batch-pause 2
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_financial_raw_init cashflow --start-date 20210802 --end-date 20260803 --batch-size 100 --batch-pause 2
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_financial_raw_init fina_indicator --start-date 20210802 --end-date 20260803 --batch-size 100 --batch-pause 2
```

对应 Raw 表为 `tushare_income_raw`、`tushare_balancesheet_raw`、`tushare_cashflow_raw`、`tushare_fina_indicator_raw`。财务接口按股票、分页、checkpoint 和重试处理；`fina_indicator` 必须保留报告期、公告日和版本。

### 4.3 Raw → Standard 与质量验收

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_financial_raw_version_backfill
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_fina_indicator_version
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_financial_raw_to_standard
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_raw_to_standard --table all
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_standard --task all
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_features --task all
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_data_quality
```

阶段一验收必须证明：Raw 不变、Standard 可从 Raw 重建、主键无重复、单位正确、日期覆盖完整、质量报告无阻断项。

## 5. 阶段二：日常增量闭环

行情按交易日一次性请求全市场：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_market_incremental_by_date --end-date 20260807 --lookback-days 5 --batch-pause 2
```

财务增量按公告窗口/报告期回补，所有响应先进入 Raw：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_financial_incremental --end-date 20260807 --lookback-days 5 --report-lookback-years 5 --batch-pause 2
```

状态、公司行为与阶段二派生：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_incremental --end-date 20260807 --lookback-days 5 --event-lookback-days 30 --batch-pause 2
```

增量完成后检查 checkpoint、Raw 版本、Standard/PIT/Feature 日期、重复键、未来函数、生命周期区间、分红业务键和质量门禁。任何失败都阻止快照发布，不得用旧 PASS 覆盖新失败。

## 6. 阶段三：Research 快照与观察池

正式历史回测必须使用多个日期的 `research-v1.2` FROZEN Snapshot：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.research_snapshot_series `
  --calendar-start 2021-08-02 `
  --calendar-end 2026-08-03 `
  --baseline-tag baseline_20260803 `
  --schema-version research-v1.2 `
  --freeze `
  --max-snapshots 50
```

首次生产建议先小范围验证，再扩大范围；`--max-snapshots 0` 代表取消保护上限，必须确认容量后才能使用。

快照验收：研究截止日为交易日、基线仍为 `validated`、PIT 财务可见日期不晚于研究日、输入统计/字段哈希/血缘/质量报告完整，且 `FROZEN` 后不可更新或删除。

观察池只能绑定 FROZEN Snapshot；证据只保存引用、URL、内容哈希和正反方角色，不复制正文，也不能让 AI 生成交易信号。

## 7. 正式 Backtest 生产流程

### 7.1 审批交易规则

候选规则先只校验，不写库：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_rules dev_doc/backtest_rules_candidate.yaml
```

人工确认费率、最低佣金、滑点、T+1、整手、市场板块、ST/新股和生效日期后，才允许追加写入：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_rules dev_doc/backtest_rules_candidate.yaml --confirm
```

规则导入是追加式的；冲突或修订必须建立新版本，不能覆盖旧规则。

### 7.2 创建并冻结 Dataset

`<snapshot-id>` 和 `<dataset-id>` 使用实际返回值：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_dataset create `
  --snapshot-id <snapshot-id> `
  --name research-v1.2-history `
  --version backtest-v1.0 `
  --baseline-tag baseline_20260803 `
  --warmup-start 2020-01-01 `
  --evaluation-start 2021-08-02 `
  --evaluation-end 2026-08-03

.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_dataset build --dataset-id <dataset-id>
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_dataset validate --dataset-id <dataset-id>
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_dataset freeze --dataset-id <dataset-id>
```

Dataset 必须冻结研究快照、warm-up/evaluation 区间、交易日历、原始执行 OHLC、公司行为、市场规则、费用规则、可见性规则、输入哈希和代码版本。缺失、重复、版本冲突或质量失败都必须 fail-closed。

### 7.3 创建正式 Run

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_run `
  --dataset-id <dataset-id> `
  --run-version run-v1.0 `
  --initial-cash 10000 `
  --risk-pct 0.01 `
  --max-single-position-pct 0.25 `
  --max-portfolio-risk-pct 0.03 `
  --reserve-cash-pct 0.10 `
  --entry-slippage-bps 10 --exit-slippage-bps 10 --gap-stress-pct 0.05 `
  --monte-carlo-method bootstrap_with_replacement --random-seed 42
```

结果检查：T 日收盘生成信号、T+1 开盘尝试成交；`actual_shares` 只来自真实 Fill；停牌/涨跌停/流动性/T+1/退市/现金结算边界正确；分红送转后的成本和止损坐标一致；订单哈希、现金、持仓、费用和每日净值可复算。

## 8. Portfolio 人工隔离

Backtest 成功订单只能导入为待审批意图：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.portfolio_simulation `
  --portfolio-id <portfolio-id> `
  --import-backtest-order-id <filled-order-id> `
  --actor <operator>
```

导入后必须是 `source_type=BACKTEST`、`status=PENDING_APPROVAL`。只有人工审批后才允许生成 `PAPER_MANUAL` 模拟成交；不访问券商、不连接 QMT、不自动下单。

## 9. 失败、恢复与数据安全

- Raw 是事实存档，只允许追加和幂等写入，禁止清理或覆盖 Raw。
- Standard/PIT/Feature/Research/Dataset 可以按规则重建，但必须从 Raw 或已冻结上游重建。
- Tushare 失败使用 checkpoint、分页和重试；不要并发启动多个同类历史任务。
- Dataset/Run/Portfolio 失败保留 FAILED/BLOCKED 审计记录，不删除失败证据。
- FROZEN Snapshot、VALIDATED/FROZEN Dataset、成功 Run 和 Portfolio 账本不可覆盖；修复应新建版本。
- 生产前保留 PostgreSQL 冷备份和已验证基线；恢复时按 Raw → Standard → PIT → Feature 顺序核验。
- OpenSearch/RSSHub 只承担新闻和外部证据检索，不替代 Tushare 行情、财务和交易事实层。

## 10. 最终完工判定

项目可以宣布“正式历史回测完成”的条件：

1. 代码版本干净且可复现；
2. Raw、Standard、PIT、Feature 质量门禁通过；
3. 多日期 Research Snapshot 全部 FROZEN；
4. Dataset 完成 `build → validate → freeze`；
5. Run 成功且输入哈希、规则版本、代码版本一致；
6. 现金、持仓、成交、费用、公司行为和净值完成对账；
7. 报告明确列出未模拟项：配股、部分成交、ATR/Trailing Stop 等；
8. 研究结果仍需人工判断，不构成自动交易授权。

当前项目状态：**三日 Baseline Dataset/Run 烟测已完成；完整五年生产回测待生成逐交易日 Snapshot 系列；自动实盘交易未实现且不会在本阶段启用。**
## 11. 2026-08-07 Baseline smoke execution record

Approved Baseline parameters were executed against a three-trading-day frozen Dataset. This is a smoke test of the complete historical simulation path, not a five-year performance conclusion.

```text
Dataset: 4013823d-2cf9-48f8-aff5-b642f0989329 (FROZEN)
Run:     9f278214-606d-40b1-9ce4-b8ee6ce2c1ad (SUCCEEDED)
Dates:   2026-07-30, 2026-07-31, 2026-08-03
Rules:   cn-a-share-v1 / research-fee-assumption-v2
Input:   c0ef9e019b55d3f4d5e489562a0317c5596b08f32da9f801e4f1846e444ead33
```
The run explicitly used initial cash 10,000, per-trade risk 1%, portfolio risk cap 3%, single-position cap 25%, reserve cash 10%, 10 bps entry/exit slippage, 5% gap stress, 5% volume participation, full-order liquidity rejection, T+1, and Monte Carlo bootstrap seed 42.
Audit results: 91 orders (6 filled, 85 rejected), 900 ordered shares reconciled to 900 filled shares, rejected/expired `actual_shares != 0` count 0, all 91 order audit hashes present, continuous cash ledger, and Run input hash equal to Dataset input hash. The final mark-to-market equity was 10018.96931 and the forced-liquidation audit equity was 9987.37913. The last evaluation day had no following trading day, so its new orders were intentionally not created.
Two implementation fixes discovered during this run are now part of the code baseline: Dataset Manifest reads market/fee rule versions; frozen rule loading reads parameters and rule hashes; non-trading or suspended no-price rows remain WARNING without synthetic prices; and Run Manifest records the effective frozen Dataset rule snapshot. The first failed smoke Run is retained as an audit record and is not a production result.
This smoke run does not create a Portfolio or broker order. Portfolio remains an explicit human approval air-gap (`PENDING_APPROVAL` -> `PAPER_MANUAL`). A full historical run still requires a FROZEN Research Snapshot for every SSE open day in the evaluation interval.
## 12. Latest status correction (2026-08-07)

The earlier statement that production has no Dataset/Run is historical and is superseded by Section 11. The current verified smoke path is:

```text
Raw → Standard/PIT/Feature → FROZEN Research Snapshot
→ FROZEN Dataset 4013823d-2cf9-48f8-aff5-b642f0989329
→ SUCCEEDED Run 9f278214-606d-40b1-9ce4-b8ee6ce2c1ad
```

The remaining production task is the full historical series, not a missing core implementation: create one FROZEN `research-v1.2` Snapshot for each SSE open day in the evaluation interval, then build and audit a new full-range Dataset/Run. Portfolio remains manual `PENDING_APPROVAL → PAPER_MANUAL`; no broker or automatic order path exists.

2026-08-08 price-input update: the formal series is now `research-v1.3`. Before creating
Snapshots, run the idempotent migrations `042_create_point_in_time_price_feature.sql` and
`043_research_v13_point_in_time_price.sql`, then rebuild `price_v2`. The v2 indicator coordinate
uses only implemented corporate actions with `ex_date <= trade_date`; raw OHLC remains the
execution coordinate. Existing v1.1/v1.2 Snapshots and v1 Dataset/Run records stay immutable.

## 13. 2026-08-09 historical Dataset production status

The five-year `research-v1.3` Snapshot series has now been generated, validated, and batch-frozen:

```text
baseline:       baseline_20260806
schema:         research-v1.3
snapshots:      1,215 FROZEN
date range:     2021-08-02 ~ 2026-08-06
git_dirty:      0
missing hashes: 0
```

The historical Dataset is:

```text
dataset_id:     7b138308-f397-4886-a255-5b5b5cdf42cb
name:           history-baseline-v13
version:        dataset-v1.3
status:         BUILDING (validation/freeze pending)
```

The materialization build has completed its execution-input phase with the following audit
counts; the build transaction must still commit and pass `validate` before the Dataset can be
called `VALIDATED` or `FROZEN`:

```text
execution bars:  6,309,894
calendar rows:   1,831
corporate acts:  17,890
benchmark rows:  1,215
dataset members: 1,215
missing rows:    93,384 (must be classified by validation; not silently ignored)
```

Because the build is one resumable transaction, the database may continue to show `BUILDING`
and child-table counts may remain invisible until commit. A live process and an empty stderr log
are the expected signs of a running build; do not start a second build for the same Dataset.

The historical build is deliberately memory-bounded. `backtest_dataset.py` uses a named
server-side cursor (`itersize=5000`), inserts execution bars in batches of 5,000, and calculates
global/per-date hashes incrementally. It must not use `fetchall()` for the full execution range.
The standalone handoff entry point supports resuming an existing DRAFT/FAILED Dataset with
`data_collect.jobs.backtest_dataset_handoff --dataset-id ...`.

The remaining production sequence is:

```text
Dataset build commit
→ validate (including the 93,384 missing-row classification)
→ freeze
→ create Baseline Run with explicit rules/risks/slippage
→ reconcile orders, fills, cash, positions, corporate actions and hashes
```

Until this sequence succeeds, the three-day smoke Run remains the only completed Backtest
result. Raw tables remain append-only and are not modified by Snapshot/Dataset construction.
