# 当前实现状态与端到端运行契约

## 2026-08-08 audit closure

The latest code closes the remaining execution-accounting gaps: reserve cash is calculated as `equity * reserve_cash_pct` and subtracted from settled available cash; unsettled sale proceeds cannot fund a new candidate. Research Snapshot build acquires the shared input boundary before the quality report and materialization. Daily-next-open execution uses prior-day liquidity capacity, real Fill quantities only, lot-targeted stops, and fail-closed delist handling. Migration `041_backtest_open_time_contract.sql` is applied to PostgreSQL. Full tests: `693 passed, 1 skipped, 5 deselected`.

The current production data audit found no FROZEN `research-v1.2` Snapshot yet. The database's 690 FROZEN `research-v1.1` snapshots are incomplete legacy records (522 missing SSE open dates in the five-year interval, dirty-code/baseline-hash gaps), and the existing v1 Dataset/Run smoke records must remain historical and immutable. A clean v1.2 Snapshot series and new v2 Dataset/Run are still required for the formal five-year result.

更新时间：2026-08-07  
适用项目：`ashares_data_collect`  
本文是当前源码、数据库实测结果和已批准 Baseline 的权威汇总。若其他开发文档中的历史状态与本文冲突，以本文为准；历史记录不代表当前数据库状态。

## 1. 当前结论

核心最小闭环已经真实跑通：

```text
Tushare
→ Raw 原始事实
→ Standard 标准化
→ PIT 时间可见性
→ Feature 派生指标
→ FROZEN Research Snapshot
→ FROZEN Backtest Dataset
→ Backtest Run
→ Signal / Order / Fill / Cash Ledger / Position / Performance
```

Portfolio 仍然是人工 Air-Gap：

```text
Backtest FILLED Order
→ Portfolio PENDING_APPROVAL
→ 人工批准
→ PAPER_MANUAL
```

不会连接 QMT/MiniQMT，不会自动访问券商，也不会自动下单。

当前已经完成的是三交易日 Baseline 烟测，不是完整五年历史绩效结论。完整五年回测仍需要为评价区间每个 SSE 开市日生成当前契约 `research-v1.2` 的 FROZEN Snapshot。

## 2. 代码执行路径

### 2.1 数据采集与 Raw

| 作用 | 主要代码 | 规则 |
|---|---|---|
| Tushare API 重试、统一调用 | `data_collect/providers/tushare_client.py` | 失败重试并保留错误上下文 |
| 历史行情初始化 | `data_collect/jobs/tushare_history_init.py` | 按接口粒度分批、checkpoint、可续传 |
| 财务 Raw 初始化 | `tushare_financial_raw_init.py` | `income/balancesheet/cashflow/fina_indicator` 分任务执行 |
| 行情日增量 | `tushare_market_incremental_by_date.py` | 按交易日请求 `daily/daily_basic/adj_factor`，自动分片 |
| 阶段二事件增量 | `tushare_phase2_incremental.py` | 状态按交易日、分红按公告窗口、名称变更按日期窗口、指数按代码区间 |
| 财务增量 | `tushare_financial_incremental.py` | 三大报表按公告窗口；`fina_indicator` 按股票和报告期窗口、分页回补 |
| 原始版本保存 | `tushare_market_raw_version`、财务 Raw version 表 | 完整 payload、`payload_hash`、来源版本、抓取时间 |

Raw 的原则是只追加、幂等、可追溯；后续 Standard/PIT/Feature 重建不能回写、修改或删除 Raw。空响应只能记录为合法 `empty`，不能伪造行情。

### 2.2 Standard、PIT、Feature

```text
Raw
→ data_collect/jobs/tushare_raw_to_standard.py
→ data_collect/jobs/tushare_financial_raw_to_standard.py
→ data_collect/jobs/tushare_phase2_standard.py
→ data_collect/jobs/tushare_phase2_features.py
→ PIT / Feature
```

关键口径：

- Tushare `daily.vol` 原始单位为手，`daily.amount` 原始单位为千元；Standard/Feature 保留来源口径，Backtest Dataset 物化为股和元。
- 财务 PIT 使用 `effective_announce_date = COALESCE(f_ann_date, ann_date)`，不得伪造 `f_ann_date`。
- `or_yoy` 映射为营业收入同比 `revenue_yoy`；`tr_yoy` 单独保留；`q_sales_yoy` 用于季度增长逻辑。
- PIT 查询必须满足公告日期不晚于决策日；Feature 的 `source_asof_date` 不得晚于研究日。
- 财务 Standard/PIT/Feature 保留报告期、公告日、版本号、来源记录键和 `payload_hash`；旧 `stock_financial_indicator` 只读兼容，不是当前主链路。
- 生命周期按名称变更区间归并，分红按业务键选择有效版本；只有实施状态且除权日可见的事件进入股东回报 Feature。

### 2.3 Research Snapshot

主要代码：

- `data_collect/jobs/research_snapshot.py`
- `data_collect/jobs/research_snapshot_series.py`
- `data_collect/jobs/research_watchlist.py`
- `data_collect/jobs/research_evidence_candidates.py`

约束：

- 一个 `snapshot_id` 只对应一个 `research_cutoff_date`。
- Research 生成器可以读取已通过质量门禁的 PIT/Feature。
- Research 分析和 Backtest 只能读取 `FROZEN` Snapshot，不能读取实时派生表。
- Snapshot 记录输入切片、字段集合、选择条件、质量报告哈希、代码版本、Git 状态、输入哈希和逐行血缘。
- `FROZEN` 后 Snapshot Manifest、股票快照和血缘子表不可更新/删除。
- 观察池只能绑定 FROZEN Snapshot；ACTIVE 观察池只能归档，成员和证据只允许追加。

## 3. Backtest Dataset 与 Run

### 3.1 Dataset 构建

主要代码：`data_collect/jobs/backtest_dataset.py`

执行顺序固定为：

```text
create
→ build
→ validate
→ freeze
```

Dataset 冻结以下输入：

- 每日 Research Snapshot；
- 原始执行 OHLC、成交量、成交额、涨跌停和交易状态；
- 交易日历；
- 实施状态的公司行为；
- 有效日期市场规则和费用规则；
- warm-up/evaluation 范围；
- 选中版本、来源记录键、`payload_hash`；
- Dataset 输入哈希、基线质量报告哈希和代码归档。

执行行情在版本 Raw 存在时选择同一条完整 payload，不跨版本按字段拼接；没有历史 Raw version 时才回退旧 Raw，并必须计算 payload 哈希。停牌、退市或当日不可交易且没有行情时保留 `WARNING`，不伪造价格；本应可交易但缺行情时为 `MISSING_SOURCE` 并阻断 Dataset。

### 3.2 Dataset 规则

本次批准的 Baseline 规则版本：

```text
market_rule_version = cn-a-share-v2
fee_rule_version    = research-fee-assumption-v3

2021-08-02 ~ 2022-04-28: stamp duty 0.10%, transfer fee 0.002%
2022-04-29 ~ 2023-08-27: stamp duty 0.10%, transfer fee 0.001%
2023-08-28 ~ present:     stamp duty 0.05%, transfer fee 0.001%
```

Baseline 交易参数：

```text
initial_cash                  = 10000
risk_pct                     = 0.01
max_portfolio_risk_pct       = 0.03
max_single_position_pct      = 0.25
reserve_cash_pct             = 0.10
commission_rate              = 0.03%
min_commission               = 5.00
entry/exit slippage           = 10 bps
gap_stress_pct               = 5%
max_volume_participation_pct = 5%
liquidity_policy              = reject_full_order
T+1                          = enabled
buy lot                      = 100 shares
sell odd lot                 = allowed
cash settlement              = 0 trading days
Monte Carlo                  = bootstrap_with_replacement, seed 42
```

规则必须先通过 `backtest_rules.py` 校验，再使用 `--confirm` 追加写入；规则表只追加，不覆盖旧版本。同一市场作用域的生效日期不能重叠或留空档。

### 3.3 回测引擎

主要代码：`data_collect/backtest/engine.py`、`models.py`、`risk.py`、`data_collect/jobs/backtest_run.py`。

日线撮合时序：

```text
T 日收盘：读取 FROZEN Research Snapshot，产生信号和计划订单
T+1 开盘：检查停牌、涨跌停、T+1、流动性和现金，按实际开盘价重算股数
T+1 日内：产生 Fill 或拒单
收盘后：更新持仓、现金账本、每日净值和下一交易日止损坐标
```

核心规则：

- `requested_shares` 是策略请求数量，`planned_shares` 是 T 日收盘计划数量，`actual_shares` 未成交前必须为 0，只有真实 Fill 后才写入。
- 实际成交股数与计划数不同才记录 `resized_at_execution`。
- 止损优先于同日策略卖出；停牌、涨停买入、跌停卖出、T+1 不可卖和流动性不足均可拒单。
- 除权日同步调整持仓成本、止损坐标和待成交订单。
- `stk_div` 是送转合计；只有缺少合计时才使用送股和转增分项之和，避免重复计算。
- 送转零碎股 v1 按 `floor(entitled_shares * stock_rate)` 取整，不产生现金补偿；第一版不模拟配股。
- 退市持仓没有真实退出事件时 fail-closed，不使用退市前旧价格伪造退出成交。
- Dataset 缺少有效市场规则时失败，不回退 Python 默认规则。
- Monte Carlo 只做风险审计，不改变真实成交账本；保存方法、参数、随机种子和交易序列哈希。

### 3.4 Run 审计

`backtest.run_manifest` 必须绑定：Dataset ID、Dataset 输入哈希、规则版本、策略版本、完整参数、参数哈希、代码版本、Git 信息和代码归档。数据库触发器强制 Run 输入哈希、市场规则和费用规则与 Dataset 一致。

订单审计要求：

- `backtest.order.order_audit_hash` 由 PostgreSQL 根据完整订单行生成；
- 拒单/过期订单 `actual_shares = 0`；
- 成交订单 `actual_shares > 0`；
- Fill 股数总和必须与订单实际股数对账；
- 现金账本余额必须连续；
- 期末假设清仓只写审计 Metric，不伪造真实 Fill。

## 4. 当前数据库实测状态

### 4.1 数据覆盖

```text
Raw daily/daily_basic/adj_factor/trade_cal 最大日期：2026-08-06
Standard 与行情/状态/估值 Feature 最大日期：2026-08-06
PIT 未来公告违规：0
Raw checkpoint failed：0
```

财务 Feature 的最新记录日期由公告可见性决定，不要求与每日行情最大日期相同；当前 `revenue_yoy` 已接入新的财务版本链路。

### 4.2 三日 Baseline 烟测

```text
Dataset = 4013823d-2cf9-48f8-aff5-b642f0989329
status  = FROZEN
range   = 2026-07-30 ~ 2026-08-03

Run     = 9f278214-606d-40b1-9ce4-b8ee6ce2c1ad
status  = SUCCEEDED
orders = 91
fills  = 6
rejects = 85
actual shares = 900
final mark-to-market equity = 10018.96931
forced liquidation audit equity = 9987.37913
```

已验证：Dataset/Run 输入哈希一致、91 条订单均有订单哈希、拒单实际股数为 0、订单与 Fill 股数一致、现金账本连续、Raw 未被回测流程写入。

首次运行留下的 `baseline-smoke-v1` FAILED 记录保留为审计证据，不是生产结果；后续有效结果以 `baseline-smoke-v3` 为准。

## 5. 当前未完成边界

1. 当前只有三日 Dataset，不能代表 2021-08-02 至 2026-08-03 的完整五年回测。
2. 完整历史回测需要每个 SSE 开市日对应的 `research-v1.2` FROZEN Snapshot，再创建新的 v2 Dataset。
3. 当前 Git 工作区仍有本轮开发修改，正式冻结五年 Dataset/Run 前必须提交 clean commit，或使用明确的代码归档并完成审计。
4. Portfolio 尚未创建正式业务组合；这属于人工审批隔离，不是回测链路缺失。
5. 全量质量脚本包含大表统计，单次执行可能较慢；本次 180 秒检查未完成，但关键 PIT 校验、checkpoint、Dataset/Run 哈希和订单账本校验均已通过，不能把超时写成质量失败。

## 6. 使用入口

每日增量完成后，应依次检查质量门禁，再生成 Snapshot；不得直接用实时 Feature 做正式回测。

```powershell
# 规则校验/导入
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_rules dev_doc/backtest_rules_candidate.yaml
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_rules dev_doc/backtest_rules_candidate.yaml --confirm

# Dataset
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_dataset create ...
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_dataset build --dataset-id <dataset-id>
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_dataset validate --dataset-id <dataset-id>
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_dataset freeze --dataset-id <dataset-id> --code-archive-path <archive.zip>

# Baseline Run：必须显式传入风险、滑点、跳空和组合约束
.\.venv\Scripts\python.exe -m data_collect.jobs.backtest_run `
  --dataset-id <dataset-id> --run-version <run-version> --initial-cash 10000 `
  --risk-pct 0.01 --max-single-position-pct 0.25 `
  --max-portfolio-risk-pct 0.03 --reserve-cash-pct 0.10 `
  --entry-slippage-bps 10 --exit-slippage-bps 10 --gap-stress-pct 0.05 `
  --monte-carlo-method bootstrap_with_replacement --random-seed 42 `
  --code-archive-path <archive.zip>
```

Portfolio 命令只用于把已成交回测订单导入人工审批意图，不代表自动买入：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.portfolio_simulation `
  --portfolio-id <portfolio-id> --import-backtest-order-id <filled-order-id> --actor <operator>
```
