# Backtest 第一版设计契约

## 2026-08-08 execution audit closure

The execution contract is now `backtest-dataset-v2` / `research-to-backtest-v2` with engine model `daily-next-open-v2`. BUY sizing at T close and T+1 open uses an absolute portfolio reserve floor based on equity; manual candidate and Portfolio paths use settled cash (`available_date <= execution date`). Liquidity uses the previous completed bar, and Snapshot build locks the quality/materialization boundary before reading. PostgreSQL migration `041_backtest_open_time_contract.sql` enforces the persisted liquidity reference date for filled v2 orders. These changes do not modify Raw data.

## 2026-08-07 前向候选边界

历史 Backtest 仍只消费冻结 Dataset/Run。手动每日候选复用确定性策略和
风险原语，但每次只消费一个 FROZEN Research Snapshot；候选记录明确的
计划股数、原始/复权价格边界、费用、滑点、跳空压力和成交量整笔拒单结果。
计划股数不是成交股数。AI Skill 只做当前研究快照的反方审计，审计通过后
仍须人工决定，系统才会创建 `DAILY_SIGNAL/PENDING_APPROVAL` 纸面意图。

## 2026-08-06 生产增强版实现状态

阶段三正式回测增强项已落地，代码与数据库迁移以 `sql/028`–`sql/033` 为准：

- `RiskPolicy` 以 `risk_policy_version` 固定入口滑点、出口滑点和跳空压力；仓位结果同时保存理论股数、计划股数、实际股数、止损最大损失、压力损失、费用、滑点和实际风险比例。
- Dataset 物化时将 Tushare `daily.vol`（手）统一转换为标准执行单位（股），将 `daily.amount`（千元）转换为元；执行 OHLC/成交量/成交额必须来自同一份 Raw payload，禁止字段级拼接。
- `MarketRule.max_volume_participation_pct` 与 `liquidity_policy=reject_full_order` 形成流动性门禁。成交量/成交额缺失、为零、单位关系异常或订单超过容量时整笔拒绝，不缩量成交。
- Monte Carlo 同时支持 `bootstrap_with_replacement` 与 `permutation_without_replacement`，保存方法、随机种子、交易序列哈希和参数哈希。
- `backtest.order` 保存风险和流动性审计字段。`requested_shares`、`planned_shares`、`actual_shares` 分别表示请求、收盘计划和实际执行数量；`actual_shares` 只能由执行结果产生，不能被解释为计划数量。
- Backtest 订单导入 Portfolio 只能创建 `source_type='BACKTEST'`、`PENDING_APPROVAL` 的人工意图；数据库触发器要求源 Run 为 `SUCCEEDED`、Dataset 为 `FROZEN`、源订单为 `FILLED`，不得直接生成批准或成交记录。
- Dataset 验证对非停牌日的成交量/成交额缺失、为零、单位关系异常和无效 OHLC 失败；停牌日可保留 `WARNING`，但引擎不会把它当作可成交行情。规则快照的 `parameters.max_volume_participation_pct` 和 `parameters.liquidity_policy` 会冻结到 `MarketRule`。
- 期末强制清仓只生成独立的 `END_OF_DATA_LIQUIDATION` 假设估值审计元数据/Metric，不伪造 Fill、不修改现金账本、不改变真实交易统计。

送转股规则固定为使用校验后的 `stk_div` 合计（无合计时才使用分项之和），记账股数为 `floor(entitled_shares * stock_rate)`；小数部分不伪造手续费或现金流水。第一版仍不模拟配股。

## Current implementation status (2026-08-06)

The Dataset/Run schema, Frozen Dataset materializer, pure Python daily engine,
event ledger persistence, explicit effective-date rules, deterministic event IDs,
and contract tests are implemented. A three-trading-day Baseline Dataset is now
FROZEN and a Baseline Run has completed successfully; this is a smoke result, not
the full historical performance run.

The calendar and current market-derived layers cover `2026-08-06`. Explicit
effective-date rules exist in `backtest.market_rule_effective`, and the three-day
Dataset contains the required daily FROZEN Research Snapshots. A full historical
build remains gated on producing one FROZEN `research-v1.2` Snapshot per SSE open day.
The materializer fails closed and never changes Raw data or silently falls back to
synthetic/current rules.
Rule imports are idempotent only when the existing primary-key content matches;
conflicting content requires a new rule version. Dataset freeze also requires the
Git identity captured during build to match the identity at freeze time.
The database trigger also prevents changing Dataset input fields during the
`VALIDATED -> FROZEN` transition, and rule import checks effective-date overlap
against already imported rows, not only rows in the current file.

更新时间：2026-08-06  
状态：阶段三核心代码完成；三日 Baseline Dataset/Run 已完成烟测；完整五年 Dataset/Run 待生成

本文件吸收 `dev_doc/gemini-doc/` 的阶段三建议，但以当前项目的
`Raw → Standard → PIT → Feature → FROZEN Research Snapshot` 模型为准。
Gemini 示例代码只作为算法参考，不能直接作为生产回测引擎。

## 1. 总体边界

正式回测只能读取不可变的、已通过质量门禁的输入：

```text
FROZEN Research Snapshot
+ FROZEN 执行行情
+ FROZEN 公司行为
+ FROZEN 交易日历/市场规则/费用
→ Backtest Dataset
→ Backtest Run
→ Strategy / Signal / Order / Fill
→ Cash Ledger / Position Lot / Daily Performance
```

Research 快照生成器可以读取已验收的 PIT/Feature；Research 分析和 Backtest
不能直接读取正在变化的实时 PIT/Feature。生产回测禁止在数据库失败时自动切换
合成数据，合成数据只允许用于单元测试。

## 2. 输入数据契约

### 2.1 Dataset Manifest

不能只记录 Snapshot ID。当前已建立回测 Dataset Manifest 和成员表，至少记录：

- `dataset_id`、数据集版本和状态；
- `research_snapshot_id`、`research_date`；
- `role`：`WARMUP` 或 `EVALUATION`；
- `warmup_start`、`evaluation_start`、`evaluation_end`；
- 交易日历版本及完整性报告哈希；
- 执行行情切片哈希；
- 公司行为切片哈希；
- 复权策略、Feature 版本和 PIT 可见性规则；
- 输入适配器版本、Schema 版本和创建代码版本。

Dataset 成员必须全部引用 `FROZEN` 快照，日期不能重复。所有输入切片、字段
集合和哈希必须固定，不能在回测期间读取可变的 `daily_kline`、
`adj_factor_daily` 或 `std.dividend_event` 后声称结果可复现。
`data_baseline_tag` 之外还必须保存创建时实际读取的
`baseline_quality_report_hash`；该哈希进入 `input_manifest_hash`，并由数据库触发器
校验其仍与 validated 发布点一致。基线质量报告变化时，原 Dataset 不得继续
build/validate/freeze，必须重新创建 Dataset。

### 2.2 执行行情与公司行为

当前 `research.stock_snapshot_daily` 没有完整的原始 OHLC、成交量、涨跌停价，
因此不能直接承担撮合输入。需要增加不可变的执行输入物料，至少包含：

```text
ts_code, trade_date,
raw_open, raw_high, raw_low, raw_close,
volume, amount, up_limit, down_limit,
is_suspended, can_buy, can_sell,
source_record_key, source_record_version, source_payload_hash
```

公司行为至少保留现金分红、送股、转增和实施状态。Tushare 的 `stk_div` 作为
送转合计；如果合计和 `stk_bo_rate + stk_co_rate` 同时存在，二者必须一致，
引擎只使用一次合计值。送转股份按除权日前的持仓记录为独立的
`CORPORATE_ACTION` 批次，使用红股上市日作为可卖日期，并继承除权后原批次的
成本/止损坐标；不会分配给除权日之后的新买入批次。配股所需的
配股价、认购期和认购结果当前模型不完整，第一版明确暂不模拟配股，不能宣称
已覆盖全部公司行为。

### 2.3 Warm-up 与绩效区间

`evaluation_start` 不能直接作为数据起点。MA120、波动率和 PE 5 年分位数需要
此前数据。第一版按实际策略的最大回看窗口计算 `warmup_start`，绩效只统计
`EVALUATION` 区间。

Dataset 的 `warmup_start`、`evaluation_start` 和 `evaluation_end` 必须在已保存的
SSE 开市日历中存在且为开市日，避免把周末/节假日误当作信号或成交边界；区间内的
其他开市日仍必须由对应的每日 FROZEN Snapshot 覆盖。

当前行情历史起点为 2021-08-02；如果在该日要求完整的 5 年 PE 分位数，现有数据
不足。策略必须在 `observation_count` 不足时输出 `INSUFFICIENT_HISTORY`，或者把
评估起点推迟到满足回看要求的日期，不能用未来数据补齐。

## 3. 时间与撮合契约

第一版默认采用：

```text
T 日收盘后生成信号
→ T+1 交易日开盘尝试成交
→ T+1 日内使用已确定的止损价和当日 OHLC 判断结果
→ T+1 收盘更新下一交易日止损价和持仓估值
```

强制规则：

- 信号日收盘价不能作为同日成交价；
- v1 使用信号时确定的固定止损，不实现 trailing stop；未来若加入追踪止损，必须在 T 日收盘
  处理完 T 日盘中撮合后更新，不能用 T 日收盘更新后的止损价回看判断 T 日最低价；
- 订单必须有计划成交日、失效日和最终状态；
- 停牌、涨停买入、跌停卖出不能成交；
- 止损和策略卖出同时触发时，第一版止损优先；
- 第一版不模拟部分成交；
- 买入后遵守 T+1 可卖约束；
- 卖出所得资金的可用时间进入规则配置；
- 回测结束同时报告按最后收盘估值和强制清仓估值，不混为一个结果。

策略使用复权价格计算均线、动量和波动率；成交、费用、涨跌停和止损撮合统一
使用原始价格。复权止损价必须转换到原始价格坐标，禁止把复权止损价直接与原始
最低价比较。除权日先按 `(前收盘-现金分红)/(前收盘*(1+送转率))`
调整原始止损坐标，持仓成本只按送转股本因子调整，现金分红只进入现金账本；
缺少除权日前收盘价时正式回测 fail-closed。除权日前已生成、尚未成交的 BUY
订单也必须同步调整止损坐标。

送转股份的第一版记账规则固定为 `floor(entitled_shares * stock_rate)`，只保留可记账的
整数股；不足一股的零碎部分不创建持仓、不生成现金补偿，也不转化为可卖股数。
这只是回测 v1 的明确模拟口径，不代表所有券商的零碎股清算政策；如需纳入真实
零碎股处理，必须新增规则版本和对应数据/结算模型，不能静默改变历史结果。

## 4. 市场规则与费用

市场规则不能写死在 Python 中。当前已建立按有效日期读取的规则配置，至少覆盖：

- 主板、创业板、科创板、北交所和 ST 的涨跌幅规则；
- 新股上市初期特殊规则；
- 最小价格变动单位和交易单位；
- 买入整手、卖出零股规则；
- T+1 可卖限制；
- 佣金、最低佣金、过户费、经手费和印花税；
- 滑点和成交量约束。

规则版本必须写入 Dataset/Run Manifest，费用参数不能只存在代码默认值中。

## 5. Run Manifest 与事件账本

Dataset 解决“使用哪些数据”，Run Manifest 解决“这次如何运行”。当前已记录：

- `run_id`、`dataset_id`；
- `strategy_id`、策略版本、参数 JSON 和参数哈希；
- 初始现金、现金可用规则和组合限制；
- 市场规则/费用/滑点版本；
- 撮合模型版本、输入适配器版本；
- Git commit、代码归档或工作区差异哈希；
- 随机种子、输入汇总哈希和运行状态。
- Monte Carlo 的抽样方法、完整参数和输入交易序列哈希。

结果采用不可变事件链：

```text
signal → order → fill/reject/expire
      → cash ledger / position lot
      → daily valuation / performance
```

必须保留拒单、未成交、过期、部分成交（即使第一版禁用，也保留状态枚举）、
分红调整、送转调整和退市退出记录。不能只依赖一个可变的 `active_positions`。

## 6. 策略与组合边界

第一版最多验证 1～2 条简单确定性策略，策略配置进入版本控制或数据库：

- 质量 + 合理估值；
- 质量 + 成长 + 基础趋势过滤。

策略必须遍历每个交易日的历史股票池和全部 `ts_code`，不能像 Gemini 示例一样
只取 `df_today.iloc[0]`。多股票信号必须采用确定性排序和资金分配规则，并记录：

- 最大持仓数；
- 单股资金上限；
- 行业或组合总风险上限；
- 预留现金比例；
- 同日先卖后买还是先买后卖。

当前 v1 规则为：先处理 SELL；BUY 按 `signal_score` 降序、`ts_code` 升序
稳定排序。`value_trend_score_v1` 的分数由估值安全边际、趋势强度和量能强度
组成，只用于同日资金优先级。正式 Run 必须显式设置
`max_portfolio_risk_pct`，不能使用无限制的 `None`；T+1 开盘后按实际成交价
重新计算股数，价格跳空时缩量成交或拒绝，而不是把 T 收盘计划股数直接硬套到开盘。

固定风险仓位器、市场温度调节和 Monte Carlo 作为可配置风控模块；它们不能
自动突破组合风险上限，也不能替代样本外验证。

## 7. 计划对象与开发顺序

计划对象（已由 `028`/`031` 迁移落地，生产数据仍为空）：

```text
backtest.dataset_manifest
backtest.dataset_member
backtest.market_rule_effective
backtest.run_manifest
backtest.strategy_definition
backtest.signal
backtest.order
backtest.fill
backtest.position_lot
backtest.cash_ledger
backtest.performance_daily
backtest.metric
```

实施顺序：

```text
Dataset Manifest/输入物料契约
→ Run Manifest/规则版本契约
→ Frozen Dataset Materializer
→ Backtest Input Adapter
→ 纯 Python 撮合与账本引擎
→ PostgreSQL 事件和绩效落库
→ PIT/成本/不可成交/幂等专项测试
→ Walk-forward、样本外和压力测试
→ Portfolio 模拟组合
```

## 8. 开发前必须确认或采用的默认值

以下默认值用于第一版，若未被否决就按此实现：

| 项目 | 第一版默认值 |
|---|---|
| 回测范围 | 2021-08-02 至 2026-08-03，实际起点受 warm-up 和数据完整性约束 |
| 股票池 | 每日 PIT 历史股票池，不使用当前观察池替代历史股票池 |
| 执行时点 | T 收盘出信号，T+1 开盘成交 |
| 方向 | 只做多 |
| 部分成交 | 不模拟，整笔成交或拒绝 |
| 配股 | 第一版不模拟 |
| 停牌估值 | 使用最近可用价格并标记估值状态，不伪造可成交价格 |
| 退市 | 区间内未形成真实退出事件则 fail-closed；未退市持仓分别计算最后估值和假设清仓结果 |
| AI | 不参与历史信号和回测参数搜索 |

## 9. 验收门槛

- 正式回测不访问实时 PIT/Feature；
- Dataset/Run 输入和规则哈希可复现；
- 任意信号都能追溯到 Snapshot、字段、版本和日期；
- 复权指标与原始成交价格没有混用；
- 没有同日收盘后信息同日成交；
- 停牌、涨跌停、T+1、费用和公司行为测试通过；
- 回测重复运行结果一致；
- 训练、验证、样本外和 Walk-forward 结果分开；
- 删除或改变输入后，运行必须失败或哈希不一致；
- 回测结果不自动转为买入指令或实盘订单。
- Raw 版本存在时必须选择一个完整 payload 解析 OHLC/量额；只有没有版本记录时才
  回退旧 Raw，禁止按字段在新旧版本之间拼接，选中版本和 payload 哈希随 Dataset 冻结。
- 历史日期没有逐日覆盖门禁记录时，必须明确声明由 `validated` 数据基线的
  `quality_report_hash` 等价覆盖；任何已存在的最新非 PASS 门禁仍会阻断 Dataset。
- `Run.input_hash` 必须等于 Dataset 的 `input_manifest_hash`，市场规则和费用规则
  版本也必须与 Dataset 一致，数据库触发器独立强制这些约束。
## Implementation status (2026-08-06)

The stage-3 backtest foundation is now implemented, but no formal backtest has been
run and no production dataset has been frozen yet.

Implemented:

- `sql/028_create_backtest_model.sql` creates Dataset/Run manifests, frozen execution
  bars, calendar, corporate actions, effective market rules, strategy definitions,
  event-ledger tables and immutability triggers.
- `sql/031_phase3_final_audit_contract.sql` adds `signal_score`、`planned_shares`、
  Dataset 基线质量哈希绑定和数据库 Run/Dataset hash/rule-version equality gate；
  `028` remains idempotently compatible for clean installations.
- `data_collect/jobs/backtest_dataset.py` provides create/build/validate/freeze and
  `load_frozen_dataset`; it reads only FROZEN Research Snapshots and materializes
  execution inputs without changing Raw tables. `validate` recomputes hashes from the
  final Dataset child rows (not from mutable upstream cursors) and checks both global
  and per-research-date execution/company-action hashes against the Manifest.
- `data_collect/backtest/` contains the pure in-memory adapter models, fee/risk rules,
  deterministic daily engine and the first value/trend example strategy. Dividend
  total/component consistency, ex-right stop/cost coordinate adjustment, delist
  fail-closed handling, score ordering, portfolio-risk allocation and T+1 open
  resizing are included. Stock-transfer fractional shares are explicitly floored
  to integer shares in v1; no fractional cash compensation is simulated.
- `data_collect/jobs/backtest_run.py` persists signal/order/fill/lot/cash/performance
  events and binds the run to the Dataset rule versions and code metadata.
- The database Run trigger independently requires `FROZEN` Dataset status, input and
  parameter hashes, and an explicit code archive whenever the recorded code is dirty;
  this is enforced even if a caller bypasses the Python runner.
- The engine records signal status transitions, explicit slippage cost, and separate
  `final_equity_mark_to_market` / `final_equity_forced_liquidation` metrics. The latter
  is a hypothetical end-of-period valuation after sell fees; blocked or missing-price
  positions are counted and warned about, and no synthetic fill is created.

Current gate:

- The database migration is idempotent and the backtest unit/contract suite passes.
- Formal Dataset build is intentionally blocked until explicit rows exist in
  `backtest.market_rule_effective` and the Dataset contains all required daily
  FROZEN Research Snapshots for its evaluation interval; the engine must never
  silently use current or synthetic rules.
- The Standard calendar has been rebuilt through `2026-08-03`, matching the current
  FROZEN Research Snapshot cutoff. No formal Dataset/Run has been created yet.

The Dataset builder now requires one FROZEN Research Snapshot for every SSE open
day from `warmup_start` through `evaluation_end`; a Dataset with an intermediate
snapshot gap is rejected before any formal backtest input is created.

The latest implementation audit (2026-08-06) passed `47` final-audit/backtest
targeted tests,
`compileall`, migration re-application, and a PostgreSQL rollback smoke covering
FROZEN snapshot immutability, dirty Dataset archive gating, and Portfolio valuation
append-only rules. An isolated temporary-database integration also completed
`Dataset → Run → Portfolio` with a successful run, manual fill, idempotent valuation,
and conflicting-input rejection; the temporary database was dropped and production
tables remained unchanged. The production read-only check currently reports
`tushare_daily_raw=6,291,619`, `tushare_daily_basic_raw=6,274,473`,
`tushare_adj_factor_raw=6,360,864`, and zero formal Dataset/Run/Portfolio rows.
The repository-wide suite currently has `640 passed, 1 skipped,
5 deselected`; two pre-existing notification-prefix tests remain unrelated failures.
Portfolio cash-ledger inserts lock the account manifest, reject missing accounts,
and use a partial unique index to allow only one `INIT` row per portfolio; fill
cash checks use the same account-row lock to avoid concurrent cash oversubscription.
Research Snapshot and Dataset freeze operations also lock their Manifest row and
require the guarded `VALIDATED` state transition to affect exactly one row.
Dataset build and validate also reject any latest non-PASS coverage gate already
recorded within the warm-up/evaluation range; legacy dates without a gate are
not retroactively assigned a synthetic gate.

Execution defaults are fixed as: T close signal, T+1 open attempt, long-only, no
partial fills, raw OHLC for execution, adjusted prices for indicators, explicit
fees/rules by effective date, and no rights-issue simulation in v1. A clean Git
commit is required before Dataset/Run formal freeze; a dirty workspace is accepted
only when an existing code archive is explicitly supplied. `diff_hash` remains
supplementary audit metadata and is not, by itself, a reproducibility substitute.

Execution data vintage is explicit: during the repeatable-read Dataset build,
each daily business key selects the latest `tushare_market_raw_version` by
`fetched_at DESC, record_id DESC`, and the execution OHLC/volume/amount are
parsed from that selected payload. Historical rows without a version use the
`tushare_daily_raw.payload` fallback. The selected source version and SHA-256 are
copied into `backtest.dataset_execution_bar`; later Raw or Standard revisions do
not change the frozen Dataset. This is a build-time historical-data vintage, not
a claim that a later Raw revision was knowable on the original trading day. PIT
financial and Research fields continue to use their own
`effective_announce_date <= research_date` visibility rule.

The version selection is source-level, not field-level: when a version row exists,
all OHLC/volume/amount fields come from that one JSON payload. A missing field in
the selected version is a quality failure; it is never filled from legacy Raw.

The historical coverage policy is explicit in `availability_rules`: an existing
latest gate for a dataset/date must be `PASS`; dates without a gate are covered by
the bound validated baseline quality report and its hash. This is an equivalence
policy, not a fabricated per-day gate record.

Legacy execution rows deserve an explicit distinction: historical
`tushare_daily_raw` rows may predate `tushare_market_raw_version`, so their
`source_record_version` can be NULL. The Dataset builder then hashes the selected
Raw payload deterministically and refuses a row without a payload hash. A NULL
version is therefore a recorded legacy limitation, not a fabricated version and
not permission to use an unpinned mutable row. Missing daily bars, missing
`std.security_status_daily`/`pit.security_tradeability_daily` rows, or upstream
status values outside `VALID`/`WARNING` are marked `MISSING_SOURCE` and block
formal Dataset validation; they are never silently converted into a tradable
observation.

## 2026-08-06 implementation audit

- Dataset manifests now record the selection predicate, selected columns, hash
  algorithm, availability rules, and the run manifest records a parameters hash.
- Each `dataset_member` now records both the execution-bar slice hash and the
  per-`ex_date` corporate-action slice hash; both are included in the Dataset input
  manifest hash. A day without a corporate action still receives a deterministic
  empty-slice hash.
- `research_snapshot_series.py` defaults to at most 50 single-date snapshots per run;
  removing that limit is an explicit operator action and requires a prior storage/time
  benchmark. The current implementation does not silently start a full multi-year series.
  A logical key of `(research_cutoff_date, data_baseline_tag, schema_version)` is unique;
  rerunning the series resumes DRAFT/FAILED snapshots and leaves VALIDATED/FROZEN snapshots
  untouched.
- The adapter accepts only `research-v1.2` snapshots, which include PIT maturity fields
  `eligible_by_market`; the existing `research-v1` FROZEN snapshot remains read-only and
  is not silently upgraded for formal Backtest use.
- Stop-loss processing precedes a same-day strategy sell. Frozen Dataset loading
  requires an explicit effective-date market rule instead of falling back to Python
  defaults.
- Dividend inputs retain announcement and stock-listing dates. `stk_div` is the
  single effective送转率 after total/component validation; on the ex-date existing
  lot stops are transformed to the new raw coordinate and corporate-action lots
  inherit adjusted cost/stop values. ATR/trailing stops are deferred until a
  versioned ATR feature and close-to-next-day state transition exist.
- Snapshot-series cutoff dates, Dataset source snapshots, and evaluation boundaries
  must be SSE open days. Rules are selected by both `market_rule_version` and
  `fee_rule_version`; the selected versions are copied into the frozen Dataset rule
  snapshot. Corporate actions are reduced by business key to one effective version
  before materialization, and duplicate business keys fail Dataset validation.
- Missing `volume_ratio_20d` is a failed momentum condition, never a confirmed signal.
  Portfolio database triggers independently enforce the manual-fill share count,
  T+1 availability, cash/fee reserve, price tick, stop-loss and snapshot-cutoff gates.
- Portfolio daily valuation rows retain the bound FROZEN Snapshot and a hash of the
  exact cash/lot/price input slice. Same-input reruns are idempotent; conflicting
  rewrites, updates and deletes are rejected. Valuation cannot be backfilled after
  a later fill has already been recorded.
- `sql/030_create_phase3_ops_gate.sql` adds the append-only `ops.market_data_quality_gate`.
  The date-batch market incremental task compares distinct securities with the median
  of the previous five Raw dates before writing a new Raw version. A drop over 1%, no
  baseline, or sample mode is never publishable; a blocked full-market day raises before
  Raw/Standard materialization. Research freeze rejects the latest non-PASS gate when one
  exists for its cutoff date.

## 2026-08-06 actual fill and Air-Gap hash closure

- `requested_shares` is the quantity requested by the strategy; `planned_shares` is the
  T-day sizing result; `order_shares` is the current execution request; `actual_shares`
  remains `0` until a real Fill is created. Rejected, expired, or otherwise unfilled
  orders always persist `actual_shares=0`.
- `backtest.order.order_audit_hash` is generated by PostgreSQL from the exact persisted
  order payload and the bound Dataset version. `portfolio.order_intent.source_order_hash`
  must equal that database-generated hash; a valid-length, caller-supplied hash alone is
  not accepted. Migration: `sql/033_backtest_order_audit_hash.sql`.
- The Portfolio import path reads the database hash and still creates only
  `BACKTEST/PENDING_APPROVAL`; it never approves or creates a broker fill. The migration
  is idempotent and does not modify Raw tables.
- `data_collect/jobs/phase3_validation.py` runs the read-only PIT cutoff guard. It checks
  `pit.financial_asof`, price/valuation Feature source dates, tradeability source dates,
  and Research financial visibility. The guard does not rebuild or mutate data. The
  current production check for `2026-08-03` passed with zero violations.
- `data_collect/jobs/backtest_rules.py` is the only supported rule-file importer. It
  accepts YAML/JSON, validates required fields, finite non-negative rates,
  effective-date ordering and overlapping scopes, and requires `parameters.provenance` before an explicit `--confirm`
  append. Migration `030` makes `backtest.market_rule_effective` append-only, enforces
  provenance on both source and Dataset rule rows, and adds a PostgreSQL exclusion
  constraint for overlapping effective ranges within the same market/rule/fee scope;
  corrections require a new rule version.
- The pure risk module includes the Gemini market-temperature multiplier as an explicit
  adapter. It is not enabled by default because the global temperature input is not yet
  a versioned Dataset field; missing temperature rejects formal adjustment rather than
  silently using a value. The engine also re-checks duplicate BUY intents at execution,
  rejects final-day T+1 fills for explicit-rule Frozen Datasets, and validates corporate
  action date order and tradable non-suspended execution limit prices. Formal Runs require
  an explicit portfolio risk cap; BUY allocation is score-descending and T+1 open sizing
  is recalculated from the actual fill price. Monte Carlo stores its method, parameters
  and input trade-sequence hash.

## 2026-08-06 guard closure

- Dataset `build`, `validate`, and `freeze` each re-check that the bound data baseline
  is still `validated`; a later baseline status change cannot be hidden by a previously
  created DRAFT/VALIDATED Dataset.
- Dataset validation failures persist `status='FAILED'`, failure time, reason, and
  quality report before raising. A FAILED Dataset can be rebuilt; it cannot be treated
  as a partially usable input.
- Backtest child rows are writable only while their Run is `RUNNING`. DRAFT, FAILED,
  CANCELLED, and FINISHED Runs are audit states, not append windows.
- Portfolio database guards independently enforce exact Snapshot cutoff matching for
  decisions, approval actor presence, fee reconciliation, append-only cash balance
  arithmetic, and non-increasing position-lot quantities. A manual fill after the
  bound one-day Snapshot cutoff is rejected; a T+1 fill therefore requires a later
  execution Snapshot/Portfolio version before production use.
- Corporate-action cash and stock legs can settle on different dates; the engine keeps
  the event pending until each applicable leg has settled instead of dropping stock
  bonus shares when cash payment is later.
- Dataset 执行行情在版本 Raw 存在时只解析一个完整 payload，禁止新旧 Raw 按字段拼接；
  退市持仓达到 `delist_date` 仍无真实退出时引擎直接失败，不再使用旧收盘价永久估值。
  历史无逐日门禁记录的日期由已验证基线 `quality_report_hash` 明确等价覆盖。

## 2026-08-07 开发完成状态

阶段三核心开发已完成，当前剩余工作属于生产门禁和数据生成，不是继续扩展回测架构：

```text
规则审批/入库
→ clean Git commit
→ 多日期 research-v1.2 FROZEN Snapshot
→ Dataset build → validate → freeze
→ 正式 Backtest Run
→ 结果、现金、持仓、订单哈希对账
```

当前生产库已经存在三日 FROZEN Dataset 和 SUCCEEDED Baseline Run；完整五年 Dataset/Run 仍待按每个 SSE 开市日生成 FROZEN Snapshot 后执行。自动实盘交易仍被 `BACKTEST/PENDING_APPROVAL` 与人工 `PAPER_MANUAL` Air-Gap 阻断。
## 2026-08-07 最新实现状态覆盖

本文早期“尚未生成正式 Dataset/Run”描述为历史记录。当前代码和数据库已经完成并验证三日 Baseline 烟测；权威完整说明见 `dev_doc/current_implementation_state_20260807.md`。

实测对象：

```text
Dataset 4013823d-2cf9-48f8-aff5-b642f0989329 = FROZEN
Run     9f278214-606d-40b1-9ce4-b8ee6ce2c1ad = SUCCEEDED
```

当前实现已接通：Dataset Manifest 规则版本/基线质量哈希、同一完整 Raw payload 物化、行情单位归一化、停牌/不可交易 WARNING、缺失可执行行情 fail-closed、T 收盘信号/T+1 开盘撮合、实际成交股数审计、送转合计防重复、除权止损坐标调整、退市无退出 fail-closed、Monte Carlo 审计、Run/Dataset 输入哈希绑定和 PostgreSQL 订单审计哈希。

本次 Baseline 不是完整五年绩效结果。完整回测必须先生成评价区间每个 SSE 开市日的
`research-v1.3` FROZEN Snapshot。v1.3 的指标价格来自 `price_daily_v2`：只纳入已实施且
`ex_date <= trade_date` 的公司行为；原始 OHLC 仍用于成交和止损。之后再执行 v2 Dataset
`create → build → validate → freeze`。Portfolio 只接受人工 `PENDING_APPROVAL`，不自动下单。
