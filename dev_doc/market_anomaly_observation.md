# Market Anomaly Observation Domain（市场异常观察域）实现契约

状态：`FROZEN`（架构与契约冻结）
契约修订：2026-08-31（个股主链路与行业可选确认）
生效日期：2026-08-31
适用范围：市场异常观察、行业共振观察、事件证据关联、后验结果评估
不适用范围：买入推荐、订单生成、止盈止损、自动交易

> 本文是实现契约，不是参考资料。代码与本文冲突时，默认代码错误。任何指标、状态、时间语义、表结构或生命周期规则的变更，都必须创建新的 `methodology_hash`（方法论哈希）和新的观察发布批次，不得原地覆盖已冻结结果。

本次修订将 `ACCUMULATION_LIKE`（类吸筹形态）改为纯个股量价形态，
并将行业共振降级为独立的 `Sector Confirmation`（行业确认）证据。该变更是方法论变更，
不得用新逻辑覆盖旧观察结果；实现时必须使用新的 `methodology_hash`。

## 1. 设计边界

### 1.1 四类数据的判定标准

| 域 | 中文含义 | 允许回答的问题 | 禁止内容 |
|---|---|---|---|
| `Feature` | 特征层 | 市场客观发生了什么 | 解释性状态、买卖语义、未来结果 |
| `Observation` | 观察域 | 按固定方法当时发现了什么异常 | 未来收益、策略参数、订单语义 |
| `Evaluation` | 评估域 | 观察之后实际发生了什么 | 回写历史观察、改变历史评分 |
| `Strategy / Scenario` | 策略/场景评估 | 按某套行动规则会发生什么 | 污染通用市场事实 |

`Observation`（观察域）独立于 `Research Snapshot`（研究快照）。二者可以在 `Candidate Context`（候选上下文）或 `Research Package`（研究包）中组合，但任一方不得要求另一方存在才能生成。

观察域内部也分为两个层次：

```text
Stock Observation（个股异常观察）
    = 核心事实链，独立计算，决定 anomaly_flag / primary_state

Sector Confirmation（行业共振确认）
    = 可选增强证据，只补充解释，不反向改变个股主状态
```

### 1.2 不使用表名版本后缀，但保留内部可复现身份

新表不使用 `v1/v2` 后缀。生产复现仍必须记录：

- `methodology_hash`：指标、阈值、窗口、状态优先级和代码契约的 SHA-256；
- `data_baseline_id`：输入数据基线；
- `input_manifest_hash`：实际输入分区、行数、哈希和来源版本的摘要；
- `calculation_run_id`：一次计算批次的运行标识；
- `contract_hash`：本文及相关结构化契约的哈希。

算法升级只能产生新的方法论哈希和新的观察快照。旧快照保留，只能进入历史复现，不能被更新为新结果。

### 1.3 不可破坏规则

```text
OBS-ID-001
同一 observation_date + ts_code + methodology_hash + data_baseline_id
必须映射到同一个 observation_id。重试、续跑和并发恢复不得新建另一条主体记录。

OBS-ID-002
观察主体进入 FROZEN 后，observation_id、业务身份、指标、标签引用关系和输入身份不可变。

OBS-PIT-001
RETROSPECTIVE_CAUSE 证据永远不得参与历史 Observation 的指标、评分、标签或主状态重算。

OBS-DOMAIN-001
Observation 观察域不得写入未来收益、买入语义、订单参数或止盈止损参数。

OBS-EVAL-001
Evaluation 评估域只能追加或幂等写入后验结果，不得回写 Observation 主体。

OBS-EVIDENCE-001
observation.event_link 只保存观察日已可见的 AS_OF_EVIDENCE；
事后证据必须写入 evaluation.retrospective_event_link，且不得改变已冻结观察哈希。

OBS-SECTOR-001
行业映射、行业分母或行业聚合不可用时，不得阻断个股 Observation；
个股主链路必须将行业确认状态记为 UNKNOWN，而不是伪造为否定结果。

OBS-SECTOR-002
NOT_CONFIRMED 只允许在行业数据完整、PIT 有效、分母满足质量门禁且确认规则计算成功时使用；
数据缺失、覆盖不足、时间语义不明或计算失败都必须使用 UNKNOWN。

OBS-SECTOR-003
Sector Confirmation 不得修改 anomaly_flag、primary_state、state_reason_codes 或个股指标。

OBS-METHOD-001
取消行业共振对 ACCUMULATION_LIKE 的强制依赖属于方法论变更；
必须创建新的 methodology_hash、calculation_run_id 和观察发布批次，旧结果只读保留。
```

`observation_id` 的生成必须可重试。若使用 UUID，应使用由业务身份确定性生成的 UUID，或在数据库唯一冲突时读取并复用已存在的 ID；禁止每次重试随机生成新 ID。

## 2. 输入边界与 PIT 规则

### 2.1 数据链路

```text
Raw 原始层
 → Standard 标准层
 → PIT 时点层
 → Feature 特征层
 → Observation 观察域
 → Research / Candidate / Strategy 研究、候选、策略
```

观察域只读取已经通过质量门禁的 `Standard`、`PIT` 和 `Feature` 数据。作业代码不得直接持有数据库连接、游标或手写 SQL；所有数据库访问必须经过 `Repository → Kernel`（仓储到内核）。

### 2.2 观察日可见性

对观察日 `T`：

- 行情、成交额、换手率只使用 `T` 收盘时已确定的数据；
- 行业归属使用 `PIT`（时点）有效映射，而不是当前行业分类；
- 股票池分母使用观察日有效且符合交易资格的股票集合；
- 公告、财报、回购、解禁、股东和机构活动必须满足 `event_visible_at <= T`；
- `fetched_at`（本地抓取时间）不能替代业务可见时间；
- 观察日之后才出现的事件只能标记为 `RETROSPECTIVE_CAUSE`（事后原因），不得参与当日评分或状态重算。

### 2.3 观察资格

以下情况不得伪造为零值：

- 上市历史不足所需窗口；
- 停牌、无成交或成交额为零；
- 行业映射缺失；
- 价格复权因子缺失；
- 股票池或基准指数缺失；
- 事件源无法确认可见时间。

应写入 `calculation_status`（计算状态）、`quality_status`（质量状态）和 `missing_reason_code`（缺失原因码）。不满足最小历史窗口的记录不能进入有效异常统计。

行业映射缺失只影响 Sector Confirmation 及行业聚合字段，不得把个股核心指标和个股主状态强制改为缺失；
此时个股观察仍可进入 `VALIDATED`，但必须带有 `sector_confirmation_status = UNKNOWN` 和明确缺失原因。

## 3. 指标契约

### 3.1 通用窗口规则

- 所有窗口均按交易日计算，不按自然日计算；
- `T-k:T` 表示包含 `T` 在内的最近 `k+1` 个交易日；
- “历史基线窗口”排除当前异常窗口，避免异常数据污染分母；
- 默认最大滚动窗口为 252 个交易日；
- 默认最小有效样本数为窗口长度的 80%，不足则为缺失；
- 比值分母小于最小正数阈值时不计算，不能返回无穷大；
- 金额、成交量和换手率统一保留原始单位，计算前不得静默混用；
- 所有阈值由 `observation_policy_id`（观察策略标识）管理，不得散落硬编码在作业中。

### 3.2 交易异常指标

| 字段 | 中文含义 | 正式定义 |
|---|---|---|
| `amount_ratio_5_60` | 成交额五日/历史六十日比值 | `mean(amount[T-4:T]) / mean(amount[T-64:T-5])` |
| `amount_ratio_20_60` | 成交额二十日/历史六十日比值 | `mean(amount[T-19:T]) / mean(amount[T-79:T-20])` |
| `turnover_ratio_5_60` | 换手率五日/历史六十日比值 | 与成交额五日比值同窗，字段替换为有效换手率 |
| `turnover_ratio_20_60` | 换手率二十日/历史六十日比值 | 与成交额二十日比值同窗，字段替换为有效换手率 |
| `amount_zscore_60` | 成交额六十日标准分 | `(log1p(amount[T]) - mean(log1p(amount[T-60:T-1]))) / stddev(...)` |
| `abnormal_days_20` | 二十日异常天数 | 最近 20 个交易日中 `amount_zscore_60 >= 2` 或单日成交额相对历史基线 `>= 1.5` 的天数 |

标准差为零、有效样本不足或成交额无效时，`amount_zscore_60` 为缺失，不能将其当成 0。

### 3.3 价格结构指标

所有长期价格表现默认使用 `adj_close_base`（底座复权收盘价）。

| 字段 | 中文含义 | 正式定义 |
|---|---|---|
| `return_5d` | 五日收益 | `adj_close_base[T] / adj_close_base[T-5] - 1` |
| `return_20d` | 二十日收益 | `adj_close_base[T] / adj_close_base[T-20] - 1` |
| `price_position_52w` | 52 周价格位置 | `(adj_close_base[T] - min_252) / (max_252 - min_252)`，范围 `[0,1]` |
| `distance_ma20` | 相对二十日均线距离 | `adj_close_base[T] / mean(adj_close_base[T-19:T]) - 1` |
| `distance_ma60` | 相对六十日均线距离 | `adj_close_base[T] / mean(adj_close_base[T-59:T]) - 1` |
| `max_drawdown_20d` | 二十日最大回撤 | 最近 20 日复权收盘价相对此前滚动峰值的最小比例变化 |

价格区间无变化时 `price_position_52w` 为缺失。`raw_close`（原始收盘价）仅用于展示、执行坐标和审计，不作为长期跨期收益主口径。

### 3.4 量价结构指标

上涨/下跌日以 `return_1d`（一日收益）正负区分，平盘日不计入任何方向。默认使用成交量；若来源只有成交额，必须显式记录字段来源，不得静默替换。

| 字段 | 中文含义 | 正式定义 |
|---|---|---|
| `up_volume_ratio_20` | 二十日上涨日成交量占比 | 上涨日成交量之和 / 最近 20 日有效成交量之和 |
| `down_volume_ratio_20` | 二十日下跌日成交量占比 | 下跌日成交量之和 / 最近 20 日有效成交量之和 |
| `up_down_volume_ratio` | 上涨/下跌成交量比 | 上涨日成交量之和 / 下跌日成交量之和 |
| `price_response_efficiency_20` | 二十日成交异常价格响应效率 | `abs(return_20d) / max(log1p(turnover_ratio_20_60), floor)`，并按政策截尾 |

`price_response_efficiency_20`（成交异常价格响应效率）描述相对于换手异常的价格变化幅度，不代表经典市场微观结构意义上的价格效率，也不代表趋势质量或买入价值。

### 3.5 持续性指标

| 字段 | 中文含义 | 正式定义 |
|---|---|---|
| `anomaly_streak` | 连续异常天数 | 从 `T` 向前连续满足异常门槛的交易日数 |
| `anomaly_frequency_20` | 二十日异常频率 | `abnormal_days_20 / 20` |
| `days_since_first_anomaly` | 首次异常距今天数 | 在同一 `methodology_hash` 观察序列中，`T` 到首次有效异常日的交易日数 |

首次异常日期必须来自同一方法论和观察基线，不能跨方法论拼接。

### 3.6 行业与市场相对指标

| 字段 | 中文含义 | 正式定义 |
|---|---|---|
| `sector_anomaly_breadth` | 行业异常广度 | 行业有效股票中异常股票数 / 行业有效股票总数 |
| `sector_new_anomaly_ratio` | 行业新增异常比例 | 当日新增异常数 / 行业有效股票总数 |
| `sector_persistence` | 行业异常持续性 | 行业内连续异常股票数 / 行业内异常股票数 |
| `relative_return_20d` | 相对市场二十日收益 | 个股 `return_20d` - 同期基准指数收益 |
| `relative_volume_zscore` | 相对市场成交量标准分 | `amount_zscore_60(stock,T) - median(amount_zscore_60(all eligible stocks,T))` |

`relative_volume_zscore` 的市场截面只包含观察日有效股票集合，使用截面中位数而不是均值，以降低 A 股成交异常长尾对基准的影响。有效股票数低于政策最小值时，该指标为缺失，并记录 `INSUFFICIENT_UNIVERSE`（股票池不足）。行业广度的分母同样必须固定为观察日有效股票集合。

行业指标属于可选增强字段。行业输入不可用时，相关字段为 NULL，
并由 `sector_data_status` 和 `sector_confirmation_status` 解释原因；不得用 0、空字符串或当前行业分类补齐。

## 4. 评分、标签与状态机

### 4.1 五类评分

评分字段仅表示观察强度，不表示买入价值：

- `trading_anomaly_score`（交易异常强度）；
- `price_structure_score`（价格结构）；
- `persistence_score`（持续性）；
- `sector_confirmation_score`（行业共振）；
- `risk_score`（风险）。

`observation_priority`（观察优先级）是排序和人工研究分流字段，禁止命名为买入评分、交易评分或收益预测评分。

评分计算必须使用固定的标准化、截尾、缺失处理和权重，并由 `observation_policy_id` 与 `methodology_hash` 锁定。任一输入缺失时，必须记录参与评分的字段和缺失原因；禁止用未来数据或默认正面分数补齐。

### 4.2 标签与主状态分离

`anomaly_tags`（异常标签）与 `primary_state`（主状态）必须分开保存。

默认标签包括：

```text
VOLUME_EXPANSION      成交量放大
TURNOVER_EXPANSION    换手放大
LOW_POSITION          低位
SECTOR_CONFIRMATION   行业共振
PRICE_DIVERGENCE      价格背离
PERSISTENT_ANOMALY    持续异常
MARKET_RELATIVE       相对市场异常
```

同一股票同一天可以有多个标签，但只能有一个主状态。标签明细写入 `observation.stock_anomaly_tag`（个股异常标签表），主状态写入个股观察主表。

### 4.3 主状态优先级

当多个状态条件同时满足时，按以下顺序选择主状态：

```text
PANIC_SELLING
HIGH_POSITION_DIVERGENCE
TREND_EXPANSION
ACCUMULATION_LIKE
NEW_ANOMALY
COOLING
NORMAL
```

每次状态必须保存 `state_reason_codes`（状态原因码）。状态转换只依赖 `T` 当时可见的指标、标签和前一观察状态，不得读取未来 Outcome（后验结果）。

### 4.4 `ACCUMULATION_LIKE`（类吸筹形态）规则

该状态只能表示形态类似，不表示真实资金吸筹。默认必须同时满足：

- `price_position_52w < 0.35`；
- `amount_ratio_5_60 >= 1.5`；
- `turnover_ratio_5_60 >= 1.3`；
- `anomaly_frequency_20 >= 0.50`；
- `return_20d` 位于 `[-0.05, 0.12]`；
- `max_drawdown_20d > -0.10`；
- `up_down_volume_ratio >= 1.10`；
- 不依赖行业映射、行业广度或行业确认。

该状态只表示个股价格与成交行为在规则上呈现“类吸筹形态”，不表示真实资金吸筹，
也不表示买入价值。行业共振另行通过 `Sector Confirmation` 保存。

### 4.5 Sector Confirmation（行业共振确认）

行业确认不得参与个股 `primary_state` 的必要条件。每条个股观察必须明确保存：

```text
sector_confirmation_status
    CONFIRMED       行业数据完整且满足确认规则
    NOT_CONFIRMED   行业数据完整但未满足确认规则
    UNKNOWN         数据缺失、PIT 不足、覆盖不足或计算失败

sector_data_status
    CONFIRMED       已通过行业 PIT、覆盖率和连续性门禁
    DAILY_CAPTURED  已按日捕获，但尚未证明完整 PIT 语义
    MISSING         没有可用行业输入
    INVALID         输入存在但未通过质量门禁
```

只有 `sector_data_status = CONFIRMED` 时，`sector_confirmation_status` 才允许为
`CONFIRMED` 或 `NOT_CONFIRMED`。`DAILY_CAPTURED` 不等于 `CONFIRMED` PIT，
不得把每日抓取的当前成分直接宣称为五年历史行业 PIT。

历史行业数据不可用时，个股观察照常计算，行业确认记为 `UNKNOWN`。未来行业数据补齐时，
必须创建新的计算批次和输入哈希，不得更新已经 `FROZEN` 的观察记录。

## 5. 数据表与唯一性

### 5.1 `observation.snapshot`（观察快照表）

保存一次观察发布批次的元数据：

- `snapshot_id`：物理主键；
- `observation_date_start/end`：观察日期范围；
- `methodology_hash`、`data_baseline_id`、`input_manifest_hash`；
- `observation_policy_id`、`contract_hash`；
- `status`、`quality_report_hash`、`code_identity`；
- 行数、股票数、行业数和日期覆盖统计。

### 5.2 `observation.stock_anomaly_daily`（个股异常观察日表）

至少包含：

- `observation_id`：UUID 或 bigint，物理主键；
- `snapshot_id`；
- `observation_date`、`ts_code`；
- `methodology_hash`、`data_baseline_id`；
- 全部指标、五类评分、`observation_priority`、`primary_state`；
- `state_reason_codes`、`calculation_status`、`quality_status`；
- 输入行数/哈希摘要和创建时间。

业务唯一约束：

```text
UNIQUE (
    observation_date,
    ts_code,
    methodology_hash,
    data_baseline_id
)
```

同一业务身份必须映射到同一个 `observation_id`。重试、断点续跑和失败恢复不能产生幽灵记录。

### 5.3 子表

- `observation.stock_anomaly_tag`（个股异常标签表）：引用 `observation_id`；
- `observation.sector_anomaly_daily`（行业异常观察日表）：按观察日和 PIT 行业代码保存行业统计；
- `observation.stock_sector_confirmation`（个股行业确认表）：引用 `observation_id`，保存行业体系、行业代码、
  行业数据状态、确认状态、确认分数和质量原因；
- `observation.event_link`（当时可见事件证据表）：只保存 `AS_OF_EVIDENCE`（观察日当时可见证据）；
- `evaluation.retrospective_event_link`（事后事件关联表）：只保存 `RETROSPECTIVE_CAUSE`（观察日之后确认的可能原因）；
- `evaluation.market_anomaly_outcome`（异常观察后验结果表）：只保存未来客观表现；
- `evaluation.scenario_result`（策略场景结果表）：保存带入场、执行、止盈、止损和费用假设的场景结果。

子表统一通过不可变的 `observation_id` 引用主观察记录，不复制四字段联合业务键作为外键。
行业确认表是附加证据表，不是个股主状态表；行业记录缺失不能阻止个股主表生成。

## 6. AS_OF 与 RETROSPECTIVE 证据

`observation.event_link` 只允许保存 `AS_OF_EVIDENCE`，必须包含：

```text
evidence_role
event_visible_at
source_record_id
source_hash
```

该表的 `evidence_role` 固定为：

- `AS_OF_EVIDENCE`：观察日已经可见，可参与当日解释和规则判断；

观察日之后才确认的原因必须写入 `evaluation.retrospective_event_link`，并包含：

- `observation_id`；
- `event_visible_at`；
- `source_record_id`；
- `source_hash`；
- `evidence_role = RETROSPECTIVE_CAUSE`；
- `recorded_at`（记录时间）。

`RETROSPECTIVE_CAUSE` 永远不得触发历史评分、标签、主状态、`observation_hash`（观察哈希）或 `snapshot_hash`（快照哈希）更新。它属于 Evaluation / Retrospective Analysis（评估/事后分析），不属于“当时看到什么”的冻结观察内容。

## 7. Evaluation（评估）契约

### 7.1 通用后验结果

`evaluation.market_anomaly_outcome` 不使用买入语义，默认以观察日 `T` 为起点：

```text
future_price_return_xd
= adj_close_base[T+xd] / adj_close_base[T] - 1

future_total_return_xd
= total_return_price[T+xd] / total_return_price[T] - 1
```

其中：

- `adj_close_base`（底座复权收盘价）用于价格表现；
- `total_return_price`（总回报价格）用于含分红的股东总回报；
- `raw_close`（原始收盘价）只用于展示、执行坐标和审计；
- `T+xd` 指第 `xd` 个未来交易日，不是自然日。

通用结果至少包含：

```text
future_price_return_5d/10d/20d/60d/120d
future_total_return_5d/10d/20d/60d/120d
excess_return_xd
max_drawdown_xd
max_favorable_excursion_xd
max_adverse_excursion_xd
volatility_xd
best_return_xd
worst_return_xd
```

未来窗口尚未成熟时保持 `PENDING_MATURITY`（等待成熟），不得用当前结果填充。相同观察记录和相同评估方法只允许幂等写入。

### 7.2 策略场景结果

`evaluation.scenario_result` 才允许包含：

- 入场时间和成交价格；
- T+1 开盘、滑点、手续费；
- 涨跌停、停牌和部分成交规则；
- 止损、止盈、持有期和资金规则；
- `hit_target_before_stop`（先达到目标还是先触发止损）。

这些字段不得进入通用观察结果或通用 Outcome（后验结果）。

## 8. 生命周期与不可变性

观察快照状态：

```text
DRAFT → BUILDING → VALIDATED → FROZEN
                     └→ FAILED → BUILDING
```

补充终态：

- `SUPERSEDED`：被正常的新方法论或新基线替代，允许只读复现；
- `CORRUPTED`：已确认数据污染或严重缺陷，禁止研究和评估使用。

规则：

- `FROZEN` 后观察主体、标签、`observation.event_link` 中的 AS_OF 证据和输入哈希不可更新、删除；
- `FROZEN` 后行业确认及行业聚合证据同样不可更新、删除；行业数据后来升级为更高质量状态时，
  必须生成新的观察发布批次，不能原地把 `UNKNOWN` 改为 `CONFIRMED`；
- `evaluation.retrospective_event_link` 可以在未来追加 RETROSPECTIVE_CAUSE，但不得更新观察主体、标签、AS_OF 证据、`observation_hash` 或 `snapshot_hash`；
- 算法变化、基线变化或输入修正必须创建新快照；
- `VALIDATED` 也不能被静默重算；必须显式创建新的计算批次；
- `SUPERSEDED` 不代表数据错误；`CORRUPTED` 才表示禁止复现；
- 后验 Outcome 只能新增成熟窗口结果，不能修改冻结观察主体。

## 9. 增量、回填与事务边界

### 9.1 历史回填

历史回填只读取已经存在并通过门禁的 PIT/Feature，不重建 Raw/Standard。按交易日小批次执行：

```text
日期小批次
 → 个股观察
 → 可用时执行行业聚合与行业确认
 → 事件证据
 → 小批次质量门禁
 → VALIDATED
```

行业输入缺失时，个股观察仍必须完成；该批次只将行业确认置为 `UNKNOWN`，不能把缺失降级成 `NOT_CONFIRMED`。

必须使用 `Checkpoint`（检查点）恢复，禁止跨五年日期的大事务。历史首次计算需要最大 252 个交易日暖机窗口；`days_since_first_anomaly` 只在同一方法论观察序列内累计。

### 9.2 每日增量

每日只计算新交易日，读取最多 252 个交易日的有限回看窗口，并复用前一日已冻结观察状态。后验结果按成熟窗口独立追加，不重新扫描五年数据。

### 9.3 数据修正

若上游修正影响某日及之后的滚动窗口：

1. 识别受影响日期范围；
2. 创建新的 `calculation_run_id` 和 `methodology_hash`/输入哈希；
3. 只重算受影响的小范围观察批次；
4. 原冻结结果保留为历史版本；
5. 不允许原地更新旧 `observation_id`。

所有写入必须通过 `Observation Repository → Kernel`，使用受限的 `SCOPED_MERGE`（范围合并）或 `PARTITION_REPLACE`（分区替换）。禁止万能 `execute()`、公开连接/游标和业务层 `commit`。

## 10. 质量门禁

进入 `VALIDATED` 前至少检查：

1. 业务唯一键无重复；
2. `observation_id` 与业务身份稳定映射；
3. 日期和股票核心观察覆盖率达到政策要求；行业覆盖率仅在行业确认状态不是 `UNKNOWN` 时作为行业门禁；
4. 计算窗口满足最小有效样本数；
5. 行业确认启用时，行业分母确实来自观察日 PIT 股票池；行业不可用时必须存在 `UNKNOWN` 原因码；
6. 事件可见时间没有晚于观察日的 `AS_OF_EVIDENCE`；
7. `RETROSPECTIVE_CAUSE` 没有进入评分、标签和状态计算；
8. 主状态满足优先级和原因码规则；
9. 标签明细与主表记录一一对应；
10. 输入基线、方法论、代码身份和字段哈希完整；
11. 行业确认启用时，行业异常总数、广度和新增/持续/退出数量相互一致；
12. 无跨日期大事务、N+1 查询或无边界全表扫描；
13. 同一输入重复运行结果完全一致；
14. `FROZEN` 后数据库层拒绝更新和删除。

任何 `BLOCK`（阻断级）质量问题都不得进入 `FROZEN`。`WARNING`（警告）必须保留原因和统计数量，不得静默吞掉。

## 11. Repository 与测试要求

至少建立以下 Repository（仓储）：

- `StockAnomalyObservationRepository`（个股异常观察仓储）；
- `SectorAnomalyObservationRepository`（行业异常观察仓储）；
- `StockSectorConfirmationRepository`（个股行业确认仓储）；
- `ObservationEventRepository`（观察事件仓储）；
- `MarketAnomalyOutcomeRepository`（后验结果仓储）；
- `ScenarioEvaluationRepository`（策略场景评估仓储）。

必须补齐：

- 指标公式固定样本单元测试；
- 缺失、停牌、上市不足和行业缺失测试；
- PIT 未来事件拒绝测试；
- AS_OF/RETROSPECTIVE 隔离测试；
- 状态优先级和状态转换测试；
- 断点续跑、重试和幂等测试；
- FROZEN 更新/删除拒绝测试；
- 行业聚合一致性测试；
- 行业缺失不阻断个股观察、`UNKNOWN` 与 `NOT_CONFIRMED` 区分测试；
- `DAILY_CAPTURED` 不得直接升级为 `CONFIRMED` PIT 测试；
- 后验窗口成熟前后测试；
- 通用 Outcome 与 Scenario 语义隔离测试；
- Repository → Kernel 数据访问契约扫描；
- 小批量真实数据库集成测试。

## 12. 实施顺序

严格按以下顺序推进：

```text
本文契约冻结
 → 字段/来源缺口盘点
 → Schema 与 DatasetPolicy
 → Repository 与质量门禁
 → 单元/集成测试
 → 小批量历史回放
 → 结果抽样和哈希核对
 → 正式历史回填
 → 每日增量与成熟 Outcome 自动化
```

当前正式方法论采用“个股主链路 + 行业可选确认”。行业历史 PIT 尚未闭环时，
不得阻断个股历史回放；但也不得把没有 PIT 证据的行业字段标记为 `CONFIRMED`。

在契约、表结构、Policy（策略）和测试完成前，不得启动五年历史回填。该观察域不得修改既有 `Research Snapshot` 契约，也不得把观察评分直接改造成策略买入信号。
