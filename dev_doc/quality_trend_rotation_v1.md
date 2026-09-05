# QualityTrendRotation_V1（质量趋势轮动 V1）执行契约

本文件是 `QualityTrendRotation_V1`（质量趋势轮动 V1）的实现契约。代码、配置和
测试与本文件冲突时，默认代码错误。该策略用于可复现研究与回测，不构成投资建议。

## 1. 领域边界

`Feature`（特征）只提供价格、成交量和财务事实；`Strategy`（策略）只决定候选、
排序和信号；`Scenario`（交易场景）决定资金、整手、撮合和交易费用；`Evaluation`
（评价）只决定评价区间、基准和统计口径。策略代码只接收 FROZEN（冻结）的
`BacktestDataset`（回测数据集），不连接数据库、不查询实时 PIT/Feature。

## 2. 信号频率和执行时序

1. `signal_frequency = WEEKLY_LAST_TRADING_DAY`：每个自然周最后一个交易日的收盘
   计算一次全部买入和卖出决策。节假日由交易日历决定，不硬编码星期五。
2. 信号在 `T`（信号日）收盘形成，订单在下一交易日 `T+1` 开盘尝试成交。
3. 买入和普通卖出都只在周信号日产生；每日不重新计算候选，不因盘中价格变化
   伪造策略信号。
4. 如果卖出因停牌、跌停、不可卖、T+1 或流动性门禁失败，保留同一订单的
   `PENDING`（待处理）状态和 `retryable_sell=true` 标志，在每个后续交易日开盘
   重试。数据库状态不新增未经过 SQL 约束批准的枚举值。
5. 待卖订单存在时，该股票不能被重新买入或加仓；回测区间最后一天仍未成交则
   `EXPIRED`（过期），不得在区间外伪造成交。

## 3. Universe（股票池）和质量过滤

只有以下条件全部满足，股票才进入候选池；缺失值不是 `FALSE`（否定），而是
不满足候选资格：

- `is_listed_asof = TRUE`（观察日已上市）；
- `eligible_by_market = TRUE`（通过市场股票池门禁）；
- `is_tradeable = TRUE` 且 `can_buy = TRUE`（观察日可交易且可买）；
- `is_st_asof = FALSE`（非 ST/风险状态，V1 不允许 ST）；
- `listed_trading_day_count(T) >= 250`（含上市首个有效交易日和信号日）；
- `financial_feature_contract_version = feature-contract-v2.0`、财务来源指纹非空且
  `ttm_complete = TRUE`；
- `roe_quality_status` 与 `cashflow_quality_status` 若已物化则必须为 `VALID`；
- 财务记录的 `effective_announce_date`（有效公告日）和
  `financial_data_available_timestamp`（财务数据可用时间）均不晚于 `T`；
- `roe_ttm >= 0.08`（TTM 净资产收益率不低于 8%，底层使用比例而不是百分数）；
- `operating_cashflow_ttm > 0`（TTM 经营现金流为正）；
- `0 <= debt_to_assets <= 70`（资产负债率不高于 70%，该字段使用百分数口径）；
- 最近 20 个有效成交额观测（含 `T`）的平均值
  `avg_amount_20 >= 50,000,000`（人民币 5000 万元）。

`listed_trading_day_count` 的实现必须读取冻结快照中的
`available_observation_count`（证券自身累计有效价格观测数），不能从 Dataset 的
`warmup_start` 重新从零累计，否则会把早已上市的证券错误地当成次新股。

早期 `foundation-v2` 冻结快照的物化投影遗漏了财务组件质量状态列，但同时冻结了
财务 v2 契约版本、来源指纹、TTM 完整性及本策略消费的结果字段。兼容读取只允许：
非空 `roe_ttm` 恢复 ROE 可计算状态；`ttm_complete = TRUE` 且非空
`operating_cashflow_ttm` 恢复经营现金流可计算状态。显式存在的非 `VALID` 状态仍然
拒绝，其他 NULL 一律 fail-closed。快照聚合 `quality_status` 混合了估值等无关字段，
不得冒充本策略的财务字段质量门禁。新快照必须直接物化组件状态和现金流比率。

## 4. 趋势、排序和持仓退出

进入候选池的趋势条件为：

```text
adj_close_base(T) > MA20(T)
AND MA20(T) > MA60(T)
AND RET60(T) > 0
```

其中 `RET60`（60 个有效交易日收益率）由冻结数据集中的
`adj_close_base`（基础复权价格）使用当前值与 60 个有效观测前的值计算；缺少
61 个价格观测时不产生候选。`avg_amount_20` 和 `RET60` 的滚动状态只保留有限
窗口，不扫描数据库。

候选池内按以下顺序确定性排序：

```text
RET60 DESC
→ avg_amount_20 DESC
→ ts_code ASC
```

`Top 3`（排名前 3）产生买入信号。已有持仓不重复买入，候选不足 3 只时按实际
候选数运行，剩余资金保留为现金。

持仓卖出规则严格区分进入条件和退出条件：

- `adj_close_base < MA60`：硬趋势退出；
- 股票变为 ST、财务质量失效或当前研究输入缺失：基础资格失效退出；
- 仍在候选池但排名 `> 10`：滞回排名退出；
- 仅跌破 `MA20` 或仅 `RET60 <= 0`，但仍未跌破 `MA60`：不单独卖出；
- 暂时停牌或不可卖不改变卖出意图，只进入待卖重试流程。

## 5. Scenario（交易场景）

默认正式场景由 `ScenarioConfig`（交易场景配置）表达：

```yaml
initial_capital: 10000.00
target_positions: 3
target_position_ratio: 0.30
max_entry_position_ratio: 0.35
forced_cash_reserve_ratio: 0.00
buy_lot_size: 100
signal_time: CLOSE
execution_time: NEXT_TRADING_DAY_OPEN
rebalance_existing_positions: false
leftover_cash: KEEP_AS_CASH
slippage_bps_per_side: 10
broker_commission_rate: 0.0003
broker_minimum_commission: 5.00
fee_schedule_id: cn-a-share-fee-schedule-v1
```

仓位使用目标市值模型，不使用止损距离反推股数。目标市值为总权益的 30%，单次
买入不得超过总权益的 35%，数量向下取整为 100 股的整数倍；不足一手则拒绝该
买单。法定印花税、过户费和其他费用必须按交易日从版本化 `FeeSchedule`（费用
时间表）确定，不能用一个静态费率替代历史费率。

## 6. Evaluation（评价）

默认评价区间为 `2021-09-01` 至 `2026-08-21`，预热区间从冻结数据集的
`warmup_start`（预热起点）开始。主基准为 `CSI300_TOTAL_RETURN`（沪深 300 全收益
指数）；当前数据集只有价格指数时，必须在回测记录中明确写入
`CSI300_PRICE`（沪深 300 价格指数）降级口径，不能静默冒充全收益指数。

当前冻结 Dataset 从 `2021-08-02` 开始提供逐日复权价格，策略必须在内存中实际
积累满 61 个有效价格观测后才计算 `RET60`。因此评价区间开头尚未满足窗口的日期
保持现金且不产生候选；禁止绕过 Dataset 的 `selected_columns` 偷读未纳入冻结契约
的 `return_60d` 字段。

Sharpe（夏普比率）固定使用日收益率、年化因子 252、无风险利率 0 和样本标准差
`ddof=1`。Total Return（总收益）、CAGR（年化复合收益）、Max Drawdown（最大回撤）、
年化波动和 Calmar（卡玛比率）的计算口径与 `EvaluationConfig`（评价配置）及
回测引擎保持一致。

## 7. 版本身份

策略参数生成 `strategy_hash`（策略哈希）；资金/撮合参数生成 `scenario_hash`；
评价口径生成 `evaluation_hash`；冻结回测数据集使用 `dataset_hash`。一次回测的
`backtest_fingerprint`（回测指纹）为：

```text
hash(strategy_hash + scenario_hash + dataset_hash + engine_version)
```

任何规则、场景、评价或引擎语义变化都必须产生新的身份，不得覆盖旧策略定义、
旧回测结果或已冻结数据。
