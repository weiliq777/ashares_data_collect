# A股个人投资研究系统三阶段开发计划

版本：v1.0  
时间：2026-08-02

## 总目标

基于 Tushare、PostgreSQL、Python 和 OpenSearch，建立可追溯、可复算、避免未来函数的个人投资研究系统。系统定位为 AI 辅助研究和风险检查工具，不自动下单，人工做最终投资决策。

总体链路：

```text
数据源 → Raw 原始层 → Standard 标准层 → PIT 时间可用层
→ Feature 特征层 → Research 研究层 → Backtest 回测层
→ Portfolio 模拟组合 → 人工最终决策
```

## 阶段一：数据基础层

目标：完成可恢复、可续传、可审计的核心事实数据仓库。

优先接口：

```text
stock_basic, trade_cal, daily, daily_basic, adj_factor
fina_indicator, income, balancesheet, cashflow
```

后续补充：

```text
stock_st, namechange, suspend_d, stk_limit, dividend, index_daily
```

主要工作：

1. 完成行情和财务 raw 历史初始化。
2. 所有 Tushare 请求先保存完整 raw 响应。
3. 保持批次限流、断点续传、幂等写入和错误日志。
4. 建立独立 raw→standard 转换任务。
5. 补充 raw 表和标准表索引。
6. 增加日期覆盖、股票覆盖、重复键、空值和单位检查。
7. 保留 `ann_date`、`end_date`、抓取时间和来源字段。

验收：raw 可续传、无重复主键、覆盖范围可核对、标准表可从 raw 重建。

## 阶段二：PIT 与 Feature 层

目标：还原历史某一天真实可使用的信息，并生成可复现特征。

需要建设：

```text
security_lifecycle
security_status_daily
pit.universe_daily
pit.financial_asof
pit.security_tradeability_daily
feature.price_daily_v1
feature.valuation_daily_v1
feature.company_financial_v1
```

主要工作：

1. 根据上市、退市、ST、停牌和涨跌停状态重建历史股票池。
2. 使用公告日期构建财务 as-of 数据，避免未来函数。
3. 区分原始价格、复权价格和总回报。
4. 计算 MA20、MA60、成交量均值、趋势和波动率。
5. 计算 PE/PB 历史分位，并标记负 PE 和样本不足。
6. 计算 ROE、资产负债率、经营现金流和营收增长等特征。
7. 为特征记录版本、计算时间和数据窗口。

验收：任意历史日可还原股票池，财务数据不穿越，特征可从 raw/standard 重算。

## 阶段三：Research、Backtest 与 Portfolio

目标：形成研究快照、可解释回测和人工确认的模拟组合流程。

需要建设：

```text
research_snapshot, research_watchlist, research_evidence_reference
backtest_strategy, backtest_run, backtest_signal, backtest_order
backtest_fill, backtest_position, backtest_cash_ledger, portfolio_simulation
```

主要工作：

1. 生成结构化股票研究快照并关联公告、新闻和研报证据。
2. 实现少量清晰策略，回测纳入费用、停牌、涨跌停、分红和退市。
3. 分离信号时间、下单时间和成交时间。
4. 建立模拟组合、仓位、现金和风险记录。
5. AI 只负责解释、摘要、反方证据和风险提示，不直接下单。

验收：回测可按数据和特征版本重算，包含交易成本和不可成交情况，研究结论可追溯。

## 当前执行顺序

```text
行情 raw 初始化 → 财务 raw 初始化 → raw→standard 转换器
→ 数据质量检查 → 股票历史状态/公司行为 → PIT → Feature v1
→ Research 快照 → Backtest v1 → 模拟组合
```

## 明确不做

QMT/MiniQMT 实盘依赖、Tick/Level-2、高频交易、自动下单、AI 预测次日涨跌，以及未经回测和模拟验证的买卖建议。

## 核心开发约束（补充冻结）

1. `Standard` 层同时负责字段、单位、日期、主键标准化和质量状态标记；不静默删除异常事实数据。质量状态至少区分 `VALID`、`WARNING`、`INVALID`、`MISSING_SOURCE`、`UNIT_SUSPECTED`。
2. `PIT` 必须位于 Feature 和 Backtest 之前。财务数据必须保留 `ann_date`、`end_date` 和原始记录版本；`f_ann_date`、`report_type`、`comp_type`、`update_flag` 等字段按各接口实际提供情况保存，不强求所有接口完全一致。PIT 层统一生成 `effective_announce_date` 和 `record_version`，禁止仅按报告期覆盖旧记录。
3. 历史股票池和可交易状态分开建设：`universe_daily` 回答“是否属于研究范围”，`security_tradeability_daily` 回答“当天是否可能成交”。ST 是风险状态，不天然等于不可交易。
4. 在正式回测前优先补齐 `namechange`、`suspend_d`、`stk_limit`、`dividend`；`index_daily` 可稍后用于基准、Beta 和超额收益。
5. 明确区分：原始价格用于成交和涨跌停判断，前复权价格用于均线和趋势分析，总回报用于长期持有绩效；不能用前复权价格模拟真实成交。
6. Research 与 Backtest 分开开发：先形成可追溯研究快照，再建设信号、订单、成交、持仓和现金模型。
7. 回测必须区分信号时间、下单时间和成交时间，处理费用、停牌、涨跌停、分红和退市；不能把收盘后才知道的信息用于当天成交。
8. AI 只输出结论、支持证据、反方证据、风险、数据缺失和不确定性，不参与历史信号生成、不修改结构化特征、不自动下单。

## 阶段一最小闭环与首个里程碑

第一轮只完成阶段一，不并行开发 AI、回测和组合模块：

```text
Raw 表和任务批次 → 行情历史初始化 → 财务历史初始化
→ Raw→Standard 转换 → 数据质量报告
```

阶段一首个里程碑：删除一张 Standard 测试表后，可以通过一个命令从 Raw 完整重建，并核对行数、主键、单位、日期覆盖和质量报告一致。通过后再进入历史状态和 PIT 开发。
## 阶段一补充冻结：财务报表标准化规则

`income`、`balancesheet`、`cashflow` 先完整保存 Raw，再分别建立 Standard 财务事实表，不合并成一张大表：

```text
tushare_income_raw       -> financial_income_standard
tushare_balancesheet_raw -> financial_balance_sheet_standard
tushare_cashflow_raw     -> financial_cash_flow_standard
```

财务 Raw 还需增加不可变版本表，保留 `report_date`、`ann_date`、`f_ann_date`、`report_type`、`comp_type`、`end_type`、`update_flag`、原始 `payload`、`payload_hash`、`batch_id` 和 `fetched_at`。现有 Raw 表不得更新或删除。

财务 Standard 必须保留报告期、公告日、报表口径和版本信息；PIT 层生成 `effective_announce_date`，优先使用接口提供的 `f_ann_date`，为空时回退到 `ann_date`，同时保留两个原始日期。Standard 保留全部报告记录，研究默认使用合并报表，不能在 Raw/Standard 阶段静默覆盖其他口径或修订版本。

第一版 Standard 只映射选股、风险和现金流质量所需的核心字段，其余字段继续保留在 Raw。金额、股数、比例必须显式记录单位，不做未经确认的倍数换算。财务 Standard 主键不能只使用股票代码和报告期，必须包含公告日、报表口径、版本和来源等维度。

当前状态：四张财务 Raw、财务 Raw 版本表和三张财务 Standard 均已完成；市场 Raw/Standard 也已完成全量质量检查。阶段一已通过 Raw→Standard 独立重建验收，后续进入历史状态、公司行为和 PIT 层开发。
