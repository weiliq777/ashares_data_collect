# A股个人投资研究系统三阶段开发计划

## 2026-08-08 latest audit status

Stage 3 code is functionally closed for historical Backtest production gates. Reserve cash, settled-cash availability, prior-day liquidity, lot-targeted stops, delist fail-closed behavior, Raw lineage, Snapshot build consistency, and database order contracts are implemented and tested. Remaining work is production data generation and audit: keep the long checkpointed historical Raw initializer running, create the complete multi-date FROZEN Snapshot series, then build/validate/freeze the full Dataset and Run. No automatic trading is enabled.

2026-08-08 增补：时点价格链路已升级为 `feature.price_daily_v2`，正式 Research 契约为
`research-v1.3`。它只使用已实施且 `ex_date <= trade_date` 的公司行为构造时点总回报指标；
旧 `price_daily_v1`/`research-v1.1`/`research-v1.2` 仅作只读历史兼容，不能混入新的正式回测。

## 2026-08-06 阶段三生产增强版进度

阶段三的回测核心已完成生产增强收口：风险压力模型、Tushare 执行单位归一化、成交量参与率门禁、Monte Carlo 双方法、Backtest→Portfolio 人工隔离和数据库订单审计哈希已实现并有专项测试。数据库迁移为 `028`–`033`，其中 `032`/`033` 只扩展 Backtest/Portfolio，不触碰 Raw。

代码开发已经完成；正式历史回测仍必须执行既定生产门禁：规则审批、干净 Git commit、多日期 `research-v1.2` FROZEN Snapshot、Dataset `build → validate → freeze`、再创建正式 Run。Portfolio 只能人工批准 `BACKTEST/PENDING_APPROVAL` 意图，禁止自动下单；第一版配股仍明确不模拟。

截至 2026-08-07，三日 Baseline Dataset/Run 已真实完成烟测；后续属于完整历史 Snapshot 生成、五年 Dataset/Run 生产和结果审计，不再以“阶段三核心代码未完成”描述当前状态。

完整执行顺序、命令模板、验收门禁和恢复边界见 `dev_doc/project_completion_runbook.md`。

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

### 阶段二本轮实现补充（2026-08-04）

阶段二已冻结为以下可重建流水线：

```text
六类状态/公司行为/指数 Raw
→ 空响应补采
→ Standard 状态与公司行为
→ PIT 股票池、可交易性、财务 as-of
→ Feature v1
→ 质量、未来函数、单位和幂等验收
```

本轮新增的 Raw 接口为 `stock_st`、`suspend_d`、`stk_limit`、`namechange`、`dividend` 和 `index_daily`。历史初始化可以按股票断点执行；日常增量按接口能力选择：状态类按交易日，分红按公告日期窗口，名称变更低频按日期窗口，指数按代码和日期区间。所有任务使用 checkpoint 续传；完整原始 JSON、哈希、批次和抓取时间必须保留，Raw 禁止更新或删除。

对应派生对象为 `std.security_lifecycle`、`std.security_status_daily`、`std.dividend_event`、`std.index_daily_tushare`、`pit.universe_daily`、`pit.security_tradeability_daily`、`pit.financial_asof`，以及 `feature.price_daily_v1`、`feature.valuation_daily_v1`、`feature.company_financial_v1`、`feature.shareholder_return_v1` 和 `ops.feature_registry`。

阶段二由 `tools/phase2_resume_and_finish.ps1` 串行编排，避免并发触发接口流控；完成后由只读质量任务检查 Raw 覆盖、重复键、空响应、财务未来函数、单位、可交易性、缺失复权因子和重建幂等性。阶段二未通过验收前，不进入 Research、Backtest 和 Portfolio。

## 阶段三：Research、Backtest 与 Portfolio

### 2026-08-06 Backtest 实施进度（更新）

阶段三已从设计进入代码实现。已新增 `sql/028_create_backtest_model.sql`、
`data_collect/jobs/backtest_dataset.py`、`data_collect/jobs/backtest_run.py` 和
`data_collect/backtest/`，覆盖 Dataset/Run Manifest、执行行情/日历/公司行为/规则
冻结、纯 Python 日线撮合、事件账本、风险预算和结果持久化。

当前状态：迁移可重复执行，专项测试通过；三日 Backtest Dataset/Run 已完成烟测，完整历史 Dataset/Run 待生成。
正式构建前必须补齐评估区间交易日历，并显式配置
`backtest.market_rule_effective`，包括市场、费用、T+1、整手、价位和资金结算规则。
当前数据库交易日历已覆盖现有 Research 快照截止日，但市场/费用规则仍未配置，且单一快照不足以支撑历史回测；因此禁止伪造日期或直接开始正式回测。

目标：形成研究快照、可解释回测和人工确认的模拟组合流程。

需要建设：

```text
research_snapshot, research_watchlist, research_evidence_reference
backtest_strategy, backtest_run, backtest_signal, backtest_order
backtest_fill, backtest_position, backtest_cash_ledger, portfolio_simulation
```

阶段三当前实现边界：`research_watchlist.py` 只消费已通过质量门禁的快照，默认要求 `FROZEN`；`allow_preview` 仅用于联调 `VALIDATED` 快照。观察池使用固定 `watchlist-v1` 规则保存价值质量/成长趋势分轨、总人数上限、去重结果和结构化理由。`research.evidence_reference` 只保存 OpenSearch、URL 或人工材料的引用定位、内容哈希和正反方角色，不复制正文。详见 `dev_doc/research_watchlist.md`。

当前实现状态（2026-08-05）：正式 FROZEN 快照为
`9afb9a08-da75-439b-8b4a-fcc688df3787`，截止日为 `2026-08-03`，包含 5,535 条股票快照、8 张输入统计和 44,280 条血缘记录；正式观察池
`01b53411-155e-4672-9548-189396e7a3e0` 已激活，包含 3 个 `value_quality` 成员和 9 条 PostgreSQL 快照事实引用。`sql/027_research_audit_immutability.sql` 已保证 ACTIVE 成员/证据只能追加，ARCHIVED 观察池、成员和证据全部不可变。

外部证据候选由 `data_collect/jobs/research_evidence_candidates.py` 只读检索，返回 OpenSearch 文档身份、来源、时间、URL 和摘要片段，并标记 `REVIEW_REQUIRED`；它不会自动写入 `research.evidence_reference`。真实联调时，当前 3 个成员的股票标签没有命中，名称文本命中不能直接视为证据，必须人工核验后再调用 `add_evidence`。因此 Research 观察池框架已完成，外部证据人工复核仍待完成；Backtest Dataset/Run 输入设计与基础实现已完成，正式数据构建待日历和规则配置补齐。

Backtest 设计约束：当前正式 FROZEN 快照只有截止日 `2026-08-03`，只能用于该日期的研究和观察池，不能单独支撑历史回测。历史回测必须先生成多个历史截止日的 FROZEN 快照，或建立引用多个日快照的 Backtest Dataset Manifest；随后才冻结 Backtest 表结构、输入契约和回测引擎。回测必须处理费用、停牌、涨跌停、分红、退市以及信号时间与成交时间，不接入自动下单。

阶段三 Backtest 已进入代码实现，具体契约见 `dev_doc/backtest_design.md` 和任务 `T-018`、`T-028`。第一版已把 FROZEN Research Snapshot、执行行情、公司行为、交易日历、市场规则和费用设计为 Dataset 冻结边界，再由 Run Manifest 绑定策略版本、参数、代码和运行规则。当前正式回测尚未开始：数据库日历已覆盖现有快照截止日，但市场费用规则尚未配置，且历史回测仍缺少多个日期的 FROZEN 快照；Gemini 示例代码只作为参考，不能直接读取实时表或在数据库失败时回退合成数据。新增的按日覆盖率熔断和 PIT 只读检查已接线，但不会替代显式规则和多日期快照门禁。

2026-08-06 已补充 Portfolio v1：`sql/029_create_portfolio_model.sql`、`data_collect/portfolio/` 和
`data_collect/jobs/portfolio_simulation.py`。它只允许绑定 FROZEN Snapshot/ACTIVE Watchlist，
交易意图必须人工批准，成交固定为 `PAPER_MANUAL`；已覆盖整手、预留现金、T+1、费用、禁止已有持仓加仓、
追加式现金/审计账本和基于 FROZEN 原始收盘价的每日估值。当前尚未创建正式 Portfolio。

第一版默认采用：T 日收盘生成信号、T+1 日开盘尝试成交、只做多、不模拟部分成交、暂不模拟配股；使用每日 PIT 历史股票池而不是当前观察池替代历史股票池。正式开发顺序为 Dataset/Run Manifest → Frozen Dataset Materializer → Input Adapter → 撮合与事件账本 → 绩效和样本外验证。

主要工作：

1. 生成结构化股票研究快照并关联公告、新闻和研报证据。
2. 实现少量清晰策略，回测纳入费用、停牌、涨跌停、分红和退市。
3. 分离信号时间、下单时间和成交时间。
4. 建立模拟组合、仓位、现金和风险记录。
5. AI 只负责解释、摘要、反方证据和风险提示，不直接下单。

验收：回测可按数据和特征版本重算，包含交易成本和不可成交情况，研究结论可追溯。

阶段三第一主线冻结为 Research 快照可复现基础：

```text
024 快照表结构
→ Manifest/输入切片统计
→ REPEATABLE READ 一致性生成
→ source lineage
→ validate
→ FROZEN
→ Research 观察池
→ Backtest
```

第一版一个 `snapshot_id` 只允许一个 `research_cutoff_date`。快照生成器可以读取通过
质量门禁的 PIT/Feature；Research 和 Backtest 只能读取 `FROZEN` 快照。输入哈希只覆盖
实际使用的切片和字段，并保存筛选条件、字段集合、哈希算法、行数、日期范围和内容哈希。
Manifest 必须绑定 validated 数据基线、Schema 版本、Git 版本、质量报告哈希和输入汇总哈希。
工作区有未提交修改时必须提供存在的代码归档；`diff_hash` 仅作为补充审计信息，不能替代可复现代码。数据库触发器保证非法状态跳转和
`FROZEN` 后的更新/删除失败。详细实施契约见 `dev_doc/research_snapshot.md`。

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

## 阶段三开发基线更新（2026-08-06，正式版审计修复后）

Backtest/Portfolio 的第一版代码已落地，但正式历史回测仍受门禁约束：

- `backtest.dataset_manifest` 已冻结输入切片谓词、字段集合、哈希算法、可见性规则和输入哈希；`run_manifest` 已记录参数哈希。
- `dataset_member` 已按研究日保存执行行情切片哈希和公司行为切片哈希，并共同进入 Dataset 输入 Manifest；新增 `research_snapshot_series.py` 只按显式日期/标准交易日历编排多个单日快照，不改变“一个 snapshot 对应一个截止日”的边界。
- 回测时序固定为 T 收盘信号、T+1 开盘尝试成交；止损优先于同日策略卖出；Frozen Dataset 不允许缺市场规则时回退默认值。正式 Run 必须显式设置组合总风险上限。
- 公司行为仅使用实施版本，保留公告日期和送转到账日期；`stk_div` 是送转合计且必须与分项一致；除权日调整原始止损/送转成本，第一版不模拟配股，暂不实现 ATR/Trailing Stop。送转零碎股按整数股向下取整，不产生现金补偿；Dataset 同时绑定基线 `quality_report_hash` 并将其纳入输入哈希，基线变化时必须重新构建。
- 当前交易日历及行情派生层已覆盖到 `2026-08-06`；`backtest.market_rule_effective` 已导入批准规则，三日 FROZEN Dataset/Run 已完成烟测；完整历史回测仍缺少逐 SSE 开市日的 Snapshot 系列。
- Portfolio 仅允许人工批准的 `PAPER_MANUAL` 模拟成交，不连接券商、不自动下单；组合 Manifest 保存 T+1、现金结算天数、费用和最小价位规则；Raw 表继续只读、不可修改或删除。每日估值保存绑定 Snapshot 与 `valuation_input_hash`，同输入幂等、冲突拒绝，且禁止在更晚成交后回补过去估值。
- Dataset 校验已升级为对账最终物化子表：全局日历/规则/执行行情/公司行为哈希，以及成员级按研究日哈希，都必须与 Manifest 一致；规则同时按 `market_rule_version`、`fee_rule_version` 和生效日期筛选，公司行为按业务键去重，Dataset 创建还要求 warmup 到 evaluation_end 的每个 SSE 开市日均有 FROZEN Snapshot，独立 validate 也会复核完整区间及成员角色；规则同主键内容冲突、审核元数据未批准、以及 build 后代码身份变化均会阻断正式发布；快照系列和 Dataset 截止日必须为开市日；Dataset 还保存基线 `baseline_quality_report_hash` 并将其纳入 `input_manifest_hash`，数据库触发器校验与 validated 发布点一致；47 项正式审计专项测试、迁移执行和数据库回滚烟测已通过。规则有效区间另由 PostgreSQL `EXCLUDE USING gist` 防止同一作用域重叠。执行行情缺少日线/状态/可交易性任一来源时标记 `MISSING_SOURCE` 并阻断正式 Dataset；行情物化或全局派生重建失败会追加最新 `BLOCKED` 门禁，不能让旧 PASS 放行；历史未建立 Raw version 的行允许版本号为空但必须有 payload 哈希。Dataset 选择版本 Raw 时只解析一个完整 payload，不跨版本按字段拼接；退市持仓无真实退出事件时 fail-closed；同日 BUY 按分数排序，T+1 开盘按实际价格重算股数，实际股数变化会记录 `resized_at_execution`；送转零碎股 v1 按整数股向下取整，不产生现金补偿；Monte Carlo 记录方法/参数/输入序列哈希；Run 触发器强制输入哈希和规则版本与 Dataset 一致。另在临时隔离数据库完成 Dataset→Run→Portfolio 全链路联调，结果已清理且未改生产库。新增 `ops.market_data_quality_gate` 按日覆盖率门禁和 `phase3_validation.py` PIT 只读校验；正式 Dataset/Run/Portfolio 仍受显式规则、多日期 `research-v1.1` FROZEN 快照和干净代码版本门禁。
## 2026-08-07 最新状态覆盖

本节覆盖本文中早于 2026-08-07 的“尚无 Dataset/Run”历史描述，详细权威说明见 `dev_doc/current_implementation_state_20260807.md`。

阶段三当前不是“尚未创建 Dataset/Run”：三日 Baseline 已真实完成：

- Dataset `4013823d-2cf9-48f8-aff5-b642f0989329` 已 `FROZEN`；
- Run `9f278214-606d-40b1-9ce4-b8ee6ce2c1ad` 已 `SUCCEEDED`；
- 91 条订单、6 条成交、85 条拒单，实际成交股数与 Fill 对账为 900/900；
- Dataset/Run 输入哈希、规则版本、现金账本和订单审计哈希检查通过；
- 停牌/退市/不可交易无行情保留 WARNING，不伪造价格；`actual_shares` 只能由真实 Fill 产生；
- Raw、Standard/PIT/Feature 不被 Backtest 回写。

当前仍未完成的是完整五年生产回测：需要为评价区间每个 SSE 开市日生成 `research-v1.2` FROZEN Snapshot，并重新创建 v2 Dataset。Portfolio 仍保持 `PENDING_APPROVAL → PAPER_MANUAL` 人工隔离，不自动下单。旧的“生产库尚无 Dataset/Run”只作为历史记录保留，不再代表当前状态。

## 2026-08-08 最新生产门禁状态

阶段三代码审计已通过，当前全量测试为 `693 passed, 1 skipped, 5 deselected`。但生产数据尚未切换到当前契约：源码要求 `research-v1.2` FROZEN Snapshot 和 v2 Dataset/适配器/撮合版本，数据库现有 v1.1 快照系列不完整（690 条 FROZEN、1 条 DRAFT、五年区间缺 522 个 SSE 开市日），旧 v1 Dataset/Run 只能作为不可变烟测历史。后续必须在干净代码版本上生成 v1.2 系列，再执行 Dataset `build → validate → freeze` 和正式 Run；不得静默升级旧记录。
