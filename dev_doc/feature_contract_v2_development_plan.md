# A股通用研究数据底座建设方案

**状态：Architecture Approved；Ready for Implementation；Not Production Frozen**  
**适用范围：Raw → Standard → Unified As-of → Feature → Research Snapshot → Strategy**  
**编制日期：2026-08-12**

## 1. 方案结论

本轮审计的总体结论合理，可以作为 A 股通用研究数据底座的正式开发基线，但冻结前必须把“字段名称、数据来源、可见性口径和兼容策略”写成可执行契约。

本方案不建议直接改名或删除 V1 字段，而是采用“新增规范字段 + 保留旧字段兼容”的迁移方式。当前研究快照、特征脚本和历史查询仍可能依赖 net_profit、roe、V1 特征表等旧语义，直接重命名会造成隐性回测断裂。

V2 的核心原则如下：

1. 现金流质量使用合并口径净利润：cashflow.n_cashflow_act / income.n_income。
2. 股东回报、ROE 和面向股东的利润增长使用归母净利润：income.n_income_attr_p。
3. 供应商原始 ROE 与自行计算的 roe_ttm 分开命名，不能把 Tushare 的报告值伪装成自算 TTM。
4. 所有 YTD 财务数据先做单季桥接或 TTM 桥接，再进入跨期比较和特征计算。
5. PIT 是选择规则，不由 Feature 临时取“最新值”；同一决策日的跨表组件必须使用同一个可见性截止点。
6. `dv_ttm` 直接保存 Tushare `daily_basic` 的供应商口径，同时保存 `dividend` 事件事实；事件重建值不冒充供应商值。
7. `comp_type` 不是本底座的策略过滤器；所有公司类型保留，按指标分别输出适用性状态，策略层再自行筛选。
8. 当前版本是架构批准和开发基线，不是生产冻结契约；生产冻结必须经过实现、全历史回填、PIT 回放、样本核验和结果复现。

## 1.1 建设边界：增量补齐，不是系统重构

本阶段的目标是补齐研究数据底座，而不是重新设计量化系统。当前架构已经具备 Raw → Standard → PIT-aware Feature → Research Snapshot 的主链路，本轮工作应采用“增量扩展、定向回算、必要时版本隔离”的方式推进。

### 分层改造原则

| 层级 | 本阶段原则 | 是否删除旧数据 |
|---|---|---|
| Raw | 保留完整原始 payload、抓取时间、hash 和来源信息；新增字段优先从既有 payload 投影 | 不删除 |
| Standard | 增加明确的规范字段和来源映射；必要时从 Raw 重新生成规范投影 | 不删除旧字段，旧字段仅作兼容 |
| PIT | 提供统一的 as-of 选择逻辑，保证 income、cashflow、balancesheet 使用同一观察时点；独立物化 PIT 表是物理实现选择 | 不覆盖 Raw/Standard 事实 |
| Feature | 新增指标直接增加字段；只有旧字段语义改变、粒度改变或需要并行对账时才升 Feature 版本 | V1 不原地改语义 |
| Research Snapshot | 物化并冻结指定契约版本的结果；后续回算生成新快照，不修改已冻结快照 | 已冻结快照不覆盖 |

### 三类变更的处理方式

1. **新增字段且旧语义不变**：增加 Standard 列、增加 Feature 列并回填受影响历史数据，不需要新建整套 V2。
2. **修正旧字段语义**：Raw 保持不动，Standard 增加显式字段，旧字段保留兼容；Feature 使用新字段，必要时生成 V2，避免旧回测被重新解释。
3. **改变主键、数据粒度或 PIT 选择边界**：增加新视图、表或版本，并保留旧结果用于对账；这才属于需要版本隔离的改造。

因此，本文中的“V2”主要表示一组更明确的字段契约和必要的 Feature 版本，不表示 Raw、Standard、PIT、Feature 全链路推倒重建。

## 1.2 数据底座与策略层的职责边界

本方案一次性建设长期通用、与具体策略无关的研究数据能力。后续策略只能消费这些数据，不应因为更换价值、成长、高股息、质量、趋势或困境反转策略而重新解释基础字段。

### Standard 层：可解释事实

Standard 只保存来源明确、口径稳定、可追溯的事实，不保存投资判断：

```text
net_profit_consolidated
net_profit_parent
revenue
operating_cashflow_ytd
parent_equity_end
pe_ttm_vendor
ps_ttm_vendor
pb_vendor
dv_ttm_vendor
```

以下内容不属于 Standard：

```text
quality_score
cheap_flag
good_company
buy_signal
```

### Feature 层：公共派生指标

Feature 保存任何多个策略都可能复用的公共派生指标，例如：

```text
net_profit_parent_ttm
operating_cashflow_ttm
roe_ttm
gross_margin_ttm
ocf_to_net_profit_consolidated_ttm
revenue_growth_yoy
profit_cagr_5y
pe_percentile_5y
```

`value_score`、`growth_score`、候选池、买卖信号和 AI 排序属于策略或研究消费者，不写入通用 Feature 契约。

### 字段契约必须同时记录

每个 Standard/Feature 字段至少记录：来源、原始字段、计算公式、报告/交易时间口径、单位、精度、空值规则、质量状态和版本追溯信息。示例：

| 字段 | 来源/计算 | 关键说明 |
|---|---|---|
| `net_profit_parent` | `income.n_income_attr_p` | 归母净利润，原始财务事实 |
| `net_profit_parent_ttm` | YTD + FY - 同期 YTD | 归母净利润 TTM |
| `roe_ttm` | 归母净利润 TTM / 平均归母权益 | 系统自算口径，期初期末报告期末权益平均 |
| `dv_ttm_vendor` | `daily_basic.dv_ttm` | Tushare 供应商滚动股息率，不等于重建值 |

## 2. 外部数据源核验

当前设计所需的关键数据，Tushare 均有对应接口或现有 Raw 数据来源：

| 目标数据 | Tushare 来源 | 关键字段 | 能否获取历史数据 | 方案判断 |
|---|---|---|---|---|
| 合并净利润 | income | n_income | 可以，按股票和报告期查询 | 作为 net_profit_consolidated 主来源 |
| 归母净利润 | income | n_income_attr_p | 可以 | 作为 net_profit_parent 主来源 |
| 经营现金流净额 | cashflow | n_cashflow_act | 可以，按股票和报告期查询 | 作为 operating_cashflow_ytd 主来源 |
| 供应商 ROE | fina_indicator | roe、roe_yearly、q_roe | 可以 | 仅作为 reported/annualized/quarterly 原始指标 |
| 期末归母权益 | balancesheet | total_hldr_eqy_exc_min_int | 可以 | 自算 roe_ttm 的权益端 |
| 期末合并权益 | balancesheet | total_hldr_eqy_inc_min_int | 可以 | 资产负债表标准层保留 |
| 滚动股息率 | daily_basic | dv_ttm | 可以按交易日历史查询 | 作为 `dv_ttm_vendor` 直接来源 |
| 股息事件 | dividend | cash_div、cash_div_tax、ex_date | 可以 | 作为分红事实和 `cash_div_12m` 的来源 |

官方文档入口：

- [income 利润表](https://tushare.pro/document/2?doc_id=33)
- [cashflow 现金流量表](https://tushare.pro/document/2?doc_id=44)
- [fina_indicator 财务指标](https://tushare.pro/document/2?doc_id=79)
- [daily_basic 每日指标](https://tushare.pro/document/2?doc_id=32)
- [dividend 分红送转](https://tushare.pro/document/2?doc_id=103)
- [balancesheet 资产负债表](https://tushare.pro/document/2?doc_id=36)

### 2.1 数据源边界

接口“存在”不等于已经满足生产回溯。开发前需要把以下限制纳入任务设计：

- income、cashflow、balancesheet 的标准财务字段是报告期口径，利润和现金流字段通常是年初至今累计值。
- cashflow.net_profit 在部分季度可能为空，不能把它当作利润主来源；利润主来源固定为 income.n_income 或 income.n_income_attr_p。
- fina_indicator.ocf_to_profit 的分母是营业利润，不是净利润，因此不能用于命名或填充 ocf_to_net_profit。
- daily_basic.dv_ttm 已经存在历史数据源，但它是供应商估值字段，不等同于自行按分红事件和价格重建的股息率。
- 直接按全市场、全历史补数时，应按 Tushare 当前积分、频率、单次返回量和批量权限设计分批任务，并支持断点续抓。
- fina_indicator 不提供 comp_type；ROE 的适用类型必须通过同报告期的 income、cashflow 或 balancesheet 记录关联获得，不能假设 ROE 记录自身带有 comp_type。
- income、cashflow、balancesheet 的正常 Feature 默认只消费 report_type=1（最新合并报表）；report_type=4、5 等调整或调整前记录保留在 Standard，用于 PIT 审计、版本差异分析和更正研究，不进入正常 Feature。
- TTM 桥接必须同时校验 end_date、end_type 和财年归属，不能只按报告期日期排序；Q1、H1、Q3、FY 的累计值必须在同一财年内配对。
- daily_basic 官方说明按交易日 15:00–17:00 更新，但输出字段没有逐行 update_time；不能凭空生成供应商更新时间，系统应使用“官方更新时间策略 + source_observed_at”推导可用时间。
- Tushare 普通财务接口主要按单只股票查询，fina_indicator 每次返回量有限，daily_basic 需要按交易日分批；全市场回填必须具备分页、断点、限速、重试和失败任务恢复能力。

### 2.2 接口支持结论与工程限制

结论是“数据源支持，但需要按接口能力设计采集与回填”，不能把接口存在理解为已经具备无限量、精确发布时间和完美历史版本。

- `income`、`cashflow`、`balancesheet` 均支持历史报告期查询，并提供 `ann_date`、`f_ann_date`、`end_date`、`report_type`、`comp_type`、`end_type` 等财务版本选择字段；三张表应按同一业务键和同一报告版本规则配对。
- 三大报表的普通接口主要按单只股票查询；按报告期获取全市场数据需要相应 VIP 接口和权限。全 A 股五年回填必须先确认账户积分、频率和 VIP 权限，再确定任务规模。
- `fina_indicator` 支持 `roe`、`roe_yearly`、`q_roe` 以及大量供应商增长/利润率指标，但官方输出字段不应被假定为完整的 `comp_type`、`report_type`、`end_type` 版本事实。它作为供应商指标补充，并通过三大报表关联公司类型。
- `daily_basic` 支持历史 `pe_ttm`、`ps_ttm`、`pb`、`dv_ttm`，单次返回量有限且按交易日批量获取；`dv_ttm` 必须命名为供应商口径，不能与事件重建值混用。
- `dividend` 支持 `cash_div`、`cash_div_tax`、`record_date`、`ex_date`、`pay_date` 等分红事件事实。税前/税后单位、除息日和每股口径必须在字段字典固定。
- Tushare 没有为所有历史财务版本提供可等同交易所公告原文的精确发布时间，也没有为 `daily_basic` 提供逐行更新时间。因此 PIT 只能声明为 `as-observed PIT`，并保存证据等级，不能宣称 exchange-perfect PIT。

## 3. 当前仓库基线与缺口

当前代码已经具备 Raw、Standard、Feature 和 Research Snapshot 的主体链路，但尚未完全实现 V2 契约：

| 位置 | 当前状态 | V2 需要的改造 |
|---|---|---|
| data_collect/jobs/tushare_financial_raw_to_standard.py | income 保留了旧 net_profit，未规范映射 n_income | 增加合并净利润和归母净利润的显式映射 |
| data_collect/jobs/tushare_raw_to_standard.py | valuation_daily 已有 dv_ratio，未透传 dv_ttm | 增加 dv_ttm 及来源字段 |
| sql/017_create_financial_standard.sql | 财务标准表有部分旧字段，权益和现金流语义未完整规范 | 增加规范字段和必要索引，保留兼容字段 |
| sql/021_create_fina_indicator_version_standard.sql | 主要保存 roe | 增加 roe_reported、roe_annualized、roe_quarterly |
| sql/018_create_phase2_model.sql | 只有 V1 估值、公司财务、股东回报等结构 | 优先增加兼容扩展列；仅在语义或粒度变化时增加 V2 输出结构 |
| data_collect/jobs/tushare_phase2_features.py | 当前 OCF/利润使用旧 net_profit，只有零值保护 | 改用合并净利润、增加负利润熔断、TTM 和质量状态 |
| data_collect/jobs/research_snapshot.py | 已按 version_available_date 选择可见 Feature | 兼容旧日期字段，同时迁移到 decision_timestamp/data_available_timestamp，暂不强行拆出独立 PIT 表 |
| data_collect/jobs/backtest_dataset.py | 使用冻结快照和特征版本 | 增加 V2 契约版本、字段完整性和数据质量检查 |
| dev_doc/current_data_model.md 等文档 | 已描述部分 V2 方向，但字段契约不完整 | 更新字段字典、依赖关系、迁移和验收说明 |

特别需要注意：当前体系更准确的表述是 PIT-aware Feature，即在特征生成或快照读取时按可见版本筛选；独立、统一物化的 PIT Dataset 是可选物理实现，不应与 as-of 选择契约混为一谈。

## 4. V2 数据契约

### 4.1 Standard 层字段

#### 利润表标准字段

| Standard 字段 | 来源 | 语义 |
|---|---|---|
| net_profit_consolidated | income.n_income | 合并口径净利润，包含少数股东损益 |
| net_profit_parent | income.n_income_attr_p | 归属于母公司股东的净利润 |
| net_profit | 兼容别名 | 当前兼容语义等同于 net_profit_consolidated，必须在字段字典中标明别名语义 |

net_profit 不建议删除，但从 V2 开始所有新计算不得依赖模糊字段名。下游新代码必须优先使用显式字段。

#### 现金流量表标准字段

| Standard 字段 | 来源 | 语义 |
|---|---|---|
| operating_cashflow_ytd | cashflow.n_cashflow_act | 经营活动产生的现金流量净额，报告期累计值 |
| n_cashflow_act | 兼容保留 | 与原始 Tushare 字段同义 |
| cashflow_net_profit_reconciliation | cashflow.net_profit | 现金流量表附带利润字段，仅用于核对，不作为利润主来源 |

#### 资产负债表标准字段

| Standard 字段 | 来源 | 语义 |
|---|---|---|
| parent_equity_end | balancesheet.total_hldr_eqy_exc_min_int | 期末归属于母公司股东权益 |
| total_equity_end | balancesheet.total_hldr_eqy_inc_min_int | 期末合并股东权益 |

roe_ttm 的分母使用 parent_equity_end。这里的平均权益是报告期末权益的平均值，不应表述为每日平均权益。

#### ROE 标准字段

| Standard 字段 | 来源 | 语义 |
|---|---|---|
| roe_reported | fina_indicator.roe | Tushare 报告值，具体算法遵循供应商定义 |
| roe_annualized | fina_indicator.roe_yearly | Tushare 年化/年度相关指标，不能直接等同自算 TTM ROE |
| roe_quarterly | fina_indicator.q_roe | Tushare 季度相关指标 |
| roe | 兼容保留 | 旧字段，必须标记为供应商口径，不再作为 roe_ttm 的别名 |

#### 估值标准字段

| Standard 字段 | 来源 | 语义 |
|---|---|---|
| dv_ratio | daily_basic.dv_ratio | 供应商股息率字段，单位为百分比（%），保留原语义 |
| dv_ttm | daily_basic.dv_ttm | 供应商滚动股息率字段，单位为百分比（%） |
| dv_ttm_source | 派生 | 供应商字段固定标记为 TUSHARE_DAILY_BASIC |
| data_available_timestamp | 派生 | 按来源可见性策略计算的统一可用时间，不等同于 source_observed_at |

### 4.2 数据来源与报告版本规则

Standard 必须保留供应商返回的全部必要版本信息，但正常 Feature 只消费统一的默认报告版本：

```text
report_type = 1
```

这条规则适用于 income、cashflow 和 balancesheet；`end_type` 也必须作为报告期类型保留和校验。`report_type=4`（调整合并报表）、`report_type=5`（调整前合并报表）及其他报告类型不得被三个表分别自动选取后再混合 Join。它们只用于：

- PIT 版本审计；
- 调整前后数据差异分析；
- 财报更正研究；
- 数据质量排查。

`fina_indicator` 没有 `comp_type`，ROE 标准记录必须通过 `ts_code + end_date + PIT 可见版本` 与财务报表主表关联获得合并类型。关联失败时不得猜测类型，应输出 `UNKNOWN_COMP_TYPE` 或对应缺失状态。

### 4.3 Standard 版本事实规则

Standard 不是只保留“当前最新值”的覆盖表。对于同一业务记录发生数据变化的情况，必须保留可追溯版本：

```text
standard_record_id       # 代理标识
ts_code
end_date
report_type
comp_type
end_type
payload_hash
source_observed_at
update_flag
```

`payload_hash` 用于识别同一报告期的内容变化，`source_observed_at` 表示本系统观察到该版本的时间，`update_flag` 保留 Tushare 的更新标识。Standard 的物理主键不应简单使用 `ts_code + end_date`，也不应因新版本到来而删除旧版本；PIT/Feature 层再按决策时点选择唯一可见版本。

同一 payload 的重复抓取可以幂等去重，但不得把不同 hash 的版本互相覆盖。`source_observed_at` 是数据血缘字段，不得直接冒充供应商公告可用时间。

### 4.4 Feature 层字段

逻辑上按以下领域组织，物理上优先采用现有 Feature 表新增列。只有字段语义改变、数据粒度改变或需要与旧结果并行对账时，才新增 V2 版本表或版本化物化结果；不因领域划分自动拆表。

#### company_quality（逻辑分组）

~~~text
roe_reported
roe_annualized
roe_quarterly
roe_ttm
roe_quality_reason
roe_avg_5y
roe_std_5y
roe_observation_years
debt_to_assets
gross_margin_ttm
net_margin_consolidated_ttm
comp_type
roe_quality_status
~~~

roe_ttm 定义为：

~~~text
归母净利润 TTM /
((期初归母权益 + 期末归母权益) / 2)
~~~

建议只在净利润 TTM、期初权益、期末权益齐全且平均权益为正时输出有效值。至少定义以下状态：

- VALID
- MISSING_NET_PROFIT
- MISSING_EQUITY
- NEGATIVE_OR_ZERO_EQUITY
- NOT_APPLICABLE

单位约定：供应商 `roe_reported/roe_annualized/roe_quarterly` 保留 Tushare 的百分数口径；
系统自算 `roe_ttm`、`gross_margin_ttm`、`net_margin_consolidated_ttm`、增长率和 CAGR
统一使用小数比例。毛利率只允许使用营业收入与营业成本计算；营业成本缺失时输出 NULL，
不得使用包含税金及期间费用的营业总成本替代。`debt_to_assets` 使用资产负债表负债/
资产自算小数比例；缺失时保持 NULL，不直接回退到百分数单位的供应商指标。

#### cashflow_quality（逻辑分组）

~~~text
operating_cashflow_q
operating_cashflow_ttm
net_profit_consolidated_q
net_profit_consolidated_ttm
net_profit_parent_ttm
ocf_to_net_profit_consolidated_ttm
ttm_basis
ttm_complete
ttm_required_component_count
ttm_component_count
ocf_quality_reason
ocf_positive_years_5y
profit_positive_years_5y
cashflow_observation_years_5y
cashflow_window_complete
comp_type
cashflow_quality_status
~~~

经营现金流质量比率定义为：

~~~text
ocf_to_net_profit_consolidated_ttm
= operating_cashflow_ttm / net_profit_consolidated_ttm
~~~

熔断规则：

- net_profit_consolidated_ttm <= 0 时，比率输出 NULL，并写入 NEGATIVE_OR_ZERO_PROFIT。
- OCF 为负但利润为正时保留负比率，不得把它静默裁剪为零。
- 分子、分母任一缺失时输出 NULL，并记录缺失原因。
- 不使用 fina_indicator.ocf_to_profit 填充该字段。

`ocf_positive_years_5y` 和 `profit_positive_years_5y` 必须同时配套观察年数和 `cashflow_window_complete`；历史不足五年时不能把较小的正数年数解释为完整五年质量结论。

#### TTM 桥接规则

利润表和现金流量表的 TTM 使用同一报告期、同一可见性截止点和同一财年桥接规则：

~~~text
TTM = 本期 YTD + 上一财年 FY - 上年同期 YTD
~~~

例如 2026Q1：

~~~text
2026Q1 TTM = 2026Q1 YTD + 2025FY - 2025Q1 YTD
~~~

单季度值使用同一财年内相邻累计值相减；Q1 直接使用 Q1 YTD，年报直接使用 FY。计算时必须校验 `end_type`，不得因为日期排序或缺失而把不同财年、不同报告类型的记录相减。每个组件都必须记录：

- ttm_basis：DIRECT_FY、YTD_BRIDGE 或 UNAVAILABLE
- ttm_required_component_count：该 basis 理论上需要的组件数；DIRECT_FY 为 1，YTD_BRIDGE 为 3
- ttm_component_count：实际参与桥接的组件数量
- ttm_complete：DIRECT_FY 且实际数为 1，或 YTD_BRIDGE 且实际数为 3 时为 true

缺少上一财年或上年同期时，不得用不完整值伪装为完整 TTM；可以输出状态为 INCOMPLETE_TTM 的结果，但默认数值字段为空。

#### growth（逻辑分组）

~~~text
revenue_growth_yoy
profit_growth_yoy
revenue_cagr_5y
profit_cagr_5y
profit_loss_event_5y
loss_year_count_5y
profit_observation_years_5y
profit_window_complete
profit_recovery_flag
recovery_basis_period
profit_metric_basis
growth_quality_status
~~~

面向股东的利润增长默认使用 net_profit_parent，现金流质量仍使用合并净利润。所有泛化字段如 profit_growth_yoy 都必须通过 profit_metric_basis 明确是归母还是合并口径，避免未来误用。

第一版困境反转事实口径固定为：

- `profit_loss_event_5y`：在过去 5 个完整年度数据齐全时，归母净利润小于等于 0 的年度至少有 1 个；
- `loss_year_count_5y`：上述亏损年度数量；
- `profit_observation_years_5y`：实际可见的完整年度数量；
- `profit_window_complete`：是否具备完整 5 年年度观察窗口；
- `profit_recovery_flag`：过去 5 年窗口完整、存在亏损年度，且最新可见归母净利润 TTM 大于 0；窗口不完整时输出 NULL 并记录 `INSUFFICIENT_HISTORY`，不能把历史不足误判为“没有亏损”；
- `recovery_basis_period`：实际用于判断恢复的报告期，例如 `2026Q1_TTM`。

第一版不把“连续两个季度转正”或扣非净利润纳入该事实字段；这些属于后续策略或扣非质量模型。

#### valuation（逻辑分组）

~~~text
pe_ttm
pb
ps
dv_ratio
dv_ttm
dv_ttm_source
pe_percentile_5y
pb_percentile_5y
dv_ttm_percentile_5y
dv_window_start
dv_window_end
dv_observation_days
dv_window_complete
data_available_timestamp
~~~

dv_ttm_percentile_5y 只使用决策日前可见的 dv_ttm 历史观测；零股息率是有效观测，不应当被当作缺失值。最低计算门槛固定为 250 个有效交易日：少于 250 天时分位数为空，达到 250 天可以计算；是否覆盖完整 5 年由 `dv_window_complete` 表示，而不是由字段名假定。估值分位的窗口、最小观测数和异常值规则必须写入配置或字段字典，不能散落在脚本常量中。

#### market（逻辑分组）

沿用现有价格和交易特征链路。本方案不额外扩展技术指标范围，只保留 feature_contract_version 或等价版本标识，保证回测能区分不同契约结果。

### 4.5 合并类型适用性

数据底座保留 comp_type=1/2/3/4 的原始财务数据。不同指标必须分别判断适用性，不能给整只股票设置一个笼统的适用/不适用结论；例如银行的 ROE 可以有效，但现金流质量比率可能是 `NOT_APPLICABLE`。具体策略是否筛选 comp_type=1，由策略层决定。

`comp_type` 本身缺失时，现金流适用性必须标记 `UNKNOWN_COMP_TYPE`，不能归入
`NOT_APPLICABLE`。利润同比只有上年同期基数为正时才输出百分比；亏损转盈利使用
`profit_recovery_flag` 表达，不输出误导性的超大增长率。

Feature 至少分别输出：

~~~text
cashflow_quality_status
roe_quality_status
growth_quality_status
~~~

策略层可以限制 comp_type=1，但 Feature 层不应删除或过滤其他公司类型。对于不适用的指标输出：

~~~text
<metric>_quality_status = NOT_APPLICABLE
~~~

不能把不适用的合并类型混入同一排名池，也不能默认为零。

### 4.6 需要版本隔离时的可选物理粒度

以下是发生语义或粒度变化、需要并行对账时的可选物理粒度，不代表本期必须全部新建：

| 对象 | 一行代表 | 关键标识 |
|---|---|---|
| Standard 财务事实 | 一个来源业务记录的一个可追溯版本 | `standard_record_id`、业务键、`payload_hash` |
| company_financial_v2 | 一个股票、报告期、公司类型、报告类型下被选中的 Feature 版本 | `ts_code`、`report_period`、`comp_type`、`report_type`、所选 Standard 版本 |
| valuation_daily_v2 | 一个股票在一个交易日、一个契约版本和一个数据截止点的估值结果 | `ts_code`、`trade_date`、`feature_contract_version`、`data_cutoff_timestamp` |
| research_snapshot_v2 | 一个快照中的一只股票在一个 as-of 时点的冻结结果 | `snapshot_id`、`ts_code`、`asof_date`、`feature_contract_version` |

不能只使用 `ts_code + end_date` 对财务事实去重。同一报告期的不同 `report_type`、更正版本和不同 payload 必须能够同时存在；Feature 或 PIT 选择层再产生唯一的可消费记录。

## 5. PIT 与版本规则

### 5.1 当前阶段的实现方式

当前仓库的 PIT 能力主要体现在 Feature 和 Research Snapshot 的 version_available_date <= decision_date 筛选，因此数据底座继续采用 PIT-aware Feature，并通过统一 as-of 选择逻辑保证跨表一致；是否物化独立 PIT 表不改变契约。

PIT 级别明确为 `as-observed PIT`：基于系统实际保存或观察到的供应商版本进行时间还原，不宣称等价于交易所原始公告事件流或当时所有市场参与者看到的完整信息。

跨表计算必须强制遵守统一可见性规则：

~~~text
effective_data_available_timestamp
= MAX(现金流组件可用时间,
      利润表组件可用时间,
      资产负债表组件可用时间)
~~~

一条 Feature 记录的有效可见时间应取所有输入组件中最晚的 `data_available_timestamp`。查询某个 `decision_timestamp` 时，所有 TTM 组件都要先按该时点筛选版本，再进行桥接计算。

### 5.2 来源可用时间规则

统一字段为 `data_available_timestamp`，而不是把 `ann_date`、`trade_date` 和抓取时间混为一个概念：

| 来源 | component_available_date | data_available_timestamp |
|---|---|---|
| income/cashflow/balancesheet | `COALESCE(f_ann_date, ann_date)` | 日期型公告字段按保守的日末可见策略转换；如未来取得公告时分，才允许使用更精确时间 |
| fina_indicator | `ann_date` | 按同一日期型公告策略转换；该接口没有 `f_ann_date` |
| daily_basic | `trade_date` | 按 Tushare 官方交易日 15:00–17:00 更新窗口，第一版采用交易日 17:00 的可用策略 |

`daily_basic` 官方输出没有逐行 `update_time`，因此不得伪造供应商更新时间。`source_observed_at` 只表示本系统实际抓取或观察到数据的时间，用于血缘和 as-observed 证据，不直接替代来源可用时间。

统一 PIT 判断为：

~~~text
data_available_timestamp <= decision_timestamp
~~~

因此盘前决策不能使用当日 `daily_basic.dv_ttm`；收盘后且达到数据可用时间的研究任务才可以使用当日值。若历史数据只有交易日字段、没有对应时点的保存记录，应标记 PIT 证据等级，而不能宣称具备完美市场 PIT。当前历史财务以公告日期策略推定可见时间，同日多版本更正按仓库实际观察时间约束；因此 Research V2 整体证据标记为 `SOURCE_DATE_WITH_OBSERVED_VERSIONS`，不得标记为完美 `AS_OBSERVED` 或交易所精确时点。

### 5.3 版本字段要求

Raw 层继续保存抓取时间、原始 payload、payload hash 和请求批次信息。Standard 层至少保留：

~~~text
ann_date
f_ann_date
version_available_date
standard_record_id or stable surrogate identifier
source_record_key
record_version
source_observed_at
payload_hash
update_flag
data_available_timestamp
~~~

对于更正公告或同一报告期多版本记录，不得只按报告期覆盖。需要保留可追溯版本，并在 Standard 去重或 Feature 选择时使用明确的版本优先规则。

### 5.4 Research Snapshot 规则

Research Snapshot 负责冻结策略实际使用的 Feature 结果：

- 保存 feature_contract_version = V2。
- 保存输入数据版本或快照批次标识。
- 保存 decision_date、decision_timestamp、data_cutoff_timestamp、trade_date 和各组件的最大可见时间。
- 保存 PIT 证据等级或 provenance 状态，区分实际观察版本与按来源日期推断的可用时间。
- 禁止在快照生成后回查最新财报覆盖历史值。

## 6. 需要改造的代码、表和文档

### 6.0 完整建设范围

本方案不拆分数据能力等级。凡是长期通用、与具体策略无关、后续多个策略都会复用的能力，本次均纳入数据底座建设；执行时按依赖顺序推进，但不以 P0/P1/P2 推迟基础契约。

#### Standard 事实层

- 补齐 `net_profit_consolidated`、`net_profit_parent`、`operating_cashflow_ytd`、`parent_equity_end`、`total_equity_end`、收入、成本和利润字段。
- 补齐 `pe_ttm_vendor`、`ps_ttm_vendor`、`pb_vendor`、`dv_ttm_vendor`，保留供应商来源标识。
- 保留原始字段、`report_type`、`end_type`、`comp_type`、`update_flag` 和版本追溯字段；不删除旧字段或改变其既有语义。

#### Unified as-of 层

- 统一 income、cashflow、balancesheet、fina_indicator 的可见版本选择规则。
- 任何跨表 Feature 取数都使用同一 `decision_date/decision_timestamp`，有效可见时间取所有输入组件的最大值。
- 可以由公共 SQL/函数或统一 Feature 输入查询实现；是否物化独立 PIT 表属于物理实现选择，不改变 as-of 契约。
- 明确声明 `as-observed PIT`，保存可见性证据，不伪造供应商不存在的精确发布时间。

#### 公共 Feature 层

- 完成单季度、TTM、ROE、Margin、Growth、现金流质量、估值分位和五年历史窗口字段。
- 完成 `ttm_basis`、组件数量、完整性、观察年数、窗口完整性和指标级质量状态。
- 新增指标优先扩展现有 Feature；只有语义或粒度变化时才做 Feature 版本隔离。
- 不写入 `value_score`、`growth_score`、`buy_signal`、候选池或 AI 排序等策略判断。

#### 历史回填与验证

- 完成至少五年、目标覆盖范围内的历史回填，并具备分片、断点、限速、重试和失败恢复能力。
- 完成 Raw → Standard → as-of → Feature 的逐层对账和可回溯检查。
- 样本覆盖白马、少数股东权益较大的公司、亏损股、ST、次新股、周期/资源股、制造业、银行以及存在财报更正的公司。

### 6.1 Standard、Feature、PIT 与模型改造

#### 代码

1. ashares_data_collect/data_collect/jobs/tushare_financial_raw_to_standard.py
   - 将 income.n_income 映射为 net_profit_consolidated。
   - 将 income.n_income_attr_p 映射为 net_profit_parent。
   - 保留 net_profit 兼容字段，并明确其等同于合并净利润。
   - 将 cashflow.n_cashflow_act 规范为 operating_cashflow_ytd。
   - 不把 cashflow.net_profit 当作利润主来源。
   - 保留 income、cashflow、balancesheet 的 report_type 和 comp_type；正常 Feature 统一选择 report_type=1。
   - 对 fina_indicator 记录通过财务报表主表关联 comp_type，关联失败时输出未知类型状态，不自行猜测。
   - 生成 data_available_timestamp，并继承 payload_hash、source_observed_at、update_flag。

2. ashares_data_collect/data_collect/jobs/tushare_phase2_features.py
   - 重构公司财务特征输入，读取显式 Standard 字段。
   - 增加财年内单季差值和 TTM 桥接。
   - 增加负利润、零利润、缺失组件、负权益等状态。
   - 计算 roe_ttm、现金流质量和利润增长时分别使用规定口径。
   - 组件选择统一使用同一 decision_timestamp，并计算最大 data_available_timestamp。
   - 实现 ttm_required_component_count、ttm_component_count 和 ttm_complete 的 basis-specific 规则。
   - 输出 loss_year_count_5y、recovery_basis_period 及指标级质量状态。

3. ashares_data_collect/data_collect/jobs/tushare_raw_to_standard.py
   - 将 daily_basic.dv_ttm 透传到估值标准层。
   - 写入 dv_ttm_source = TUSHARE_DAILY_BASIC。
   - 按官方 15:00–17:00 更新窗口生成可用时间；不依赖不存在的逐行 update_time 字段。

#### SQL/模型

1. ashares_data_collect/sql/017_create_financial_standard.sql
   - 为利润、现金流和权益增加显式规范字段。
   - 增加 report_type、comp_type、data_available_timestamp、source_observed_at、payload_hash、update_flag。
   - update_flag 按供应商字段继承；daily_basic 等未提供该字段的来源标记为 NULL 或 NOT_APPLICABLE，不自行伪造。
   - 采用追加式版本事实设计，增加 standard_record_id 或等价代理标识及相关索引。
   - 保留原字段，避免破坏历史兼容查询。

2. ashares_data_collect/sql/021_create_fina_indicator_version_standard.sql
   - 增加 roe_reported、roe_annualized、roe_quarterly。
   - 保留 roe，并在字段字典标记为供应商兼容字段。

3. ashares_data_collect/sql/018_create_phase2_model.sql
   - 先以新增列或兼容扩展为主，不因每个新增指标都创建 V2 表。
   - 只有旧字段语义改变、数据粒度改变或需要并行对账时，才增加 company_financial_v2、valuation_daily_v2 等独立对象。
   - 若创建 V2 结果集，必须区分 ts_code、report_period/trade_date、comp_type、report_type、Feature 契约版本和所选 Standard 记录。
   - V1 不删除、不原地改变语义；新增字段的回填不等于覆盖已冻结 Snapshot。

### 6.2 公共估值指标与研究输出

- `dv_ttm` 作为 Standard/Feature 的通用字段补齐，并明确标记供应商来源。
- 实现 dv_ttm_percentile_5y、250 个有效交易日最低门槛、窗口起止日和 dv_window_complete。
- 增加 comp_type、cashflow_quality_status、roe_quality_status、growth_quality_status、feature_contract_version。
- 更新 research_snapshot.py，冻结公共 Feature 字段、版本和最大可见时间。
- 增加 decision_timestamp、data_cutoff_timestamp 和 PIT 证据等级。
- 更新 backtest_dataset.py 的字段校验和版本兼容逻辑。

### 6.3 策略或专项研究阶段的内容

- 基于 dividend 事件、除权价格和税前/税后口径重建 `dv_ttm_rebuild`，并与供应商值做差异研究。
- 将供应商 dv_ttm 与重建值做差异监测。
- 针对特殊 comp_type 和金融、地产等行业建立专门的权益与现金流模型。
- 价值、成长、高股息、质量、趋势、困境反转的组合评分和信号定义。
- AI 解释、候选池、排序和买卖决策。

## 7. 推荐实施顺序（依赖关系，不是能力分期）

### 步骤 1：字段契约和迁移准备

- 固化字段字典、来源映射、单位、精度、空值和质量状态。
- 确认 V1 兼容字段的别名关系。
- 设计新增列、索引、唯一键和回滚方式；只有语义变化或粒度变化才设计独立 V2 表。
- 固化 report_type=1 的正常 Feature 选择规则，以及调整版本的审计边界。
- 固化 date-only 公告数据、daily_basic 交易日数据到 data_available_timestamp 的转换策略。
- 先做小样本映射验证，样本至少包括 000858.SZ、600519.SH、存在少数股东的公司、亏损公司和财报更正记录。

### 步骤 2：Standard 事实层补齐

- 补齐利润、现金流、权益和 ROE 显式字段。
- 补齐 dv_ttm。
- 对历史 Raw 做可重复、可断点的增量回填。
- 运行源字段与标准字段的行数、报告期、单位和数值对账。

全市场回填任务必须采用可恢复的 Collector 设计，至少支持：

- 按接口、股票、报告期或交易日分片；
- 分页和断点续跑；
- 频率限制和指数退避重试；
- 失败任务记录与恢复；
- Raw 幂等写入和 payload_hash 变化检测。

### 步骤 3：统一 as-of 与公共 Feature 计算

- 先实现单季度转换和 TTM 桥接。
- 再实现 roe_ttm、ocf_to_net_profit_consolidated_ttm、增长和估值分位。
- 最后实现指标级质量状态、适用范围、PIT 时间戳和契约版本。
- 新增指标优先写入现有 Feature 的新增列；只有字段语义变化、粒度变化或需要并行对账时，才写入独立 V2 对象。
- 任何情况下都不原地改变 V1 已有字段的语义。

### 步骤 4：历史回填、PIT 回放与 Snapshot 验证

- 用多个 decision_timestamp 重放同一股票。
- 验证公告日边界、daily_basic 当日可用时点、财报更正、跨表最大可见时间和缺失 TTM 组件。
- 验证公共 as-of 选择逻辑；独立物化 PIT Dataset 不是本次必须的物理形态。
- 只有当新增 Feature 契约需要并行验证时，才生成 V2 Research Snapshot；已有冻结 Snapshot 不重新解释。

### 步骤 5：全范围回填与结果发布

- 先用代表性样本和有限区间做工程烟测，确认字段映射、TTM、as-of 和异常状态正确。
- 烟测通过后完成目标范围至少五年的全历史回填，而不是把有限样本当作最终数据底座。
- 仅对确实发生语义变化的字段做 V1/新契约并行对账；新增字段直接纳入公共 Feature。
- 回填、对账和 Snapshot 复现通过后，再向策略层发布数据契约。

## 8. 验收标准

### 8.1 字段与来源验收

- net_profit_consolidated 与 income.n_income 可逐记录追溯。
- net_profit_parent 与 income.n_income_attr_p 可逐记录追溯。
- operating_cashflow_ytd 与 cashflow.n_cashflow_act 可逐记录追溯。
- roe_reported、roe_annualized、roe_quarterly 的供应商来源清晰。
- dv_ttm 能从历史 daily_basic 记录回溯，并带有来源标识。
- comp_type 能从财务报表主表追溯，fina_indicator 不被错误地当作自带 comp_type。
- report_type=1 的正常 Feature 选择与 report_type=4/5 的审计用途边界清晰。
- Standard 记录具备 payload_hash、source_observed_at、update_flag 和 data_available_timestamp。

### 8.2 计算验收

- Q1、H1、Q3、FY 的单季差值正确。
- TTM 配对同时通过 end_date、end_type、财年和 report_type 校验，不混用不同财年或调整版本。
- TTM 使用“本期 YTD + 上年 FY - 上年同期 YTD”，并能通过人工样本复核。
- DIRECT_FY 满足 required=1/actual=1，YTD_BRIDGE 满足 required=3/actual=3；缺失组件时 complete=false。
- 缺少桥接组件时不会生成“看起来完整”的 TTM。
- 合并净利润用于 OCF/利润比率，归母净利润用于 roe_ttm 和股东口径增长。
- 利润小于等于零时 OCF/利润比率为空且带原因；不会出现负负得正的假健康结果。
- 负 OCF/正利润的比率保留真实负值。
- profit_loss_event_5y、loss_year_count_5y、profit_recovery_flag 和 recovery_basis_period 能通过人工样本复核。
- 历史不足五年时，profit_window_complete 和 cashflow_window_complete 为 false，相关五年结论不会被误输出为“无事件”或完整质量结论。
- dv_ttm 少于 250 个有效交易日时分位数为空；达到门槛但不足完整五年时 window_complete=false。

### 8.3 PIT 验收

- decision_timestamp 之前不可见的数据不会进入历史特征。
- 跨表输入的 effective_data_available_timestamp 等于各组件可用时间的最大值。
- 盘前决策不会使用当日 daily_basic；收盘后只有达到官方可用时间才能使用当日值。
- 财报更正不会静默覆盖历史版本。
- Research Snapshot 可复现同一批次的结果。

### 8.4 业务样本验收

至少完成以下样本复核：

- 000858.SZ：验证传统白马股的现金流、分红和估值字段。
- 600519.SH：验证高 ROE、分红和 TTM 估值字段。
- 有较大少数股东权益的公司：验证合并净利润与归母净利润没有混用。
- 亏损或经营现金流为负的公司：验证熔断和质量状态。
- 存在更正公告的公司：验证 PIT 版本选择。

## 9. 兼容、回滚和运维要求

### 兼容策略

- V1 表、字段和结果不删除、不原地改语义。
- 新代码优先读取语义明确的 Standard/Feature 字段；只有版本隔离场景才读取独立 V2 对象。
- 对外查询层可以继续提供旧字段别名，但必须在字典中标记兼容属性。
- Feature 和 Snapshot 都带版本号，策略显式选择 V1 或 V2。
- Standard 采用追加式版本事实；同一业务键出现不同 payload_hash 时新增版本，不覆盖旧记录。

### 回滚策略

- Standard 新字段采用追加式迁移。
- 新增指标优先使用现有 Feature 的新增列；发生语义变化时使用独立表、独立视图或独立版本分区。
- 发现问题时只切回 V1 输入，不回滚或删除 Raw 数据。
- 所有回填任务记录批次、参数、开始结束时间、成功失败计数和错误样本。

### 监控指标

- 各表按报告期的缺失率和重复率。
- TTM 完整率、ttm_component_count 分布。
- NEGATIVE_OR_ZERO_PROFIT、MISSING_EQUITY、NOT_APPLICABLE、UNKNOWN_COMP_TYPE 占比。
- source_observed_at 与 data_available_timestamp 的延迟和异常分布。
- Standard 与 Raw 的抽样差异。
- dv_ttm 与未来 dv_ttm_rebuild 的差异率。
- 旧契约与新契约在有效重叠期间的分布差异和排名变化。

## 10. 本期明确不做的事情

- 不把 Tushare ocf_to_profit 改名后直接当作 OCF/净利润。
- 不把 Tushare roe_yearly 直接标记为自算 roe_ttm。
- 不用 cashflow.net_profit 替代 income.n_income。
- 不把供应商 `dv_ttm` 与事件重建值混为一个字段。
- 不在没有明确行业会计口径前，把所有 comp_type 混入同一评分模型。
- 本文件是后续代码、SQL、配置和数据库迁移的唯一开发输入之一；实现前仍需将字段字典和迁移脚本纳入版本管理。

## 11. 完成定义

满足以下条件后，Feature Contract V2 才可从“开发基线”升级为“冻结版本”：

1. Standard 字段来源和兼容别名已落定。
2. TTM、单季度、负值熔断和质量状态已有自动化测试。
3. PIT 边界和跨表最大可见时间通过回放验证。
4. dv_ttm 已完成历史透传并能在估值 Feature 中使用。
5. V1/V2 并行对账完成，差异有解释。
6. Research Snapshot 可以复现公共 Feature 结果，Strategy 可以显式选择契约版本。
7. 文档、字段字典、迁移脚本和回滚步骤齐全。

届时建议将版本名称正式定为：

~~~text
Feature Contract v2.0
~~~

并按本文“推荐实施顺序”拆解工程任务后开始编码；不得再次把通用底座能力按策略需求拆成延后的数据层补丁。

## 12. 实施状态与运行纪律（2026-08-12）

本方案已进入实现和真实数据验证阶段，落地迁移为
`sql/045_research_data_foundation_v2.sql`。实现遵守以下固定纪律：

1. Raw 不删除、不改写；本轮字段均由已有 Raw payload/版本事实投影。
2. Standard 新字段增量 upsert，按股票批次提交；估值日频投影按月提交。
3. `feature.company_financial_v2` 独立保存新财务粒度；估值公共新增字段扩展
   `feature.valuation_daily_v1`，不为纯新增字段机械创建新表。
4. 每个正式运行、阶段和分区写入 `ops.foundation_rebuild_run/checkpoint`；已完成分区可复用，
   Feature 数据写入与完成 checkpoint 同事务收口；Standard 使用页级短事务 upsert，
   中断后幂等重放当前分区。运行保存 Git/dirty diff 身份，续跑保留代码身份变化历史。
5. 整个正式运行持有 PostgreSQL session advisory lock，避免后台子进程残留时重复启动
   第二个全量任务。
   会话锁获取和释放后必须立即提交隐式事务；锁由数据库会话继续持有，但不得让后台全量任务长期处于
   `idle in transaction`，避免保留旧 MVCC 快照并增加数据库资源压力。
6. 固定先执行自动化测试和真实小样本写入；小样本逐层质量通过后才允许全量派生回填。
7. 正式输出从 `2021-08-02` 开始，`2016-08-01` 至输出日之前只用于 TTM、CAGR 和历史分位
   预热，不伪装成完整研究输出。
8. `feature-contract-v2.0` 数据发布必须同时通过旧基础门禁、V2 全量门禁，并存在输出范围
   覆盖发布截止日的 `VALIDATED` 正式构建运行；历史 Snapshot 不要求人为制造恰好同日结束的运行。
9. 过往季度或年报更正时，不只重算被更正报告期；还要生成受影响的后续单季、TTM、
   ROE、CAGR 和五年窗口新版本，并把实际使用的年度组件纳入来源指纹。
10. income 决定 Feature 的 `comp_type`；cashflow、balance 和经财务报表 PIT 关联后的
    fina_indicator 都必须精确匹配该类型。供应商 ROE 不得跨公司类型混入。
11. 财务 Standard 和 fina_indicator Standard 的冲突更新必须跳过字段完全相同的记录；
    checkpoint 结果哈希覆盖完整 Feature 输出，而不只覆盖来源指纹。
12. 财务可见日期、Snapshot 审计日期都显式使用 `Asia/Shanghai`，不得依赖数据库会话时区。
13. 发布事务固定为：取得事务级发布锁、锁定输入表、运行质量门禁、生成来源 Manifest、
    supersede 旧基线、validate 新基线、提交；输入锁释放与新基线可见必须发生在同一提交点。
14. 进程或容器异常终止后，同一 `run_id` 续跑必须先成功取得全局 session advisory lock；
    持锁后才允许自动接管数据库中遗留的陈旧 `RUNNING`，并把原状态写入 resume history。
15. 所有高吞吐写任务共享同一个仓库级 session advisory lock。Raw→Standard、状态、价格、
    估值和财务 Feature 只能按交易日、自然月或股票分区短事务提交；高容量生产路径禁止
    `TRUNCATE + 全表 INSERT`。通用写入器每 10,000 行独立提交，日快照替换的 DELETE 与
    INSERT 必须在同一事务中。
16. `limit_stocks` 只允许 dry-run；真实小样本写入必须落到独立 smoke 表，禁止以少量股票
    替换生产表整日数据。Raw 写入计数必须返回实际新增版本数，不能把 API 返回行数冒充新增数。
17. 财务增量在 Raw 版本落地时先持久化 `financial_derived_v2` 待办，再标记 API checkpoint
    完成；Feature 成功后才清除待办。这样进程在 Raw 与 Feature 之间失败时，重跑仍会恢复
    派生计算，而不会因 Raw 幂等写入为 0 而永久跳过。
18. 五年利润、现金流和 ROE 统计统一以最新可见利润年度作为窗口锚点；现金流少了最近年度时
    `cashflow_window_complete=false`，不得改用更早的五年窗口伪装完整。非一般工商业保留 OCF
    事实和 TTM，但 OCF/净利润比率必须为 NULL 且状态为 `NOT_APPLICABLE`。
19. 涨停日 `can_buy=false`、跌停日 `can_sell=false`；`is_tradeable` 仅表示存在行情且未停牌，
    不能替代方向性可成交判断。分红当前事实的稳定业务键由股票、报告期和处理阶段组成，
    `record_date/ex_date/实施公告日` 是可更正属性，不得进入会制造重复事件的稳定键。
20. 批量 Snapshot 可复用一次覆盖最大研究日的质量扫描，但必须保存该扫描的真实截止日；
    禁止复制报告后把截止日改写成较早日期。build/validate/freeze 均按已保存真实边界复核。

已通过的真实 smoke 运行是 `e67a5414-f359-427b-9e8f-9df73a7c551e`：9 只跨类型股票、
15,277 行 Standard、149 行财务 V2 Feature、8,025 行估值 Feature。所有强制为零的
映射、未来函数、TTM、OCF、行业适用性和股息分位异常均为 0。该结果只证明链路和写入
正确，不代替全市场正式回填、发布门禁和验证 Snapshot。
