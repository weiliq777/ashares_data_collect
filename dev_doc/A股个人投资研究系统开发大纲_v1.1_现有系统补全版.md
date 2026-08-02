# A股个人投资研究系统开发大纲（基于现有系统补全版）

**Tushare + PostgreSQL + Python + AI（个人长期投资研究方向）**  
**版本：v1.1｜2026-08-02**

> 本文件是工程开发大纲，不构成投资建议，也不承诺系统具有正期望或稳定盈利能力。v1.1 已按当前实际模型进行差异核验，采用增量补全方式，不要求推倒重建现有表。

## 0. 最终确认结论

整体方案可以定稿并进入开发，但不能表述为“已经完全没有问题”。正确结论是：

- 架构方向合理，适合“程序员 + 小资金 + 长周期 + 人工最终决策”的个人投资研究场景。
- 系统定位应固定为“个人投资研究基础设施”，不是自动赚钱机器，也不是与机构高频量化竞争的系统。
- 正式架构应由原来的 `Raw -> Clean -> Feature -> Research -> Strategy`，升级为：

```text
Source / Metadata
      ↓
Raw 原始事实与版本层
      ↓
Standard / Clean 标准化与质量层
      ↓
PIT Point-in-Time 历史时点可用性层
      ↓
Feature 可复现特征层
      ↓
Research / AI 研究解释层
      ↓
Backtest 历史验证层
      ↓
Strategy / Portfolio 策略与组合层
      ↓
Paper Trading / Small Capital 模拟与小资金验证
      ↓
Human Decision 人工最终决策
```

必须补上的关键修正：

1. `ann_date <= trade_date` 只是基础条件；还必须保存 `f_ann_date`、`report_type`、`comp_type`、`update_flag` 和财报修订版本。
2. 仅保留退市股票仍不足以消除幸存者偏差；必须按历史日期重建当日可投资股票池。
3. 不应只保存一份会随锚点变化的前复权历史；应永久保存原始价格和复权因子，并用稳定调整序列计算收益与指标。
4. 长期投资必须纳入分红、送转、退市和公司行动，否则总回报与回测现金流不完整。
5. AI 不进入第一版历史回测决策链。AI 先用于当前时点的结构化研究与反方审查，避免模型自身知道历史结果造成隐性未来信息。
6. “单笔亏损不超过2%”和“固定价格止损”不是普遍真理，只能作为待验证的风险参数；长期基本面策略可能更适合组合上限、论点失效退出和再平衡。
7. 银行、保险、证券与一般工业企业不能强行使用完全相同的财务指标和阈值。


### 0.1 基于当前实际系统的重新核验结论

根据当前已启用模型，现有系统与目标架构**高度同源**，并非从零开始：

- 已经具备 `Tushare -> Raw -> 标准表` 的主链路。
- 股票基础、原始日线、每日估值、复权因子、交易日历、财务指标和三大报表原始JSON均已有承载模型。
- 新闻、政策、监管和公司公告已进入OpenSearch，文本研究基础甚至领先于第一版最低要求。
- 已有历史初始化脚本、断点表和索引，说明采集工程已经进入可运行状态。

因此，后续原则不是“重新设计一套新库”，而是：

```text
保留现有表和代码
    + 修正少数时间/版本主键
    + 补齐历史状态、公司行动与基准数据
    + 新建PIT、Feature、Research、Backtest层
    = 可回放的个人投资研究系统
```

当前最关键的缺口不在“股票行情有没有”，而在以下四个中间层能力：

1. **历史时点股票池**：某一天哪些股票已经上市、是否ST、是否停牌、是否可成交。
2. **财务版本与可用时间**：同一报告期的更正版本不能覆盖旧版本，且必须计算最早可用交易日。
3. **公司行动与总回报**：分红、送转、除权除息必须进入收益与组合现金账本。
4. **可复现的PIT/Feature/Backtest链路**：当前标准表能做当下查询，但还不足以严谨回放历史决策。

### 0.2 当前成熟度判断

| 层级 | 当前状态 | 判断 |
|---|---|---|
| 数据源与采集 | Tushare兼容服务、巨潮、新闻与政策源已接入 | 基础良好，继续补历史覆盖即可 |
| Raw原始层 | 行情、估值、复权、日历、财务指标及三大报表Raw已建 | 主体具备，但股票基础快照和财务版本键需增强 |
| Standard/Clean | `stock_basic_info`、`daily_kline`、`valuation_daily`、`stock_financial_indicator` 已有 | 可继续使用，不建议大规模改名；需补质量状态和部分字段 |
| PIT历史可用层 | 尚未形成独立模型 | **必须新增** |
| Feature特征层 | PE/PB分位、均线、财务派生指标尚未生产化 | **下一阶段核心工作** |
| Research/AI | OpenSearch资料齐全，但缺统一股票研究快照、证据引用和AI报告模型 | 部分具备，需要结构化连接 |
| Backtest/Portfolio | 当前未形成完整订单、成交、现金、持仓模型 | Feature完成后再新增 |

### 0.3 不推倒重建的实施原则

1. 现有 `stock_basic_info`、`daily_kline`、`valuation_daily`、`stock_financial_indicator` 继续作为当前标准表使用。
2. 现有 `tushare_*_raw` 表继续保留；只对存在版本覆盖风险的财务表和股票基础快照增加新版表。
3. 从现在开始新增 `ops`、`pit`、`feature`、`research`、`backtest`、`portfolio` Schema；旧表可以通过兼容视图映射到 `raw/std` 逻辑层，不要求一次性搬表。
4. 迁移采用“新表写入 + 校验 + 切换视图”的方式，避免直接破坏当前采集任务。
5. QMT、Tick、Level-2、分钟线和自动下单仍不进入第一版。


## 1. 项目目标与边界

### 1.1 目标

建立一个能够长期维护、可追溯、可回放、可验证的个人A股研究系统，实现：

- 自动获取并保存全市场历史事实数据。
- 在任意历史日期还原“当时真正可见的信息”。
- 生成少量、清晰、版本化的价格、估值、财务和风险特征。
- 自动形成股票研究快照和观察池。
- 让AI基于结构化事实做解释、找风险、列反方证据，不直接预测次日涨跌。
- 用统一回测框架验证简单策略，纳入真实交易约束和成本。
- 先模拟运行，再用约1万元做小规模、可承受的真实反馈实验。

### 1.2 非目标

第一版明确不做：

- 高频、分钟级或盘口套利。
- 预测次日涨停、短期价格点位。
- 端到端机器学习自动下单。
- 100个以上技术指标或大规模因子挖掘。
- 自动照搬AI的买卖建议。
- 在没有回测和模拟验证前宣称“正期望”。

### 1.3 核心原则

1. **事实与观点分离**：原始表只保存来源事实，研究结论单独存放。
2. **事件时间与可用时间分离**：报告期、公告日、实际披露日、系统获取时间分别保存。
3. **历史可回放**：任何一天的结果都能用当时数据重算。
4. **版本可追溯**：数据、特征、策略、代码和AI提示词都有版本号。
5. **保守处理不确定性**：缺失不等于零，异常不直接删除，无法确定时标记不可用。
6. **第一版简单**：少量指标、少量策略、统一验证框架。
7. **人工最终决策**：系统帮助减少错误，不替代责任主体。

## 2. 推荐数据库分层

建议使用 PostgreSQL Schema 隔离，而不是依靠表名后缀堆在同一个命名空间：

| Schema | 作用 | 是否允许覆盖历史 |
|---|---|---|
| `ops` | 任务、批次、检查点、质量结果、版本元数据 | 可更新运行状态，不删除审计历史 |
| `raw` | Tushare原始响应与版本记录 | 原则上追加；行情可幂等更新，财务必须保留版本 |
| `std` | 类型、单位、主键、状态标准化后的数据 | 可重建，规则版本化 |
| `pit` | 历史日期当时可见的数据和股票池 | 可重建，必须无未来数据 |
| `feature` | 可复现的价格、估值、财务特征 | 可重建，必须记录特征版本 |
| `research` | 股票快照、观察池、人工笔记、AI报告 | 保存生成时间和输入快照 |
| `backtest` | 策略定义、运行、订单、成交、持仓、绩效 | 不覆盖已完成实验 |
| `portfolio` | 模拟与真实组合、交易计划、复盘 | 严格区分模拟和真实 |


### 2.1 基于现有物理表的渐进式落地

当前物理表多数位于原有命名空间。为了避免“大迁移”带来的停机和代码改造，推荐先增加逻辑Schema和兼容视图：

| 现有物理表 | 目标逻辑层 | 第一阶段处理 |
|---|---|---|
| `tushare_daily_raw` | `raw.stock_daily` | 保留原表，建立只读兼容视图 |
| `tushare_daily_basic_raw` | `raw.daily_basic` | 保留原表，建立兼容视图 |
| `tushare_adj_factor_raw` | `raw.adj_factor` | 保留原表，完成历史初始化 |
| `tushare_trade_cal_raw` | `raw.trade_calendar` | 保留原表 |
| `daily_kline` | `std.stock_daily` | 保留原表，补验证字段或建立扩展表 |
| `valuation_daily` | `std.valuation_daily` | 保留原表，通过视图统一单位和字段名 |
| `stock_financial_indicator` | `std.financial_indicator_latest` | 暂作“当前最新摘要”，另建版本表承载历史vintage |
| `news-YYYY` | `research.document_event`的文本来源 | OpenSearch继续保存正文，PostgreSQL只保存证据引用和报告元数据 |

建议把“Schema分层”视为逻辑边界，而不是必须立即执行的数据库搬家。等新链路稳定后，再决定是否将旧物理表真正迁入对应Schema。


## 3. Source / Metadata 与任务控制层

### 3.1 这一层做什么

这一层不保存股票业务指标，负责保证抓取任务能够限流、重试、断点续传、审计和恢复。

### 3.2 核心表

#### `ops.job_run`

建议字段：

- `job_run_id`
- `job_name`
- `job_version`
- `batch_id`
- `started_at`
- `finished_at`
- `status`：RUNNING / SUCCESS / PARTIAL / FAILED
- `source_max_date`
- `rows_fetched`
- `rows_inserted`
- `rows_updated`
- `error_count`
- `error_summary`
- `git_commit`
- `config_hash`

#### `ops.etl_checkpoint`

- `job_name`
- `partition_key`：日期、股票代码或报告期
- `last_success_value`
- `updated_at`
- `retry_count`

#### `ops.api_call_log`

- 接口名、请求参数哈希、调用时间、耗时、返回行数、状态码、错误类别、批次号。
- Token 不进入日志。

#### `ops.data_quality_result`

- 检查名称、表、分区、严重级别、实际值、阈值、状态、样本记录、运行批次。

### 3.3 抓取器实现建议

建立统一客户端：

```text
TushareClient
├── rate_limiter
├── retry_with_backoff
├── request_logger
├── dataframe_schema_check
├── checkpoint_manager
└── endpoint adapters
```

要求：

- 指数退避重试，仅对超时、限流和临时服务错误重试。
- 参数错误、权限不足、字段变化不进行无限重试。
- 每次调用生成 `request_hash`，便于去重和审计。
- 大任务按日期、代码或报告期切分；单分区失败不阻塞全部任务。
- 历史初始化和每日增量使用同一套写入函数，避免两套逻辑长期漂移。

## 4. Raw 原始事实与版本层

### 4.1 Raw层职责

- 尽可能原样保存Tushare返回字段。
- 不根据投资观点删除ST、退市、亏损或异常公司。
- 不把空值自动填0。
- 不把更正后的财务数据覆盖掉原始版本。
- 附加采集元数据：`batch_id`、`retrieved_at`、`request_hash`、`source`、`payload_hash`。

### 4.2 必须接口与表映射（MVP）

| Tushare接口 | Raw表 | 粒度 | 拉取建议 | 主要用途 |
|---|---|---|---|---|
| `stock_basic` | `raw.stock_basic_snapshot` | 快照日 × 股票 | 分别拉取 L/D/P/G | 上市、退市、市场、行业基础信息 |
| `trade_cal` | `raw.trade_calendar` | 交易所 × 日历日 | 按年份 | 交易日轴、下一个交易日 |
| `daily` | `raw.stock_daily` | 股票 × 交易日 | 按交易日全市场 | 原始OHLCV和成交额 |
| `adj_factor` | `raw.adj_factor` | 股票 × 交易日 | 按交易日全市场 | 公司行动调整 |
| `daily_basic` | `raw.daily_basic` | 股票 × 交易日 | 按交易日全市场 | PE/PB/PS、换手率、市值、股本 |
| `suspend_d` | `raw.suspend_daily` | 股票 × 日期 × 类型 | 按日期 | 停复牌与可交易性 |
| `stock_st` / `namechange` | `raw.security_status_event` | 股票 × 日期/区间 | 增量 | 历史ST和名称状态 |
| `income` | `raw.income_vintage` | 股票 × 报告期 × 报表版本 | 按股票或VIP按报告期 | 收入、利润、费用 |
| `balancesheet` | `raw.balance_vintage` | 股票 × 报告期 × 报表版本 | 同上 | 资产、负债、权益 |
| `cashflow` | `raw.cashflow_vintage` | 股票 × 报告期 × 报表版本 | 同上 | 经营、投资、融资现金流 |
| `fina_indicator` | `raw.fina_indicator_vintage` | 股票 × 报告期 × 公告版本 | 同上 | 标准财务指标交叉验证 |
| `dividend` | `raw.dividend_event` | 股票 × 分红方案/实施事件 | 按公告日增量 | 分红、送转、总回报、现金变动 |
| 指数日线 | `raw.index_daily` | 指数 × 交易日 | 按日 | 基准收益与市场状态 |

### 4.3 建议补充接口（P1）

- `disclosure_date`：披露计划和实际披露日期。
- `forecast`、业绩快报：研究层提前风险预警。
- `fina_audit`：审计意见。
- `repurchase`：回购行为。
- `share_float`：限售解禁。
- `anns_d`：公告正文入口，需单独权限时后置。

### 4.4 Raw层主键策略

行情类可使用业务主键幂等写入：

```text
(ts_code, trade_date)
```

但财务报表不能只用 `(ts_code, end_date)`，建议保留：

```text
(ts_code, end_date, report_type, comp_type,
 coalesce(f_ann_date, ann_date), update_flag, payload_hash)
```

如果同一业务键数据内容改变，新增版本而不是覆盖旧版本。

### 4.5 Raw层示例DDL（简化）

```sql
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.stock_daily (
    ts_code        varchar(16) NOT NULL,
    trade_date     date        NOT NULL,
    open            numeric(18,6),
    high            numeric(18,6),
    low             numeric(18,6),
    close           numeric(18,6),
    pre_close       numeric(18,6),
    change          numeric(18,6),
    pct_chg         numeric(18,6),
    vol_hand        numeric(24,4),
    amount_k_cny    numeric(24,4),
    batch_id        uuid        NOT NULL,
    retrieved_at    timestamptz NOT NULL,
    payload_hash    char(64)    NOT NULL,
    PRIMARY KEY (ts_code, trade_date)
);
```

说明：字段单位应体现在字段名或数据字典中，避免把“手、股、千元、元”混用。

## 5. Standard / Clean 标准化与数据质量层

### 5.1 Clean层不是“删除不好数据”

Clean层要完成：

- 日期、数字、单位和枚举标准化。
- 数据完整性和一致性检查。
- 建立证券生命周期、历史状态与可交易状态。
- 财务版本规范化和单季度/累计口径拆分。
- 把不可靠记录送入隔离区并标记原因。

推荐保留：

- `quality_status`：VALID / WARNING / INVALID / UNKNOWN
- `quality_reason_codes`
- `source_batch_id`
- `standard_rule_version`

### 5.2 证券生命周期与历史股票池

建立 `std.security_lifecycle`：

- `ts_code`
- `list_date`
- `delist_date`
- `exchange`
- `market`
- `company_type`
- `effective_from`
- `effective_to`

建立 `std.security_status_daily`：

- `trade_date`
- `ts_code`
- `is_listed`
- `is_st`
- `is_suspended`
- `is_tradeable`
- `limit_status`
- `status_reason`

历史当日股票池的基本规则：

```text
list_date <= trade_date
AND (delist_date IS NULL OR trade_date <= delist_date)
```

再叠加当日停牌、ST和策略允许的市场板块。保留退市股票只是第一步，真正防止幸存者偏差的是“每个历史日期使用当时存在的股票集合”。

### 5.3 行情标准化

建立 `std.stock_daily`，完成：

- 统一日期类型和单位。
- 校验 `high >= max(open, close, low)`、`low <= min(...)`。
- 校验涨跌额与前收盘价的基本关系，但允许四舍五入差异。
- 检查交易日是否在交易日历中。
- 停牌日没有行情不应自动判为数据缺失。
- 新股、退市整理期、除权日和价格限制规则单独标记。

不要跨停牌日无条件前向填充价格和成交量。

### 5.4 复权与稳定价格序列

永久保存：

- 原始价格 `raw_close`
- 当日复权因子 `adj_factor`

推荐计算稳定的调整基准：

```text
adj_close_base_t = raw_close_t × adj_factor_t
adjusted_return_t = adj_close_base_t / adj_close_base_(t-1) - 1
```

用于图表展示的前复权价格可以按查询锚点动态派生：

```text
qfq_close(t, anchor) = adj_close_base_t / adj_factor_anchor
```

这样做的原因：前复权通常依赖所选结束日或最新日作为锚点，未来发生分红送转后，过去整段前复权数值可能整体缩放。整体缩放不一定改变均线交叉，但会影响缓存、审计和历史结果复现，因此不应把“当前锚点的一份qfq历史”当作永久事实。

真实成交、资金占用、止损价格、手续费和手数计算一律使用原始交易价格。

### 5.5 财务标准化与版本管理

原始财报常包含：

- `ann_date`
- `f_ann_date`
- `end_date`
- `report_type`
- `comp_type`
- `update_flag`

Clean层必须：

1. 保留所有历史版本。
2. 区分合并报表、单季报表、调整前后报表和母公司报表。
3. 默认研究一般公司时优先采用合并报表，但规则必须显式配置。
4. 不把“今天看到的最新修订值”倒灌到修订前的历史日期。
5. 累计利润表和现金流量表要转换为单季度数据时，按同一财年内相邻累计值相减；跨年不能直接相减。
6. 银行、保险、证券使用 `comp_type` 分流到专用特征，不用一般工商业的负债率、毛利率标准硬套。

### 5.6 数据隔离区

建立 `std.quarantine_record`：

- 来源表、业务主键、错误码、原始批次、发现时间、处理状态。
- INVALID数据不进入特征，但不能从Raw删除。
- WARNING数据可进入特征，但下游必须携带质量标记。

## 6. PIT：Point-in-Time 历史时点可用性层

### 6.1 为什么必须单独建PIT层

财报的 `end_date` 表示数据属于哪个报告期，不表示投资者何时知道。历史回测必须使用“决策时点已经公开且系统允许使用的数据”。

### 6.2 可用时间字段

建立统一字段：

- `event_date`：业务发生或报告期结束时间。
- `ann_date`：公告日期。
- `f_ann_date`：实际公告日期。
- `available_at`：系统定义的信息可用时间。
- `available_trade_date`：最早允许用于日频决策的交易日。
- `retrieved_at`：本系统获取时间。

保守的日频默认规则：

```text
available_date = coalesce(f_ann_date, ann_date)
available_trade_date = available_date之后的第一个可执行交易日
```

如果未来获得公告精确发布时间 `rec_time`，可区分盘前、盘中和盘后。第一版宁可晚一个交易日使用，也不要提前使用。

### 6.3 财务As-Of查询

示例逻辑：

```sql
SELECT DISTINCT ON (ts_code, end_date)
       *
FROM std.financial_vintage
WHERE available_trade_date <= :decision_date
  AND report_scope = 'CONSOLIDATED'
ORDER BY ts_code, end_date,
         available_trade_date DESC,
         version_no DESC;
```

注意：查询的是“决策日当时最新可用版本”，而不是数据库当前最新版本。

### 6.4 PIT股票池

`pit.universe_daily` 建议包含：

- `trade_date`
- `ts_code`
- `is_listed_asof`
- `is_st_asof`
- `is_suspended_asof`
- `has_price_history`
- `days_since_listing`
- `eligible_by_market`
- `exclusion_reason`

策略回测不能直接读取今天的 `stock_basic` 列表。

### 6.5 防未来数据自动测试

建立“截断一致性测试”：

1. 用完整数据计算截至日期T的结果。
2. 物理删除或隐藏T之后的数据，再计算一次。
3. T及之前的股票池、财务快照和特征必须完全一致。

这是整个系统最重要的自动化测试之一。

## 7. Feature 可复现特征层

### 7.1 特征层职责

- 只计算可复现的数值或状态，不产生“建议买入”之类观点。
- 所有滚动窗口只使用当日及之前数据。
- 每条特征记录保存 `feature_set_version`、`calculated_at`、`source_asof_date`、`code_version`。
- 不同粒度分表，不把季度财务每天重复复制到永久大表。

### 7.2 第一版价格与交易特征

表：`feature.price_daily_v1`

建议字段：

- `ts_code`, `trade_date`
- `raw_close`
- `adj_close_base`
- `return_1d`, `return_20d`, `return_60d`
- `ma20`, `ma60`, `ma120`
- `close_to_ma20`, `ma20_to_ma60`
- `volatility_20d`
- `atr14`
- `max_drawdown_60d`, `max_drawdown_250d`
- `amount_ma20`
- `turnover_ma20`
- `volume_ratio_20d`
- `available_observation_count`
- `quality_status`

实现要求：

- `min_periods` 不满足时返回NULL，不用未来值或全样本均值填充。
- 停牌期间不制造虚假零收益序列；可根据策略口径决定是否保持上一估值，但需另有可交易标记。
- 拆股送转附近，成交量指标优先结合成交额和换手率，避免单看原始股数误判。

### 7.3 第一版估值特征

表：`feature.valuation_daily_v1`

建议字段：

- `pe_ttm`, `pb`, `ps_ttm`
- `dv_ttm`
- `total_mv`, `circ_mv`
- `pe_pct_3y`, `pe_pct_5y`
- `pb_pct_3y`, `pb_pct_5y`
- `industry_pe_pct`（P1）
- `is_pe_meaningful`
- `valuation_observation_count`

估值分位规则：

- 用滚动历史窗口，不使用全历史一次性排名。
- PE小于等于0时，不与正常正PE样本直接混排；标记“盈利为负或PE无意义”。
- 至少满足设定观测数，例如3年窗口不少于500个有效交易日，才输出稳定分位。
- 新上市公司没有5年历史应返回“数据不足”，不能直接判为昂贵或便宜。

### 7.4 第一版财务与质量特征

表：`feature.company_financial_v1`

粒度：股票 × 财务版本/报告期，不是每日。

一般工商业建议字段：

- 营业收入、归母净利润、扣非净利润、经营现金流。
- 收入同比、净利润同比、扣非利润同比、经营现金流同比。
- ROE、毛利率、净利率、资产负债率。
- 经营现金流 / 净利润。
- 应收账款增长、存货增长与收入增长的差异。
- 近3年收入和利润复合增速（数据足够时）。
- 连续亏损期数、负经营现金流期数。
- 财报修订次数、非标准审计意见标记（P1）。

银行/保险/证券：

- 第一版可先分组，不进入统一质量总分。
- 后续单独定义净息差、不良率、拨备、偿付能力等行业专用特征；数据源不充分时不要伪造通用指标。

### 7.5 分红与总回报特征

表：`feature.shareholder_return_v1`

- 近12个月现金分红。
- 近3年分红稳定性。
- 股息率历史分位。
- 分红支付率（口径明确）。
- 回购和增发稀释标记（P1）。

长期投资不能只看价格涨幅，回测组合现金也必须正确接收分红。

### 7.6 特征注册表

建立 `ops.feature_registry`：

- `feature_name`
- `feature_version`
- `description`
- `formula`
- `source_tables`
- `lookback_window`
- `minimum_observations`
- `null_policy`
- `applicable_company_type`
- `owner`
- `created_at`

## 8. Research / AI 研究层

### 8.1 研究层输出

建立 `research.stock_snapshot_daily`，按某个研究日期组合：

- 当日原始价格和可交易状态。
- PIT可用的最新财务数据。
- 价格、估值、质量特征。
- 数据完整性与风险警告。
- 行业和生命周期信息。

这是给前端、筛选器和AI的统一只读入口，不是原始事实表。

### 8.2 AI的正确角色

AI第一版只做：

- 总结财务变化。
- 解释指标之间的联系。
- 强制寻找反方证据。
- 指出缺失信息和矛盾。
- 生成进一步核查清单。
- 将公告或财报文本与结构化数据对照。

AI第一版不做：

- 预测明天涨跌。
- 单独决定买入或卖出。
- 在缺少数据时补写事实。
- 把行业通用叙事当作公司事实。
- 参与历史回测信号生成。

### 8.3 AI输入JSON示例

```json
{
  "as_of_date": "2026-08-01",
  "ts_code": "000001.SZ",
  "data_quality": {
    "status": "VALID",
    "warnings": []
  },
  "market": {
    "raw_close": 12.34,
    "return_60d": 0.08,
    "max_drawdown_250d": -0.19
  },
  "valuation": {
    "pe_ttm": 7.8,
    "pe_pct_5y": 0.22,
    "is_pe_meaningful": true
  },
  "financial_asof": {
    "available_trade_date": "2026-04-28",
    "end_date": "2025-12-31",
    "roe": 0.112,
    "operating_cashflow": 123456789
  }
}
```

### 8.4 AI输出结构

```json
{
  "summary": "",
  "strengths": [],
  "risks": [],
  "bear_case": [],
  "data_gaps": [],
  "contradictions": [],
  "questions_for_human": [],
  "evidence_refs": []
}
```

要求：

- 每个结论必须引用输入字段、公告片段或来源记录ID。
- 没有证据时输出“无法判断”。
- 保存模型名、模型版本、提示词版本、输入快照哈希、生成时间。

### 8.5 历史AI回测禁区

通用大模型可能已经知道过去公司的后来结果。即使只提供历史数据，模型参数中的后见知识仍可能泄漏到历史分析。因此MVP阶段：

- AI报告仅用于当前时点研究，不计入历史回测信号。
- 回测信号必须由确定性代码和PIT数据生成。
- 未来若研究AI历史回测，需要独立设计严格的时间隔离实验，不能沿用普通聊天模型直接回看历史。

## 9. Backtest 历史验证层

### 9.1 回测层必须独立

研究层回答“发生了什么、风险在哪里”；回测层回答“某组规则在过去经过现实约束后表现如何”。不能根据单只历史成功案例直接推断策略有效。

### 9.2 核心对象

- `backtest.strategy_definition`
- `backtest.run`
- `backtest.signal`
- `backtest.order`
- `backtest.fill`
- `backtest.position_daily`
- `backtest.cash_ledger`
- `backtest.performance_daily`
- `backtest.metric`

### 9.3 信号与成交分离

必须保存：

- `signal_date`：产生信号的日期。
- `decision_time`：决策时点。
- `intended_execution_date`：计划交易日。
- `actual_fill_date`：实际成交日。
- `signal_price`、`order_price`、`fill_price`。

收盘后产生的信号，默认只能在后续交易时点成交，不能用同日收盘价假装已成交。

### 9.4 A股现实约束

回测参数应按市场板块和生效日期配置，而不是写死在代码中：

- 最小交易单位和零股处理。
- 买入后可卖时间限制。
- 不同板块、ST和特殊日期的涨跌幅限制。
- 停牌、涨停买不到、跌停卖不出。
- 券商佣金、最低佣金、经手费、过户费、印花税。
- 滑点、成交量约束。
- 分红、送转、配股、退市现金处理。

建立 `backtest.market_rule_effective` 保存规则生效区间，防止未来规则套用到过去。

### 9.5 验证方法

至少包含：

1. 训练区间 / 验证区间 / 样本外区间。
2. 滚动或扩展窗口的Walk-Forward验证。
3. 参数稳定性测试，不只挑最好的一组参数。
4. 不同行情阶段和行业分组表现。
5. 加倍交易成本和滑点的压力测试。
6. 去掉表现最好的少数股票后的结果。
7. 随机延迟一天成交的鲁棒性测试。
8. 与简单基准比较，而不是只看绝对收益。

### 9.6 核心指标

- 年化收益、总收益。
- 最大回撤、回撤持续时间。
- 波动率、Sharpe、Sortino。
- 最差年度/季度/月度。
- 换手率、交易次数、持仓集中度。
- 胜率、盈亏比，但不把胜率当唯一目标。
- 相对基准收益和跟踪误差。
- 税费前后差异。

### 9.7 策略否决条件

出现以下任一情况，不进入小资金阶段：

- 轻微调整参数，结果大幅反转。
- 样本外显著失效。
- 主要收益来自少数极端股票。
- 加入现实费用后优势消失。
- 无法解释数据可用时间。
- 删除退市/ST后才表现良好。
- 交易信号在涨停/停牌日实际上无法成交。

## 10. Strategy / Portfolio 策略与组合层

### 10.1 策略配置化

策略规则不要散落在Python `if` 中，建议使用YAML或数据库定义：

```yaml
strategy_id: quality_value_v1
universe:
  min_days_since_listing: 500
  allow_st: false
features:
  - roe_ttm
  - ocf_to_net_profit
  - pe_pct_5y
ranking:
  quality_weight: 0.6
  valuation_weight: 0.4
rebalance: monthly
execution: next_open
```

代码负责解释配置，配置进入版本控制。

### 10.2 第一版策略范围

第一版最多验证1到2条简单逻辑，例如：

- 质量 + 合理估值。
- 质量 + 成长 + 基础趋势过滤。

不同时引入几十个指标，不为了提高历史收益不断增加例外条款。

### 10.3 组合风险而非迷信固定止损

需要做风险管理，但形式必须与策略匹配：

- 单股最大资金占比。
- 行业最大暴露。
- 组合最大持仓数量。
- 预留现金比例。
- 单次再平衡最大换手。
- 论点失效退出：财务恶化、审计风险、数据造假线索、长期假设破坏。
- 价格止损可以测试，但不能预先宣称一定正确。

对于约1万元本金，还要考虑100股交易单位和最低佣金可能使“精确2%风险倒推股数”不可执行。系统应输出“无法满足风险预算”，而不是强行成交。

## 11. Paper Trading 与小资金阶段

### 11.1 顺序

1. 历史回测。
2. 只读每日运行。
3. 模拟组合记录计划成交与真实可成交性。
4. 至少经历若干财报发布、分红、停牌或市场波动场景。
5. 小资金真实验证。

### 11.2 小资金阶段目标

第一年的主要目标不是收益最大化，而是验证：

- 数据是否按时更新。
- 系统信号是否可复现。
- 实际成本与回测是否接近。
- 自己能否遵守流程。
- 研究结论出错时是否能复盘。

真实组合和模拟组合必须使用不同的账户/表标识，禁止混写。

## 12. 数据质量规则清单

### 12.1 完整性

- 开市日全市场行情数量与历史区间比较。
- 日线、复权因子、每日指标的股票集合差异。
- 每只股票上市后应有的交易日与实际记录数差异。
- 财务四个报告期是否连续，允许IPO和特殊公司例外。

### 12.2 唯一性

- 行情 `(ts_code, trade_date)` 唯一。
- 财务版本业务键 + 内容哈希唯一。
- 同一批次不得重复写入相同原始响应。

### 12.3 时间一致性

- `list_date <= delist_date`。
- `end_date <= ann_date/f_ann_date`，异常进入WARNING或INVALID。
- `available_trade_date` 不早于公告可用时间。
- 特征源数据最大日期不超过特征日期。

### 12.4 数值一致性

- OHLC关系合理。
- 股本、市值、价格之间允许误差的交叉验证。
- 资产 = 负债 + 权益的近似一致性。
- 现金流量表现金净增加与期初期末现金的近似一致性。
- 财务指标与原始报表计算值差异监控。

### 12.5 分布漂移

- 每日空值率、0值率、极端值率。
- 接口字段突然全为空或数量骤降。
- 复权因子异常跳变必须能对应公司行动。

### 12.6 严重级别

- `ERROR`：停止该分区下游计算。
- `WARNING`：允许计算但携带标记。
- `INFO`：记录预期变化，如IPO或退市。

## 13. ETL与增量计算流程

### 13.1 历史初始化顺序

```text
1. stock_basic（L/D/P/G）
2. trade_cal
3. daily
4. adj_factor
5. daily_basic
6. suspend / ST / namechange
7. income / balancesheet / cashflow / fina_indicator
8. dividend
9. index benchmark
10. Raw验收
11. Standard/Clean重建
12. PIT重建
13. Feature回填
```

### 13.2 每日增量建议

收盘后：

1. 确认当日是交易日。
2. 拉取当日 `daily`、`daily_basic`。
3. 拉取/校验 `adj_factor`。
4. 拉取停复牌和风险状态。
5. 拉取公告日范围内新增或更新财务记录。
6. 拉取分红和公司行动增量。
7. 执行Raw质量检查。
8. 更新Standard与PIT。
9. 增量计算当日特征。
10. 生成研究快照和AI候选列表。
11. 任务完成后发布日报；任何ERROR不生成投资候选。

### 13.3 重算策略

- 行情修订：重算受影响股票从最早修订日到当前的滚动特征。
- 复权因子变化：重算对应股票全部调整收益或受影响区间。
- 财务修订：新增vintage，并从 `available_trade_date` 起重算PIT财务特征，不能修改修订前历史结果。
- 特征公式升级：写入新版本表或新版本字段，旧结果保留。

## 14. 项目代码目录建议

```text
finance_cn/
├── pyproject.toml
├── README.md
├── .env.example
├── config/
│   ├── base.yaml
│   ├── endpoints.yaml
│   ├── features_v1.yaml
│   └── backtest_rules.yaml
├── sql/
│   ├── migrations/
│   ├── raw/
│   ├── std/
│   ├── pit/
│   ├── feature/
│   └── research/
├── src/finance_cn/
│   ├── cli.py
│   ├── clients/
│   │   └── tushare_client.py
│   ├── ops/
│   │   ├── checkpoint.py
│   │   ├── rate_limit.py
│   │   └── job_run.py
│   ├── ingestion/
│   │   ├── stock_basic.py
│   │   ├── market_daily.py
│   │   └── financials.py
│   ├── standardization/
│   ├── pit/
│   ├── features/
│   ├── research/
│   ├── ai/
│   ├── backtest/
│   └── portfolio/
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── golden/
│   └── leakage/
├── scripts/
│   ├── init_history.py
│   ├── update_daily.py
│   └── rebuild_features.py
└── docs/
    ├── data_dictionary.md
    ├── feature_registry.md
    └── runbook.md
```

### 14.1 CLI建议

第一版不必上Airflow。可以用Typer/Click提供可重复命令，再由Cron或系统任务调度：

```text
finance init raw --start 2016-01-01 --end 2026-08-01
finance update daily --date 2026-08-03
finance build std --from 2026-08-03
finance build pit --as-of 2026-08-03
finance build features --date 2026-08-03 --version v1
finance research snapshot --date 2026-08-03
finance backtest run --strategy quality_value_v1
```

## 15. 测试计划

### 15.1 单元测试

- 复权收益计算。
- 前复权锚点变化只造成尺度变化，不改变预期收益关系。
- 单季度财务由累计值相减。
- 滚动分位只使用历史窗口。
- 下一个交易日映射。
- 财务As-Of版本选择。
- 负PE与缺失值策略。

### 15.2 集成测试样本

至少选择这些类型的股票作为固定黄金样本：

- 长期正常上市的一般公司。
- 银行或保险公司。
- 新上市不足一年公司。
- 曾ST后摘帽公司。
- 已退市公司。
- 长期停牌公司。
- 发生高送转/拆股/大额分红公司。
- 财报发生更正公司。
- 连续亏损公司。

### 15.3 关键防泄漏测试

- 截断一致性测试。
- 特征表中任意行的源数据最大可用日不得晚于特征日。
- 回测订单成交时间不得早于信号时间。
- 历史股票池不得含尚未上市股票。
- 当前修订财报不得覆盖旧vintage。

### 15.4 幂等性测试

同一初始化任务重复执行两次：

- Raw行情行数不翻倍。
- 财务相同版本不重复。
- 特征结果哈希一致。
- 任务状态可正确恢复。

## 16. 监控与运行手册

每日监控至少包括：

- 各接口成功率和耗时。
- 当日行情/估值/复权行数。
- 数据更新时间是否超过阈值。
- DQ ERROR/WARNING数量。
- 特征生成行数与昨日差异。
- 最新可用财报数量。
- 研究快照是否完整。

告警原则：

- 数据不完整时宁可不产生候选，也不要带病运行。
- 自动重试失败后保留分区状态，允许人工从检查点恢复。
- Token、数据库密码和券商信息不进入代码仓库和日志。

## 17. 基于现有系统的8到12周增量实施路线

### 阶段A：现状审计与兼容层（第1周）

交付：

- 冻结当前表结构快照、索引和任务清单。
- 为已有表补充数据字典：单位、时间语义、主键语义、是否会覆盖历史。
- 新建 `ops` Schema及运行、调用、质量审计模型。
- 建立 `raw/std` 兼容视图，不移动现有大表。

验收：

- 当前历史初始化脚本仍可运行。
- 每次任务有 `job_run_id`，能追踪请求、写入和失败分区。
- 同一分区重复执行不会产生重复数据。

### 阶段B：补齐现有Raw历史覆盖（第2至3周）

优先完成当前已建表的初始化，而不是同时建设策略：

1. `tushare_adj_factor_raw` / `adj_factor_daily`。
2. `tushare_daily_raw` / `daily_kline` 的目标历史窗口。
3. `tushare_daily_basic_raw` / `valuation_daily`。
4. 全市场 `fina_indicator`、`income`、`balancesheet`、`cashflow`。
5. `trade_cal` 全历史审计。

同时新增并补数：

- 历史股票基础快照。
- `stock_st` / `namechange`。
- `suspend_d`。
- `stk_limit`。
- `dividend`。
- 至少一个宽基指数日线。

验收：

- 覆盖率、空值率、重复率和日期范围报告通过。
- 退市、ST、停牌、分红和财报更正黄金样本可查询。

### 阶段C：财务版本、标准报表与PIT（第4至5周）

交付：

- 财务Raw Vintage表，不再用 `(ts_code, ann_date, end_date)` 覆盖所有版本。
- 三大报表策略所需字段的标准化表。
- `available_trade_date` 计算。
- `pit.universe_daily` 和财务As-Of视图。

验收：

- 截断一致性测试通过。
- 同一报告期更正前后的版本均能回放。
- 决策日期不能读取当时尚未披露的数据。

### 阶段D：Feature v1（第6至7周）

交付：

- `feature.price_daily_v1`。
- `feature.valuation_daily_v1`。
- `feature.company_financial_v1`。
- `feature.shareholder_return_v1`。
- 特征注册表、版本和重算命令。

验收：

- 随机样本与独立Pandas计算一致。
- 所有滚动指标只读取当日及以前的有效记录。
- PE无意义、历史不足、停牌等情况返回明确状态而不是伪造数值。

### 阶段E：Research / AI（第8周）

交付：

- `research.stock_snapshot_daily`。
- OpenSearch文档证据引用表。
- AI结构化输入、输出、提示词版本和输入哈希。

验收：

- AI每个结论能追溯至结构化字段或公告文档ID。
- 数据不足时输出“无法判断”。
- AI不直接进入历史信号回测。

### 阶段F：Backtest v1（第9至10周）

交付：

- 策略定义、信号、订单、成交、现金、持仓、绩效模型。
- A股交易规则、费用、停牌、涨跌停、分红和退市处理。
- 1至2个简单策略实验。

验收：

- 收盘后信号不能同日收盘成交。
- 不可成交和公司行动样本处理正确。
- 样本外、参数稳定性和成本压力测试完整。

### 阶段G：模拟运行与小资金准备（第11至12周）

交付：

- 每日模拟组合、交易计划和复盘记录。
- 真实与模拟账户严格分离。
- 约1万元试验的暂停条件、单股/行业上限和人工确认流程。

验收：

- 连续运行无重大数据错误。
- 每次决策均可追溯至PIT快照、特征版本、策略版本和人工确认。
## 18. 最小可用版本（MVP）验收清单

只有全部满足，才进入策略验证：

- [ ] 股票列表包含上市、退市、暂停和未交易状态。
- [ ] 交易日历完整。
- [ ] 原始行情、复权因子、每日估值已入库。
- [ ] 财务报表保留公告日、实际公告日、报告类型、公司类型和更新标记。
- [ ] 分红和基本公司行动已入库。
- [ ] Raw任务可限流、重试、断点续传和幂等执行。
- [ ] 历史日期可还原当时股票池。
- [ ] PIT财务查询无未来数据。
- [ ] 稳定调整收益序列和原始执行价格分离。
- [ ] Feature v1有版本、最少观测数和空值规则。
- [ ] AI报告不直接给出自动交易指令。
- [ ] 回测信号、订单和成交时间分离。
- [ ] 回测纳入费用、不可成交、分红、退市与市场规则。
- [ ] 截断一致性、幂等性和黄金样本测试通过。

## 19. 立即开始的开发任务（按当前项目迁移号续接）

当前已经存在 `014_create_tushare_market_raw.sql` 和 `015_tushare_indexes.sql`，后续不应从001重新开始。建议按以下顺序继续：

### P0-1：`016_create_ops_audit.sql`

新增：

- `ops.job_run`
- `ops.etl_checkpoint`（可把现有 `tushare_history_checkpoint` 迁移或建立兼容视图）
- `ops.api_call_log`
- `ops.data_quality_result`
- `ops.quarantine_record`

同时统一 `batch_id`、`request_hash`、`payload_hash`、`retrieved_at` 字段语义。

### P0-2：`017_create_security_history.sql`

新增：

- `tushare_stock_basic_snapshot_raw`
- `security_lifecycle`
- `security_name_event`
- `security_status_daily`

现有 `tushare_stock_basic_raw` 和 `stock_basic_info` 保留，分别继续作为“最新原始缓存”和“当前标准摘要”。

### P0-3：`018_create_tradeability_models.sql`

新增：

- `tushare_stock_st_raw`
- `tushare_namechange_raw`
- `tushare_suspend_d_raw`
- `tushare_stk_limit_raw`
- 对应标准事件/每日状态表

目标是能够确定历史某日：是否ST、是否停牌、涨跌停价以及是否具备可执行性。

### P0-4：`019_upgrade_financial_vintage.sql`

不要直接破坏当前财务Raw主键。新增版本表：

- `tushare_income_vintage_raw`
- `tushare_balancesheet_vintage_raw`
- `tushare_cashflow_vintage_raw`
- `tushare_fina_indicator_vintage_raw`

业务键至少包含：

```text
ts_code + end_date + report_type + comp_type
+ coalesce(f_ann_date, ann_date) + update_flag + payload_hash
```

现有三大报表Raw表先保留，完成回填和一致性核对后再切换采集器。

### P0-5：`020_create_standard_financials.sql`

新增三张“最小字段标准表”，第一版只拆策略与风险核验必需字段：

- `std_income_statement`
- `std_balance_sheet`
- `std_cashflow_statement`

并新增 `stock_financial_indicator_vintage`；现有 `stock_financial_indicator` 可继续作为“最新可用摘要表/视图”。

### P0-6：`021_create_corporate_action_benchmark.sql`

新增：

- `tushare_dividend_raw` / `dividend_event`
- `index_daily`（至少沪深300或中证全指等宽基基准）

分红事件必须保留公告日、实施公告日、股权登记日、除权除息日和派息日等不同时间，不可只留一个日期。

### P0-7：`022_create_pit_layer.sql`

新增：

- `pit.universe_daily`
- `pit.financial_asof`
- `pit.security_tradeability_daily`

实现并测试 `available_trade_date`，通过交易日历把公告可用日期映射到最早允许使用的交易日。

### P0-8：`023_create_feature_v1.sql`

新增：

- `feature.price_daily_v1`
- `feature.valuation_daily_v1`
- `feature.company_financial_v1`
- `feature.shareholder_return_v1`
- `ops.feature_registry`

先用10至20只黄金样本跑通，不直接全市场计算。

### P1：`024_create_research_models.sql`

新增研究快照、候选池、AI报告、证据引用和人工审阅模型，并为OpenSearch增加稳定文档ID、内容哈希、版本和附件信息。

### P1：`025_create_backtest_portfolio.sql`

新增策略、运行、信号、订单、成交、现金、持仓、绩效、模拟组合和真实组合模型。

### 当前第一条开发主线

```text
完成现有Raw历史初始化
        ↓
补股票历史状态 / 停复牌 / 涨跌停 / 分红
        ↓
修正财务版本键并拆最小标准字段
        ↓
构建PIT股票池与财务As-Of
        ↓
Feature v1
```

在这条主线完成前，不开发复杂评分模型和自动买卖逻辑。
## 20. 最终工程判定

这套方案已经具备进入开发的条件，核心方向没有结构性错误。但它的可靠性不来自“Raw、Clean、Feature这些名称”，而来自以下真正的底线：

- 历史股票池按日期重建。
- 财务修订版本不覆盖历史。
- 信息可用时间严格晚于公告披露。
- 原始成交价、复权分析价和总回报现金流分离。
- AI不污染历史验证。
- 回测包含真实可成交性和成本。
- 所有结果可以复算和追溯。

只要这些底线被落实，项目就可以从架构讨论转向代码实现。反之，即使表名设计得很漂亮，也可能得到虚假的回测和错误的信心。

## 21. 基于当前模型的逐表补全清单

### 21.1 可以直接保留的现有模型

以下模型设计与目标需求一致，原则上不重建：

| 当前模型 | 结论 | 后续动作 |
|---|---|---|
| `tushare_daily_raw` | Raw行情设计正确 | 完成历史覆盖、批次和质量审计 |
| `tushare_daily_basic_raw` | Raw估值设计正确 | 完成历史覆盖 |
| `tushare_adj_factor_raw` | 复权事实层正确 | 初始化完成后做跳变与公司行动核验 |
| `tushare_trade_cal_raw` | 日历Raw正确 | 保留并定期审计 |
| `daily_kline` | 原始执行价格标准表可用 | 不覆盖为前复权价；补可选校验字段 |
| `adj_factor_daily` | 复权标准表可用 | 与日线按代码和日期连接 |
| `trading_calendar` | 可直接作为日期轴 | 增加“下一交易日”查询索引/函数 |
| OpenSearch `news-YYYY` | 新闻公告承载合理 | 保留正文搜索能力，补证据与版本字段 |

### 21.2 需要扩展但不应删除的模型

#### `stock_basic_info`

当前表适合表达“现在是什么状态”，但不适合独立承担历史股票池。保留原表，并新增：

- `stock_basic_snapshot`：每次股票列表抓取的快照。
- `security_lifecycle`：上市、退市、市场变更的有效区间。
- `security_name_event`：名称、ST标识变化事件。

不要依靠当前 `name` 字段反推十年前是否ST。

#### `daily_kline`

当前OHLCV字段足以做基础特征。建议补充或通过视图提供：

- `pre_close`、`change`、`pct_chg`，用于源数据交叉校验。
- `source`、`source_batch_id`、`quality_status`。
- 如果未来接入多行情源，再将唯一键升级为 `(stock_code, trade_date, source)`；当前只有Tushare时无需急改。

#### `valuation_daily`

当前估值字段主体够用，但应补：

- `dv_ttm`（来源支持时）。
- `total_share`、`float_share`、`free_share`，用于股本、流通性和稀释核验。
- 明确市值单位。不要在旧列上静默改单位；新增 `_cny` 标准列或建立统一单位视图。
- `quality_status` 和 `source_batch_id`。

#### `stock_financial_indicator`

当前表适合做当前摘要，但不足以作为严格PIT历史表。建议新增版本模型，并给摘要表/视图补：

- `f_ann_date`
- `report_type`
- `comp_type`
- `update_flag`
- `available_trade_date`
- `version_no`
- `payload_hash`
- `quality_status`

#### 财务Raw表

当前 `(ts_code, ann_date, end_date)` 主键不能保证容纳同报告期不同报表类型、合并/母公司口径和更正版本。最安全的方式是新增 `_vintage_raw` 表，旧表作为兼容缓存，完成核对后再切换。

### 21.3 必须新增的P0模型

| 数据域 | 新模型 | 为什么必须 |
|---|---|---|
| 运行审计 | `ops.job_run`、`ops.api_call_log`、`ops.data_quality_result` | 当前只有检查点不足以证明一次数据生产是否完整可信 |
| 股票历史池 | `stock_basic_snapshot`、`security_lifecycle`、`pit.universe_daily` | 解决当前股票列表倒灌历史造成的幸存者偏差 |
| 历史风险状态 | `stock_st`、`namechange`对应Raw/标准模型 | 当前名称只代表现在，不能还原历史ST状态 |
| 可交易性 | `suspend_d`、`stk_limit`对应Raw/标准模型 | 回测需要识别停牌、涨停买不到、跌停卖不出 |
| 财务版本 | 四张财务 `_vintage_raw` 和 `stock_financial_indicator_vintage` | 防止财报更正覆盖过去的信息集 |
| 三大报表标准层 | `std_income_statement`、`std_balance_sheet`、`std_cashflow_statement` | Feature和AI不应在运行时直接解析JSONB |
| 公司行动 | `dividend_event` | 长期投资与回测必须正确处理现金分红和送转 |
| 基准 | `index_daily` | 不能只看策略绝对收益，必须与简单宽基比较 |
| PIT | `financial_asof`、`security_tradeability_daily` | 将“今天数据库最新值”转换为“历史当时可见值” |
| 特征 | 四组Feature v1 | 将事实层转成稳定、版本化、可重算的研究输入 |

### 21.4 建议新增的P1模型

P1在P0稳定后增加：

- `disclosure_date`：披露计划和实际披露时间交叉验证。
- `fina_audit`：非标准审计意见是长期基本面风险的重要信号。
- `forecast`、`express`：业绩预告和快报，用于当期风险预警，但需严格按公告时间处理。
- `share_float`：限售股解禁和潜在供给变化。
- `repurchase`：回购计划与实施进度。
- 行业分类与历史成分：用于同业比较、行业暴露和估值分位。
- 股东人数、质押统计、前十大股东：适合风险核验，不应成为第一版主要买入因子。

### 21.5 暂时不需要补的P2模型

第一版不需要：

- Tick、Level-2、盘口队列。
- 分钟线与高频因子。
- 资金流、筹码分布、龙虎榜全量体系。
- 自动下单、QMT交易接口。
- 大规模宏观数据库和上千个因子。

这些数据不会解决当前最核心的PIT、版本、可成交性和数据质量问题，过早加入只会增加维护成本。

### 21.6 OpenSearch现有模型的增量优化

现有 `title/content/pub_time/fetch_time/source/channel/stocks/url/vec_status` 已经能支持搜索。建议增加：

- `document_id`：系统内稳定ID。
- `source_document_id`：来源站点原始ID。
- `content_hash`、`dedup_key`：跨源和重复采集去重。
- `document_version`、`supersedes_id`：公告更正或正文变化。
- `announcement_type`、`report_period`、`event_date`。
- `attachment_url`、`attachment_hash`、`attachment_parse_status`。
- `stock_match_method`、`stock_match_confidence`。
- `embedding_model`、`embedding_version`。
- `available_at`：最早可供研究使用的时间。

PostgreSQL新增 `research.evidence_reference`，保存AI结论与OpenSearch文档ID、片段位置和结构化字段之间的关系。OpenSearch继续负责全文和向量检索，不需要把正文重复写入PostgreSQL。

### 21.7 修订后的完整链路

```text
现有Tushare采集器
    ↓
现有 tushare_*_raw（继续补历史）
    + 新增状态/停牌/涨跌停/分红/基准Raw
    + 新增财务Vintage Raw
    ↓
现有标准表（继续使用）
    + 三大报表最小标准字段
    + 历史证券状态与可交易状态
    ↓
PIT：历史股票池 + 财务As-Of + 最早可用交易日
    ↓
Feature：价格 / 估值 / 财务 / 股东回报
    ↓
Research Snapshot + OpenSearch证据 + AI反方分析
    ↓
Backtest：信号 / 订单 / 成交 / 现金 / 持仓 / 公司行动
    ↓
模拟组合 → 小资金人工确认
```

### 21.8 当前阶段的停止条件

发生以下任一情况时，停止向Feature或候选股层发布结果：

- 当日行情或估值覆盖率异常下降。
- 复权因子与日线无法对齐。
- 财务公告时间缺失或版本冲突未解决。
- 某历史日期无法重建当日股票池。
- 停牌/ST/涨跌停状态缺失导致可成交性无法判断。
- Raw和标准表抽样核对不一致。

系统在数据不完整时应输出“本日不可生成候选”，而不是用空值、零值或AI猜测继续运行。


## 22. 核验依据与参考资料

本大纲结合了你已完成的Tushare接口验证结果，以及以下官方/研究资料核验：

1. Tushare官方：股票基础信息 `stock_basic`，支持上市、退市、暂停和未交易状态及上市/退市日期。
2. Tushare官方：交易日历 `trade_cal`。
3. Tushare官方：历史日线 `daily`、复权因子 `adj_factor`、每日指标 `daily_basic`。
4. Tushare官方：利润表、资产负债表、现金流量表和财务指标，包含公告日、实际公告日、报告期、报表类型、公司类型及更新标识等字段。
5. Tushare官方：前复权机制按设定结束日期动态计算，因此本方案采用原始价格 + 复权因子作为永久事实。
6. Tushare官方：历史ST接口可按交易日期提供状态，但较早年份覆盖存在限制，需通过名称变更等数据补充并保留覆盖范围标记。
7. 回测研究文献：使用期末成分代替历史成分会造成显著的前视/幸存者偏差。
8. 近期金融LLM研究：通用模型可能通过参数记忆知道历史结果，故第一版不让AI参与历史信号生成。

9. 当前系统模型说明（2026-08-02）：已存在标准表、市场Raw、财务Raw、交易日历、复权因子、OpenSearch新闻公告及历史初始化检查点。
10. Tushare官方：`suspend_d`每日停复牌、`stock_st`历史ST列表、`namechange`历史名称变化、`stk_limit`每日涨跌停价格。
11. Tushare官方：`dividend`分红送股、`fina_audit`财务审计意见、`forecast`业绩预告。
