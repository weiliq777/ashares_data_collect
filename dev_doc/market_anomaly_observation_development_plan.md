# Market Anomaly Observation Domain（市场异常观察域）开发实施方案

状态：`APPROVED`（正式实施方案）
生效日期：2026-08-31
对应契约：[market_anomaly_observation.md](F:/a_github/Finance_cn/ashares_data_collect/dev_doc/market_anomaly_observation.md)

> 本文规定如何把市场异常观察域落地到现有 Data Foundation Access Framework（数据基座统一访问框架）。
> 观察域契约是最高实现依据；本文只规定实施顺序、组件边界、测试和运行纪律，不得重新解释指标语义。

## 1. 实施目标

建立一条可长期运行、可审计、可复现的观察链路：

```text
Standard / PIT / Feature
        ↓
Stock Observation（个股异常观察，核心链路）
        ↓
Sector Confirmation（行业共振确认，可选增强）
        ↓
Evaluation（后验结果）
```

核心原则：

1. 个股 Observation 不依赖行业数据，可以独立完成历史回填和每日增量；
2. 行业确认只提供附加证据，不修改个股 `anomaly_flag`、`primary_state` 和指标；
3. 行业数据缺失时使用 `UNKNOWN`，不能伪造为 `NOT_CONFIRMED`；
4. 任何方法论、输入基线或字段语义变化都生成新的 `methodology_hash`，不覆盖旧冻结结果；
5. 所有数据库访问严格遵守 `Repository → Kernel`，业务作业不持有 Connection/Cursor（连接/游标），不手写 SQL，不执行 commit/rollback。

## 2. 当前输入盘点与处理策略

### 2.1 核心个股输入

| 输入 | 中文含义 | 用途 | 要求 |
|---|---|---|---|
| `std.daily_kline` | 标准日线行情表 | 收盘价、成交量、成交额、日收益 | 观察日收盘数据完整 |
| `std.valuation_daily` | 标准估值日表 | 普通换手率、自由流通换手率、自由流通股本 | 必须明确 `turnover_rate_f` 单位和缺失规则 |
| `pit.universe_daily` | 时点股票池表 | 观察日股票池和截面分母 | 必须按观察日过滤 |
| `pit.security_tradeability_daily` | 时点交易资格表 | 停牌、涨跌停、可交易性 | 不得把不可交易日当作零成交 |
| `feature.price_daily_v2` | 时点价格特征表 | 均线、波动率、收益所需价格基础 | 必须提供或派生两个明确价格坐标 |

个股主链路只依赖上述核心输入。行业数据或事件数据缺失，不得阻断个股 Observation。

### 2.2 必须补齐的本地字段

`feature.price_daily_v2` 必须明确提供：

- `adj_close_base`（底座复权收盘价）：用于价格表现，不包含未来才可见的公司行为信息；
- `total_return_price`（总回报价格）：用于含分红的总回报统计；
- 两个字段的公司行为处理、基准日、税费和缺失规则必须记录在 Feature 契约中。

`std.valuation_daily` 必须标准化保存：

- `turnover_rate`（普通换手率）；
- `turnover_rate_f`（自由流通换手率）；
- `free_share`（自由流通股本）；
- 明确统一单位，不能在 Repository（仓储）内隐式换算后不留痕迹。

### 2.3 行业增强输入

目标输入为：

```text
pit.industry_membership_daily
```

至少包含：

```text
trade_date
ts_code
industry_system
industry_level
industry_code
industry_name
source_asof_date
data_available_timestamp
source_record_key
record_version
quality_status
```

当前 Tushare（数据供应商）接口情况：

- `index_member_all`（申万行业成分）和 `ci_index_member`（中信行业成分）存在，但当前访问路径只能稳定返回最新成分，不能作为五年历史 PIT；
- `dc_member`（东方财富板块成分）和 `tdx_member`（通达信板块成分）可以返回部分日期成分，但分类体系不同，不能伪装成 SW2021（申万2021）；
- `ths_member`（同花顺板块成分）缺少可用纳入/剔除日期，不能单独满足 PIT。

因此分两个阶段处理：

1. 历史观察：行业确认统一为 `UNKNOWN`，不影响个股 Observation；
2. 新增数据：若每日捕获行业成分，先标记 `DAILY_CAPTURED`，通过连续性、覆盖率和变更审计后才允许标记 `CONFIRMED`。

禁止使用当前 `stock_basic_info.industry`（股票当前行业字段）倒灌历史行业归属。

### 2.4 事件输入

事件证据属于增强能力，不阻断纯量价观察。接入时必须统一为：

```text
event_id
event_type
event_visible_at
effective_date
source_record_id
source_hash
quality_status
```

观察日后发现的事件只能写入 `evaluation.retrospective_event_link`（事后事件关联表），不得参与历史观察重算。

## 3. 目标数据对象

### 3.1 观察发布批次

`observation.snapshot`（观察快照表）保存：

- `snapshot_id`（快照物理主键）；
- 观察日期范围；
- `methodology_hash`、`data_baseline_id`、`input_manifest_hash`；
- `observation_policy_id`、`contract_hash`；
- `calculation_run_id`、代码身份和质量报告；
- 日期、股票、行业覆盖统计和状态。

### 3.2 个股观察主体

`observation.stock_anomaly_daily`（个股异常观察日表）保存：

- `observation_id`（观察物理主键）；
- `observation_date`、`ts_code`；
- 全部个股指标、五类评分和 `observation_priority`（观察优先级）；
- `anomaly_flag`（异常标记）、`primary_state`（主状态）、`state_reason_codes`（状态原因码）；
- `calculation_status`、`quality_status`、`missing_reason_code`；
- 输入字段摘要和哈希。

业务唯一键固定为：

```text
observation_date + ts_code + methodology_hash + data_baseline_id
```

### 3.3 行业确认与行业聚合

`observation.stock_sector_confirmation`（个股行业确认表）保存：

- `observation_id`；
- `industry_system`、`industry_level`、`industry_code`、`industry_name`；
- `sector_data_status`：`CONFIRMED / DAILY_CAPTURED / MISSING / INVALID`；
- `sector_confirmation_status`：`CONFIRMED / NOT_CONFIRMED / UNKNOWN`；
- 行业确认分数、原因码、来源快照和质量状态。

`observation.sector_anomaly_daily`（行业异常观察日表）保存行业级广度、新增、持续和退出统计。
行业确认表与行业聚合表都引用不可变 `observation_id` 或明确的观察批次身份，不能复制并维护多套联合业务键。

### 3.4 评估对象

`evaluation.market_anomaly_outcome`（市场异常后验结果表）只保存未来实际路径表现，
例如未来 5/10/20/60/120 个交易日的价格收益、总回报、最大有利 excursion（有利偏移）、
最大不利 excursion（不利偏移）、波动率和超额收益。

`evaluation.scenario_result`（策略场景结果表）才允许保存 T+1 开盘、手续费、滑点、止盈止损和持有规则。

## 4. 代码分层与组件边界

### 4.1 纯计算层

新增纯函数模块，输入为已规范化的 DataFrame（数据表）或不可变记录，输出指标和状态，不接触数据库：

```text
data_collect/observation/
├── contracts.py              # 字段、状态、状态码和方法论身份
├── policy.py                 # 阈值、窗口、权重和缺失策略
├── stock_metrics.py          # 个股指标
├── stock_state_machine.py    # 个股主状态
├── sector_confirmation.py    # 行业确认，可选
├── outcome_metrics.py        # 后验结果，不参与观察计算
└── hashing.py                # 规范化输入和结果哈希
```

纯计算层必须可以在没有数据库和 Tushare Token（访问令牌）的情况下运行完整单元测试。

### 4.2 Repository（仓储）层

至少建立：

```text
StockAnomalyObservationRepository
StockSectorConfirmationRepository
SectorAnomalyObservationRepository
ObservationEventRepository
MarketAnomalyOutcomeRepository
ScenarioEvaluationRepository
```

复杂 `SET-based SQL`（集合式 SQL）只能存在于 Repository 内；业务 Job（作业）只编排范围、策略和计算结果。

Repository 必须通过：

```text
DataAccessKernel
  ├─ read.query / read.stream
  ├─ write.upsert / write.bulk_insert
  ├─ rebuild.publish
  ├─ publish.snapshot / publish.dataset
  └─ quality.record
```

禁止新增万能 `execute()`、公开连接/游标、业务层 SQL、业务层 commit，以及按股票逐只查询数据库。

### 4.3 运行范围与事务

- 历史回填按交易日小批次，建议每批 1～5 个交易日；
- 单个批次内部先读取有限范围，再计算，再写入，再质量校验；
- 每批独立事务并立即写 `Checkpoint`（检查点）；
- 最大回看窗口为 252 个交易日，暖机数据只读，不重复写入；
- 每日增量只处理新交易日及受影响的有限窗口；
- 上游修正只重算受影响日期，不从 2021 年重新扫描；
- 使用 `SCOPED_MERGE`（范围合并）或 `PARTITION_REPLACE`（分区替换），禁止无范围删除和大事务。

## 5. 开发阶段与完成标准

### 阶段 0：契约与输入冻结

工作内容：

- 固定本文对应的观察契约版本；
- 固定 `STOCK_FIRST_OPTIONAL_SECTOR`（个股优先、行业可选）方法论身份；
- 固定主市场基准，默认建议使用 `000985.CSI`（中证全指），最终值必须进入 `DatasetPolicy`（数据集策略）；
- 记录当前输入字段缺口，不用旧字段偷换新语义。

完成标准：契约哈希、策略哈希和字段来源清单完整。

### 阶段 1：Schema 与 DatasetPolicy（数据集策略）

工作内容：

- 创建 `observation`、`evaluation` Schema（模式）及表结构；
- 登记表级白名单、主键、业务唯一键、允许操作和敏感字段；
- 添加 FROZEN（冻结）后数据库层拒绝更新/删除的约束；
- 校验 `turnover_rate_f`、`free_share`、`adj_close_base`、`total_return_price` 字段来源和单位。

完成标准：迁移脚本可重复检查；Policy 与唯一键一致；不影响现有 Research Snapshot（研究快照）。

### 阶段 2：个股纯计算引擎

工作内容：

- 实现交易异常、价格结构、量价结构和持续性指标；
- 实现异常标签和主状态优先级；
- 将 `ACCUMULATION_LIKE` 实现为纯个股条件；
- 明确缺失、停牌、上市不足和分母为零的处理；
- 生成稳定的 `state_reason_codes` 和输入哈希。

完成标准：纯函数测试通过；重复输入产生完全一致的指标、状态和哈希；没有未来字段参与计算。

### 阶段 3：行业确认增强层

工作内容：

- 实现 `UNKNOWN / NOT_CONFIRMED / CONFIRMED` 三态判断；
- 实现 `DAILY_CAPTURED` 与 `CONFIRMED` PIT 的质量门禁；
- 行业缺失时只写原因和状态，不阻断个股观察；
- 行业聚合分母严格来自观察日 PIT 股票池；
- 行业输入后续升级时使用新批次，不更新旧冻结观察。

完成标准：行业数据缺失仍能生成个股观察；缺失不被统计为负样本；行业确认不能改变主状态。

### 阶段 4：Repository、Job 与质量门禁

工作内容：

- 实现观察批次创建、构建、校验、冻结和续跑；
- 将所有读写接入 `Repository → Kernel`；
- 将长查询拆成受控读取和小批量写入；
- 写入 `RunContext`、Manifest、Lineage（血缘）和 Checkpoint；
- 增加超时、重试、锁和幂等处理。

完成标准：契约扫描为 0 违规；不存在业务层连接/游标/SQL/commit；不存在 N+1 查询和跨五年大事务。

### 阶段 5：测试与小批量回放

先使用固定 10～50 个股票、5～20 个交易日的小范围数据进行真实数据库集成测试，验证：

- 指标公式与边界值；
- `UNKNOWN` 与 `NOT_CONFIRMED`；
- 停牌、涨跌停、上市不足和缺失字段；
- 重试、断点恢复和幂等；
- FROZEN 后更新/删除拒绝；
- 同一输入重复运行哈希一致；
- 行业确认不改变个股主状态；
- 查询规模和事务耗时处于策略上限内。

小批量结果必须抽样核对原始行情、自由流通换手率和价格坐标，确认后才允许历史回填。

### 阶段 6：历史回填

历史回填顺序：

```text
读取 252 个交易日暖机窗口
 → 计算个股 Observation
 → 行业可用时计算行业确认，否则写 UNKNOWN
 → 写入 Manifest / Lineage
 → 小批次质量校验
 → VALIDATED
```

历史回填只读取已有 Standard/PIT/Feature，不重建 Raw、Standard、PIT 或 Feature。
回填过程中不得因为行业缺失而反复重跑已经成功的个股日期。

### 阶段 7：每日增量与后验评估

每日流程：

```text
盘后数据门禁
 → 读取最近 252 个交易日
 → 计算新交易日个股观察
 → 捕获行业快照并标记 DAILY_CAPTURED
 → 通过质量门禁后冻结观察批次
 → 仅对成熟窗口追加 Outcome
```

后续行业数据质量升级、公司行为修正或价格坐标修正，都通过新 `calculation_run_id` 和新输入哈希处理，
不重跑无影响日期，不修改既有 FROZEN 结果。

## 6. 测试矩阵

| 类别 | 必测内容 |
|---|---|
| 公式 | 比值、Z-score、均线、收益、回撤、成交结构、截面中位数 |
| 数据缺失 | 缺行情、缺换手、缺自由流通股本、缺行业、缺基准、窗口不足 |
| 时间语义 | 观察日可见、公告可见时间、未来事件隔离、交易日窗口 |
| 状态机 | 状态优先级、状态转换、`ACCUMULATION_LIKE` 纯个股条件 |
| 行业增强 | `UNKNOWN`、`NOT_CONFIRMED`、`CONFIRMED`、`DAILY_CAPTURED` 门禁 |
| 幂等 | 重试、断点续跑、重复批次、业务唯一键和 `observation_id` 稳定 |
| 冻结 | FROZEN 后主体、标签、行业确认、输入哈希拒绝修改/删除 |
| 性能安全 | 无 N+1、无无界查询、批次事务、超时、锁和内存上限 |
| 访问契约 | Repository → Kernel、无业务 SQL/Connection/Cursor/commit |
| 集成 | 小范围真实 PostgreSQL（数据库）构建、校验、冻结和恢复 |

## 7. 正式验收标准

满足以下条件后，观察域才可以进入正式生产：

1. 个股 Observation 在没有行业输入时可以完整运行；
2. 行业缺失严格记录为 `UNKNOWN`，不产生负样本污染；
3. 行业确认通过有效期、覆盖率、连续性和 PIT 门禁后才允许 `CONFIRMED`；
4. `ACCUMULATION_LIKE` 不包含行业必要条件；
5. 新旧方法论结果可以通过 `methodology_hash` 并存和复现；
6. `FROZEN` 结果不可更新、不可删除；
7. 历史回填与每日增量均可断点恢复，不重复写入；
8. 所有数据库访问经过 `Repository → Kernel`；
9. 全量质量门禁、契约扫描、单元测试和集成测试通过；
10. 小批量真实数据抽样与独立来源核对通过。

## 8. 明确禁止事项

- 不得把当前行业分类当作历史 PIT 行业归属；
- 不得把 `UNKNOWN` 统计成“行业未确认”；
- 不得用 DC/TDX/THS 分类冒充 SW2021；
- 不得为了补齐行业字段而重建 Raw/Standard/PIT/Feature；
- 不得原地修改旧 `ACCUMULATION_LIKE` 或旧冻结观察；
- 不得把未来 Outcome 写入 Observation；
- 不得在业务 Job 中新增 SQL、连接、游标、commit 或万能 `execute()`；
- 不得使用无范围全表扫描、逐股票查询或跨五年单事务。

## 9. 交付物

本任务的正式交付物为：

1. [market_anomaly_observation.md](F:/a_github/Finance_cn/ashares_data_collect/dev_doc/market_anomaly_observation.md)：观察域实现契约；
2. 本文：观察域开发实施方案；
3. 后续代码提交必须同时附带方法论哈希、输入字段清单、质量报告和测试结果；
4. 每次历史回填或每日增量必须保存批次 Manifest、Checkpoint、Lineage 和可复现代码身份。

## 10. 实现前性能、事务与恢复硬门禁

本节是对现有 Data Foundation Access Framework（数据基座访问框架）约束的具体化。Observation（观察域）实现必须满足以下规则；任何一项未满足，不得进入五年历史回填。

### 10.1 查询范围必须同时存在于 WorkScope 和 SQL

- `WorkScope`（工作范围）只是操作意图，不能替代 SQL（结构化查询语言）中的实际过滤条件。
- 日期型输入必须在 SQL 中显式使用 `trade_date`/契约日期字段的有界条件。
- 股票型输入必须在 SQL 中显式使用 `ts_code = ANY(...)` 或等价的有界条件。
- Repository（仓储）不得仅通过传入一个非空 `WorkScope` 来宣称查询已受控。
- 必须有测试证明：Scope（范围）存在但 SQL 缺少日期或股票过滤时，查询会被拒绝；查询计划也必须验证实际使用预期索引。

### 10.2 读取、计算、写入不得合并为大事务

- 不得把 252 个交易日的读取、计算、写入和质量校验放入 `run_atomic()`。
- `run_atomic()` 仅允许用于小型状态切换、Manifest（输入清单）登记等短事务；它只约束写入预算，不能约束读取量和内存占用。
- 长结果集不得使用会从首批数据一直持有事务的公共 `read.stream()`；Repository 必须使用具备分批事务边界的受控流式桥，且禁止在 `run_atomic()` 内调用长流式读取。
- 每个写入批次必须在行数、估算字节数和事务时限内独立提交；提交后才能推进 Checkpoint（检查点）。

### 10.3 Observation 的固定切片大小

- 历史回填默认按 1～5 个观察日组织外层批次，内部再按 100～500 只股票切片；最终大小必须通过真实数据库基准测试确定。
- 单次读取只允许读取 `[T-252, T]` 的必要字段和当前代码切片，不得读取五年全量结果后再在 Python（编程语言）内存中过滤。
- 单次写入不得把多日、全市场结果传给 `upsert()` 或 `bulk_insert()`；必须逐切片生成、写入、校验和记录 Checkpoint。
- Gateway（网关）会对输入序列进行整体物化，因此业务代码不得向写入接口传入五年数据、全市场 DataFrame（数据表）或无界生成结果。

### 10.4 N+1 与重复查询门禁

- 不得按股票逐只查询 252 日历史、前一状态、行业统计或未来 Outcome（后验结果）。
- `relative_volume_zscore`（相对成交量标准分）按观察日一次完成市场截面统计；行业广度、行业新增比例和行业持续性使用集合聚合完成。
- `anomaly_streak`（异常连续天数）和 `days_since_first_anomaly`（距首次异常天数）必须批量读取前序状态或使用有界状态缓存。
- Outcome 的多个 horizon（未来窗口）必须通过批量集合查询完成，不得为每条 Observation 单独查询未来行情。
- 单元测试和集成测试必须记录查询次数，并证明查询次数随代码数量增长不是线性增长。

### 10.5 幂等、Checkpoint 与提交不确定性

- Checkpoint 的身份至少绑定 `methodology_hash`（方法论哈希）、`data_baseline_id`（数据基线）、观察日期范围、代码切片、输入 Manifest 哈希和排序规则。
- 批次必须使用稳定排序，优先记录确定性的 `(observation_date, ts_code)` 最后键值，而不是只记录不稳定的行号。
- 写入使用业务唯一键和幂等 `UPSERT`，或使用带唯一约束的 `INSERT ... ON CONFLICT DO NOTHING`。
- 普通不可重放的 `bulk_insert()` 遇到提交结果不明确时，必须先对账，再决定恢复；不得直接盲目重试。
- Checkpoint 只能在对应数据提交成功后推进；失败或回滚的批次不得标记完成。

### 10.6 哈希、质量校验与数据库索引

- 禁止对大范围数据使用 `string_agg(row_to_json(...))` 形成单个超大聚合值。
- 输入和结果哈希必须按固定字段顺序、稳定键排序、分块读取和分块计算，再汇总块哈希。
- 所有质量校验必须带日期、代码或快照范围；禁止对每条 Observation 运行一组独立质量查询。
- 开发前必须通过 `EXPLAIN`（执行计划）确认核心查询使用 `(ts_code, trade_date)` 或 `(trade_date, ts_code)` 等适合访问方向的索引。
- 输出表必须具备业务唯一键索引；行业确认、Outcome 和血缘查询必须具备按 `observation_id` 及观察日期的索引。

### 10.7 锁与运行可观测性

- 锁粒度限定为 `methodology_hash + data_baseline_id + observation_date` 或小日期批次，禁止锁住五年全量序列。
- 所有相关 Repository 使用一致的锁定顺序，避免股票、行业、Outcome 之间形成死锁环。
- 每个批次必须记录读取行数、写入行数、查询耗时、事务耗时、批次大小、重试次数、Checkpoint 和失败原因。
- 集成验收必须证明：消费端等待时不存在长时间 `idle in transaction`，单批次不会超过事务时间、行数和字节预算。

### 10.8 开发前阻断项

在以下测试通过前，不得开始五年历史回填：

1. Scope-SQL（范围与 SQL）绑定测试；
2. 公共流式读取提前中断和慢消费者事务释放测试；
3. 写入大输入不会整体物化的边界测试，或明确证明 Observation 只传入受控切片；
4. Observation 查询次数、内存峰值和单批事务耗时测试；
5. 重试、提交不确定性、断点恢复和重复运行幂等测试；
6. 核心查询执行计划和索引命中测试。
