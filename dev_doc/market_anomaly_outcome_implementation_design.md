# 市场异常后验结果自动评估实现设计

状态：常规每日增量评估代码完成；尚未应用数据库迁移、执行计划验收或启用生产调度。Outcome 源修订的显式创建入口未实现，需先有可验证的来源版本/修正清单契约。
上位契约：[market_anomaly_observation.md](market_anomaly_observation.md)；本设计只细化 `evaluation.market_anomaly_outcome`（市场异常后验结果表）的自动生成、增量成熟、审计和验收，不改变观察域边界。

## 1. 目标与非目标

目标是让系统能够稳定回答：

> 一条已冻结的异常观察发生后，市场在未来 5、10、20、60、120 个交易日实际如何变化？

输出是客观事后路径，不是交易建议，也不是策略收益。它不包含入场、成交、仓位、手续费、滑点、止盈或止损；这些只属于 `evaluation.scenario_result`（策略场景结果表）和 Backtest（回测）。

实施约束：

- 只读取已冻结的 Observation（异常观察）及其对应的价格、交易日历、基准数据；不重采 Raw（原始层），不重建 Standard/PIT/Feature（标准/时点/特征层）。
- 只评估 `anomaly_flag = TRUE` 的冻结个股观察，不为正常股票每日生成无意义的后验行。
- Outcome（后验结果）永不回写或改变 Observation、标签、输入哈希和冻结状态；策略决策不得读取未来 Outcome。
- 每次处理有明确日期、快照、证券代码和批量上限；不做五年无界扫描，不用 N+1（逐条查询），不跨历史区间开大事务。

## 2. 当前代码与交付边界

已实现并有专项测试覆盖：

- `data_collect/observation/outcome_metrics.py`（后验指标纯计算）：固定 5/10/20/60/120 个 SSE 交易日、复权价格和总回报口径、基准超额收益、路径风险、缺失原因及稳定哈希。
- `MarketAnomalyOutcomeRepository`（市场异常后验结果仓储）：只读 FROZEN（已冻结）且异常标记为真的事件；按最多 100 个事件成批读写；价格路径最多 121 个交易日；显式快照回放限 5 个快照。
- `OutcomeEvaluator`（后验评估器）：方法哈希隔离水位、同日锚点捕获、历史锚点不可复原时 fail-closed、每次最多 5 个事件批次和自动初始化最多检查 31 个快照日期，超限明确报告 `INCOMPLETE_RUN`。
- `market_anomaly_outcome_daily.py`（每日后验评估作业）：只接受最近 31 日范围内最新 PASS（通过）市场门禁日期；缺门禁或非 PASS 直接失败，不触碰上游重建。
- `sql/113_market_anomaly_outcome_incremental.sql`（后验结果增量迁移）：新增不可变锚点、期限状态/来源清单、方法内修订身份和按方法/到期日的待处理部分索引。
- Repository（仓储）提供初始化、到期队列、价格路径的 `EXPLAIN (FORMAT JSON)` 入口；调用它们只规划查询，不执行源查询。

未执行数据库迁移 113、未运行真实 PostgreSQL `EXPLAIN`、未写生产 Outcome 行，也未回放历史快照。因为迁移尚未应用，依赖新增字段/索引的真实执行计划仍是部署前门禁；每日 DAG（有向任务流）中的 Outcome 节点在 `config.example.yaml` 里保持注释关闭。`outcome_revision` 已进入数据身份和 Schema（数据库结构），但常规作业只创建 revision 1；不能通过任意重跑覆盖终态，也没有源修订的人工/自动重算入口。待上游来源提供可核验的修正版本身份后，再开发显式、限量、保留旧版本的 revision 操作。不得把代码测试通过等同于生产数据库已验收。

因此不是从零造表，而是在既有 Outcome 表、纯计算和仓储基础上补一个小而深的 `MarketAnomalyOutcomeEvaluator`（市场异常后验评估器）模块，并对 Outcome 表做最小契约升级。

## 3. 固定计算契约

### 3.1 观察事件与资格

一个事件以不可变 `observation_id`（观察记录 ID）为身份。只有满足以下条件才创建 Outcome：

- 所属 `observation.snapshot` 状态为 `FROZEN`（已冻结）；
- `anomaly_flag IS TRUE`；
- Observation 的 `observation_date`、`ts_code`、方法论哈希及数据基线身份完整；
- 观察日价格起点可从该观察记录绑定的已验证数据发布/输入 Manifest（清单）中确定。

行业状态为 `UNKNOWN`（未知）不阻止后验评估；只有对应价格输入无效或无法确定时，才将价格结果标为无效/不完整。重复异常是不同日期的不同事件，保留各自的 `observation_id`，不去重成“每只股票一条”。

### 3.2 时间与价格口径

- `T` 是观察日；`T+n` 是 `std.trading_calendar`（标准交易日历）中严格晚于 T 的第 n 个 SSE 开市日。它不是自然日，也不按该证券自己的有效行情行数漂移。
- `future_price_return_n`（未来价格收益）=`adj_close_base[T+n] / adj_close_base[T] - 1`。
- `future_total_return_n`（未来总回报）=`total_return_price[T+n] / total_return_price[T] - 1`。
- `excess_return_n`（超额收益）=`future_price_return_n - benchmark_price_return_n`，以百分点差表达；基准代码绑定 Outcome 方法论（默认沿用 Observation 契约中的 `000985.CSI`，中证全指）。没有合格基准时只将超额收益标为缺失，不抹掉可用的个股路径指标。
- `raw_close`（未复权收盘价）仅用于展示/执行审计，不作为跨期收益主口径。
- 路径指标使用观察日 T 到目标 T+n 的完整有效 `adj_close_base` 序列（含起点和终点）：`max_drawdown`（最大回撤）按运行峰值计算；MFE/MAE（最大有利/不利变动）相对 T 计算；`best_return`/`worst_return` 分别与 MFE/MAE 完全同值；`volatility_n`（n 日波动率）是区间日收益样本标准差 `ddof=1`，不年化。
- 若 T+n 的股票行情因停牌、退市或来源缺失而不存在，不顺延到下一条行情、不以前值伪造；该期限记录明确缺失原因。终点有效但中间路径不完整时，端点收益可用，依赖完整路径的回撤/MFE/MAE/波动率置空并标明路径缺失。

### 3.3 方法论与输入复现

`evaluation_methodology_hash`（评估方法哈希）独立于 Observation 阈值，覆盖：期限集合、交易日历规则、价格字段/复权契约、基准代码与收益公式、路径指标定义、缺失/停牌规则、计算器契约版本。

每个结果输入 Manifest 至少绑定：

- `observation_id`、Observation 的 `input_manifest_hash`（输入清单哈希）和数据基线身份；
- 观察日两个价格坐标及其数据发布/来源身份；
- 每个期限目标日和所用股票、基准价格行的稳定业务键、来源发布/版本及质量状态；
- 计算方法哈希。

对以上规范化内容计算 `input_manifest_hash`；对方法哈希、输入哈希、成熟状态和结果字段计算 `outcome_hash`（后验结果哈希）。重试同一输入必须幂等；上游已确认修正时增加 `outcome_revision`（结果修订号），保留旧结果，不覆盖已终结版本。仅仅重跑但输入哈希相同，不增加修订号。

**观察日价格锚点是硬门禁。** 首选在 Observation 冻结的同一数据发布/基线下取得锚点并写进初始 Outcome。如果历史记录无法从该数据基线或保存的来源清单重建锚点，不得拿“当前最新版”静默替代；写入明确的 `INVALID_INPUT`/`ANCHOR_NOT_RECONSTRUCTABLE`，并在质量报告中计数。

## 4. 生命周期与状态

每个期限独立记录 `PENDING`（尚未到期）、`MATURED`（期限已到且数据合格）或 `MISSING_SOURCE`（质量门禁已确认目标日期已过，但来源仍缺失/无效），并保存该期限的目标交易日、缺失原因和输入哈希。

行级 `maturity_status` 定义：

| 状态 | 含义 |
|---|---|
| `PENDING_MATURITY` | 至少一个期限目标日尚未到，或对应交易日的生产质量门禁尚未通过；未成熟指标保持 NULL。 |
| `MATURED` | 五个期限均已终结且数据完整。 |
| `INCOMPLETE` | 五个期限均已到期，但至少一个期限的目标/路径/基准源被质量门禁确认缺失；已计算的其他期限仍保留。 |
| `INVALID_INPUT` | 观察日锚点无效或不可复现，不能计算有效结果。 |

到期判断使用 `through_date`（已通过生产质量门禁的最新交易日），不能用系统当天日期。行情采集尚未完成时继续 `PENDING_MATURITY`；不能因数据晚到而提前写 `MISSING_SOURCE`。

期限从 `PENDING` 变成终态后不可被普通重试重写。Outcome 行在仍有期限待成熟时允许受控增量更新；全部终结后只读。确认上游修正需要显式新 `outcome_revision`，不能原地覆盖旧终态。所有状态转换都通过 Repository/Policy 和事务管理器完成。

## 5. 深模块接口与职责

外部调用者只需要一个接口：

```python
OutcomeEvaluator.run(
    *,
    through_date: date,                  # 必须是已通过生产质量门禁的日期
    observation_snapshot_ids: Sequence[UUID] = (),  # 可选，精确指定待初始化的冻结快照
    context: RunContext,
) -> OutcomeRunReport
```

接口不接受表名、SQL、任意日期范围或任意批大小。方法内部设置硬上限（每次最多 5 个事务批次，每批最多 100 个事件；自动初始化最多检查 31 个快照日期；显式历史回放最多 5 个快照）。超过上限返回 `INCOMPLETE_RUN`，由 Checkpoint（检查点）继续，不能报告全部完成。日常调用不传快照时，从按评估方法隔离的持久化水位读取尚未初始化的冻结观察日期；历史回放必须显式指定有限快照 ID。

`OutcomeEvaluator` 内部隐藏：

1. 验证快照冻结状态和数据质量水位；只选 `anomaly_flag=TRUE`。
2. 初始化/恢复待成熟 Outcome，按下一个到期日领取有限事件批次。
3. 批量读取交易日历、股票价格路径和基准路径；一次读一组证券/日期，不逐事件查询。
4. 调用纯函数计算各期限值、期限状态及完整性原因。
5. 生成确定性输入/结果哈希，通过 `MarketAnomalyOutcomeRepository` 批量写入。
6. 每个有限批次提交并记录 Checkpoint；失败可安全重试，已完成期限不重复覆盖。
7. 返回本次扫描日期、候选事件数、成熟/待成熟/不完整/无效数、读写行数、最后水位和错误摘要。

Repository（仓储）负责所有 SQL；Job（作业）只提供已通过质量门禁的 `through_date`、快照范围和 `RunContext`（运行上下文），不写 SQL/commit，不持有 Connection/Cursor（连接/游标）。纯计算模块接受规范化的 Observation、股票路径、基准路径及不可变 OutcomePolicy，返回结果，不访问数据库。

## 6. 最小 Schema（数据库结构）变化

复用现有 `evaluation.market_anomaly_outcome`，不建每日全股票复制表。实现前需通过版本化 SQL 迁移：

- 增加 `anchor_adj_close_base`、`anchor_total_return_price`，持久化观察日锚点；
- 增加 `next_evaluation_date`，使待处理队列按到期日可索引；
- 增加 `horizon_status JSONB`、`input_provenance JSONB`、`missing_reason_codes JSONB`，保存期限级成熟/缺失与来源信息；
- 增加 `outcome_revision`，将唯一身份从 `(observation_id, evaluation_methodology_hash)` 扩为 `(observation_id, evaluation_methodology_hash, outcome_revision)`；
- 扩展状态约束以允许 `INCOMPLETE`；已有收益、超额收益、路径指标、输入哈希及 `calculation_run_id` 继续复用；
- 建立面向待到期领取的部分索引：`(evaluation_methodology_hash, next_evaluation_date, outcome_id) WHERE maturity_status='PENDING_MATURITY'`；完成后清空 `next_evaluation_date`。

迁移前只做目录检查、重复键/空值的有界检查和执行计划预检；不要对大型表做全表 `COUNT/MAX/GROUP BY`。若需唯一索引，先 `EXPLAIN (FORMAT JSON)`，确认可控后使用 `CREATE INDEX CONCURRENTLY`；索引/约束 DDL 与数据迁移分别评估锁和 IO。所有 Repository 读 SQL 在实现前都必须经 Kernel 执行 `EXPLAIN (FORMAT JSON)`（不带 `ANALYZE`），证明日期范围、证券代码数组、分区裁剪/索引路径和最大估算行数符合预算。

## 7. 有界读取与运行方式

### 日常增量

日常 Outcome 作业作为 `production_market_activity`（每日成交观察生产任务）之后的独立下游任务运行，前提是该日市场质量门禁已通过且观察快照已冻结。Outcome 失败只影响评估积压，不回滚、不阻断已冻结观察或 Raw/Standard/Feature 主链；通过同一 Outcome Checkpoint 重试。

候选事件按冻结 `snapshot_id`/日期键集读取；到期事件使用 `next_evaluation_date <= through_date`、固定排序和 `LIMIT`（硬上限），避免每晚扫描全部历史 Observation。股票/基准价格查询限制为：有限证券代码数组、有限交易日区间（最多 121 个市场交易日）及必要的字段投影；每批最多 100 个事件/证券组，价格返回行硬上限为证券数 × 121。事件同证券可复用同批价格序列，不能逐事件查询。

一个事务只写一个有限事件批次。使用序列/批次级锁防并发重复领取；使用 `ops.data_access_checkpoint` 保存快照/日期/方法哈希/页游标。单次运行达到批次或时间上限且仍有积压时返回 `INCOMPLETE_RUN`，不能报成功。

### 历史回放

只对已经存在的 FROZEN Observation 快照按日期分片处理；不默认回填五年，也不重跑 Raw、Standard、PIT、Feature 或 Research Snapshot。没有历史冻结异常记录的日期不会凭当前规则伪称“当时已观察到”。若目标是研究五年异常事件，历史 Observation 回放是单独、逐日、有 Checkpoint 的工作，需先抽样验证价格锚点可按原基线重建，再决定范围。

## 8. 统计研究交付

Outcome 表是事件级事实，不在本模块内做“最佳股票 Top N”或交易筛选。后续 Evaluation（评价）按标签/阈值/市场状态聚合事件数、成熟数、均值、中位数、分位数、正收益比例、超额收益和路径风险。

同一证券的重复事件和重叠窗口相关，统计报告必须同时给出原始事件数及按证券聚类/去重敏感性结果，不能把数千条相关事件当作独立样本。探索性报告不能证明 Alpha（超额收益优势）；如要验证异常信号是否给策略增加价值，应新建明确的 StrategyVersion（策略版本），与基准策略在相同冻结 Dataset、成本和样本外区间比较。Outcome 本身永远不进入产生同一观察日信号的决策输入。

## 9. 测试与验收

### 纯计算测试

- 五个期限的 T+交易日映射、收益公式、总回报、基准超额收益；自然日、错序、重复交易日、跨证券输入必须拒绝。
- 完整路径回撤/MFE/MAE/样本波动率；部分路径只让依赖的字段为空，不伪造 0。
- T 锚点缺失、Feature 质量无效、目标价格缺失、基准缺失、停牌和上市/退市边界的状态码正确。
- 未成熟期限不写值；质量门禁未通过时不会从 `PENDING` 转成 `MISSING_SOURCE`。
- 相同输入得相同哈希；Scenario 字段被拒绝；Outcome 不会修改 Observation。

### Repository/集成测试

- 只选 `FROZEN + anomaly_flag=TRUE`；DRAFT/VALIDATED/NORMAL 不产生结果。
- 一批多个证券使用固定数量 SQL；返回行数不得超过硬上限；没有逐事件 N+1。
- 重试幂等、断点恢复、并发锁、事务失败回滚、超限明确 incomplete、修订产生新 revision 而旧终态保留。
- 对候选事件、到期队列、价格路径、基准路径逐条检查 `EXPLAIN (FORMAT JSON)`；不得有大表无范围 Seq Scan（顺序扫描），不得有无界日期/证券范围。
- 生产抽样核对至少一条事件的 T/T+5/T+20/T+120 日期、价格坐标、基准和手工收益计算；查询为只读、小范围，不运行历史回填。

### 分批验收

1. 先在纯函数层通过测试，不访问数据库。
2. 只读执行计划核验；没有计划通过不部署新 SQL。
3. 用 1 个冻结异常、最多 1 个证券验证完整路径和哈希，不写历史大批数据。
4. 小批最多 100 个事件，验证幂等、状态推进、数据可复算和数据库负载。
5. 观察期确认每日下游任务只追新冻结事件和到期事件；再决定是否对已有历史冻结事件做分片回放。

## 10. 落地顺序

```text
已完成：契约/纯计算/仓储/有界评估器/迁移脚本/可选 DAG 接口/专项测试
  → 待部署门禁：在目标库核对现有数据与重复键
  → 应用 SQL 113
  → 对 Repository 中每条 SQL 执行 EXPLAIN (FORMAT JSON)，不带 ANALYZE
  → 单个冻结异常的只读对照
  → 最多 100 事件的小批写入与状态复核
  → 再启用每日独立下游
  → 历史回放另行明确范围，逐快照分批批准
```

完成标准是：每条异常事件能够追溯观察锚点、未来价格/基准输入、方法哈希、输入哈希和结果哈希；未成熟/缺失有明确状态；查询有界、可续跑；结果可验证但永不污染当时 Observation 或策略决策。
