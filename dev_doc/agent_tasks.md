# Agent 任务清单

更新时间：2026-08-05

## T-011｜历史行情缺口补采与派生层重建

- 时间：2026-08-04
- 状态：已完成
- 结果：补齐 `daily`、`daily_basic` 缺失交易日，核心行情 Raw 覆盖 1212/1212；Standard、PIT、Feature 已重建，质量审计通过。

## T-008｜阶段一数据质量与 Raw→Standard 验收

- 时间：2026-08-04
- 目标：完成当前数据基座的全量质量检查，并证明 Standard 可从 Raw 完整重建。
- 状态：已完成
- 当前结果：质量检查覆盖股票基础、交易日历、行情、估值、复权、财务 Raw/Standard 和版本表；关键表重复键均为 0。使用 `data_collect/jobs/tushare_rebuild_validation.py` 从 `tushare_daily_raw` 全量重建 4,063,123 行，逐行比对生产 `daily_kline`，差异为 0；测试表已删除。
- 注意：质量报告发现 37 条零/异常价格事实，需在后续质量状态中标记，不能删除；历史旧任务保留 1 条失败 checkpoint 作为审计记录，当前按交易日增量任务无失败记录。
- 下一步：进入阶段二，先建设历史股票状态、公司行为和 PIT 层。

## T-007｜按交易日批量增量任务

- 时间：2026-08-04
- 目标：历史初始化按股票断点续传，日常增量按交易日一次获取全市场行情、估值和复权因子。
- 状态：已完成
- 当前结果：已停止旧的逐股票复权重试；新任务完成 2026-07-29、07-30、07-31、08-03 四个交易日的三类全市场增量，Raw、版本表和 Standard 行数一致，版本表无重复哈希；2026-08-04 尚无接口数据，按空数据保留既有 Standard。
- 下一步：后续每日收盘后使用该任务同步最近 5 个交易日，并继续执行数据质量检查。

## T-004｜Raw→Standard 正式转换前准备

- 时间：2026-08-03
- 目标：记录抽样验证结论，清理可由 Raw 重建的 Standard 表，准备正式转换。
- 状态：已完成
- 当前结果：四张市场 Raw 和四张财务 Raw 已完成；抽样 1000 条验证通过。发现部分原始日线存在停牌/无成交导致的零 OHLC，需要在质量报告中标记，不能删除。
- 规则：禁止修改、删除或覆盖任何 `*_raw` 表；三大报表 Raw 已有对应 Standard 结构，但正式全量转换由 T-006 管理。
- 下一步：验收已由 T-008 完成，后续处理质量状态标记。

## T-005｜Raw 版本化与每日增量

- 时间：2026-08-03
- 目标：实现最近 5 个交易日回补、Raw 版本留存、Standard 按日重建和增量 checkpoint。
- 状态：按交易日批量增量已完成真实验证
- 当前结果：新增 `sql/016_create_tushare_raw_versions.sql`、`tushare_market_raw_version` 和按交易日增量任务；旧的 `tushare_adj_factor_incremental_retry.py` 已停止，不再作为日常任务使用。四个交易日的 Raw、版本和 Standard 数量已核对一致。
- 下一步：后续观察接口修正时的版本追加和 Standard 重建结果。

### 已知问题记录

- 2026-08-03：财务 Raw 版本回填曾在同一数据库游标上同时读取和写入，触发 `psycopg2.ProgrammingError: no results to fetch`。已修复为读写独立连接，并保留分批提交；后续禁止在同一游标上边 `fetchmany` 边执行 INSERT。
- 2026-08-03：Tushare `adj_factor` 偶发 `SSLEOFError`。已改为按股票 checkpoint 的独立重试任务；失败只重试未完成股票，不影响已完成的 daily/daily_basic。
- 2026-08-04：按交易日批量任务请求当日 `20260804` 时三类接口均返回空数据，任务未删除既有 Standard，并记录为 `empty` checkpoint；收盘前或数据尚未发布时这是预期状态。

## T-006｜财务报表 Standard 设计冻结

- 时间：2026-08-03
- 目标：将利润表、资产负债表、现金流量表分别转换为可审计的 Standard 财务事实表。
- 状态：代码已完成，正式同步进行中
- 当前结果：已建立 `tushare_financial_raw_version`、三张财务 Standard 表、唯一索引、版本回填和转换任务；三张表各抽样 100 条验证通过。
- 下一步：版本回填、三张 Standard 转换和质量验收已完成，后续进入 PIT 可见性处理。

## T-001｜Tushare 历史 raw 初始化

- 时间：2026-08-02
- 目标：按批次获取近五年行情、估值、复权因子、交易日历和财务原始数据。
- 状态：历史初始化已完成
- 当前结果：行情、估值、复权因子、交易日历和四张财务 Raw 均已入库；每日增量另由 T-005 管理。
- 下一步：阶段一验收已由 T-008 完成，进入 PIT/Feature 开发前需先实现历史状态数据。

## T-002｜Raw 到标准表转换器

- 时间：2026-08-02
- 目标：从 raw 表离线生成标准化数据。
- 状态：代码和抽样验证已完成
- 当前结果：已创建 `data_collect/jobs/tushare_raw_to_standard.py` 和 `data_collect/jobs/tushare_data_quality.py`；抽样 1000 条通过，全量 Standard 已执行。
- 下一步：质量报告和 Raw→Standard 全量重建验收已由 T-008 完成。

## T-003｜三阶段系统开发计划

- 时间：2026-08-02
- 目标：按数据基础层、PIT/Feature 层、Research/Backtest/Portfolio 层推进开发。
- 状态：已完成
- 当前结果：计划已保存至 `dev_doc/development_plan_3_phases.md`，并补充 PIT、可交易状态、公司行为、回测和 AI 边界约束。
- 下一步：完成 T-001/T-002 的全量质量报告和 Raw 重建验收，再进入 PIT。

## T-010｜基线冻结与日常增量改造

- 时间：2026-08-04
- 目标：历史数据验收后建立冻结基线，并将后续行情/阶段二增量改为按日期或事件窗口请求。
- 状态：已完成
- 当前结果：新增 `tushare_data_release`、`tushare_release.py`、`tushare_phase2_incremental.py`；行情接口按日请求，触及接口上限自动按代码分片；分红按公告日期窗口；Raw 继续哈希幂等追加。216 只股票历史复权因子缺口已全部修复，生命周期 Standard NULL 区间重复已清理，Standard/价格 Feature 已重建，最终质量审计通过。
- 验证结果：真实接口 dry-run 验证 `adj_factor` 单日 5549 行；`stock_st` 209 行；`suspend_d` 10 行；`stk_limit` 触发 5800 上限后分片成功；重复真实写入未增加 Raw 行数。
- 下一步：以 `baseline_20260731` 为增量起点，运行正式每日增量并进入 Research/Backtest 前的研究快照设计。

## T-009｜阶段二 PIT 与 Feature

- 时间：2026-08-04
- 目标：补齐历史状态、公司行为、指数 Raw，并建立生命周期、PIT 和 Feature v1。
- 状态：历史基线和日常闭环完成
- 当前结果：阶段二迁移、六类 Raw 同步任务、Standard/PIT/Feature 代码和历史串行后台编排已完成首版；历史事件 Raw 已按 checkpoint 处理，日常增量已改为独立按日/事件窗口任务。
- 关键约束：Raw 只追加、不修改、不删除；历史初始化按原接口粒度断点执行，日常状态类按交易日、分红按公告日期窗口、名称变更低频按日期窗口、指数按代码区间请求；`empty` 响应单独补采；所有派生表允许重建但不得反写 Raw。
- 当前结果：六类阶段二 Raw、历史 Standard/PIT/Feature 和 `baseline_20260803` 已完成验收；空响应按合法状态保留。日常增量后的 Feature 依赖重建、生命周期区间归并、分红业务版本选择、财务版本同步和自动质量门禁均已接通并通过当前库验收。

## T-012｜财务指标版本链路与阶段二最终验收

- 时间：2026-08-04
- 目标：将 `tushare_fina_indicator_raw` 接入版本 Raw、Standard、PIT 和 Feature，完成可审计的财务事实链路。
- 状态：已完成
- 实施范围：新增 `fina_indicator_raw_version` 和 `fina_indicator_standard`；明确 `or_yoy` 为营业收入同比、`tr_yoy` 为营业总收入同比、`q_sales_yoy` 为单季度营业收入同比；保留 `ann_date`、版本号、`payload_hash` 及原始字段。
- 约束：PIT/Feature 停止直接读取 `stock_financial_indicator`；使用 `COALESCE(f_ann_date, ann_date)`，不得伪造 `f_ann_date`；旧 `stock_financial_indicator` 先作为只读兼容表保留。新旧 PIT 差异不要求逐行相同，但必须能由公告日、版本和指标口径解释并审计。
- 当前结果：已创建 `fina_indicator_raw_version`、`fina_indicator_standard`，完成现有 115,646 条 Raw 回填；`or_yoy`、`tr_yoy`、`q_sales_yoy` 已进入 Standard、PIT 和 Feature，财务 PIT/Feature 已按新链路重建，重复写入幂等验证通过。
- 验收结果：Raw→Standard 对账、版本可追溯、`revenue_yoy` 映射、未来函数、重复键和重建幂等全部通过；已创建并验证 `baseline_20260803`，旧 `baseline_20260731` 保留为 superseded 审计记录。
- 下一步：进入 Research/Backtest 前的研究快照设计；仍不自动下单。

## T-013｜阶段二日常持续运行闭环

- 时间：2026-08-05
- 目标：补齐日常增量从数据获取、Raw 版本、Standard/PIT 到 Feature 的完整依赖链，并建立可阻断错误发布的质量门禁。
- 状态：已完成
- 财务增量：`income`、`balancesheet`、`cashflow` 按公告日期窗口增量；`fina_indicator` 按股票、公告日/报告期范围和最近若干季度回补，遵守单次 100 条限制，使用分页、checkpoint 和重试。
- 生命周期：使用 `namechange.start_date/end_date`，对开放 `end_date` 通过下一条 `start_date - 1 天` 推导，生成同一证券同一日期最多一个有效区间。
- 分红：区分预案和实施；按业务键保留版本，只有 `div_proc='实施'` 且 `ex_date <= decision_date` 的有效事件进入股东回报 PIT/Feature。
- 依赖重建：行情增量后重建价格/估值 Feature；分红增量后重建股东回报 Feature；财务增量后重建财务 Standard/PIT/Feature；任务失败不得发布新基线。
- 验收：补充生命周期重叠、分红业务去重、增量后派生同步、质量门禁和阶段二专项测试；通过后才进入 Research/Backtest。
- 已实现：三张财务报表实时写入同步版本层；财务增量任务按接口语义分流，`fina_indicator` 按报告期季度切片并对 100 条上限拒绝截断；生命周期 Standard 已归并为非重叠区间；分红 Standard 按业务键取最新版本，股东回报仅使用“实施”且已除权事件；行情、分红、财务增量均接入派生重建编排；发布校验改为质量门禁先行。
- 当前结果：已用 3 只股票完成四接口真实财务增量验证：`income`、`balancesheet`、`cashflow` 均真实请求并记录 empty checkpoint，`fina_indicator` 真实写入 5 条 Raw/版本记录；随后自动重建财务 Standard/PIT/Feature（财务 PIT 6,198,431 行、公司财务 Feature 115,651 行）。质量门禁仍通过，版本血缘和未来函数检查均为 0。
- 备注：31 条上游“实施但缺少 ex_date”的原始分红记录作为 Standard WARNING 保留，因无除权日不会进入股东回报 Feature，不阻断发布；全量 pytest 574 passed、1 skipped、2 个既有通知前缀测试失败，与本任务无关。
- 下一步：补充正式调度接线，再建立 Research 发布快照；仍不自动下单。

## T-014｜阶段三 Research 快照可复现基础

- 时间：2026-08-05
- 目标：建立 Research 快照 Manifest、输入切片哈希、逐行血缘、一致性生成和数据库冻结机制。
- 状态：已完成基础实现与血缘补强，暂不生成正式快照
- 当前结果：新增 `sql/024_create_research_snapshot.sql`、`sql/025_research_lineage_sources.sql`、`research_snapshot.py` 和专项测试；数据库迁移已应用。事务回滚验证真实执行 build→validate→freeze，5,535 行单日快照、8 张输入统计和 8 个血缘组均通过，回滚后无残留数据；派生来源已写入稳定哈希与 v1 版本，生命周期已补齐 Raw record_id/payload_hash 血缘。
- 测试口径：新增血缘回归测试后，`tests/test_research_snapshot.py` 单独 6 passed；与 `tests/test_phase2_business_rules.py` 合计 12 passed。
- 约束：Research 生成器可读取通过质量门禁的 PIT/Feature；Research/Backtest 只允许读取 `FROZEN` 快照；正式快照待 Research 表和观察池完成后生成。
- 下一步：开发 Research 观察池和证据引用，再生成第一份正式 FROZEN 快照。

## T-015｜阶段三 Research 观察池与证据引用

- 时间：2026-08-05
- 目标：在快照之上建立可解释观察池和 OpenSearch/URL 证据引用，不接入自动交易。
- 状态：已完成最小实现，暂不生成正式快照
- 当前结果：新增 `sql/026_create_research_watchlist.sql`、`sql/027_research_audit_immutability.sql`、`research_watchlist.py` 和观察池专项测试；默认只消费 FROZEN，预览模式显式允许 VALIDATED；规则版本为 `watchlist-v1`，支持价值质量/成长趋势两条轨道、总人数上限、去重和结构化理由；证据表只保存来源定位、内容哈希和正反方角色，不复制正文。
- 审计约束：ACTIVE 观察池只能归档，ACTIVE/ARCHIVED 成员和证据只能追加；应用层和数据库触发器均校验股票属于快照，绑定观察池时必须属于观察池成员。
- 验证：事务内生成并激活 1 个 FROZEN 观察池、写入 1 条证据引用；成员/证据修改删除、观察池删除和快照外股票引用均被阻断，回滚后无残留。
- 下一步：清理并提交全部代码后，重新确认 `baseline_20260803=validated`，生成第一份正式 FROZEN 快照，再创建正式观察池。
