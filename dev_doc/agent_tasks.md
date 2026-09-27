# Agent 任务清单

## T-042｜stock_basic 行业每日观测增量（2026-09-25）

- 状态：代码与本地验证完成；生产库迁移已应用，今日收盘后单次采集已排程，待执行与抽样验收。
- 设计：复用每日股票基础资料任务的同一次 `stock_basic` 响应；通过独立 `IndustryMembershipRepository` 写入 `pit.industry_membership_daily`。只记录交易日收盘后的供应商当前行业名称，证据状态为 `DAILY_CAPTURED`/`UNKNOWN`，不伪造行业代码、不回填历史、不升级为有效 PIT。
- 安全：单日期、最多 10,000 行、日期级 advisory lock；重复内容幂等；中断后按股票代码校验已有子集并只追加缺失行，已有内容不一致则失败关闭；行业日表 Policy 仅允许批量追加。
- 迁移：`sql/107_tushare_industry_daily_capture.sql` 已于 2026-09-25 应用到 `ashares` 生产库；验证 `industry_code` 已可空，`ashares_app` 有 SELECT/INSERT 权限。迁移由数据库管理员账号执行，程序仍使用 `ashares_app`。
- 验证：行业仓储/任务与 Kernel、Repository、写安全及 Pipeline 回归共 `234 passed`；Data Access Contract 扫描 236 个文件、0 违规；修改文件编译和 `git diff --check` 通过。尚未调用 Tushare 或写行业观测数据；单次任务 `AsharesIndustryCaptureOnce_20260925` 计划上海时间 18:05 执行，日志为 `logs/industry_capture_20260925.log`，届时再抽样验收。

## T-041｜Research v1.3 历史快照批量物化（2026-08-08）

- 状态：进行中，后台断点执行。
- 范围：`2021-08-02` 至 `2026-08-06` 共 1,215 个 SSE 开市日；已完成 2 个烟测快照，正式范围当前已创建 1,213 个 Manifest。
- 实现：`research_snapshot_series.py` 使用每批 10 日的集合 SQL，批量写入 `stock_snapshot_daily`、`snapshot_lineage` 和 `snapshot_input`；baseline/质量报告在批次内复用；Raw 不写入。
- 当前进度：约 60 个快照处于 `BUILDING`，其余待处理日期为 `DRAFT/FAILED`；本轮先完成 `build → validate`，不绕过 dirty Git 冻结门禁。
- 续跑：重复执行同一命令会跳过 `VALIDATED/FROZEN`，按日期继续；异常中断的 `BUILDING` 记录需先确认无运行进程后转为 `FAILED` 再续跑。
- 下一步：完成全部日期校验，提交干净 Git 后统一执行 `research-v1.3` FROZEN 门禁。

## T-040｜基线门禁与财务派生重建收口（2026-08-08）

- 状态：已完成。
- 修复：重建财务 Raw→版本→Standard→PIT→Feature 链路；Standard 沿用 Raw `business_key` 作为血缘键；门禁按唯一 `payload_hash` 检查重复源键；发布报告支持 PostgreSQL `Decimal` 序列化。
- 结果：`fina_indicator`、income、balancesheet、cashflow 的版本元数据缺失为 0；PIT 财务 6,211,407 条；公司财务 Feature 116,492 条；估值 Feature 6,291,067 条。
- 基线：`baseline_20260806` 已通过门禁并为 `validated`，含 `source_manifest_hash`；`baseline_20260803` 自动变为 `superseded`。Raw 未修改。
- 验证：全量 `pytest` 为 `699 passed, 1 skipped, 5 deselected`；`compileall`、`git diff --check` 通过。
- 下一步：基于该 validated baseline 生成完整 `research-v1.3` FROZEN Snapshot 系列；完成前不进入正式五年回测。

## T-039｜时点复权价格 Feature v2（2026-08-08）

- 状态：代码、迁移和生产派生重建已完成。
- 设计：不修改既有 `adj_factor_daily`/`price_daily_v1`；仅使用 `std.dividend_event` 中“实施”且 `ex_date <= trade_date` 的现金分红与送转事件，构造 `feature.price_adjustment_factor_asof` 和时点总回报坐标。
- 接入：Research 默认版本升级为 `research-v1.3`，Snapshot/Backtest/候选历史读取 `price_daily_v2`；`price_daily_v1` 只读保留兼容。
- 迁移：`sql/042_create_point_in_time_price_feature.sql`、`sql/043_research_v13_point_in_time_price.sql`；Raw、旧 Standard/PIT/Feature 不被删除或覆盖。
- 约束：原始 OHLC 仍是成交/止损坐标，v2 仅用于指标；配股仍不模拟，未来公司行为不得进入历史指标。
- 后续：重建完成后抽样核对除权日前后连续性、公司行为事件映射、行数和质量状态，再发布带完整 Source Manifest 的新 baseline 并生成 `research-v1.3` 快照系列。
- 实测：`price_adjustment_factor_asof=6,308,213`、`price_daily_v2=6,308,213`，覆盖 `2021-08-02` 至 `2026-08-06`；截止 `2026-08-03` 与 `daily_kline` 均为 `6,291,619` 行，`MISSING_SOURCE=0`。`WARNING` 仅为上市初期不足 60 个观察日的预热行。
- 验证：全量测试 `699 passed, 1 skipped, 5 deselected`，compileall 和 `git diff --check` 通过；PostgreSQL 正常且未 OOM。下一步为只读抽样、发布新 baseline 和生成 v1.3 Snapshot。

## T-038｜历史初始化与 Research 时点安全修复（2026-08-08）

- 状态：历史时点门禁修复已由 T-039 的 `price_daily_v2` 方案替代；等待 v2 派生重建后生成正式 `research-v1.3` 快照。
- 历史初始化：`daily`/`daily_basic` 单交易日响应达到 Tushare 上限时，自动按股票代码分片重试；新 checkpoint 保存 Raw 版本行数，空响应不标记完成，避免静默截断。
- 历史股票状态：Research Snapshot 使用 `pit.universe_daily` 和 `std.security_lifecycle` 的有效区间，不再把当前 `stock_basic` 名称/市场/上市状态倒灌到历史日期；Raw stock_basic 仅作基准内身份与日期字段来源。
- 复权时点门禁：Snapshot 的 create/build/validate/freeze 均检查截止日前 365 个日历日技术窗口内，`adj_factor_daily` 存在且 `fetched_at <= research_cutoff_date`。缺失或未来获取的因子直接 fail-closed，不修改既有 FROZEN 快照。
- 验证：`compileall` 通过；Research Snapshot 与系列专项测试 `11 passed`；数据库只读核验显示 `2026-08-03` 当前因子仍有晚于截止日获取的历史行，故正式历史快照会正确阻断，`2021-08-02` 同样阻断。
- 后续：完成 v2 派生表验收，发布带完整 Source Manifest 的新 validated baseline，再生成 `research-v1.3` 快照系列。

## T-037 follow-up — 2026-08-08 audit closure

### Production data audit — 2026-08-08

- Code audit result: no new P0/P1 execution defect found after the reserve-cash, settled-cash, Snapshot input-lock, and `041` open-time contract repairs; `693 passed, 1 skipped, 5 deselected`.
- Production data result: current code requires `research-v1.2` Snapshots and v2 Dataset/adapter/engine contracts, but the database has no `research-v1.2` FROZEN Snapshot. The existing `research-v1.1` series is incomplete: 690 FROZEN dates, one DRAFT date, and 522 missing SSE open dates in `2021-08-02`–`2026-08-03`; its frozen rows are legacy dirty records without baseline quality hashes and must not be silently reused.
- Existing `dataset-v1`/`dataset-v1.1` and legacy v1 Runs remain immutable smoke/history records. A new clean-code `research-v1.2` Snapshot series and a new v2 Dataset/Run are required before formal five-year results.
- Background Raw initialization remains active and must not be interrupted; this audit did not write or delete Raw data.
- Additional gate: `baseline_20260803` is `validated` but has an empty persisted `source_manifest_hash/source_manifest`; the current Snapshot code correctly fails closed before creating v1.2. Publish a new complete baseline after Raw/derived work finishes before starting the new Snapshot series.

- Status: code fixes completed; 59 focused tests and 693 full tests pass (`1 skipped`, `5 deselected`).
- Fixed: reserve cash is an absolute equity floor in Backtest, candidate generation, DB Portfolio fills, and manual Portfolio simulation.
- Fixed: candidate sizing now uses settled `available_date` cash while preserving total cash for equity/risk.
- Fixed: Research Snapshot build locks the quality/materialization input boundary before quality reads.
- Fixed: Dataset/Run schema uses `backtest-dataset-v2`, `research-to-backtest-v2`, and `daily-next-open-v2`; migration `sql/041_backtest_open_time_contract.sql` is applied.
- Boundary: Portfolio remains manual `PENDING_APPROVAL -> PAPER_MANUAL`; corporate actions are handled by Backtest, while Portfolio valuation consumes explicitly bound frozen daily snapshots and does not create broker orders.
- Background: the long historical Raw initializer remains running with checkpointed `adj_factor` progress; do not interrupt it.

## T-037｜正式回测逻辑审计修复

- 时间：2026-08-08
- 目标：修复样本成熟度、撮合时序、涨跌停时点、每日候选冻结、基本面策略、Raw-first 调度、财务版本和代码归档门禁。
- 状态：进行中
- 当前结果：已完成代码、数据库和测试审计；旧 `research-v1.1` 快照只读保留，不作为修复后的正式五年基线。
- 下一步：完成 `research-v1.2` 与各项门禁代码，专项验证后后台回补 warm-up 数据并重建新快照。

## T-036｜阶段三开发完成与正式历史回测生产准备（2026-08-07）

- 状态：阶段三代码开发完成；正式历史回测生产门禁待执行。
- 已确认：Backtest Dataset/Run、撮合、风险、公司行为、PIT/快照边界、Portfolio 人工 Air-Gap、数据库审计约束和 `actual_shares`/订单哈希收口均已实现。
- 验证：当前环境全量 `pytest` 为 `652 passed, 1 skipped, 5 deselected`；`compileall`、`git diff --check` 通过；033 迁移可幂等重放；生产库 Dataset/Run/Portfolio/规则均为 0 行；Raw 未修改。
- 当前边界：可以进入正式历史回测生产环节，但尚未生成生产数据。自动实盘交易仍不支持，Backtest 只能进入 `BACKTEST/PENDING_APPROVAL`，成交只能是人工批准的 `PAPER_MANUAL`。
- 生产顺序：规则审批并入库 → 干净 Git commit → 多日期 `research-v1.1` FROZEN Snapshot → Dataset `build→validate→freeze` → 创建正式 Run → 结果、现金、持仓和审计哈希对账。
- 完工流程文档：`dev_doc/project_completion_runbook.md`，以后按该手册执行生产操作和验收。
- 不再继续扩展阶段三核心代码；后续发现问题按小版本修复，并在生产门禁中记录证据。

## T-035 阶段三生产增强版（2026-08-06）

- 状态：代码与迁移完成，专项测试通过；正式多日期回测门禁待按生产流程执行。
- 完成：风险压力仓位字段、成交量/成交额标准单位和整笔流动性拒单、Monte Carlo bootstrap/排列法及哈希、Backtest→Portfolio `BACKTEST/PENDING_APPROVAL` 人工隔离。
- 补强：Dataset 对非停牌异常成交量/成交额和无效行情失败；规则参数进入冻结 MarketRule；期末假设清仓使用独立 `END_OF_DATA_LIQUIDATION` 审计 Metric，不伪造 Fill。
- 迁移：`sql/032_backtest_portfolio_airgap.sql`；Raw 表只读，已核对 Raw 行数未改变。
- 入口：`data_collect/jobs/portfolio_simulation.py::import_backtest_order`。该入口只创建待审批意图，不批准、不成交、不访问券商。
- 验收：新增风险、流动性、Monte Carlo、人工隔离专项测试通过；正式回测前仍需 clean Git、Snapshot series、Dataset freeze 和正式 Run 验收。
- 测试文件：`tests/test_risk_gap_model.py`、`tests/test_liquidity_rules.py`、`tests/test_monte_carlo.py`、`tests/test_portfolio_airgap.py`；规则候选文件已加入参与率和整笔拒单策略，但仍保持 `REVIEW_REQUIRED`，未写入生产规则表。
- 最终验收：全量 `pytest` 为 `650 passed, 1 skipped, 5 deselected`；`compileall` 与 `git diff --check` 通过；032 迁移重复执行和事务回滚演练通过，Raw 核心表行数未改变。

## T-034｜阶段三正式版回测审计修复

- 时间：2026-08-06
- 目标：按正式回测审计结论封口送转股、除权止损、退市、组合排序、数据版本、覆盖门禁、压力测试和 Run/Dataset 血缘。
- 状态：代码修复与专项验证完成；正式规则、快照系列和生产回测仍待业务门禁。
- 当前结果：`DividendEvent` 以 `stk_div` 合计为唯一送转率并校验分项；除权日调整原始止损坐标、送转成本和待成交订单；退市持仓无真实退出时 fail-closed；同日 BUY 按 `signal_score DESC, ts_code ASC` 分配，T+1 开盘按实际价格重算股数且 `actual_shares != planned_shares` 才标记 resize；Dataset 只选择一个完整 Raw payload，不跨版本按字段拼接；保存并绑定 validated baseline 的 `baseline_quality_report_hash`，该哈希进入 Dataset `input_manifest_hash`，基线变化会阻断后续 build/validate/freeze；无逐日覆盖门禁时使用该基线报告作为等价覆盖；Monte Carlo 保存方法、参数和交易序列哈希；Run 数据库触发器强制输入哈希及市场/费用版本与 Dataset 一致。送转零碎股 v1 按整数股向下取整，不产生现金补偿，真实券商政策需新规则版本。
- 文件：`data_collect/backtest/models.py`、`engine.py`、`risk.py`、`data_collect/jobs/backtest_dataset.py`、`backtest_run.py`、`sql/028_create_backtest_model.sql`、`sql/031_phase3_final_audit_contract.sql`、`dev_doc/backtest_design.md`、`dev_doc/current_data_model.md`、`dev_doc/development_plan_3_phases.md`。
- 验证：阶段三最终专项 `47 passed`；全量 `640 passed, 1 skipped, 5 deselected`，仍有 2 个既有通知前缀测试失败；`compileall`、`git diff --check`、028/029/030/031 迁移重放和 Run/Dataset 哈希触发器烟测通过；生产 Dataset/Run/Portfolio/门禁仍为 0 行，Raw 行数未变。
- 约束：当前工作区仍需后续提交为 clean commit；规则 YAML 仍为 `REVIEW_REQUIRED`，正式回测必须先导入已批准的有效日期规则、生成多日期 `research-v1.1` FROZEN 快照并完成 Dataset `build→validate→freeze`。
- 文档同步：已将 Dataset 基线质量哈希绑定、T+1 实际股数 resize 审计标记和送转零碎股 v1 向下取整规则同步到数据模型、回测设计和三阶段开发计划。

## T-032｜阶段三 Gemini 设计吸收与正式回测开发基线

- 时间：2026-08-06
- 目标：将 Gemini 阶段三建议收敛为本项目可复现的 Dataset、撮合、账本、组合和运行门禁契约。
- 状态：代码与契约已完成，正式生产门禁待业务确认
- 当前结果：已落地 Dataset/Run/Portfolio 模型、执行行情与公司行为物化、warm-up/evaluation 区间、数据可见性与版本血缘、T 收盘→T+1 开盘撮合、有效日期市场/费用规则、不可变事件账本、人工确认的 `PAPER_MANUAL` 组合及阶段三专项测试；Dataset 的 `VALIDATED→FROZEN` 只允许代码血缘字段变化，规则导入同时拒绝数据库既有重叠区间；并修复 `028/029/030` 迁移中注释与 SQL 粘连导致的可重放语法问题。Portfolio 现金账本新增组合行锁和唯一 `INIT` 索引，避免并发初始化/现金检查穿透；生产库尚未写入正式规则、Dataset、Run 或 Portfolio。
- 验证：阶段三定向测试在 warm-up 全区间校验、运行前代码预检、执行来源缺失门禁、账本约束和失败后 BLOCKED 门禁补强后为 59 项通过；`028`/`029`/`030` 迁移重放和 PostgreSQL 回滚烟测通过；候选规则默认只校验，`--confirm` 在 `REVIEW_REQUIRED` 状态下被拒绝；生产 Dataset/Run/Portfolio/规则/门禁均为 0 行，Raw 行数未变化。
- 当前门禁：候选规则文件 `dev_doc/backtest_rules_candidate.yaml` 仍为 `REVIEW_REQUIRED`，不得使用 `--confirm`；正式回测还需要多个 `research-v1.1` FROZEN 快照和干净 Git 版本。第一版不模拟配股、部分成交、ATR/Trailing Stop，也不接入自动下单。
- 下一步：确认规则费率/滑点和正式回测日期范围；审核通过后导入规则，生成完整快照系列，执行 Dataset `build→validate→freeze`，再联调 Run 与 Portfolio。执行行情的 vintage 口径固定为构建事务内按 `fetched_at DESC, record_id DESC` 选择的 Raw 版本，并在 Dataset 中保存版本/哈希。

### T-033｜阶段三数据来源与状态约束收口

- 时间：2026-08-06
- 目标：封口执行行情来源缺失、历史 Raw 版本兼容、Dataset 基线复核和 Run/Portfolio 数据库状态约束。
- 状态：已完成
- 当前结果：Dataset 物化对 `daily_kline`、`std.security_status_daily` 和 `pit.security_tradeability_daily` 任一缺失均标记 `MISSING_SOURCE`，禁止静默生成可交易行；历史没有 `tushare_market_raw_version` 的日线允许版本号为空，但必须由选中 Raw payload 生成确定性哈希。build/validate/freeze 均重新确认基线仍为 `validated`；验证失败持久化为 FAILED；Run 子表仅在 RUNNING 可写；Portfolio 数据库增加决定日/快照截止日精确匹配、审批人、费用、现金余额和持仓批次约束；公司行为现金腿与送转腿可分开结算。
- 验证：阶段三定向测试 59 项通过；`compileall`、`git diff --check`、028/029/030 迁移重放及 PostgreSQL 回滚烟测通过；规则区间排他约束回滚烟测通过；生产 Dataset/Run/Portfolio/规则/门禁仍为 0 行，Raw 未修改。
- 补充：Portfolio 数据库同时要求决定日、计划执行日、成交日、现金账本日期和持仓批次日期均为 `trading_calendar` 中的 SSE 开市日；无交易日历记录时拒绝写入。
- 补充：Research `freeze` 也会在同一数据库会话中重新确认 validated 基线，并复核质量报告哈希；validate 后基线或质量门禁变化时不得冻结。
- 补充：Dataset 执行 OHLC/成交量/成交额现在直接解析构建事务选定的 `tushare_market_raw_version.payload`，历史无版本时才回退 `tushare_daily_raw.payload`；不再从可变 `daily_kline` 取执行价格。
- 补充：Portfolio 现金账本插入会锁定账户 Manifest，缺少账户时拒绝写入；唯一部分索引保证每个组合只有一条 `INIT` 记录，成交现金检查也锁定账户行。
- 补充：行情覆盖率 PASS 只表示接口响应达到历史基准；单日 Raw/Standard/checkpoint 或全局派生重建失败时追加最新 `BLOCKED` 门禁，禁止旧 PASS 让 Research freeze 放行。
- 补充：Research Snapshot 与 Backtest Dataset 的 freeze 会先 `FOR UPDATE` 锁定 Manifest，并要求 `UPDATE ... WHERE status='VALIDATED'` 命中一行，避免并发冻结竞态。
- 补充：Backtest Dataset build/validate 会检查 warm-up 到 evaluation_end 区间内已有的最新覆盖门禁；历史无门禁记录不强行补造，但任何已记录的非 PASS 都会阻断物化和验收。
- 下一步：等待规则费率/滑点与正式回测日期范围确认；随后生成多日期 `research-v1.1` FROZEN 快照并执行 Dataset `build→validate→freeze`。在此之前不导入候选规则、不创建正式 Dataset/Run/Portfolio。

## T-018：阶段三 Backtest Dataset/Run 与日线引擎实现
- 时间：2026-08-05
- 目标：把已冻结的阶段三契约落实为可复现的 Dataset、Run、规则快照、事件账本和纯 Python 日线回测基础。
- 状态：基础实现完成，正式 Dataset/Run 待门禁
- 当前结果：已新增并幂等应用 `sql/028_create_backtest_model.sql`；实现 `backtest_dataset.py`、`backtest_run.py` 和 `data_collect/backtest/`。已覆盖 FROZEN Snapshot 输入、执行 OHLC/涨跌停/可交易性、日历/公司行为/规则冻结、T 收盘→T+1 开盘、T+1 可卖、费用、风险预算、资金结算、确定性事件 ID、不可变状态和结果持久化；专项回归已通过。
- 当前门禁：交易日历已覆盖到 2026-08-03；`backtest.market_rule_effective` 仍未配置，且只有一个日 FROZEN Research Snapshot，不能支撑完整历史回测。代码按 fail-closed 处理，不补造日期、不回退当前/合成规则、不修改 Raw。
- 下一步：确认显式市场/费用规则并准备多日期 FROZEN 快照后，创建小范围 DRAFT Dataset，执行 build→validate→freeze，再联调首个 Backtest Run。

## T-017｜阶段三 Backtest 输入、撮合与账本契约

- 时间：2026-08-05
- 目标：吸收归档参考样例 `dev_doc/过时/gemini-doc/` 的有效设计，建立基于多日期 FROZEN 快照的可复现 Backtest Dataset、Run Manifest、日线撮合和不可变账本契约。
- 状态：已由 T-018 接续完成（历史记录）
- 当前结果：已确认 Research 快照研究链路基本完成，但正式回测仍缺少执行行情物料、公司行为冻结、warm-up/evaluation 区间、数据 vintage、市场规则/费用版本、撮合状态机和 Run Manifest。设计已保存至 `dev_doc/backtest_design.md`；Gemini 示例代码暂不直接接入生产。
- 关键边界：Backtest 不读取实时 PIT/Feature，不使用当前 3 只观察股替代历史股票池，不在生产模式回退合成数据；复权价格只用于指标，原始价格用于成交。
- 默认方案：T 收盘出信号、T+1 开盘尝试成交、只做多、不模拟部分成交、第一版暂不模拟配股；若无异议按此实现。
- 下一步：先完成 Dataset/Run Manifest 及执行输入物料的 DDL 和测试契约，再实现 Frozen Dataset Materializer、Input Adapter 和纯 Python 撮合引擎。

更新时间：2026-08-05

## T-016：Research 外部证据候选检索最小闭环
- 时间：2026-08-05
- 目标：按 ACTIVE 观察池成员与快照截止日，只读检索 OpenSearch 新闻/公告候选。
- 状态：已完成
- 当前结果：新增 `data_collect/jobs/research_evidence_candidates.py`，返回 `document_index/document_id/url` 等元数据并标记 `REVIEW_REQUIRED`，不自动写入证据表；`news_search` 默认输出保持兼容，候选模式可显式返回 OpenSearch 文档身份。专项回归 47 passed，真实联调未命中三只成员的股票标签，名称搜索结果不能直接当作证据。
- 下一步：人工核验后用 `add_evidence` 绑定 supporting/contrary/context；正式回测仍暂缓。

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
- 状态：已完成，正式 FROZEN 快照已生成
- 当前结果：新增 `sql/024_create_research_snapshot.sql`、`sql/025_research_lineage_sources.sql`、`research_snapshot.py` 和专项测试；正式快照 `9afb9a08-da75-439b-8b4a-fcc688df3787` 已基于干净 commit `658a8c8` 完成 build→validate→freeze，5,535 行单日快照、8 张输入统计和 44,280 条血缘记录通过校验。
- 测试口径：新增血缘及冻结前质量/基线复核测试后，`tests/test_research_snapshot.py` 单独 7 passed；与 `tests/test_phase2_business_rules.py` 合计 13 passed。
- 约束：Research 生成器可读取通过质量门禁的 PIT/Feature；Research/Backtest 只允许读取 `FROZEN` 快照；正式快照已冻结，后续不得更新或删除其子表。
- 下一步：继续补充真实 OpenSearch/公告/研报证据引用，再进入 Research 人工复核；暂不进入正式 Backtest。

## T-015｜阶段三 Research 观察池与证据引用

- 时间：2026-08-05
- 目标：在快照之上建立可解释观察池和 OpenSearch/URL 证据引用，不接入自动交易。
- 状态：已完成，正式观察池已创建并激活
- 当前结果：新增 `sql/026_create_research_watchlist.sql`、`sql/027_research_audit_immutability.sql`、`research_watchlist.py` 和观察池专项测试；默认只消费 FROZEN，预览模式显式允许 VALIDATED；规则版本为 `watchlist-v1`，支持价值质量/成长趋势两条轨道、总人数上限、去重和结构化理由；证据表只保存来源定位、内容哈希和正反方角色，不复制正文。
- 审计约束：ACTIVE 观察池只能归档；ACTIVE 成员和证据只能追加；ARCHIVED 观察池、成员和证据全部不可变；应用层和数据库触发器均校验股票属于快照，绑定观察池时必须属于观察池成员。
- 验证：正式观察池 `01b53411-155e-4672-9548-189396e7a3e0` 已基于 FROZEN 快照激活，当前 3 个 `value_quality` 成员；已写入 9 条 PostgreSQL 快照事实引用，重复执行幂等；外部 OpenSearch/公告/研报证据待真实文档绑定。
- 下一步：为 3 个成员补充真实外部证据和反方证据，完成 Research 人工复核后再设计 Backtest 输入。
# 当前任务状态覆盖（2026-08-05）

- T-017 的“下一步先完成 DDL/引擎”属于历史描述；现已由 T-018 完成。
- 当前阶段三状态以 T-018 和 `dev_doc/backtest_design.md` 为准：基础代码、迁移和专项测试已完成；正式 Dataset/Run 仍等待交易日历完整覆盖及显式市场/费用规则配置。

## T-019：阶段三 Portfolio 人工模拟组合
- 时间：2026-08-06
- 目标：建立不连接券商的人工批准模拟组合，保留成交、T+1 持仓、现金、估值和审计事件。
- 状态：代码与契约已完成；生产实例待 T-027
- 当前结果：已新增 `sql/029_create_portfolio_model.sql`、`data_collect/portfolio/`、`data_collect/jobs/portfolio_simulation.py` 和 `dev_doc/portfolio_design.md`；已覆盖 FROZEN Snapshot/ACTIVE Watchlist 门禁、PAPER_MANUAL、整手、预留现金、现金结算天数、禁止已有持仓加仓、T+1 和追加式账本。
- 当前门禁：数据库中尚未创建正式 Portfolio；正式使用前仍需干净代码版本，并且所有成交必须人工批准。
- 下一步：生产使用仍等待多日期 `research-v1.1` FROZEN 快照、干净代码版本和正式规则；隔离数据库的组合成交/估值联调已完成。

## T-020：阶段三时间截面与样本外门禁
- 时间：2026-08-06
- 目标：阻断 Research/PIT 财务可见日期穿越，并提供回测 Dataset 的 cutoff 隐藏未来数据验证。
- 状态：代码与契约已完成；多日期数据待 T-027
- 当前结果：Dataset validate 已检查 Snapshot 截止日与研究日一致、`effective_announce_date <= research_date`；新增 `data_collect/backtest/validation.py` 和专项测试。
- 当前门禁：历史回测仍需要多个日期的 FROZEN Research Snapshot；当前单一 2026-08-03 快照不能支撑完整历史回测。

## T-021：阶段三回测契约加固与组合联调（2026-08-06）

- 状态：代码与契约已完成；生产门禁待 T-027
- 已完成：Dataset Manifest 增加切片谓词、字段集合、哈希算法、可见性规则；Run Manifest 增加参数哈希；止损优先于同日策略卖出；Frozen Dataset 缺规则时 fail-closed；公司行为增加公告/送转到账日期与可见性校验；Portfolio Python/数据库两层补齐 T+1 日期约束；Portfolio 账户 Manifest 持久化 `cash_settlement_days`，延迟结算时拒绝缺失或同日可用日期；Portfolio 意图校验快照/观察池股票归属，估值校验 FROZEN 原始收盘价。
- 验证：Backtest/Portfolio 专项测试已通过；`028`、`029` 迁移可重复执行；Portfolio 正向/负向数据库烟测均回滚，相关表保持 0 行；Raw 未修改；新增账户现金结算字段后的迁移和烟测已重跑通过。
- 当前门禁：`trading_calendar` 已覆盖到 2026-08-03，但 `backtest.market_rule_effective` 仍无显式规则，正式 Dataset/Run 仍未创建；只有一个日 FROZEN Research Snapshot，尚不能做完整历史回测。
- 规则边界：第一版使用固定信号止损，不宣称已实现 ATR/Trailing Stop；配股暂不模拟；不自动下单。
- 下一步：确认并录入按生效日期的市场/费用规则，准备多日期 FROZEN Snapshot 生成与 Dataset build→validate→freeze；最小 fixture 的 Run 和 Portfolio 账本联调已完成。

## T-023：阶段三可复现与估值不可变门禁

- 时间：2026-08-06
- 目标：收口快照重跑、组合估值覆盖以及未来成交回补历史估值的可复现风险。
- 状态：已完成
- 当前结果：快照逻辑键 `(research_cutoff_date, data_baseline_tag, schema_version)` 唯一；系列任务可复用 DRAFT/FAILED，保留 VALIDATED/FROZEN；正式冻结只接受干净 Git 或存在的代码归档。Portfolio 估值保存 FROZEN Snapshot 和现金/持仓/价格输入哈希，同输入重跑幂等，冲突输入及数据库更新/删除均拒绝，且禁止在更晚成交后回补过去估值。
- 验证（中间结果）：迁移幂等、PostgreSQL 估值不可变烟测和隔离 Dataset→Run 联调通过；阶段三专项测试最终由 T-026 更新为 39 项。
- 下一步：待录入有效日期市场/费用规则并生成多日期 `research-v1.1` FROZEN 快照后，执行正式 Dataset→Run 联调；隔离联调已先行通过。

## T-022：阶段三多日期快照与 Dataset 血缘补强

- 时间：2026-08-06
- 目标：把多日期 FROZEN Research Snapshot 编排和 Dataset 成员级公司行为血缘做成可复现、可验收的代码契约。
- 状态：代码与契约已完成；正式快照待 T-027
- 当前结果：新增 `data_collect/jobs/research_snapshot_series.py`，只接受明确日期或标准化交易日历范围，按单日 Snapshot 的 create→build→validate→freeze 顺序执行，默认最多 50 个并要求显式取消上限；Research 升级为 `research-v1.1` 并接通 `eligible_by_market`；`dataset_member` 已写入按 `ex_date` 的 `corporate_action_hash`，并纳入 Dataset 输入 Manifest 哈希；Portfolio 意图和估值门禁已补齐。纯 Python/静态专项测试通过，数据库迁移和 Portfolio 现金结算/观察池成员烟测通过且全部回滚。
- 当前门禁：尚未生成新的 `research-v1.1` 正式历史 FROZEN 快照；旧 `9afb...` 快照不作为正式 Dataset 输入；`backtest.market_rule_effective` 仍为空，不能创建正式 Dataset/Run；当前工作区仍需干净 commit 才能正式冻结。
- 下一步：先确定并录入真实有效日期的市场/费用规则，再选择历史快照日期执行小范围 DRAFT→VALIDATED 验证；通过后再在干净代码版本上冻结 Dataset 并联调 Run/Portfolio 账本。

## T-024｜阶段三物化哈希与估值不可变门禁收口

- 时间：2026-08-06
- 目标：确保 Dataset 校验对账最终物化子表，并让 Portfolio 估值输入可幂等、可追溯且不可覆盖。
- 状态：已完成
- 当前结果：`backtest_dataset.py` 现在重算物化日历、规则、执行行情和公司行为的全局/按日哈希，并与 Manifest、`dataset_member` 对账；修正 Portfolio 估值日期门禁位置，估值保存 Snapshot/现金/持仓/价格哈希，同输入幂等，冲突、更新、删除和更晚成交后的历史回补均拒绝。
- 验证（中间结果）：`compileall`、024/028/029 迁移可重复执行、PostgreSQL 回滚烟测和临时隔离数据库全链路联调通过；阶段三定向测试最终由 T-026 更新为 39 项，生产正式 Dataset、Portfolio 和 Raw 均无新增或修改。
- 下一步：录入经确认的有效日期市场/费用规则，生成多个 `research-v1.1` FROZEN 快照后，执行正式数据集联调；全量测试仍有 2 个既有通知前缀失败，非阶段三逻辑。

## T-025｜阶段三结算边界与迁移契约收口

- 时间：2026-08-06
- 状态：已完成
- 当前结果：回测卖出发生在 Dataset 最后一天且现金结算需要未来交易日时改为 fail-closed，不再错误地把未结算资金当日释放；补充回归测试。分红 Standard 基础 DDL 已声明 `business_key` 和 `payload_hash`，与版本转换及 Dataset 公司行为血缘查询保持一致。
- 验证（中间结果）：阶段三定向测试当时为 37 项；最终由 T-026 更新为 39 项。生产库只读计数确认阶段三正式 Dataset/Run/Portfolio 仍为 0，Raw 行数未变化。

## T-026｜阶段三物化门禁、规则版本与 Portfolio 数据库约束收口

- 时间：2026-08-06
- 目标：把 Gemini 阶段三设计中的时间截面、输入物化、市场/费用版本、公司行为去重和人工组合约束落实到代码与数据库。
- 状态：已完成
- 当前结果：快照系列只接受开市日；Dataset 校验基线、截止日和开市日，按 `market_rule_version` 与 `fee_rule_version` 选择有效规则；公司行为按业务键取有效版本并检查重复；缺失成交量动量按 fail-closed 处理；Portfolio 数据库触发器补齐成交股数、T+1、现金、价位、止损、快照截止日和已有持仓等门禁。修复 `028`/`029` 重复 SQL，使迁移可安全重放。
- 验证：阶段三定向测试 39 项通过；`compileall`、`git diff --check` 和 `028`/`029` 幂等迁移通过；生产只读核对 `tushare_daily_raw=6,291,619`、`tushare_daily_basic_raw=6,274,473`、`tushare_adj_factor_raw=6,360,864`，Dataset/Run/Portfolio 均为 0，Raw 未执行写入。
- 下一步：确认并录入按生效日期的市场/费用规则，生成多个 `research-v1.1` FROZEN 快照后，再创建正式 Dataset 并运行第一份回测；不伪造规则、不自动下单。

## T-027｜阶段三正式回测生产门禁

- 时间：2026-08-06
- 目标：完成多日期 FROZEN 快照、规则数据集和正式 Dataset→Run→Portfolio 验收。
- 状态：待开始
- 当前结果：代码和隔离数据库全链路已具备，但生产库尚无正式 Dataset/Run/Portfolio；现有唯一正式快照为旧 `research-v1`，不能静默升级为 `research-v1.1`。
- 下一步：先确定市场板块/ST/新股及费用规则的有效日期和版本，再小范围生成并验证快照系列，随后在干净 Git commit 上执行 Dataset `build→validate→freeze`。

## T-028｜阶段三运行门禁、PIT 校验与撮合边界补强

- 时间：2026-08-06
- 目标：吸收 Gemini MVP 中可落地的覆盖率熔断、PIT 物理截面检查和风险适配要求，封住回测引擎的重复买单与数据边界漏洞。
- 状态：代码完成；正式生产门禁仍待 T-027
- 已完成：新增 `sql/030_create_phase3_ops_gate.sql` 和 `ops.market_data_quality_gate` 追加式门禁；按日全市场增量在 Raw 写入前检查历史覆盖率，低于 1% 阈值、无基线或抽样结果均不可作为正式发布；Research 快照冻结检查最新门禁结果。新增 `data_collect/jobs/phase3_validation.py` 的 PIT 只读校验，检查财务公告日、Feature/Tradeability `source_asof_date` 和 Research 快照截止日；新增 `data_collect/jobs/backtest_rules.py`，规则 YAML/JSON 先校验有效区间、有限数值和 `parameters.provenance`，再显式 `--confirm` 只追加入库，数据库禁止修改/删除规则。新增市场温度风险倍率纯适配器，但未隐式接入第一版回测。撮合层新增执行时重复 BUY/最大持仓二次检查、正式 Dataset T+1 边界拒绝、分红支付/上市日期顺序校验；物化遇到涨跌停空值、非有限值、非正值或 100000 哨兵时保留价格但标记 WARNING 并关闭买卖，退市/不可交易行不伪造限制价。
- 验证：阶段三定向测试 52 项通过；全量实测为 `628 passed, 1 skipped, 5 deselected`，另有 2 个既有通知前缀测试失败；PIT 只读校验（`2026-08-03`）5 项均为 0；`compileall`、`git diff --check` 和 `030` 迁移执行通过。生产 Raw 行数仍为 `daily=6,291,619`、`daily_basic=6,274,473`、`adj_factor=6,360,864`，门禁表当前 0 行，未写入模拟门禁记录。
- 剩余门禁：仍需用户/业务确认并录入按有效日期的市场板块、ST/新股、费用、滑点和资金结算规则；生成多个 `research-v1.1` FROZEN 快照；在干净 Git commit 上构建正式 Dataset/Run。第一版仍不接自动下单，不宣称 ATR/Trailing Stop 或配股已实现。

## T-029｜Dataset 全区间 Snapshot 覆盖与候选发布不可变性

- 时间：2026-08-06
- 目标：避免回测 Dataset 存在中间交易日缺少 Research 输入，或验收后被修改再冻结。
- 状态：已完成
- 当前结果：`create_dataset()` 现在要求 `warmup_start` 至 `evaluation_end` 每个 SSE 开市日均绑定一个 FROZEN `research-v1.1` Snapshot；`VALIDATED` Dataset 及其成员、执行行情、日历、公司行为和规则子表均禁止修改，只有 `VALIDATED → FROZEN` 发布状态及代码元数据更新允许发生。新增 `missing_snapshot_dates()` 和状态锁定回归测试。
- 验证：阶段三定向测试 51 项通过；全量实测 `627 passed, 1 skipped, 5 deselected`，2 个既有通知前缀测试失败；`028/030` 迁移重放成功，VALIDATED 锁定烟测通过，生产 Dataset/Run/Portfolio 和规则表保持 0 行，Raw 行数未变化。
- 下一步：补录经确认的有效日期市场/费用规则，按完整开市日生成多个 `research-v1.1` FROZEN Snapshot，再创建正式 Dataset；未完成前不进入正式回测。

## T-030｜规则冲突与 Dataset 代码血缘收口

- 时间：2026-08-06
- 目标：防止规则导入静默吞掉同主键修订，以及 Dataset 在 build 后使用另一版本代码冻结。
- 状态：已完成
- 当前结果：规则导入对同主键执行内容指纹核对；相同内容幂等，不同内容要求新增 `rule_version`。Dataset Manifest 保存 build 时的 `code_version/git_commit/git_dirty/diff_hash`，freeze 前必须与当前工作区一致，否则必须重新 build。
- 验证：阶段三定向测试 52 项通过；全量实测 `628 passed, 1 skipped, 5 deselected`，2 个既有通知前缀测试失败；Raw、正式规则和 Dataset/Run/Portfolio 均未写入或修改。
- 下一步：继续等待有效日期规则和完整 Snapshot 日期范围确认，再执行正式生产数据集流程。

## T-031｜正式规则候选文件与审核入口

- 时间：2026-08-06
- 目标：提供可审计、可校验但不会自动入库的第一版市场/费用规则候选。
- 状态：候选已完成，等待审核
- 当前结果：新增 `dev_doc/backtest_rules_candidate.yaml`，按印花税生效日期拆分两段规则；交易单位、T+1、价位和费用均显式记录，涨跌停不由静态百分比伪造，而使用每日冻结 `std.security_status_daily` 限价。佣金、最低佣金和零滑点明确标记为研究假设，文件包含官方规则引用。
- 验证：`uv run python -m data_collect.jobs.backtest_rules dev_doc/backtest_rules_candidate.yaml` 返回 `validated=2`，未写入数据库；必须人工确认后才允许 `--confirm`。
- 下一步：确认候选费率/滑点及正式回测日期范围；确认后再入库并生成完整 `research-v1.1` Snapshot 系列。

## T-032｜阶段三成交语义与 Air-Gap 数据库哈希收口

- 时间：2026-08-06
- 目标：确保 `actual_shares` 只代表真实成交，并让 Backtest→Portfolio 的来源哈希不能被直接 SQL 绕过。
- 状态：已完成
- 当前结果：回测引擎在计划和拒单阶段保持 `actual_shares=0`，创建 Fill 后才写入 Fill 股数；数据库新增 `backtest.order.order_audit_hash` 及生成触发器，Portfolio 触发器强制 `source_order_hash` 等于该真实订单哈希；Portfolio 导入逻辑改为读取数据库权威哈希。
- 验证：回测/Air-Gap 专项测试 19 项通过；`compileall` 通过；`sql/033_backtest_order_audit_hash.sql` 已在 Docker PostgreSQL 应用并完成事务回滚烟测，直接插入订单自动生成 64 位哈希；Raw 表未修改。
- 后续门禁：全量测试、干净 Git commit、多日期 FROZEN Snapshot、正式 Dataset/Run 仍按 T-027/T-031 执行；本次修复不改变第一版“不自动下单”的边界。
## T-033: Approved Baseline smoke execution and audit closure

- Date: 2026-08-07
- Status: completed (three-day smoke; full historical run pending)
- Scope: approved fee rules -> FROZEN Dataset -> explicit Baseline Run -> ledger/order/hash audit.
- Result: Dataset `4013823d-2cf9-48f8-aff5-b642f0989329` FROZEN; Run `9f278214-606d-40b1-9ce4-b8ee6ce2c1ad` SUCCEEDED; 91 orders, 6 fills, 85 rejects; actual shares and fills reconcile 900/900; rejected actual shares 0; Dataset/Run input hashes match.
- Fixes: corrected Dataset Manifest rule-version loading, frozen rule parameter/hash loading, no-price WARNING classification for suspended/non-trading securities, and Run Manifest recording of effective frozen rules. Raw tables were not modified by Dataset/Run.
- Boundary: this is not a five-year performance conclusion and does not create Portfolio/broker orders. Portfolio remains manual `PENDING_APPROVAL` -> `PAPER_MANUAL`.
## T-034: Documentation synchronization to verified implementation

- Date: 2026-08-07
- Status: completed
- Updated: `dev_doc/data_access_current_status_20260818.md`, `development_plan_3_phases.md`, `current_data_model.md`, `backtest_design.md`, and `project_completion_runbook.md`.
- Synchronized: Raw append/checkpoint rules, Standard/PIT/Feature mapping, FROZEN Snapshot contract, Dataset materialization, effective market/fee rules, T+1 execution, actual share semantics, corporate actions, Run audit hashes, Baseline smoke result, and Portfolio manual Air-Gap.
- Current boundary: three-day Baseline smoke passed; full historical Snapshot series and five-year Dataset/Run remain pending. No automatic trading.

## T-035: 手动每日候选 / AI 审计 Skill

- 状态：代码开发完成；生产使用仍保持手动。
- 新增 `sql/034_daily_candidate_ai_audit.sql`，覆盖候选运行、候选哈希、
  AI 审计 JSON、人工作决定、每日 Snapshot 绑定和 DAILY_SIGNAL Air-Gap。
- 新增 `data_collect/decision/` 服务，以及 `skills/ashares-decision-audit/`
  的候选生成、候选包、历史数据、AI 审计落库、人工作决定和待审批意图入口。
- Skill 不启动定时任务、不连接券商、不自动批准、不自动成交。
- 034 迁移在实际 PostgreSQL 上重复执行成功；正式候选、Portfolio 和每日
  绑定数据仍为 0 行。

## T-036: research-v1.3 historical Dataset materialization

- 时间：2026-08-09
- 状态：Snapshot 系列完成；Dataset build 已完成物化阶段，当前等待 validate/freeze。
- 已完成：`baseline_20260806` 下 1,215 个 `research-v1.3` Snapshot 全部 `FROZEN`，覆盖
  `2021-08-02 ~ 2026-08-06`；工作区干净，快照哈希完整。
- 当前 Dataset：`7b138308-f397-4886-a255-5b5b5cdf42cb`，名称
  `history-baseline-v13`，版本 `dataset-v1.3`，状态 `BUILDING`。
- 构建统计：执行行情 6,309,894 条、交易日历 1,831 条、公司行为 17,890 条、基准行情
  1,215 条、Dataset 成员 1,215 个；另有 93,384 条缺失执行行情待验证分类，不得静默忽略。
- 工程约束：`backtest_dataset.py` 使用服务端游标 `itersize=5000`、每 5,000 行批量写入和
  增量哈希，禁止对完整执行区间 `fetchall()`；`backtest_dataset_handoff.py` 支持通过
  `--dataset-id` 断点续建，避免重复创建和内存暴涨。
- 下一步：等待当前事务提交，执行 Dataset `validate → freeze`；通过后创建五年 Baseline
  Run，并对订单、成交、现金、持仓、公司行为、输入哈希进行全量对账。冻结前三日 smoke
Run 仍是唯一可引用的完成回测结果，Raw 不允许被 Dataset 流程修改。

## T-037｜Dataset 分交易日批次构建与资源保护

- 时间：2026-08-09
- 状态：代码完成；小范围真实烟测通过；五年任务待重新启动
- 目标：解决旧版 Dataset Build 单个长事务导致 WAL、死元组、索引维护和 Docker/WSL 内存峰值过高的问题，同时保留原始计算口径和最终审计哈希。
- 实现：新增 `sql/043_backtest_dataset_batch_checkpoint.sql` 和 `data_collect/jobs/backtest_dataset_batched.py`；`backtest_dataset_handoff.py` 默认按交易日批次构建，`--batch-days` 默认 5（允许 1~60）。每个批次只删除当前 Dataset 的日期区间，批量读取和写入后提交；批次 checkpoint 与该批次写入同事务收口，失败后重做，已完成批次仅在代码版本和批次哈希一致时跳过。
- 边界：不对共享 Dataset 子表执行全局 `TRUNCATE` 或全表删除；不修改任何 Raw/Standard/PIT/Feature；小型日历、分红、规则、基准表在执行行情完成后单独重建；最终仍执行原有完整 Validate → Freeze 门禁。
- 验证：新迁移已在 Docker PostgreSQL 幂等应用；2026-07-30~2026-08-03 三日真实 Dataset `afe4a191-be9d-4af6-b623-b46e7994ea18` 分两批完成，16,606 条执行行情、3 个成员、无缺失行情，`validate` 返回 `issues=[]`；相关专项测试 59 项通过。
- 下一步：恢复 Docker 后使用同一个五年 Dataset ID `eaa310e5-e3b3-45b6-aba0-f973c3ea3f4f`，以 `--batch-days 5` 重新执行；完成后再 Validate/Freeze。旧 Dataset `7b138308-f397-4886-a255-5b5b5cdf42cb` 的已提交物化结果不删除，继续作为历史对照。

## T-038｜A股通用研究数据底座完整建设

- 时间：2026-08-12
- 目标：按 `2016-08-01` 财务与行情预热、`2021-08-02` 正式研究输出的边界，完成显式 Standard 财务/估值字段、统一 as-of、TTM/ROE/现金流质量/Growth/Margin/估值 Feature、Snapshot 契约、五年历史回填和新数据基线发布。
- 状态：进行中
- 当前结果：`sql/045_research_data_foundation_v2.sql`、显式 Standard 投影、统一财务 V2 Feature、估值 V2 字段、质量门禁、Research V2 Snapshot、Release V2 门禁和可续跑流水线均已实现。真实 smoke 运行 `e67a5414-f359-427b-9e8f-9df73a7c551e` 覆盖 9 只跨类型股票，写入 15,277 行 Standard、149 行财务 Feature、8,025 行估值 Feature，强制异常项全部为 0。2026-08-13 完成全项目写入安全、增量恢复、PIT/Release/Snapshot 逻辑审计；`compileall`、高置信静态检查、72 项聚焦回归及全量 `766 passed, 1 skipped, 5 deselected` 全部通过。
- 正式运行：`f8e34e0e-61ec-410f-b2fe-b8b135c312cd` 从已落地 Raw 回填至 `2026-08-06`；59 个财务 Standard 股票分区已完成，估值 Standard 继续按月 checkpoint、内部按单交易日集合式短事务提交并限速。任务使用隐藏后台进程、全局 advisory lock、Git/diff 身份和续跑历史。
- 工程审查：已补齐 vendor ROE 的精确 `comp_type` 匹配、上海时区日期、完整 Feature 结果哈希、Standard no-op UPDATE 跳过、估值可见时间空值和分位范围硬门禁、Snapshot fail-closed 验证，以及 Release 质量/Manifest/状态切换的单事务原子性。所有高吞吐写者共享全局 session lock，高容量路径改为日/月/股票分区短事务；真实写入禁用 `limit_stocks`，财务派生检查点可独立恢复，代码身份变更时会废弃旧 checkpoint 重算。
- 资源纪律：数据库正式回填期间禁止并发全量 pytest、迁移、全表质量聚合或第二个回填任务；只允许低频、带 `statement_timeout` 的 checkpoint/资源查询。Docker/PostgreSQL 重启后必须先等待 crash recovery 完成并确认 `pg_is_in_recovery=false`；续跑程序取得全局 advisory lock 后可自动接管无活动 worker 遗留的陈旧 `RUNNING`，无需人工改写运行状态。
- 下一步：等待正式派生回填完成，单独执行 V2 全量质量门禁，发布 `feature-contract-v2.0` 数据基线，创建并验证 `research-v2.0` Snapshot，最后同步实测行数和发布证据。

## T-039｜每日 Tushare 生产增量收口（2026-09-26）

- 目标：让每日 Raw（原始层）→ Standard（标准层）/PIT（时点层）/Feature（特征层）→ 财务派生→生产质量门禁只由一条串行 DAG（有向无环任务流）触发，避免行情与财务拆分定时造成重复执行或先验收后更新。
- 当前结果：当前机器的本地 `config.yaml`（被 Git 忽略的运行配置）把 `tushare_production_daily` 配置为每日 20:00（Asia/Shanghai）唯一 Tushare 定时链；市场派生先于较慢的财务抓取执行，最终质量门禁同时依赖市场和财务派生。`tushare_daily`、`tushare_financial` 保留手动诊断，但取消独立定时；调度器和代码契约会拒绝缺阶段、参数漂移及旧拆分链独立定时。行业每日采集沿用 `stock_basic`（股票基础信息接口）同次响应，不增加额外接口调用。其他部署环境需把这段非敏感 DAG 配置同步到其运行配置。
- 范围与安全：市场/阶段二输入仍是最近 20 个交易日、公司行为窗口 30 个自然日；财务按最近披露窗口及变更队列运行；最终质量门禁检查最近 20 个交易日。估值滚动分位数仍按既有契约读取最多 5 年预热数据，但通过 `(stock_code, trade_date, source)` 键按 15 只股票分批读取、每批短事务发布；这是重复读 IO 成本，不是单个全市场大查询或长事务，首次生产运行仍需核对 `EXPLAIN`（执行计划）及实际耗时。未增加历史全量重建、表迁移或数据库操作；Research Data Baseline（研究数据基线）和 FROZEN Research Snapshot / Backtest Dataset（冻结研究快照/回测数据集）保持独立发布，不纳入每日采集 DAG。
- 验证：`tests/test_production_pipeline.py` 与 `tests/test_pipeline.py` 共 33 项通过；数据访问契约扫描 237 个文件、0 违规。尚未执行 dry-run 或真实生产增量，也未启动调度器。
- 下一步：在用户指定的生产窗口执行一次有界 dry-run 和一次真实日增量，核对任务日志、财务队列清空、日期覆盖与门禁；验收通过后再启动/重启常驻调度器。策略读取最新日期还需单独完成基线发布及新增 FROZEN 快照/Dataset 扩展。

## T-042｜日增量数据库查询预算硬门禁（2026-09-26）

- 目标：避免调度参数或环境变量意外把每日 Raw→Feature（原始层→特征层）任务扩成大范围回建，保护大体量 PostgreSQL。
- 实现：市场增量/阶段二增量最多回看 31 个交易日、事件最多回看 90 个自然日；生产派生输出最多跨 45 个自然日；每日质量窗口最多 31 日。估值 5 年预热仍按代码分批读取，每条查询最多 15 只股票且最多返回 25,000 行；超限失败，不放大内存或静默截断。生产 DAG（有向无环任务流）要求任务名/拓扑显式登记，并拒绝未审查任务，尤其是全历史 Release、Snapshot、Dataset 入口。
- 验证：相关测试 118 项通过；数据访问契约扫描 237 个文件、0 违规；未连接数据库、未运行数据任务。
- 边界：这些门禁限制单次查询/输出范围，不代表估值全市场累计历史读取很小；首次实跑仍需用低负载窗口观察日志与数据库读负载。Research（研究域）增量 Release 门禁仍未实现，日常不能调用全历史 `tushare_release validate` 来绕过；最新日期策略可用性仍需完成有界研究发布及新增 FROZEN Snapshot/Dataset（冻结研究快照/回测数据集）链路。

## T-043｜每日质量查询计划核验与部署 DAG 固化（2026-09-26）

- 目标：用生产 PostgreSQL 的只读 `EXPLAIN`（执行计划）检查每日估值预热与最近窗口质量查询，避免把“有返回行上限”误当成“数据库扫描有界”；补齐干净部署缺少每日 DAG 的配置缺口。
- 实测计划：估值输入查询（15 只股票，2021-08-01～2026-09-25）命中 `std.valuation_daily` 2021～2026 年分区上的主键 Bitmap Index Scan，没有全表 Seq Scan，规划估算约 18,061 行。该五年读取是跨批累计 IO，不是单条全表读取；每批最多返回 25,000 行。
- 质量门禁：19 个工作日合并的代码集合校验虽使用各事实表日期索引，但计划估算约 455,873 个中间键并排序/归并；单日计划约 27,572 个中间键。已改为逐交易日执行完全相同的双向精确集合差异比较和指纹校验，每条最多 5 个结果，并限制最多 31 个预期交易日；没有放宽验收语义。质量门禁的大表访问均使用日期索引；仅约 78 行的 `ops.market_data_quality_gate` 控制表选择 Seq Scan，属小表安全扫描。
- 安全边界：所有计划通过 Repository → Kernel → `ReadGateway` 生成；专用入口仅允许单条 `SELECT/WITH`，固定 `EXPLAIN (FORMAT JSON)`，拒绝 `ANALYZE` 和写入语句。没有运行源查询、ANALYZE、数据任务或迁移，也没有写运行清单；EXPLAIN 成本是规划器估算，不等同真实耗时。
- 配置：将本机已验证的 20:00 Asia/Shanghai 串行 `tushare_production_daily` DAG（有向无环任务流）加入受版本控制的 `config.example.yaml`，不含密钥；保留本机忽略配置原样，未启动调度器。
- 验证：Gateway/Repository/财务特征/生产流水线/调度专项 `213 passed`；数据访问契约扫描 237 文件、0 违规；编译与 `git diff --check` 通过。
- 未完成边界：每日 Raw→Standard/PIT/Feature→生产质量门禁的代码与配置已有有界链路；Research 数据发布增量、每日 FROZEN Snapshot（冻结研究快照）及 Backtest Dataset（回测数据集）扩展仍未实现。Research 质量规则跨多张可变历史事实表，当前唯一安全路径仍是完整审计；禁止把它挂到日增量 DAG 或用最近日期过滤冒充完整验收。下一阶段须逐条定义影响范围并做完整审计/增量多日影子对账，再实现发布推进。
