# Finance_cn Agent Notes

## 专业名词中文说明规则

以后在对话、任务记录、日志说明和交付报告中，首次提到表名、字段名、模块名、接口名、类名、任务名或其他专业标识符时，必须同时补充对应的中文业务含义。例如：`research.candidate_run`（研究候选运行记录表）、`snapshot_id`（研究快照标识）、`Quality Gate`（质量门禁）。后续重复使用时可只写英文标识符，但不得只给出无法理解业务含义的英文名称。

## 策略层读取规则

- ROE、PE 分位、趋势等指标由 Feature 层提前计算；策略层只读取指定 `snapshot_id` 的当日结果，不重新扫描五年历史。
- 策略必须按 `snapshot_id` 批量读取股票数据，禁止逐股票 N+1 查询；Raw 只用于数据转换、审计和重建，不作为策略实时查询入口。
- 日常研究只处理最新交易日增量；数据质量门禁失败时不生成正式候选，不自动下单。

## 2026-08-11 大数据任务资源约束

- 本次 Research Snapshot 基线统计对 16GB `tushare_market_raw_version` 进行了重型统计查询，造成 Docker/WSL 高负载并失去响应；快照任务已停止，Raw 未被修改。
- 后续所有大表任务必须单并发、分表/分日期分批、每批独立提交，并保留 checkpoint、日志和失败重试；禁止多个任务同时扫描同一大表。
- 禁止一次性全表 `COUNT/MAX/GROUP BY` 作为启动前置检查；优先使用已保存的基线 Manifest、索引范围查询、近似统计或预聚合统计，必要时离线分批计算。
- 启动后台任务前必须确认同类进程只有一个；发现 Docker/WSL 内存或 IO 持续升高时立即暂停任务，先检查进程、数据库活动查询和错误日志。
- 任务完成后必须同时核对：进程是否退出、数据库 checkpoint/状态、错误日志、目标行数/日期范围和主键约束；不能只依据本地进度日志判断成功。

## 2026-08-08 production data audit follow-up

The code audit is green (`693 passed, 1 skipped, 5 deselected`; `compileall` and `git diff --check` pass). The active code contract is `research-v1.2` → `backtest-dataset-v2` → `research-to-backtest-v2` → `daily-next-open-v2`.

The production database still contains legacy smoke/partial data, not a current five-year production baseline: `research-v1.2` has no frozen snapshot; `research-v1.1` has 690 FROZEN snapshots plus one DRAFT, with 522 SSE open dates missing in `2021-08-02`–`2026-08-03`; 689 of those FROZEN rows are dirty and all 690 lack `baseline_quality_report_hash`. Existing `dataset-v1`/`dataset-v1.1` and `daily-next-open-v1` runs remain immutable historical records and are not valid inputs for the current v2 path. The approved v3 market/fee rules are present.

The long Raw initializer for `2016-08-01`–`2021-08-01` is still running in the background; its last observed checkpoint is `adj_factor=930 done, 0 failed` at `2026-08-08 03:15:16`. Do not interrupt it without an explicit decision. Raw data was not modified by this audit.

The current `baseline_20260803` release is `validated` but its persisted `source_manifest_hash/source_manifest` is empty. The current Snapshot code intentionally rejects this baseline before creating a v1.2 snapshot; do not start a Snapshot series against it. After the active Raw/derived data work is complete, publish a new validated baseline with a complete source manifest, then resume the v1.2 series.

## 2026-08-08 current audit status

Stage 3 code audit repairs are complete. Formal Backtest uses frozen Dataset inputs; reserve cash is an equity floor, settled cash is required for new candidates, Snapshot build locks its quality/input boundary, and migration `sql/041_backtest_open_time_contract.sql` is applied. Full test result: `693 passed, 1 skipped, 5 deselected`. The long checkpointed historical Raw initializer is still running; do not stop it. Full five-year Snapshot series/Dataset/Run production generation remains a separate pending operation. Portfolio stays manual `PENDING_APPROVAL -> PAPER_MANUAL`; no broker or automatic order path.

## 阶段三生产增强版（2026-08-06）

风险压力模型、Tushare 执行单位归一化、成交量参与率整笔拒单、Monte Carlo 双方法及 Backtest→Portfolio 人工隔离已完成。对应迁移为 `sql/032_backtest_portfolio_airgap.sql`，Raw 表继续只读；`import_backtest_order()` 只能写入 `BACKTEST/PENDING_APPROVAL`，不得自动批准、成交或访问券商。正式回测仍需 clean Git、Snapshot series、Dataset freeze 和正式 Run 门禁。

## 阶段二质量审计修复（2026-08-04）

最终审计发现 216 只股票的历史日线有数据但复权因子不完整；已用真实接口确认可补回，新增 `tushare_adj_factor_gap_repair.py` 按股票 checkpoint 追加 Raw。缺口已全部修复，`adj_factor_daily` 和价格 Feature 已重建，最终质量报告通过，`baseline_20260731` 已标记为 `validated`。

## 阶段二故障记录（2026-08-04）

`tushare_phase2_raw_init.py --retry-empty` 曾把所有已存在 checkpoint 都当作可重试对象，可能重复请求 `done` 日期并浪费 Tushare 配额。现已增加精确状态读取，仅重试 `status='empty'`；Raw 表保持不可变，修复只作用于后续任务筛选逻辑。

## 当前目标

以 Tushare 为主要数据源，建立可追溯、可断点续传的 A 股数据仓库，为基本面筛选、趋势分析、估值分析和风险计算提供事实数据。

数据链路必须保持：

```text
Tushare 原始接口 -> raw 原始表 -> 标准化表 -> 策略/分析层
```

策略层不得直接依赖接口字段，也不得用公告日之后才公开的数据回测历史交易日。

## 关键目录和文件

说明表名时必须同时附带业务含义；涉及 Raw 表时注明对应的 Tushare 接口和原始数据用途，避免只给出技术名称。

- `config.yaml`：数据库、Tushare、OpenSearch、RSSHub 配置
- `docker-compose.yml`：PostgreSQL、OpenSearch、Dashboards、Redis、RSSHub
- `data_collect/providers/tushare_client.py`：Tushare 客户端、限流和重试
- `data_collect/jobs/a_share_instrument_tushare.py`：股票基础资料
- `data_collect/jobs/a_share_daily_tushare.py`：每日增量日线
- `data_collect/jobs/a_share_valuation_tushare.py`：每日增量估值
- `data_collect/jobs/a_share_financial_tushare.py`：财务指标和财务原始表
- `data_collect/jobs/tushare_history_init.py`：历史初始化、断点、批次暂停
- `data_collect/jobs/tushare_daily_incremental.py`：历史兼容的 Raw-first 每日增量
- `data_collect/jobs/tushare_market_incremental_by_date.py`：正式每日增量，按交易日批量获取全市场 daily/daily_basic/adj_factor
- `data_collect/jobs/tushare_market_gap_repair.py`：按交易日历与 Raw 实际覆盖发现并补采 daily/daily_basic 缺口
- `data_collect/jobs/tushare_phase2_incremental.py`：阶段二日常增量，状态类按交易日、分红按公告日期窗口、名称变更低频窗口、指数按代码区间
- `data_collect/jobs/tushare_financial_incremental.py`：财务日常增量；报表按公告窗口，`fina_indicator` 按股票和季度报告期窗口
- `data_collect/jobs/tushare_derived_rebuild.py`：行情/阶段二/财务增量后的派生依赖重建编排
- `data_collect/jobs/research_snapshot.py`：Research 快照 Manifest、输入切片哈希、血缘、校验和冻结
- `data_collect/jobs/phase3_validation.py`：阶段三 PIT 截面只读校验入口
- `data_collect/ops/market_coverage.py`：按日市场覆盖率门禁与熔断记录（只读 Raw 计数、追加 ops 事实）
- `data_collect/jobs/tushare_release.py`：历史基线发布点的创建、验收和查询
- `data_collect/jobs/tushare_adj_factor_gap_repair.py`：历史复权因子缺口的按股票断点修复
- `data_collect/jobs/tushare_raw_to_standard.py`：Raw 离线重建 Standard
- `data_collect/jobs/tushare_rebuild_validation.py`：Raw 到独立验证表的全量重建验收
- `data_collect/jobs/tushare_data_quality.py`：阶段一聚合质量检查
- `data_collect/jobs/tushare_phase2_raw_init.py`：阶段二公司状态、公司行为和指数 Raw 历史同步
- `data_collect/jobs/tushare_phase2_standard.py`：阶段二 Standard、生命周期和 PIT 重建
- `data_collect/jobs/tushare_phase2_features.py`：阶段二价格、估值、财务和股东回报 Feature 重建
- `sql/023_phase2_version_and_business_keys.sql`：财务 Standard 血缘字段和分红业务键迁移
- `sql/024_create_research_snapshot.sql`：Research 快照 Manifest、输入统计、股票快照、血缘和数据库冻结约束
- `sql/025_research_lineage_sources.sql`：生命周期 Standard 来源哈希字段与 pgcrypto 迁移
- `sql/026_create_research_watchlist.sql`：Research 观察池、观察池成员和证据引用表
- `data_collect/jobs/research_watchlist.py`：只读快照生成观察池和写入证据引用元数据
- `data_collect/jobs/research_evidence_candidates.py`：只读检索 OpenSearch 外部证据候选，不自动入库
- `sql/028_create_backtest_model.sql`：Backtest Dataset/Run、执行输入、规则快照和事件账本模型
- `data_collect/backtest/`：不访问数据库的纯 Python 日线撮合、风险、费用和输入模型
- `data_collect/jobs/backtest_dataset.py`：Dataset 创建、Frozen Materializer、质量校验、冻结和加载
- `data_collect/jobs/backtest_rules.py`：市场/费用规则 JSON 校验与只追加导入
- `data_collect/jobs/backtest_run.py`：FROZEN Dataset 回测运行、事件账本和绩效结果持久化
- `dev_doc/backtest_design.md`：阶段三回测契约、默认边界和当前验收门禁
- `sql/029_create_portfolio_model.sql`：人工确认模拟组合、成交、持仓、现金和估值模型
- `sql/030_create_phase3_ops_gate.sql`：阶段三按日市场覆盖率门禁和追加式数据库约束
- `data_collect/portfolio/`：不访问数据库/券商的 Portfolio 纯 Python 模型
- `data_collect/jobs/portfolio_simulation.py`：Portfolio 创建、人工意图、模拟成交和估值入库
- `dev_doc/portfolio_design.md`：Portfolio 边界、审批、资金和审计契约
- `sql/027_research_audit_immutability.sql`：ACTIVE 追加式、ARCHIVED 全不可变的观察池审计约束
- `data_collect/normalize/`：接口数据到标准模型的转换
- `data_collect/utils/db.py`：PostgreSQL 写入、幂等和表结构对齐
- `sql/012_create_tushare_standard.sql`：标准表和已接入财务原始表
- `sql/013_create_tushare_history.sql`：交易日历、复权因子和初始化断点表
- `sql/014_create_tushare_market_raw.sql`：行情、估值、复权因子、交易日历原始表
- `sql/018_create_phase2_model.sql`：阶段二 Raw、Standard、PIT、Feature 和注册表
- `sql/019_create_tushare_release.sql`：历史基线发布点和增量起始控制表
- `sql/020_fix_lifecycle_null_unique.sql`：生命周期 Standard 的 NULL 区间唯一约束修复
- `dev_doc/current_data_model.md`：当前完整表模型和数据来源说明
- `dev_doc/tushare_history_initialization.md`：历史初始化和每日同步流程
- `dev_doc/agent_tasks.md`：简明任务清单、状态和下一步

## 数据库端口

Windows 保留了 5432、5601、2100 等端口，当前主机映射为：

- PostgreSQL `127.0.0.1:15432`
- OpenSearch `127.0.0.1:9200`
- OpenSearch Dashboards `127.0.0.1:15601`
- RSSHub `127.0.0.1:11200`

## 操作约束

1. Token 只从 `TUSHARE_API_KEY` 环境变量读取，不写入代码、文档或日志。
2. 新增数据源时必须同时考虑 raw 表、标准表、来源字段、抓取时间和幂等键。
3. 历史数据必须分批、限流、可断点续传；不能并发启动多个全量任务。
4. 清理数据前先确认目标表，默认保留基础资料和新闻数据。
5. 修改数据库模型后，必须同步更新 `current_data_model.md` 和对应 SQL 迁移文件。
6. 原始数据用于重算和审计，策略代码优先读取标准化表。
7. 阶段二执行顺序固定为：公司状态/公司行为 Raw → Standard 状态和生命周期 → PIT 股票池/可交易性/财务快照 → Feature；未完成上游 Raw 时不得宣称下游全量完成。

## Agent 工作模式

所有持续性工作都在 `dev_doc/agent_tasks.md` 中登记一条任务，保持简明：

```markdown
## T-编号｜任务名称

- 时间：YYYY-MM-DD
- 目标：一句话说明
- 状态：待开始 / 进行中 / 已完成 / 已阻塞
- 当前结果：一句话说明
- 下一步：一句话说明
```

开始工作时更新为“进行中”；真正完成后更新为“已完成”；遇到无法继续的外部阻碍时才使用“已阻塞”。每次涉及新的长期目标，都追加新任务，不覆盖历史任务。

## Tushare 原始数据强制规则

以后所有 Tushare 接口请求都必须先保存一份完整原始响应到对应的 `*_raw` 表，再进行标准化转换。禁止接口请求后直接写入标准表。

Raw 表是不可变事实层：任何任务不得删除、更新、覆盖或清理 `*_raw` 表中的历史记录。字段映射、单位转换和错误修复只能通过重新生成 Standard 表完成；如需修复 Raw，必须新增版本记录或单独建立迁移方案，不得直接改写原始数据。

原因：Tushare 接口会消耗积分；保留完整 raw 数据后，字段映射、单位换算或标准模型发生错误时，可以直接从数据库重新转换和恢复，不必重复请求接口。

新增 Tushare 接口时必须同步完成：

1. 建立对应 raw 表，保留原始 JSON、业务主键、来源和抓取时间。
2. 使用 raw 表主键和幂等写入，避免重复数据。
3. 再建立独立的 raw 到标准表转换逻辑。
4. 在 `current_data_model.md` 中记录接口、raw 表、标准表和字段映射关系。

阶段二补充规则：`stock_st` 只表达 ST/风险状态，不能直接等同于不可交易；`suspend_d`、`stk_limit` 和日线价格共同决定当日可交易性；`namechange` 生成名称有效区间；`dividend` 的 `ex_date` 不晚于决策日才可进入股东回报 Feature；指数数据可用于基准和超额收益，但不进入个股 PIT。

故障预防：分批读取数据库时，不能在同一游标上执行 `fetchmany` 后再执行写入 SQL；应使用独立读写连接或先安全缓存批次。Tushare 网络 EOF 必须记录失败 checkpoint，并按业务键续传，不能把已完成批次全部重跑。

Windows 后台启动 Tushare 任务时，必须显式保证子进程继承 `TUSHARE_API_KEY`；Token 不得写入脚本或日志。`.venv` 的 uv Python shim 可能返回空的进程退出码，监督脚本应结合启动/完成日志和数据库 checkpoint 判断结果，不能把空退出码直接判为失败。

阶段二空响应规则：历史交易日接口返回 `empty` 不能直接视为完整无数据；Raw 序列结束后使用 `tools/phase2_retry_empty.ps1` 只补采空 checkpoint，随后使用 `tools/phase2_rebuild_after_retry.ps1` 重建派生层。

行情覆盖规则：历史行情 checkpoint 不能单独证明 Raw 已入库；必须以 `trade_cal` 开市日与 `tushare_daily_raw`/`tushare_daily_basic_raw` 的实际日期覆盖交叉核对。缺口使用 `tushare_market_gap_repair.py` 按交易日请求补采，独立记录 `daily_gap_repair`/`daily_basic_gap_repair` checkpoint；补采前后不得删除或覆盖已有 Raw。

每日增量规则：最近 5 个交易日允许重复回补；正式任务使用 `data_collect.jobs.tushare_market_incremental_by_date` 按交易日获取全市场数据，不得逐股票请求；每次抓取完整响应写入不可变 `tushare_market_raw_version`，现有 `*_raw` 只做首次业务记录的幂等追加；Standard 按交易日重建。历史初始化仍可按股票断点续传。不得用 checkpoint=done 永久跳过需要修复的回补日期。

历史基线规则：历史 Raw/Standard 验收后在 `ops.tushare_data_release`（运维数据发布基线表）建立发布标签；只有 `status='validated'` 的基线可被增量任务默认读取。阶段二日常任务使用 `tushare_phase2_incremental`，不再启动历史 `tushare_phase2_raw_init` 编排；`dividend` 使用公告日期/实施公告日期窗口，`namechange` 使用低频日期窗口。

阶段三快照规则：`research_snapshot` 生成器可读取已通过质量门禁的 PIT/Feature；Research 和 Backtest 只能读取 `FROZEN` 快照。一个 `snapshot_id` 只对应一个截止日；生成使用 REPEATABLE READ 和输入切片哈希；FROZEN 后由数据库触发器禁止更新/删除。`research-v1.1` 额外保存并哈希 PIT 的 `eligible_by_market`；旧版本 FROZEN 快照只读保留，正式 Backtest Dataset 不得静默升级或使用。新增版本血缘必须有来源版本和 SHA-256；历史 `tushare_daily_raw` 行可能尚无 `tushare_market_raw_version`，此时允许 `source_record_version=NULL`，但必须以选中 Raw payload 计算确定性 SHA-256，并在 Dataset 中标明这是 legacy 限制，不得伪造版本或回退到无哈希数据。正式冻结只接受干净 Git 或存在的代码归档，`diff_hash` 不能替代可复现代码。快照系列按 `(截止日, 基线, Schema)` 幂等续跑，不重复创建或重写 FROZEN 快照。正式快照前不得进入正式回测或自动交易。

Research 审计规则：`ACTIVE` 观察池只能归档；ACTIVE 成员和证据只能追加；ARCHIVED 观察池、成员和证据全部不可变；证据必须属于快照股票，绑定观察池时必须属于该观察池成员；修正必须新建 `watchlist_id`。

阶段三运行门禁补充：`sql/030_create_phase3_ops_gate.sql` 的 `ops.market_data_quality_gate` 只追加门禁记录；按日全市场增量以历史 Raw 前五交易日的中位股票数检查覆盖率，下降超过 1%、无基线或抽样都不可发布，失败在 Raw 版本写入前中止。PASS 只表示接口覆盖率，Raw/Standard/checkpoint 或全局派生重建失败时必须追加最新 `BLOCKED`，防止旧 PASS 让 Research freeze 放行。Research 冻结会检查该截止日最近门禁。`data_collect/jobs/phase3_validation.py` 是只读 PIT 截面校验入口；物化遇到涨跌停空值/100000 哨兵会保留价格但关闭买卖并标记 WARNING；Dataset 创建还要求 warmup 到 evaluation_end 的每个 SSE 开市日都有 FROZEN Research Snapshot；规则同主键内容冲突会拒绝静默跳过，审核元数据未 APPROVED 时禁止 `--confirm`，Dataset build/freeze 代码身份必须一致；Dataset 执行行情按构建事务内 `fetched_at DESC, record_id DESC` 选取 Raw 版本并冻结 source version/hash，缺少日线、状态或可交易性来源的行标记 `MISSING_SOURCE` 并阻断正式验证；历史无 Raw version 的行允许版本号为空但必须有确定性 payload hash；市场温度倍率目前仅为显式适配器，未加入未版本化的默认回测输入。Dataset build/validate/freeze 会重新检查基线仍为 validated，失败会持久化为 FAILED；Run 子表仅在 RUNNING 状态可写，Portfolio 数据库还校验审批人、费用、现金账本余额和持仓数量单调性，现金账本会锁定组合账户并以唯一部分索引保证单一 `INIT`；规则有效区间另由 PostgreSQL `EXCLUDE USING gist` 防止同一作用域并发重叠。最新定向测试为 59 项通过，全量测试结果以实际运行记录为准，另有 2 个既有通知前缀失败。
规则输入补充：市场/费用规则只能通过 `data_collect/jobs/backtest_rules.py` 导入；JSON 必须带 `parameters.provenance`、有效日期和版本，默认只校验，`--confirm` 才追加入库。`backtest.market_rule_effective` 与 Dataset 规则快照均由数据库约束禁止 UPDATE/DELETE，修订必须新增规则版本。

阶段三冻结补充：`VALIDATED→FROZEN` 只允许状态、冻结时间和代码血缘字段变化；规则导入除文件内有效区间检查外，还必须检查数据库已有的同市场/同市场规则版本/费用规则版本区间，禁止跨文件形成重叠规则。该门禁已在 PostgreSQL 回滚烟测中验证，测试数据未持久化。

阶段三 Backtest 规则：正式回测必须基于多日期 `FROZEN` Research Snapshot 和不可变 Dataset/Run Manifest；不能直接读取实时 PIT/Feature，也不能在生产模式自动回退合成数据。一个 snapshot 只对应一个截止日；`research_snapshot_series.py` 只负责按显式日期或标准交易日历编排多个单日快照，且截止日必须为开市日。执行行情使用原始 OHLC/成交量/涨跌停价，复权价格只用于指标；Dataset 构建时按 `tushare_market_raw_version.fetched_at DESC, record_id DESC` 选择每个日线业务键的最新 Raw 版本，并把 source version/hash 物化冻结；必须区分 warm-up 与 evaluation 区间、信号/订单/成交时间，并版本化交易日历、市场规则、费用、滑点、策略参数、适配器和代码。第一版默认 T 日收盘出信号、T+1 日开盘尝试成交、只做多、不模拟部分成交、暂不模拟配股；使用信号时确定的固定止损，不宣称已实现 ATR/Trailing Stop；缺规则必须 fail-closed。Dataset 成员必须同时保存执行行情和公司行为切片哈希，`validate` 必须重新对账最终物化子表的全局及按日哈希，并独立检查 warm-up 到 evaluation_end 的每个开市日覆盖；市场规则必须按 `market_rule_version`、`fee_rule_version` 和有效日期筛选，公司行为按业务键选择有效版本；送转只分配给除权日前持仓并落为 `CORPORATE_ACTION` 批次，卖出结算日超出 Dataset 时拒绝成交，不提前释放未结算资金。结果按 signal→order→fill/reject/expire→cash/position→daily performance 事件链保存。Portfolio 当前仅允许人工批准的 `PAPER_MANUAL` 模拟成交，组合 Manifest 必须保存 T+1、现金结算、费用和最小价位规则，不连接券商；数据库还要独立拒绝不符合成交股数、现金、价位、止损、已有持仓、SSE 开市日和快照截止日的成交，现金账本和持仓日期也必须命中冻结交易日历。Portfolio 每日估值必须绑定同一 FROZEN Snapshot 和 `valuation_input_hash`；相同输入只能幂等重放，不同输入、更新和删除均拒绝，且不得在更晚成交后回补过去估值。开发任务和详细契约见 `dev_doc/agent_tasks.md` 的 T-018/T-019/T-020/T-021/T-022/T-023/T-024/T-025/T-026/T-027 与 `dev_doc/backtest_design.md`、`dev_doc/portfolio_design.md`。
当前阶段状态（2026-08-07）：阶段三核心代码开发已完成，正式历史回测可以进入生产准备；当前数据库尚无正式 Dataset/Run/Portfolio/规则记录，后续执行生产门禁即可。生产顺序为规则审批入库、clean Git commit、多日期 FROZEN Snapshot、Dataset build/validate/freeze、正式 Run 和结果审计。自动实盘交易仍禁止，Backtest→Portfolio 必须人工审批并使用 PAPER_MANUAL。

## 数据库批量写入资源规则（2026-08-09）

曾因 Dataset 执行行情一次性放入数百万行长事务，造成 WAL、死元组、索引维护和 Docker/WSL 内存压力，且阶段级 checkpoint 无法在 Build 中途续传。以后所有大批量数据库任务必须遵守：

- 按日期/业务分区拆成有限批次，单批独立事务并及时提交；
- 批次写入必须有 checkpoint、心跳、幂等键和失败重试；已完成批次只在代码版本和批次哈希一致时跳过；
- 不能使用全局 `TRUNCATE` 或一次性删除/重建共享表；清理必须限定明确的业务范围；
- 不以多线程直接向 PostgreSQL 共享表堆写来解决性能问题，先采用批量写入、合理索引、服务端游标和受控事务；
- 数据库任务异常后，必须同时检查进程、数据库 checkpoint 和错误日志，不能只看本地进度日志；
- Raw 表始终只读，不因性能优化而修改、覆盖或删除原始数据。

本次对应实现：`sql/043_backtest_dataset_batch_checkpoint.sql`、`data_collect/jobs/backtest_dataset_batched.py` 和 `backtest_dataset_handoff.py` 的 `--batch-days`。
本次财务暖身任务补充：`pit.financial_asof` 原先的全量 `TRUNCATE + INSERT` 会形成大事务，已改为按日期窗口写入完整构建表、每窗口独立提交，最后以短事务切换表；失败只清理构建表，不影响 Raw 或旧 PIT。后续新建派生重建任务不得恢复全量长事务。

## 低频 Tushare 源数据抽样对账

偶尔需要核对真实 API 时使用 `tools/audit_tushare_sample.py`；它只读 Tushare 和数据库 Raw，不写入生产表。默认抽样 10 条，也可用 `--sample-size 50`，结果可保存为 JSON；不纳入日常主流程。

示例：`.venv\Scripts\python.exe tools\audit_tushare_sample.py --trade-date 2026-08-21 --sample-size 10 --dataset all --output logs\tushare_source_audit_20260821.json`

## 日常增量执行方式

完整生产增量统一运行 DAG，不逐个手动触发任务：

```powershell
uv run python tools/run_pipeline_once.py `
  --pipeline tushare_production_daily `
  --run-date YYYYMMDD `
  --no-dag
```

## 日线行情表迁移记录

`public.daily_kline`（旧公共日线行情表）已完成 524 个日期窗口校验，源/目标均为 10,672,777 行且分块哈希一致；正式消费者统一使用 `std.daily_kline`（标准分区日线行情表）。旧表保留为 `public._legacy_daily_kline_95cc209be402`（旧表备份），应用账号已撤销其读写权限，不删除，供回滚和审计使用。

`public.adj_factor_daily`（旧公共复权因子表）同样完成 524 个日期窗口校验，源/目标均为 11,028,658 行且分块哈希一致；正式消费者统一使用 `std.adj_factor_daily`（标准分区复权因子表）。旧表保留为 `public._legacy_adj_factor_daily_2f5af8e6142b`（旧表备份），应用账号无权读取。

`public.valuation_daily`（旧公共估值表）完成 524 个日期窗口校验，源/目标均为 10,594,158 行且分块哈希一致；正式消费者统一使用 `std.valuation_daily`（标准分区估值表）。旧表保留为 `public._legacy_valuation_daily_88f539fe61ed`（旧表备份），应用账号无权读取。

流程按依赖顺序执行 Raw、Standard/PIT/Feature、Financial Derived、Market Derived 和 Quality Gate；失败按配置重试，上游失败时下游停止。`--only-task` 仅用于排障，不用于正常增量。

## 财务标准表迁移记录

`public.financial_income_standard`（旧公共财务利润表）、`public.financial_balance_sheet_standard`（旧公共财务资产负债表）、`public.financial_cash_flow_standard`（旧公共财务现金流量表）和 `public.fina_indicator_standard`（旧公共财务指标表）已按主键窗口完成复制、分块哈希校验和正式切换；正式消费者统一使用 `std.financial_income_standard`、`std.financial_balance_sheet_standard`、`std.financial_cash_flow_standard` 和 `std.fina_indicator_standard`（标准层财务表）。四张旧表保留为 `public._legacy_*`（旧表备份），程序账号无权读取；目标序列已对齐到最大主键，后续增量可直接写入。

以后提及技术表名、Schema（模式）或其他专业名词时，同时给出对应中文名称；Raw（原始层）仍保留在 `public`（公共模式），Standard（标准层）正式表使用 `std`（标准模式）。

`public.stock_financial_indicator`（旧公共财务指标兼容表）已完成 444 个 7 日窗口校验，共 115,646 行，正式消费者和旧适配器统一使用 `std.stock_financial_indicator`（标准层财务指标兼容表）；旧表保留为 `public._legacy_stock_financial_indicator_beafb40fec91`（旧表备份），程序账号无权读取。核心财务 v2 不依赖该兼容表，后续可在确认旧适配器完全退役后再按保留周期清理备份。

`public.trading_calendar`（旧公共交易日历表）和 `public.stock_basic_info`（旧公共股票基础信息表）已完成标准层切换，正式写入统一使用 `std.trading_calendar`（标准交易日历表）和 `std.stock_basic_info`（标准股票基础信息表）；程序账号已撤销旧公共表权限，旧表仅由超级用户保留作回滚/审计源。

`financial_change_queue`（财务变更队列）、`financial_ingest_anomaly`（财务采集异常）和 `financial_version_cutover_watermark`（财务版本切换水位）属于运维控制表，已通过目录级 `SET SCHEMA`（模式迁移）从 `public`（公共模式）移动到 `ops`（运维模式），数据与索引未复制；VIP 财务链路已统一使用 `ops.*`（运维表）。

`public.tushare_history_checkpoint`（旧公共历史检查点表，约 8 万行）和 `public.tushare_data_release`（旧公共数据发布基线表）已通过 `sql/076_operational_baselines_to_ops.sql` 使用目录级 `SET SCHEMA` 移动为 `ops.tushare_history_checkpoint`（运维历史检查点表）和 `ops.tushare_data_release`（运维数据发布基线表）。这不是历史数据重采，也不复制行；研究/回测外键、发布序列、`ashares_app`（程序账号）权限和所有 Repository（仓储）查询均已切换到 `ops`。`contract.dataset_release`（契约数据集发布表）仍是独立的 FROZEN（冻结）数据产品，不与运维基线表混用。

权限收口：`sql/077_revoke_legacy_public_app_privileges.sql` 已撤销 `ashares_app` 对旧空业务表（旧财务/行业/ETF/指数辅助表）和损坏 Raw 备份表的全部权限；程序账号在 `public`（公共模式）仅保留 Raw（原始层）正式表的 SELECT/INSERT，不具备 UPDATE/DELETE。超级用户权限保留，用于人工数据库运维。

财务写入契约修复：`std.financial_income_standard`（标准利润表）、`std.financial_balance_sheet_standard`（标准资产负债表）和 `std.financial_cash_flow_standard`（标准现金流量表）的 Upsert（幂等写入）键是 `(source, source_record_key, record_version)`，不能使用自增 `financial_id`；`std.fina_indicator_standard`（标准财务指标表）实际冲突键为上述三列加 `payload_hash`；`std.stock_financial_indicator`（兼容财务指标表）必须包含 `source`。Policy（策略）必须与 PostgreSQL（数据库）唯一索引和 Repository（仓储）实际冲突键一致。

同日财务修订规则：同一业务键和同一公告日出现不同 payload 时，Standard（标准层）写入 `quality_status='WARNING'`；`version_available_date`/`data_available_timestamp` 保留业务可见边界，PIT（时点层）使用 `record_version DESC` 二级仲裁，不能用 `fetched_at`（仓库抓取时间）伪造官方公告时刻。

并发规则：财务 Standard/PIT/Feature（标准层/时点层/特征层）转换、财务增量、财务迁移和基础重建统一持有 `warehouse-write/foundation-rebuild`（仓库写入排他锁）；新增财务写任务不得自建互不相交的锁名。
