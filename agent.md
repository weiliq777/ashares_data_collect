# Finance_cn Agent Notes

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

历史基线规则：历史 Raw/Standard 验收后在 `tushare_data_release` 建立发布标签；只有 `status='validated'` 的基线可被增量任务默认读取。阶段二日常任务使用 `tushare_phase2_incremental`，不再启动历史 `tushare_phase2_raw_init` 编排；`dividend` 使用公告日期/实施公告日期窗口，`namechange` 使用低频日期窗口。

阶段三快照规则：`research_snapshot` 生成器可读取已通过质量门禁的 PIT/Feature；Research 和 Backtest 只能读取 `FROZEN` 快照。一个 `snapshot_id` 只对应一个截止日；生成使用 REPEATABLE READ 和输入切片哈希；FROZEN 后由数据库触发器禁止更新/删除。非缺失血缘必须有来源版本和 SHA-256，`:missing` 只表示上游确实没有记录。正式快照前不得进入正式回测或自动交易。

Research 审计规则：`ACTIVE` 观察池只能归档；ACTIVE 成员和证据只能追加；ARCHIVED 观察池、成员和证据全部不可变；证据必须属于快照股票，绑定观察池时必须属于该观察池成员；修正必须新建 `watchlist_id`。
