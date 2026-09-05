# Finance_cn Agent Notes

> 本文件是开发执行纪律，不是项目状态日报。`dev_doc/data_access_kernel.md`（数据访问内核设计契约）是数据库访问的唯一实现基线；代码与该文档冲突时，默认代码错误。当前任务状态以 `dev_doc/agent_tasks.md`（任务清单）、对应设计契约和实际检查结果为准；历史记录见 `dev_doc/agent_history.md`（智能体历史归档）。

## 一、长期不可破坏的架构边界

数据链路固定为：

```text
Tushare 原始接口
  → Raw（原始层）
  → Standard（标准层）
  → PIT（时点层）
  → Feature（特征层）
  → Data Baseline（数据基线）
  → Observation（观察域）
  → Research Snapshot（研究快照）
  → Backtest Dataset（回测数据集）
  → Strategy / Publish（策略 / 发布）
```

- 所有新增数据库访问必须经过 `Repository → DataAccessKernel`（仓储 → 数据访问内核）。业务代码不得持有 `Connection/Cursor`（数据库连接/游标），不得手写 SQL、`commit` 或调用旧 `get_connection()`；不得新增万能 `execute()` 兼容入口。
- 数据库访问必须使用 `RunContext`（运行上下文）、`WorkScope`（工作范围）、`SqlPlan`（SQL 计划）、`DatasetPolicy`（数据集策略）、`QueryGuard`（查询守卫）、`ChunkPlanner`（分块规划器）、`TransactionManager`（事务管理器）、`RetryPolicy`（重试策略）、`Checkpoint`（检查点）等契约控制对象。
- Gateway（网关）只暴露文档规定的能力；复杂的 SET-based SQL（集合式 SQL）只能位于对应 Repository（仓储），不能下沉到业务任务。
- Raw 表是不可变事实层：接口响应先完整写入对应 `*_raw` 表，再做 Standard 转换。不得更新、覆盖、删除历史 Raw；修复通过新增版本或重新生成下游层完成。
- 新增数据源必须同时登记 Raw 表、业务键、来源、抓取时间、幂等规则、Standard 映射和 `dev_doc/current_data_model.md`（当前数据模型文档）。
- Research、Backtest 和 Strategy 只能消费明确绑定的 `FROZEN`（冻结）快照/数据集，不得直接读取实时 PIT/Feature 作为正式回测输入。
- Observation 的行业输入是可选增强，不是当前 Research/Backtest 的必需依赖。历史行业归属不可用时使用 `NULL/UNKNOWN`（空值/未知），不得用当前行业倒填；只有未来明确开发行业横截面策略时，才增加该功能自己的门禁。

## 二、数据库与数据语义纪律

- 所有专业标识符首次出现时必须同时写中文业务含义：表名、字段名、Schema（模式）、模块、接口、类、任务名等均适用。
- Token（接口令牌）只从环境变量读取，不写入代码、文档或日志。
- PIT 只能使用业务可见时间：公告、实施日等官方字段按契约截断；`fetched_at`（本地抓取时间）不能冒充官方可见时间。
- `stock_st`（ST/风险状态）不能直接等同于不可交易；可交易性须结合停牌、涨跌停和行情事实判断。公司行为只有在业务可见日满足条件时才能进入 Feature。
- 策略层读取指定 `snapshot_id`（研究快照标识）的批量结果，禁止逐股票 N+1 查询，也不重新扫描五年历史。Raw 只用于转换、审计和重建。
- 数据缺失必须与否定结果区分：`UNKNOWN` 表示无法判断，不能写成 `FALSE`。

## 三、大数据任务与事务安全纪律

- 百万级以上表操作必须先用 `EXPLAIN` 验证日期/业务键索引路径；禁止用跨日期列的 `CASE/OR` 破坏索引。预期为零的异常优先使用精确谓词和部分索引。
- 大任务必须按日期或业务范围拆成有限批次；每批独立事务、及时提交，并具备 checkpoint、心跳、幂等键和失败重试。禁止恢复全量长事务。
- 禁止把全局 `COUNT/MAX/GROUP BY` 作为启动前置检查；优先使用 Manifest（清单）、索引范围、预聚合或分批统计。
- 禁止使用全局 `TRUNCATE` 或一次性删除/重建共享表；清理必须限定明确业务范围。不得用多线程直接向 PostgreSQL（数据库）共享表堆写解决性能问题。
- 同一数据范围只能有一个受控写入者。启动后台任务前确认同类进程只有一个；内存、IO 或数据库活动查询持续升高时暂停并检查进程、锁、活动事务和错误日志。
- checkpoint 只能在对应物理数据仍可恢复时跳过。临时表因进程/连接重启丢失时，必须清除对应旧游标并从首个分块重新装载；运行状态重置后不得被旧“已完成”检查点静默跳过。
- 代码或配置修复后，先核对影响层级、输入输出、代码版本和 checkpoint 范围；上游输入未变化且已验证时复用结果，不得无条件从头重跑。
- 任务完成必须同时核对进程退出、数据库状态/检查点、错误日志、目标行数/日期范围、覆盖率、哈希和主键约束，不能只看本地进度日志。

## 四、Tushare（数据接口）与日常增量

- 日常生产增量统一运行 DAG（有向无环任务流），不逐个手动触发：

```powershell
uv run python tools/run_pipeline_once.py `
  --pipeline tushare_production_daily `
  --run-date YYYYMMDD `
  --no-dag
```

- 最近五个交易日允许受控回补；正式行情增量按交易日批量请求全市场，Raw 追加不可变版本，Standard 按受影响日期重建。历史初始化与日常增量不得混用。
- 空响应不能直接当作完整无数据；必须记录 checkpoint，并仅重试明确的空状态范围。
- 低频真实接口抽样只用于审计，不进入主流程：
  - `tools/audit_tushare_sample.py`（Tushare 与 Raw 对账）
  - `tools/probe_tushare_index_member_all.py`（行业成分接口只读探针）
- `index_member_all`（申万行业成分接口）当前实测可返回当前成员，但没有可用的非最新历史记录；不能据此构造五年行业 PIT。未来每日采集也只能先标记 `DAILY_CAPTURED`（每日采集），通过连续性和覆盖率门禁后才能升级为 PIT 证据。

## 五、任务登记、验证与权威文档

- 所有持续性工作先在 `dev_doc/agent_tasks.md` 登记：目标、状态、当前结果和下一步保持简明；完成后再标记已完成，外部条件真正阻断时才标记已阻塞。
- 数据访问改动必须运行契约扫描：

```powershell
uv run python tools/check_data_access_contract.py
```

- 新增/修改逻辑必须同步补充单元测试；真实数据库或外部接口行为补充集成测试，并重点验证 N+1、大事务、幂等、超时回滚、重试边界、权限隔离、PIT、FROZEN 不可修改和失败即关闭。
- 详细约束以专门文档为准：
  - `dev_doc/data_access_kernel.md`：数据访问框架
  - `dev_doc/数据基座架构拆分方案.md`：数据基座分层与迁移
  - `dev_doc/current_data_model.md`：当前表模型和来源映射
  - `dev_doc/market_anomaly_observation.md`：观察域契约
  - `dev_doc/research_snapshot.md`：研究快照契约
  - `dev_doc/backtest_design.md`：回测契约
  - `dev_doc/portfolio_design.md`：组合模拟与审批边界
  - `dev_doc/project_completion_runbook.md`：生产执行顺序

## 六、常用目录边界

- `data_collect/data_access/`：数据访问内核、Gateway（网关）、Policy（策略）和 Repository（仓储）。
- `data_collect/normalize/`：接口响应到 Standard 模型的纯转换。
- `data_collect/observation/`：Observation 纯计算与行业可选确认。
- `data_collect/backtest/`、`data_collect/portfolio/`：不直接访问数据库/券商的纯 Python 模型。
- `data_collect/jobs/`：任务编排和受控入库入口；不得在任务中绕过 Repository。
- `sql/`：版本化数据库迁移；修改模型时同步更新当前数据模型文档和测试。

## 2026-09 Research Snapshot 血缘优化规则

- Research Snapshot（研究快照）血缘逻辑仍固定为十个字段组；Foundation-v2 新快照使用按月分区的 `research.snapshot_lineage_bundle`（紧凑血缘包）和 `research.lineage_source_record`（来源记录字典），旧 `research.snapshot_lineage`（旧血缘表）仅作兼容读取。
- v2 血缘包哈希必须基于十个来源内容身份哈希，不得依赖自增 `source_ref_id`；来源字典写入使用事务级互斥，保证不同快照系列并发构建时引用解析稳定。
- 紧凑血缘切换只重建 Research Snapshot 子表；Raw（原始层）、Standard（标准层）、PIT（时点层）和 Feature（特征层）是只读输入，不得因快照物理优化而重跑。
- 任何大表优化先做有界分批、短事务、Explain（执行计划）和等价性校验；不可因速度问题绕过 Repository（仓储）→ DataAccessKernel（数据访问内核）、冻结保护或输入哈希。
- 旧格式迁移必须直接读取已存在的 `research.snapshot_lineage`（旧血缘表）并按有限代码批次聚合到 v2；不得调用完整 Snapshot（研究快照）构建器，不得改写 `stock_snapshot_daily`（股票快照表）或上游层。来源不完整时整批回滚并失败关闭。
- 没有既有股票快照/旧血缘的 `DRAFT`（草稿）日期不属于格式迁移，必须走正常的 v2 Snapshot 构建路径；迁移入口不得为了“补齐”而隐式重建上游数据。
- 旧血缘迁移按有限 `ts_code`（证券代码）集合分批；代码集合必须以可下推到 `(snapshot_id, research_date, ts_code, field_group)` 索引的连接条件进入 SQL。禁止使用只在索引扫描后过滤的数组谓词，避免同一交易日旧血缘被每个代码批次重复扫描。
- 格式迁移的验收必须验证旧血缘十组、紧凑血缘包行数和来源字典引用；不得用当前版本的 `snapshot_input`（快照输入清单）定义反向否定历史快照，也不得为了通过验收补写或改写历史输入清单。
