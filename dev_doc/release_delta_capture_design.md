# Release Delta 变更捕获设计与实施状态

## 目标

将来以一个已通过完整质量门禁的 `FULL` 数据发布作为父基线；日常只为新增/修订写入生成有界变更包，并对包内受影响范围运行质量检查；周期性执行完整审计。日常链路不得扫描五年 Raw、PIT 或 Feature 历史。

本阶段实现安全封包前提：在事实写事务内记录变更证据、用事务级 advisory lock（咨询锁）建立提交边界、将完整审计绑定到账本水位，并把有界事件封存为候选包。它不改变 Raw/Standard/PIT/Feature 数据，也不把候选包冒充成已验证基线。

## 深模块与唯一捕获接缝

`data_access.release_change_capture` 是唯一的变更描述模块，负责：

- 只接受 `public/raw/std/pit/feature` 数据关系，不记录 `ops`、Research/Backtest 或临时 staging；
- 从 `WorkScope` 和当前写入 chunk 推导日期/证券范围，并验证行是否越界；
- 对范围与输入生成稳定 SHA-256 身份；
- 范围无法证明有界时保留事件，但标记 `delta_eligible=false` 和全量审计原因。

Raw append、标准 Gateway 写入、SET-based rebuild 和最终 staging publish 在调用 `Kernel._run_chunk` / `run_atomic` 时附带事件描述。Kernel 在**同一个数据库事务、提交事实写入之前**插入 scope 与 event：

```text
事务级 advisory shared lock（共享锁）
    ↓
事实写/发布 SQL
    ↓
写入去重后的 ops.release_change_scope
    ↓
写入 ops.release_change_event
    ↓
一次 COMMIT
```

事件写入失败会回滚该事实事务；不会出现“事实成功、事件漏记”的静默窗口。重试身份确定，唯一键使重复事件幂等。scope 按哈希去重，证券代码列表不在每个事件中重复保存。

共享锁在事实写之前取得。完整发布审计起始水位和最终提交都取 exclusive lock（排他锁）：起始锁等待此前持锁的写事务提交；质量门禁及 Manifest（清单）检查期间如有后续事件，最终提交会发现水位变化并拒绝发布。审计不需要长时间持锁，也不会静默发布跨越写入时点的混合结果。

## 表与事务预算

`sql/110_release_change_capture.sql` 与 `sql/111_release_delta_packages.sql` 只新建/扩展小型控制面结构，不扫描/重写既有事实表：

- `ops.release_change_scope`：内容寻址的日期/证券影响范围，可被多个数据集事件复用；
- `ops.release_change_event`：一次已提交 Gateway 写入的目标、操作、RunContext、输入哈希和受影响行数。
- `ops.tushare_data_release.release_change_watermark`：完整审计在启用变更捕获后记录的账本边界。旧的已验证基线该值为空，不能直接作为增量父基线；需在捕获开启后重新完成一次完整质量审计。
- `ops.release_delta_package` / `ops.release_delta_event`：封存候选包及精确事件成员。单包最多 1000 个事件，事件键有序哈希；同一数据域内同一事件不能重复链接。

仓储入口 `ReleaseRepository.capture_delta_candidate()` 只封存候选；`delta_candidate()` 按 package UUID 有界读取清单和最多 1000 条事件证据，并核对事件成员数量。这里没有把 SQL/连接暴露给 Job。

按事务 chunk 记录，不按每个股票/字段记录；预计普通每日市场增量为几十至低百条事件量级，scope 复用可进一步减少范围存储。事件量最终应在小范围试运行后按数据库实测校准；暂不做自动清理，以免删掉尚未被任何增量包消费的证据。

写入额外成本是每个有实际变更的 chunk 最多两条小型 `INSERT ... ON CONFLICT DO NOTHING`，与事实写同一短事务；无历史大查询、无全表 hash、无 `string_agg(row_to_json(...))`。现有 chunk、事务时间/行数/字节预算保持不变。

## Fail-closed 规则

- `TABLE_REPLACE`、缺少可识别日期/证券键、超出最大事件证券列表或行越界，都会标记 `delta_eligible=false`；后续只能执行完整审计。
- 为避免“有日期边界但仍等于重扫多年”的伪增量，纯日期范围最多 31 天；超过 366 天且证券范围超过 100 只也会要求完整审计。单日全市场增量仍可按日期有界；少量证券的历史修订可按证券范围有界。
- `capture_release_changes` 默认关闭，可由 `DATA_RELEASE_CHANGE_CAPTURE=1` 显式开启；启用前必须部署 110/111 迁移，并保证所有能写 Raw/Standard/PIT/Feature 的生产进程同时运行支持共享 advisory lock 的版本、没有新旧版本混跑。启用后先对当前完整基线重跑一次完整质量审计，创建可信 watermark，之后才可封存候选包。
- 事件捕获完整不等于增量质量已通过。只有事件生产 Run 成功、事件范围被相应的表级质量规则完整覆盖、与父基线衔接且无并发遗漏后，才能建立并发布 `DELTA` 包。
- 候选包要求父基线当前仍为 `validated`、具有可信 watermark；包中全部生产 Run 必须为 `SUCCEEDED`，全部 scope 必须 `delta_eligible=true`，且事件数不超过 1000。条件不满足时事务回滚并要求完整审计。
- 包状态永久为 `CAPTURED`（已封存候选），不能被识别为 `VALIDATED`，也不会替换 `ops.tushare_data_release` 当前基线。现有完整发布路径仍是唯一正式路径。

## 必须完成的后续发布步骤

1. 在非生产环境应用 110/111 并启用捕获；生产部署前确认应用角色通过 Kernel 持有咨询锁并有相应控制表权限。
2. 捕获开启后重新执行一次完整质量审计，得到带 watermark 的当前完整基线。若审计期间源写入，提交会安全失败，需在写入静默窗口重试。
3. 已新增纯函数 `release_delta_impact.resolve_delta_impact()` 和 Repository（仓储）预检入口 `ReleaseRepository.plan_delta_impact()`。当前仅支持将 `std.daily_kline`（标准日线行情表）变更映射为 Market Activity（市场成交观察）截止日重检计划；默认只规划一个截止日和固定 45 个自然日输入窗口，默认输入预算 250,000 行。Research（研究）域、其他目标、范围不完整、非成功 Run 或超出截止日的事件均要求完整审计。预检只读取最多 1 个候选包、最多 1,000 条事件和 1 条父发布记录，不查询 Raw/Standard/PIT/Feature 事实表，不更改发布状态。
4. Candidate（候选包）的增量质量门禁仍未实现。规划结果 `BOUNDED_RECHECK` 不是质量通过或发布许可；也不支持把旧 cutoff 自动推进到新交易日。
5. Market Activity 当前无需另造“增量门禁”：它的完整发布审计本身已是单个截止日 + 固定 45 日输入窗口 + 最多 250,000 行的有界检查，可每天按新 cutoff 生成新的完整 Market Activity release（发布基线），不扫描五年历史。Research（研究）域仍需专门的影响解析和有界门禁；完成多日 shadow 对账之前不能实现其 DELTA 状态或自动发布。

Market Activity 完整发布的来源 Manifest 已改为 `market-activity-input-v2`：它绑定实际报告读取的 `std.daily_kline` 行窗口、精确输入哈希、报告哈希和质量摘要。旧版对 180 天 Raw 表做多个聚合的做法与观察计算实际消费输入不一致，已从该域移除。新 Manifest 证明的是报告直接输入，而不是重建 Standard 的全部上游血缘；Raw→Standard 的转换正确性仍由既有生产门禁负责。

## 影响计划的查询预算审查

- `plan_delta_impact()` 的 SQL 仅访问控制面表 `ops.release_delta_package`、`ops.release_delta_event`、`ops.release_change_event`、`ops.release_change_scope`、`ops.run_manifest` 和父 `ops.tushare_data_release`；事实表零扫描。
- 事件成员读取显式 `LIMIT 1001`，用于检测超过 1,000 条或包元数据不一致；事件 ID、包 ID、scope 哈希和 Run ID 均通过主键/索引关联。
- Market Activity 完整审计只流式读取 `cutoff - 45 days` 至 `cutoff` 的 `std.daily_kline`，按 `(trade_date, stock_code)` 分区索引顺序读取；SQL 有 `LIMIT max_bars + 1` 硬上限，默认最多返回 250,001 行用于检测越界，超限即失败。不会再扫描 180 天 Raw 历史。
- 该 45 日窗口与报告实际依赖一致，输入哈希逐行覆盖报告使用的股票代码、日期、收盘价、成交量和成交额。它不是全链路 Raw 血缘证明；如将来要求验证 Raw→Standard 的映射，必须另设按同一日期范围的、索引支撑的有界校验，不得悄悄恢复多年聚合。
- `plan_delta_impact()` 仅做控制面预检，不执行上述行流，也不会自动启动重算或发布。Delta 范围门禁和水位推进仍待完成。
- 目前候选包不可升级为正式发布；尤其不能把 `BOUNDED_RECHECK` 误读成日常新交易日已经完成。Market Activity 日常更新应走现有 `tushare_release create/validate` 完整路径，其单次数据范围已有硬上限。

## 回归范围

`tests/test_release_change_capture.py` 验证范围推导、越界 fail-closed、事件幂等、写事务锁顺序、完整审计水位封口、候选包精确成员及不合格输入拒绝。测试使用 Fake DB，不连接生产库。迁移只提供部署文件，不在本开发任务中执行。

## Research 发布质量门禁审计（2026-09-25）

- `ReleaseRepository.quality_gate()` 是完整 Research 发布审计；`FoundationQualityRepository` 将五年范围拆成最多 180 日窗口，估值覆盖预筛进一步拆成最多 60 日窗口。窗口限制单条语句的规模，不代表总工作量只覆盖最近数据。
- `phase2_invariants()` 中仅 `financial_future_rows`、价格特征成熟度、估值分位成熟度已有针对违规行的部分索引（迁移 088）。财务版本/年代元数据、PIT 血缘、交易可用性、标准证券状态、价格缺复权因子、Raw/Standard 对账等检查没有统一日期范围，完整发布时仍需全历史检查。Checkpoint（断点）表是控制面数据，虽按全表汇总，但规模与事实表不同。
- `security_lifecycle`（证券生命周期表）重叠检查及 `dividend_event`（分红事件表）重复键/缺少除权日检查为全表约束；不能只检查 cutoff 附近日期，因为历史区间冲突也会污染基线。
- Research Snapshot（研究快照）序列编排会共享一次基线质量证据，不应对每个交易日各自重跑整套发布门禁。单快照命令可能重新计算质量报告；批量生产继续使用共享证据入口。
- 本次发现 `implementation_without_ex_date`（已实施分红缺少除权日）虽被统计并写入报告，却未进入 `failures`（失败项）；已修复为阻断 Research 发布，并由 `test_research_release_gate_blocks_implemented_dividends_without_ex_date` 覆盖。修复只改变已有结果的判定，不增加数据库查询。
- Research 的增量门禁暂不实现：当前变更事件捕获为可选项，且已有影响解析器只证明 Market Activity（市场成交观察）直接输入的 45 日窗口。Research 规则涉及多个可变事实表及派生关系；在逐规则证明“受影响范围可完整枚举”、完成全量审计与增量审计的多日影子对账前，所有 Research 候选继续要求 FULL_AUDIT（完整审计），不可升级或发布。
- 本次仅做静态 SQL/索引审查与 Fake DB 测试，没有对生产数据库运行 `EXPLAIN`、全历史查询、索引构建或迁移。将来若优化完整审计成本，应在可代表生产数据规模的环境验证执行计划与资源预算；不得把日期过滤直接当成正确性证明。

### 逐类扫描范围

| 门禁类别 | 当前 SQL 范围 / 支撑 | 增量处理判断 |
|---|---|---|
| 财务 PIT（时点）未来版本、价格特征未成熟、估值分位未成熟 | 全表谓词；分别有 `088` 中的违规部分索引，谓词与门禁条件对应。`max_rows=2` 只限制返回行，不限制扫描行。 | 技术上可让索引缩小“零违规”检查，但未取得生产执行计划前不声称实际索引扫描；事件捕获不完整时仍做完整审计。 |
| 财务版本可用日/仓库观察时间、财务特征、交易可用性、证券状态、价格复权来源、财务血缘、Raw→Standard（原始层→标准层）财务对账 | 多张 PIT/Standard/Feature/Raw 事实表全历史谓词；部分有业务查找索引，但未见与全部违规谓词严格匹配的违规部分索引。`financial_feature_missing_revenue_mapping` 仅有截止日上界且 V2 会跳过。 | 上游到派生表的写事件与依赖覆盖尚未证明完整，全部保持 FULL_AUDIT。 |
| 生命周期区间重叠、分红业务键重复、已实施分红缺除权日 | `security_lifecycle` 自连接；`dividend_event` 分组与全表谓词。分别有生命周期代码区间索引、分红业务键索引；“缺除权日”无专用违规索引。 | 这些是跨历史完整性约束；仅查最近日期可能漏掉旧区间/重复键，保持完整审计。缺除权日计数现已实际阻断发布。 |
| Foundation v2（数据基座 V2）字段映射、PIT 证据、TTM（滚动十二个月）/估值质量与覆盖 | 五年窗口分成最多 180 日语句；估值覆盖先按最多 60 日分组指纹筛查，再对疑似日期精确 anti-join。每条语句有界，但全次审计仍会覆盖完整五年。 | 新基线完整验收保留全范围；不能把分段执行误当作增量验收。 |
| Checkpoint（断点）完整性 | 聚合整个 `ops.tushare_history_checkpoint` 并读取其记录日期跨度；控制表而非大型行情/财务事实表。 | 保留完整聚合，确认该控制表规模；不能据此推断事实表门禁也便宜。 |

表中“有索引”是静态 DDL（数据库结构定义）核对结果，不代表已验证 PostgreSQL 实际选用该索引。没有执行计划和代表性基数前，不新增覆盖索引：`CREATE INDEX CONCURRENTLY` 仍会读取整个目标表，生产建索引本身可能造成显著 I/O（磁盘读写负载）。
