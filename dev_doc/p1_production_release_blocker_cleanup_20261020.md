# P1 生产 Release 阻断清理报告

- 项目：`ashares_data_collect`
- 核查日期：2026-10-20
- 研究区间：2021-08-02 至 2026-08-21
- 生产写入：未执行
- 正式 Release / Snapshot / Dataset：未创建
- 正式策略审批、部署、五年全量重算：未执行

## 结论

| 阻断项 | 状态 | 结论 |
|---|---|---|
| A. `600699.SH` 公司行为 | **BLOCKED** | 生产记录仍为 `UNRESOLVED`。找到公司公告的转载文本和第三方除权日记录，但没有找到足以替代原始官方事件证据的材料；不改分类、不补日期。 |
| B. 2021-06 Feature 覆盖 | **PASS（隔离小样本） / NOT RUN（整月恢复）** | 173 行对应的物化范围止于 2021-08-01，正好是研究信号开始前的 warmup 截止。来源中已有更多在 2021-08-21 前可见的报告，但还未物化到 Feature。10 个真实证券在隔离库按 2021-08-21 截止重算，字段与来源 lineage 对账一致。 |
| C. PIT 时间戳缺失 | **PASS（来源归因） / BLOCKED（生产投影）** | 20,469 条 Standard 缺时间戳记录均能关联到 Raw 版本；20,448 条严格匹配，另 21 条只有空 `end_type` 被 Standard 写成字符串 `NaN`。Raw 有公告日期，但只能形成 `SOURCE_ANNOUNCEMENT_ASSUMED` 假设，不能证明历史仓库当时已持有数据。 |
| D. 迁移与重算准备 | **PASS（隔离迁移与样本重算） / NOT RUN（生产兼容性）** | 新隔离数据库应用 119 项迁移，包含迁移 116；真实 Standard 小样本经正式 Feature Job 重算成功。生产迁移、锁竞争和权限切换未执行。 |

**正式 Foundation Release 当前仍不具备创建条件。** 公司行为门禁仍有 1 条全库未决，生产缺少迁移 116 和新版 Feature 物化；PIT 时间证据也没有生产版重投影。完整 `ReleaseRepository.quality_gate()` 本轮未运行。

## A. `600699.SH` 公司行为

生产只读 Repository 返回的范围审计如下：

| 项目 | 结果 |
|---|---:|
| 缺 `ex_date` 的全库已实施记录 | 31 |
| 生命周期证据确认范围外 | 26 |
| 有正式证据说明当前价格模型不适用 | 4 |
| 零现金/股本影响 | 0 |
| 范围内未决 | 0 |
| 全库未决 | **1** |

未决记录为 `600699.SH`，`source_record_key=600699.SH:1993-12-31:实施`，`record_version=69862`。生产 Standard 只有 `implementation_announce_date=1994-04-02`，没有可核实的 `record_date`、`ex_date`、`pay_date` 或 `div_list_date`。当前 `corporate-action-ex-date-evidence-v2` 因此继续返回 `UNRESOLVED`。

公开检索找到的新浪转载公告文本称该次 1993 年分配是每 10 股现金分红 1.20 元，并列出部分股东领取现金的日期；东方财富个股日历列出登记日和除息日。这些资料显示事件很可能早于 2021 年研究起点，但前者不是发行人/交易所原始档案，后者是第三方日历，不能据此升级正式证据或推测填入日期。[新浪转载公告](https://m.cj.sina.cn/page/aHR0cDovL2ZpbmFuY2Uuc2luYS5jb20uY24vc3RvY2svY29tcGFueS9zaC82MDA2OTkvMjQvNC5zaHRtbA?from=redirect)；[东方财富个股日历](https://data.eastmoney.com/stockcalendar/600699.html)

**处理决定：保持 `UNRESOLVED`。** 需要取得可追溯的发行人/交易所原始公告、正式定期报告或登记结算记录，证明现金派发及相关权益生命周期在研究区间前结束；在此之前，Release Gate 仍应阻断。未修改证据分类器，也未运行完整 Release Gate。

## B. 2021-06 Feature 覆盖

### 实际覆盖与截止日期

生产数据按报告期 `2021-06-30` 对账：

| 数据层 / PIT 截止 | 行数 | 证券数 | 说明 |
|---|---:|---:|---|
| 收入 Standard 总量 | 9,652 | 4,881 | `report_type='1'`，公告时间字段完整 |
| 收入 Standard，至 2021-08-01 可见 | 206 | 173 | 按现有代码在 `2021-08-02 00:00 Asia/Shanghai` 前截断 |
| 现有 `company_financial_v2` | 173 | 173 | 173 行都在上述 warmup 截止前可见；晚于截止的 Feature 行为 0 |
| 收入 Standard，至 2021-08-21 可见 | 2,357 | 1,248 | 按 `2021-08-22 00:00 Asia/Shanghai` 截断 |
| 现有 `company_financial_v2`，至 2021-08-21 可见 | 173 | 173 | 没有随信号窗口扩展而重算 |

因此，**173 并非源财报总覆盖，也不是 4,708 条来源缺失**。它是只物化到 2021-08-01 后的 Feature 覆盖。截止 2021-08-21，另有 1,075 只证券存在已公告的六月收入 Standard，却没有相应报告期 Feature。该 1,075 是候选物化缺口；是否每只都能产出完整 ROE/增长因子，仍取决于其他报表输入、质量和 PIT 条件，不能当成 1,075 条保证有效的因子。

### 构建运行与源码依据

- `ops.foundation_rebuild_run` 中没有覆盖 2021-06-30 的正式 Feature Run 记录。
- `ops.run_manifest` 中找到 3 条 `tushare_foundation_features` 写入记录，scope 均为 `date_from=2016-08-01`、`date_to=2021-08-01`，分别记录写入 68,792、68,792、90,316 行。scope 证实历史重算窗口截至 warmup 日期；本地 `run_logs`、`runtime_logs`、`logs` 没有找到能补充说明这批重算为何只截至该日的日志。
- `FinancialRepository.load_events()` 用 `output_end` 同时约束报告日期和公告可用截止；`rebuild_company_financial_v2()` 将该 `output_end` 传入读取与计算路径。当前算法会排除截止日之后才公告的报告。这符合 PIT 规则，但 warmup-only 的截止日期不能代替信号窗口末日。

**归因：已确认物化 horizon 截短；未证明某个计算过滤器或原始财报缺失导致 173。** 需要按真实信号日期范围（而非 warmup 结束日）重新物化，才能恢复该研究窗的历史可见 Feature。生产整月重算未执行。

### 隔离环境真实 Standard 重算

创建了独立测试容器 `ashares-p1-june-recompute-postgres`，环境 ID `41f64181-fd27-4d6a-a235-20adc2dee3b7`，数据库 `ashares_test`，端口 `15435`，卷 `ashares_p1_june_recompute_pgdata`。从生产 `ashares_app` 的只读 Repository 导出真实 Standard 事件夹具，输入按 `REAL_STANDARD_SAMPLE_TEST_ONLY` 标记，不包含 Raw 导入，也不构成正式 Release。

在截止日严格设为 2021-08-21 的 10 证券样本中：

- 导入 1,086 条真实 Standard 事件：income 289、balance sheet 294、cash flow 257、indicator 246。
- 用正式 `rebuild_company_financial_v2()` 生成 10 行；用正式 `rebuild_debt_to_assets_features()` 生成 20 个来源版本行（10 只证券、每证券 2 个可用时间版本）。
- 数值、状态、来源键、版本、payload hash、可用时间及 source fingerprint 与同一输入在正式计算函数上的结果逐字段一致，0 差异。
- 这 10 行均标记 `SOURCE_ANNOUNCEMENT_ASSUMED`。整体 Feature `quality_status=VALID` 不等于每个因子都能通过 Alpha 门禁：ROE 原因 9 条为 `SOURCE_COMPONENT_QUALITY_NOT_VALID`、1 条为 `NEGATIVE_OR_ZERO_EQUITY`；增长状态 8 条为 `SOURCE_COMPONENT_QUALITY_NOT_VALID`、1 条为 `INSUFFICIENT_HISTORY`、1 条为 `VALID`。D/A 的 20 个版本行均为 `VALID`。

另外用 2021-09-30 截止的独立 10 证券样本重算 10 行财务 Feature 和 20 行 D/A Feature，同样逐字段一致。该样本用于验证较长读取尾部的代码路径，不计入上述 2021-08-21 截止样本。

两份压缩样本 Bundle 的 SHA-256：

- `p1-june-financial-standard-v1`：`3c8df2638f01705b2e1893a1f3674fa52831362774357300847ad8dec0466672`
- `p1-june-available-by-2021-08-21-v1`：`34c19e48ea1226301cd3e74a27be70876370e83137b6a58610e1721a645b5819`

隔离库完整应用 119 项迁移（用时约 209 秒），包括迁移 116；导入后数据库约 29,957,143 bytes。第二次迁移检查为 119/119 已应用、0 项新增。容器现已停止，命名数据卷保留。该结果证明迁移及小样本重算在隔离环境可运行，不证明全月/全历史覆盖已经恢复。

## C. 20,469 条 PIT 时间戳缺失记录

生产只读审计按年份分块，在 `record_version` 对应 Raw 行后，逐项核对证券、报告日、报告类型、合并类型、end type、payload hash、公告日期和其他 Raw 版本：

| Standard 来源 | 缺时间戳 | 严格 Raw 身份匹配 | 仅 `end_type` 空值编码不同 |
|---|---:|---:|---:|
| balance sheet | 8,184 | 8,183 | 1 |
| cash flow | 5,946 | 5,942 | 4 |
| income | 6,339 | 6,323 | 16 |
| **合计** | **20,469** | **20,448** | **21** |

21 条记录的 Standard `end_type` 是字面字符串 `'NaN'`，Raw 对应值为 SQL `NULL`；证券、报告日、报告类型、合并类型和 payload hash 相同。没有发现证券/报告日期/报表类型/合并类型/payload hash 冲突。找不到 Raw 业务键的记录为 0；其他 Raw 版本命中为 0；多版本歧义为 0；公告日期冲突为 0；Standard 必需 lineage 缺失为 0。21 条的原始 Raw 行也有公告日期。

当前 `tushare_financial_raw_to_standard.transform()` 对有公告日期的记录使用 `COALESCE(f_ann_date, ann_date) + 1 天` 生成上海时区日界的可用时间，并标为 `SOURCE_ANNOUNCEMENT_ASSUMED`；只有无公告日期时才会退回抓取时间。现有 Raw 公告日期支持的是这个**显式公告日假设**，不是 2015/2016 年仓库已观察到该记录的证明。`fetched_at` 不能作为历史 PIT 时间，本轮没有把它投影为可用时间。

**根因分类：Raw 来源不存在不是阻断；当前 Standard/PIT 投影缺字段及 21 条空值编码不一致是阻断。** 20,469 条可进入版本化重投影候选清单；21 条应在新投影中把空值编码规范为 null，同时保留原始 payload/hash 和 source record identity。历史仓库观察时间证据仍不存在，正式报告必须保留公告日假设等级。生产 Standard/PIT 未写入，也未回填旧 Feature/FROZEN。

## D. 迁移与生产重算准备

生产只读查询确认 `to_regclass('feature.debt_to_assets_v1')` 为 null，迁移 116 尚未部署。迁移 116 在空白隔离库成功创建独立 D/A 表及索引、为 `research.stock_snapshot_daily` 添加 D/A 溯源列、更新 Feature registry 并授予应用角色所需权限。隔离库中的正式 Job 能以 `ashares_app` 写入 Standard 夹具和 D/A/财务 Feature，表明最小测试权限路径可用。

### 生产风险与恢复方案

- 迁移 116 对 Snapshot 表执行多列 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`。列无默认值，通常无需重写已有行，但仍要取得表级强锁；若存在长事务或高并发，会等待或阻塞。生产操作应设置短 `lock_timeout`，在低流量窗口执行并监控锁队列。生产锁竞争未测试。
- 新表和索引为空表创建成本低；生产 Snapshot 根表当前是带分区/大数据链路的一部分，不能据空库迁移时间推断生产锁时长。
- 迁移采用加法式 DDL；恢复策略应为停用新 Feature Job、保留新增列/表并做前向修复，不应靠生产 `DROP TABLE/COLUMN` 回滚。
- 生产版本还需先处理 PIT 投影身份与重算截止日：`warmup_start`、`output_start` 和实际信号 `output_end` 必须分开；当前 `output_end` 需要覆盖信号日期，不能仍停在 2021-08-01。
- 本轮没有生产迁移、DML、Feature 写入、Release `quality_gate()` 或 Snapshot/Dataset 操作。

## 状态与剩余阻断

| 验收项 | 状态 | 后续证据/动作 |
|---|---|---|
| `600699.SH` 生命周期与权益结束日期 | **BLOCKED** | 获取官方源证据；否则维持全库 unresolved=1。 |
| 2021-06 Feature 物化原因 | **PASS（截止归因）** | 已确认旧产物截止 2021-08-01；整月需要按实际信号窗口定向重算并做资格分类。 |
| 真实 Standard → Feature 隔离重算 | **PASS（20 个样本证券，分两个截止窗口）** | 不代表全月 Feature 有效率，也没有运行 Snapshot/Release 发布。 |
| PIT 缺时间戳 Raw lineage | **PASS（20,469 条均有 Raw 版本；21 条空值规范差异）** | 可生成公告日假设投影；不等同仓库历史观测证据。 |
| 生产 PIT 重投影 / Feature 分批物化 | **NOT RUN** | 需先确定版本化投影身份、目标范围、检查点与回滚方案并授权。 |
| 迁移 116 隔离应用 | **PASS** | 119 项迁移已应用；测试库复查新增迁移 0。 |
| 迁移 116 生产锁/兼容性 | **NOT RUN** | 需在部署窗口做 schema/锁/权限预检；本任务禁止生产迁移。 |
| 全量 Release `quality_gate()` | **NOT RUN** | 当前存在明确公司行为未决，不具备正式发布判断条件。 |
| 正式 Foundation Release | **BLOCKED** | 公司行为未决、生产迁移未部署、v2.2 Feature 尚未物化。 |

### 下一批最小变更顺序

1. 取得 `600699.SH` 的官方原始公告/登记结算证据；若无法取得，接受 Release 继续被阻断，不猜测日期。
2. 在部署计划中固定迁移 116 的 `lock_timeout`、执行窗口、运行身份和失败后的前向恢复步骤；先在生产只读预检中验证依赖和角色，再单独授权迁移。
3. 为 20,469 条 Raw 命中生成新的 PIT/Standard 投影版本；严格区分 `SOURCE_ANNOUNCEMENT_ASSUMED` 和 `WAREHOUSE_OBSERVED`，规范 21 条 `NaN` 空值表示。不更新旧 FROZEN/旧研究报告。
4. 按真实研究区间分批重建 Feature：warmup 只用于计算依赖，Feature 可用截止应覆盖每批信号结束日；分开记录来源可用、组件质量拒绝、Feature 生成和 Alpha 因子有效数。
5. 公司行为门禁解除后，生成新质量报告/基线并运行完整 Release Gate；之后才创建统一 `research-v2.2` Snapshot/Dataset。

## 本轮代码与验证

新增了有界的 PIT Raw lineage 诊断、字段级冲突细分、确定性 Feature 重算候选和显式截止日比较 Repository 方法，并添加对应单元测试。没有修改业务计算公式、PIT 门禁、正式策略或生产数据。

- `tests/test_historical_coverage_audit_repository.py`：**4 passed**。
- Data Access Contract：扫描 258 个文件，0 violations，**PASS**。
- Paper Trading Boundary：扫描 317 个 Python 文件，未发现 QMT 交易 API，**PASS**。
- Ruff：**NOT RUN**，当前环境未安装 `ruff` 可执行文件。
- 既有 P0 83 Snapshot E2E：**本轮未重跑**，沿用验收报告 [`research_v22_real_financial_window_clean_e2e_20261020.md`](research_v22_real_financial_window_clean_e2e_20261020.md)。
