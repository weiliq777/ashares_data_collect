# P2-A 历史财务 Feature 隔离重算与 P2-B PIT 投影准备验收

- 项目：`ashares_data_collect`
- 核查日期：2026-10-20
- 目标报告期：2021-06-30
- 实际研究信号窗口：2021-08-02 至 2021-08-20
- 最后信号时点：2021-08-20 15:00（Asia/Shanghai）；2021-08-21 为周六
- 生产数据库：只读查询
- 隔离测试数据库：`ashares_test`，环境 ID `04714bee-339e-4b19-b674-a67176190327`
- 生产写入、生产迁移、正式 Release、FROZEN 修改：未执行

## 结论

| 工作 | 状态 | 结论 |
|---|---|---|
| P2-A：2021-06-30 报告期 v2.2 财务 Feature 重算 | **PASS（隔离环境）** | 使用真实 Standard 历史输入和正式 Feature Job，截止覆盖到最后信号日；输出与独立只读计算参考逐字段一致。 |
| 四个财务因子数据资格 | **PASS（分类与证据完整）** | 四因子均有可解释的有效、质量拒绝或缺值状态；Alpha 有效率仍受上游来源质量影响，不能据此宣称因子有效。 |
| 确定性重跑与恢复 | **PASS（隔离环境）** | 固定输入/代码身份、确定性 Run ID、批次检查点；模拟末分区失败后只重算该分区，输出哈希保持不变。 |
| P2-B：20,469 条缺 PIT 时间戳记录归因清单 | **PASS（只读准备）** | 全部有精确 Raw 版本和公告日期；21 条只存在 `end_type` 的 NaN/NULL 规范差异。 |
| P2-B：版本化投影小批验证 | **PASS（隔离临时表）** | 21 条差异记录加 3 条严格匹配对照经正式 Raw→Standard 转换，结果读回一致；临时表提交后删除。 |
| P2-B：生产 Standard/PIT 重投影 | **NOT RUN / BLOCKED** | 尚未确定不覆盖既有 Standard/PIT 的正式版本化存储身份；当前同源 upsert 会覆盖相同业务键，不能直接执行。 |
| 正式 Foundation Release | **BLOCKED** | `600699.SH` 公司行为仍未决；生产迁移 116、新版 Feature 物化和完整 Release Gate 均未完成。 |

P2-A 已证明“物化范围曾止于 warmup 截止”可以在隔离环境按正确信号截止日恢复。它没有证明五年历史 PIT 都是仓库当时实际观察值，也没有证明四个因子具有预测力。

## P2-A：2021-06-30 完整报告期重算

### 范围与输入

| 参数 | 值 |
|---|---|
| `warmup_start` | 2015-01-01 |
| `output_start` | 2021-06-30 |
| `output_end` | 2021-08-20 |
| `signal_start` | 2021-08-02 |
| `signal_cutoff` | 2021-08-20 15:00 +08:00，严格小于此时点的记录才可见 |
| 财报目标报告期 | 2021-06-30 |
| 证券代码范围 | 975 个输入证券代码 |
| 输入 Standard 事件 | 89,495 条 |

生产来源读取身份经连接检查为 `ashares / ashares_app / transaction_read_only=on / superuser=false`。输入数据经现有 `FinancialRepository` 导出、导入全新隔离库，仓库读回 89,495 条，内容哈希一致；未将 Raw 抓取时间用作 PIT 时间。

按 2021-08-20 信号时点可见的目标报告期来源：收入 956 只证券，资产负债表 972 只，合并 975 只。输入中目标报告期 Standard 行数为：income 1,773、balance 1,773、cashflow 1,771、indicator 1,818。财务 Feature 重算使用正式 `rebuild_company_financial_v2()` 与 `rebuild_debt_to_assets_features()`。

隔离数据库使用独立实例 `p2-june-full-recompute`，迁移 119/119 已应用（包含 116）。导入和物化后的数据库大小为 95,558,679 bytes；迁移完成、导入前为 27,466,775 bytes。输入 Bundle 压缩后为 10,961,244 bytes。迁移运行耗时 193.083 秒；Feature 全流程分阶段耗时没有被原 Run 记录，故记为 **NOT MEASURED**，不从文件时间差推定精确运行时间。

### 生成与对账

| 输出 | 版本行数（截至信号时点） | 证券数 | 参考对账 |
|---|---:|---:|---|
| `company_financial_v2` 目标报告期 | 957 | 956 | 957/957 匹配，缺行 0、多行 0、字段差异 0 |
| `debt_to_assets_v1` 目标报告期 | 1,773 | 972 | 1,773/1,773 匹配，缺行 0、多行 0、字段差异 0 |

财务输出与独立参考的数值、报告类型、合并类型、来源键、版本、Payload Hash、质量、逐因子 lineage、来源指纹及 PIT 字段逐项一致。对 `source_observed_at` 仅按原始墙上时间核对，因为 Standard 的追踪列为无时区时间、Feature 列为带时区时间；该列是来源观察追踪信息，不是 PIT 可用时间。

旧 173 条 Feature 以 2021-08-02 15:00 为同一截止基线复核：

- 173 个证券均包含在新重算结果中。
- 截至最后信号时点共有 956 个财务 Feature 证券，其中 783 个未出现在旧 173 证券集合内。
- `roe_ttm`：164 个非空值逐值相同，9 个两边均缺值。
- `revenue_growth_yoy`：172 个非空值逐值相同，1 个两边均缺值。
- `profit_growth_yoy`：144 个非空值逐值相同，29 个两边均缺值。
- D/A：173 个值均满足新独立因子值 `round(旧比例值 × 100, 4)`。这是 `debt-to-assets-v1` 的百分数单位契约，不是财务数值重新计算偏差；旧记录保持未改。
- 新旧 Feature 的 lineage 与 source fingerprint 身份不同，符合新版 `financial-source-quality-v1` 与逐因子 `used_by` 证据契约，不能把它当作字节级旧版本复现。

### 四因子资格统计

分母为截至 2021-08-20 15:00 可见的相应来源证券；“生成”表示计算输出存在，不等同 Alpha 因子有效。

| 因子 | 来源证券 | Feature 生成 | Alpha VALID | INVALID | MISSING | 主要拒绝/缺失原因 |
|---|---:|---:|---:|---:|---:|---|
| `debt_to_assets` | 972 | 972 | 971（99.90%） | 1 | 0 | 1 条来源质量不是 VALID；971 条 PIT 证据等级仍为 `SOURCE_ANNOUNCEMENT_ASSUMED` |
| `roe_ttm` | 956 | 956 | 20（2.09%） | 936 | 0 | 929 条财务组件来源质量不合格；7 条 ROE 自身质量状态不合格 |
| `revenue_growth_yoy` | 956 | 956 | 146（15.27%） | 809 | 1 | 809 条财务组件来源质量不合格；1 条增长值缺失/非有限 |
| `profit_growth_yoy` | 956 | 956 | 134（14.02%） | 809 | 13 | 809 条财务组件来源质量不合格；13 条增长值缺失/非有限 |

公司 Feature 的 957 个版本行和 D/A 的 1,773 个版本行均以 `SOURCE_ANNOUNCEMENT_ASSUMED` 作为 PIT 证据。组件质量计数为 lineage 组件出现次数（VALID 13,265、WARNING 8,151），不是证券数。不得把假设级时间证据表述成 2021 年历史仓库观察记录。

### Run 身份、幂等与恢复

- 批次版本：`p2-financial-feature-2021-06-v1`
- 批次标记：`P2_JUN21_4cb565a2`
- 批次哈希：`4cb565a269a8ade8a5130433720792833589a55991063735d7464e97ca265f5b`
- 输入 Bundle SHA-256：`e2f0b71127ea880f8546c50890078814e0f5b1e16271d7a2ef2adb8f1728bd9f`
- 输入 Manifest SHA-256：`711bd1553e34126d45184178eeb964df96e988aa772c0366ebee1f517717f4ea`
- 输入 Standard 事件内容哈希：`0e16a52a2ed5e3f19c013bfc3d6bc6b9f4ca9debac2c731799d5f601ad9a4376`
- 计算代码身份 SHA-256：`38276c1bbdf62b3ce8ea649c2d3bfc54eaefd1f3c52b9486bbeb7bce8bb2fc6a`
- Run ID：`4152f0e3-97ad-52c5-bfd0-625a22f5fda0`
- 公司 Feature 输出哈希：`b93b71152bb8c812f3080222446f2e80f8c48831b5e8a2d8c1a7e10fc38d2102`
- D/A 输出哈希：`70588bb6e11f362baaa4ae4024c666b5257ca228a83c453ef2c1fd29c954e4b3`

运行使用 5 证券分区，共 195 个分区。再次执行时已完成分区不重写；恢复演练将公司 Feature 与 D/A 的末分区分别标记失败，恢复只处理 3 行与 4 行，逐项重新对账通过，两个输出哈希不变。旧 FROZEN、旧 Snapshot、正式 Release 未触碰。

## P2-B：历史 PIT 时间戳缺失投影准备

### 全量候选清单

使用只读生产 Repository 按来源、年度和 Standard ID 游标分页；每页最多 1,000 条。目标区间 2015-01-01 至 2021-08-01，未执行无界全表扫描。

| 来源 | 候选记录 | Raw 严格匹配 | 仅 end_type NaN/NULL 规范差异 |
|---|---:|---:|---:|
| balance sheet | 8,184 | 8,183 | 1 |
| cash flow | 5,946 | 5,942 | 4 |
| income | 6,339 | 6,323 | 16 |
| **合计** | **20,469** | **20,448** | **21** |

其余归因：Raw 未命中 0、Raw 版本身份未决 0、公告日期缺失 0、Raw/Standard 公告日期冲突 0、规范化来源键冲突 0。21 条的证券、报告日、报表类型、合并类型及 Payload Hash 均与对应 Raw 记录一致；差异仅为 Standard 的 `end_type='NaN'`、Raw 的 SQL NULL。

候选投影规则沿用正式 `tushare_financial_raw_to_standard.transform()`：`COALESCE(f_ann_date, ann_date) + 1 个日历日`，上海时区当日零点，并保持 `SOURCE_ANNOUNCEMENT_ASSUMED`。`fetched_at` 只保留为 `source_observed_at` 追踪字段，不参与 PIT 时间计算。该规则表示公告日可见性假设，不证明历史系统当时已收到该版本。

新的清单身份：

- Projection 版本：`financial-source-announcement-projection-v2`
- 批次哈希：`83c479685d0950e36a8b435c3c333aa9e2c7ff45391217e62ac3105d6ba912b5`
- Manifest SHA-256：`0b3ccd749f3e042cf2c2de227079005c96d2032ae18a28be7e55beef279361d9`
- Bundle SHA-256：`6d34ffb43c5885b4d91ebbf2c5eff0a744c8029f265115971cc8b80db12fcabc`
- 候选内容哈希：`848235cc07224b15b9915318ce42173063b7c58ef090d65c4062e31d77b33577`
- 投影代码身份哈希：`2cff400304d75782fa6c8be2643e5aff6f0f0bfef233315756e4cb91af0df12d`
- 代码身份包含 Git HEAD `0e72b13b5be3c442a88af76d60a3743ebeb63675` 与 Raw→Standard、FinancialStandardRepository、历史审计 Repository 三个文件的 SHA-256。

### 隔离小批投影

精确读取全部 21 条 NaN/NULL 差异记录的 Raw Payload，再从三个来源各取 1 条严格身份匹配对照，共 24 条。官方转换函数在隔离验证中生成投影，写入连接级临时表并回读；事务提交后临时表按定义删除。

- Raw ID、Source Payload Hash：24/24 精确匹配。
- 21 条 `end_type`：转换输出均为 NULL。
- PIT 时间戳：24/24 等于公告日期加一天、上海零点。
- PIT 证据：24/24 保持 `SOURCE_ANNOUNCEMENT_ASSUMED`。
- `fetched_at`：仅作为来源追踪，未被用作可用时间。
- 生产 Standard/PIT、隔离库永久 Standard/PIT 表：均未写入。

仓库新增了 `FinancialStandardRepository.raw_versions_by_ids()` 有界读取方法，仅允许三类财报来源、最多 200 个精确 Record ID，并要求报告日期范围；对应测试与领域契约通过。

### 生产投影阻断及实施准备

当前 `FinancialStandardRepository.upsert_standard_rows()` 的幂等键为 `(source, source_record_key, record_version)`，更新会写入已有行。若用原有 `source='tushare'` 对相同记录重投影，会覆盖旧 Standard；因此即使候选清单完整，也不能直接复用该 upsert 作为“版本化修复”。

生产写入前必须先确认并隔离投影身份：新版本需可追加、可按版本回读，且 Feature/Release 选择器能显式选择该版本。应先在迁移与隔离库测试中证明旧版可并存、失败可恢复、哈希可复核，再申请单独生产写入授权。任何实现都必须维持公告日期假设等级，不能将其提升为 `WAREHOUSE_OBSERVED`。

## 门禁与遗留阻断

| 项目 | 状态 | 说明 |
|---|---|---|
| 2021-06 报告期目标截止日重算 | **PASS（隔离）** | 使用 2021-08-20 15:00 的最后实际信号时点；不再将 2021-08-01 warmup 截止误作信号截止。 |
| 四因子数值、单位、来源与 lineage | **PASS（隔离对账）** | 对计算参考无差异；Alpha 因质量门产生的低有效率如实保留。 |
| PIT 缺失候选 Raw 归因 | **PASS（只读）** | 20,469 条全部有唯一 Raw 版本和公告日期；21 条仅需 NULL 规范化。 |
| PIT 版本化生产投影 | **BLOCKED / NOT RUN** | 新旧投影如何并存及被下游显式选择尚未定案；生产未写入。 |
| `600699.SH` 公司行为 | **BLOCKED** | 继续保留 `UNRESOLVED`，没有增加或推断任何除权日期。 |
| 迁移 116 生产兼容性、锁和权限切换 | **NOT RUN** | 仅隔离库已应用，生产迁移不在本批授权范围。 |
| 新 Foundation Release / 五年 v2.2 | **BLOCKED / NOT RUN** | 完整质量 Gate 未运行；本批未创建 Release、Snapshot 或 Dataset。 |
| 20/40/60 日 Alpha 预测价值 | **N/A（本批）** | 本批验收数据计算与来源契约，不作因子有效性结论。 |

## 验证结果

- `uv run pytest -q tests/test_financial_contract_repairs.py tests/test_historical_coverage_audit_repository.py tests/test_financial_feature_v2.py`：**65 passed**。
- Data Access Contract：扫描 258 个文件，0 violations，**PASS**。
- Paper Trading Boundary：扫描 317 个 Python 文件，未发现 QMT 交易 API，**PASS**。
- Ruff：**NOT RUN**，当前环境找不到 `ruff` 可执行文件。
- 先前通过的 83 Snapshot 真实窗口 E2E 本批未重跑，沿用 [`research_v22_real_financial_window_clean_e2e_20261020.md`](research_v22_real_financial_window_clean_e2e_20261020.md)。
- P2-B 候选清单分页导出约 8 秒；24 条精确版本投影验证约 2.5 秒。Feature 全流程分阶段耗时未记录。

## 产物与下一步

本地可复核产物保存在 `.codex/p2-batches/`：

- `financial-feature-2021-06-v1.json.gz`、`financial-feature-2021-06-v1.manifest.json`、`financial-feature-2021-06-v1.result.json`
- `financial-pit-projection-candidates-v2.json.gz`、`financial-pit-projection-candidates-v2.manifest.json`、`financial-pit-projection-sample-v2.result.json`
- `run_2021_06.py`、`export_pit_candidates.py`、`verify_pit_projection_sample.py`

下一项最小安全工作是：设计并隔离测试带显式投影版本身份的 Standard/PIT 追加式落点与下游选择，再在测试库对 20,469 条候选批次完整重投影及恢复演练。之后另行提交生产迁移与小批写入计划；在 600699.SH 未解决前，正式 Release 继续阻断。
