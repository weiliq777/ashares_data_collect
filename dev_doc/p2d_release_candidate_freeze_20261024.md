# P2-D｜发布候选代码冻结验收

- 核查日期：2026-10-24
- 仓库：`F:/a_github/Finance_cn/ashares_data_collect`
- 当前分支：`agent/stage1-data-validation`
- HEAD：`0e72b13b5be3c442a88af76d60a3743ebeb63675`；与 `origin/agent/stage1-data-validation` 相同
- 结论：**BLOCKED；本轮未形成可提交的冻结版本，未提交、未推送、未部署**
- 生产数据库：本轮未连接、未写入

## 结论摘要

研究 v2.2、财务来源质量、独立 D/A 与 PIT 投影版本的代码身份可在当前源码中核对；116/117 的 SHA-256 与 2026-10-20 的 P2-C 报告一致。全仓默认 pytest、数据访问契约和纸面交易边界检查通过。

当前不能把整份工作区认定为冻结候选：工作区有 73 个未暂存路径；全仓 Ruff 有 1,349 项诊断，当前变更的 43 个 Python 文件中有 31 项相对 HEAD 新增诊断；真实财务窗口数据库 E2E 因 Docker 引擎不可用而未重跑。此前 2026-10-20 的 E2E 报告是历史证据，不绑定本轮工作区差异身份。

本报告记录问题和下一步，不对 Ruff 做自动修复，也不清理或覆盖任何既有工作区改动。

## 当前工作区身份

| 项目 | 结果 |
|---|---|
| HEAD / 远端 | `0e72b13b5be3c442a88af76d60a3743ebeb63675`，HEAD 与 origin 同步 |
| 暂存区 | 0 个文件 |
| 未暂存路径 | 73：28 个修改、2 个删除、43 个未跟踪 |
| 发布候选工作树差异摘要 | `1dc477a3b943abe9fa7efebc38bf48f02da2299697ec49ef1b9a90abef563e79` |
| 摘要范围 | 按核查时 73 个未暂存路径的状态、相对路径与文件 SHA-256 计算；不含本报告和 Git 忽略文件。后续任一候选文件变化后须重算。 |

当前源码包含以下契约标识：`research-v2.2`、`frozen-factor-set-v4`、`financial-source-quality-v1`、`debt-to-assets-v1`。这证明字符串与代码当前存在，不代表生产历史 Feature、Release 或 Dataset 已发布。

## SQL 迁移身份

| 顺序 | 文件 | 当前 SHA-256 | 检查结果 |
|---|---|---|---|
| 1 | `sql/116_independent_debt_to_assets_factor.sql` | `13effca380f96eec94000e07ad080b018a7af8ca3f8e32ca28d5a66705ffe421` | 与 P2-C 报告一致 |
| 2 | `sql/117_financial_standard_pit_projection.sql` | `e8662de091104cdabee2ec886d5d7d2389d3f6eb80c0d22869bbac958c0f162d` | 与 P2-C 报告一致；依赖 116 创建的 D/A Feature 表 |

生产迁移仍为 **BLOCKED**。P2-C 中的生产备份/恢复证明、主机磁盘余量、获准的 DDL 身份及变更账本尚未补齐；部署前还须重查锁和长事务。迁移哈希一致不等于生产部署获准。

## 检查结果

| 检查 | 状态 | 证据与限制 |
|---|---|---|
| 全仓默认测试 | **PASS** | `uv run --no-project -- python -m pytest -q`：1,571 passed，1 skipped，19 deselected，53.23 秒。pytest 默认通过 `pytest.ini` 排除 `integration` 标记。 |
| 黄金样本与 PIT 单元回归 | **PASS** | `tests/test_alpha_research_golden_sample.py tests/test_real_golden_reference.py tests/test_financial_pit_projection.py`：15 passed，2.05 秒。 |
| Data Access Contract | **PASS** | 扫描 259 个文件，0 violations。 |
| Paper Trading Boundary | **PASS** | 检查 320 个 Python 文件，未发现 QMT 交易 API。 |
| Ruff 全仓 | **FAIL** | 临时工具环境 Ruff 0.17.2；`ruff check .` 报 1,349 项。未修改项目依赖，也未自动修复。 |
| Ruff 当前变更文件归因 | **FAIL** | 43 个变更/新增 Python 文件：当前 148 项，HEAD 对照 128 项；按文件、规则和诊断消息匹配，117 项与基线相同，31 项是当前候选新增，另有 11 项基线诊断在工作区消失。新增规则主要为 ISC004、I001、FURB162、TRY004。 |
| Ruff 新增诊断主要文件 | **需处理/确认** | `historical_coverage_audit_repository.py`（7）、`research_snapshot_repository.py`（5）、`corporate_action_evidence.py`（3）、`financial_standard_repository.py`（3）、`export_real_financial_window.py`（3）、`alpha_research/factors.py`（2）；其余 8 项散见 8 个文件。FURB162 被 Ruff 标注为不安全自动修复，本轮未采用自动修复。 |
| Ruff 版本管理 | **NOT RUN（项目级固定）** | Ruff 通过 `uv tool run --from ruff` 临时运行，没有加入 `requirements.txt`；仓库没有 `pyproject.toml` / `uv.lock` 来锁定 Ruff 版本。此次版本为 0.17.2，需在发布运行手册或 CI 中固定版本后才能长期复现相同 lint 口径。 |
| `git diff --check` | **PASS** | 无空白错误；Git 提示 `AGENTS.md`、QMT Skill 存在 CRLF/LF 转换提示。 |
| 真实窗口数据库 E2E | **NOT RUN** | Docker Desktop Linux Engine pipe 不存在，无法启动或连接隔离 PostgreSQL。集成测试有 19 项被默认配置排除。不得用旧报告替代本轮重跑。 |
| 生产迁移 / Release / 重算 | **N/A（本轮禁止范围）** | 未连接生产库，未执行生产写入、迁移、Release 或历史重算。 |

### 真实 E2E 输入身份

- `real_financial_window_v1.manifest.json` SHA-256：`7ee17c25a1cbe112785bd753150f87a6250f5af2e59ed782545f658b2b3a4ab4`
- 本机忽略目录 `.codex/real-sample/real-financial-window-v1.json.gz`：7,119,324 bytes；实际 SHA-256 `bfe695f509e0517b2f0c94a63745c6b55cce7be5ae8fdec541cd8889facaf615`，与 manifest 声明一致。
- Bundle 位于 Git 忽略目录，不属于本次 73 个候选路径。若要让其他机器复现，需通过受控测试工件存储或明确的样本导出流程取得同一哈希文件；不应把本地 `.codex` 文件误认为已随代码版本发布。
- 其他 manifest：`real_golden_sample_v1.manifest.json` 为 1,348,221 bytes，SHA-256 `ade7931d6e0776bf14a07ae8952044fdec9161a0b7b81ffa26e375a0c28c88ed`；`real_golden_sample_v2.manifest.json` 为 1,348,734 bytes，SHA-256 `2c3585fcf8ceb606d9721324989a581015ab51dcfbc82f28abf59d9c18074924`。这些是未跟踪的结构化样本清单，需在提交清单中明确纳入或排除。

## 代码范围与提交判断

| 范围 | 判断 |
|---|---|
| v2.2 因子、PIT、Snapshot/Dataset Repository 与迁移 | 与研究发布相关；可作为候选核心，但还需处理新增 Ruff 诊断并重跑受影响验收。 |
| 测试、黄金样本工具、P2-A/B/C 报告 | 相关，但需逐文件核实身份与是否要把大体量 manifest 纳入版本库。 |
| `.agents/skills/news-opensearch/SKILL.md` 与 `skills/news-opensearch/SKILL.md` 删除 | 与本次 P2-D 目标无直接关系；需确认是否有意删除，建议单独处理，不能混入研究数据发布候选。 |
| `skills/qmt-xtquant/SKILL.md` 修改 | 与本次研究数据发布无直接关系，建议从 P2-D 候选中分离。 |
| `AGENTS.md` 大幅重写、README 技能引用调整 | 跨项目协作/文档范围变化，须单独审阅；README 的新闻 Skill 引用变化与上述删除耦合。 |
| 敏感配置检查 | `.env` 和 `config.yaml` 未被 Git 跟踪；新测试脚本使用运行时随机凭据并写入忽略的 `.codex`。扫描到的 API/数据库连接文本位于占位示例或 loopback 文档示例，不属于本次新增差异；未发现本次候选新增的硬编码生产凭据。 |

**是否适合直接提交全部工作区：否。** 当前没有任何内容暂存。不能 `git add .` 作为一次性提交：有明确的跨范围技能删除和 QMT Skill 修改，且测试/报告文件需要拆分核对。当前结果适合形成审查报告，不等同于代码版本已冻结。

## 生产与研究状态边界

- P2-C 生产 Schema 部署：**BLOCKED**，需补备份恢复、磁盘容量、获准迁移账号/变更记录，并在维护窗口复核锁。
- `600699.SH` 公司行为：仍为 **UNRESOLVED**，正式 Foundation Release 继续 **BLOCKED**。
- 正式 Foundation Release、生产 v2.2 Snapshot/Dataset、五年因子有效性验收：尚未完成。
- 本次通过的测试只证明当前工作区中的工程测试结果，不证明生产迁移已执行或策略/因子有效。

## 下一项最小工作

1. 将 P2-D 核心文件与无关技能/文档变更分开，确认两个 News Skill 文件删除的意图。
2. 审阅 31 项新增 Ruff 诊断；只修正候选新增问题，不做全仓格式化，不采用语义不明的自动修复。修复后重跑全仓测试和边界检查。
3. 在 Docker 引擎可用的隔离环境中，以现有 manifest 和匹配 SHA-256 的 bundle 重跑 `test-real-window`，保存新 E2E Run 结果并绑定代码差异摘要。
4. 生成最终候选清单，绑定 Git commit、工作树差异摘要、迁移 SHA-256、因子/Schema 版本、测试命令和真实样本工件身份。之后再由用户决定是否提交；生产部署继续另行授权。

## 附录：未暂存路径清单

### 修改（28）

- `AGENTS.md`
- `README.md`
- `data_collect/alpha_research/contracts.py`
- `data_collect/alpha_research/factors.py`
- `data_collect/data_access/kernel.py`
- `data_collect/data_access/policy_catalog.py`
- `data_collect/data_access/query_guard.py`
- `data_collect/data_access/repository/__init__.py`
- `data_collect/data_access/repository/dataset_repository.py`
- `data_collect/data_access/repository/financial_repository.py`
- `data_collect/data_access/repository/financial_standard_repository.py`
- `data_collect/data_access/repository/release_repository.py`
- `data_collect/data_access/repository/research_snapshot_repository.py`
- `data_collect/data_access/repository/snapshot_repository.py`
- `data_collect/data_access/research_contract.py`
- `data_collect/data_access/schema.py`
- `data_collect/jobs/backtest_dataset.py`
- `data_collect/jobs/research_snapshot.py`
- `data_collect/jobs/tushare_foundation_features.py`
- `data_collect/jobs/tushare_foundation_pipeline.py`
- `skills/qmt-xtquant/SKILL.md`
- `tests/test_alpha_research_factors.py`
- `tests/test_alpha_research_job.py`
- `tests/test_backtest_contract.py`
- `tests/test_data_access_repository.py`
- `tests/test_financial_contract_repairs.py`
- `tests/test_financial_feature_v2.py`
- `tests/test_research_snapshot.py`

### 删除（2）

- `.agents/skills/news-opensearch/SKILL.md`
- `skills/news-opensearch/SKILL.md`

### 未跟踪（43）

- `.agents/skills/ashares-research-engineering/`
- `CONTEXT.md`
- `data_collect/alpha_research/golden_sample.py`
- `data_collect/data_access/corporate_action_evidence.py`
- `data_collect/data_access/repository/financial_projection_repository.py`
- `data_collect/data_access/repository/historical_coverage_audit_repository.py`
- `dev_doc/debt_to_assets_factor_v2_acceptance_20261018.md`
- `dev_doc/debt_to_assets_source_attribution_20261018.md`
- `dev_doc/frozen_v21_gate_readiness_20261018.md`
- `dev_doc/integration_test_database.md`
- `dev_doc/integration_test_database_acceptance_20261018.md`
- `dev_doc/p1_production_release_blocker_cleanup_20261020.md`
- `dev_doc/p2_financial_standard_projection_v1_20261020.md`
- `dev_doc/p2_historical_financial_feature_rebuild_20261020.md`
- `dev_doc/p2_production_migration_readiness_20261020.md`
- `dev_doc/real_data_golden_sample_reconciliation_20261018.md`
- `dev_doc/real_golden_sample_e2e_20261018.md`
- `dev_doc/research_v22_five_year_coverage_20261019.md`
- `dev_doc/research_v22_real_financial_window_20261019.md`
- `dev_doc/research_v22_real_financial_window_clean_e2e_20261020.md`
- `dev_doc/small_window_v21_dataset_acceptance_20261018.md`
- `docker-compose.integration-test.yml`
- `docs/`
- `skills/ashares-research-engineering/`
- `sql/116_independent_debt_to_assets_factor.sql`
- `sql/117_financial_standard_pit_projection.sql`
- `tests/fixtures/`
- `tests/integration_fixtures/`
- `tests/test_alpha_research_golden_sample.py`
- `tests/test_corporate_action_ex_date_scope.py`
- `tests/test_financial_pit_projection.py`
- `tests/test_historical_coverage_audit_repository.py`
- `tests/test_integration_alpha_research_dataset.py`
- `tests/test_integration_test_database.py`
- `tests/test_integration_test_database_guard.py`
- `tests/test_read_only_audit_kernel.py`
- `tests/test_real_golden_reference.py`
- `tools/build_real_golden_sample_v2.py`
- `tools/export_financial_pit_projection.py`
- `tools/export_real_financial_window.py`
- `tools/export_real_golden_sample.py`
- `tools/integration_test_db.ps1`
- `tools/replay_financial_pit_projection.py`
