# P2-E 发布候选收敛与复验

- 日期：2026-10-24
- 项目：`ashares_data_collect`
- HEAD：`0e72b13b5be3c442a88af76d60a3743ebeb63675` (`agent/stage1-data-validation`)
- 总体状态：**BLOCKED — 尚未达到 READY FOR COMMIT**

## 结论

发布范围已逐文件划定；候选 Python 文件没有新增 Ruff 诊断；全仓默认测试、数据访问契约、纸面交易边界、隔离 PIT 投影恢复和 `research-v2.2` 真实窗口均通过。

`real-golden-v2` 仍在 Dataset 导入前因不可变样本包中的 D/A 报告期与来源身份不一致而失败。不能通过改写旧 bundle 或参考值消除该失败，因此 P2-E 总体保持 **BLOCKED**。`real-golden-v1` 是有意保留的负向控制：它验证旧 schema 的 D/A 值会被新版因子门禁拒绝，数据集冻结被正确阻断；它不是正向研究验收通过。

## 验收摘要

| 检查 | 状态 | 结果 |
|---|---|---|
| 发布候选范围 | PASS | 70 个候选文件已按路径和内容哈希固定；另有 14 个无关路径排除。 |
| Ruff 候选增量 | PASS | Ruff 0.17.2；47 个变更 Python 路径，按路径、规则码和消息与 HEAD 对照，新增 0 项，已有诊断减少 13 项。 |
| Ruff 全仓 | NOT CLEAN | 当前 1,312 项既有/存量诊断；本次只处理候选增量，没有做全仓格式化。 |
| 默认 pytest | PASS | `uv run pytest -q`：1,571 passed、1 skipped、19 deselected，41.92 秒。 |
| Data Access Contract | PASS | 扫描 259 个文件，0 violations。 |
| Paper Trading Boundary | PASS | 检查 320 个可执行 Python 文件，未发现 QMT 交易 API。 |
| `git diff --check` | PASS | 仅提示两个排除范围文件有 CRLF/LF 转换提醒。 |
| 隔离数据库集成 | PASS | 120 项迁移；数据库契约测试 7 passed，合成 Dataset 生命周期 1 passed；真实窗口契约 7 passed。 |
| `real-golden-v1` | PASS（负向安全控制） | 30 个旧 D/A 值全部因版本不受支持被拒绝；冻结门禁 BLOCKED，90 条标签无差异。E2E 结果按预期为 FAIL。 |
| `real-golden-v2` | FAIL / BLOCKED | fixture 前置校验拒绝旧包；未导入样本、未创建 Dataset。 |
| `research-v2.2` 真实窗口 | PASS | 83 个 Snapshot、690 个信号观测、2,760 个因子值 0 差异、2,070 条标签 0 差异；数据集冻结及幂等重读通过。 |
| PIT 版本化投影 | PASS | 隔离库中完成 20,469 行、42 批；中断恢复、完整性、哈希、冲突拒绝、旧 Standard 不变和显式版本读取均通过。 |
| 生产迁移/写入 | NOT RUN | 生产仍只读；没有生产 DDL、DML、Feature 重算或 Release。 |

默认 pytest 配置排除了 19 个 `integration` 标记测试。P2-E 本次通过项目隔离数据库脚本运行了所需的数据库契约、合成 Dataset、Golden Sample 和真实窗口场景；未运行依赖外部 CNINFO、模型服务、OpenSearch 或生产只读数据库的其他集成场景。

## Ruff 增量处理

Ruff 通过临时工具环境运行，版本为 `0.17.2`，没有加入项目依赖。当前全仓报告 1,312 项诊断；P2-D 报告中记录的历史数字为 1,349。本次增量判定只在变更 Python 文件内按相对路径、规则码和诊断消息比对 HEAD，不将全仓存量诊断或这两个全仓计数的差值归因于本次修复。

HEAD 对照覆盖 25 个已有 Python 文件；新增文件也纳入当前候选扫描。47 个变更 Python 路径的当前诊断总数为 115，HEAD 对应已有文件基线为 128；增量集合结果是 0 新增、13 项旧诊断消失。修正集中于导入与类型注解、字符串拼接、数值判断和异常类型，不改财务公式或 PIT 门禁。测试断言的两处调整分别对应：旧 Golden Sample 的 D/A 版本 fail-closed，以及已冻结 Dataset 的幂等复跑允许 `NOT_APPLICABLE_FROZEN`。

Ruff 没有项目级锁定文件；使用临时 `uvx --from ruff` 可运行，但要在 CI 或发布手册固定 Ruff 版本，才能长期复现同一静态检查口径。

## 文件范围

### 候选核心文件（54 项）

以下路径全部在建议发布范围内：

**`data_collect/`（22）**

```text
data_collect/alpha_research/contracts.py
data_collect/alpha_research/factors.py
data_collect/alpha_research/golden_sample.py
data_collect/data_access/corporate_action_evidence.py
data_collect/data_access/kernel.py
data_collect/data_access/policy_catalog.py
data_collect/data_access/query_guard.py
data_collect/data_access/repository/__init__.py
data_collect/data_access/repository/dataset_repository.py
data_collect/data_access/repository/financial_projection_repository.py
data_collect/data_access/repository/financial_repository.py
data_collect/data_access/repository/financial_standard_repository.py
data_collect/data_access/repository/historical_coverage_audit_repository.py
data_collect/data_access/repository/release_repository.py
data_collect/data_access/repository/research_snapshot_repository.py
data_collect/data_access/repository/snapshot_repository.py
data_collect/data_access/research_contract.py
data_collect/data_access/schema.py
data_collect/jobs/backtest_dataset.py
data_collect/jobs/research_snapshot.py
data_collect/jobs/tushare_foundation_features.py
data_collect/jobs/tushare_foundation_pipeline.py
```

**迁移与隔离数据库配置（3）**

```text
docker-compose.integration-test.yml
sql/116_independent_debt_to_assets_factor.sql
sql/117_financial_standard_pit_projection.sql
```

**`tests/`（23）**

```text
tests/fixtures/real_financial_window_v1.manifest.json
tests/fixtures/real_golden_sample_v1.manifest.json
tests/fixtures/real_golden_sample_v2.manifest.json
tests/integration_fixtures/__init__.py
tests/integration_fixtures/alpha_research_v21.py
tests/integration_fixtures/real_golden_reference.py
tests/integration_fixtures/real_golden_sample.py
tests/test_alpha_research_factors.py
tests/test_alpha_research_golden_sample.py
tests/test_alpha_research_job.py
tests/test_backtest_contract.py
tests/test_corporate_action_ex_date_scope.py
tests/test_data_access_repository.py
tests/test_financial_contract_repairs.py
tests/test_financial_feature_v2.py
tests/test_financial_pit_projection.py
tests/test_historical_coverage_audit_repository.py
tests/test_integration_alpha_research_dataset.py
tests/test_integration_test_database_guard.py
tests/test_integration_test_database.py
tests/test_read_only_audit_kernel.py
tests/test_real_golden_reference.py
tests/test_research_snapshot.py
```

**受控样本工具（6）**

```text
tools/build_real_golden_sample_v2.py
tools/export_financial_pit_projection.py
tools/export_real_financial_window.py
tools/export_real_golden_sample.py
tools/integration_test_db.ps1
tools/replay_financial_pit_projection.py
```

**本次之前已有的研究验收报告（16）**

```text
dev_doc/debt_to_assets_factor_v2_acceptance_20261018.md
dev_doc/debt_to_assets_source_attribution_20261018.md
dev_doc/frozen_v21_gate_readiness_20261018.md
dev_doc/integration_test_database_acceptance_20261018.md
dev_doc/integration_test_database.md
dev_doc/p1_production_release_blocker_cleanup_20261020.md
dev_doc/p2_financial_standard_projection_v1_20261020.md
dev_doc/p2_historical_financial_feature_rebuild_20261020.md
dev_doc/p2_production_migration_readiness_20261020.md
dev_doc/p2d_release_candidate_freeze_20261024.md
dev_doc/real_data_golden_sample_reconciliation_20261018.md
dev_doc/real_golden_sample_e2e_20261018.md
dev_doc/research_v22_five_year_coverage_20261019.md
dev_doc/research_v22_real_financial_window_20261019.md
dev_doc/research_v22_real_financial_window_clean_e2e_20261020.md
dev_doc/small_window_v21_dataset_acceptance_20261018.md
```

本报告是额外的 P2-E 验收证据；未计入下文候选内容哈希，以避免自引用。

### 排除在本次候选之外（14 项）

这些现有更改原样保留，没有重置或清理：

```text
.agents/skills/ashares-research-engineering/SKILL.md
.agents/skills/news-opensearch/SKILL.md              [删除]
AGENTS.md
CONTEXT.md
docs/agents/domain.md
docs/agents/issue-tracker.md
docs/agents/triage-labels.md
README.md
skills/ashares-research-engineering/references/architecture.md
skills/ashares-research-engineering/references/frozen-and-research.md
skills/ashares-research-engineering/references/pit-and-data.md
skills/ashares-research-engineering/SKILL.md
skills/news-opensearch/SKILL.md                     [删除]
skills/qmt-xtquant/SKILL.md
```

### 不纳入 Git 的本地输入及运行产物

样本压缩包、PIT 输入包、测试数据库凭据和运行结果均位于被忽略的 `.codex/` 下，没有加入候选文件。其哈希见下节。重放脚本可按 manifest 与受控导出路径验证输入；提交前仍应确认这些二进制包在目标构建环境中可取得。

候选文件清单中没有 `.env`、运行配置或数据库凭据文件。当前 `git diff --cached` 为空；没有暂存、提交或推送操作。

## 隔离数据库与 E2E 结果

所有数据库写入均限定在项目身份校验过的 `ashares_test` 容器中，容器已停止，命名卷保留。没有连接生产写入目标。项目迁移账本为 120 项，116 在 117 之前。

### Golden Sample v1：负向安全控制

- 测试工件：`.codex/real-sample/results/p2e-20261024-real-golden-v1-final.json`，SHA-256 `5f9f8a54ab79b1afa0b17724be5fbf2fc7d7fe158e9f411ac7959223a93d8fa8`。
- 61 个 Snapshot、30 个信号观测、300 个因子值；30 项 D/A 差异全部因 `DEBT_TO_ASSETS_FEATURE_VERSION_NOT_SUPPORTED` 被拒绝；90 条未来标签无差异。
- Dataset `21722345-aef0-55b6-a297-2517aa70412d` 保持 `VALIDATED`，冻结动作 `BLOCKED_REFERENCE_MISMATCH`。脚本的业务 E2E 状态是 `FAIL`，这是这个旧 v2.1 参考在新版契约下的预期 fail-closed 结果；测试断言通过，证明门禁没有放行。
- v1 manifest SHA-256：`ade7931d6e0776bf14a07ae8952044fdec9161a0b7b81ffa26e375a0c28c88ed`；bundle SHA-256：`aa6525c99d025374cb9a232e49e32909ef842997cb38e2547b27986d393d8f79`。

### Golden Sample v2：仍为阻断

- v2 manifest SHA-256：`2c3585fcf8ceb606d9721324989a581015ab51dcfbc82f28abf59d9c18074924`；bundle SHA-256：`8b0fe32c246a38e066acfd2a5ceca233af859aba994b96f6a0a415639deb7226`。
- 1,044 条 D/A 计算内容哈希不匹配：fixture 声明的 hash 为 `6ecb99ef549351adca60d5409296845a748968a90dc86dac0786a68558b90dbe`，当前计算 hash 为 `cb5df93bfcee99cc5c9fcfa84f8444a028f1de1e7c32397c3c8ceada7133a3a1`。
- 首个差异的声明报告期为 `2023-06-30`，但来源键记录的是 `000046.SZ:2016-09-30:1:1:3`。本次保留 v2 manifest 和 bundle 原样；fixture 校验在导入前拒绝，未创建 Snapshot/Dataset。
- 结论：不能将 v2 标为通过。下一项最小工作是核实并新建独立版本（例如 `real-golden-v3`）的来源身份和参考结果，保留 v2 作为失败证据，再运行正向 Golden Sample E2E。

### research-v2.2 真实财务窗口：正向通过

- 结果工件：`.codex/real-sample/results/p2e-20261024-real-financial-window-v1-e7.json`，SHA-256 `b7f601968ac812a6431b0fb4c9cfe8f0ba6d0048c353fc7b92687cb83fb740be`。
- Dataset `bd346de0-3de6-5615-a306-1c41d6fd773b`，统一 schema `research-v2.2`；83 个 Snapshot、690 个信号观测、2,760 个因子值 0 差异、2,070 条标签 0 差异。
- 停牌标签 98 条、退市终值未知标签 63 条均保留其状态；执行问题未解决数为 0。
- 新建冻结路径通过；重复运行识别为已冻结数据集，返回 `NOT_APPLICABLE_FROZEN`，仍保持因子和标签匹配 PASS。再次运行没有重写旧 FROZEN。
- window manifest SHA-256：`7ee17c25a1cbe112785bd753150f87a6250f5af2e59ed782545f658b2b3a4ab4`；bundle SHA-256：`bfe695f509e0517b2f0c94a63745c6b55cce7be5ae8fdec541cd8889facaf615`。

### PIT 追加式投影：中断恢复和幂等通过

- 在新建的独立环境 `ashares_test` / `TEST_FIXTURE`（环境 ID `33951bc0-a92f-4638-abdc-004b6a3cd018`）上运行；120 项迁移账本。容器停止后命名卷保留。
- 使用 `financial-source-announcement-projection-v6`、manifest SHA-256 `b6611c604de9f30a55e14718c462946e8c086859f74a3362e20520c4046697f1`；输入包 SHA-256 `8949796ad07a83254ebc5f99d2048122bbe7235998829f52fb2a3d0ef65fbb6e`。
- 1/42 批中断后，用同一 Run/manifest 恢复。最终 20,469 行唯一身份、42 批完整；分来源为 balance 8,184、cashflow 5,946、income 6,339。21 条 NaN/NULL 被规范化为 NULL（1/4/16）；所有 20,469 条仍标记 `SOURCE_ANNOUNCEMENT_ASSUMED`。
- 不完整版本被拒绝；同一身份的内容变更被拒绝；旧 Standard 20,469 行 PIT NULL 保持不变，前后哈希均为 `1970c72c2a41fbd647428808da679656838baf3f5f69b99b1d37247f03b7a532`；显式投影读取 120 条样本，未投影基础读取 0 条。
- 数据库验证总计 62,708,759 bytes。结果工件 SHA-256：`7db090d50683271f7cf76596cdf8d2fa36b5ec46eeade4c719269aed317cc393`。
- 首次在另一个隔离库执行时，数据库检查也完成，但旧固定结果文件拒绝被覆盖；保留了旧工件。最终可复验结果写入独立 `.codex/p2-batches/financial-pit-projection-replay-p2e-recovery-20261024.result.json`，未修改重放代码或旧结果。

### 迁移哈希与契约身份

| 身份 | 值 |
|---|---|
| SQL 116 SHA-256 | `13effca380f96eec94000e07ad080b018a7af8ca3f8e32ca28d5a66705ffe421` |
| SQL 117 SHA-256 | `e8662de091104cdabee2ec886d5d7d2389d3f6eb80c0d22869bbac958c0f162d` |
| Alpha schema | `research-v2.2` |
| 因子集 | `frozen-factor-set-v4` |
| 财务来源证据 | `financial-source-quality-v1` |
| 独立 D/A | `debt-to-assets-v1` |
| PIT 投影版本 | `financial-source-announcement-projection-v6` |

## Git 范围与候选哈希

P2-D 的 `git status` 路径数按未展开目录统计为 73；使用 `--untracked-files=all` 展开后，本轮开始时为 84 个真实路径：候选 70 个，排除 14 个。新增本报告后总数为 85 个。该报告自身不纳入候选哈希。

- HEAD 与远端提交身份：`0e72b13b5be3c442a88af76d60a3743ebeb63675`。
- 候选 70 文件哈希：`1dd563e5ba5810ae1b8da29d84d8a491597430394fb0457aca745b0f624782ea`。
- 报告前全工作区 84 路径哈希：`905df2902982be3fe34ffc72858027c2d22b9c626bea086a49c74ee44f076fc0`。
- 哈希算法：按路径排序，每行拼接 `git status` 两位状态、相对路径、文件 SHA-256，以 Tab 分隔并以 LF 结尾，再对整体 UTF-8 字节取 SHA-256。删除路径内容标记为 `<deleted>`。
- `.codex/` 的大包、凭据及 E2E 运行产物被忽略，不包含在 Git 候选 hash；其相关内容通过本报告列出的 SHA-256 单独绑定。
- 提交建议：**暂不提交此候选整体**。先修正或替换 v2 Golden Sample，重新跑正向 E2E，再生成新候选哈希。各项未暂存改动仍保持原状；未执行 `git add`、提交、推送。

## 仍未通过的发布条件

1. 正向 `real-golden-v2` E2E 尚未通过；当前是参考数据身份问题，不应改成放宽因子门禁。
2. 输入压缩包位于忽略目录，并非 Git 自包含；在进入 CI/部署流程前，需要确认归档存储或通过只读导出脚本可重建同哈希输入。
3. 生产 Schema 迁移 116/117 尚未执行。生产备份与恢复证明、剩余磁盘空间、授权 DDL 账号及正式变更记录尚待单独验收。
4. `600699.SH` 公司行为仍是 `UNRESOLVED`；正式 Foundation Release 继续 BLOCKED。
5. 生产财务 Feature 物化、新 Foundation Release、五年 Snapshot/Dataset 和正式 Alpha 研究均未运行。

生产迁移与 Release 状态继续为 **BLOCKED**。本轮未写入生产数据库、未修改历史 FROZEN、未创建正式 Release、未审批策略、未提交或推送代码。
