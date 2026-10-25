# research-v2.2 真实财务窗口干净环境 E2E 验收

- 项目：`ashares_data_collect`
- 样本 Bundle：`real-financial-window-v1`（未修改）
- 新 E2E 身份：`real-financial-window-e2e-v7`
- 信号区间：2023-09-12 至 2023-10-20，共 23 个交易日
- 标签行情尾部：至 2024-01-15，共 83 个交易日
- 执行日期：2026-10-20（Asia/Singapore）
- 结论：**干净环境 E2E PASS；有意注入的对账差异未能冻结；生产 Release 与五年研究仍未验收**

## 执行摘要

本次新建独立 PostgreSQL 测试实例，以全新数据卷重放项目的 119 项 SQL 迁移，再从不可变的真实财务窗口 Bundle 导入并验证 Standard 事件，使用正式 Feature 计算路径重算，完成单一 `research-v2.2` Snapshot、Dataset 和 Alpha Reader 链路。

与旧 `ashares_test` 不同，新实例没有 `real-golden-v1/v2` 的历史行。83 个 Snapshot 全部冻结；新 Dataset 通过独立参考对账后冻结。四项财务因子 2,760 个证券日值、2,070 条收益标签均与参考一致，D/A 先前的 23 条差异归零。

另外创建一个独立 Dataset，在其状态为 `VALIDATED` 时向门禁输入一个人为注入的因子差异。门禁返回 `BLOCKED_REFERENCE_MISMATCH`，Dataset 仍为 `VALIDATED`，没有冻结。这个负向用例不改变主 Dataset 或旧 FROZEN。

本报告不修改、不覆盖旧 `research_v22_real_financial_window_20261019.md` 和旧 E2E v6 结果；旧数据库中的失败 Dataset 继续保留为历史证据。

## 隔离环境身份

| 项目 | 结果 |
|---|---|
| 数据库 | `ashares_test` |
| 角色 | 测试业务角色 `ashares_app`；迁移由测试容器管理账号执行 |
| 环境标记 | `TEST_FIXTURE` |
| Environment ID | `dfe5e3fa-ef03-402f-bebf-a3df7fb5cc30` |
| 容器 | `ashares-real-window-clean-v1-postgres` |
| 端口 | `127.0.0.1:15434`，测试连接校验要求与该实例显式端口一致 |
| 数据卷 | `ashares_real_window_clean_v1_pgdata`，与原 `ashares_test` 卷分离 |
| 执行后状态 | 容器已停止，命名卷保留，可复查或恢复运行 |
| 迁移 | 119/119，Migration chain hash `4fd0994f01acd28696253c4ae004fe26601347dca7ce800f8c725fc90baf90da` |
| 数据库大小 | 75,594,775 字节（约 72.1 MiB） |
| 生产库访问 | 本轮 E2E 使用已固化 Bundle；没有连接生产库或执行生产写入 |

测试脚本支持 `-InstanceName` 和 `-HostPort` 创建单独的本地测试实例。测试连接仍强制校验 `127.0.0.1`、`ashares_app`、`ashares_test`、显式测试端口和持久化 Environment ID；默认实例的既有配置保持兼容。

## 新一轮数据身份与生命周期

| 产物 | 身份 / 状态 |
|---|---|
| Bundle SHA-256 | `bfe695f509e0517b2f0c94a63745c6b55cce7be5ae8fdec541cd8889facaf615` |
| Manifest SHA-256 | `7ee17c25a1cbe112785bd753150f87a6250f5af2e59ed782545f658b2b3a4ab4` |
| 测试 Release tag | `it-real-financial-window-e2e-v7`，仅测试夹具，不是正式 Foundation Release |
| Snapshot | 83 个；2023-09-12 至 2024-01-15；全部 `research-v2.2 / FROZEN` |
| Snapshot ID 范围（字典序） | `00447f0f-c763-546f-afdc-e157a59a7317` 至 `fe40e48a-4689-579d-a3a7-bfc9bde4ed36` |
| 主 Dataset | `bd346de0-3de6-5615-a306-1c41d6fd773b`，`FROZEN` |
| 冻结前负向 Dataset | `245af549-c81c-50f6-afb9-7936cb7ce8ed`，保留为 `VALIDATED` |
| 运行身份 | `real-financial-window-e2e-v7`，Run ID 以该版本前缀隔离 |

新实例内共有 83 个 Snapshot 和两个 Dataset：主 Dataset 已冻结；负向测试 Dataset 有意停留在 `VALIDATED`。两者共享只读引用的 FROZEN Snapshot，但 Dataset 身份各自独立。

## 来源、Feature 与因子结果

| 检查 | 结果 |
|---|---|
| Standard 来源事件 | 4,329 条；Bundle 与 Repository 重读及重算输入身份一致 |
| `company_financial_v2` | 717 条重算输出：496 `VALID`、221 `INCOMPLETE_TTM` |
| `debt_to_assets_v1` | 1,102 条重算输出：794 `VALID`、308 `WARNING / SOURCE_QUALITY_NOT_VALID` |
| Snapshot observations | 2,434 条（83 日行情/特征观察） |
| 公司行为 | 2 条，限于窗口包范围 |

在 690 条信号横截面（23 日 × 30 只证券）中，Alpha Reader 因子状态为：

| 因子 | 通过 / 可用状态 | 拒绝或缺失状态 |
|---|---:|---|
| `debt_to_assets` | 690 `VALID`（声明公告时间假设） | 0 |
| `roe_ttm` | 552 `VALID` | 92 `INVALID / ROE_QUALITY_STATUS_NOT_VALID`；46 PIT `INVALID` |
| `revenue_growth_yoy` | 483 `VALID`；115 `VALID / SIBLING_GROWTH_FACTOR_INSUFFICIENT` | 46 `MISSING`；46 PIT `INVALID` |
| `profit_growth_yoy` | 483 `VALID` | 161 `MISSING`；46 PIT `INVALID` |

`SOURCE_ANNOUNCEMENT_ASSUMED` 仍是明确的历史可见性假设，不代表已保存真实的历史首次公开时刻。这里的 `VALID` 仅表示通过当前工程契约，不表示因子具有投资预测能力。

## 对账与冻结门禁

| 项目 | 结果 |
|---|---|
| Standard → Feature 数值与身份 | PASS；Feature 指纹与不可变 Bundle 重算值一致 |
| 四财务因子逐证券逐日对账 | PASS；2,760 个因子行，0 差异 |
| `000666.SZ` D/A | PASS；23 个信号日均选中正确报告期版本，之前 23 条差异归零 |
| 20/40/60 日标签 | PASS；2,070 条，收益值、状态和拒绝原因均 0 差异 |
| Alpha Reader Schema | PASS；单一 `research-v2.2`，83 个 FROZEN Snapshot |
| 主 Dataset 正向冻结 | PASS；对账通过后 `VALIDATED → FROZEN` |
| Dataset 负向冻结门禁 | PASS；注入 1 条因子差异后返回 `BLOCKED_REFERENCE_MISMATCH`，状态保持 `VALIDATED` |
| 旧 FROZEN 保护 | PASS；新环境与旧卷分离；旧数据库 Dataset `3115e55d-af1e-59e1-9ea8-696b6506956e` 仍为 `FROZEN`，旧窗口仍有 83 个 FROZEN Snapshot |
| 停牌/退市边界标签 | 标签对账通过；98 条标签记录包含停牌路径、63 条记录保留未知退市终值（分类可能与其他不完整状态并存） |

旧 `real-financial-window-e2e-v6` 的 FAIL 报告和冻结 Dataset 未修改。新结果写到独立的 `real-financial-window-e2e-v7.json`，不覆盖 v6。

本次结构化结果路径：`.codex/real-sample/results/real-financial-window-e2e-v7.json`。

## 代码和验证结果

本批修改：

- `docker-compose.integration-test.yml`、`tools/integration_test_db.ps1`：支持独立测试实例、容器/卷/端口及新真实窗口测试入口。
- `data_collect/data_access/schema.py`：增加显式测试端口校验，仍要求固定本地地址、数据库、角色和 Environment ID。
- `tests/integration_fixtures/real_golden_sample.py`：为 v7 创建独立 Release/Snapshot/Dataset/Run 身份，并集中实现参考对账后的冻结门禁。
- `tests/test_integration_alpha_research_dataset.py`：保留旧 v6 预期失败断言，增加 v7 全因子通过和差异阻止冻结断言。
- `tests/test_integration_test_database_guard.py`、`tests/test_alpha_research_factors.py`、`tests/test_financial_feature_v2.py`：覆盖显式端口隔离、来源版本/hash 不完整或无效、报告期与期末类型不一致等拒绝条件。

| 验收命令 / 检查 | 状态 | 结果 |
|---|---|---|
| 新 PostgreSQL 迁移 | PASS | 119 项全部应用；耗时约 222 秒 |
| `tests/test_integration_test_database.py` | PASS | 7 passed；含连接身份、权限、Migration ledger 和 FROZEN Snapshot 不可变门禁 |
| 真实窗口 v7 集成测试 | PASS | 1 passed，约 267 秒；E2E 主 Dataset 和负向门禁均核验 |
| 定向单元测试 | PASS | 69 passed |
| Data Access Contract | PASS | 扫描 258 个文件，0 violations |
| Paper Trading Boundary | PASS | 检查 317 个 Python 文件，未发现 QMT 交易 API |
| Python compileall | PASS | 本批 Python 文件编译通过 |
| `git diff --check` | PASS | 本批已修改文件无空白错误 |
| Ruff | NOT RUN | 环境中未确认 Ruff 可执行；没有安装依赖 |
| 全项目测试 | NOT RUN | 仅运行与本批直接相关的测试 |

## 尚未验收的项目

- 该结果是隔离测试环境 E2E，不代表正式生产 Foundation Release 已通过。
- `600699.SH` 公司行为未决、2021-06 Feature 覆盖异常、20,469 条 PIT 时间戳缺失记录仍是生产数据阻断。
- 生产独立 D/A Feature 迁移和历史 v2.2 Feature 重物化尚未授权、未运行。
- 没有创建生产 Release、Snapshot 或五年 Dataset；没有运行正式 Alpha 有效性/策略收益研究。
- 代码仍有工作区未提交变更，本轮没有提交或推送。
