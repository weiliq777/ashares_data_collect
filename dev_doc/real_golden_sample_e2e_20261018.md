# 真实黄金样本 E2E 集成验收

日期：2026-10-18<br>
项目：`ashares_data_collect`<br>
范围：隔离测试数据库 `ashares_test`；不代表全市场质量或正式研究结论。

## 结论

黄金样本已能重复导入并完成 `research-v2.1 Snapshot → Dataset → Alpha Reader` 流程。61 个 Snapshot 均为 `FROZEN`，Dataset 验收为 `VALID` 并冻结；Alpha Reader 能读取单一 `research-v2.1` Schema。90 条 20/40/60 日标签与独立参考完全一致，停牌和未知退市终值均保留。

**整体真实因子对账为 FAIL，不能将本轮标记为阶段二真实数据验收通过。** 300 个因子值中有两处 `debt_to_assets` 不一致。两个旧冻结观察行包含数值，但没有财报身份、来源哈希或 PIT 时间证据；在新 Snapshot 中没有可追溯的财务 Feature，因此该字段为空。报告保留原始参考，不用新结果覆盖参考值。

## 样本身份与复现

| 项目 | 值 |
|---|---|
| 样本版本 | `real-golden-v1` |
| 信号日 / 标签目标日 | 2023-09-22 / 2023-12-25 |
| 证券数 | 30 |
| 选择规则 | 6 个固定边界案例，加上至多 100 个信号日行情候选中按 `md5(ts_code || seed)` 确定性选择 |
| Manifest | [real_golden_sample_v1.manifest.json](../tests/fixtures/real_golden_sample_v1.manifest.json) |
| Manifest SHA-256 | `ade7931d6e0776bf14a07ae8952044fdec9161a0b7b81ffa26e375a0c28c88ed` |
| 压缩数据包 | `.codex/real-sample/real-golden-v1.json.gz`，4,239,168 bytes |
| 数据包 SHA-256 | `aa6525c99d025374cb9a232e49e32909ef842997cb38e2547b27986d393d8f79` |
| 重复运行命令 | `./tools/integration_test_db.ps1 -Action test-real-golden` |
| 最近一次机器结果 | [.codex/real-sample/results/real-golden-e2e-v4.json](../.codex/real-sample/results/real-golden-e2e-v4.json) |

黄金样本清单是新建的不可变版本，不声称恢复了此前没有持久化的旧 30 股名单。导出端检查连接必须使用非 superuser 的生产应用角色，且仅要求只读权限。真实证券数据只写入带 `REAL_SAMPLE_TEST_ONLY` 标记的 `ashares_test`；未写入生产库。

## 测试数据库与导入规模

测试 PostgreSQL 身份检查为 `ashares_test / ashares_app / TEST_FIXTURE`，应用账号不是 superuser。迁移账本为 118 项。此次运行后的数据库大小为 58,604,567 bytes（约 55.9 MiB）。

黄金样本数据包及隔离库物化行数：

| 数据 | 行数 |
|---|---:|
| 股票观察 / 执行行情 | 1,788 / 1,788 |
| 实际标准日线 / 价格 Feature | 1,760 / 1,760 |
| Universe / Tradeability / Valuation / Shareholder Return | 各 1,788 |
| 财务 Standard 事件（固定包中的来源输入） | 4,113 |
| 财务 Feature v1 / v2 | 55 / 690 |
| Snapshot 行 | 1,788（61 日 × 30 证券） |
| Dataset 执行行情 / 日历 / 基准 / 公司行为 | 1,788 / 95 / 61 / 2 |
| 独立标签参考 / 因子参考 | 90 / 30 |

1,788 个行情观测中有 28 个没有对应源价格 Feature；导入器没有为这些行合成价格 Feature。4,113 条 Standard 事件保存在哈希绑定的样本包中，并用于离线重算 v2 Feature；本测试没有将其伪称为完整 Raw → Standard → PIT 转换。

## 端到端结果

| 检查 | 结果 | 证据 |
|---|---|---|
| 确定性样本包哈希与版本核验 | PASS | 每次导入前校验 bundle、manifest 和内容哈希 |
| 幂等重复导入 | PASS | 同版本、相同内容第二次返回 `REUSED` |
| 测试 Release / 新 Snapshot | PASS | Release 为测试专用；61/61 Snapshot 为 `FROZEN`，Schema 单一且为 `research-v2.1` |
| Dataset 创建、物化、质量校验及冻结 | PASS | Dataset `21722345-aef0-55b6-a297-2517aa70412d`，质量状态 `VALID`、Dataset 状态 `FROZEN` |
| Alpha Reader | PASS | 拒绝非 FROZEN 的行为由契约保护；本 Dataset 读取 30 个信号日观察、1,788 条行情和 2 条公司行为 |
| 财务来源 WARNING 因子门禁 | PASS | WARNING 来源分量用例被拒绝，原因保持明确 |
| 缺 PIT 时间证据 | PASS | 1 个信号日证券案例，财务因子维持不可用状态 |
| 因子逐值对账 | **FAIL** | 300 项中 2 项 `debt_to_assets` 不一致，具体见下表 |
| 20/40/60 日标签对账 | PASS | 90/90 行身份、状态、原因和收益数值一致 |
| 停牌路径 | PASS | 5 个标签明确标记 `SUSPENDED_PATH` |
| 退市未知终值 | PASS（保留限制） | 3 个标签标记 `DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE`，未假定结算收益 |
| 未知执行行情 | PASS | 本 Dataset 未发现未解决的执行行情缺失 |
| Raw → Standard → PIT → Feature 全链路 | NOT RUN | 本批目标是下游集成，不运行生产转换作业 |
| 正式生产 Release / 五年 Dataset | NOT RUN | 未创建、未修改 |

### 因子对账差异

| 证券 | 因子 | 旧冻结参考 | 新 Snapshot / Alpha Reader | 结论 |
|---|---|---|---|---|
| `688426.SH` | `debt_to_assets` | `VALID: 8.619` | `MISSING: VALUE_MISSING_OR_NONFINITE` | 旧观察值没有可追溯财务身份；当前重建 Feature 不提供对应值 |
| `688448.SH` | `debt_to_assets` | `VALID: 27.9137` | `MISSING: VALUE_MISSING_OR_NONFINITE` | 同上 |

这两项不应通过把旧值硬写入测试 Feature、修改参考值或放宽 PIT 门禁消除。需要下一步追踪其 Standard 版本及旧 Snapshot 字段来源，确认能否按正式财务 Feature 计算并绑定 PIT lineage；若无法证明，正式研究应将它们报告为缺值。

## Ruff 与自动化检查

| 检查 | 结果 |
|---|---|
| 本批涉及的黄金样本导入器、集成测试与导出器 Ruff | PASS，0 项 |
| 相关 Alpha、Snapshot、Dataset、公司行为和访问边界单元测试 | PASS，152 项 |
| `test-real-golden` 测试命令 | PASS（诊断断言稳定通过；业务因子验收仍为 FAIL） |
| 仓库全量 Ruff | FAIL，1,320 项，跨多个既有模块和工具 |

当前工作树在本任务开始前已包含大量未提交变更，缺少可比的干净基线，因此无法把全仓 1,320 项逐条划分为历史与之前任务引入。**本批实际改动的两个 Python 文件及黄金样本相关的四文件定向检查均为 0 项告警**；没有为清理无关代码而扩大修改范围。

## 剩余阻断与边界

1. `debt_to_assets` 的两个来源追溯差异需要定位并修复或明确拒绝旧快照数值；在此之前，真实因子逐值验收仍为 FAIL。
2. 本样本来自已有旧冻结 Dataset `f348bc11-b4f0-41fe-a8b5-d376bb5cf401`，其 Release `foundation_v2_20260821` 仅用于受控提取和对照，不是新版全市场质量证明。
3. 本结果不能证明五年全市场 PIT 完整、退市终值完整、Raw 转换无误、Alpha 有效或策略可交易。
4. 正式 Release、全历史 v2.1 Snapshot/Dataset、正式 Alpha 统计与策略增量价值均未运行。

下一步应先解决两条 `debt_to_assets` 来源差异并更新下一版黄金样本/参考身份（保留 v1），再重复完整集成测试。只有因子逐值对账通过后，才进入正式历史 Dataset 建设。
