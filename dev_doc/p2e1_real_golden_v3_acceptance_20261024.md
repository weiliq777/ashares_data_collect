# P2-E1：real-golden-v2 来源身份修复与发布候选复验

- 日期：2026-10-24
- 项目：`ashares_data_collect`
- 基础 HEAD：`0e72b13b5be3c442a88af76d60a3743ebeb63675`（`agent/stage1-data-validation`）
- 结果：**Golden Sample 正向 E2E PASS；候选可供定向提交；生产发布仍 BLOCKED**

## 结论

没有改写 `real-golden-v2`。其来源报告期身份与内容不一致，继续保留为失败证据；新增 `real-golden-v3`，从 v2 保留的 Standard 财务事件按精确来源身份重新计算 D/A、生成因子参考，并绑定独立 bundle 与 manifest 哈希。新版报告期—来源校验和负向门禁测试均通过。

本轮未修改生产数据库、旧 FROZEN、正式 Release 或策略审批；没有暂存、提交或推送。发布候选代码身份已计算，可供后续定向提交。生产 Schema、数据发布及五年研究不因本次测试通过而获批。

## 根因与修复边界

对 v2 的 1,044 条 D/A Feature 逐行按证券、来源键、来源版本和 payload hash 与 Standard 来源核对：

- 1,044 条 Feature 均声明 `report_date=2023-06-30`，这是输出截止日，不是各行实际来源报告期。
- 1,044 条都能匹配精确 Standard 来源身份；D/A 数值与当前计算结果一致，没有数值差异。
- Standard 记录和当前 D/A 计算器的报告期一致，错配在旧黄金样本产物的 Feature 身份生成阶段。
- `000666.SZ` 的旧参考将 D/A 标记为 `INVALID/REPORT_PERIOD_TYPE_INVALID`；按实际 `2023-03-31` 资产负债表来源重算后为 `36.619`，状态为 `VALID/PIT_SOURCE_ANNOUNCEMENT_ASSUMED`。

因此没有改公式、来源选择、PIT 门禁或 v2 的答案。新校验要求每条有效 D/A Feature 都能按 `(ts_code, source_record_key, record_version, payload_hash)` 唯一匹配 Standard 来源，且报告期相等；没有来源、存在歧义或报告期不同均拒绝。新增负向测试覆盖报告期错配。

## v2 与 v3 身份

| 产物 | Manifest SHA-256 | Bundle SHA-256 | 状态 |
|---|---|---|---|
| `real-golden-v2` | `2c3585fcf8ceb606d9721324989a581015ab51dcfbc82f28abf59d9c18074924` | `8b0fe32c246a38e066acfd2a5ceca233af859aba994b96f6a0a415639deb7226` | 原样保留；来源报告期校验应拒绝 |
| `real-golden-v3` | `270afa6c831fd8b7e9de452cfdeb2126a7516f49ee84d1251c8a4e72b81b6e81` | `255eed17fc8a2fd5597fe748fc2248b30641cc9caa2fda93547a07d0c2d89cef` | 新版本；正向与负向 E2E 通过 |

v3 绑定父版本 v2 的两项哈希，使用同一 30 只证券和信号日；从 4,113 条 Standard 事件重算出 690 条财务 Feature、1,044 条 D/A Feature，并生成 30 项因子参考。bundle 为 4,346,493 bytes，标记为 `REAL_SAMPLE_TEST_ONLY`。压缩 bundle 位于忽略目录 `.codex/real-sample/`，不会进入 Git；manifest 与生成工具在候选范围内。

`000666.SZ` 的 Alpha Reader 实际读取身份为：报告期 `2023-03-31`、来源键 `000666.SZ:2023-03-31:1:1:1`、来源版本 `491097`、D/A `36.619`、来源哈希 `0e768431ffc85182c5b9d8cb2314e633a83e9d22c07df723ea143f859b6a60d8`，状态 `VALID`，原因 `PIT_SOURCE_ANNOUNCEMENT_ASSUMED`。

## 本轮验证

| 检查 | 结果 | 证据 |
|---|---|---|
| D/A 报告期错配负向单测 | PASS | 8 项因子/报告期测试通过 |
| 全新隔离 PostgreSQL 初始化 | PASS | 环境 ID `9eb5b77d-b473-455e-9bb6-08063d268132`，实例 `real-golden-v3-final2`；120 项迁移完成；只连接 `ashares_test` |
| Golden Sample 单测 | PASS | 1 passed |
| 保留 v2 的身份拒绝 | PASS | 从不可变 v2 bundle 重算时得到 `D/A feature report_date does not match Standard source identity`；manifest/bundle 哈希不变 |
| v3 Snapshot → Dataset → Alpha Reader E2E | PASS | 1 passed，166.30 秒；61 个 Snapshot 均冻结 |
| 财务因子参考对账 | PASS | 300 个因子值，0 差异；来源、PIT 和拒绝状态对齐 |
| 未来收益标签对账 | PASS | 90 条 20/40/60 日标签，0 差异；停牌及未知退市终值状态保留 |
| 正向 Dataset 冻结 | PASS | Dataset `461a4fdf-0a72-5fa2-a1fb-d12a5c38a2a8`，`FROZEN_AFTER_REFERENCE_PASS` |
| 注入参考差异后的冻结门禁 | PASS | Dataset `5f9b4dde-4358-5f03-afc2-6bf831987992`；1 项差异；冻结动作 `BLOCKED_REFERENCE_MISMATCH`，状态仍为 `VALIDATED` |
| 默认 pytest | PASS | `uv run pytest -q`：1,572 passed、1 skipped、19 deselected，43.74 秒 |
| Data Access Contract | PASS | 扫描 259 个文件，0 violations |
| Paper Trading Boundary | PASS | 检查 321 个可执行 Python 文件，无 QMT 交易 API |
| Ruff | PASS | Ruff 0.17.2；本次触及的 4 个 Python 文件全部通过；原 P2-E 47 个候选 Python 路径的增量基线为 0 新增诊断 |
| `git diff --check` | PASS | 无空白错误；仅提示排除范围内 `AGENTS.md` 与 QMT Skill 的 CRLF/LF 转换提醒 |
| Git 暂存状态 | PASS | index 为空；未提交、未推送 |

第一次全新 E2E 到达反向门禁时发现测试用 Dataset 版本字符串超过 40 字符上限。已缩短为稳定短身份，并在第二个全新隔离实例重跑成功。首次失败记录保留在 `.codex/real-sample/results/real-golden-e2e-v6-first-run.json`；最终结果在 `.codex/real-sample/results/real-golden-e2e-v6.json`。

最终结果 JSON 的 SHA-256：`2b1d29637199fec3433cf0118e4641f4216aae11183eaf995f212d8719a42532`（该文件处于忽略目录，不纳入候选 Git 内容哈希）。

## 发布候选身份

- 候选范围：原 P2-E 70 个候选文件，加 `tests/fixtures/real_golden_sample_v3.manifest.json` 与 `tools/build_real_golden_sample_v3.py`，共 72 个文件。
- 按路径、Git 两位状态、文件 SHA-256 排序后计算的候选状态/内容哈希：`01875180b6d70bb560bbe34399134644eb4ce0a48dc6bd9c24048fa8a61b037f`。
- 当前候选路径状态：25 个已跟踪修改、47 个未跟踪新增。另有原报告列出的 14 个无关路径仍在排除范围，未重置或清理。
- 迁移 116/117 身份未改，分别为 `13effca380f96eec94000e07ad080b018a7af8ca3f8e32ca28d5a66705ffe421` 与 `e8662de091104cdabee2ec886d5d7d2389d3f6eb80c0d22869bbac958c0f162d`。
- 计算范围排除了本报告和运行产物，避免自引用；`.codex` 中的真实 bundle 不受 Git 候选哈希覆盖，须通过 manifest SHA 和受控归档/导出流程保留。

候选结论：**READY FOR SELECTIVE COMMIT**。此状态只表示当前代码和验收身份适合进行指定文件的 Git 提交；不代表生产迁移或正式数据发布可执行。

## 仍然阻断的事项

1. `600699.SH` 公司行为仍为 `UNRESOLVED`，正式 Foundation Release 继续 BLOCKED。
2. 生产迁移 116/117 未执行；生产备份恢复证明、磁盘空间、授权 DDL 账号及维护窗口尚待独立验收。
3. 生产财务投影、Feature 物化、新 Foundation Release、五年 Snapshot/Dataset 和正式 Alpha 研究均未运行。
4. v3 压缩 bundle 被 `.gitignore` 排除。提交后必须确保该哈希的 bundle 可从受控归档取得或按可复现来源重新导出；Git 本身不含此测试输入。

生产写入、迁移、Release、正式策略审批及 Git 提交/推送：**NOT RUN**。
