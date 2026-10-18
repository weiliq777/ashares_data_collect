# 离线实验回测与正式策略审批解耦设计

日期：2026-10-18

状态：设计建议，未修改源码或数据库。

## 目标与约束

允许在 FROZEN Dataset 上对一个明确版本的未批准策略做离线研究；实验结果必须可复现并清楚标识为实验。正式基准回测、候选生成、人工决策候选和纸面交易继续受 `APPROVED` 门禁保护。实验功能不写入 Candidate、Portfolio 或 paper-trading 记录，不自动批准策略。

## 当前审批决策

本轮决定不登记或批准 `fundamental_value_growth_trend_v2`。数据库当前没有该策略的已批准版本，因此它的正式回测入口受 `APPROVED` 门禁阻断；候选服务也不会将其视为可运行策略。此状态按正式路径的阻断项报告，不通过临时批准绕过。若需要继续研究，应先实现并验收本文的隔离实验路径，再以 `EXPERIMENTAL` Run 执行；实验路径落地前，该策略仍不能启动正式回测。

## 当前审批耦合点

| 位置 | 现有行为 | 对实验的影响 |
|---|---|---|
| `data_collect/jobs/backtest_run.py:67-111` | `register_strategy()` 只允许已存在的 `APPROVED` 策略；新登记要求审批人和证据哈希，并直接登记为 `APPROVED` | 未批准策略无法登记为可回测版本 |
| `data_collect/jobs/backtest_run.py:116-163` | `create_run()` 显式绑定版本，并要求 Repository 返回 `APPROVED` 策略和 `FROZEN` Dataset | 正式 Run 门禁阻断实验策略 |
| `data_collect/data_access/repository/backtest_run_repository.py:22-31` | `strategy()` SQL 只查询 `approval_status='APPROVED'` | 不能用现有正式查询解析 DRAFT |
| `sql/095_backtest_strategy_version_binding.sql:38-70` | 数据库触发器要求所有进入 `RUNNING`/`SUCCEEDED` 的 Run 绑定 APPROVED 版本 | 只绕过 Python 检查不能成功写入 Run |
| `sql/049_data_foundation_stage1_contracts.sql:72-91` | 策略版本表支持 `DRAFT` 状态，并对版本元数据实施 append-only | 可记录非批准定义，但不能原地把实验定义改成正式定义 |
| `data_collect/decision/candidate_service.py:269-283` | 候选服务独立读取 `approved_strategy()`，并校验参数哈希 | 应保留该门禁，不让实验状态流入候选 |

这说明单独加一个 `--allow-unapproved` 参数不够：数据库触发器仍会拒绝 Run。实验身份必须进入 Run 契约并由数据库一并验证。

## 最小设计

### 1. 保留同一引擎与 Run 子表，增加明确的 Run 用途

为 `backtest.run_manifest` 增加不可变的 `run_purpose` 字段，建议值：`FORMAL`、`EXPERIMENTAL`；历史无法可靠分类的旧记录迁移为 `LEGACY`。新 Run 必须显式提供用途，不能从 Run 名称或调用者意图推断。

信号、订单、成交、持仓、日净值等仍按 `run_id` 写入现有 Run 子表，不复制回测引擎或成交存储。Run 报告和结果查询按用途显式分区：正式评价/基线只选 `FORMAL`，探索报告只选 `EXPERIMENTAL`，两者都不纳入 `LEGACY`。报告固定显示用途水印。

### 2. 实验版本保持 DRAFT，不能借实验流程审批

增加专用 `register_experimental_strategy()` 路径，只能追加 `DRAFT` 策略版本，包含：策略 ID/版本、参数及哈希、代码版本、代码归档 SHA-256、创建时间和实验说明。拒绝 `APPROVED` 状态输入，不填写 `approved_by`、`approved_at` 或审批证据哈希。策略版本记录保持不可变。

当前策略表已经支持 DRAFT，因而第一版无需另造实验策略定义表。正式 `register_strategy()` 保持现状。由于策略元数据不可原地更新，若实验通过，正式审批应以一个新的版本号登记正式定义和审批证据；原 DRAFT Run 继续是实验结果。

### 3. 增加独立实验 Run 入口与数据库分支校验

新增 `create_experimental_run()`，不放宽现有 `create_run()`。它复用相同 BacktestEngine 和 Repository，但必须校验：

- 策略版本存在且状态严格为 `DRAFT`，运行参数哈希与登记值一致；
- Dataset 是 `FROZEN`，研究区间、交易日历、费用/市场规则符合冻结契约；
- 参数、场景、执行模型、输入哈希和代码归档身份完整，脏工作区必须归档；
- Run manifest 写入 `run_purpose='EXPERIMENTAL'`，状态流转和审计字段与正式 Run 一样不可覆盖。

更新 `backtest.assert_run_strategy_binding()`：`FORMAL` Run 仍要求策略版本为 `APPROVED`；`EXPERIMENTAL` Run 只允许绑定 `DRAFT` 策略版本，并要求 FROZEN Dataset；不允许通过空值或未登记版本运行。对历史 `LEGACY` Run 只保留原记录，不允许新的 Run 使用该用途。

### 4. 正式候选与回测基线继续隔离

`candidate_service.py` 继续只检查 `APPROVED`。实验 Run ID 不得作为 CandidateRun、纸面意图或 Portfolio 的 `source_backtest_run_id`。正式策略基线、资本敏感性报告、Readiness 清单和生产候选查询都显式过滤 `run_purpose='FORMAL'`。实验结果可由研究报告通过实验 Run ID读取，但不能被呈现为已审批策略表现。

## 最小迁移顺序

1. 增加 `run_purpose` 字段及约束；根据可追溯的策略审批记录标记历史正式 Run，其余保守标记 `LEGACY`。
2. 新增实验策略登记 Repository API 和实验 Run API；既有正式 API 保持严格 APPROVED 校验。
3. 更新数据库 Run 绑定触发器，在用途分支上验证相应的策略状态与 Dataset 状态；保留 append-only 和运行身份不可变约束。
4. 将正式评价、策略复现、候选和 Readiness 查询显式限定为 `FORMAL`；新增实验报告筛选和醒目水印。
5. 在非生产测试数据库验证迁移与回滚；然后对小型 FROZEN Dataset 跑一个 DRAFT 策略实验，验证未批准 Run 能完成且不进入正式候选、纸面交易或正式基线。

## 验收用例

- DRAFT 策略 + FROZEN Dataset + 完整身份 → 可以创建并完成 `EXPERIMENTAL` Run。
- DRAFT 策略 + 正式 Run API → 拒绝；数据库触发器也拒绝。
- APPROVED 策略 + 正式 Run API → 保持现有行为。
- CandidateService 对 DRAFT 版本仍拒绝；不得从实验 Run 创建候选、Portfolio 或纸面意图。
- Run 子表和报告通过 Run ID 可复现；改变参数、代码归档、Dataset 或规则身份会产生不同哈希/Run 身份。
- 正式基线与 Readiness 统计不含实验及 LEGACY Run；实验报告标注 `EXPERIMENTAL / NOT APPROVED`。

## 设计边界

这项设计只解决“未批准策略能否用于可追溯的离线实验”，不批准 `fundamental_value_growth_trend_v2`，不改变 `QualityTrendRotationV1` 或其他策略定义，不允许生产候选/自动交易，也不宣称实验表现已通过策略收益验收。当前阶段仅建议实现前先在测试数据库上验证迁移和门禁逻辑。
