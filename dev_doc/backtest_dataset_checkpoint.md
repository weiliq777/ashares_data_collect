# Backtest Dataset 构建与恢复规则

更新时间：2026-08-09

## 当前实现

`backtest.dataset_build_checkpoint` 记录三个阶段：

```text
BUILD → VALIDATE → FREEZE
```

每个阶段记录状态、处理数量、阶段哈希、详情、错误信息和心跳时间。checkpoint 只记录构建流程元数据，不写入或修改 Raw、Standard、PIT、Feature。

## 恢复规则

- `BUILD` 已完成且 Dataset 代码版本、Git commit 和干净状态仍一致时，重启可跳过 BUILD，直接进入 VALIDATE。
- BUILD 中途失败时，当前版本仍按原子流式构建处理；下次会重新构建，避免使用不完整物化数据。
- VALIDATE 失败后，可以复用已完成 BUILD，不重复物化执行行情。
- FROZEN Dataset 不允许修改 checkpoint。
- `--force-rebuild` 可显式忽略已完成 BUILD checkpoint，强制重新物化。

## 一致性边界

本次优化没有改变：

- Raw 版本选择和基线高水位；
- 行情、涨跌停、停牌、公司行为和规则字段映射；
- Dataset 成员级及全局哈希；
- Validate 质量门禁；
- FROZEN 后的只读约束。

当前首版仍保留最终完整 Validate 扫描，确保物化子表内容可审计。因而 checkpoint 首版主要减少“验证失败后重复 Build”的成本，并提供阶段状态可观测性；它不是逐交易日提交的断点续传。后续若要实现交易日级续传，需要单独设计分日物化表、批次事务和分日哈希契约，不能直接把现有单事务改成并发写入。

## 启动方式

继续已有 Dataset：

```powershell
uv run python -m data_collect.jobs.backtest_dataset_handoff `
  --baseline-tag baseline_20260806 `
  --schema-version research-v1.3 `
  --expected-snapshots 1215 `
  --warmup-start 2021-08-02 `
  --evaluation-start 2021-08-02 `
  --evaluation-end 2026-08-06 `
  --dataset-name history-baseline-v13-final `
  --dataset-version dataset-v1.3-ee0a053 `
  --dataset-id <DATASET_ID>
```

强制重新 Build：

```text
追加 --force-rebuild
```
