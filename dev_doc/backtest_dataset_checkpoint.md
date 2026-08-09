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

## 2026-08-09 批次构建优化

旧版 `BUILD` 虽然使用了 `execute_values`，但仍把整个执行行情物化放在一个长事务中；这会在五年数据量下制造较大的 WAL、死元组、索引维护和临时排序压力。现已新增 `sql/043_backtest_dataset_batch_checkpoint.sql`，并由 `backtest_dataset_handoff.py` 默认使用 `build_dataset_batched()`。

新流程保持原有 Raw 高水位、单一 Raw payload 选择、单位换算、质量标记、血缘字段和最终哈希口径不变，只改变事务边界：

```text
Dataset BUILDING
→ 读取 5 个交易日（可用 --batch-days 调整，范围 1~60）
→ 删除当前 Dataset 这一个日期区间的旧执行行
→ 查询并批量写入这一个区间
→ 同事务更新成员统计和批次 COMPLETED
→ 提交
→ 下一批
→ 小型日历/分红/规则/基准表单独重建
→ 流式重算全局哈希
→ 原有 Validate → Freeze 门禁
```

`backtest.dataset_build_batch_checkpoint` 的主键是 `(dataset_id, stage, batch_start)`，记录日期范围、行数、批次哈希、代码版本、状态和心跳。进程在批次中途退出时，数据库事务回滚；重启会重新执行该批次。只有状态为 `COMPLETED`、日期范围、批次哈希和代码版本均匹配时才跳过已完成批次。`--force-rebuild` 只清理当前 Dataset 的批次 checkpoint，随后仍按日期区间逐批删除和重建，不对共享表执行全局 `TRUNCATE` 或全表删除。

该优化不修改任何 Raw/Standard/PIT/Feature，也不改变 Dataset 的最终输入哈希和 Validate 规则。建议五年任务先使用 `--batch-days 5`；如数据库资源稳定，可逐步试到 10 或 20。Docker/数据库不可用时不要启动构建，恢复后可直接复用同一个 `--dataset-id` 续跑。

强制重新 Build：

```text
追加 --force-rebuild
```
