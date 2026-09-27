# Dataset Bundle 归档前置计划

本文件记录 `backtest.dataset_manifest` 的整体归档边界。它是执行前置契约，不授权物理删除。

## 当前真实结论

已批准的 3 个 smoke Dataset 当前仍有子表依赖，不能把主表当作孤立记录归档：

| Dataset | 状态 | 子表依赖总数 | 主要依赖 |
|---|---|---:|---|
| `foundation-v2-smoke-20260813` | APPROVED | 16,624 | execution_bar 16,606；member/calendar/checkpoint/rule 等 |
| `smoke-baseline` | APPROVED | 16,615 | execution_bar 16,606；member/calendar/rule 等 |
| `smoke-batched-20260809` | APPROVED | 16,621 | execution_bar 16,606；member/calendar/checkpoint/rule 等 |

三者当前 `run_manifest` 和 `portfolio.order_intent` 引用均为 0，但这不代表没有数据依赖。数据库外键为 `NO ACTION`，主表不能先于子表删除。

## Bundle 边界

以 `dataset_id` 为根，必须一并核对：

```text
backtest.dataset_manifest
├─ backtest.dataset_member
├─ backtest.dataset_calendar
├─ backtest.dataset_execution_bar
├─ backtest.dataset_rule_snapshot
├─ backtest.dataset_benchmark_bar
├─ backtest.dataset_build_checkpoint
├─ backtest.dataset_build_batch_checkpoint
├─ backtest.dataset_dividend_event
├─ backtest.run_manifest
└─ portfolio.order_intent (source_dataset_id)
```

其中 `run_manifest`、`order_intent` 属于外部使用者引用；其他表是 Dataset 自有子数据。任何一项非零，都不能执行“仅归档主表”。

## 必须遵守的执行顺序

1. 以 `registry_id` 和 `dataset_id` 重新执行引用预检，并设置有界查询超时。
2. 对主表及全部子表生成行数、业务键集合和恢复备份指纹。
3. 将完整 Bundle 写入独立、版本化、可恢复的归档存储，并完成抽样恢复演练。
4. 再次执行引用预检；出现新引用时整批停止，不改变生产事实。
5. 只有整体归档和恢复验证通过后，才允许在单一受控事务中更新注册表为 `ARCHIVED`。
6. 当前数据库契约将 Dataset Manifest 设为 append-only，并将 `VALIDATED/FROZEN` 子数据设为不可变；因此本管理入口不提供物理删除。若未来确需回收存储，必须另行设计并审批数据库级迁移、完整恢复演练和停机窗口。

## 当前禁止事项

- 禁止直接 `DROP backtest.dataset_manifest`。
- 禁止只删除 `dataset_manifest` 而保留或孤立子表。
- 禁止因 `run_manifest = 0` 就自动删除 Dataset。
- 禁止把 `APPROVED` 解释为已经完成物理归档。
- 禁止在没有独立恢复证据时把注册表改为 `ARCHIVED`。
- 禁止通过禁用触发器、超级用户绕过或临时改状态来删除 Dataset Bundle。

当前只读预检入口：

```text
python -m data_collect.jobs.retention_archive_admin --action plan
```

该入口只返回依赖和阻断状态，不执行任何物理归档或删除。
