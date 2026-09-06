# Backtest Diagnostic Trace（回测诊断追踪）

`data_collect.jobs.backtest_diagnostics` 是针对已持久化 Backtest Run（回测运行）的只读诊断工具。它不重新运行策略、不修改任何回测子表，也不创建逐笔诊断副本。

## 使用

```powershell
python -m data_collect.jobs.backtest_diagnostics `
  --run-id b97f6f67-8d8a-45e4-aeaa-2998d7f8401d
```

输出 JSON（结构化报告）包含：

- `order_rejections`：订单拒绝原因的稳定分类和原始原因样例；
- `trades`：由 `position_lot.open_fill_id / close_fill_id` 重建的买入、卖出、持有天数、费用、分红和净盈亏；
- `exit_attribution`：`MA60_EXIT`、`RANK_EXIT`、`QUALITY_EXIT`、`ST_EXIT` 等退出归因；
- `entry_diagnostics`：实际成交买入的 `RET60`、排名、历史 `is_st_asof`（时点 ST 状态）和输入快照；
- `forward_returns`：从成交价到未来第 5/10/20/40 个交易日收盘价的纯价格路径；
- `persisted_metrics`：数据库已保存的运行指标，供交叉核对。

## 口径与边界

1. 交易归因是对现有不可变事实的重建：`signal（信号）→ order（订单）→ fill（成交）→ position_lot（持仓批次）`。部分成交或一笔成交对应多个持仓批次时，费用按成交股数比例分摊。
2. `forward_returns` 只用于观察入场后的价格路径，不代表策略净收益，也不参与历史信号生成；未来行情读取限制在每笔实际买入后的 120 个自然日内，并只保留第 5/10/20/40 个交易日。
3. 所有数据库访问仍经过 `BacktestRunRepository → DataAccessKernel`；诊断任务不持有 Connection/Cursor，不手写 SQL。
4. 当前模型没有独立的 `realized_trade`（已实现交易）表，因此诊断结果默认即时生成。若未来需要长期保存报告，应将报告作为带 `run_id`、`input_hash`、`report_hash` 的独立不可变审计产物，而不是把它混入回测事实表。
