# Backtest Diagnostic Trace（回测诊断追踪）

## 标准 Evaluation（评估）边界

诊断报告现在同时提供 `standard-evaluation-v1`（标准评估 v1）派生结果：

- `calendar.month / quarter / year`：按自然月、自然季度和自然年复合策略/基准日收益；
- `rolling_12m`：252 个交易日窗口、每 21 个交易日一个观察点；
- `market_regime`：基准 `BENCHMARK_MA120_SLOPE_V1`，即收盘价相对 MA120（120 个交易日均线）及均线方向划分 BULL（上涨）、BEAR（下跌）、SIDEWAYS（震荡）；历史不足标记 UNKNOWN（未知），不当作震荡；
- `build_capital_sensitivity()`：只汇总同一 Dataset（回测数据集）、Strategy（策略）、Evaluation（评价口径）和交易参数下的不同 Scenario（交易场景）Run，初始资金之外的身份不一致时拒绝比较。

标准评估中的 `max_drawdown`（最大回撤）统一保存为非负的回撤幅度，与已持久化 Run 指标一致；收益率和超额收益率保留正负号。

这些结果是只读派生视图，不写入 Raw（原始层）、Standard（标准层）、PIT（时点层）、Feature（特征层）或 Frozen Dataset（冻结数据集），也不改变 Strategy V1（策略 V1）的交易决策。

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
- `evaluation`：标准化年月、滚动 12 个月和市场状态切片；其中市场状态只用于事后评价。

## 前端看板

启动本地只读 Dashboard（回测看板）：

```powershell
python -m data_collect.web.backtest_dashboard --port 8765
```

浏览器打开 `http://127.0.0.1:8765/?run_id=<run_id>`，或在页面输入 `run_id`。看板提供：

- 策略净值、基准净值、回撤、现金占比和持仓数量曲线；
- 月度/年度收益、拒单类别、退出原因和入场 `RET60`（60日收益率）分布；
- 可点击的完整交易表；
- 单笔交易的基础复权价格、MA20、MA60 与买卖点。

资金敏感性接口：

```text
GET /api/backtests/compare?run_ids=<run_id_10k>,<run_id_30k>,<run_id_50k>,<run_id_70k>,<run_id_100k>
```

接口最多读取 10 个 Run，使用同一份只读诊断报告做身份校验和汇总，不会自动启动回测。

服务只绑定 `127.0.0.1`（本机），API 只读，不提供数据库写入路由。

## 口径与边界

1. 交易归因是对现有不可变事实的重建：`signal（信号）→ order（订单）→ fill（成交）→ position_lot（持仓批次）`。部分成交或一笔成交对应多个持仓批次时，费用按成交股数比例分摊。
2. `forward_returns` 只用于观察入场后的价格路径，不代表策略净收益，也不参与历史信号生成；未来行情读取限制在每笔实际买入后的 120 个自然日内，并只保留第 5/10/20/40 个交易日。
3. 所有数据库访问仍经过 `BacktestRunRepository → DataAccessKernel`；诊断任务不持有 Connection/Cursor，不手写 SQL。
4. 当前模型没有独立的 `realized_trade`（已实现交易）表，因此诊断结果默认即时生成。若未来需要长期保存报告，应将报告作为带 `run_id`、`input_hash`、`report_hash` 的独立不可变审计产物，而不是把它混入回测事实表。
