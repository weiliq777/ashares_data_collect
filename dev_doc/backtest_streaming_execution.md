# Backtest Dataset 流式执行契约

## 目的

`backtest.dataset_execution_bar` 与 Research Snapshot 观察输入规模较大时，正式回测不得把整个五年序列一次性物化到 Python 内存。正式回测使用 `StreamingFrozenDataset`，按交易日分页读取并保持策略状态连续。

## 不可破坏的边界

- 只读 `FROZEN` 的 Backtest Dataset；不回写 Raw、Standard、PIT、Feature 或 Research Snapshot。
- 数据库访问仍然是 `Repository → Kernel → Read Gateway`，引擎和策略不接触 SQL、连接或游标。
- 行情与观察数据按单个交易日、固定页大小和 `(trade_date, ts_code)` / `(research_date, ts_code)` keyset 分页读取。
- 同一交易日数据在本次回测中只加载一次；只保留有限日期 LRU 缓存，不执行逐股票查询。
- 交易日之间的策略滚动状态由同一个 `BacktestEngine` 连续维护，不能按页或按日重置策略。
- 日期边界、PIT 输入列、规则、除权事件、基准和 Dataset `input_manifest_hash` 与内存版读取器保持一致。

## 运行方式

`run_frozen_backtest()` 默认使用流式读取。命令行的 `--stream` 用于显式声明该模式；`--materialized` 仅用于受控的小样本调试和单元测试，不适合五年正式回测。

流式读取器默认每次最多读取 10,000 行，行情缓存 3 个交易日、观察缓存 2 个交易日。辅助输入（交易日历、规则、基准和公司行为）规模较小，可以物化；大表输入不得改回全量内存加载。

## 验证要求

变更流式读取逻辑后必须通过：

1. 流式与内存版在相同小样本上的指标和逐日净值一致；
2. 分页游标严格前进、日期不越界、重复业务键 fail-closed；
3. 同日重复访问命中缓存，缓存淘汰后才重新读取；
4. 读取器关闭幂等，关闭后禁止继续读取；
5. 全量单元测试、数据访问契约扫描、Python 编译检查；
6. 真实库只读小窗口回测成功后，才允许启动长区间正式回测。
