# Market Activity → Observation 每日链路

## 目的

这是成交活跃度观察域的单入口运行方式，不改变 Raw（原始层）、Standard（标准层）、PIT（时点层）、Feature（特征层）、Research Snapshot（研究快照）或 Backtest（回测）数据。

```text
std.daily_kline
    ↓ 只读
Market Activity（成交事实）
    ↓ FROZEN（冻结）
Observation V1（异常标签与证据）
    ↓ FROZEN（冻结）
Dashboard / AI Research（展示与分析）
```

## 每日生产入口

每日 Tushare 生产链在 `production_quality`（生产质量门禁）通过后，自动执行
`data_collect.jobs.market_activity_production_daily`。它只在最近 31 个自然日内
读取最新一条 `daily`（日线行情）质量门禁，要求状态为 `PASS`，并为该交易日
创建或复用 `market-activity-YYYYMMDD` 的 `MARKET_ACTIVITY`（市场观察域）发布基线。
发布通过后，仅冻结该交易日的 Market Activity（成交活跃度）和 Observation（异常观察）。

同一日期的有效发布会被复用；禁止把当前发布基线回退到更早日期，也不会调用完整
Research（研究域）发布审计、重建历史数据或触碰策略/回测数据。

手动补跑/诊断时仍可使用下方单日入口，但必须提供当前有效的成交观察基线。

## 手动单日入口

```powershell
python -m data_collect.jobs.market_activity_observation_daily `
  --trade-date YYYY-MM-DD `
  --data-baseline-id <validated-baseline>
```

入口固定执行两步：

1. 构建并冻结 `observation.market_activity_snapshot` 及其明细；
2. 读取该冻结成交快照，构建并冻结 Observation V1。

两步均使用稳定的计算运行身份，重复执行已冻结日期只读校验并跳过，不重复重建。

## 基线截止日门禁

`data_baseline_id` 必须指向当前 `VALIDATED（已验证）` 的、兼容成交观察契约的数据发布点。允许的发布契约是 `market-activity-v1` 或完整基础契约 `feature-contract-v2.0`。同时必须满足：

```text
trade_date <= baseline_cutoff_date
```

超过截止日时，入口在创建快照前失败，不会把新日期写入旧基线。要处理更晚日期，先发布覆盖该日期的新 validated 基线，再运行同一入口；不是修改旧快照，也不是重跑 Raw。

当只是扩展成交观察日期时，发布 `market-activity-v1` 基线即可；不应为了观察域重新声称一份尚未完成的 `feature-contract-v2.0` 全量基线。

## V1 当前边界

Observation V1 只产生成交量、成交额相对前 20 个有效交易日的异常标签和证据，不产生买卖结论，也不把成交额称为资金净流入。`return_1d` 尚未进入 Market Activity 冻结输入，因此放量上涨/下跌标签不在本版本中生成。

## 运行安全

- 所有读取和写入仍通过 Repository → Kernel → Gateway；
- 日期窗口和股票行数均有上限；
- 每个日期独立生命周期和短事务，不执行全历史扫描；
- 生产调度器应将同一次重试的 `--run-id` 固定传入；
- 当前 `foundation_v2_20260821` 只能处理不晚于 `2026-08-21` 的日期。
