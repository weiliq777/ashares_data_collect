# Market Activity Monitor（市场成交活跃度监控）

## 定位

该模块回答：

> 某个交易日，哪些股票成交量和成交额最高，市场成交额是否集中，个股相对过去交易日是否明显放量。

它不回答“真实资金净流入了哪只股票”。成交量和成交额同时包含买方与卖方，只能描述交易活跃度和成交集中度。

## 数据边界

```text
std.daily_kline（标准日线：成交量=股，成交额=元）
        ↓ 只读 Repository
MarketActivityMonitor（市场成交监控）
        ↓
JSON / Dashboard / Research
```

该模块不写入业务数据表，不修改 Raw（原始层）、Standard（标准层）、PIT（时点层）、Feature（特征层）、Research Snapshot（研究快照）或 Strategy（策略）。生产 Kernel 仍会按统一访问契约记录 `ops.run_manifest`（运行清单）控制面审计信息。

## 公开接口

```python
MarketActivityMonitor(repository).daily(
    MarketActivityRequest(
        trade_date=date(...),
        lookback_trading_days=20,
        calendar_lookback_days=45,
        top_n=20,
    ),
    context,
)
```

接口保证：

- 单个有界日期窗口、单次流式读取，不按股票 N+1 查询；
- 不使用未来交易日数据；
- 同一股票同一交易日重复记录直接失败；
- 历史不足时返回 `INSUFFICIENT_LOOKBACK`，不填充为 0；
- 排名按指标降序、股票代码升序打破并列；
- 只读通过 `Repository → DataAccessKernel → ReadGateway`。

## 主要字段

| 字段 | 中文含义 |
|---|---|
| `volume` | 成交量（股） |
| `amount` | 成交额（元） |
| `amount_share` | 个股占当日有效成交额比例 |
| `amount_ratio_previous` | 相对前 20 个可用交易日平均成交额的倍数 |
| `volume_ratio_previous` | 相对前 20 个可用交易日平均成交量的倍数 |
| `top_n_amount_share` | 成交额最高 N 只股票占全市场有效成交额比例 |
| `quality_status` | 当前标准日线输入的完整性状态；不等同于全市场覆盖认证 |
| `insufficient_history_stock_count` | 缺少完整滚动窗口的股票数量 |

## 供应商资金流的后续边界

供应商的主力/超大单净流入属于估算型资金流，必须另建 Raw → Standard 数据域，并标记 `source_vendor`、字段口径和版本。不得回写旧 Observation（观察域）或改变现有回测输入。
