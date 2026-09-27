# Market Activity Observation V1（成交活动观察 V1）

状态：实现契约（2026-09-24）

## 1. 目标和边界

Market Activity（成交活动）回答“市场发生了什么”；Observation（观察域）回答“哪些事实出现了异常”。本模块不输出买入、卖出、看涨、看跌或主力资金流入结论，也不修改 Raw（原始层）、Standard（标准层）、PIT（时点层）、Feature（特征层）和已冻结的 Market Activity（成交活动）事实。

输入必须是 `observation.market_activity_snapshot.status = FROZEN` 的 Market Activity 快照。Observation 结果复用现有 `observation.snapshot`、`observation.stock_anomaly_daily` 和 `observation.stock_anomaly_tag`，通过新的方法论哈希与旧 Observation 结果隔离。

## 2. V1 方法论

默认参数：

```yaml
policy_id: market_activity_rules_v1
min_history_observation_count: 20
ratio_threshold: 2.0
high_amount_share_threshold: 0.005  # 0.5%，可配置且进入 methodology_hash
```

仅当 `calculation_status = VALID` 且 `history_observation_count >= 20` 时，才允许触发以下标签：

| 标签 | 触发规则 | 证据来源 |
| --- | --- | --- |
| `ABNORMAL_VOLUME`（异常成交量） | `volume_ratio_previous >= 2.0` | `volume_ratio_previous` |
| `ABNORMAL_AMOUNT`（异常成交额） | `amount_ratio_previous >= 2.0` | `amount_ratio_previous` |
| `SUDDEN_ACTIVITY`（突然活跃） | 两个倍数都 `>= 2.0` | 两个倍数的较小值，证据中同时保留两个输入 |
| `HIGH_AMOUNT_SHARE`（成交额占比高） | `amount_share >= 0.5%` | `amount_share` |

`volume_ratio_previous` 和 `amount_ratio_previous` 都是目标日相对于前 20 个有效交易日平均值的倍数；目标日不进入自身基线。所有规则确定性排序，股票代码是最终稳定排序键。

每条标签证据保存：

```json
{
  "tag_code": "ABNORMAL_VOLUME",
  "triggered": true,
  "actual_value": 7.72,
  "threshold": 2.0,
  "source_metric": "volume_ratio_previous",
  "lookback_trading_days": 20,
  "history_observation_count": 20,
  "source_activity_snapshot_id": "...",
  "source_input_manifest_hash": "...",
  "source_activity_hash": "..."
}
```

数据库只保存触发的标签；没有标签记录表示该股票在本规则下未触发，而不是缺失数据。股票主体行仍保存所有输入事实和 `calculation_status`，因此可以区分“未触发”和“无法判断”。

## 3. 明确暂不实现的标签

`VOLUME_PRICE_UP`（放量上涨）和 `VOLUME_PRICE_DOWN`（放量下跌）暂不在 V1 产生。当前冻结的 Market Activity V1 输入没有目标日 `return_1d`，Observation 不会直接读取可变的 Standard 表来补字段，也不会根据缺失字段猜测涨跌。

如果以后需要这两个标签，必须先发布带明确 `return_1d` 的新 Market Activity 输入契约，再生成新的 Observation methodology hash；旧的 Observation 结果不可覆盖。

## 4. 身份和生命周期

Observation 快照身份由以下内容确定：

```text
source_activity_snapshot_id
observation_date
methodology_hash
data_baseline_id
```

规则参数、输入 Market Activity 契约版本和支持的标签都会进入 `methodology_hash`；输入行集合进入 `input_manifest_hash`；输出行和证据进入 `observation_hash`。状态遵循：

```text
DRAFT → BUILDING → VALIDATED → FROZEN
             └──────────────→ FAILED
```

`FROZEN`、`SUPERSEDED`、`CORRUPTED` 记录由数据库触发器保护，不能更新或删除。重复执行同一身份是幂等的；参数或输入来源变化必须生成新身份。

## 5. 运行方式

先执行受版本控制的迁移 `sql/106_market_activity_observation_v1.sql`，再调用：

```powershell
python -m data_collect.jobs.market_activity_observation `
  --activity-snapshot-id c6397da9-e398-5e76-bc13-f4673cd4314d `
  --freeze
```

迁移和运行都不会重建历史基础数据。当前代码开发阶段只完成契约、实现和测试；生产迁移/回填必须在单独的运行步骤中执行，并先用一个已冻结快照做小范围验证。

## 6. 质量状态

| 状态 | 含义 |
| --- | --- |
| `VALID` | 所有输入股票均有足够历史且输入有效 |
| `PARTIAL_HISTORY` | 至少一只股票历史不足；该股票不打异常标签，其他有效股票仍可观察 |
| `INVALID` | 输入行存在不可接受的身份或质量错误；构建应失败而不是降级 |

`PARTIAL_HISTORY` 不是把缺失数据补成 0，也不是把未触发误判为正常。AI 或 Dashboard 应同时查看主体行的 `calculation_status`、标签证据和质量状态。
