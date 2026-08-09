# Backtest Run 审计记录

## 1. 审计对象

- Run：`c53dd0ef-3a41-46a5-8678-11bf5c2b2614`
- Dataset：`eaa310e5-e3b3-45b6-aba0-f973c3ea3f4f`
- Dataset 状态：`FROZEN`
- 策略：`pure_value_trend_v1`
- Run 状态：`SUCCEEDED`
- 评估区间：2021-08-02 至 2026-08-06
- 样本外起点：2025-08-06
- Git commit：`b5cce4e51bf9c5628c5a8fc2b3727f235ff054da`
- Git dirty：`false`
- Run `input_hash` 与 Dataset `input_manifest_hash`：一致

本记录只读核验数据库结果，不修改 Raw、Standard、PIT、Feature、Research Snapshot、Backtest Dataset 或 Run 结果。

## 2. 工程完整性核验

| 检查项 | 结果 | 说明 |
|---|---|---|
| Run / Dataset 状态 | PASS | Run 为 `SUCCEEDED`，Dataset 为 `FROZEN` |
| 输入哈希 | PASS | Run 与 Dataset 输入哈希一致 |
| 代码可追溯 | PASS | Git 工作区干净，记录了当前 commit |
| 交易日覆盖 | PASS | 1,215 个绩效日，覆盖 Dataset 评估区间 |
| 信号快照日期 | PASS | 250/250 条信号与对应 Snapshot 日期、股票代码一致 |
| T+1 | PASS | 250/250 条订单的 `order_date > decision_date` |
| 订单审计哈希 | PASS | 250/250 条订单均有 `order_audit_hash` |
| 成交关联 | PASS | 12 条成交均关联有效订单 |
| `actual_shares` | PASS | 实际成交前为 0；成交后与 Fill 股数一致；对账异常 0 |
| 拒单数量约束 | PASS | 拒单/过期订单的 `actual_shares` 均为 0 |
| 买入整手 | PASS | 买入请求股数均符合 100 股一手规则 |
| 现金账本 | PASS | 现金余额链逐笔连续一致 |
| 每日权益 | PASS | `cash + position_value + receivable` 与 `total_equity` 一致 |
| 负费用/负价格 | PASS | 费用无负值，执行行情无 INVALID 记录 |

结果表记录数量：signals 250、orders 250、fills 12、position_lot 8、position_daily 42、cash_ledger 13、performance_daily 1,215、metrics 31。

## 3. 规则与风控核验

本次 Run 使用的核心规则：

- 初始资金：10,000 元；
- 单笔风险：1%；组合风险上限：3%；
- 单只最大仓位：25%；预留现金：10%；
- 买入整手：100 股；T+1：启用；卖出零股：允许；
- 单边滑点：10 bps；成交量参与率：5%；超出流动性整笔拒单；
- 佣金：0.03%，最低 5 元；印花税按三段历史规则；
- Monte Carlo：bootstrap with replacement，seed 42。

订单最大实际风险约 0.9356%，未超过单笔 1% 上限。执行行情中 6,282,097 条为 `VALID`，27,797 条为 `WARNING`，0 条 `INVALID`，0 条 `MISSING_SOURCE`。WARNING 主要来自停牌、不可买卖或无价格证券，未被伪造为可执行行情。

## 4. 回测结果

- 最终权益：9,900.12 元；
- 总收益：-0.9988%；
- 最大回撤：-0.9988%；
- 成交：12 笔，4 个交易；
- 最终未平仓：4 个持仓批次；
- 基准总收益：-5.7243%；
- 全区间相对基准：+4.7256%；
- 样本外总收益：-0.9988%；
- 样本外基准收益：+13.3512%；
- 样本外相对收益：-14.3500%。

## 5. 研究有效性结论

### 结论：工程审计通过，五年策略有效性审计不通过

当前策略的 250 条信号全部集中在 2026-07-01 至 2026-08-05 的 26 个交易日，2021-08-02 至 2026-06-30 没有产生任何信号。原因是：

```text
feature.valuation_daily_v1.pe_pct_5y
```

在当前 Feature 数据中直到 2026-07-01 才开始出现有效值。`pure_value_trend_v1` 将 `pe_pct_5y` 作为硬过滤条件，因此前面约四年的数据不是“没有股票符合策略”，而是缺少完整的五年估值分位暖机区间。

因此，本次 Run 只能证明：

1. Frozen Dataset 能被加载并执行；
2. Candidate → Order → Fill → Ledger → Performance 链路可运行；
3. T+1、整手、费用、风控和审计哈希逻辑通过本次数据验证。

本次 Run 不能证明 `pure_value_trend_v1` 在五年周期内有效。整体相对基准的正值也不能作为策略优势结论，因为大部分区间没有策略交易，样本外实际只有最后约一个月产生交易。

## 6. 进入正式研究前的必要修复

应以当前 Raw 为事实源，扩展历史标准化和特征暖机区间：

```text
Raw（日线/估值数据已有 2016-08-01 起）
→ Standard 行情与估值至少回补至 2016-08-02
→ PIT / Feature 重建五年 PE 分位暖机
→ 重新生成 2021-08-02 至 2026-08-06 的 FROZEN Research Snapshot
→ 重新创建、build、validate、freeze Backtest Dataset
→ 重新运行并审计 Run
```

至少需要在 2021-08-02 评估日前准备约五年的估值历史；实际暖机起点应由指标窗口规则和可用交易日共同确定。现有 Raw 已具备 2016-08-01 起的数据，但当前 `daily_kline`、`valuation_daily` 和 Feature 层从 2021-08-02 开始，因此不能直接复用当前五年 Dataset。

在暖机区间修复、重新冻结并确认有足够跨年份交易样本前：

- 不发布正式策略绩效结论；
- 不进入 AI 决策 API；
- 不生成真实交易建议；
- 不接入自动交易。

