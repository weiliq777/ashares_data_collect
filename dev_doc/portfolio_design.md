# Portfolio 模拟组合设计契约

## 2026-08-08 scope clarification

Portfolio v1 is a manual, single-frozen-snapshot paper-account boundary. Corporate-action settlement (cash dividends, bonus shares, conversions and ex-right stop transformation) is modeled by Backtest, not synthesized by `portfolio_simulation.mark_to_market`. A multi-day paper account must bind a later frozen execution Snapshot/Portfolio version before valuation; it must not reuse a mutable dividend table or infer dividend cash from a changed raw close. This keeps Portfolio from claiming a longitudinal total-return result that it cannot currently reproduce.

## 2026-08-07 每日候选与 AI 审计隔离

每日前向纸面组合采用独立的人工链路：

```text
FROZEN Snapshot
  -> candidate_run / candidate
  -> as-of candidate packet
  -> strict AI audit JSON
  -> human APPROVE/REJECT/DEFER
  -> DAILY_SIGNAL/PENDING_APPROVAL intent
  -> separate intent approval
  -> PAPER_MANUAL fill
```

`sql/034_daily_candidate_ai_audit.sql` 为候选、AI 审计、人工作决定和
`portfolio.snapshot_binding` 提供不可变关联。每日组合可以按交易日绑定
不同的 FROZEN Snapshot；历史 BACKTEST 导入仍使用原有固定 Snapshot 和订单
审计哈希门禁。AI 审计不是买入信号，也不会批准意图、写入成交或调用券商。

## 2026-08-06 Backtest 人工隔离增强

`sql/032_backtest_portfolio_airgap.sql` 将 Backtest 与 Portfolio 的边界提升为数据库门禁：

```text
SUCCEEDED Backtest Run + FROZEN Dataset + FILLED order
    -> import_backtest_order()
    -> Portfolio order_intent: BACKTEST / PENDING_APPROVAL
    -> 人工批准
    -> PAPER_MANUAL 模拟成交
```

导入记录保存 `source_backtest_run_id`、`source_backtest_order_id`、`source_dataset_id`、`source_signal_id` 和规范化 `source_order_hash`，同一 Portfolio/Backtest 订单幂等导入；Hash 变化时拒绝覆盖。导入函数不会批准、成交、访问券商或发送真实订单；人工从 `PENDING_APPROVAL` 更新为 `APPROVED` 后仍必须通过既有人工成交门禁。数据库触发器还要求源订单、Run、Dataset 状态正确，非 BACKTEST 意图不得携带 Backtest 血缘字段。

Portfolio v1 仍绑定单个 FROZEN Snapshot，导入订单的决策日必须等于该 Snapshot cutoff，若绑定 ACTIVE Watchlist，标的必须属于该观察池。多日期组合运行需要新的 Snapshot/Portfolio 版本，不能把未来日期回写到当前组合。

更新时间：2026-08-06

## 1. 边界

Portfolio 是 Research/Backtest 之后的人工确认模拟组合层，不是券商接口，也不自动生成或发送订单。
组合只能绑定 `FROZEN` Research Snapshot；如果绑定观察池，观察池必须是同一快照下的 `ACTIVE` 观察池。

```text
FROZEN Research Snapshot / ACTIVE Watchlist
→ portfolio.account_manifest
→ order_intent（待人工批准）
→ trade_fill（仅 PAPER_MANUAL）
→ position_lot / cash_ledger
→ position_daily / performance_daily
```

## 2. 人工审批与状态

- 意图初始为 `PENDING_APPROVAL`，只有明确写入 `APPROVED` 和批准人后才能模拟成交。
- `APPROVED` 只能进入 `EXECUTED`、`CANCELLED` 或 `EXPIRED`；已批准的交易事实不可回改。
- 模拟成交禁止部分成交，成交股数必须等于意图股数。
- `trade_fill.source_type` 固定为 `PAPER_MANUAL`，系统不访问券商、不发送真实订单。
- 意图的股票必须存在于绑定的 FROZEN Snapshot；如果指定观察池，还必须是该 Snapshot 下
  的 ACTIVE 观察池成员，且决策日不能晚于快照截止日。
- 如果组合 Manifest 绑定了观察池，意图必须继续绑定同一个观察池，不能通过省略字段绕过观察池范围。

## 3. 资金与持仓规则

- 买入必须按 `min_buy_lot` 整手，且扣除佣金、过户费后不能突破预留现金。
- Portfolio v1 禁止已有持仓再次 BUY，避免向下摊平和隐式加仓。
- T+1 组合必须显式提供下一个交易日作为 `available_sell_date`；卖出前逐批检查可卖日期。
- 卖出零股、资金结算日、最小价位和费用均来自组合 Manifest，不能使用隐式当前规则；
  `cash_settlement_days` 必须持久化到 `portfolio.account_manifest`。若结算延迟，
  卖出入账必须显式提供晚于成交日的 `cash_available_date`。

## 4. 估值写入与复现门禁（2026-08-06）

`position_daily` 和 `performance_daily` 不是可覆盖缓存，而是估值事件结果：每行保存
`source_snapshot_id` 与 `valuation_input_hash`，后者覆盖估值日的现金、持仓批次和
FROZEN Snapshot 原始收盘价。相同输入重复执行只返回幂等结果；同一组合/日期出现
不同输入时拒绝覆盖，数据库也禁止更新或删除估值行。

估值日期必须单调推进，且不能在已经写入更晚成交后回补过去日期；需要修正时创建
新的 Portfolio 版本。这样可以避免未来成交被带入历史估值，也保留可审计的输入血缘。

现金账本、成交和审计事件追加写入；持仓批次只允许更新剩余数量和关闭信息，开仓事实不可变。

### 4.1 估值输入

每日估值 v1 必须显式提供与估值日严格相同截止日的 `FROZEN` Snapshot；传入价格必须逐个
与该快照中的原始收盘价一致。缺少持仓价格或价格不一致时拒绝生成有效估值，避免把外部、
未来或缺失价格混入组合账本。
每次估值保存价格切片哈希、快照 ID、现金、持仓市值、总权益和回撤。

数据库触发器还会独立校验人工成交的请求股数、整手/零股规则、T+1 可卖日期、
现金与费用、价格最小变动单位、买入止损、已有持仓禁止加仓，以及成交日不能晚于
绑定 FROZEN Snapshot 截止日。Python 校验失败或被绕过时，数据库仍必须拒绝不一致成交。

实现文件：

- `sql/029_create_portfolio_model.sql`
- `data_collect/portfolio/`：纯 Python 人工审批、成交、T+1、费用和估值模型
- `data_collect/jobs/portfolio_simulation.py`：组合创建、意图、人工状态、成交和估值入库

当前生产数据库尚未创建正式 Portfolio；纯 Python 测试、迁移契约和临时隔离数据库的 Dataset→Run→Portfolio 联调已通过，联调数据已清理。阶段三最新定向测试为 59 项通过；正式创建组合仍需干净 Git 版本、多日期 `research-v1.1` FROZEN 快照和有效日期规则，不能绕过 FROZEN Snapshot 门禁。Portfolio v1 默认 `cash_settlement_days=0`，但该规则仍写入账户 Manifest，后续改变必须创建新的组合版本。

### 4.2 单日快照与 T+1 边界

Portfolio v1 当前一个账户绑定一个单日 `FROZEN` Snapshot，且决定日必须严格等于该 Snapshot 的截止日。数据库会拒绝晚于该截止日的人工成交；因此使用 T+1 规则时，决定日可以先记录为待审批意图，但正式成交必须绑定后续可见的执行 Snapshot/Portfolio 版本。不能用当前单日快照的收盘价伪造下一交易日开盘成交，也不能把未来成交回写到过去估值。正式多日组合运行需先完成 Snapshot series 或等价的执行数据集设计。

### 4.3 数据库级收口

除 Python 校验外，数据库还拒绝无审批人、费用与账户规则不一致、首笔现金未按账户初始现金初始化、重复 INIT、现金余额无法由前一账本余额加本笔流水解释、持仓批次数量增加或关闭事实被改写的记录。现金账本写入会锁定账户行，唯一部分索引保证每个组合只有一条 INIT，避免并发现金检查穿透。相同输入可幂等重放，冲突输入只能创建新组合版本。

决定日、计划执行日、成交日、现金账本交易日/可用日以及持仓开仓日/可卖日，均必须存在于
`trading_calendar` 的 SSE 开市日中；缺失交易日历时 fail-closed。该约束与 Backtest Dataset
的冻结交易日历保持一致，不能用自然日推断可成交日期。
