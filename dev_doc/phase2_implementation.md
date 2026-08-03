# 阶段二实施记录：状态、PIT 与 Feature

更新时间：2026-08-04

## 目标

阶段二不直接生成买卖建议，目标是还原历史某个交易日当时可知、可研究、可交易的信息，并生成可复算特征：

```text
Raw/Standard → 生命周期/状态 → PIT → Feature → Research/Backtest
```

## 已验证接口

已用真实 Tushare 请求验证：

- `stock_st(trade_date=...)`：历史 ST 列表
- `suspend_d(trade_date=...)`：停复牌事件
- `stk_limit(trade_date=...)`：涨跌停价格
- `namechange(ts_code=...)`：历史名称区间
- `dividend(ts_code=...)`：分红送转事件
- `index_daily(ts_code=..., start_date=..., end_date=...)`：指数日线

## Raw 表和同步任务

迁移文件：`sql/018_create_phase2_model.sql`。

同步任务：`data_collect/jobs/tushare_phase2_raw_init.py`。

| 接口 | Raw 表 | 历史拉取粒度 | checkpoint |
|---|---|---|---|
| `stock_st` | `tushare_stock_st_raw` | 交易日 | 日期 |
| `suspend_d` | `tushare_suspend_d_raw` | 交易日 | 日期 |
| `stk_limit` | `tushare_stk_limit_raw` | 交易日 | 日期 |
| `namechange` | `tushare_namechange_raw` | 股票 | `ts_code` |
| `dividend` | `tushare_dividend_raw` | 股票 | `ts_code` |
| `index_daily` | `tushare_index_daily_raw` | 指数 | `ts_code` |

所有 Raw 保存完整 `payload`、`payload_hash`、`batch_id`、来源和抓取时间；重复请求不会重复插入，也不会修改历史 Raw。

说明：`stk_limit` 接口单次最多 5,800 行，而单个交易日全市场通常约 5,400 行，因此历史同步必须按交易日请求；不能用多年区间请求替代，否则可能因接口上限截断结果。

## Standard、PIT 和 Feature

Standard/PIT 任务：`data_collect/jobs/tushare_phase2_standard.py`。

- `std.security_lifecycle`：上市、退市、名称有效区间。
- `std.security_status_daily`：ST、停牌、涨跌停和日线状态。
- `std.dividend_event`：分红送转事件标准化。
- `std.index_daily_tushare`：指数日线标准化。
- `pit.universe_daily`：历史研究股票池。
- `pit.security_tradeability_daily`：当日可交易、可买、可卖状态。
- `pit.financial_asof`：公告日不晚于决策日的财务快照。

Feature 任务：`data_collect/jobs/tushare_phase2_features.py`。

- `feature.price_daily_v1`：原始价格、复权基准价、收益、MA20/60/120、波动率、量能。
- `feature.valuation_daily_v1`：PE/PB 3年和5年滚动有效样本分位。
- `feature.company_financial_v1`：ROE、负债率、成长、现金流质量。
- `feature.shareholder_return_v1`：现金分红和分红稳定性。
- `ops.feature_registry`：特征版本、粒度、窗口和来源注册。

规则：

1. 原始价格用于成交和涨跌停判断；复权基准价用于趋势和收益特征。
2. 财务使用 `announce_date <= decision_date`，不能按 `end_date` 倒灌。
3. 负 PE 不参与正常 PE 分位；样本不足输出 NULL/质量标记。
4. 非有限浮点值 `NaN` 在 Feature 层转为 NULL，并标记 `WARNING`。
5. Raw 不允许删除、更新、覆盖；派生表允许重建。
6. 分红 JSON 中的 `pay_date`、`div_listdate`、`imp_ann_date` 按 Tushare 的 `YYYYMMDD` 解析为 Standard 日期；原始 JSON 保持不变。
7. 日常回补模式必须重新请求最近交易日，即使 checkpoint 已完成；Raw 通过 `payload_hash` 幂等追加，用于接收收盘后补发或修订数据。
8. 趋势均线使用有复权因子的 `adj_close_base`；复权因子缺失时复权价格和相关收益/均线不伪造为原始收盘价。
9. 收盘价为空、零或负数的记录保留为事实，但不得判定为可交易；财务特征关联利润表和现金流量表时不得使用指标公告日之后的修订记录。

## 后台运行命令

历史状态源逐个运行，避免并发触发 Tushare 流控：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_raw_init --dataset stock_st --start-date 20210802 --end-date 20260803 --pause 0.5
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_raw_init --dataset suspend_d --start-date 20210802 --end-date 20260803 --pause 0.5
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_raw_init --dataset stk_limit --start-date 20210802 --end-date 20260803 --pause 0.5
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_raw_init --dataset namechange --start-date 20210802 --end-date 20260803 --pause 0.5
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_raw_init --dataset dividend --start-date 20210802 --end-date 20260803 --pause 0.5
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_raw_init --dataset index_daily --start-date 20210802 --end-date 20260803 --pause 0.5
```

后台启动时必须先检查 `logs/phase2_*` 的启动日志；失败通过 checkpoint 续传，不得全量重跑。

如果历史接口对交易日返回空响应，不直接认定为“完整无数据”。可以使用补采模式只重试 `empty` checkpoint：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.tushare_phase2_raw_init --dataset stk_limit --start-date 20210802 --end-date 20260803 --pause 1.0 --retry-empty
```

`tools/phase2_retry_empty.ps1` 会在 Raw 顺序任务结束后依次补采 `stock_st`、`suspend_d`、`stk_limit` 的空日期；补采完成后由 `tools/phase2_rebuild_after_retry.ps1` 再次重建 Standard、PIT 和 Feature。

## 验收标准

- Raw 业务键和哈希无重复。
- 任意历史交易日可还原股票池和可交易状态。
- 退市股票不从历史集合中消失。
- 财务公告日之后才进入 PIT。
- 价格、估值和财务 Feature 可重复计算。
- 复权因子缺失时标记 `MISSING_SOURCE`，不伪造数据。
- 质量脚本额外检查财务 PIT 未来穿越、可买/可卖 NULL、非正价格可交易和阶段二失败 checkpoint。
- 下一阶段才能进入 Research、Backtest 和模拟组合。

## 当前进度（2026-08-04）

- 接口能力验证、SQL/任务代码和 Python 语法检查：已完成。
- `stock_st`、`suspend_d`、`stk_limit` 五年 Raw：主同步已完成；其中少量 `empty` 交易日由后续补采链处理，不能直接视为无数据。
- `namechange`：后台串行同步中，完成后继续 `dividend`、`index_daily`；所有事件接口按股票 checkpoint 续传。
- 后台编排：`tools/phase2_resume_and_finish.ps1`，按 Raw → 空响应补采 → Standard/PIT → Feature 顺序执行，Tushare 请求不并发。
- 价格、估值、公司财务 Feature：已有初步构建和抽样检查；Raw 全部完成后必须再次全量重建。
- 状态/PIT/分红/指数 Feature：等待本轮 Raw 和空响应补采结束后统一重建和验收。
- 阶段二当前仍未验收完成；必须通过 Raw 覆盖、重复键、未来函数、单位、可交易性和幂等检查后，才能进入 Research/Backtest。
