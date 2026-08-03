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

## 验收标准

- Raw 业务键和哈希无重复。
- 任意历史交易日可还原股票池和可交易状态。
- 退市股票不从历史集合中消失。
- 财务公告日之后才进入 PIT。
- 价格、估值和财务 Feature 可重复计算。
- 复权因子缺失时标记 `MISSING_SOURCE`，不伪造数据。
- 下一阶段才能进入 Research、Backtest 和模拟组合。

## 当前进度

- 接口能力验证：已完成。
- SQL/任务代码：已完成第一版，已通过 Python 语法检查。
- `stock_st` 五年 Raw：已完成。
- `suspend_d` 五年 Raw：后台同步中。
- `stk_limit`、`namechange`、`dividend`、`index_daily`：等待前序历史任务完成后依次同步。
- 价格、估值、公司财务 Feature：已完成初步构建并完成抽样检查。
- 状态/PIT/分红 Feature：等待对应 Raw 完成后构建和验收。
