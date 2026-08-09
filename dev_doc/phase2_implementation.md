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
- `feature.company_financial_v1`：ROE、负债率、成长、现金流质量；当前营收成长字段尚未接入。
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

## 本次改造核心（2026-08-04）

- 新增六类阶段二 Raw 表及 `sql/018_create_phase2_model.sql`：ST、停牌、涨跌停、名称变更、分红送转和指数日线；所有原始响应保留 `payload`、哈希、批次和抓取时间。
- 新增 `tushare_phase2_raw_init.py`：历史初始化阶段的日期类接口按交易日、事件类接口按股票、指数接口按指数代码分别建立 checkpoint；支持断点续传、哈希幂等和 `empty` 响应补采。历史初始化完成后不再作为日常任务重复运行。
- 新增 `tushare_phase2_standard.py`：从 Raw 重建生命周期、每日状态、分红事件、指数标准表以及 `universe_daily`、`security_tradeability_daily`、`financial_asof`。
- 新增 `tushare_phase2_features.py`：计算价格趋势、估值分位、公司财务和股东回报 Feature v1，并登记到 `ops.feature_registry`。
- 新增阶段二 checkpoint 完整性和未来函数检查；`tools/phase2_resume_and_finish.ps1` 负责 Raw → 补采 → Standard/PIT → Feature 的串行编排，避免并发触发 Tushare 流控；`tools/phase2_quality_after_finish.ps1` 在编排完成后启动只读质量审计。
- 以上任务只允许重建派生表，禁止修改、删除或覆盖任何 Raw；缺失复权因子、异常价格和缺失源数据必须保留并标记质量状态。

## 验收标准

- Raw 业务键和哈希无重复。
- 任意历史交易日可还原股票池和可交易状态。
- 退市股票不从历史集合中消失。
- 财务公告日之后才进入 PIT。
- 价格、估值和财务 Feature 可重复计算。
- 复权因子缺失时标记 `MISSING_SOURCE`，不伪造数据。
- 质量脚本额外检查财务 PIT 未来穿越、可买/可卖 NULL、非正价格可交易和阶段二失败 checkpoint。
- 下一阶段才能进入 Research、Backtest 和模拟组合。

## 基线冻结与日常增量（2026-08-04）

历史 Raw/Standard 验收后建立 `tushare_data_release` 发布点，例如
`baseline_20260731`。只有 `status='validated'` 的发布点才能作为默认增量起点；
日常任务只处理基线之后的日期，不重跑冻结历史。

正式行情增量使用 `data_collect.jobs.tushare_market_incremental_by_date`：
`daily`、`daily_basic`、`adj_factor` 均按 `trade_date` 请求全市场。单次返回达到
Tushare 上限时，任务自动按股票代码分片，并且只有分片后未达到上限才写入完成
checkpoint，避免静默截断。

阶段二状态/事件增量使用
`data_collect.jobs.tushare_phase2_incremental`：

- `stock_st`、`suspend_d`、`stk_limit` 按交易日请求；
- `dividend` 按最近公告日和实施公告日窗口请求，不再每天遍历全部股票；
- `namechange` 按低频日期窗口请求，若接口返回能力不足再使用历史股票级任务复查；
- `index_daily` 按指数代码和最近日期区间请求。

增量任务允许回补最近 5 个交易日和最近 30 个自然日。Raw 继续通过完整 payload、
payload_hash 和批次号追加保存；Standard/PIT 只允许重建，不得修改或删除 Raw。
覆盖率 PASS 只代表接口响应达到历史基准；Raw 写入、Standard/checkpoint 或全局派生
重建失败时，任务会追加最新 `BLOCKED` 门禁，防止旧 PASS 让 Research freeze 放行。

当前实现：行情增量完成后自动重建状态/PIT、价格和估值 Feature；分红增量完成后自动重建分红 Standard 和股东回报 Feature；财务增量完成后自动重建三张财务 Standard、指标 Standard、财务 PIT 和公司财务 Feature。财务报表按公告日期窗口增量；`fina_indicator` 只能按股票查询，单次最多 100 条，按季度报告期窗口拆分并使用 checkpoint、重试，达到上限时拒绝把截断结果标记为完成。`namechange` 的开放 `end_date` 通过下一条 `start_date - 1 天` 归并；`dividend` 区分预案/实施，按业务键保留最新版本，只有实施且 `ex_date <= decision_date` 的事件进入回报 Feature。发布命令会在替换 validated 基线前执行质量门禁。
`data_collect.jobs.tushare_release` 负责创建、验收和查询历史基线标签。

## 当前进度（2026-08-04）

- 接口能力验证、SQL/任务代码和 Python 语法检查：已完成。
- `stock_st`、`suspend_d`、`stk_limit` 五年 Raw：主同步已完成；其中少量 `empty` 交易日由后续补采链处理，不能直接视为无数据。
- `namechange`：已完成股票 checkpoint 同步；少量空响应保留为可审计状态。
- `dividend`：历史初始化按股票 checkpoint 完成；日常增量改由 `tushare_phase2_incremental.py` 按公告日期/实施公告日期窗口查询。
- `index_daily`：四个基准指数 Raw/Standard 已同步完成。
- 后台编排：`tools/phase2_resume_and_finish.ps1` 仅用于历史补齐和阶段性派生表重建；日常增量使用 `tushare_market_incremental_by_date.py` 和 `tushare_phase2_incremental.py`，不再启动历史全量编排。
- 价格、估值、公司财务、股东回报 Feature 已完成全量重建。
- 状态/PIT/分红/指数 Feature 已完成全量重建和质量验收。
- 2026-08-04 阶段二持续运行闭环验收：发现并修复 216 只股票的历史复权因子缺口；财务报表版本链路、增量派生重建、生命周期非重叠区间、分红业务键选择和发布质量门禁已接通。当前检查中未来函数、版本血缘缺失、阶段二 checkpoint 缺失/失败、生命周期重叠和分红业务键重复均为 0；`baseline_20260803` 继续作为已验证基线，`baseline_20260731` 保留为 superseded 审计记录。31 条上游实施事件缺少 `ex_date`，仅作为 Standard WARNING，不进入股东回报 Feature。
- 2026-08-05 小范围真实财务增量验收：使用 3 只股票真实调用 `income`、`balancesheet`、`cashflow`、`fina_indicator`；前三者记录合法 empty checkpoint，`fina_indicator` 新增 5 条 Raw/版本记录，并自动完成财务 Standard/PIT/Feature 重建。质量门禁通过，四类财务增量均有运行证据。

## 财务指标版本链路（2026-08-04）

`fina_indicator` 不再通过旧的 `stock_financial_indicator` 进入 PIT/Feature，正式链路为：

```text
tushare_fina_indicator_raw
→ fina_indicator_raw_version
→ fina_indicator_standard
→ pit.financial_asof
→ feature.company_financial_v1
```

`or_yoy` 作为营业收入同比，`tr_yoy` 作为营业总收入同比，`q_sales_yoy` 作为单季度营业收入同比；三者不能混用。版本层和 Standard 必须保留 `ann_date`、可用时的 `f_ann_date`、业务版本字段、`payload_hash` 和抓取批次。`effective_announce_date` 使用 `COALESCE(f_ann_date, ann_date)`，不得伪造来源未提供的 `f_ann_date`。

旧 `stock_financial_indicator` 暂作为只读兼容表保留。新旧 PIT 结果不要求逐行完全相同，但所有差异必须能由公告日期、版本和指标口径解释并审计。财务链路验收完成前，不进入正式 Research/Backtest；验收通过后以实际完整数据日 `2026-08-03` 创建 `baseline_20260803`，并保留 `baseline_20260731`。
## 2026-08-07 当前状态覆盖

阶段二 Raw → Standard → PIT → Feature 主链路已完成历史基线和日增量实测。行情、估值、复权因子、交易日历及阶段二派生层当前最大日期为 2026-08-06；Raw failed checkpoint 当前为 0，PIT/Research 截面校验通过。

财务链路仍按公告可见性运行：财务 Feature 最大日期不要求与行情日期一致；`fina_indicator` 的 `or_yoy → revenue_yoy` 映射和版本血缘已接入。所有增量先写 Raw/Raw version，再重建受影响 Standard/PIT/Feature，禁止回写 Raw。
