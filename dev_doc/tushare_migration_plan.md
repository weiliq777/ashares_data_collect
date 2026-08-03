# Tushare 主数据源改造执行计划

## 1. 目标

短期不依赖 QMT/MiniQMT，以 Tushare 兼容服务作为股票研究数据的主数据源，完成：

```text
Tushare 原始接口 -> 原始数据留存 -> 统一标准数据模型 -> 基本面/估值/趋势计算 -> 候选股报告与风险仓位计算
```

新闻、政策、监管、公告继续使用现有 OpenSearch 新闻体系；Tushare 负责股票基础信息、行情、估值和财务数据。

## 2. 已验证结论

2026-08-02 已验证以下接口全部成功：

| 接口 | 用途 | 结果 |
|---|---|---|
| `stock_basic` | 股票基础信息 | 5534条 |
| `trade_cal` | 交易日历 | 31条 |
| `daily` | 股票日线 | 21条 |
| `daily_basic` | PE/PB/市值/换手率 | 21条 |
| `income` | 利润表 | 成功 |
| `balancesheet` | 资产负债表 | 成功 |
| `cashflow` | 现金流量表 | 成功 |
| `fina_indicator` | ROE/负债率/财务指标 | 成功 |

验证脚本：`dev_doc/tushare_validation/validate_basic.py`。验证只证明接口可调用和返回结构可用，不代表所有历史范围、特色接口和批量额度均已确认。

## 3. 总体架构

```text
Tushare raw API
      ↓
tushare_*_raw 原始留存层
      ↓
daily_kline / valuation_daily / stock_financial_indicator / stock_basic_info
      ↓
基本面筛选 / 估值分位 / 趋势判断 / 候选池 / 仓位计算
```

现有 QMT 表和任务暂时保留，不删除、不与 Tushare 任务并行写入同一目标表。

## 4. 数据表策略

### 4.1 复用 `daily_kline`

```text
ts_code -> stock_code
trade_date -> trade_date
open/high/low/close -> 同名字段
vol -> volume（乘以100，从手转换为股）
amount -> amount（乘以1000，从千元转换为元）
```

主键继续为 `(stock_code, trade_date)`。

### 4.2 新增标准表

`stock_basic_info`：股票代码、名称、市场、行业、上市日期和状态。

`valuation_daily`：

```text
stock_code, trade_date, pe, pe_ttm, pb, ps, ps_ttm,
dv_ratio, turnover_rate, total_mv, circ_mv, source, fetched_at
```

`stock_financial_indicator`：

```text
stock_code, report_date, announce_date,
roe, roa, debt_to_assets, grossprofit_margin, netprofit_margin,
ocf_to_or, revenue_yoy, netprofit_yoy, eps, bps, source, fetched_at
```

### 4.3 原始表

```text
tushare_income_raw
tushare_balancesheet_raw
tushare_cashflow_raw
tushare_fina_indicator_raw
tushare_daily_basic_raw（建议保留）
```

原始表至少包含代码、报告/交易日期、抓取时间、来源和原始字段，便于审计和重放。

## 5. 接口与任务映射

| 任务 | 接口 | 目标 |
|---|---|---|
| 股票基础信息 | `stock_basic` | `stock_basic_info` |
| 交易日历 | `trade_cal` | 交易日判断/交易日表 |
| 日线行情 | `daily` | `daily_kline` |
| 复权因子 | `adj_factor` | 复权因子表 |
| 估值指标 | `daily_basic` | `valuation_daily` |
| 利润表 | `income` | 原始表 + 标准指标 |
| 资产负债表 | `balancesheet` | 原始表 + 标准指标 |
| 现金流量表 | `cashflow` | 原始表 + 标准指标 |
| 财务指标 | `fina_indicator` | 原始表 + 标准指标 |
| 分红 | `dividend` | 分红/复权分析 |

## 6. 日期和未来函数规则

必须同时保存：

- `report_date`：来自 `end_date`，表示报告期
- `announce_date`：来自 `ann_date`，表示市场实际获知日期
- `trade_date`：行情日期

历史筛选和回测只能使用：

```sql
announce_date <= trade_date
```

禁止仅用报告期判断数据是否已公开。财务修订记录不得覆盖历史版本。

## 7. 代码改造边界

新增：

```text
data_collect/providers/tushare_client.py
data_collect/providers/tushare_market.py
data_collect/providers/tushare_financial.py
data_collect/normalize/tushare_daily.py
data_collect/normalize/tushare_financial.py
data_collect/jobs/a_share_daily_tushare.py
data_collect/jobs/a_share_financial_tushare.py
data_collect/jobs/a_share_instrument_tushare.py
data_collect/jobs/a_share_valuation_tushare.py
```

客户端统一处理 Token、`https://teajoin.com`、限速、重试、超时、权限错误、批次进度和断点；不记录 Token 和完整响应。策略层只读取标准表，不直接调用 Tushare SDK。

## 8. 配置方案

```yaml
tushare:
  enabled: true
  api_url: "https://teajoin.com"
  token_env: "TUSHARE_API_KEY"
  request_interval: 0.25
  retries: 3
  batch_size: 100

data_backend:
  instrument: tushare
  daily: tushare
  valuation: tushare
  financial: tushare
  dividend: tushare
```

真实 Key 只通过环境变量提供，不写入配置、源码、日志或 Git。

## 9. 执行阶段

### 阶段一：客户端和表结构

- [ ] 增加 Tushare 依赖和客户端
- [ ] 增加限速、重试、超时、权限探测
- [ ] 建立标准表和原始表迁移 SQL

### 阶段二：基础信息、日线和估值

- [ ] 接入 `stock_basic`、`trade_cal`
- [ ] 接入 `daily`，写入 `daily_kline`
- [ ] 验证成交量和成交额单位
- [ ] 接入 `adj_factor`
- [ ] 接入 `daily_basic`，写入 `valuation_daily`

### 阶段三：财务数据

- [ ] 接入 `income`、`balancesheet`、`cashflow`
- [ ] 接入 `fina_indicator`
- [ ] 保存原始数据
- [ ] 生成统一财务指标
- [ ] 校验 `ann_date` 和 `end_date`

### 阶段四：调度和质量检查

- [ ] 增加 Tushare 日线管线
- [ ] 增加 Tushare 财务管线
- [ ] 支持失败重试和断点续传
- [ ] 检查重复、空值、异常单位和交易日完整性
- [ ] 与 AkShare/公告数据抽样交叉验证

### 阶段五：策略层

- [ ] ROE连续多年筛选
- [ ] 资产负债率和经营现金流筛选
- [ ] PE/PB历史分位
- [ ] MA20/MA60和成交量信号
- [ ] 候选股票报告
- [ ] 2%风险仓位计算
- [ ] 历史回测和模拟交易

## 10. 必须验收的风险点

1. 逐项确认 `daily_basic`、`fina_indicator`、`pro_bar` 等实际权限。
2. 确认代理服务对批量、分页和历史日期的限制。
3. 确认 `daily.vol`、`daily.amount` 单位并测试转换。
4. 确认 ROE 等指标是百分数还是小数。
5. `ann_date` 缺失时不得用 `end_date`冒充公告日。
6. 明确 ST、退市、北交所和停牌股票筛选范围。
7. 复权价格只用于技术分析，不覆盖原始交易价格。
8. 同一目标表禁止 Tushare 和 QMT 任务并行写入。

## 11. 验收标准

- 不启动QMT即可完成股票基础信息、日线、估值和财务采集。
- 重复执行同一天任务不会重复写入。
- 中断后可从上次批次继续。
- 日线单位和财务日期经过测试确认。
- 历史筛选不会使用公告日之后的数据。
- 策略层只依赖统一标准表。
- 权限不足时明确报告失败接口，不产生静默空数据。

## 12. 当前实现状态修订（2026-08-03）

本文件前面的阶段清单是迁移初始计划；以本节和 `dev_doc/development_plan_3_phases.md` 为当前执行基线。

已落地：

```text
历史行情/估值/复权因子/交易日历 Raw
四张财务 Raw
tushare_market_raw_version
tushare_financial_raw_version 表结构
daily_kline / valuation_daily / adj_factor_daily
financial_income_standard
financial_balance_sheet_standard
financial_cash_flow_standard
Raw-first 历史转换和每日增量代码
```

当前仍在验收：

```text
adj_factor 独立失败重试
每日增量重复运行与修正版本验证
全量数据质量报告
删除 Standard 后从 Raw 完整重建
财务版本回填和三张财务 Standard 全量转换
```

每日行情增量默认回补最近 5 个交易日；`daily`、`daily_basic` 和 `adj_factor` 均先写 Raw/版本层，再按交易日重建 Standard。任何 Tushare Raw 表不得被更新、删除或覆盖。
- OpenSearch 新闻系统继续独立运行。

## 12. 最终流程

```text
新闻/公告(OpenSearch) + 行情/估值/财务(PostgreSQL)
        ↓
基本面过滤
        ↓
公告与新闻交叉验证
        ↓
估值分位
        ↓
日线趋势确认
        ↓
候选股清单
        ↓
止损与仓位计算
        ↓
人工复核、模拟交易、再考虑实盘
```

系统输出研究结论和风险参数，不承诺盈利，也不直接替代用户的投资决策。
