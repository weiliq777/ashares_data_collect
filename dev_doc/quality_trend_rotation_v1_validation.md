# QualityTrendRotation_V1（质量趋势轮动 V1）代码验收记录

验收日期：2026-09-05。

本记录是首次登记 `APPROVED`（已批准）策略版本时使用的代码证据。证据文件的
SHA-256 作为 `approval_evidence_hash` 写入不可变策略登记，不代表投资收益承诺。

## 验收范围

- 策略、交易场景和评价身份相互独立并生成稳定哈希；
- 周频信号、Top 3 买入、Top 10 排名滞回和 MA60 硬退出；
- 上市 250 个有效交易日、PIT 财务可见性和缺失值 fail-closed；
- 1 万元目标市值仓位、100 股整手、T+1 开盘和历史费用规则；
- 停牌、跌停或不可卖时的每日卖出重试；
- 有界日期分页加载，避免小样本回测载入五年全量数据；
- 评价区间覆盖、价格指数降级标识、CAGR 和 Sharpe 口径；
- Repository → Kernel 数据访问边界。

## 验收结果

```text
pytest -q
1147 passed, 1 skipped, 8 deselected

python tools/check_data_access_contract.py
scanned 209 files; violations: 0

python -m compileall -q data_collect
passed

git diff --check
passed
```

正式五年回测前仍必须先执行小范围真实回测，核验候选数量、拒单原因、成交、费用、
持仓和净值路径；小范围运行结果属于数据行为验收，不替代本代码验收记录。
