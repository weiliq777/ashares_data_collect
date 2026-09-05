# QualityTrendRotation_V1 运行时修复验证记录

本记录对应策略登记后的运行时与数据库绑定修复，不修改
`dev_doc/quality_trend_rotation_v1_validation.md` 中已经写入策略登记的批准证据。

## 代码门禁

```text
pytest -q
1153 passed, 1 skipped, 8 deselected

python tools/check_data_access_contract.py
scanned 209 files; violations: 0

python -m compileall -q data_collect tests
passed

git diff --check
passed
```

关键提交：

- `8ca4e6b`：冻结 Dataset 的有界加载、分红事件显式映射和财务质量兼容门禁；
- `d0461c1`：QualityTrendRotation_V1 默认跳空压力与冻结规则对齐；
- `f65c54e`：正式 Run 绑定 `strategy_id + strategy_version`，切断旧策略外键；
- `32d916f`：Run 状态通过命名 UPDATE 完成，禁止用部分行 upsert 绕过 NOT NULL/状态门禁。

## 真实小范围链路验证

Dataset：`f8760f3c-342c-49d5-9fcd-a989cc09955f`，状态 `FROZEN`。

评价窗口为 `2021-09-01`～`2022-01-28`，预热起点为 `2021-08-02`。运行记录：
`76f10f88-f663-41d2-9f6e-83c02f627ce6`，状态 `SUCCEEDED`。

```text
signals = 37
orders = 37
fills = 15
performance_daily = 100
```

该窗口只用于验证读取、候选、订单、撮合、费用、状态转换和审计落库，不能作为策略收益结论。

## 数据库结构验证

`sql/095_backtest_strategy_version_binding.sql` 已应用到生产库：

- 删除 `run_manifest_strategy_id_fkey` 对旧 `backtest.strategy_definition` 的绑定；
- 新增 `(strategy_id, strategy_version)` 到 `backtest.app_strategy_version` 的外键；
- 正式 `RUNNING/SUCCEEDED` Run 必须绑定 `APPROVED` 应用策略版本；
- 既有历史 Run 保留，`strategy_version` 允许为 NULL，未改写既有审计记录。

