# Research 快照血缘补强记录

更新时间：2026-08-05

## 规则

- `research.snapshot_lineage` 每个股票必须有 8 个字段组：`universe`、`tradeability`、`price`、`valuation`、`financial`、`shareholder_return`、`lifecycle`、`instrument`。
- `price`、`valuation`、`universe`、`tradeability`、`shareholder_return` 的来源记录来自实际使用的 PIT/Feature v1 行，哈希只覆盖快照读取的字段；`source_record_version=1` 表示 v1 派生记录。
- `financial` 使用 PIT 财务版本的真实 `record_version` 和 `payload_hash`。
- `lifecycle` 使用 `std.security_lifecycle` 的真实 Raw `record_id` 和 `source_payload_hash`；该字段由 `sql/025_research_lineage_sources.sql` 增加并由生命周期重建任务填充。
- `instrument` 使用 `tushare_stock_basic_raw` 的 payload SHA-256。
- 上游确实没有记录时使用 `:missing` 占位；占位不伪造版本或哈希，快照质量状态负责暴露缺失。

## 验收

`research_snapshot.validate` 除了检查 8 个字段组，还检查所有非 `:missing` 的血缘记录必须同时具有 `source_record_version` 和 `source_payload_hash`。快照构建仍使用 `REPEATABLE READ`，验证失败必须重新构建。

测试口径：当前新增血缘回归测试后，`tests/test_research_snapshot.py` 单独 6 个测试；与 `tests/test_phase2_business_rules.py` 合并运行共 12 个测试。正式 FROZEN 快照应使用干净 Git commit，dirty 工作区只保留为补救审计信息。
