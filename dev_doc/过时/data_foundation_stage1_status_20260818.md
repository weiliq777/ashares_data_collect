# Data Foundation Stage 1 落地状态（2026-08-18）

本文件是 `dev_doc/数据基座架构拆分方案.md` 的执行记录，不改变该方案的架构契约。

## 本批已完成

- 新增幂等迁移 `sql/049_data_foundation_stage1_contracts.sql`：
  - `contract.contract_version`
  - `contract.data_dictionary`
  - `backtest.app_strategy_version`
  - `backtest.app_execution_params`
  - `ops.trading_day_build`
- 契约元数据和应用参数表均为追加式；数据库触发器拒绝 UPDATE/DELETE。
- `app_execution_params` 从既有 `backtest.market_rule_effective` 拆出佣金、最低佣金、滑点、成交量参与率等执行假设；印花税/过户费没有复制到应用参数表。
- 新增 `FoundationContractRepository`，所有访问经过 Repository → Kernel → Gateway；没有新增 `execute()`、连接或游标暴露。
- DatasetPolicy 已为新增对象显式登记，未依赖宽泛通配策略绕过约束。
- 单元/静态测试和全量回归通过：`879 passed, 1 skipped, 8 deselected`；契约扫描 138 个文件、0 违规。

## 尚未宣称完成

- 现有 `public.tushare_*_raw_version` 尚未物理迁移到 Raw schema。需要双读/双写、校验、回滚和切换窗口，不能用一次性 `ALTER TABLE ... SET SCHEMA` 冒险替代。
- 现有 `research.snapshot_*` 尚未迁入 `contract.*`；旧快照数据没有被复制、覆盖或删除。
- 数据字典目前是首批核心字段基线，不代表已经覆盖所有对外 Feature/Mart 字段。
- `mart.v_price_feature`、wave3 指标下沉、engine 参数读取和 Snapshot 去策略化属于 Stage 2，尚未完成。
- 049 迁移尚未在生产库执行：当前 Docker Desktop 的 `docker-desktop` WSL engine 处于停止/不可用状态。恢复 PostgreSQL 后必须由管理员执行迁移，再用 `ashares_app` 验证读取、追加写入和不可修改约束。

## 恢复数据库后的验收顺序

1. 管理员执行 049，确认只新增对象和数据字典基线，既有表行数/哈希不变。
2. 使用 `ashares_app` 验证：可读 contract；可写 `ops.trading_day_build`；不能 UPDATE/DELETE contract 或应用参数；FROZEN/追加式触发器生效。
3. 运行全量测试、契约扫描和一次只写状态的交易日流水线试运行。
4. 只有以上验收通过，才推进 Raw/contract snapshot 的双写迁移设计。
