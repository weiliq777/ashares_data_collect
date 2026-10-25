# P2-C｜生产迁移 116 → 117 就绪验收

- 项目：`ashares_data_collect`
- 核查日期：2026-10-20
- 生产数据范围：仅只读核查；未执行生产 DDL、DML 或 Release
- 隔离验证：`ashares_test`，环境 ID `f18d4fc4-a362-41ba-b188-3c300705f29f`
- 结论：**生产部署就绪状态 BLOCKED**。116/117 的 SQL 与隔离恢复已验证，但生产备份/恢复证据、磁盘余量和获准的迁移身份尚未确认。当前不能据此执行生产迁移。

## 结论摘要

1. 生产数据库当前确认为 `ashares`，连接角色 `ashares_app`，事务只读，非超级用户。116、117 对应的新对象均不存在；`research.stock_snapshot_daily` 仍是分区父表，26 个子分区，合计 `105,421,078,528` bytes（PostgreSQL 显示约 98 GB，估算约 34.5 百万行）。
2. 迁移顺序必须是 116 后 117。116 在分区父表添加 11 个可空且无默认值的字段，并创建 D/A Feature 表；117 依赖该表，再添加 `source_projection_version`，并创建三张追加式 PIT 投影表。
3. 隔离库按 116 → 117 完成迁移、重复运行和中断后前向恢复。D/A 发布所需的应用权限已验证；PIT 投影 Run/Batch/Row 表拒绝应用账号的 UPDATE、DELETE、TRUNCATE，且完整性读取会拒绝不完整投影。
4. `research.stock_snapshot_daily` 上没有观测到其他会话持有的锁或长事务。该结果只是核查时点的快照，维护窗口仍须重新检查。
5. 生产 `archive_mode=off`。这不能证明没有外部备份，但本次无法验证备份时间、可恢复点、磁盘可用空间或最近一次恢复演练；这是部署就绪的实际阻断。
6. `600699.SH` 公司行为仍为 `UNRESOLVED`，继续阻断正式 Foundation Release 和后续正式 FROZEN 发布；它不阻止单独部署 116/117 的 Schema。

## 验收状态

| 检查项 | 状态 | 证据与限制 |
|---|---|---|
| 生产连接身份及只读边界 | PASS | `ashares / ashares_app / transaction_read_only=on / superuser=false / PostgreSQL 16.14`。本轮全部生产查询经只读审计连接。 |
| 生产 116/117 对象状态 | PASS | D/A Feature 表、Snapshot D/A 列、三张 PIT Projection 表均不存在；确认生产尚未部署。 |
| 生产迁移文件哈希账本 | BLOCKED | 生产只找到 `ops.foundation_table_migration`，它不是 SQL 文件名与内容哈希账本；无法在生产侧核对已执行 SQL 文件历史。 |
| 生产迁移权限身份 | BLOCKED | `ashares_app` 没有数据库或 `research`/`feature`/`pit` Schema 的 CREATE 权限；Schema owner 为 `ashares`。本次没有验证可供正式部署的专用迁移账号。 |
| 生产表锁与活动事务 | PASS（时点快照） | 目标父表及 26 个分区未观察到其他会话持锁；排除审计连接后无其他活动会话或打开事务。部署前必须重查。 |
| 生产磁盘剩余空间 | NOT RUN | 数据库角色无法读取 PostgreSQL 主机文件系统容量；本轮未取得主机侧指标。 |
| 生产备份及恢复演练 | BLOCKED | `archive_mode=off`、`wal_level=replica`；未取得外部快照/备份证明，也未验证恢复。 |
| 116 → 117 迁移依赖与静态契约 | PASS | 117 对 D/A 表添加列，必须先执行 116；116/117 本地 SHA-256 见下文。 |
| 116 → 117 隔离部署、重跑和恢复 | PASS（隔离） | 新鲜隔离库 120 个项目迁移均成功；116/117 顺序、重复应用及两处模拟中断后的前向恢复均通过。 |
| 应用账号 D/A Feature 读写权限 | PASS（隔离） | 查询、插入、更新及范围删除在单事务内成功并回滚；TRUNCATE 被拒绝。范围删除是 Feature `SCOPED_MERGE` 的组成部分，不能撤销 DELETE。 |
| 应用账号 PIT 投影权限 | PASS（隔离） | Run/Batch/Projection 仅授予 SELECT/INSERT；UPDATE、DELETE、TRUNCATE 权限均为 false；触发器对行变更也拒绝。 |
| 完整投影读取与不完整拒绝 | PASS（隔离） | P2-B 完整版本 20,469 行、42 批次通过 `assert_complete`；测试环境中的缺行版本被拒绝。 |
| 旧 Standard/PIT/FROZEN 不变 | PASS（隔离） | 116/117 重跑前后 Standard/PIT 内容哈希一致；测试快照 1,660 行的冻结内容哈希未变。生产数据未写入。 |
| Data Access Contract / Paper Trading Boundary | PASS | 数据访问扫描 259 个文件、0 violations；交易边界扫描 320 个 Python 文件、未发现 QMT 交易 API。 |
| Ruff | NOT RUN | 当前 `uv` 环境没有 `ruff` 可执行文件；本轮不安装新依赖。 |
| 正式 Foundation Release / 五年 Dataset | BLOCKED / NOT RUN | 未创建 Release、Snapshot 或 Dataset；`600699.SH` 未决状态保持不变。 |

## 生产只读核查结果

### 数据库和对象

- 数据库大小：`462,967,962,647` bytes（约 431 GiB）。
- `research.stock_snapshot_daily` 是分区父表，拥有 26 个分区；子分区总大小 `105,421,078,528` bytes，估算行数约 `34,546,572`。
- 父表现有非内部触发器 5 个；没有 D/A Snapshot 列。
- `feature.debt_to_assets_v1` 和 `pit.financial_standard_projection_run`、`pit.financial_standard_projection_batch`、`pit.financial_standard_projection` 均不存在。
- 排除本次只读审计会话后，目标关系无活动锁；数据库没有其他活动会话或未结束事务。
- 生产 `ashares_app` 没有数据库 CREATE 权限，也没有 `research`、`feature`、`pit` Schema 的 CREATE 权限，符合应用账号不承担迁移职责的边界。

生产数据库可访问的迁移相关表只有 `ops.foundation_table_migration`。它记录 Foundation 表迁移状态，不记录本次 SQL 文件的 SHA-256。部署前应在既有 DBA 变更单或受控发布记录中记录数据库身份、116/117 文件名和哈希、执行人、开始/结束时间及结果；不应把 `ashares_app` 提权为迁移账号。

### 116/117 SQL 契约

| 迁移 | 行为 | 锁/数据影响判断 |
|---|---|---|
| `116_independent_debt_to_assets_factor.sql` | 建立 `feature.debt_to_assets_v1` 和索引；向分区表增加 11 个可空、无默认值字段；更新 Feature Registry；授予应用 Feature 写入权限。 | PostgreSQL 对 `ALTER TABLE` 默认使用 `ACCESS EXCLUSIVE` 锁。新增可空且无默认值的字段不需要重写历史行；但分区父表较大且只有短元数据操作，并不消除等待强锁和阻塞查询的风险。 |
| `117_financial_standard_pit_projection.sql` | 建立 Run、Batch、Projection 三张带主键/外键/唯一键/检查约束的表及索引；创建不可变触发器；限制应用账号只读和追加；在 D/A 表增加投影版本列。 | 117 依赖 116 创建的 D/A 表。新 PIT 表的行变更由触发器阻止，应用 ACL 也撤销 UPDATE/DELETE/TRUNCATE。新增 D/A 列可空且无默认值。 |

迁移依赖的文件身份：

| 文件 | SHA-256 |
|---|---|
| `116_independent_debt_to_assets_factor.sql` | `13effca380f96eec94000e07ad080b018a7af8ca3f8e32ca28d5a66705ffe421` |
| `117_financial_standard_pit_projection.sql` | `e8662de091104cdabee2ec886d5d7d2389d3f6eb80c0d22869bbac958c0f162d` |

**权限审查澄清：** 迁移 116 执行后，`ashares_app` 对 D/A Feature 表具有 DELETE。该权限来自 Feature Schema 的既有默认 ACL；单看迁移 116 的显式 GRANT 容易忽略它。源码中 `FinancialRepository.publish_debt_to_assets_partition()` 使用 `RebuildMode.SCOPED_MERGE`，而 `RebuildGateway` 会先对指定代码和日期范围执行 DELETE，再插入 staging 数据。因此范围删除是当前重算流程需要的权限。隔离环境验证了该范围操作可在事务内完成；撤销 DELETE 会破坏 Feature 发布，本次没有保留任何撤权迁移，也没有改变 116/117 文件。

迁移 117 的 PIT 投影写入通过 Repository 校验输入哈希，并在写入后回读比较预期身份与已存哈希；同一身份内容不同会报错。Feature Reader 必须绑定明确的 `projection_version` 与 manifest hash，并先检查投影完整性；不能把数据库中“最新一行”当作版本选择。

## 隔离部署演练

隔离环境：容器 `ashares-p2c-migration-readiness-postgres`，数据库 `ashares_test`，环境 ID `f18d4fc4-a362-41ba-b188-3c300705f29f`。环境中保留已有合成 Snapshot 与 Alpha E2E 证据，没有复制或写入生产数据。

- 从空白测试实例执行完整迁移链：120 个 SQL 文件成功，耗时约 192 秒；迁移 116、117 分别为账本顺序 119、120，哈希与当前文件一致。
- 对 116、117 进行重跑：迁移状态无漂移，新增迁移数为 0。
- 中断恢复演练分别在 116 已创建表但未记账、117 已创建投影表但未添加 D/A 列时模拟失败；标准迁移按 `IF NOT EXISTS`/可重复触发器定义重跑后恢复成功。旧数据内容哈希不变。
- `ashares_app` 在临时事务中执行 D/A Feature INSERT、UPDATE、SELECT 和范围 DELETE 后回滚，事务外确认探针行数为 0；TRUNCATE 仍被拒绝。
- PIT 投影三表的应用权限为 SELECT/INSERT，UPDATE/DELETE/TRUNCATE 均拒绝；Superuser/表 owner 的 UPDATE/DELETE 也由不可变触发器拒绝。
- `FinancialProjectionRepository.assert_complete()` 对缺失批次/行的版本返回“不完整”错误。该测试环境保留一个 `p2c-incomplete-probe-v1` 的失败 Run 标记（expected 1、0 批次、0 行），用于留存拒绝证据；不要把此 `ashares_test` 误称为空白数据库。
- 项目迁移脚本原来在 Windows PowerShell 的 Docker Go-template 参数解析上误判容器标签。已将标签读取改为解析 `docker inspect` JSON；用 `pwsh` 验证 `status`、迁移和测试均能通过身份保护。Windows PowerShell 5.1 仍不兼容脚本中现有的 `ConvertFrom-Json -AsHashtable`，执行脚本应使用 PowerShell 7 `pwsh`。

### 测试结果

| 检查 | 结果 |
|---|---|
| `tests/test_integration_test_database.py` | PASS，7 项 |
| `tests/test_integration_alpha_research_dataset.py` | PASS，1 项；31.04 秒 |
| `tests/test_financial_pit_projection.py` | PASS，8 项 |
| `tools/check_data_access_contract.py` | PASS，259 个文件，0 violations |
| `tools/check_paper_trading_boundary.py` | PASS，320 个可执行 Python 文件，无 QMT 交易 API |
| `tools/integration_test_db.ps1 -Action migrate` 重跑 | PASS，0 个新增迁移；测试账本 120 项 |
| Ruff | NOT RUN，当前环境未安装/未暴露 `ruff` 命令 |

## 生产执行前必须补齐的条件

1. DBA 提供近期备份或快照的标识、时间与恢复点，并证明恢复路径可用；不能以 `archive_mode=off` 状态下的数据库连接测试替代恢复证据。
2. 由数据库主机/平台负责人提供数据库盘可用空间与增长水位。116/117 是可空列和新表的元数据部署，不应触发 `stock_snapshot_daily` 全表重写，但当前无法核实磁盘余量。
3. 明确获准的迁移身份及审批记录。应用账号没有 DDL 权限；部署不得使用日常应用连接。
4. 建立外部 SQL 文件/哈希执行记录；生产自身没有发现与 116/117 对应的 SQL 文件迁移账本。
5. 安排维护窗口，暂停可能持有长事务或使用 Snapshot 父表的任务；执行前再次检查父表及全部子分区锁、长事务和活动查询。
6. 固定执行版本与哈希，严格按 116 → 117 顺序执行。两份 SQL 均为事务性 DDL，没有 `CREATE INDEX CONCURRENTLY`；推荐每个迁移文件独立事务运行，并限制等待和执行时间。

## 获批后的执行清单（本轮未执行）

### 变更前

- 再次确认目标库名、主机、迁移角色及事务状态；迁移连接身份必须有已批准的 DDL 权限。
- 记录数据库备份/恢复点、主机可用空间、迁移文件 SHA-256 和当前 Schema 目录清单。
- 重查 `research.stock_snapshot_daily` 与全部分区的锁和长事务；若有等待中的 AccessExclusive 竞争、业务长事务或不确定锁，立即中止并改期。
- 推荐每个文件单独使用事务并设置 `lock_timeout=2s`、`statement_timeout=5min`；超时即回滚当前文件，不通过延长等待挤压业务查询。

### 顺序和验证

1. 先执行 116，事务提交后只读验证 D/A 表、索引、11 个 Snapshot 列、Feature Registry 和应用权限。
2. 再执行 117，事务提交后只读验证三张 PIT 表、主外键/唯一键、索引、三个不可变触发器、D/A 投影版本列以及应用 ACL。
3. 在获准的最小写验证中，应用账号走 DataAccessKernel/Repository 做一条明确测试范围的 Feature 合并，并在事务中回滚；直接对 PIT 投影执行 UPDATE/DELETE/TRUNCATE 应被拒绝。
4. 验证 Standard、旧 PIT 行、FROZEN Snapshot 行数与内容哈希未发生变化；确认没有正式 Release 或 Dataset 被创建。
5. 在 DBA 变更记录中登记每份文件哈希、数据库身份、执行时间、迁移结果和验证证据。完成 Schema 部署不代表可以开始 20,469 条投影或五年 Feature 重算。

### 中止及恢复

- 若每个迁移按事务执行，当前文件内任何语句失败都回滚该文件；停止执行下一文件，保留已成功的前置迁移并记录状态。
- 若 116 已提交而 117 失败，不回滚 116 的表/列；修正明确失败原因后前向重跑 117。不要删除新对象来“回滚”。
- 若遇到锁超时或 Schema 与预检不一致，停止，不放宽门禁；重新盘点对象和哈希后制定前向修复。
- 若发生意外数据变化或需要恢复，使用已证明可恢复的备份/快照方案；本轮尚无可验证恢复点，因此恢复方案仍是部署阻断。

## 尚存阻断和下一步

**P2-C 生产部署就绪：BLOCKED。** 隔离工程验收已通过，但部署前必须取得备份/恢复、磁盘余量及获准迁移身份的实际证据，并建立生产变更账本。上述条件确认后，下一项最小变更才是经单独授权的 116 → 117 Schema 部署；不包括投影回灌、Feature 重算或 Release。

**正式 Foundation Release：BLOCKED。** `600699.SH` 公司行为保持 `UNRESOLVED`。部署 116/117 不会改变该分类，也不能视为 Release Gate 通过。

本报告和本轮隔离检查没有提交或推送。生产数据库没有发生迁移、DML、正式 Release 或 FROZEN 修改。

## 依据

- 本地 SQL：`sql/116_independent_debt_to_assets_factor.sql`、`sql/117_financial_standard_pit_projection.sql`、`sql/048_data_access_app_role.sql`
- 本地应用路径：`data_collect/data_access/repository/financial_repository.py`、`data_collect/data_access/rebuild_gateway.py`、`data_collect/data_access/repository/financial_projection_repository.py`
- 前序验收：`dev_doc/p2_historical_financial_feature_rebuild_20261020.md`、`dev_doc/p2_financial_standard_projection_v1_20261020.md`
- PostgreSQL 16 文档：[ALTER TABLE](https://www.postgresql.org/docs/16/sql-altertable.html)、[显式锁定](https://www.postgresql.org/docs/16/explicit-locking.html)
