# 独立集成测试数据库建设验收

日期：2026-10-18<br>
项目：`ashares_data_collect`<br>
环境类型：`TEST_FIXTURE`<br>
验收结论：**隔离数据库、迁移/安全集成测试，以及合成 v2.1 Snapshot → Dataset → Alpha Reader 生命周期通过；生产数据质量和正式 Release 发布能力未在本测试库验收。**

## 已交付

- 新建独立 PostgreSQL 16 Docker Compose 配置、命名卷和容器，数据库为 `ashares_test`，只绑定本机 `127.0.0.1:15433`。未复用项目原有 PostgreSQL 容器或卷。
- 生成独立的管理员与应用账号凭据，保存在被 Git 忽略的 `.codex/integration-test/`。应用账号无 superuser、createdb、createrole 权限，也不能连接迁移兼容数据库 `ashares`。
- 迁移原有 118 个项目 SQL 文件，未修改迁移历史文件。迁移文件名及 SHA-256 持久化在 `integration_test.schema_migration`；重新执行 `migrate` 会校验已应用文件的哈希。
- 因历史迁移 048 写死数据库名，在测试集群内创建空的兼容数据库 `ashares`，迁移完成后撤销 `PUBLIC` 与 `ashares_app` 的连接权限。该库不含业务数据。
- 集成测试连接配置通过 `data_collect.data_access.schema` fail-closed 校验：必须精确匹配测试主机、端口、数据库、应用用户和数据库内环境 UUID 标记。
- 增加了隔离环境操作脚本，支持 `init`、`migrate`、`status`、`test`、`stop` 和受标签校验的 `clean -RemoveVolume`。普通 `stop`/`clean` 保留命名卷。
- 数据库实测确认正式 FROZEN Snapshot 更新触发器会拒绝变更。测试在事务中创建临时记录并回滚，不留下测试业务行。

## 实测结果

| 项目 | 结果 |
| --- | ---: |
| PostgreSQL | 16 |
| SQL 迁移 | 118 / 118 |
| 全新命名卷完整迁移时间（含数据库哈希账本写入） | 本轮 183.167 秒；此前一次 184.644 秒 |
| 最近一次重复迁移检查时间 | 3.710 秒；新增迁移 0 |
| 合成研究夹具装载前 `ashares_test` 大小 | 27,532,311 字节（约 26.3 MiB） |
| 用户表 | 416 |
| 用户触发器 | 120 |
| 数据库集成测试 | 7 passed |
| 研究/数据访问定向测试 | 106 passed |
| DataAccess 契约门禁 | 通过；扫描 256 个文件，0 项违规 |
| 纸面交易边界门禁 | 通过；扫描 312 个 Python 文件，无 QMT 交易 API |
| Python compileall | 通过 |
| Snapshot → Dataset → Alpha Reader 合成集成测试 | 1 passed；155.62 秒 |
| 合成研究样本 | 20 个虚构证券；23 个信号日；60 个未来交易日 |
| 合成 Dataset | FROZEN；83 个成员/执行日；115 条日历行（含预热）；1,660 条执行行情 |
| 合成研究数据 | 83 个 FROZEN Snapshot；1,660 条 Snapshot 观测；460 条信号观测 |
| 未来收益标签 | 1,380 条；20/40/60 日均为 `COMPLETE` |
| 测试库大小 | 约 44 MiB（生命周期测试后） |

容器与卷当前保留并运行。凭据、运行配置及本地迁移检查点位于被忽略的 `.codex/integration-test/`，没有写入报告。

## 安全与数据边界

- 本轮没有连接、读取或写入生产数据库；没有复制生产数据。
- 没有修改现有 FROZEN、生产迁移、正式策略或策略审批状态，也没有部署或推送。
- 测试库中新增的研究输入均为虚构证券与合成数据，不能用于正式因子研究、市场覆盖或策略收益结论。
- 生成的测试 Dataset ID：`8d18788c-8bca-4aef-91d0-5fd8447b18c8`。它只属于 `TEST_FIXTURE` 环境，不得复制或解释为生产 Dataset。
- 测试通过真实 Snapshot/Dataset Repository 和 Alpha Reader 查询接口完成。输入事实直接由 fixture 装载，没有运行生产 Raw → Standard → PIT → Feature 转换流程。
- 合成 Snapshot 的最终冻结使用 Snapshot Repository 并执行范围内身份、PIT/lineage 与内容检查；它没有通过正式全市场 Release 发布门。Dataset 本身仍经正常的 create → materialize → validate → freeze 作业流程。
- 正式 Release 门禁拒绝该合成环境是预期结果；测试明确断言不存在已验证的正式 `foundation_v2` Run。正式质量门和候选审批边界保持未变。

## 尚未验收的生产范围

当前空库的正式 Release 质量门按预期拒绝发布，已观测到的阻断包括：

- 4 个 `index_daily` 检查点缺失；
- `foundation_v2` 公司和估值数据为空；
- 没有覆盖目标范围且经过验证的生产模式 Foundation Run；
- 空的 `std.dividend_event` 无法提供除息日分类所需的列信息。

这些约束属于正式全市场 Release 契约。测试没有伪造生产 Foundation Run，也没有放宽门禁；测试专用基线只用于验证下游 Snapshot、Dataset 和 Alpha Reader 的连接行为。当前集成测试不能证明真实数据源转换流程正确、全市场历史数据完整、财务 PIT 无泄漏、退市收益口径正确，或任何因子具有投资价值。若要验收这些内容，仍需在受控的真实历史研究数据上单独执行数据质量审计与正式发布。

## 文件

- `docker-compose.integration-test.yml`：独立 PostgreSQL 服务与卷。
- `tools/integration_test_db.ps1`：环境初始化、迁移、运行、状态和受控清理。
- `tests/test_integration_test_database_guard.py`：连接目标 fail-closed 单元检查。
- `tests/test_integration_test_database.py`：真实数据库隔离、迁移、权限、质量门拒绝和 FROZEN 保护检查。
- `tests/integration_fixtures/alpha_research_v21.py`：隔离数据库中的合成 v2.1 研究数据夹具。
- `tests/test_integration_alpha_research_dataset.py`：Snapshot、Dataset 冻结和 Alpha Reader/标签集成测试。
- `data_collect/data_access/schema.py`：集成测试环境连接身份校验。
- `dev_doc/integration_test_database.md`：运行说明和边界。
