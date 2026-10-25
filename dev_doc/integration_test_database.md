# 独立 PostgreSQL 集成测试环境

该环境只用于验证数据库迁移、数据访问边界及小型测试夹具。它使用独立的 PostgreSQL 16 容器、命名卷、测试数据库 `ashares_test` 和受限登录 `ashares_app`，不会复用项目现有 `ashares-postgres` 容器或其数据卷。

## 启动与验证

从项目根目录运行：

```powershell
./tools/integration_test_db.ps1 -Action init
./tools/integration_test_db.ps1 -Action migrate
./tools/integration_test_db.ps1 -Action test
./tools/integration_test_db.ps1 -Action status
```

Compose 只发布 `127.0.0.1:15433`。本地生成的连接凭据、环境 UUID、迁移检查点和临时运行配置放在已忽略的 `.codex/integration-test/` 下，不显示密码。应用连接还要求数据库名、端口、用户和该数据库内的环境 UUID 标记全部匹配；`ashares` 或其他端点会在打开连接前被拒绝。

旧迁移 `048_data_access_app_role.sql` 写死 `GRANT CONNECT ON DATABASE ashares`。迁移脚本因此只在新的测试容器集群中创建一个空的兼容数据库 `ashares`，以便原迁移原样执行；迁移完成后撤销 `PUBLIC` 和 `ashares_app` 对它的连接权。所有项目迁移仍应用到 `ashares_test`。

迁移版本和 SHA-256 哈希保存在 `integration_test.schema_migration`，本地忽略目录中的检查点仅作恢复辅助。每次迁移会比对已记录文件哈希；已应用的 SQL 文件被修改时会停止。`057_daily_kline_partitioned_pilot.sql` 在 `050_mart_contract_views.sql` 前执行，以满足两者间已有对象依赖，不修改迁移原文。

`-Action stop` 和默认 `-Action clean` 保留命名卷。只有显式传入 `-RemoveVolume`，脚本才会验证卷标签后删除该测试卷。不会删除、停止或清理现有项目容器。

## 数据与验收边界

当前实现完成独立集群、完整项目 SQL 迁移、环境身份校验，以及合成 `research-v2.1` 数据上的 Snapshot → Dataset → Alpha Reader 集成测试。测试库不含生产样本，也不提供可供正式研究的行情或财报。`TEST_FIXTURE` 环境标记仅说明该数据库的用途，不能作为全市场数据质量或投资表现证明。

`./tools/integration_test_db.ps1 -Action test` 会运行数据库隔离/迁移检查和合成 Alpha 研究 Dataset 生命周期测试。后者使用 20 个虚构证券、合成工作日历和合成价格/财务特征；它通过真实 Snapshot、Dataset Repository 与 Alpha Reader 接口，验证 83 个 Snapshot、冻结 Dataset、五项因子读取及 20/40/60 日标签构建。测试输入在 fixture 中直接装载到项目数据层，未执行生产 Raw → Standard → PIT → Feature 转换作业，也未通过正式全市场 Release 质量门。因此这项测试证明的是下游研究链路的契约与集成行为，不证明生产数据覆盖、PIT 完整性、财务公式有效性或收益预测能力。

真实黄金样本使用独立命令 `./tools/integration_test_db.ps1 -Action test-real-golden`。默认版本为 `real-golden-v1`；它的 manifest、bundle 和旧参考保持不变，历史结果仍包含两条 `debt_to_assets` 不匹配记录。设置 `$env:ASHARES_REAL_GOLDEN_SAMPLE_VERSION='real-golden-v2'` 后运行同一命令，会使用新版本 manifest 和 bundle，测试 `research-v2.2`、`frozen-factor-set-v4` 及独立的 `debt-to-assets-v1` 来源质量/PIT 契约。v2 保留 v1 的 30 只证券、信号日和标签窗口，以版本化资产负债表 Standard 事件补齐 D/A Feature 身份，并单独保存新的因子参考结果。重建 v2 bundle 的确定性命令是 `uv run python -m tools.build_real_golden_sample_v2`；若目标 bundle 已存在但内容哈希不同，生成器会拒绝覆盖。

v1 与 v2 共用测试数据库中的 Release 和交易日历表，两个版本不能在同一数据库状态中依次重跑；选择另一版本前必须使用 `./tools/integration_test_db.ps1 -Action clean -RemoveVolume` 清理经容器及卷标签校验的测试卷，再运行目标版本。该清理会删除整个隔离测试卷及其测试数据，不能用于生产数据库。`test-real-golden` 仅写入 `ashares_test`，不会创建正式 Release，也不证明 Raw → Standard → PIT → Feature 的生产转换或全市场质量。v2 当前 E2E 结果与版本身份见 [debt_to_assets_factor_v2_acceptance_20261018.md](debt_to_assets_factor_v2_acceptance_20261018.md)。

测试专用 Release/基线与正式生产质量基线分开。正式 Release 质量门在此合成环境中仍应拒绝；测试不得伪造生产 Foundation Run、修改正式 FROZEN 数据或放宽质量门。当前测试范围和剩余生产验收边界见 [integration_test_database_acceptance_20261018.md](integration_test_database_acceptance_20261018.md)。
