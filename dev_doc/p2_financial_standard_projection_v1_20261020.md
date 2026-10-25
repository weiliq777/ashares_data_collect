# P2-B：财务 Standard/PIT 追加式版本化验收

日期：2026-10-20<br>
项目：`ashares_data_collect`<br>
结论：**隔离环境工程验收 PASS；生产迁移与投影 NOT RUN**

## 1. 验收范围

本批将 20,469 条缺失 Standard 可用时间的财务来源，转换为独立 PIT 投影。生产库仅通过只读账号和只读事务导出受限输入；所有写入均发生在独立 PostgreSQL `ashares_test`。

本批不修改旧 Standard/PIT、Raw、FROZEN、策略或正式 Release，不将公告日期假设升级为实际历史观察证据。

## 2. 版本化存储契约

Raw 的 `record_id` 继续作为原始来源版本身份，Standard 的 `source_record_key`、`record_version`、原始载荷哈希及财务数值保持不变。新的 PIT 解释存入 `pit.financial_standard_projection`，其身份独立于 Raw 版本：

- 投影键：`projection_version + source_dataset + standard_id`。
- 原始身份：`raw_dataset + raw_record_id`，并校验 `source_record_version == raw_record_id`。
- 内容身份：Raw 载荷哈希、Standard 内容哈希、公告日期假设、PIT 时间、质量状态、规则版本共同生成投影内容哈希。
- 批次身份：投影版本、来源类别、批次号、边界来源 ID、行数及输入/输出哈希。
- Run 身份：输入 Bundle、候选清单、代码、规则、基线投影、总行数和预期输出哈希都写入不可变 Manifest。

新增迁移 [117_financial_standard_pit_projection.sql](../sql/117_financial_standard_pit_projection.sql) 建立 Run、Batch、Projection 三张追加式表。数据库触发器拒绝 UPDATE/DELETE，应用账号没有 UPDATE、DELETE、TRUNCATE 权限。该迁移不向旧 Standard 表补写 `data_available_timestamp`。

下游通过 `FinancialProjectionSelection(projection_version, manifest_sha256)` 显式选择投影；Manifest 同时声明基线 `standard-inline-v1`。读取只连接选定投影版本，未覆盖的既有 Standard 时点仍按其声明的基线读取，不会读取其他投影版本。Feature Builder/CLI 支持传入这一选择，批次检查点也绑定投影版本和 Manifest 哈希，财务组件 lineage 保留投影版本与投影内容哈希。

## 3. 生产只读输入

输入通过 `FinancialStandardRepository` 有界查询获得，未复制生产数据库。生产连接身份核验结果：数据库 `ashares`、账号 `ashares_app`、非 superuser、`transaction_read_only=on`。

| 来源 | 行数 |
| --- | ---: |
| 资产负债表 | 8,184 |
| 现金流量表 | 5,946 |
| 利润表 | 6,339 |
| **合计** | **20,469** |

- 输入 Bundle：9,112,734 字节；SHA-256 `8949796ad07a83254ebc5f99d2048122bbe7235998829f52fb2a3d0ef65fbb6e`。
- 候选清单 SHA-256：`848235cc07224b15b9915318ce42173063b7c58ef090d65c4062e31d77b33577`。
- Raw 严格身份匹配 20,448 条；其余 21 条是 `end_type` 的 NaN/NULL 表示差异，经规范化为空值后匹配，没有更改 Raw 哈希。
- 所有 20,469 条均使用公告日期加一个自然日、上海时区午夜作为投影可用时间，并保留 `SOURCE_ANNOUNCEMENT_ASSUMED`。Raw 抓取时间只保留为 `source_observed_at`，没有用于 PIT 可用时间。
- 转换后的证券、报告期、公告日期、报告类型、合并类型、`end_type`、业务键、来源版本、质量状态与载荷哈希检查：0 项来源身份差异；成功 20,469，拒绝 0。

这只证明投影遵守当前声明的公告日假设规则；不能证明仓库当年实际观察到了这些财报，也不能把 `SOURCE_ANNOUNCEMENT_ASSUMED` 视为已验证公告时间。

## 4. 隔离全量投影结果

隔离实例：`ashares-p2-projection-v2-postgres`，数据库 `ashares_test`，环境类型 `TEST_FIXTURE`，账号 `ashares_app`，非 superuser。环境 ID：`86f89de7-66ea-49ea-86f8-90b017d12ce4`。当前迁移账本 120 项，包含迁移 117。测试库最终大小约 140.6 MB；其中保存了多个调试投影版本，不能把该大小视为单次生产投影的容量估算。

最终通过的投影版本：`financial-source-announcement-projection-v6`。<br>
Run Manifest SHA-256：`f9345ace0b8a8c1b89a3f8a2dac6b1f62794b3a18e79c6b3c5c2bd75a133c987`。<br>
预期输出 SHA-256：`eb0de73501535eede8230175b8cd5cae80803ad5bfacfd6016eebae69f663828`。

| 验收项 | 结果 |
| --- | --- |
| 隔离 Standard 基础记录 | 20,469 条 |
| v6 投影记录 | 20,469 条，Raw 版本唯一 20,469 条 |
| 按来源投影 | 8,184 / 5,946 / 6,339 |
| 公告日假设 PIT 状态 | 20,469 条全部为 `SOURCE_ANNOUNCEMENT_ASSUMED` |
| `end_type` NaN→NULL | 21 条（资产负债表 1、现金流 4、利润表 16） |
| 20,469 条原 Standard 内容 | 前后哈希相同：`1970c72c2a41fbd647428808da679656838baf3f5f69b99b1d37247f03b7a532` |
| 原 Standard PIT 为空值 | 20,469 条仍保持为空值 |
| 未选择投影时读取缺时点来源 | 0 条 |
| 显式选择 v6 的财务 Reader | 120 条样本事件 |
| 选定版本参与 Feature 计算 | 30 个证券生成 30 行内存 Feature；120 个 lineage 项中 90 个财务来源指向 v6，另 30 个是每个 Feature 的质量证据标记 |
| 生产写入 | NOT RUN |

Feature Reader 检查使用明确的 v6 + Manifest 选择，随后执行 `calculate_features()`，没有向 Feature 表写入整批历史结果。

## 5. 中断恢复与拒绝行为

进程级恢复分两次独立命令完成：

1. 第一次运行提交 42 批中的前 9 批后正常退出，状态为 `INTERRUPTED_CHECKPOINT`。对不完整投影调用完整性门禁，按预期拒绝读取。
2. 第二次进程以相同输入、Run ID、代码哈希和 Manifest 执行 `--resume`。已提交的 9 批为幂等无操作，余下批次追加完成。最终 42 批、20,469 行及输出哈希与 Manifest 一致。

其他负向检查：

- 同一投影版本/Manifest 下以不同内容重投已存在身份：拒绝。
- 错误 Manifest 哈希：拒绝。
- 可用时间不等于公告日次日上海时区午夜：拒绝，且未写入该错误行。
- 应用账号对投影表 UPDATE、DELETE、TRUNCATE 权限：均为 `false`。
- 旧 Standard 值和 PIT 字段不因投影而改变。

此前尝试的 v2–v5 测试投影记录仍留在隔离库中，保留为调试证据；只有 v6 的最终验收结果标记为 PASS。所有下游读取必须携带完整投影版本和 Manifest，不使用这些早期测试版本。

## 6. 实际代码与检查结果

- 新增 Repository：`FinancialProjectionRepository`，负责受限 Run/Batch/Projection 追加、幂等冲突校验和全量完整性校验。
- 新增只读导出工具和隔离回放工具；回放工具只允许 `integration_test` 身份。
- 扩展 `FinancialRepository.load_events()`、财务 Feature 重算入口及 CLI，使其能够显式选择投影版本，并将版本哈希传入 lineage 和断点身份。
- 新增迁移 117、P2-B 单元测试和本报告。

检查：

| 检查 | 结果 |
| --- | --- |
| PIT 投影、财务契约、财务 Feature 测试 | PASS，66 passed |
| Integration Test Database 迁移/安全测试 | PASS，7 passed |
| 新增/本批工具文件 Ruff | PASS |
| Data Access Contract | PASS，259 文件，0 violations |
| Paper Trading Boundary | PASS，320 Python 文件，无 QMT 交易 API |
| compileall | PASS |
| `git diff --check` | PASS；仅提示既有 AGENTS/QMT 文档 CRLF 行尾警告 |
| 生产迁移、生产投影、正式 Release | NOT RUN |
| 全量 Feature 物化及正式 Alpha 重算 | NOT RUN |

全文件 Ruff 扫描还报告了 `financial_repository.py`、`financial_standard_repository.py` 及既有财务 Feature 文件中的其他告警；本批只修正新增代码告警，未扩大到无关清理。

## 7. 生产执行准备方案

本批证明了隔离 Schema、写入策略和读取接口；没有证明生产迁移或生产历史写入已经安全完成。

1. **单独核实当前生产迁移账本。** 不假定生产已应用 116；先确认 116 与 117 的依赖和实际 Schema。迁移 117 依赖 `feature.debt_to_assets_v1`，应在 116 确认完成后执行。
2. **生产变更前置核验。** 备份/恢复演练、磁盘余量、迁移账号和应用账号权限、当前 D/A 表大小、活跃事务、连接身份及维护窗口均需记录。迁移对 D/A 表增加 nullable 列，仍需为短暂表级锁设置 `lock_timeout` 并在低峰执行；不得与历史重算同时运行。
3. **部署先于数据写入。** 先部署 Schema/权限并验证 SELECT/INSERT 和 UPDATE/DELETE/TRUNCATE 拒绝，再由单独授权启动投影批次。迁移 117 不提供删除旧表或回写旧 Standard 的回滚路径；部署错误使用前向修复，投影错误创建新版本，不修改既有投影。
4. **批次投影。** 固定输入 Bundle、候选清单、代码、规则、投影版本、基线、总输出哈希；500 行一批记录断点。每批先追加，冲突时比较内容哈希；版本完整性通过前 Feature 不得读取。
5. **生产 Feature 与 Release。** 另行授权后，才按投影版本重算 Feature、逐因子统计覆盖和拒绝原因，再运行 Release Gate。当前 `600699.SH` 公司行为仍未解决；即使 PIT 投影成功，也不能据此创建正式 Foundation Release 或五年 FROZEN Dataset。

## 8. 最终状态与下一步

P2-B 的版本化存储、20,469 条隔离投影、进程级中断恢复和显式 Feature 输入读取已通过工程验收。剩余事项是生产环境的迁移/权限前置审查及独立授权的分批投影；历史公告时间仍是受限假设，`600699.SH` 与其他 Release 质量门继续阻断正式发布。

下一项最小工作：基于生产只读 Schema/迁移账本做一次 116→117 兼容性与锁风险核验，提交具体执行清单；该清单完成后再申请生产结构部署，不应直接启动五年投影或 Feature 重算。
