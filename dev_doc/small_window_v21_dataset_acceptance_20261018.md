# research-v2.1 小窗口 Dataset 端到端验收

日期：2026-10-18<br>
项目：`ashares_data_collect`<br>
范围：2023-09-12 至 2023-10-20 的 23 个信号日，并覆盖 60 个交易日标签尾部

## 结论

代码侧的公司行为证据和 Dataset 基线门禁已补强，针对性测试通过。只读数据预检确认目标日期共有 83 个 SSE 开市日，均有同一基线、同一 `research-v2.1` 的 FROZEN Snapshot；未来行情和沪深 300 基准也有日期级覆盖。

**端到端 Dataset 生命周期没有执行，当前状态为 BLOCKED。** DBX 连接指向 `ashares` 主库，`postgres` 数据库只有默认 `public` schema，没有隔离测试数据库。目标 Release `foundation_v2_20260821` 已是 `superseded`，其质量报告还缺少当前版本的公司行为范围审计。没有向主库写入，也没有新建或修改任何 FROZEN 数据。

## 本次代码修复

1. 公司行为证据版本提升到 `corporate-action-ex-date-evidence-v2`。除精确公告版本外，新增或删除 `std.dividend_event` 字段都会使 Schema 检查失败；未知字段（例如 `rights_issue_rate`）会把相关记录归为 `UNRESOLVED_SCHEMA_EXTENSION`，不会继承旧公告的通过结论。
2. Release Gate 现在检查公司行为表字段契约，并将 schema mismatch 纳入失败项。Foundation Release 默认按其五年数据范围生成范围审计，同时保留全库缺少 `ex_date` 的总量。
3. 新 Foundation Snapshot 和 Backtest Dataset 不再复用缺少当前除权日审计版本、记录总数不符或 Schema 检查未通过的旧 Release 质量报告。Dataset Repository 读取基线时也取回 `schema_version` 和 `quality_report`，使 Dataset 创建门禁可以核验这些证据。
4. 旧 Snapshot、Dataset、Raw 和研究结果均未修改。质量报告版本变化后，旧基线不会被当作当前证据使用。

只读执行了与 Release Repository 相同的 PostgreSQL 字段探测查询。当前 `std.dividend_event` 返回 22 个字段，与代码支持的字段契约完全一致；这证明当前 Schema 可识别，不代表未来字段扩展自动兼容。

## 小窗口只读预检

| 项目 | 结果 | 说明 |
|---|---|---|
| 信号日期 | PASS（预检） | 2023-09-12 至 2023-10-20，共 23 个 SSE 开市日 |
| 60 日标签尾部 | PASS（日期覆盖） | 2023-10-20 后第 60 个 SSE 开市日为 2024-01-15 |
| Dataset Snapshot 日期 | PASS（预检） | 2023-09-12 至 2024-01-15 共 83 个开市日；83 日均有一个同基线、同 Schema 的 FROZEN v2.1 Snapshot |
| 执行行情 | PARTIAL | 2023-09-13 至 2024-01-15 有 82 个交易日、435,037 条 `std.daily_kline` 行；尚未验证物化后逐证券覆盖、限价和成交资格 |
| 沪深 300 基准 | PARTIAL | 2023-09-13 至 2024-01-15 有 82 个交易日；尚未验证 Dataset 物化哈希和基准来源绑定 |
| 公司行为 | PARTIAL | 2023-09-12 至 2024-01-15 有 251 条已实施且带 `ex_date` 的记录；未执行 Dataset 物化后的逐条核验 |
| warmup 参数 | NOT RUN | 83 日是信号区间至标签尾部的 Snapshot 日期范围；尚未通过 Dataset 创建契约验证正式 `warmup_start` 配置 |
| Alpha Reader | NOT RUN | 没有该小窗口的新 Dataset ID；不能把直接 SQL 计数当作 Alpha Reader 验收 |
| 20/40/60 日标签成熟度 | NOT RUN | 未构建 Dataset，也未对逐证券未来行情和公司行为执行标签验收 |

以上仅为日期及源表覆盖预检，不等价于 Dataset 的创建、物化、校验、冻结或因子质量通过。

## Release 与数据库阻断

只读查询得到：

- 当前 DBX 连接为 `ashares`，`pg_is_in_recovery=false`，属于主库。
- 可见数据库只有 `ashares` 与 `postgres`；`postgres` 只有默认 `public` schema，没有项目测试表或独立环境。
- `foundation_v2_20260821` 的状态是 `superseded`，Schema 为 `feature-contract-v2.0`，质量哈希为 `b42c8ec78751344e92618c85c1e7934da40724a96a0e903898ede1a83b4b4c62`。
- 当前 `std.dividend_event` 有 31 条 `implementation_status='实施' AND ex_date IS NULL` 记录；该 Release 保存的全库计数为 0，且没有范围审计或审计版本。因此旧报告不能作为新 Dataset 的当前 Release 证据。
- 现有冻结 Dataset 包括 `f348bc11-b4f0-41fe-a8b5-d376bb5cf401`（`mixed-v2.1`）和 `f8760f3c-342c-49d5-9fcd-a989cc09955f`（Foundation）。它们不是本次隔离小窗口 Dataset，且不能替代新 Release 下的单一 Schema 生命周期验收。

由于质量报告和基线哈希是不可变身份的一部分，重新验证 Release 后不能只给旧 v2.1 快照补两个缺日，再与 710 个旧基线快照组成新 Dataset。最终研究集需要绑定单一新 Release 时，应在该基线下重建目标日期范围内的 Snapshot；小窗口试验也应在新基线下为全部 83 日建立测试快照。

## 2024-01-31、2024-02-01 Snapshot 缺口

| 日期 | `research-v2.1` | 相邻日期 | 相关记录 |
|---|---|---|---|
| 2024-01-31 | 缺失；只有 v2.0 Snapshot | 2024-01-30 与 2024-02-02 均有 v2.1 FROZEN Snapshot | 有底层 Price、Valuation、Universe、Tradeability、Financial PIT 特征；对应快照批次曾两次因 `pit.universe_daily` 查询超时失败，之后存在 v2.0 成功批次 |
| 2024-02-01 | 缺失；只有 v2.0 Snapshot | 同上 | 有底层特征；存在 v2.0 成功批次 |

当前证据排除了“底层 Feature/PIT 完全无数据”这一简单解释，但**还不能确定 v2.1 特定缺口的最终根因**。`ops.run_manifest.scope` 记录了批次日期和目标表，却没有 `schema_version` 或输出 Snapshot ID；现有的成功日志不能证明它们生成了 v2.1。可见日志表明 1 月 31 日有超时重试，随后仅能确认 v2.0 输出；2 月 1 日也只能确认 v2.0 输出。应在新基线下对这两日执行定向 Snapshot 重建并保留新身份，不能覆盖旧快照。

全区间清点为 710 个 FROZEN v2.1 Snapshot（2023-09-12 至 2026-08-21），预期 712 个开市日，缺口正是上述两日。目标 23 日研究窗口本身及其到 2024-01-15 的标签尾部没有 Snapshot 日期缺口。

## 财务因子与 Alpha Reader

本次没有通过新 Dataset 的 Alpha Reader，也没有生成 20/40/60 日因子统计。对全窗口 `stock_snapshot_daily` 做单次 SQL 聚合时发生 DBX 30 秒超时，因此没有把超时查询当作字段有效率证据。

可沿用最近一次 2026-08-21 v2.1 Snapshot 的既有统计作为字段能力参考：ROE 5,218、营收增长 5,354、利润增长 4,086 条满足数值和各自质量状态；24 条缺少财务可用时间戳。它们不是本次 23 日窗口的结果，也不是 PIT 完整性通过证明。新 Dataset 建成后，仍需由 Alpha Reader 按 Dataset ID 输出字段覆盖、拒绝原因、PIT 证据和有效样本数。

## 测试与质量检查

| 检查 | 状态 | 结果 |
|---|---|---|
| 相关单元与契约测试 | PASS | 174 passed，包含公司行为版本变更、额外/缺少字段、旧 Release 拒绝、单一 Schema 与基线身份 |
| Data Access Contract | PASS | 扫描 256 个文件，0 violations |
| Paper Trading Boundary | PASS | 检查 312 个可执行 Python 文件，未发现 QMT 交易 API |
| Python compileall | PASS | 本次修改的 Python 文件编译通过 |
| `git diff --check` | PASS | 本次文件无空白错误；输出中仅有已有 `AGENTS.md` 与 `skills/qmt-xtquant/SKILL.md` 的 CRLF 提示 |
| Ruff | NOT RUN | 当前环境没有 `ruff` 可执行文件；未安装依赖 |
| 数据库写入 / Dataset 生命周期 | NOT RUN | 无隔离测试库，且生产连接明确为主库 |

## 下一步可执行计划

1. 提供或配置真正隔离的测试数据库；迁移项目 Schema 后，仅在测试库验证 Release Gate v2、Snapshot/Dataset 单一 Schema 和身份拒绝路径。
2. 在隔离环境创建一份新 Release 质量证据，核实当前 31 条记录的全库统计、范围分类与公司行为 Schema。仍未解决的记录保持阻断，不填造 `ex_date`。
3. 对 2023-09-12 至 2024-01-15 的 83 日，在新 Release 身份下运行 `create → materialize → validate → freeze`；核验 Dataset 成员、每日行情、交易规则、基准、公司行为、哈希和逐证券标签覆盖。
4. 用新 Dataset ID 运行 Alpha Reader，仅把 2023-09-12 至 2023-10-20 作为信号日；分别输出 ROE、营收同比、利润同比、估值和趋势因子的覆盖及拒绝原因。
5. 对 2024-01-31、2024-02-01 建新 v2.1 Snapshot，并通过新基线重新生成最终目标区间全部 Snapshot，确保 Dataset 内基线哈希统一。五年扩建前，先分月预检 2021-08 至 2023-09 Feature/PIT/lineage 覆盖与计算耗时。

本轮没有创建 Dataset ID；Dataset 生命周期和 Alpha Reader 仍未验收。阶段一回撤一致性、四组资金场景以及正式因子有效性结论均未被本报告标记为通过。
