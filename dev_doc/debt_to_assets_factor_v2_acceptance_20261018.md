# `debt_to_assets` 独立因子与真实黄金样本 v2 验收

日期：2026-10-18<br>
项目：`ashares_data_collect`<br>
范围：资产负债率独立计算与来源/PIT 门禁；真实黄金样本 v2 隔离集成测试。

## 结论

**工程实现和隔离集成测试通过；生产财务 PIT 与正式研究验收仍未通过。**

资产负债率现在从版本化资产负债表独立生成，不依赖收入、利润增长或现金流 Feature 是否完整。Alpha 因子读取会验证专属 Feature 版本、来源质量、来源身份、载荷哈希、报告期与信号日可用时间。新 `real-golden-v2` 在 `ashares_test` 中完成 Snapshot → Dataset → Alpha Reader 及 20/40/60 日标签链路，保留一条缺 PIT 证据样本并按 fail-closed 规则拒绝。

这项验收不证明生产 Raw → Standard → PIT → Feature 转换完整，也不证明历史公告时间戳真实留存、全市场质量或因子具有预测价值。旧 `real-golden-v1` 的 bundle、manifest、参考结果与两条失败记录保持不变。

## 契约与实现

| 项目 | 当前契约 |
|---|---|
| 研究 Schema | `research-v2.2`；v2.1 保持原义 |
| 因子集 | `frozen-factor-set-v4` |
| D/A Feature | `debt-to-assets-v1`，独立于完整财务 v2 行 |
| 公式与单位 | `total_liab / total_assets × 100`，百分数，保留四位小数；资产为零、字段缺失或非有限值拒绝 |
| 计算输入 | 对应报告期的资产负债表版本；不依赖收入、现金流或同比因子 |
| Alpha 质量门禁 | Feature 版本必须匹配；Feature 与来源质量须为 `VALID`/`PASS`；需有效 source key、record version、64 位 payload hash；报告期不能晚于信号日；有时区的可用时间必须早于上海时区信号日 15:00 |
| PIT 证据 | 仅接受声明支持的 `SOURCE_ANNOUNCEMENT_ASSUMED` 或 `WAREHOUSE_OBSERVED_ONLY`；仓库观测模式还要求观测时间早于信号收盘。公告日假设会保留为受限证据原因，不等同于当时留存的仓库时间戳 |

来源/时间证据不充分时，因子返回 `INVALID` 及具体原因；不会因数值非空或整行财务 Feature 为 `WARNING` 就视为有效。独立 D/A Feature 的质量判断也不会被无关收入报表缺失阻断。

## 黄金样本版本与来源身份

`real-golden-v1` 不变。v2 使用独立 manifest/bundle 和 `real-golden-e2e-v5` 身份，沿用 v1 的 30 只证券、2023-09-22 信号日及标签窗口，新增资产负债表 Standard 事件与独立 D/A Feature 参考。v2 的研究 Schema 为 `research-v2.2`，数据库对象带有 `REAL_SAMPLE_TEST_ONLY` 标记。

| 产物 | v2 身份 |
|---|---|
| Manifest | `tests/fixtures/real_golden_sample_v2.manifest.json`；SHA-256 `2c3585fcf8ceb606d9721324989a581015ab51dcfbc82f28abf59d9c18074924` |
| Bundle | `.codex/real-sample/real-golden-v2.json.gz`；4,345,345 字节；SHA-256 `8b0fe32c246a38e066acfd2a5ceca233af859aba994b96f6a0a415639deb7226` |
| D/A Standard 与 Feature 输入 | 各 1,044 条；测试库独立导入 |
| 因子参考 | 30 个信号横截面参考；因子总对账 300 条 |
| Dataset | `3b8e2927-e350-53fb-b3cd-d5ac3ea16e66`，状态 `FROZEN` |
| Snapshot | 61 个目标日快照，状态均为 `FROZEN` |
| Dataset 质量 | `VALID`；61 个成员日；无执行行情、涨跌停或 lineage 缺失问题 |

v2 bundle 由版本化 Standard 资产负债表事件生成，并以固定的两个真实案例单测核验单位与公式：`688426.SH` 得到 `8.6190`，`688448.SH` 得到 `27.9137`。需要限定“独立参考”的含义：v2 的批量 D/A 参考构建器复用了生产 D/A 计算函数，不能把 30 个值的对账描述为完全独立的数学实现；两只目标证券的原始资产/负债输入及结果另有显式公式回归测试。

v1 manifest 当前 SHA-256 仍为 `ADE7931D6E0776BF14A07AE8952044FDEC9161A0B7B81FFA26E375A0C28C88ED`。v1 原报告中的因子不匹配仍是历史 FAIL，不以 v2 结果回写或覆盖。

## 实际验收结果

| 检查 | 结果 | 证据 |
|---|---|---|
| D/A 单位与计算 | PASS | 两只目标证券真实 Standard 资产负债表输入的公式回归测试 |
| 财务来源质量/PIT fail-closed | PASS | 缺来源质量、来源身份不完整、信号收盘后可用、错误 Feature 版本等因子用例 |
| v2 隔离导入及重复运行 | PASS | 导入标记为 `REAL_SAMPLE_TEST_ONLY`；重复调用返回 `REUSED` 并校验哈希 |
| Snapshot → Dataset → Alpha Reader | PASS | v2 Dataset 冻结；Alpha Reader 使用 `research-v2.2` |
| 因子对账 | PASS | 300 条，0 mismatch；包含 D/A |
| 未来收益标签 | PASS | 90 条（30 证券 × 20/40/60 日），0 mismatch |
| 关键拒绝/不完整案例 | PASS | 保留 1 条缺 PIT 因子；5 条停牌标签、3 条未知退市终值标签；没有将其伪装成完整收益 |
| Data Access Contract | PASS | 257 个 Python 文件扫描，0 项违规 |
| Paper-trading boundary | PASS | 315 个 Python 文件扫描，未发现 QMT 交易 API |
| Ruff | NOT RUN | 项目虚拟环境没有 Ruff 可执行文件；未安装工具以消除告警 |
| 生产 Release / 数据写入 | NOT RUN | 只在隔离 `ashares_test` 执行；未写生产库、旧 FROZEN 或正式审批状态 |

验证命令：

```powershell
uv run pytest tests/test_alpha_research_golden_sample.py tests/test_alpha_research_factors.py tests/test_financial_feature_v2.py tests/test_research_snapshot.py tests/test_backtest_contract.py tests/test_real_golden_reference.py -q
$env:ASHARES_REAL_GOLDEN_SAMPLE_VERSION='real-golden-v2'
./tools/integration_test_db.ps1 -Action test-real-golden
uv run python tools/check_data_access_contract.py
uv run python tools/check_paper_trading_boundary.py
```

单元/契约回归结果：**131 passed**。隔离数据库真实样本质量用例与 E2E 用例分别 **1 passed**、**1 passed**。测试数据库为 `ashares_test`，环境标记 `TEST_FIXTURE`；本次结束时数据库大小约 76.6 MB，迁移账本 119 项。生产库只读，不存在生产写入。

从项目根目录确定性重建 v2 bundle/manifest：

```powershell
uv run python -m tools.build_real_golden_sample_v2
```

生成器验证已存在的不可变产物并拒绝哈希不匹配的覆盖；本次重跑后 bundle 与 manifest 哈希保持一致。

## 尚未解决与验收边界

1. 样本中的 `SOURCE_ANNOUNCEMENT_ASSUMED` 是依据公告日建立的历史可见性假设，源事件在后来采集；这不是 2023 年当时归档的公告/仓库观测证据。涉及该假设的财务 PIT 结论仍需按限制口径解释。
2. 该夹具从现有真实 Standard/PIT/Feature 证据装载到隔离库，不执行生产全量 Raw → Standard → PIT → Feature 重建；也不代表 30 证券之外的覆盖率。
3. v1 老 Snapshot 缺少逐因子来源身份，不能被新契约追溯性证明。v2 是新研究/Schema 身份，没有修改或重写旧 FROZEN。
4. 现有真实样本标签中，停牌与未知退市终值继续保持相应非完整状态。此 E2E 不消除退市选择偏差，也不证明标签完整性适用于全历史股票池。
5. 阶段一的策略回撤一致性、四组策略资金场景证据，及正式 Release/五年 Snapshot 重建不属于本次验收，仍保持原状态。
6. 工程对账通过不构成因子有效性结论；未进行 Alpha 显著性或策略收益验收，也未进入 LightGBM。

本次实现文件与测试变更留在当前工作区，未提交或推送。
