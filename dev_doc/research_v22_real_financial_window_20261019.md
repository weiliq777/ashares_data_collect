# research-v2.2 真实财务窗口重算与集成验收

- 项目：`ashares_data_collect`
- 样本版本：`real-financial-window-v1`
- 信号区间：2023-09-12 至 2023-10-20，共 23 个交易日
- 执行行情尾部：至 2024-01-15，共 83 个交易日
- 审计日期：2026-10-19
- 生产库访问：`ashares.ashares_app`，只读事务
- 写入范围：仅 `ashares_test`，`REAL_SAMPLE_TEST_ONLY`
- 结论：**财务来源读取与四因子重算通过；Dataset 生命周期通过；因测试库遗留 D/A 行造成因子对账失败，阶段二 E2E 未通过**

## 执行结论

本次从不可变样本包导入 4,329 条真实 Standard 财务事件到 `ashares_test`，再通过 `FinancialRepository.load_events()` 和正式 `calculate_features()` / `calculate_debt_to_assets_features()` 路径重算。重算值和样本中按来源事件生成的参考结果哈希一致；测试库内生成 717 条财务 v2 Feature 与 1,102 条 D/A Feature。

83 个 `research-v2.2` 测试 Snapshot、Dataset 物化、验证、冻结和 Alpha Reader 均可运行。20/40/60 日合计 2,070 条标签在 Dataset Reader 与 Alpha Reader 两条读取路径上均与参考一致。

四项财务因子的计算参考中，D/A 690/690 有效；ROE、营收同比和利润同比按各自质量及 PIT 门禁产生有效、缺失或拒绝状态。Snapshot 中有 23 个信号日的 D/A 选择了测试库早期 golden 测试留下的错误报告期行。实际差异全部为 `000666.SZ / debt_to_assets`，实际值被判为 `REPORT_PERIOD_TYPE_INVALID`，应有的当前版本值为 36.619。其余 3 个财务因子、财务 Feature 行存在性和 2,070 条标签没有对账差异。

因此，本报告不把测试 Dataset 标记为因子 E2E 通过，也不把工程计算通过等同于数据研究验收通过。

## 样本与数据身份

| 项目 | 结果 |
|---|---|
| 固定证券 | 30 只，沿用既有黄金样本证券集合；未伪造或改写旧 manifest |
| Bundle SHA-256 | `bfe695f509e0517b2f0c94a63745c6b55cce7be5ae8fdec541cd8889facaf615` |
| Manifest | `tests/fixtures/real_financial_window_v1.manifest.json` |
| 压缩 Bundle | 7,119,324 字节；本地忽略文件 `.codex/real-sample/real-financial-window-v1.json.gz` |
| 数据库 | `ashares_test`，角色 `ashares_app`，环境标记 `TEST_FIXTURE`；迁移账本 119 项 |
| 测试库大小 | 本次运行后约 102 MB；未复制生产整库 |
| 测试 Release | `it-real-financial-window-e2e-v6`，`REAL_SAMPLE_TEST_ONLY`，不是正式 Foundation Release |
| Snapshot | 83 个，全部 FROZEN，Schema 为 `research-v2.2`，基线为测试专用 Release |
| Dataset | `3115e55d-af1e-59e1-9ea8-696b6506956e`，状态 FROZEN，测试名 `real-financial-window-e2e-v6` |

该 Dataset 的质量报告为 `VALID`，只说明 Dataset 自身的成员、执行行情、日历和物化质量门通过；独立因子对账为 `FAIL`，不能用前者替代后者。

## 来源与财务 Feature 重算

### Standard 事件身份

| Standard 来源 | Bundle 事件数 | 测试库事件数 | 结果 |
|---|---:|---:|---|
| Balance Sheet | 1,102 | 1,102 | PASS |
| Income | 1,101 | 1,101 | PASS |
| Cash Flow | 967 | 967 | PASS |
| Fina Indicator | 1,159 | 1,159 | PASS |
| 合计 | 4,329 | 4,329 | PASS |

重算前按证券、报告期、数据可用时间、来源键、版本和 payload hash 对齐了读取事件；字段身份和值均与不可变 Bundle 相符。因子输入没有使用旧 FROZEN 中已有的财务 Feature 作为计算结果。

### Feature 数量和质量

| 输出 | 重算数 | 质量状态 |
|---|---:|---|
| `company_financial_v2` | 717 | 496 `VALID`；221 `INCOMPLETE_TTM` |
| `debt_to_assets_v1` | 1,102 | 794 `VALID`；308 `WARNING / SOURCE_QUALITY_NOT_VALID` |

D/A 公式为 `total_liab / total_assets × 100`，保留四位小数。例如 `000666.SZ` 2023-03-31 的输入为负债 14,374,979,181.50、资产 39,255,502,389.51，结果为 36.619。该事件来源时间为 2023-04-29 16:00 UTC，质量状态 `VALID`；没有使用现金流或收入报告作为 D/A 的共同依赖。

### 四项因子在参考信号横截面的状态

以下 690 条记录来自样本的财务 Feature 参考和当前因子门禁。`VALID` 表示通过现有工程质量契约，不表示具有统计预测价值。

| 因子 | 有效状态 | 其他状态 |
|---|---:|---|
| `debt_to_assets` | 690 `VALID` | 0 |
| `roe_ttm` | 552 `VALID` | 92 `INVALID / ROE_QUALITY_STATUS_NOT_VALID`；46 `INVALID / FINANCIAL_PIT_EVIDENCE_NOT_SOURCE_ANNOUNCEMENT` |
| `revenue_growth_yoy` | 483 `VALID`；115 `VALID / SIBLING_GROWTH_FACTOR_INSUFFICIENT` | 46 `MISSING / GROWTH_VALUE_MISSING_OR_NONFINITE`；46 PIT `INVALID` |
| `profit_growth_yoy` | 483 `VALID` | 161 `MISSING / GROWTH_VALUE_MISSING_OR_NONFINITE`；46 PIT `INVALID` |

不得将缺值、质量拒绝和 PIT 拒绝合并称为财务数据未采集。样本窗口财务因子绝大部分使用 `SOURCE_ANNOUNCEMENT_ASSUMED`；它是显式历史可见性假设，不等同于仓库当年保留了公告首次公开时刻。

## 收益标签和 Alpha Reader

| 检查项 | 结果 |
|---|---|
| 信号观察 | 690 条（23 日 × 30 只） |
| 执行行情 | 2,434 条（83 日 × 样本证券日） |
| 公司行为 | 2 条，限于该样本窗口 |
| 20/40/60 日标签 | 2,070 条；不完整、停牌和未知退市终值状态按原契约保留 |
| Dataset 标签对账 | PASS，0 差异 |
| Alpha Reader 标签对账 | PASS，0 差异 |
| Dataset 因子对账 | FAIL，23 个 D/A 差异 |
| Alpha Reader 因子对账 | FAIL，同样 23 个 D/A 差异 |
| Alpha Reader 数据身份 | PASS，单一 `research-v2.2` Schema，83 个 FROZEN Snapshot |

本 Bundle 的不可变因子参考只覆盖四项财务因子；本次没有把 PE、动量和其他非财务因子报告成已完成逐值对账。

## D/A 差异根因

旧 `real-golden-v1/v2` 导入曾把 D/A 计算中错误残留的报告期写入测试库。修复后的计算器已为每条来源版本保留正确报告期，并新增的 1,102 个 v1 D/A Feature 行哈希与参考一致；旧测试行没有删除、覆盖或改写。

对 `000666.SZ`，旧表中有一条来源键为 `000666.SZ:2022-12-31:1:1:4` 的记录，却被旧计算器标成 `report_date=2023-06-30`。Snapshot 选择器在信号截止前按 `report_date DESC, data_available_timestamp DESC, record_version DESC, feature_id DESC` 排序，于是旧行以更晚的假报告期压过正确的 2023-03-31 来源版本。2023-09-12 Snapshot 实际保存了旧行的 37.1845 并因报告期无效拒绝；参考行应选择 2023-03-31、36.619。其余 22 个信号日同样受该错误旧行影响。

这是**隔离测试数据库跨样本版本污染**，不是本次 Standard 输入重算错误。因为旧记录没有样本版本列，Snapshot 选取器无法判断它来自哪个测试夹具。

在本轮首次对账失败之前，测试 Dataset 已被旧流程冻结；冻结是不可变的，本轮保留该 Dataset 和结果作为失败证据。随后代码已增加规则：处于 `VALIDATED` 的 Dataset 只有在因子和标签参考对账都通过时才冻结。当前再运行时，原 Dataset 显示 `dataset_freeze_gate=BLOCKED_REFERENCE_MISMATCH`、`dataset_freeze_action=NOT_APPLICABLE_FROZEN`；这表示原已冻结对象未更改，并不声称新门控已通过全新 Dataset 的集成验证。

## PIT warmup 与历史阻断

### Warmup 缺少 `data_available_timestamp`

先前五年审计在 2015-01 至 2021-08-01 统计到 493,281 条 Standard 财务行，其中 20,469 条缺可用时间戳：Balance Sheet 8,184、Cash Flow 5,946、Income 6,339；Fina Indicator 为 0。

本轮在生产只读事务中对三类缺时间戳 Standard 记录各抽 5 条，并按证券、报告期、报告类型、合并类型、报告期末类型和 payload hash 匹配 Raw 版本。15/15 条都找到相符的 Raw 版本；公告日期与 Standard `announce_date` 一致，Raw `update_flag` 存在。Standard `source_record_key` 与 Raw `business_key` 字符串格式不相同，因此身份核对使用了源字段组合和 payload hash，而不是假设两键字面相等。

此结果说明这 15 条样本具备重新投影的来源线索，但不能外推为 20,469 条全部可修复。没有填时间戳，也没有写生产库。下一步应对全量缺失集合按源版本逐批完成 Raw 匹配，再通过正式 Standard/PIT 版本化流程重投影；不能直接把 Raw 日期当成历史实际观察时刻。

### 2021-06 Feature 覆盖异常

既有只读统计为：2021-06 资产负债表 Standard 9,805 行、Income Standard 9,652 行，但 `company_financial_v2` 只有 173 行；2021-03 Feature 为 4,583 行。`ops.foundation_rebuild_run` 未找到覆盖 2021-06 输出区间的记录。当前证据不能确定是任务范围、任务中断、依赖状态还是历史过滤造成。它仍是早期历史重算前的阻断；没有据此认定 Raw 缺失，也没有补造 Feature。

### `600699.SH` 公司行为

既有公司行为范围报告显示：该证券 source version `69862`、报告期 `1993-12-31`，目前只有 1994-04-02 实施公告日期，缺少可靠除权日/权益影响结论。报告日期很早不足以证明其不影响审计范围；记录继续保持 `UNRESOLVED`，正式 Foundation Release 仍被阻断。没有推测 ex-date 或修改质量门。

## 本轮代码变更

- `data_collect/jobs/tushare_foundation_features.py`：修复 D/A 输出循环误用前序变量报告期的问题，按当前源版本身份使用其自身 `report_date`。
- `tests/test_financial_feature_v2.py`：增加跨多个财报期 D/A 日期回归用例。
- `tests/integration_fixtures/real_golden_sample.py`：新增窗口包验证、Standard 来源导入、正式计算路径重算、四财务因子逐日比较、Alpha Reader 比较；参考不一致时禁止冻结新的 `VALIDATED` Dataset。
- `tests/test_integration_alpha_research_dataset.py`：将当前测试库 D/A 污染作为明确失败证据断言，不把端到端 `FAIL` 改写成研究通过。
- `tests/test_alpha_research_golden_sample.py`：当某版本 manifest 没有专属 warning-case oracle 时，跳过依赖该 oracle 的单项检查。
- `tools/integration_test_db.ps1`：为新测试窗口添加独立、仅测试库使用的市场/费用规则版本，覆盖 2023-09-12 起始交易日。
- `tests/fixtures/real_financial_window_v1.manifest.json` 与本地 `.codex/real-sample/real-financial-window-v1.json.gz`：新版本样本身份和真实输入包；旧 golden v1/v2 的文件身份未改。

## 质量门结果

| 检查 | 状态 | 结果 |
|---|---|---|
| 生产数据读访问 | PASS | 仅通过 `read_only_audit=True`，生产事务只读；未执行生产写入 |
| `ashares_test` 身份 | PASS | `ashares_test / ashares_app / TEST_FIXTURE`，迁移账本 119 |
| 来源事件与 Feature 重算 | PASS | 4,329 条来源事件；717 公司财务 Feature、1,102 D/A Feature 哈希与 Bundle 重算一致 |
| Snapshot/Dataset 生命周期 | PASS | 83 Snapshot 与 Dataset 物化、校验和冻结完成；仅为测试专用身份 |
| 四财务因子参考对账 | FAIL | 23 个 `000666.SZ` D/A 对账差异，原因已定位为旧测试 D/A 行污染 |
| 20/40/60 标签对账 | PASS | Dataset 和 Alpha Reader 共 2,070 条，均为零差异 |
| Alpha Reader 因子对账 | FAIL | 同上 23 个 D/A 差异 |
| 600699.SH 公司行为 | BLOCKED | 官方除权/权益证据仍不足，正式 Release 不可发布 |
| 2021-06 Feature 覆盖 | NOT RESOLVED | 173 行异常根因未定位 |
| Warmup 20,469 时间戳缺失 | PARTIAL | 15 条抽样 Raw 对账一致；全量逐行核查未完成 |
| Ruff | NOT RUN | `uv run ruff` 找不到可执行文件；未安装新依赖 |
| Python compileall | PASS | 本轮改动 Python 文件编译成功 |
| 定向单元测试 | PASS | 66 passed |
| 窗口集成断言 | PASS（验证预期失败状态） | 1 passed；报告中的实际 E2E 状态仍是 FAIL |
| 专属 warning-case oracle 测试 | NOT APPLICABLE | v1 manifest 不含 `quality_cases`，但一般因子质量门测试在定向单测中通过 |
| 全项目测试 | NOT RUN | 本轮仅运行相关单测与窗口集成测试 |

## 后续解除阻断的最小动作

1. 不清理或改写当前测试库旧 golden/FROZEN。用新的隔离数据库环境或与旧样本不重叠的新证券样本，重新验证修正后的 v2.2 Snapshot 选择；新结果使用新的 Release、Snapshot 和 Dataset 身份。
2. 在干净的新测试身份上验证 D/A 选择规则逐证券逐日匹配来源与质量条件，再确认 `VALIDATED + 对账失败` 会保持未冻结。
3. 对 warmup 缺时间戳集合分批做 Raw exact payload 对账，先报告可重新投影比例，再单独授权任何生产重建。
4. 继续保留 2021-06 物化异常及 `600699.SH` 未决记录；其解决前不创建生产 Foundation Release，不开展正式五年 v2.2 发布。

本报告区分了计算/集成工程结果与因子研究结果：计算路径和收益标签可以重现，但当前真实窗口 D/A Snapshot 读取不一致，且没有开展任何 Rank IC、策略收益或投资效果评价。
