# `debt_to_assets` 黄金样本差异来源归因

日期：2026-10-18

范围：`688426.SH`、`688448.SH`，信号日 2023-09-22

证据源：生产 `ashares` 的 `ashares_app` 应用角色只读事务、`real-golden-v1` 不可变样本包及当前源码。

## 结论

1. 两个旧冻结值都能在当前 `std.fina_indicator_standard`、`pit.financial_asof` 和 `feature.company_financial_v1` 中找到相同数值；资产负债表也能独立复算出相同的四位小数值。它们不是无来源的随机数字。
2. 旧 FROZEN 观察行本身没有保存 `report_date`、来源键、版本、载荷哈希、财务可用时间戳或 vintage 证据。当前源记录是在 2026 年采集，并用公告日推定历史可见日。因此可以证明“当前重建的 PIT/Feature 与旧值相符”，不能证明“旧冻结行当时确实选择了这条源版本”。旧参考的原始血缘仍为**不可验证**。
3. `feature.company_financial_v2` 没有这两只股票的记录，直接原因是截至信号日没有对应的 Raw/Standard 收入报表；`_build_feature()` 在缺少收入事件时提前返回。资产负债表记录本身存在且质量为 `VALID`。不过 Snapshot 的 `debt_to_assets` 当前由 v1 Feature 提供，v2 enrichment 不写这个字段；本次黄金样本测试导入器因旧观察行 `report_date` 为空而没有导入对应 v1 Feature，所以测试 Snapshot 的 `MISSING` 是夹具未装入 v1 行，不足以证明生产 Snapshot 丢失了该值。
4. 确认并修复了 v2 计算单位错误：`safe_margin()` 返回小数比例，而字段与 `fina_indicator`、v1 Feature 和策略阈值采用百分数。现按百分数乘 100 并保留四位小数；没有修改 `real-golden-v1` manifest、bundle 或参考值。

## 逐证券证据

| 证据项 | `688426.SH` | `688448.SH` |
|---|---|---|
| 旧 FROZEN 观察，2023-09-22 | `debt_to_assets=8.619`，`quality_status=MISSING_SOURCE`；`report_date`、公告日、record version、source key、payload hash、vintage、source observed time 和 `data_available_timestamp` 全为空；仅 `financial_version_available_date=2023-08-31` | `debt_to_assets=27.9137`，`quality_status=MISSING_SOURCE`；上述财报身份、哈希及时间证据全为空；仅 `financial_version_available_date=2023-08-30` |
| Raw `fina_indicator` | `end_date=2023-06-30`，`ann_date=2023-08-30`，载荷值 `8.619`；Raw `fetched_at=2026-08-02 20:06:20.921233` | `end_date=2023-06-30`，`ann_date=2023-08-29`，载荷值 `27.9137`；Raw `fetched_at=2026-08-02 20:06:32.245911` |
| Standard `fina_indicator` | `report_date=2023-06-30`；key `688426.SH:20230830:20230630:::`；version `103965`；hash `85ab4f46d68b290852234880b6ae8d011d350619b9daffee5153e3f9a5f72072`；quality `VALID`；version date `2023-08-31`；`SOURCE_ANNOUNCEMENT_ASSUMED`；data available `2023-08-30 16:00:00+00` | `report_date=2023-06-30`；key `688448.SH:20230829:20230630:::`；version `104145`；hash `c927f9dd846e5065d5f0f6433c5ea18bf3f22c3a18f9bf7fc8d1acfb1093e1b1`；quality `VALID`；version date `2023-08-30`；`SOURCE_ANNOUNCEMENT_ASSUMED`；data available `2023-08-29 16:00:00+00` |
| Standard 资产负债表 | `report_type=1`、`comp_type=1`、`end_type=2`、`update_flag=0`；负债 `165,171,823.69`、资产 `1,916,363,186.94`；key `688426.SH:2023-06-30:1:1:2`；hash `7badb9a089bee33c820abad1cfe31f13a7dac5ab9422934c0deff74358314b58`；record versions `197192`、`503282`，同 key/hash/数值；quality `VALID`；data available `2023-08-30 16:00:00+00` | `report_type=1`、`comp_type=1`、`end_type=2`、`update_flag=0`；负债 `365,572,388.12`、资产 `1,309,653,787.13`；key `688448.SH:2023-06-30:1:1:2`；hash `24c7d93494e65e1bad10a947f2fca1950ffb0995306cfa1114068a2736177ae0`；record versions `167991`、`474081`，同 key/hash/数值；quality `VALID`；data available `2023-08-29 16:00:00+00` |
| PIT `financial_asof`，decision date 2023-09-22 | `report_date=2023-06-30`；公告日 `2023-08-30`；record version `103965`；source key/hash 与上方 Standard 指标相同；值 `8.619`；quality `VALID`；version date `2023-08-31` | `report_date=2023-06-30`；公告日 `2023-08-29`；record version `104145`；source key/hash 与上方 Standard 指标相同；值 `27.9137`；quality `VALID`；version date `2023-08-30` |
| 当前 Feature v1 | `report_date=2023-06-30`，值 `8.619`，`version_available_date=2023-08-31`，record version `1`，quality `WARNING`；feature key `combined:0acd8a040c86d87cb12d11da102db3f30de0ed2f31cb2355ea970ea68577bf50` | `report_date=2023-06-30`，值 `27.9137`，`version_available_date=2023-08-30`，record version `1`，quality `WARNING`；feature key `combined:c6db0aee2e013c2eac2cc3e2ca4964af5c0ddf5e152e40fbebc4032f0376e612` |
| 当前 Feature v2，截至信号日 | 无记录 | 无记录 |

Standard 指标行的 `report_type`、`comp_type`、`linked_comp_type`、`end_type` 和 `update_flag` 均为空；对应 Raw 指标载荷也没有这些字段。两条 Standard 指标均另有一条迁移/兼容版本：`688426.SH` version `219617`、`688448.SH` version `219797`。它们的值、公告日、availability、哈希及 quality 与表中主版本相同，但 source key 为仅证券和报告期组成的兼容键。资产负债表也各有一条具有相同 key、hash 和数值的后续版本。重复版本没有改变数值，但说明仅凭旧快照日期无法还原它选用的物理版本。

Raw 财务表只保存业务键、载荷和抓取时间，不提供 `record_version` / `payload_hash` 列；相应 Standard 层才有版本及哈希。`pit.financial_asof` 和 `feature.company_financial_v1` 均没有 `data_available_timestamp` 列，保存的是 `version_available_date`。Feature v1 也不存 `report_type` / `comp_type`；其 `combined:` key/hash 是组合 Feature 身份，不能替代具体 Standard source key。

完整版本字段补充：

| 层 | `688426.SH` | `688448.SH` |
|---|---|---|
| `fina_indicator_standard` 主版本 | `record_version=103965`；`payload_hash=85ab4f46d68b290852234880b6ae8d011d350619b9daffee5153e3f9a5f72072`；`version_available_date=2023-08-31`；`data_available_timestamp=2023-08-30 16:00:00+00`；`source_observed_at=2026-08-02 20:06:20.921233`；`vintage_quality=SOURCE_ANNOUNCEMENT_ASSUMED`；`quality_status=VALID` | `record_version=104145`；`payload_hash=c927f9dd846e5065d5f0f6433c5ea18bf3f22c3a18f9bf7fc8d1acfb1093e1b1`；`version_available_date=2023-08-30`；`data_available_timestamp=2023-08-29 16:00:00+00`；`source_observed_at=2026-08-02 20:06:32.245911`；`vintage_quality=SOURCE_ANNOUNCEMENT_ASSUMED`；`quality_status=VALID` |
| `financial_balance_sheet_standard` 版本 | versions `197192` / `503282`；两者 `version_available_date=2023-08-31`、`data_available_timestamp=2023-08-30 16:00:00+00`、`vintage_quality=SOURCE_ANNOUNCEMENT_ASSUMED`、`quality_status=VALID`；`source_observed_at` 分别为 `2026-08-03 15:01:45.709943` / `2026-08-08 20:37:29.801338` | versions `167991` / `474081`；两者 `version_available_date=2023-08-30`、`data_available_timestamp=2023-08-29 16:00:00+00`、`vintage_quality=SOURCE_ANNOUNCEMENT_ASSUMED`、`quality_status=VALID`；`source_observed_at` 分别为 `2026-08-03 15:00:34.854277` / `2026-08-08 20:36:10.610082` |
| `pit.financial_asof` | `record_version=103965`；source key/hash 指向指标 Standard 主版本；`report_type`、`comp_type` 为 NULL；该 PIT 表没有 `data_available_timestamp` 列，保存 `version_available_date=2023-08-31`、`vintage_quality=SOURCE_ANNOUNCEMENT_ASSUMED`、`quality_status=VALID` | `record_version=104145`；source key/hash 指向指标 Standard 主版本；`report_type`、`comp_type` 为 NULL；该 PIT 表没有 `data_available_timestamp` 列，保存 `version_available_date=2023-08-30`、`vintage_quality=SOURCE_ANNOUNCEMENT_ASSUMED`、`quality_status=VALID` |
| `company_financial_v1` | `record_version=1`；source key `combined:0acd8a040c86d87cb12d11da102db3f30de0ed2f31cb2355ea970ea68577bf50`；payload hash `0acd8a040c86d87cb12d11da102db3f30de0ed2f31cb2355ea970ea68577bf50`；`version_available_date=2023-08-31`；`vintage_quality=SOURCE_ANNOUNCEMENT_ASSUMED`；`source_observed_at=2026-08-02 20:06:20.921233`；`quality_status=WARNING` | `record_version=1`；source key `combined:c6db0aee2e013c2eac2cc3e2ca4964af5c0ddf5e152e40fbebc4032f0376e612`；payload hash `c6db0aee2e013c2eac2cc3e2ca4964af5c0ddf5e152e40fbebc4032f0376e612`；`version_available_date=2023-08-30`；`vintage_quality=SOURCE_ANNOUNCEMENT_ASSUMED`；`source_observed_at=2026-08-02 20:06:32.245911`；`quality_status=WARNING` |

所有当前 Raw 的 `fetched_at`、Standard/PIT/Feature 的 `source_observed_at` 都晚于 2023-09-22；PIT 资格依赖 `SOURCE_ANNOUNCEMENT_ASSUMED`，即按公告日推定历史可见性，而非 2023 年当时留存的仓库观测。两只股票的 PIT / Feature 值与公告日支持该历史时点，但不构成当时源载荷版本的直接归档证明。

## 单位与公式核验

现有源值和策略阈值都采用百分数：`fina_indicator` 返回 `8.619`、`27.9137`，传统策略的 `debt_to_assets_max` 默认值为 `50.0`，QualityTrend 阈值也以 0–100 范围配置。

资产负债表计算如下：

| 证券 | `total_liab / total_assets` | 百分数，四位小数 | `fina_indicator` |
|---|---:|---:|---:|
| `688426.SH` | `0.0861902508` | `8.6190` | `8.619` |
| `688448.SH` | `0.2791366632` | `27.9137` | `27.9137` |

原 v2 调用 `safe_margin(total_liab, total_assets)`，得到前一列的小数比例并直接写入百分数字段，产生 100 倍单位偏差。本次改为乘 `100` 并按现有供应商字段精度保留四位；`safe_margin()` 本身不变，利润率等比例仍保持小数口径。单测使用这两个真实资产负债表事实验证输出值。Feature v2 对这些证券仍不产生行，因为缺少收入输入；本修复只纠正有输出时的单位。

## v2 缺行与 E2E 差异归因

- 只读查询中，这两只证券截至 2023-09-22 在 `tushare_income_raw` 和 `std.financial_income_standard` 均无记录；相应 balance 与 fina indicator Raw/Standard 行存在。
- `data_collect/jobs/tushare_foundation_features.py` 的 `_build_feature()` 先要求同报告期 `income`，缺少时直接返回 `None`。所以 `calculate_features()` 不会产出完整的 v2 财务行。该行为解释了 v2 无行；它不代表 D/A 的资产负债表输入不存在。
- `ResearchSnapshotRepository.stock_snapshot_sql()` 从 `feature.company_financial_v1` 取 `debt_to_assets`；Foundation-v2 enrichment 不覆盖该字段。生产路径当前有 v1 Feature 值可读，但整体 v1 行状态为 `WARNING`，其来源是组合 Feature，不是逐因子独立状态。
- `tests/integration_fixtures/real_golden_sample.py` 仅从旧观察行重建 v1 Feature，且跳过 `report_date is None` 的行。两条旧观察正是这种情况，因此隔离 E2E 库没有相应 v1 Feature，导致测试 Snapshot 为 `MISSING`。这属于夹具来源缺项，不足以证明真实生产 Snapshot 当前会丢失该值。
- 当前 `debt_to_assets` Alpha 因子只读取数值字段，没有单独检查 v1 组合 `WARNING`。本次遵照范围没有改变 PIT 或质量门禁。

## 修改与验收

- 修正 [tushare_foundation_features.py](../data_collect/jobs/tushare_foundation_features.py)：将余额表比率转换成百分数并保留四位小数。
- 在 [test_financial_feature_v2.py](../tests/test_financial_feature_v2.py) 添加两个真实比例的参数化回归，确保 8.619 和 27.9137 不再缩小 100 倍。
- 未修改 v1 manifest、bundle、factor reference、Raw、Standard、PIT、Feature 数据库记录、FROZEN、PIT 门禁、策略、审批或生产数据库。
- v1 旧观察行的来源身份仍不可验证；真实黄金样本因子逐值验收仍为 **FAIL**，不能把本次代码测试 PASS 报成研究验收 PASS。若以后要绑定当前 Standard/PIT 证据并更改黄金样本参考语义，另建 `real-golden-v2`，保留 v1 原样。

### 检查结果

| 检查 | 结果 |
|---|---|
| 财务 Feature / Alpha 因子 / QualityTrend 策略相关测试 | PASS，69 项（财务 Feature 文件单独为 47 项） |
| Data Access Contract | PASS，257 个文件，0 项违规 |
| Paper-trading boundary | PASS，314 个可执行 Python 文件 |
| Ruff | NOT RUN：项目虚拟环境没有 Ruff；临时 `uv run --with ruff` 启动受 Windows PE trampoline 权限拒绝 |
| 生产数据写入 / 重建 / 新 Release | NOT RUN；本次仅只读查询 |
