# 真实 A 股黄金样本数据对账

日期：2026-10-18<br>
项目：`ashares_data_collect`<br>
范围：真实历史数据只读审计；有限真实证券样本；未创建正式 Release、Snapshot 或 Dataset。

## 验收结论

| 检查项 | 状态 | 结论 |
| --- | --- | --- |
| 财务公式与样本值重算 | PASS | 有效 ROE TTM、营收同比、利润同比均与当前 PIT/Feature 计算值相符。 |
| 财务来源版本、哈希与可见时间 | PASS（样本范围） | 701/701 条财务血缘与 Standard 来源行及哈希匹配；未发现未来可见时间。 |
| 独立财务来源质量门禁 | PASS（代码修复与样本复核） | 质量状态按因子实际依赖组件判断；无关现金流警告不再误伤营收/利润增长。 |
| 公司行为与 Raw 身份对账 | PASS（8 条已冻结事件） | 8/8 条与 `tushare_dividend_raw` 的记录身份、载荷哈希和数值匹配；不代表已核验官方公告准确性。 |
| 20/40/60 日标签参考值 | PASS（30 证券 × 3 窗口） | 90 条标签状态、原因和完整收益与独立参考计算一致。 |
| 真实样本进入 `ashares_test` 后的 Snapshot → Dataset → Alpha Reader | NOT RUN | 现有隔离集成测试只用合成样本；没有将真实样本冒充正式 Release，也没有绕过完整生命周期直接构造 FROZEN 记录。 |
| 全市场、全历史财务 PIT 与公司行为完整性 | NOT RUN | 本次是有限样本对账，不能外推为全市场五年验收。 |
| 因子预测有效性或策略收益 | N/A | 本次验证计算与来源正确性，不评价投资有效性。 |

**当前不能据此发布新的正式研究基线。** 原诊断 Dataset `f8760f3c-342c-49d5-9fcd-a989cc09955f` 使用的旧基线已 superseded，且不满足新版正式公司行为审计要求。代码中的来源质量契约已提升版本，必须重新构建 Release、Snapshot 和 Dataset 后才能在冻结研究链路采用。

## 样本范围与可追溯性

- 研究信号日：2023-09-22；财务与快照对账使用同一研究时点。
- 价格与标签检查区间：2023-09-22 至 2024-04-01，共 124 个冻结交易日。
- 样本规模：30 只证券，18 只基础样本及 12 个边界案例；数据抽取约 1,411,923 字节（约 1.35 MiB），为内存核查体积，未持久化到测试数据库。
- 使用的历史诊断身份：Research Snapshot `c23f043f-074e-4f09-a12a-75dccaea5e10`；其上游 Dataset 为旧诊断 Dataset `f8760f3c-342c-49d5-9fcd-a989cc09955f`。这两个身份仅用于定位旧数据证据，不作为新的合格研究基线。
- 本轮复核中重点识别的边界证券：`000666.SZ`（观察期内退市）、`002800.SZ`（停牌路径）、`300965.SZ`（信号日收盘缺失）、`002721.SZ`（40/60 日路径停牌）、`000046.SZ`（2023-12-19 近退市标签案例）。

**可追溯性缺口：** 原抽样运行没有把全部 30 个 `ts_code`、抽样规则、依赖报告期清单和文件哈希保存为机器可读 manifest。现有运行摘要能确认数量、窗口和上述边界样本，但不能恢复完整 30 只名单。因此这次“样本数和计算复核”有证据，完整黄金样本清单持久化验收为 **FAIL**。下轮必须先产出不可变抽样 manifest，再进行真实样本隔离库导入或扩大抽样。

## 财务数据对账

### 来源与 PIT

- 只读核查覆盖样本 2015-01-01 至 2023-09-22 的财报来源，读取 4,389 条 Standard 财务事件，计算 776 条 Feature 结果。
- 30 条快照观测中，29 条与相应 Feature 的 `(ts_code, report_date, data_available_timestamp)` 身份匹配；`688426.SH` 没有 Feature 行、来源时间或 lineage，保持缺失，未补造。
- 701/701 条被核查的 lineage 条目匹配 Standard 源记录及 payload hash；没有观察到来源键/版本/哈希不匹配或晚于信号日的可用时间。
- 在 2,116 个有效公告业务组中，91 组存在同一有效公告日期的不同 payload。Standard 将这些歧义记录标为 `WARNING`；这类来源质量必须由下游门禁保留，不能只检查是否有数值。
- 旧实现的 `FinancialRepository.load_events()` 没有返回 `quality_status`，Feature lineage 也没有记录组件质量，因而 Alpha Reader 无法按因子依赖审查来源警告。本次已补齐这条链路。

### 因子值与可用性

以新代码在内存中重算、并按各因子依赖组件的来源质量过滤后，29 条有 Feature 的观测结果如下：

| 因子 | VALID | MISSING | INVALID | 对账结论 |
| --- | ---: | ---: | ---: | --- |
| `roe_ttm` | 25 | 0 | 4 | 有效值与父公司 TTM 利润 / 平均父公司权益口径一致；4 条输入质量或因子状态不合格，拒绝使用。 |
| `revenue_growth_yoy` | 28 | 1 | 0 | 有效值与同报告期营业收入同比公式一致；增长因子不依赖现金流 TTM 完整性。 |
| `profit_growth_yoy` | 20 | 9 | 0 | 有效值与同报告期归母净利润同比公式一致；缺失保持 MISSING。 |

`688426.SH` 不在上述 29 条中，因为没有匹配 Feature。三个因子公式均未改变；修复的是逐组件质量证据与因子门禁。无关组件的警告不会阻断因子：例如现金流来源为 `WARNING` 时，不会单独使由收入组成的同比因子失效；相关收入或权益组件为 `WARNING` 时，对应因子会明确返回 `FINANCIAL_SOURCE_COMPONENT_QUALITY_NOT_VALID`。

## 行情、公司行为与未来标签

### 信号日行情

- 2023-09-22 冻结执行行情有 30 条；相应的旧版 `tushare_daily_raw` 和 `std.daily_kline` 各有 28 条。
- 28/28 条已有 Raw 日行情与冻结 OHLC 数值匹配；成交量按 `vol × 100`、成交额按 `amount × 1000` 转换后匹配。
- `000666.SZ` 和 `300965.SZ` 缺少相应 Raw 日行情；冻结记录标为 `WARNING`、停牌/无价格且开收盘为空。未把缺失价格补成数值，也没有伪造 Raw 版本哈希。
- 该旧日行情来源无法提供不可变记录版本与内容哈希，因此本项只能证明数值与当时存储值一致，不能证明正式冻结来源身份满足新版契约。

### 公司行为

- 冻结数据中的 8 条公司行为均与 `tushare_dividend_raw.record_id`、`payload_hash`、业务字段和数值对应。
- 标签构建按实施状态和来源记录版本选择公司行为，完整样本标签未发现重复复权或双计分红。
- 这只是数据库内 Raw ↔ Frozen 对账，不是对交易所/上市公司原始公告的外部核验。全局 31 条缺 `ex_date` 记录也没有在本批重新审查；范围内未知除权日仍是正式 Release 阻断项，不得因这 8 条匹配而视为解决。

### 未来收益标签

| 窗口 | COMPLETE | INCOMPLETE | 不完整原因摘要 |
| --- | ---: | ---: | --- |
| 20 交易日 | 27 | 3 | 退市、观察路径停牌、信号日收盘缺价 |
| 40 交易日 | 26 | 4 | 上述问题及新增停牌路径 |
| 60 交易日 | 26 | 4 | 与 40 日相同的有效性数量 |
| 合计 | 79 | 11 | 不完整标签没有被填成 0，也未静默删除 |

90 条标签的状态、失败原因及可计算收益均与独立参考计算一致。79 条完整收益与 `price_daily_v2` 的 point-in-time `asof_close` 口径一致。独立近退市样本 `000046.SZ`（信号日 2023-12-19）在 20 日因 `SUSPENDED_PATH` 不完整；40/60 日因 `DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE` 不完整。未知退市终值继续保持未知，没有假设为 -100%、-50%、0% 或最后交易价。

标签结果只说明当前选定样本下实现与参考口径一致，不代表退市/停牌总体选择偏差已经消除，也不表示该旧 Dataset 满足正式标签冻结要求。

## 代码变更

1. `FinancialRepository` 的四类财务事件读取都返回标准化 `quality_status`。
2. Feature lineage 增加版本化质量证据、来源组件质量和每个源组件的 `used_by` 因子列表；ROE 与两个报告期同比因子分别记录自身计算依赖。
3. Alpha 财务因子拒绝缺少新版质量证据的旧 lineage，并且仅审查该因子实际依赖组件的来源质量。
4. `FACTOR_SET_VERSION` 从 `frozen-factor-set-v2` 提升至 `frozen-factor-set-v3`，来源质量证据版本为 `financial-source-quality-v1`。旧 FROZEN 记录与旧报告未改写；旧 lineage 将按新契约 fail-closed。
5. 新增回归测试覆盖缺质量证据、相关/不相关组件警告、增长与 ROE 的独立门禁；没有改写三个因子的公式或正式策略。

主要实现位置：

- [contracts.py](../data_collect/alpha_research/contracts.py)：因子集与来源质量证据版本。
- [financial_repository.py](../data_collect/data_access/repository/financial_repository.py)：财务来源质量读取。
- [tushare_foundation_features.py](../data_collect/jobs/tushare_foundation_features.py)：lineage 和独立依赖质量。
- [factors.py](../data_collect/alpha_research/factors.py)：Alpha 因子来源质量门禁。
- [test_financial_feature_v2.py](../tests/test_financial_feature_v2.py)：公式依赖和来源质量回归样本。
- [test_alpha_research_factors.py](../tests/test_alpha_research_factors.py)：Alpha 因子 fail-closed 测试。

## 测试与环境状态

| 检查 | 状态 | 结果 |
| --- | --- | --- |
| 当前财务/Alpha 定向测试 | PASS | 57 passed，1.30 秒。 |
| 本轮此前的扩展 Research/财务测试 | PASS | 195 passed，2.18 秒。 |
| 隔离数据库迁移/数据库测试 | PASS | 118 个迁移已应用；数据库集成测试 7 passed。 |
| 合成 Snapshot → Dataset → Alpha Reader E2E | PASS（合成数据） | 1 passed；不是真实数据端到端证据。 |
| DataAccess 契约检查 | PASS | 扫描 256 个文件，0 项违规。 |
| 纸面交易边界检查 | PASS | 扫描 312 个 Python 文件，无 QMT 交易 API。 |
| `git diff --check` | PASS | 无空白错误；CRLF/LF 提示仅涉及既有文档文件。 |
| Ruff | FAIL（4 项既有规则告警） | 已修复本次触及文件的导入排序和未使用导入；剩余 `factors.py` 的时间戳解析告警 1 项，以及 `financial_repository.py` 的可变类属性/宽泛异常处理 3 项。没有为了通过静态检查改变其运行语义。 |
| `ashares_test` 当前状态 | PASS（环境健康） | PostgreSQL 16 健康；84,098,071 字节；416 张用户表、120 个触发器、118 个迁移。没有导入本次真实样本。 |
| 生产写入 | N/A | 未执行生产写入、Release、Snapshot、Dataset 创建或数据重建。 |

`ashares_test` 的已有 E2E 仍然是 `TEST_FIXTURE` 合成证券和数据。没有通过直接插入/构造 FROZEN 身份绕过正常生命周期，也没有把这次数据库内存抽样复制进测试库。因此真实数据下游 E2E 必须另行完成。

## 正式研究前的阻断项与下一步

### P0：真实样本复现与端到端链路

1. 持久化完整黄金样本 manifest：30 只证券、抽样规则、信号日、相关财报期及所有行情/公司行为依赖、源身份和内容哈希。之后才能把“样本清单”从 FAIL 变成 PASS。
2. 在 `ashares_test` 增加受限真实数据导入器，强制标记 `REAL_SAMPLE_TEST_ONLY`，保持环境 fail-closed；经测试从真实源身份构建测试 Snapshot/Dataset 并由 Alpha Reader 读取。不得伪装正式全市场 Release。
3. 先解决生产应用账号对 `backtest.dataset_component` 的只读权限缺口，或提供符合 DataAccessKernel 契约的安全 Repository 读取路径；不使用超级用户或权限绕过获取生产数据。

### P1：发布新的正式研究基线

1. 对 31 条缺 `ex_date` 记录继续做范围化分类；有范围内未知公司行为时保持阻断，官方日期证据采用追加版本，不修改 Raw 历史事实。
2. 对旧版价格数据补齐不可变来源版本/哈希证据，重新核对涨跌停、停牌和公司行为协调后的冻结价格。
3. 建立新的有效 Release，以新版质量证据重建 v2.1 Snapshot，再生成同一 Schema 的新 Dataset。保留 superseded 旧身份和报告。
4. 新数据身份通过质量门后，扩大覆盖并重新执行 20/40/60 因子研究。该步骤才可评价正式研究结果；本次没有判断因子有效性。

## 边界声明

- 生产数据以只读方式检查；测试数据库仅查询状态，没有导入真实证券数据。
- 没有修改 Raw、Standard、既有 PIT/Feature 记录、FROZEN Snapshot、Dataset、历史回测或策略审批。
- 代码更改与测试只证明新增质量证据门禁的行为正确；真实样本 PASS 只覆盖本报告列出的有限样本；全市场数据质量、正式 Release 资格、Alpha 有效性和策略收益均未验收。
- 当前工作区存在其它未提交的历史项目变更；本轮未提交、未推送，也未清理或重置这些文件。
