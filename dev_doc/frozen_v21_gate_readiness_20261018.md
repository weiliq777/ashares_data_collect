# 新冻结研究数据集前置修复与验收

日期：2026-10-18<br>
范围：公司行为缺失日期范围核查、Research v2.1 财务因子输入核验、单一 Schema Dataset 契约及小窗口只读预检。

## 结论

主线 A 的工程前置修复已完成，数据库和历史产物均未写入。本次没有生成新 Snapshot/Dataset，也没有重算旧 Alpha 报告或回测。

工程上，Release Gate 现在可保留全库 `ex_date` 异常总数，同时按明确研究范围逐条归因；未分类或计数对不上的记录会阻断 Release。新建的物化 Dataset 必须使用单一 Research Snapshot Schema，并绑定同一基线及其质量、来源哈希。

数据上，四条范围内的送转股记录均取得发行人公告证据，公告称该重整转增不向原股东分配，股票价格不作除权调整。因此它们被记为“当前股息/送转模型不适用”，而不是推测 ex-date 或补造日期。31 条异常仍完整保留在全库统计中。

Research v2.1 的三个财务字段、公式、质量状态和 lineage 已进入 Snapshot/Dataset 读取契约，因子提取也没有使用全局 `ttm_complete` 作为三者共同门槛。但历史可见时间证据仍有限：24 行缺可用时间戳，其余使用 `SOURCE_ANNOUNCEMENT_ASSUMED`。此外，现存 v2.1 FROZEN Snapshot 只有约三年，不能支持五年研究；因此阶段二正式因子结论仍未验收。

## 1. 缺失 ex-date 的范围诊断

### 数据证据

只读核查结果：`std.dividend_event` 中共有 31 条 `implementation_status='实施' AND ex_date IS NULL`。31 条均能对应到 Raw 来源版本，Raw 中的 ex-date 也为空，说明不是 Standard 转换过程丢弃了已存在日期。

| 归因 | 数量 | 处理 |
|---|---:|---|
| 研究范围外：已知下游实施/权益/上市里程碑早于范围起点 | 26 | 保留全库异常记录，在范围报告中标记范围外 |
| 有发行人公告证明不发生价格除权调整 | 4 | 标记为当前模型不适用；不填 ex-date |
| 明确无现金或股份数值影响 | 1 | 单独列示为零模型影响，不隐藏记录 |
| 仍未解决 | 0 | 若未来出现，Release Gate 阻断 |
| **全库缺失 ex-date 合计** | **31** | **所有报告继续显示该总数** |

四条公告证据按来源键和 Raw 版本号精确绑定，证据版本为 `corporate-action-ex-date-evidence-v1`：

| 证券 | Raw 版本 | 公告证据 |
|---|---:|---|
| `000523.SZ` | 3062 | [发行人实施公告（2021-12-03）](https://static.cninfo.com.cn/finalpage/2021-12-03/1211779449.PDF)，称重整转增不向原股东分配且股票价格不作除权调整 |
| `000980.SZ` | 11380 | [发行人不实施除权公告（2021-12-09）](https://static.cninfo.com.cn/finalpage/2021-12-09/1211847800.PDF) |
| `002072.SZ` | 14556 | [发行人实施公告（2021-12-17）](https://static.cninfo.com.cn/finalpage/2021-12-17/1211932991.PDF)，称不向原股东分配且股票价格不作除权调整 |
| `002175.SZ` | 16986 | [发行人不实施除权公告（2021-12-21）](https://static.cninfo.com.cn/finalpage/2021-12-21/1211970923.PDF) |

公告中的登记日和新增股份上市日没有被当作 ex-date。四条记录不是“缺失日期已修复”；它们的事件性质有官方证据支持，当前按股息/送转价格调整模型判为不适用。

### 最小实现

- 新增 `data_collect/data_access/corporate_action_evidence.py`：保存精确来源键、版本号、公告标题和链接；分类器不生成或回写日期。
- `ReleaseRepository.quality_gate()` 保留原全库计数 `implementation_without_ex_date`。未指定研究范围时，旧的 fail-closed 行为不变。
- Foundation Release 校验传入五年研究范围；报告同时输出全库总数、逐记录分类和证据版本。存在 `IN_SCOPE_UNRESOLVED`、`UNRESOLVED` 或全库计数与逐行分类数量不一致时失败。
- 查询 `WorkScope` 明确标注读取的是全库缺失记录总体，研究范围只用于归因，不伪装成 SQL 日期过滤。
- 证据仅存在于版本控制的代码中；没有更新 Raw、Standard、Release 或历史 FROZEN 记录。

因此，当前 31 条的范围诊断可通过；全库无范围的通用质量检查仍会报告 31 条并阻断。这保留了异常可见性，也避免把范围外异常静默删除。

## 2. 财务因子公式、状态与 PIT

### 公式与输入

源码 `data_collect/jobs/tushare_foundation_features.py` 中的计算口径为：

- `roe_ttm`：TTM 归母净利润除以期初、期末归母权益平均值；TTM 由匹配报告期、报表类型和公司类型的财报组件桥接。
- `revenue_growth_yoy`：本报告期营业收入相对上年同报告期营业收入的同比变化。它是报告期同比，不是 TTM 同比。
- `profit_growth_yoy`：本报告期归母净利润相对上年同报告期归母净利润的同比变化。当前 `safe_growth` 要求比较基数为正。

现金流 TTM 是现金流质量条件的输入，不是这三个因子的共同直接输入。`ttm_complete` 汇总了合并利润、归母利润、经营现金流三套 TTM 组件完整性，不适合作为 ROE 或两个报告期同比因子的统一质量门槛。

当前 Alpha 提取实现已按因子使用质量字段：ROE 使用 `roe_quality_status`；两个增长因子分别检查各自数值，并接受 `growth_quality_status` 的 `VALID` 或 `INSUFFICIENT_HISTORY`。后一状态下，存在值的增长因子仍可单独使用，并附带 `SIBLING_GROWTH_FACTOR_INSUFFICIENT` 原因。没有放宽 PIT 或质量条件，也没有修改公式。

### v2.1 Snapshot 覆盖证据

在只读检查的最新 FROZEN v2.1 Snapshot（2026-08-21，5,550 条证券观测）中：

| 检查项 | 数量 | 说明 |
|---|---:|---|
| `roe_ttm` 有限数值且 ROE 状态有效 | 5,218 | 因子值本身可用，不代表 PIT 证据全部通过 |
| `revenue_growth_yoy` 有限值且状态可接受 | 5,354 | 与利润增长分别读取 |
| `profit_growth_yoy` 有限值且状态可接受 | 4,086 | 与营收增长分别读取 |
| `ttm_complete` 为真 | 3,211 | 不能拿来作为以上三个因子的统一门槛 |
| 财务组件 lineage 存在 | 5,550 | 有来源组件追溯信息 |
| `SOURCE_ANNOUNCEMENT_ASSUMED` 且有可用时间戳 | 5,526 | 时间戳早于信号日收盘的行可通过现有因子 PIT 检查 |
| 缺少可用时间戳 | 24 | 现有因子提取会拒绝，必须保留为缺失/无效 |

`SOURCE_ANNOUNCEMENT_ASSUMED` 是基于公告时间的假设证据，不等同于已经独立证明的精确公开可见时间。当前结果只能说明系统按所存时间戳执行 PIT 检查，不能宣称所有历史财务值都具有官方时间戳级别的 PIT 证明。

三个财务因子和上述状态、时间戳、版本、lineage 已列入 Research Snapshot 的财务字段契约与 Dataset 字段选择。没有发现需要修改已冻结数据的原因；后续若修复时间证据或质量状态，必须产出新 Feature/Snapshot/Dataset 版本。

## 3. 单一 Schema 与 Dataset 身份

`data_collect/jobs/backtest_dataset.py` 的物化 Dataset 创建、验证和冻结路径现在都要求：

1. 每条 Snapshot 都声明受支持的 `schema_version`；
2. Dataset 内只能出现一个 Research Snapshot Schema；
3. 每条 Snapshot 的 `data_baseline_tag` 与 Dataset 基线一致；
4. 每条 Snapshot 的基线质量报告哈希和 Raw 来源清单哈希与所绑定基线一致。

创建时把实际唯一 Schema 及 `single_schema_version_per_materialized_dataset` 写入 availability rules。复合 Dataset 继续通过既有组件兼容性契约检查，读取历史组件，不复制或改写 FROZEN 事实。

数据库只读清点显示：

- `research-v2.1`：710 个 FROZEN Snapshot，日期为 2023-09-12 至 2026-08-21。
- 同一基线下 `research-v2.0`：1,226 个 FROZEN Snapshot，日期为 2021-08-02 至 2026-08-21。
- v2.1 区间应有 712 个 SSE 开市日，缺少 2024-01-31 和 2024-02-01 两个 Snapshot。
- 不能把 v2.0 的较早 Snapshot 与 v2.1 混合，冒充一个完整五年、单 Schema 数据集。

## 4. 小窗口只读预检

只读检查了 `2023-09-12` 至 `2023-10-20` 的 23 个连续 SSE 开市日：23 日均有 FROZEN v2.1 Snapshot，且只有一个 Schema 和同一基线身份。因此该窗口适合作为下一次测试环境 Dataset 构建的候选窗口。

这次没有实际调用 `create_dataset`，因为它会写入 Dataset manifest 和成员；也没有构建或冻结小窗口 Dataset。只读预检只证明 Snapshot 日期覆盖及身份约束，不能替代执行行情、复权公司行为、Dataset 物化哈希和标签成熟度验收。若此后要研究 20/40/60 日收益，Dataset 的执行行情还必须覆盖评价期之后至少 60 个交易日及必要的公司行为尾部。

## 5. 验证结果与阻断状态

| 类别 | 状态 | 证据 |
|---|---|---|
| 新代码回归测试 | PASS | 相关 Release、因子、Snapshot、Dataset 测试 140 passed；Dataset 流式/替换/组合/契约测试 36 passed |
| Data Access Contract | PASS | 扫描 256 个文件，0 violations |
| Paper Trading Boundary | PASS | 扫描 312 个可执行 Python 文件，未发现 QMT 交易 API |
| Ruff | NOT RUN | 当前项目虚拟环境没有 `ruff` 可执行文件；未安装或下载额外依赖 |
| 公司行为范围分类 | PASS（当前 31 条） | 26 范围外、4 有官方无除权证据、1 无模型现金/股份影响、0 未解决；全库总数仍是 31 |
| 财务字段公式和 lineage | PASS（源码与字段存在） | 三项数值和组件 lineage 已进入 v2.1 Snapshot/Dataset 路径 |
| 财务 PIT 证明 | PARTIAL | 24 行缺时间戳；其余主要依赖 `SOURCE_ANNOUNCEMENT_ASSUMED` |
| 五年单一 Schema 覆盖 | FAIL / BLOCKED | v2.1 从 2023-09 才有数据；且中间缺两日，不能以 v2.0 补齐 |
| 新 Dataset 小窗口构建 | NOT RUN | 本轮只读预检；未写 Dataset manifest 或物化表 |
| 因子有效性结论 | NOT ESTABLISHED | 工程字段可用不等于有 Alpha；旧 186 日结果仍是探索性数据 |
| 阶段一回撤/四场景复现 | NOT RECHECKED | 沿用既有报告状态，不标记为已解决 |

## 6. 后续新冻结集计划

1. 先在测试数据库或明确授权的写入环境中，用上述 23 日窗口实际创建新的临时 Dataset；验证 Snapshot schema、基线哈希、执行行情覆盖、公司行为哈希、数据访问与回滚路径。不要覆盖现有 FROZEN 数据。
2. 单独查明并补齐 2024-01-31、2024-02-01 两日的 v2.1 Snapshot。修复后的产物使用新 Snapshot 身份和来源证据。
3. 如目标仍是五年因子研究，按 `research-v2.1` 契约重建 2021-08 起缺少的 v2.1 历史 Snapshot；禁止用 v2.0 数据混入新 Dataset。
4. 对 24 条无时间戳财务记录及假设公告时间记录，决定可补的 PIT 证据范围；无法证明的值继续标记无效，不回填未来修订值。
5. Dataset 全窗口完成 20/40/60 标签成熟度、逐证券未来行情覆盖、公司行为和退市处理审计后，才运行新的 Alpha 研究。新结果使用新 Dataset ID 和研究 Run 身份，旧报告只作探索性对照。

### 变更文件

- `data_collect/data_access/corporate_action_evidence.py`
- `data_collect/data_access/repository/release_repository.py`
- `data_collect/jobs/backtest_dataset.py`
- `tests/test_corporate_action_ex_date_scope.py`
- `tests/test_backtest_contract.py`

本次不修改正式策略、不执行策略审批、不做实验回测数据库迁移，也不修改用户已有的其他工作区文件。
