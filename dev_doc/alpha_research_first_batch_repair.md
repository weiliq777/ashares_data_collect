# 阶段二第一批修复记录：财务字段诊断边界与退市敏感性

日期：2026-10-17
范围：复用已完成的 FROZEN Run `7feecaf0c249904db807b9781e8482b570ed3606d54d11795f15e02fa17e21b7`；只修离线评价汇总，不写数据库、不重建标签。

## 结果摘要

- 退市敏感性汇总已升级为 `cross-sectional-evaluation-v4`。每个假设结果只与同因子、同窗口、同受影响信号日的完整标签基准比较，并输出匹配日期、两侧股票—日期数、基准值、情景值和差值。
- 新版报告写入 `evaluations/<evaluation_id前缀>/`，旧版根目录 `report.json`、`daily_evaluation_manifest.json` 和 `performance_evaluation.json` 保持原样。
- 186 日 Run 的旧逐日评价产物已通过原有清单校验并被复用；新汇总显示 `cache_hit=true`，标签样本扫描及逐日统计耗时均为 0。20/40/60 日标签没有重建。
- 当前结果：[v4 JSON](../data/alpha_research/oos_v3_2025-08-18_2026-05-28/7feecaf0c249904db807b9781e8482b570ed3606d54d11795f15e02fa17e21b7/evaluations/18070b112cc77d10/report.json) / [v4 Markdown](../data/alpha_research/oos_v3_2025-08-18_2026-05-28/7feecaf0c249904db807b9781e8482b570ed3606d54d11795f15e02fa17e21b7/evaluations/18070b112cc77d10/report.md)。
- 财务因子的原始数值覆盖率、状态重叠和组件缺失交叉表**无法从旧样本产物恢复**。因此本轮没有放宽任何财务因子门禁，也没有修改 Feature、Snapshot、Dataset 或 FROZEN 内容。

## 财务公式与当前数据诊断

### 源码确认的计算与 PIT 时间

1. `roe_ttm` 在 [tushare_foundation_features.py](../data_collect/jobs/tushare_foundation_features.py#L375) 中按归母净利润 TTM 除以期初、期末归母权益平均值计算；[roe_quality_status](../data_collect/jobs/tushare_foundation_features.py#L535) 直接由该公式返回的 `roe_reason` 导出。
2. 同处的 `ttm_complete` 要求 consolidated、parent、OCF 三组 TTM bridge 均完整（[代码](../data_collect/jobs/tushare_foundation_features.py#L396)）。它比 ROE 公式使用的 parent TTM 净利润和权益输入更宽。
3. `revenue_growth_yoy` 是报告期本期 revenue 与上年同报告期 revenue 的同比；`profit_growth_yoy` 是本期 parent net profit 与上年同报告期 parent net profit 的同比。两者调用 `safe_growth`，要求当前值及前值有限、且前值大于零（[safe_growth](../data_collect/features/financial_v2.py#L88)、[增长计算](../data_collect/jobs/tushare_foundation_features.py#L410)）。这两个字段是报告期同比，不是滚动 TTM 同比。
4. 目前单一 `growth_quality_status` 只有在两个增长字段都有效时才为 `VALID`（[状态生成](../data_collect/jobs/tushare_foundation_features.py#L469)）。这使一个指标的缺值可以拦截另一个有值的指标。
5. Research Snapshot 的当前过滤条件是 `data_available_timestamp < research_date 次日零点（Asia/Shanghai）`，见 [research_snapshot_repository.py](../data_collect/data_access/repository/research_snapshot_repository.py#L1115)。它排除了次日及之后的数据，但允许信号日收盘后发布、次日零点前可用的数据。若策略信号时点严格定义为 T 日收盘瞬间，这个截止点偏晚；若决策在收盘后、T+1 开盘前生成，则需要由策略契约明确确认。当前样本没有逐条组件 timestamp/lineage，故本轮不能验证每项财务输入是否在实际决策时点已可见。

### 已冻结 Run 可证明的内容

旧 `samples.jsonl` 每条只保存 factor 的最终 `value/status/reason` 和标签，不保存原始 `roe_ttm` / 增长值、`ttm_complete`、质量状态原值或组件 lineage。被门禁拦截时 FactorValue 的数值就是 `null`。因此不能把这些 `null` 当作 Feature 原始字段为空。

现有报告按每个 horizon 的 1,017,302 行记录了以下**首个拒绝原因**：

| 因子 | TTM 不完整 | 质量状态非 VALID | 当前有效数 | 可解释范围 |
|---|---:|---:|---:|---|
| `roe_ttm` | 291,437 | 725,865 `ROE_QUALITY_STATUS_NOT_VALID` | 0 | 这是 extractor 的先后门禁分桶；不能说明被 TTM 门禁挡住的 ROE 值是否有限，也不能算 TTM 与质量状态的重叠。 |
| `revenue_growth_yoy` | 291,437 | 725,865 `GROWTH_QUALITY_STATUS_NOT_VALID` | 0 | 共同增长状态不能区分 revenue 自身缺值和 profit 缺值。 |
| `profit_growth_yoy` | 291,437 | 725,865 `GROWTH_QUALITY_STATUS_NOT_VALID` | 0 | 共同增长状态不能区分 profit 自身缺值和 revenue 缺值。 |

这些是按股票—信号日—horizon 记录的拒绝原因，每个 horizon 各有一套相同分母；它们不是 3 个 horizon 间去重后的唯一标的数。现有日评价和样本文件里没有字段可计算原始值的缺失、非有限异常、具体组件缺失以及多个拒绝条件的交集。

**数据质量状态：NOT RUN / 未能从现有产物验证。** 生产 Kernel 的受控 Repository 读取会建立并更新 `ops.run_manifest` 控制表记录；本任务约束禁止任何生产数据库写入，因此没有启动该读取。没有从代码推断原始值覆盖率。

## 建议的独立财务质量契约

以下规则依据真实计算公式拟定；应用到真实样本前仍需逐行 Feature 证据验证。

| 因子 | 公式与独立输入 | 接受条件 | 应分别报告的拒绝原因 |
|---|---|---|---|
| `roe_ttm` | `net_profit_parent_ttm / ((parent_equity_begin + parent_equity_end) / 2)` | parent TTM bridge 完整；期初、期末权益有限；平均权益大于零；结果有限；`roe_quality_status=VALID`；财务 timestamp 不晚于信号日截止时间且 lineage 可追溯。`ttm_complete` 保留为诊断列，不应因无关的 consolidated/OCF bridge 缺失而自动拒绝 ROE。 | parent TTM 缺失、期初/期末权益缺失、平均权益非正、结果非有限、quality status 不符、PIT timestamp/lineage 缺失或越界。 |
| `revenue_growth_yoy` | 当期报告期 revenue / 上年同报告期 revenue − 1 | 两期属于同一报告口径和报告期；数值有限；前值大于零；数值与可追溯输入一致；两个组件在信号日截止前可用。无需依赖 `ttm_complete` 或利润增长状态。 | 当前 revenue 缺失、上年同期间 revenue 缺失、前值非正、报告口径/期间不匹配、非有限结果、PIT timestamp/lineage 不合格。 |
| `profit_growth_yoy` | 当期 parent net profit / 上年同报告期 parent net profit − 1 | 两期均为同口径归母利润且报告期匹配；数值有限；前值大于零；结果可由 lineage 复算；组件在信号日截止前可用。无需依赖 `ttm_complete` 或营收增长状态。 | 当前归母利润缺失、上年同期间归母利润缺失、前值非正、口径/期间不匹配、非有限结果、PIT timestamp/lineage 不合格。 |

不在拿到原始 Observation 行前应用上述规则。尤其不允许仅删除 `ttm_complete` 后把数字直接判为有效。

## 新版本与迁移计划

旧数据保持冻结。若诊断确认需要保存逐因子原因，建议按以下顺序变更并提升身份：

1. Feature：新增独立的 revenue/profit growth quality status 与 reason；保留现有 `growth_quality_status` 兼容。将 Feature contract 从 `feature-contract-v2.0` 升到下一版本，并为每个状态写单元测试。
2. Research Snapshot：新增对应字段及公式/版本身份，提升 Research schema 版本。Snapshot 按原 PIT timestamp 和 lineage 规则生成，不回写旧 Snapshot。
3. Backtest Dataset：在 `selected_columns` 及 DatasetRepository 字段白名单中声明新字段；创建新 Dataset ID / input hash，执行质量报告及哈希校验。旧 Dataset 保留为对照。
4. Alpha：因增长指标语义从代码误标的 “TTM growth” 明确为报告期同比，提升 Factor Set Version；新评价报告再使用独立质量规则。旧 186 日统计仅作探索性对照，不能成为新规则的独立确认集。
5. 验收：先做小窗口逐证券复算与 PIT 可用时间对照，再跑新 Snapshot/Dataset 的覆盖诊断；通过后才执行完整因子评价。数据库迁移和数据重建本轮均未执行。

## 退市敏感性 v4

改动位置：[evaluation_artifacts.py](../data_collect/alpha_research/evaluation_artifacts.py#L572)、[report.py](../data_collect/alpha_research/report.py#L300)、评价身份 [contracts.py](../data_collect/alpha_research/contracts.py#L14)。

- 新版计算按 `(factor, horizon, signal_date, assumption)` 匹配每日敏感性记录与每日完整标签基准。两个均值和方向变化标志只在相同日期集合、且该统计量双方均可计算的日期上比较。
- `mean_baseline_*`、`mean_*`、delta、匹配日期数、基准与情景股票—日期数均保存在 JSON 和 Markdown。基准为完整标签有效横截面；情景为同一日期的完整标签样本加上假设退市终值的样本。由于真实终值未知，两边证券数可能不同，报告显式呈现分母。
- 对未解决退市证券，无法在不假设终值的前提下构造同证券的事实基准；因此此敏感性结果是“同日期的完整样本基准 vs. 加入假设退市样本”，不是同证券配对效应。报告明确记录两侧证券—日期数，不宣称拥有相同证券横截面。
- 无共同可计算日期时，状态为 `NO_MATCHED_VALID_DATES`，方向变化值为 `null`，不会把缺结果报告成“否”。
- 20/40/60 日三种假设终值仍只是敏感性假设，不代表退市结算事实。
- 本次复用旧版 186 日逐日数据：v4 生成 33 组比较，只有 `debt_to_assets` 的 20 日 `-100%/-50%` 假设仍有 Rank IC 符号变化；匹配日期均值分别为基准 `+0.000394`、情景约 `-0.000063`，变化接近零。分组差值没有符号翻转。
- v3 根目录报告及清单继续标为 v3；v4 报告在评价 ID 子目录中单独保存。没有覆盖旧报告。

## 质量门禁与未解决项

| 门禁 | 状态 | 证据 |
|---|---|---|
| 敏感性统计回归测试 | PASS | 同日期基准、跨日期符号误报及无匹配有效日期测试通过。 |
| Alpha 相关完整测试集 | PASS | 8 个 Alpha 测试模块，36 项通过；未执行数据库集成测试。 |
| 数据访问契约 | PASS | `check_data_access_contract.py --ci --new-only` 扫描 256 个文件，0 项违规。 |
| 纸面交易边界 | PASS | `check_paper_trading_boundary.py` 检查 311 个 Python 文件，没有 QMT 交易 API。 |
| Ruff 静态检查 | NOT RUN | 当前项目环境未安装 `ruff`，命令无法启动。 |
| 财务 Feature 原始值覆盖、异常及 overlap | NOT RUN | 旧样本产物不保存原始输入；生产 Repository 诊断会写 `ops.run_manifest`，本轮约束禁止执行。 |
| 数据库迁移 / 新 FROZEN Snapshot / Dataset | NOT RUN | 本轮没有 DB 写入，也没有改旧数据。 |
| 因子有效性 | NOT ACCEPTED | v4 只修敏感性比较口径，不改变财务门禁；因子研究仍是探索性。 |
| 阶段一回撤一致性与四组执行证据 | 未解决 | 本轮未重跑 Backtest；沿用之前的 FAIL 与 1/4 证据状态。 |
