# Alpha 因子研究设计

## 目标与边界

在现有 FROZEN Backtest Dataset 上，对历史 PIT 快照中的既有特征评估未来 20/40/60 个交易日收益。复用 `DatasetRepository`、DataAccessKernel、冻结交易日历和行情/公司行为事实；不重建 PIT、Feature 或回测引擎，不修改正式策略、冻结数据或历史回测结果，也不引入机器学习和自动交易。

工程结果与历史投资结论分开。Stage 1 的逐证券行情覆盖、回撤一致性和资金场景执行证据仍未通过时，报告只能标为探索性输出，不能给出正式因子有效性结论；策略收益增量状态必须是 `NOT RUN`。

## 关键数据契约

* 只接受状态为 `FROZEN` 的 Dataset 和 `research-v2.0` / `research-v2.1` 冻结 Snapshot。Alpha Run 必须使用单一 Snapshot Schema；财务因子需要 `research-v2.1` 中的质量状态、Feature 版本、PIT 时间和组件 lineage 字段。`research-v2.0` 仍可研究其他可用因子，但其财务因子应 fail closed 并报告不可用。
* 因子只能使用 Dataset manifest `selected_columns` 声明过的 PIT 输入。未声明字段按因子标记不可用，不能从底层 Snapshot 绕过 Dataset 身份读取。
* Snapshot 的研究日期是信号日。默认预测标签为信号日收盘到第 H 个后续冻结交易日收盘的总收益。
* 总收益按现有 `price_daily_v2` 口径逐日连乘：`(raw_close * (1 + stock_rate) + cash_div) / previous_close`；公司行为只取冻结 Dataset 中已实施的事实，现金分红使用税前 `cash_div`。任何交易日缺行情、价格事实不合格或停牌时，不跳过、不前向填充，标签保留并标成不完整。Dataset `WARNING` 若仅表示涨跌停价缺失等执行限制，且冻结 OHLC 几何、成交量额、来源哈希和未停牌状态完整，可用于价格收益标签；它仍不表示可执行成交，成交由 `BacktestEngine` 单独判断。
* `delist_date` 只从 FROZEN PIT 快照作为标签结果事实读取，绝不进入信号日因子。若退市发生在预测窗口内，当前 Dataset 没有可靠的退市结算/退出价值，标签状态记为 `INCOMPLETE`、原因记为 `DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE`，不填零，也不当作普通缺行情。若 Dataset 没有冻结 `delist_date` 字段，退市无法识别，缺行情保持未分类并在报告中明确显示。
* 另保存 T+1 开盘至相同目标日的无费用参考收益；它不是可执行收益。实际撮合、费用和退出必须由现有 `BacktestEngine` 评估。本版本不模拟实际成交。
* 完整预测标签之外，保留不完整原因和成熟状态。同期股票池等权相对收益只在完整标签样本上计算，并报告完整率和样本数。
* Rank IC 和分组收益使用完整标签样本；存在未解决退市或其他不完整窗口时，只能作为完整案例探索统计，不得作为正式因子有效性结论。报告必须包含标签原因计数、未解决退市标签数和完整案例样本口径。
* 行业字段未进入冻结输入，行业控制为 `NOT AVAILABLE`；规模控制仅在 `total_mv` 已列入 Dataset 字段契约时计算。Daily Basic 的总市值在交易日 17:00 后才视为可用，因此信号日 T 的规模控制必须使用 T−1 交易日冻结市值。
* `return_20d` / `return_60d` 仅在冻结的 `price_feature_version=price_daily_v2` 且 `price_adjustment_mode=point_in_time_total_return` 时作为趋势因子；这两个字段基于 PIT `asof_close`，包含当时可见的公司行为。`close_vs_ma60` 也检查相同的 PIT 价格坐标，但计算仍使用不含现金分红的 `adj_close_base` 与 `ma60`，不与总收益标签口径混用。
* 财务因子按 Feature 实际公式独立判断：ROE 使用有限的 `roe_ttm` 与 `roe_quality_status=VALID`，不依赖覆盖 consolidated、parent 和经营现金流的全局 `ttm_complete`；营收和利润增长是同报告期报表同比，不是 TTM 增长。增长值仅在其自身 Feature 字段有限、共享状态为 `VALID` 或 `INSUFFICIENT_HISTORY`、Feature 版本为已审查版本且 PIT 时间与 lineage 合格时可用。`INSUFFICIENT_HISTORY` 只表示另一个增长字段未通过，需在原因计数中标注，不拦截本字段自身已有的有效值。
* 财务因子只接受 `financial_data_available_timestamp` 严格早于信号日 15:00（Asia/Shanghai）的记录；要求 `financial_vintage_quality=SOURCE_ANNOUNCEMENT_ASSUMED` 且组件 lineage 非空。该证据仍是公告日加一天的日粒度假设，不代表精确披露时刻证明。

## 当前数据修复的实施选择

数据库核查显示，现有 `f348bc11-b4f0-41fe-a8b5-d376bb5cf401` Dataset 将 516 个 v2.0 与 710 个 v2.1 Snapshot 混在同一数据集中，Alpha Run 会拒绝混合 Schema。v2.1 日期范围从 2023-09-12 开始，但 2024-01-31 和 2024-02-01 只有 v2.0 Snapshot；为保持连续交易日和单一 Schema，本轮新 Dataset 从 2024-02-02 开始，首个信号日为 2024-02-05，共有 616 个 v2.1 Snapshot 至 2026-08-21。前段 v2.1 样本不与后段拼接，五年覆盖目标在当前冻结 PIT 数据下尚不可达。

新 Dataset 需显式选择 `return_20d`、`return_60d`、`price_feature_version`、`price_adjustment_mode`、`total_mv`、`financial_vintage_quality` 以及现有财务 PIT/质量字段，并包含信号开始日前一个冻结交易日以计算 T−1 市值。成熟标签边界按数据集日历确定；最后一个具有完整 60 个未来交易日的信号日为 2026-05-28。新 Dataset 不得通过更新旧 Manifest 来增加字段。

本轮 Alpha Factor Set 与 Evaluation 身份需升级，旧 186 日 Run 及 v3/v4 报告保持原样。新 Run 仍是探索性结果，因为窗口已经被观察过，且退市终值与阶段一回撤/资金场景验收尚未解决；不用于确认性结论或策略增量验收。
* 市场阶段只用冻结基准指数截至信号日的 60 交易日收盘收益分类为 `UPTREND`、`DOWNTREND`、`FLAT`；窗口或基准缺失时为 `UNKNOWN`。阶段划分不读取未来行情。

## 模块边界

| 模块 | 职责 |
| --- | --- |
| `alpha_research/contracts.py` | 固定窗口、因子元数据、输入/标签/Run 版本和确定性身份 |
| `alpha_research/factors.py` | 纯函数因子提取、有限数校验、质量状态和缺失原因 |
| `alpha_research/labels.py` | 纯函数逐证券收益标签；不连接数据库、不依赖未来信息构造因子 |
| `alpha_research/evaluation.py` | 按信号日计算 Rank IC、五分组、规模控制、年度结果和区块 Bootstrap 区间 |
| `alpha_research/report.py` | 稳定排序的 JSON/Markdown/JSONL 产物与数据质量证据 |
| `data_access/repository/alpha_research_repository.py` | 验证 FROZEN 身份/字段契约，并用现有 Repository 分页读取冻结事实 |
| `jobs/alpha_research.py` | 离线只读编排入口；分月读取，保留 60 个交易日的前向行情缓冲 |

SQL 只存在于 Repository。Repository 页接口保持 keyset 分页；标签和统计可用固定内存样本独立测试。输出文件由 Run 身份寻址，内容不带运行时间戳，重复输入可得到相同摘要。

## 输出与验收

一个 Run 输出 `report.json`、`report.md` 和按证券/信号日/窗口排序的 `samples.jsonl`。报告记录 Dataset ID、输入/标签/评价版本、Snapshot schema、信号区间、字段覆盖、样本成熟/缺失状态、逐窗口 IC/五分组/年度/市场阶段结果、区块 Bootstrap 置信区间及行业/规模控制状态。

策略增量报告区块显式显示 `NOT RUN`，直到 Stage 1 的回撤一致性和四组资金场景证据通过；不把横截面 IC 当成策略收益证据。验收需覆盖 PIT 字段不在 Dataset 契约、分页游标、复权公司行为、停牌/缺行/窗口截断、重复运行确定性、并列排名、短样本 Bootstrap 和行业字段缺失。
