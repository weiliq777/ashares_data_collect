# Research 观察池与证据引用 v1

## 外部证据候选检索

只读检索当前 ACTIVE 观察池的 OpenSearch 新闻/公告候选，不写入证据表：

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.research_evidence_candidates `
  --watchlist-id <ACTIVE_WATCHLIST_ID> --top-k 5 --mode bm25
```

候选结果只提供文档身份、来源、时间、URL 和摘要片段，并标记 `REVIEW_REQUIRED`。必须人工核验后，再使用 `add_evidence` 写入真实引用；候选不等同于 supporting 或 contrary 证据。

更新时间：2026-08-05

## 定位

观察池是 `Research Snapshot` 之上的解释层，不是交易指令层。它只读取已经通过质量门禁的 `research.stock_snapshot_daily`，不访问 Tushare，不修改 Raw/Standard/PIT/Feature，也不自动下单。

默认规则：

- 观察池必须绑定一个 `snapshot_id`；默认只允许 `FROZEN` 快照。
- `allow_preview=True` 只用于联调，允许使用 `VALIDATED` 快照，不能作为正式回测输入。
- 同一股票若同时满足两条轨道只保留一条，优先 `value_quality`；`max_members` 是观察池总人数上限。
- 当前规则版本为 `watchlist-v1`，所有入选记录保存分轨、排名、分数、规则版本和结构化理由。

## 两条最小筛选轨道

`value_quality`：可买、ROE ≥ 15、资产负债率 < 50%、经营现金流 > 0、PE 有意义且 5 年估值分位 ≤ 30%。

`growth_trend`：可买、营收同比 ≥ 30%、资产负债率 < 50%、收盘价 > MA20 > MA60、20 日成交量比 ≥ 1.5。

这些是研究筛选条件，不代表收益承诺，也没有替代连续多年财务序列验证。后续规则变化必须提升 `rule_version`，不能覆盖旧观察池。

## 证据引用

`research.evidence_reference` 只保存引用元数据：

- OpenSearch：`document_index`、`document_id`、`content_hash`、`locator`；
- 网页或人工材料：`source_url`、`source_locator`、`locator`；
- 统一保存 `snapshot_id`、股票代码、来源类型、发布时间/生效日和 `claim_role`；
- `claim_role` 为 `supporting`、`contrary` 或 `context`，鼓励同时保存支持和反方证据；
- 正文继续保留在 OpenSearch 或外部来源，PostgreSQL 不重复存储全文。

## 审计不可变规则

- `ACTIVE` 观察池只能转为 `ARCHIVED`，不能回改名称、规则、快照绑定或删除；
- `ACTIVE` 观察池成员和证据只能追加，不能更新或删除；
- `ARCHIVED` 观察池及其成员、证据均不可变，不能追加、更新或删除；需要修正时新建 `watchlist_id`；
- 新证据必须对应快照中的股票；绑定 `watchlist_id` 时，该股票必须已经是该观察池成员；
- 需要修正规则、成员或证据时，创建新的 `watchlist_id`，不覆盖旧研究结果。

数据库约束位于 `sql/027_research_audit_immutability.sql`，应用层校验只是提前报错，不能替代数据库触发器。

## 命令

```powershell
.\.venv\Scripts\python.exe -m data_collect.jobs.research_watchlist create `
  --snapshot-id <FROZEN_SNAPSHOT_ID> --name core-watchlist --max-members 50

.\.venv\Scripts\python.exe -m data_collect.jobs.research_watchlist activate `
  --watchlist-id <WATCHLIST_ID>

.\.venv\Scripts\python.exe -m data_collect.jobs.research_watchlist lineage-evidence `
  --watchlist-id <ACTIVE_WATCHLIST_ID>
```

正式使用顺序仍是：

```text
clean commit → create snapshot → build → validate → freeze
→ create watchlist → attach evidence → 人工研究/风险复核
```
