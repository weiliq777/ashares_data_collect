# Data Foundation Access Framework（数据基座统一访问框架）设计

> 定位：把整个数据基座（Foundation）的所有数据库读写行为，收口成**唯一、可治理的访问内核**。
> 业务代码（Raw/Standard/PIT/Feature/Snapshot/Dataset/Quality 各 Job）**禁止直接持有
> Connection/Cursor、禁止手写 SQL、禁止 commit/rollback、禁止按股票逐只循环查库**。
>
> 状态：**Data Foundation Access Framework Baseline v1.0（定稿，可进入实现）** ——
> 承接《数据基座架构拆分方案.md》§10 的大事务纪律，把"文档约定"变成"框架约束"。
> 主结构已冻结，后续实现中遇问题优先调整 Policy/参数/Gateway 实现，而非改分层。
>
> 更新日期：2026-08-16

---

## 0. 定位与目标

《数据基座架构拆分方案.md》已经规定了 `Chunk → Commit → Checkpoint`、增量 UPSERT、
大范围 `Staging → Validate → Atomic Publish` 等**纪律**。但这些目前只是"文档约定"，
依赖每个程序员"记得遵守"——这是长期系统最大的隐患。

本框架的目标：**把这些纪律从"约定"变成"框架强制"**。

```text
业务层描述"我要干什么"（append / upsert / rebuild / read）
        ↓
Kernel 决定"数据库怎么安全地干"（chunk / 事务 / 锁 / 幂等 / 校验 / 血缘）
```

一句话：**业务代码不再知道 SQL、事务、连接是什么。**

---

## 1. 现状诊断（为什么必须重构，而不是修补）

结合源码审计（2026-08-16），当前存在两类必须整改的问题：

### 1.1 `jobs/` 目录有 250+ 处散落的数据库直连

```text
grep -E "get_connection\(\)|cursor\.execute|conn\.commit\(\)" jobs/*.py
→ 250 处，散落在 40+ 个文件
```

典型不合规写法（当前真实存在）：

```python
# ① 业务层自己开连接、自己 commit
with get_connection() as conn:
    conn.cursor().execute("DELETE FROM ... WHERE ...")
    conn.commit()

# ② 手写大 SQL，无范围约束
cur.execute("""INSERT INTO pit.universe_daily (...) SELECT ... FROM ...""")

# ③ 事务内循环统计（research_snapshot.py:1340 的 _input_stats）
```

这些是"以后一定会出事"的代码：超大事务、无 WHERE 全表扫描、事务内统计、N+1 逐只查询。

### 1.2 `utils/db.py` 是"半套基础设施"，且是"万能入口"式的不合规

现有 `utils/db.py` 已经**零散地**具备了一些能力：

| 能力 | 现状 | 问题 |
|---|---|---|
| 超时熔断 | `get_connection` 设了 `statement_timeout/lock_timeout/idle_in_transaction_timeout` | 好，但只覆盖"连上了"之后 |
| 分批写入 | `save_to_postgres` 的 `chunk_size=10000` | 好，但只有这一个函数做了 |
| 幂等 | `ON CONFLICT DO NOTHING` | 好，但只有 append 语义，缺 upsert 语义 |
| 防注入 | `validate_table_name` | 好 |
| 防 N+1 | `get_dates_with_data` 注释写了"避免 N+1" | 只有意识，未系统化 |

**核心问题**：`get_connection()` 是一个**万能入口**——它把原始 `conn` 直接交给业务层，
于是所有 job 拿到 conn 后自己写 SQL、自己开事务。`save_to_postgres` 等函数只是
"顺手提供的便利函数"，不是强制通道。业务层**可以完全绕过它们**。

### 1.3 结论：不是"修补 db.py"，而是"重构为 Kernel"

**长期规范化的立场：当前不合规，就重构；不保留旧万能入口的兼容，不因"已有雏形"而将就。**

- `get_connection()` 作为业务可调用的公开入口 → **废弃**（收敛为 Kernel 内部私有）
- `save_to_postgres` / `replace_day_then_insert` 等 → **重构**为 Kernel 的标准接口
- 250+ 处散落 SQL → **全部迁移**到 Kernel / Repository

---

## 2. 分层结构

```text
┌──────────────────────────────────────────────────────────┐
│              Data Processing Domain（业务层）              │
│  Raw Job / Standard Job / PIT Job / Feature Job           │
│  Snapshot Job / Dataset Job / Quality Job                 │
│  【禁止：连接、游标、SQL、commit、逐只循环】                  │
└───────────────────────────┬──────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────┐
│                Repository Layer（领域仓储）                 │
│  RawRepository / SecurityRepository / MarketRepository     │
│  FinancialRepository / PitRepository / FeatureRepository   │
│  SnapshotRepository / DatasetRepository                    │
│  【SET-based 复杂 SQL 的唯一合法归属地】                      │
└───────────────────────────┬──────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────┐
│                 Data Access Kernel（内核）                 │
│                                                          │
│   RawAppendGateway      ReadGateway                       │
│   WriteGateway          RebuildGateway                    │
│   PublishGateway                                          │
│                                                          │
│   QueryGuard            ChunkPlanner                      │
│   TransactionManager    LockManager                       │
│   RetryPolicy           Checkpoint                        │
│   RunManifest/Lineage                                     │
└───────────────────────────┬──────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────┐
│           PostgreSQL（+ Parquet / Object Storage）        │
└──────────────────────────────────────────────────────────┘
```

横向统一挂接：`RunContext`、`Quality`、`Metrics`、`Lineage`、`Change Event`。

**关键设计**：SET-based 复杂 SQL（多表 JOIN + 窗口函数 + `INSERT...SELECT`）**不禁用**，
而是**从业务层下沉到 Repository 层**。SQL 只允许存在于 `data_access/` 目录内
（Kernel + Repository），业务层永远只调用 Repository 方法、看不见 SQL。

---

## 3. 核心对象：WorkScope 与 RunContext

### 3.1 WorkScope（任务范围）

所有大规模任务统一用 `WorkScope` 描述处理范围，禁止各任务自己定义"范围"：

```python
@dataclass(frozen=True)
class WorkScope:
    date_from: date | None = None      # 时间范围起点
    date_to: date | None = None        # 时间范围终点
    codes: tuple[str, ...] | None = None   # 股票范围（可为 None = 全市场）
    partition: str | None = None       # 分区范围（如 "2026-01"）
    dimensions: Mapping[str, Any] = field(default_factory=dict)  # 可扩展维度
    #   例：{"report_period": "2025Q4", "industry_code": "801120", "snapshot_id": ...}
    #   未来新增数据域（财务报告期/行业/指数成分/数据源/snapshot_id）不再改 WorkScope 结构
```

基础设施据此知道：这次任务处理的时间范围、股票范围、分区范围、预计行数。
`Read / Write / Lock / Checkpoint / Change Event / Run Manifest / Quality` 全部绑定同一个 `scope`。

### 3.2 RunContext（谁干的）

所有数据库操作**强制携带** `RunContext`，无则拒绝执行：

```python
@dataclass(frozen=True)
class RunContext:
    run_id: str                 # 本次执行唯一 ID
    job_name: str               # 任务名（如 feature_price_daily）
    code_version: str           # git commit
    config_hash: str            # 配置哈希
    pipeline_version: str       # pipeline 版本
```

任何 read/write/checkpoint/change_event/quality 都自动记录"这是谁干的"，实现真正的 Lineage。

### 3.3 OperationPlan / SqlPlan（操作意图，而非裸 SQL 字符串）

Repository 调 Kernel 时**不只传 SQL 字符串**，还要传一个**执行计划描述对象**，让 QueryGuard
不依赖脆弱的字符串分析：

```python
@dataclass(frozen=True)
class SqlPlan:
    operation: Operation              # APPEND / UPSERT / BULK_INSERT / REBUILD / READ
    target: str                       # 目标表/视图
    scope: WorkScope                  # 处理范围
    expected_mode: str                # "SET_BASED" / "ROW_BASED"
    estimated_rows: int               # 预估行数（仅预警/选模式/容量规划，非事务切分依据）
    allow_insert_select: bool         # 是否允许 INSERT...SELECT
    allow_delete: bool = False        # 是否允许 DELETE（大范围重建默认 False）
```

Kernel 检查顺序：`SqlPlan + WorkScope + DatasetPolicy（§5.1）+ 数据库角色`。
SQL 静态检查（正则/parser）只是**辅助**，不是主要防线。

---

## 4. DataAccessKernel 接口定义（6 种，不公开 execute()）

Kernel 是唯一数据库入口，**不向业务层公开 `execute()`**。按数据库行为性质拆成 6 种接口：

```python
class DataAccessKernel:
    raw: RawAppendGateway      # append()
    read: ReadGateway          # query() / stream()
    write: WriteGateway        # upsert() / bulk_insert()
    rebuild: RebuildGateway    # publish()
    publish: PublishGateway    # snapshot() / dataset()
    quality: QualityGateway    # record()
```

### 4.1 `raw.append()` —— Raw 不可变追加

```python
raw.append(
    context=run_context,
    dataset="tushare_daily",
    rows=rows,
    business_key=["ts_code", "trade_date"],
    payload_hash_column="payload_hash",
)
```

内部统一负责：去重 → 判 business_key → 判 payload_hash（同 hash ignore / 异 hash 新 version）
→ 批量 COPY → 小事务 commit → 记 Run Manifest。业务层不知道 SQL。

> **版本判断必须批量 SET-based，禁止逐条 SELECT**：
> 新 Raw batch → COPY 进 staging → **一次 SET-based 对比** business_key + payload_hash
> → 相同 hash 忽略 / 异 hash INSERT 新版本。绝不对每条 Raw 做 `SELECT`（否则退化成 N+1）。

### 4.2 `write.upsert()` —— Standard/PIT/Feature 增量写

```python
write.upsert(
    context=run_context,
    scope=work_scope,
    target="feature.price_daily_v2",
    rows=result,
    key=["ts_code", "trade_date"],
    update_columns=["ma20", "ma60", "volume_ratio_20d"],
)
```

内部**自动 ChunkPlanner**：rows → 50k/批 → 每批独立事务 → checkpoint。
**调用者不需要写 `for batch in ...` 循环**——循环属于基础设施。

### 4.3 `write.bulk_insert()` —— 大量纯插入（staging / 新版本）

```python
write.bulk_insert(
    context=run_context,
    scope=work_scope,
    target="feature.price_daily_v2__staging_build_xyz",
    rows=rows,
)
```

内部优先 `COPY` 而非逐条 INSERT（性能差异巨大），仍走 chunk + checkpoint。

### 4.4 `read.query()` —— 受控读取（不允许任意 SQL）

```python
data = read.query(
    context=run_context,
    source="mart.v_price_feature",
    columns=["ts_code", "trade_date", "ma20", "ma60"],
    where={"trade_date__gte": start, "trade_date__lte": end},
    limit=100_000,
)
```

**调用方不能传任意 SQL**。基础设施自动：按 `DatasetPolicy.scan_policy` 判断是否必须带 Scope/WHERE、
检查字段白名单、检查最大返回行数、设 statement_timeout、记录耗时。

### 4.5 `read.stream()` —— 大结果集流式读取

```python
for batch in read.stream(
    context=run_context,
    source="mart.v_stock_research",
    where={"trade_date__gte": start, "trade_date__lte": end},
    batch_rows=50_000,
):
    process(batch)
```

底层 Server-Side Cursor，50k/批，处理完释放。**全系统禁止 `fetchall()` 读超大表**。

### 4.6 `rebuild.publish()` —— 大范围重建（最重要的一个）

```python
rebuild.publish(
    context=run_context,
    scope=work_scope,                 # 例：2025-01-01 ~ 2025-01-31
    target="feature.price_daily_v2",
    producer=build_price_feature,     # 计算函数，接收 scope，产出 staging 数据
    mode=RebuildMode.PARTITION_REPLACE,
)
```

**三种发布粒度**（不能为了重算一只股票去 swap 整张表）：

| 模式 | 适用 | 发布方式 |
|---|---|---|
| `TABLE_REPLACE` | 整表重建（表较小 / 首次构建） | staging 建好 → swap 整表 |
| `PARTITION_REPLACE` | 整分区重建（如 2025 年度） | 新 partition → validate → attach/swap |
| `SCOPED_MERGE` | 小范围增量（如 600519.SH × 60 交易日） | staging → validate → scoped upsert/merge |

框架自动执行完整生命周期：

```text
创建 Staging → 分批计算 → 分批 COPY → Checkpoint
→ Build Index → Quality Validate → 计算 Hash
→ Atomic Publish（按 mode）→ 清理 Staging
```

**禁止业务层 DELETE + INSERT**——大范围重建只能走 `rebuild.publish()`。
`Impact Resolver`（见架构方案 §3.4.3）将来据此决定用哪种 mode。

### 4.7 SET-based SQL 的归属（关键校准）

Feature 计算大量是"一条 SQL 完成转换"（多表 JOIN + 窗口函数 + `INSERT...SELECT`）。
这类 SQL **不禁用**，但必须：

1. 封装在 **Repository 层**（见 §8），业务层调用 `repository.rebuild_xxx(scope, context)`；
2. 内部通过 Kernel 的**受控重建通道**执行，并携带 `SqlPlan`（§3.3）+ WorkScope；
3. 禁止业务层直接写这类 SQL。

> 注意：这类 SET-based SQL 属于"不可主动切分"的语句（见 §6），靠 statement_timeout 熔断，
> 而非 ChunkPlanner 切分。

> **接口按语义拆分，禁止泛化的 `run_set_sql()`**：`rebuild.run_insert_select()` /
> `rebuild.run_scoped_merge()` / `rebuild.run_partition_replace()` / `rebuild.run_table_replace()`，
> 每个都**强制携带 `SqlPlan`**。核心原则：**Repository 可以写 SQL，但不能获得"任意执行 SQL"
> 的能力**——否则泛化的 `run_set_sql()` 几年后会退化成新版 `execute()`。

---

## 5. DatasetPolicy 与 QueryGuard（SQL 执行前强制检查）

### 5.1 DatasetPolicyRegistry（Kernel 知道每个 Dataset 能干什么）

Kernel 怎么知道 `raw.tushare_daily` 只能 append、`contract.stock_snapshot_daily` FROZEN 后
不可改、`feature.price_daily_v2` 允许 upsert？**不能靠调用者自觉选对 Gateway**，必须有
`DatasetPolicyRegistry`：

```yaml
feature.price_daily_v2:
  write_mode: [UPSERT, REBUILD]
  primary_key: [ts_code, trade_date]
  partition_key: [trade_date]
  require_scope: true
  scan_policy: SCOPE_REQUIRED
  frozen: false

contract.stock_snapshot_daily:
  write_mode: [PUBLISH_ONLY]
  immutable_after_freeze: true
  require_snapshot_id: true
  scan_policy: SCOPE_REQUIRED

raw.tushare_daily:
  write_mode: [APPEND_VERSIONED]
  update_allowed: false
  delete_allowed: false

ops.source_registry:            # 小型维度表
  scan_policy: FULL_SCAN_ALLOWED
```

于是 `kernel.write.upsert(target="raw.tushare_daily", ...)` 会被 Kernel **直接拒绝**。
**不是程序员选择正确 Gateway，而是 Kernel 知道这个 Dataset 能干什么。**

### 5.2 QueryGuard 规则

所有 SQL 执行前过 QueryGuard，检查依据 = `SqlPlan + WorkScope + DatasetPolicy + 数据库角色`：

| 规则 | 说明 |
|---|---|
| 违反 DatasetPolicy 的写 | 如 upsert 目标是 `raw.*`（只允许 append）→ 拒绝 |
| 禁止无范围 DELETE | `DELETE FROM feature.price_daily_v2;` 直接拒绝 |
| 禁止 UPDATE/DELETE FROZEN 表 | `immutable_after_freeze=true` 的表 FROZEN 后绝对只读 |
| 禁止 DROP/TRUNCATE | 业务层一律无此权限 |
| 禁止单次读取超阈值 | 超 `max_read_rows` 强制走 stream |
| 大表禁止无 Scope 扫描 | `scan_policy=SCOPE_REQUIRED` 的表必须带分区键/日期范围 |
| 小维度表允许全表读 | `scan_policy=FULL_SCAN_ALLOWED`（如 `std.exchange`、registry） |

> **不再写死"所有查询必须有 WHERE"**——那会把交易所字典、行业字典、Registry 这类小维度表
> 也误伤。规则下沉到 `DatasetPolicy.scan_policy`，按表属性决定。

QueryGuard 分两级：**代码层**（Kernel 内检查）+ **数据库层**（角色权限，见 §7）。

---

## 6. TransactionManager 事务硬预算

业务代码禁止 `commit/rollback`，由 TransactionManager 统一决定。硬预算分两类：

```yaml
transaction:
  max_rows_per_tx: 50000      # 【可主动切分】chunk 执行前控制，ChunkPlanner 据此规划
  max_bytes_per_tx: 67108864  # 【可主动切分】chunk 执行前控制
  transaction_deadline: 30    # 【只能中止】单事务累计时间上限（秒），超时回滚
```

**正确语义（与 PostgreSQL 事务模型对齐）**：

```text
ChunkPlanner（执行前控制）：
  以【实际输入】切 chunk：按实际 row_count + memory_size 切分（Iterable/Arrow/DataFrame）
  每个 chunk ≤ max_rows_per_tx、≤ max_bytes_per_tx
  SqlPlan.estimated_rows 只用于预警/选模式/容量规划，【不是】事务切分的安全依据

TransactionManager（只能中止，不能"执行中提交"）：
  单事务累计超过 transaction_deadline → rollback 当前事务，不续开
  单条 SQL 超时 → statement_timeout → rollback 当前事务
```

> **时间预算是熔断，不是"执行到一半自动 commit"**。正在执行的
> `INSERT ... SELECT` 跑 30 秒，Kernel 不可能在 SQL 半途安全提交，只能等它跑完或由
> `statement_timeout` 中止并回滚。可主动切分的是 rows/bytes，超时只能中止。

**不是靠程序员"记得不要大事务"，而是框架根本不产生大事务。**

### 6.1 RetryPolicy（重试与错误分类）

**可以自动重试**（幂等、瞬时性）：

```text
连接瞬断 / deadlock / serialization failure / lock timeout（视操作而定）
```

**禁止自动重试**（重试无意义或有害）：

```text
SQL 语法错误 / 约束冲突 / DatasetPolicy 违规 / Quality 失败 / 业务数据错误
```

否则一个错误 SQL 被 retry × 10 毫无意义，还放大锁等待与 WAL。

---

## 7. 两级安全（代码 + 数据库角色）

只靠代码接口不够，加数据库角色权限兜底：

```text
raw_writer           → 只写 raw_*，不可 UPDATE/DELETE
standard_writer      → 只写 std
pit_writer           → 只写 pit
feature_writer       → 只写 feature
contract_publisher   → 只写 contract，且 FROZEN 后只读
research_reader      → 只读 contract.* + mart.v_*，无写权限
```

于是即使有人绕过 Kernel 直接 `cursor.execute()`，数据库自己也拒绝（无权限）。

---

## 8. Repository 层（领域仓储，SET-based SQL 的合法归属）

Kernel 是"数据库基础能力"，之上是领域 Repository。Feature 业务代码看不到 SQL：

```python
# 业务层（Feature Job）
price = market_repo.load_price_window(scope, context)
financial = financial_repo.load_asof(scope, context)
result = calculator.calculate(price, financial)
feature_repo.save_price_feature(result, scope, context)

# Repository 层（唯一合法写 SQL 的地方）
class UniverseRepository:
    def rebuild_universe(self, scope, context):
        sql = """INSERT INTO pit.universe_daily (...) SELECT ... FROM std.security_status_daily ..."""
        return self.kernel.rebuild.run_insert_select(
            sql=sql,
            plan=SqlPlan(operation=Operation.REBUILD, target="pit.universe_daily",
                         scope=scope, expected_mode="SET_BASED",
                         estimated_rows=350_000, allow_insert_select=True),
            context=context,
        )
```

最终调用链：

```text
Feature Job → FeatureRepository → DataAccessKernel
→ QueryGuard → ChunkPlanner → TransactionManager → PostgreSQL
```

---

## 9. 目录结构

```text
data_collect/
├── data_access/                    # 【唯一允许访问数据库 Driver 的目录】
│   ├── __init__.py                 # 导出 DataAccessKernel
│   ├── kernel.py                   # DataAccessKernel 组合根
│   ├── context.py                  # RunContext / WorkScope
│   ├── raw_gateway.py              # append()
│   ├── read_gateway.py             # query() / stream()
│   ├── write_gateway.py            # upsert() / bulk_insert()
│   ├── rebuild_gateway.py          # publish() / run_insert_select() / run_scoped_merge() / ...
│   ├── publish_gateway.py          # snapshot() / dataset()
│   ├── quality_gateway.py          # record()
│   ├── query_guard.py              # SQL 执行前检查
│   ├── chunk_planner.py            # 分批规划
│   ├── transaction_manager.py      # 事务硬预算
│   ├── lock_manager.py             # 单并发排他锁
│   └── repository/                 # 领域仓储（SET-based SQL 归属）
│       ├── raw_repository.py
│       ├── security_repository.py
│       ├── market_repository.py
│       ├── financial_repository.py
│       ├── pit_repository.py
│       ├── feature_repository.py
│       └── snapshot_repository.py
├── jobs/                           # 【业务层：禁止 psycopg/cursor/SQL】
├── utils/db.py                     # 【重构：旧万能入口废弃，能力迁入 data_access/】
```

---

## 10. 与现有 `utils/db.py` 的迁移对照表

| 现有（`utils/db.py`） | 处置 | 迁移到 Kernel |
|---|---|---|
| `get_connection()` | **废弃**（业务层不再可调，收敛为 Kernel 私有 `_connect()`） | `transaction_manager._connect()` |
| `require_psycopg2()` | 收敛 | `data_access` 内部使用 |
| `save_to_postgres()`（chunk + DO NOTHING） | **重构** | `raw.append()` / `write.bulk_insert()` |
| `replace_day_then_insert()`（DELETE+INSERT） | **重构** | `write.upsert()` 或 `rebuild.publish()`（按增量/全量二选一） |
| `get_table_schema/columns` + `_schema_cache` | 保留（迁入） | `data_access/schema.py` |
| `validate_table_name()` | 保留（迁入） | `query_guard.py` |
| `has_data_for_date()` / `get_dates_with_data()` | 保留（迁入） | `read.query()` 的受控封装 |

**立场**：旧函数不保留兼容别名，迁移完成即删除。当前不合规的 250+ 处散落点全部整改。

---

## 11. 十条强制铁律

1. 业务层禁止直接 SQL。
2. 业务层禁止直接持有 Connection / Cursor。
3. 业务层禁止 commit / rollback。
4. 禁止"股票循环内发生数据库 I/O"（N+1 round-trip）；允许 Batch Read → 内存按股票循环计算 → Batch Write。
5. 大读取必须 Stream / Batch（禁止 `fetchall()` 读超大表）。
6. 大写入必须 Chunk（禁止单事务塞百万行）。
7. 增量用 UPSERT。
8. 大范围重建必须 `Staging → Validate → Atomic Publish`。
9. 所有操作必须带 `RunContext` + `WorkScope`。
10. 所有 SQL 只能存在于 `data_access/`（Kernel + Repository）基础设施内。

---

## 12. 分阶段迁移计划

> 立场：**不保留旧万能入口的兼容，不怕铲掉旧的。** 分阶段是为了降低风险，不是"慢慢拖着不改"。

### 阶段 0：冻结 + 审计（半天）
- 产出 250+ 处散落点的完整清单（按文件/风险分级）
- 冻结 `utils/db.py` 的公开 API 变更

### 阶段 1：立 Kernel 核心（1~2 周）
- 实现 `data_access/` 的 `RawAppendGateway`、`WriteGateway`、`ReadGateway`、`RebuildGateway`
- 实现 `RunContext` / `WorkScope` / `QueryGuard` / `TransactionManager` / `ChunkPlanner`
- 单元测试覆盖：chunk 边界、事务预算触发、QueryGuard 拒绝、幂等重放

### 阶段 2：新代码强制走 Kernel（立即生效）
- 所有**新增** Job / Feature / Repository 必须走 Kernel，禁止 `get_connection`
- code review 硬性检查

### 阶段 3：老 Job 逐个迁移（按风险排序，2~4 周）
- 优先级：**重建类**（`rebuild_universe`/`rebuild_valuation`/`build_snapshots_batch`）→
  高频增量类 → 低频快照类
- 每迁一个，删除对应旧 `get_connection` 写法

### 阶段 4：CI 硬门禁（迁移完成后）
- CI 扫描业务目录（`jobs/`、`services/`、`features/` 等），禁止出现：
  `cursor.execute` / `connection.execute` / `psycopg.connect` / `sqlalchemy.create_engine` /
  `SELECT` / `INSERT INTO` / `UPDATE` / `DELETE FROM` / `get_connection`
- Python import 规则：业务目录禁止 `import psycopg2`，只能 `from data_collect.data_access import DataAccessKernel`

### 阶段 5：清理旧入口（收尾）
- 删除 `utils/db.py` 中已迁移的旧函数（不保留兼容别名）
- 数据库角色权限落地（§7），形成代码 + 权限双保险

---

## 13. 验收标准

1. `jobs/` 目录 grep 不到 `get_connection()` / `cursor.execute` / `conn.commit`（CI 强制）
2. `utils/db.py` 不再有业务可调用的连接入口
3. 所有大规模写走 `rebuild.publish()` 或 `write.upsert()`，无手写 DELETE+INSERT
4. 所有读走 `read.query()`/`read.stream()`，无 `fetchall()` 读大表
5. 无逐只循环查询（CI 可检测 `for ... in ...: ... execute` 模式）
6. 所有操作带 `RunContext` + `WorkScope`，可回溯到任意一次执行的输入/输出/血缘
7. 数据库角色权限生效：绕过 Kernel 直接 SQL 会被数据库拒绝

---

## 14. 附录：与《数据基座架构拆分方案.md》的关系

- 本文档是**落地层设计**，承接架构方案 §10（大数据量/大事务规避）与 §6 治理规则第 8 条
  （统一 Run Manifest）——把那些"纪律/约定"变成可执行的框架代码。
- 本文档不改变架构层级（架构已冻结为 Baseline v1.0），只解决"数据库行为如何安全收口"。

---

## 15. 正式生产边界补充（仍属于本契约）

以下规则是对前述分层、冻结、分区和时点约束的工程化收口，不改变 Gateway 数量、Repository → Kernel 调用方向或既有接口语义。

1. **Mutable Feature 不得作为业务消费接口**：`ReadGateway.query/stream/scalar` 对可变 `feature.*` 直接读取 fail-closed；Feature SQL 只能由 Repository 作为派生计算的内部实现使用。研究、候选和回测只能读取 `mart.*` 或 FROZEN `contract.*`。
2. **数据基线区分业务截止日和仓库可见时间**：`baseline_cutoff_date` 是业务时点；`data_cutoff_timestamp` 只对确有仓库观测证据的来源生效，不能把补采历史的 `fetched_at` 当作原始业务可见时间。
3. **发布状态必须可审计**：`validated` 是当前合格发布点；`superseded` 是正常替代且允许按完整 manifest 精确复现；`revoked` 表示污染或严重缺陷，禁止新研究/回测，只保留审计证据，不得删除或修改发布记录。
4. **公司行为影响范围必须显式计算**：滚动指标的 warm-up 读取窗口有界；复权累计状态使用窗口前的持久化 seed。公司行为修订从最早受影响事件日重算到目标结束日，不能用简单固定 250 日窗口错误截断累计影响，也不能对每个股票批次无界扫描全部历史。
5. **大快照物理迁移必须可恢复**：按日期小窗口复制、每窗口独立事务、checkpoint、计数与内容哈希校验；复制目标不得安装逐行查询 manifest 的 N+1 触发器。全部校验通过后，才在短锁目录切换事务中安装 FROZEN/日期触发器；旧表保留到切换后验证完成。
6. **同日财务修订必须可确定仲裁**：`version_available_date` / `data_available_timestamp` 表示业务可见边界；当同一业务键同一公告日存在多个不同 payload 时，Standard（标准层）必须标记 `quality_status='WARNING'`，PIT/Feature 选择必须使用 `(data_available_timestamp, record_version DESC)`（或等价的日期边界 + `record_version DESC`）打破平局，禁止只按日期随机取值。`fetched_at` 只能作为仓库观测证据，不能伪装成官方公告时刻。
