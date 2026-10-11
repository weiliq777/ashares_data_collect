# Finance_cn Coding Agent Standard

This document is the single project-level instruction file for AI coding agents. It combines the general engineering standards with Finance_cn's source-grounded architecture and business rules.

> **Physical Security Boundary Notice**: This document serves as behavioral constraints and reasoning guidelines for the Agent. The QMT trading API check is a source-level CI/pre-commit guard, not runtime isolation. Physical boundaries such as database write/drop authorizations, broker connectivity, code push, and deployment controls must also be enforced through tool permissions, runtime/network isolation, and CI/CD controls.

Implementation details are governed by the current source code, SQL migrations, and tests. Keep durable cross-task rules here; put task-specific implementation details in code and contracts.

---

## 1. Core Directives

### 1.1 Minimal Sufficient Change (MSC)
* **Scope Control**: Under the premise of satisfying functional correctness, necessary design quality, and test requirements, choose the reasonable implementation with the smallest scope of impact and lowest overall complexity.
* **No Unauthorized Changes**: Strictly prohibit "incidentally" formatting untouched files, refactoring unrelated modules, or modifying existing public interfaces/database migrations without authorization.
* **Resist Speculative Generality**: Never write generic abstraction layers, middleware, or design patterns that are not required by the current demand.

### 1.2 Progressive Modification & The "Two Hats" Principle
* **Strictly Separate Modification Types**: Clearly distinguish between "new feature development" and "behavior-preserving modifications (refactoring)", avoiding deep mixing of both operations in a single commit.
* **Pre-Refactoring Only When Necessary**: Perform necessary pre-refactoring only when the existing structure prevents implementing the demand correctly and safely; simple modifications must not force a refactoring process. Refactoring follows the sequence: "Establish/Verify Tests $\rightarrow$ Small-Step Refactoring $\rightarrow$ Verify Behavior Unchanged $\rightarrow$ Implement New Feature".

### 1.3 Objective Physical Quality Gates
* **Four-State Traceability**: Physical verification must be conducted via project Linters, Type Checkers, and Test Suites. Reports must explicitly mark the four states: `PASS`, `FAIL`, `NOT RUN`, or `N/A`.
* **Blockers & Baseline Isolation**: `NOT RUN` must state the reason for non-execution; `N/A` must state the basis for non-applicability. When inspection results are `FAIL` or `NOT RUN`, quality acceptance must not be marked as passed. Historical test failures must have baseline or comparative evidence and must not be assumed out of thin air to be legacy issues.

### 1.4 Production Safety & Operational Redlines
1. **Strictly Prohibit Unauthorized Operations**: Unauthorized writing to/truncating production databases, altering production configurations, or pushing/deploying code is strictly forbidden.
2. **Strictly Prohibit Credential Leakage**: Unrelated private keys, API tokens, and sensitive data must not be read, printed, or committed.
3. **Strictly Prohibit Lowering Quality Gates**: Deleting failing tests, lowering assertion criteria, or disabling static checks just to achieve `PASS` is strictly forbidden.

---

## 2. Finance_cn 项目架构与业务规则

### 主要数据流

```text
Tushare 结构化数据 → Raw → Standard → PIT / Feature
Raw 来源清单 + 质量证明 → Data Baseline

PIT / Feature + Data Baseline → Research Snapshot（FROZEN）
Research Snapshot + 执行行情/公司行为/冻结交易规则
    → Backtest Dataset（FROZEN）→ 回测引擎 → 评价
Research Snapshot → 决策候选 → 人工审阅 → 纸面组合

日常采集质量门禁 → Market Activity / Observation（独立分析分支）
```

此图表示 Tushare 结构化研究数据主链；QMT Tick、新闻等使用各自的存储流程。Data Baseline 绑定来源边界和质量证明，不是 PIT/Feature 后必经的数据转换层。Dataset 构建按来源边界读取并物化执行行情、公司行为及规则；回测引擎只读取冻结 Dataset。日生产 DAG 遵循 `data_collect/pipeline.py` 的有界任务契约，全历史构建和诊断任务不进入定时生产链。

### 核心规则

- 业务代码通过领域 `Repository` 访问数据库，由 `DataAccessKernel` 和受控 Gateway 执行读写。数据库驱动、连接、游标和领域 SQL 留在 `data_collect/data_access/`；新增代码遵守 `tools/check_data_access_contract.py` 的检查边界。
- Raw 保存采集事实，新采集以追加写入；有版本化契约的来源按版本追加。已保存记录不覆盖或删除；修复时写入新版本，再按明确范围重建下游数据。数据库写入应有界、可重试且幂等；大任务按日期或业务键分批并保存可恢复的检查点，避免全表清理或无界重建。
- PIT 使用业务可见日期表达信息何时可用；`fetched_at` 表示本地采集时间，不能替代业务可见时间。财务和公司行为按字段契约中的公告日、实施日、除权日等业务日期进入计算，不能用报告期或抓取日替代。
- 历史标的范围和属性按决策日时点取值，不能用当前证券状态或当前行业回填过去。缺少历史有效日期证据的行业数据保留为 `DAILY_CAPTURED` 或 `UNKNOWN`，不当作已验证的 PIT。
- 缺失值保留为 `UNKNOWN`，除非字段契约明确要求失败关闭。ST 状态与可交易状态分别判断；可交易性依据停牌、涨跌停和行情等时点事实。
- 正式候选使用明确绑定的 FROZEN Research Snapshot，正式回测使用物化并冻结的 Backtest Dataset。Snapshot 构建阶段可读取通过质量门禁的上游数据；冻结结果不原位改写，策略、场景、交易规则、引擎或输入语义变化时使用新的版本或哈希身份。交易时点和撮合规则遵循场景契约及 Dataset 中冻结的规则。
- 组合流程是人工批准后的纸面模拟；当前实现不连接券商，也不发送实盘订单。
- 策略和回测引擎从冻结 Snapshot/Dataset 批量读取，避免逐证券查询数据库；Dataset 构建可按来源边界读取 Raw 并物化执行输入。
- QMT 在本项目中仅用于行情数据采集和本地行情缓存维护。可执行 Python 不得导入 `xtquant.xttrader`、`XtQuantTrader`、`StockAccount`，也不得调用报单或撤单接口；增加交易连接能力属于独立的安全边界变更，必须先完成明确授权、架构审查和人工评审。
- 密钥由运行环境提供，不提交到仓库，也不写入文档或日志。

### 数据访问改动

- 新增或改变数据模型、数据源时，同步维护对应 Repository、SQL 迁移和回归测试；架构边界变化时更新 `dev_doc/source_architecture.md`。
- 数据访问改动运行契约扫描：

  ```powershell
  uv run python tools/check_data_access_contract.py
  ```

---

## 3. Precedence Protocol

When design principles collide, adjudicate according to the following priority order from highest to lowest:

$$\text{Correctness, Security \& Data Integrity} \rightarrow \text{Explicit Demand \& Authorized Contract Change} \rightarrow \text{Existing Behavior \& Interface Compatibility} \rightarrow \text{Minimal Sufficient Change} \rightarrow \text{Code Understandability \& Deep Modules} \rightarrow \text{Moderate Extensibility}$$

---

## 4. Risk Classification & Skill Dynamic Loading Matrix

Risk levels are determined by the **actual scope of impact and uncertainty** (scenarios in the table are examples; take the higher level when multiple conditions overlap). When involving irreversible data changes, security/permission boundaries, external contracts, core financial calculations, or concurrency consistency, **upgrade to at least L4**:

| Risk Level | Decision Criteria (Example Scenarios) | Recommended Skill to Load | Core Focus |
| :--- | :--- | :--- | :--- |
| **L1 (Micro Risk)** | Local stateless bug fix / Single function internal adjustment | `AGENTS.md` | Minimal Sufficient Change, no scope expansion |
| **L2 (Low Risk)** | Code quality check / Adding unit tests | `clean-code-reviewer` / `clean-test-craftsmanship` | Intentional naming, single abstraction level, F.I.R.S.T principles |
| **L3 (Medium Risk)** | New business module / API interface design | `architecture-complexity-guard` | Deep Modules, information hiding, define errors out of existence |
| **L4 (High Risk)** | Legacy code refactoring / High-risk calculation or boundary | `refactoring-recipe` | Code smell diagnosis, small steps, preserving external behavior |
| **L5 (Critical Risk)** | Cross-service architectural evolution / Core data migration | Full Skill Suite + **Human Review Gate** | Dependency evolution, full regression coverage, blast radius review |

业务技能也按实际任务动态加载；这些技能的源文件保留在 `skills/`，可发现入口位于 `.agents/skills/`：

| Trigger | Skill | Scope |
| :--- | :--- | :--- |
| FROZEN 日候选人工审查 | `ashares-decision-audit` | 只审查候选证据；不批准组合意图、不下单 |
| A 股新闻检索或 OpenSearch 分析 | `news-opensearch` | 按技能中的数据范围和查询约定操作 |
| QMT/MiniQMT 行情采集或 xtdata 排障 | `qmt-xtquant` | 仅 `xtdata` 行情数据；交易 API 不属于本项目技能范围 |
| Tick 盘口、流动性或执行指标分析 | `tick-microstructure` | 遵守 L1 快照数据能力边界 |

项目级静态边界门禁：`uv run python tools/check_paper_trading_boundary.py`。CI 与 pre-commit 均运行该门禁。

---

## 5. Handover Report Standard

When delivering a task, a concise delivery quality report must be provided alongside the response:
1. **Actual Scope of Modification**: List affected files and key functions/interfaces.
2. **Quality Gate Results**: State the four-state status (`PASS`, `FAIL`, `NOT RUN`, or `N/A`) for Linter, Type Check, and Unit Tests, with explanations.
3. **Unverified Risks**: Objectively list known risks stemming from environment limitations, missing test suites, or uncovered boundary conditions.
