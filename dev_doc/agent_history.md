# Agent 历史归档

> 本文件由 `agent.md` 的历史流水账整理而来。内容用于审计、复盘和追溯，不是当前运行指令。带日期的进程、数量、测试结果和“当前状态”均只代表记录当时的观察；如与当前数据库、`dev_doc/agent_tasks.md`、专门设计契约或实际检查结果冲突，以后者为准。

## 2026-08-04：阶段二质量审计与故障记录

- 审计发现 216 只股票的历史日线有数据但复权因子不完整；曾新增 `tushare_adj_factor_gap_repair.py`（复权因子缺口按股票检查点修复任务）追加 Raw，随后重建 `adj_factor_daily`（复权因子标准表）和价格 Feature，并发布 `baseline_20260731`（历史数据基线）。这些是当时的运行记录，不代表当前数据库仍保持相同状态。
- `tushare_phase2_raw_init.py --retry-empty`（阶段二 Raw 空响应重试任务）曾错误地把所有已有 checkpoint 当作可重试对象，可能重复请求已完成日期；后续改为只重试 `status='empty'`。Raw 不可变原则保持有效。

## 2026-08-06 至 2026-08-08：阶段三审计与生产准备记录

- 曾完成风险压力模型、Tushare 执行单位归一化、成交量参与率整笔拒单、Monte Carlo（蒙特卡洛）双方法和 Backtest→Portfolio 人工隔离；对应迁移为 `sql/032_backtest_portfolio_airgap.sql`。
- 当时的阶段三审计记录称正式 Backtest 使用冻结 Dataset，Portfolio 保持 `PENDING_APPROVAL → PAPER_MANUAL`，禁止连接券商和自动下单。
- 2026-08-08 的旧审计记录中，`research-v1.2` 尚无冻结快照，`research-v1.1` 存在缺失交易日和缺失质量哈希，旧 `dataset-v1`/`dataset-v1.1` 仅作为不可变历史记录；这些状态必须以当前数据库实际查询重新确认。
- 当时曾有 `2016-08-01`–`2021-08-01` 的长时间 Raw 初始化任务运行，记录要求不要仅凭进度日志判断成功。该进程状态已过期，不得据此启动、停止或恢复任何任务。
- 当时的 `baseline_20260803` 虽标记为 `validated`，但 Manifest 为空，Snapshot 生成器会拒绝使用；该状态同样只供历史审计参考。

## 2026-08-09：批量写入事故与修复记录

- 曾因 Dataset 执行行情一次性写入数百万行长事务，造成 WAL、死元组、索引维护和 Docker/WSL 内存压力，阶段级 checkpoint 也无法在 Build 中途续传。
- 后续约束为：按日期/业务分区拆批、单批独立提交、checkpoint/心跳/幂等键/失败重试、禁止共享表全局 `TRUNCATE`、不使用多线程直接堆写 PostgreSQL。
- 对应实现为 `sql/043_backtest_dataset_batch_checkpoint.sql`、`data_collect/jobs/backtest_dataset_batched.py` 和 `data_collect/jobs/backtest_dataset_handoff.py` 的 `--batch-days`。
- `pit.financial_asof`（财务时点表）曾由全量 `TRUNCATE + INSERT` 改为按日期窗口写入构建表、每窗口独立提交，最后短事务切换；失败只清理构建表，不影响 Raw 或旧 PIT。

## 2026-08-09 至 2026-08-29：数据层迁移与运行规则记录

- `public.daily_kline`（旧公共日线行情表）、`public.adj_factor_daily`（旧公共复权因子表）和 `public.valuation_daily`（旧公共估值表）曾完成 524 个日期窗口校验并切换到 `std.*`（标准模式）正式表；旧表改名为 `public._legacy_*`（旧表备份），供回滚和审计使用。
- 四张财务 Standard 表曾完成按主键窗口复制、分块哈希校验和切换到 `std.financial_income_standard`（标准利润表）、`std.financial_balance_sheet_standard`（标准资产负债表）、`std.financial_cash_flow_standard`（标准现金流量表）和 `std.fina_indicator_standard`（标准财务指标表）。
- `public.stock_financial_indicator`（旧公共财务指标兼容表）曾切换到 `std.stock_financial_indicator`（标准财务指标兼容表）；核心财务 v2 不依赖该兼容表，旧备份清理需以当前适配器引用和保留周期重新确认。
- `public.trading_calendar`（旧公共交易日历表）和 `public.stock_basic_info`（旧公共股票基础信息表）曾切换为 `std.trading_calendar`（标准交易日历表）和 `std.stock_basic_info`（标准股票基础信息表）。
- `financial_change_queue`（财务变更队列）、`financial_ingest_anomaly`（财务采集异常）和 `financial_version_cutover_watermark`（财务版本切换水位）曾移动到 `ops`（运维模式）；`tushare_history_checkpoint`（历史检查点表）和 `tushare_data_release`（数据发布基线表）也曾移动为 `ops.*`。这些迁移记录不替代当前 Schema 查询。
- 曾撤销程序账号对旧公共业务表和 Raw 备份表的权限，保留超级用户用于人工运维；当前权限必须以数据库实际授权查询为准。
- 曾修复财务 Standard Upsert（幂等写入）键、同日财务修订的 `WARNING`（警告）语义和财务链路统一排他锁；当前代码和数据库约束仍须用契约测试验证。
- 2026-08-28 记录了分阶段重建原则，2026-08-29 记录了 checkpoint 与临时物理状态一致性原则；两条已提炼进当前 `agent.md` 的长期执行纪律。

## 2026-09-01：行业接口抽样记录

- `tools/probe_tushare_index_member_all.py`（申万行业成分接口只读探针）实测 `000001.SZ`（平安银行）和 `857831.SI`（申万三级股份制银行Ⅲ）：`is_new=Y`（最新成员）可返回当前成员及 `in_date/out_date`（纳入/剔除日期）字段，`is_new=N`（非最新成员）未返回历史记录。
- 该结果只能说明当前账号/数据集没有返回可用的非最新历史记录，不能把当前成员表当作五年行业 PIT。历史行业缺失时 Observation 保持 `UNKNOWN`，不阻塞核心数据底座。

## 归档前曾存在但已移出的旧目录说明

旧 `agent.md` 曾列出 `data_collect/utils/db.py`（旧数据库工具入口）以及大量已完成迁移脚本。数据访问契约已经禁止业务层依赖旧万能入口；当前是否删除、归档或保留某个文件，必须以代码引用扫描、Git 状态和专门迁移文档为准，不能以本历史清单直接操作。
