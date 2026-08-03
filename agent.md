# Finance_cn Agent Notes

## 当前目标

以 Tushare 为主要数据源，建立可追溯、可断点续传的 A 股数据仓库，为基本面筛选、趋势分析、估值分析和风险计算提供事实数据。

数据链路必须保持：

```text
Tushare 原始接口 -> raw 原始表 -> 标准化表 -> 策略/分析层
```

策略层不得直接依赖接口字段，也不得用公告日之后才公开的数据回测历史交易日。

## 关键目录和文件

说明表名时必须同时附带业务含义；涉及 Raw 表时注明对应的 Tushare 接口和原始数据用途，避免只给出技术名称。

- `config.yaml`：数据库、Tushare、OpenSearch、RSSHub 配置
- `docker-compose.yml`：PostgreSQL、OpenSearch、Dashboards、Redis、RSSHub
- `data_collect/providers/tushare_client.py`：Tushare 客户端、限流和重试
- `data_collect/jobs/a_share_instrument_tushare.py`：股票基础资料
- `data_collect/jobs/a_share_daily_tushare.py`：每日增量日线
- `data_collect/jobs/a_share_valuation_tushare.py`：每日增量估值
- `data_collect/jobs/a_share_financial_tushare.py`：财务指标和财务原始表
- `data_collect/jobs/tushare_history_init.py`：历史初始化、断点、批次暂停
- `data_collect/jobs/tushare_daily_incremental.py`：历史兼容的 Raw-first 每日增量
- `data_collect/jobs/tushare_market_incremental_by_date.py`：正式每日增量，按交易日批量获取全市场 daily/daily_basic/adj_factor
- `data_collect/jobs/tushare_raw_to_standard.py`：Raw 离线重建 Standard
- `data_collect/jobs/tushare_rebuild_validation.py`：Raw 到独立验证表的全量重建验收
- `data_collect/jobs/tushare_data_quality.py`：阶段一聚合质量检查
- `data_collect/normalize/`：接口数据到标准模型的转换
- `data_collect/utils/db.py`：PostgreSQL 写入、幂等和表结构对齐
- `sql/012_create_tushare_standard.sql`：标准表和已接入财务原始表
- `sql/013_create_tushare_history.sql`：交易日历、复权因子和初始化断点表
- `sql/014_create_tushare_market_raw.sql`：行情、估值、复权因子、交易日历原始表
- `dev_doc/current_data_model.md`：当前完整表模型和数据来源说明
- `dev_doc/tushare_history_initialization.md`：历史初始化和每日同步流程
- `dev_doc/agent_tasks.md`：简明任务清单、状态和下一步

## 数据库端口

Windows 保留了 5432、5601、2100 等端口，当前主机映射为：

- PostgreSQL `127.0.0.1:15432`
- OpenSearch `127.0.0.1:9200`
- OpenSearch Dashboards `127.0.0.1:15601`
- RSSHub `127.0.0.1:11200`

## 操作约束

1. Token 只从 `TUSHARE_API_KEY` 环境变量读取，不写入代码、文档或日志。
2. 新增数据源时必须同时考虑 raw 表、标准表、来源字段、抓取时间和幂等键。
3. 历史数据必须分批、限流、可断点续传；不能并发启动多个全量任务。
4. 清理数据前先确认目标表，默认保留基础资料和新闻数据。
5. 修改数据库模型后，必须同步更新 `current_data_model.md` 和对应 SQL 迁移文件。
6. 原始数据用于重算和审计，策略代码优先读取标准化表。

## Agent 工作模式

所有持续性工作都在 `dev_doc/agent_tasks.md` 中登记一条任务，保持简明：

```markdown
## T-编号｜任务名称

- 时间：YYYY-MM-DD
- 目标：一句话说明
- 状态：待开始 / 进行中 / 已完成 / 已阻塞
- 当前结果：一句话说明
- 下一步：一句话说明
```

开始工作时更新为“进行中”；真正完成后更新为“已完成”；遇到无法继续的外部阻碍时才使用“已阻塞”。每次涉及新的长期目标，都追加新任务，不覆盖历史任务。

## Tushare 原始数据强制规则

以后所有 Tushare 接口请求都必须先保存一份完整原始响应到对应的 `*_raw` 表，再进行标准化转换。禁止接口请求后直接写入标准表。

Raw 表是不可变事实层：任何任务不得删除、更新、覆盖或清理 `*_raw` 表中的历史记录。字段映射、单位转换和错误修复只能通过重新生成 Standard 表完成；如需修复 Raw，必须新增版本记录或单独建立迁移方案，不得直接改写原始数据。

原因：Tushare 接口会消耗积分；保留完整 raw 数据后，字段映射、单位换算或标准模型发生错误时，可以直接从数据库重新转换和恢复，不必重复请求接口。

新增 Tushare 接口时必须同步完成：

1. 建立对应 raw 表，保留原始 JSON、业务主键、来源和抓取时间。
2. 使用 raw 表主键和幂等写入，避免重复数据。
3. 再建立独立的 raw 到标准表转换逻辑。
4. 在 `current_data_model.md` 中记录接口、raw 表、标准表和字段映射关系。

故障预防：分批读取数据库时，不能在同一游标上执行 `fetchmany` 后再执行写入 SQL；应使用独立读写连接或先安全缓存批次。Tushare 网络 EOF 必须记录失败 checkpoint，并按业务键续传，不能把已完成批次全部重跑。

每日增量规则：最近 5 个交易日允许重复回补；正式任务使用 `data_collect.jobs.tushare_market_incremental_by_date` 按交易日获取全市场数据，不得逐股票请求；每次抓取完整响应写入不可变 `tushare_market_raw_version`，现有 `*_raw` 只做首次业务记录的幂等追加；Standard 按交易日重建。历史初始化仍可按股票断点续传。不得用 checkpoint=done 永久跳过需要修复的回补日期。
