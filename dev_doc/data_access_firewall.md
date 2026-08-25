# Data Access Firewall

`data_access_kernel.md` 是唯一设计基线：文档不是参考资料，而是实现契约；代码与文档冲突时，默认代码错误。

迁移期间，CI 必须执行：

```text
python tools/check_data_access_contract.py --ci --new-only --base origin/main
```

本地提交前可执行不带 `--base` 的版本。它同时检查暂存区和工作区，修改文件只检查新增行，新文件按全文检查。
门禁拒绝业务代码导入旧数据库入口或 Driver、持有 Connection/Cursor 参数、调用 `execute`/`executemany`、使用
`pandas.read_sql`、手写 SELECT/INSERT/UPDATE/DELETE/CREATE TABLE SQL 或手工事务。这样历史债务可以分批清理，但新的违规访问不能进入主线。

迁移收口必须执行全仓门禁：

```text
python tools/check_data_access_contract.py --ci
```

只有 `data_collect/data_access/` 及其 Repository 可以实现 SQL 和 Driver 访问。门禁是架构约束的一部分，不是可选报告；
任何新访问都必须先进入 Repository → Kernel，并补齐对应行为测试，不能通过新增万能 `execute()` 绕过类型边界。
