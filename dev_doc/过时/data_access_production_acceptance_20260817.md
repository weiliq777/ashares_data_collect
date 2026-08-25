# Data Access Framework 生产库验收记录

日期：2026-08-17  
数据库：`ashares-postgres` / PostgreSQL 16.14 / database `ashares`

## 已执行

1. 以数据库管理员连接执行 `sql/047_data_access_framework.sql`。
2. 重复执行一次，确认 Schema、Checkpoint、角色和权限迁移可幂等重放。
3. 核对六个角色均已创建为 `NOLOGIN` 组角色。
4. 验证 `research_reader` 可以读取 `research.snapshot_manifest`。
5. 验证 `standard_writer` 直接读取旧 `public.daily_kline` 被数据库拒绝。
6. 确认代码契约扫描全量 137 个文件、0 违规；new-only 扫描也为 0 违规。

## 当前数据库事实

- 当前连接角色 `ashares` 是超级用户，不能作为业务运行账号。
- 现有 52 张旧表主要位于 `public` Schema。
- 新框架权限脚本保护的目标 Schema 是 `raw/std/pit/feature/contract/research/mart`。
- 因此六个业务角色已经存在并且对新 Schema 生效，但尚未接管现有应用连接；旧 `public` 表也尚未完成到目标 Schema 的映射。

## 单账号切换结果

已按当前单程序部署方式创建 `ashares_app`：

- `ashares` 超级用户保留，仅供人工建库、建表和迁移使用。
- 程序配置已切换到 `ashares_app`。
- `ashares_app` 为 `NOSUPERUSER`，不能建库、建角色、建表或执行 `TRUNCATE`。
- 现有 `public` Raw 表对 `ashares_app` 保留 `SELECT/INSERT`，撤销 `UPDATE/DELETE`。
- 普通程序连接实际验证通过：当前用户为 `ashares_app`，Raw 更新、建表和 `TRUNCATE` 均被拒绝。

仍需后续收口的部分：旧 `public` 表尚未整体迁移到目标 Schema；当前单账号方案因此是兼容性授权，不等同于六层数据库角色的完全隔离。后续可在不改变 Kernel 接口的情况下逐步收窄表级授权。
