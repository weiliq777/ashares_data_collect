-- 派生生命周期表的 effective_to 允许 NULL，但相同的当前区间不能重复。
-- PostgreSQL 15+ 使用 NULLS NOT DISTINCT 让 NULL 参与唯一性判断。
DELETE FROM std.security_lifecycle a
USING std.security_lifecycle b
WHERE a.ctid < b.ctid
  AND a.ts_code = b.ts_code
  AND a.effective_from = b.effective_from
  AND a.effective_to IS NOT DISTINCT FROM b.effective_to
  AND a.source = b.source;

CREATE UNIQUE INDEX IF NOT EXISTS uq_security_lifecycle_effective_interval
    ON std.security_lifecycle (ts_code, effective_from, effective_to, source)
    NULLS NOT DISTINCT;
