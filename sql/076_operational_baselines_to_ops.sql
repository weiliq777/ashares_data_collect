-- 将运维检查点与数据基线从 public（公共模式）收口到 ops（运维模式）。
-- 这是目录级迁移：不复制行、不重建索引、不扫描历史事实表，不会产生大事务。
BEGIN;

CREATE SCHEMA IF NOT EXISTS ops;

DO $$
BEGIN
    IF to_regclass('public.tushare_history_checkpoint') IS NOT NULL
       AND to_regclass('ops.tushare_history_checkpoint') IS NULL THEN
        ALTER TABLE public.tushare_history_checkpoint SET SCHEMA ops;
    ELSIF to_regclass('public.tushare_history_checkpoint') IS NOT NULL THEN
        RAISE EXCEPTION 'both public.tushare_history_checkpoint and ops.tushare_history_checkpoint exist';
    END IF;

    IF to_regclass('public.tushare_data_release') IS NOT NULL
       AND to_regclass('ops.tushare_data_release') IS NULL THEN
        ALTER TABLE public.tushare_data_release SET SCHEMA ops;
    ELSIF to_regclass('public.tushare_data_release') IS NOT NULL THEN
        RAISE EXCEPTION 'both public.tushare_data_release and ops.tushare_data_release exist';
    END IF;
END
$$;

GRANT SELECT, INSERT, UPDATE ON ops.tushare_history_checkpoint TO ashares_app;
GRANT SELECT, INSERT, UPDATE ON ops.tushare_data_release TO ashares_app;

DO $$
BEGIN
    IF to_regclass('ops.tushare_data_release_release_id_seq') IS NOT NULL THEN
        GRANT USAGE, SELECT ON SEQUENCE ops.tushare_data_release_release_id_seq TO ashares_app;
    ELSIF to_regclass('public.tushare_data_release_release_id_seq') IS NOT NULL THEN
        GRANT USAGE, SELECT ON SEQUENCE public.tushare_data_release_release_id_seq TO ashares_app;
    END IF;
END
$$;

COMMIT;
