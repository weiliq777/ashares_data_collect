-- Single application login for the current one-process deployment.
-- Run with psql and provide the secret out of band:
--   psql -v app_password='...' -f 048_data_access_app_role.sql
-- The administrative ashares role remains a superuser and is not used by jobs.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ashares_app') THEN
        CREATE ROLE ashares_app LOGIN;
    END IF;
END $$;

ALTER ROLE ashares_app WITH LOGIN PASSWORD :'app_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS INHERIT;

GRANT CONNECT ON DATABASE ashares TO ashares_app;
GRANT USAGE ON SCHEMA public, ops, raw, std, pit, feature, contract, research, mart, backtest, portfolio TO ashares_app;
REVOKE CREATE ON SCHEMA public, ops, raw, std, pit, feature, contract, research, mart, backtest, portfolio FROM ashares_app;

-- Compatibility privileges for the current database layout. New tables should
-- be placed in the framework schemas and receive narrower default privileges.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public, ops, std, pit, feature, research, backtest, portfolio TO ashares_app;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA raw, contract TO ashares_app;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO ashares_app;
REVOKE TRUNCATE ON ALL TABLES IN SCHEMA public, ops, raw, std, pit, feature, contract, research, mart, backtest, portfolio FROM ashares_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public, ops, raw, std, pit, feature, contract, research, mart, backtest, portfolio TO ashares_app;

-- Raw facts remain append-only even though the current legacy tables are in
-- public. The pattern covers *_raw, *_raw_version and raw_* names.
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ('public', 'raw')
          AND table_name ~ '(^|_)raw($|_)'
    LOOP
        EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON TABLE %I.%I FROM ashares_app', r.table_schema, r.table_name);
    END LOOP;
END $$;

ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA raw
    GRANT SELECT, INSERT ON TABLES TO ashares_app;
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA raw
    REVOKE UPDATE, DELETE, TRUNCATE ON TABLES FROM ashares_app;
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA std
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ashares_app;
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA pit
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ashares_app;
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA feature
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ashares_app;
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA research
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ashares_app;
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA ops
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ashares_app;
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA backtest
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ashares_app;
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA portfolio
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ashares_app;
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA contract
    GRANT SELECT, INSERT ON TABLES TO ashares_app;
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA mart
    GRANT SELECT ON TABLES TO ashares_app;
