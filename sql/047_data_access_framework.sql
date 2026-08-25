-- Data Foundation Access Framework: durable checkpoint and database-side role boundary.
-- Apply with an administration role. Application roles are NOLOGIN; deployment
-- maps the runtime login to exactly one of them.

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS std;
CREATE SCHEMA IF NOT EXISTS pit;
CREATE SCHEMA IF NOT EXISTS feature;
CREATE SCHEMA IF NOT EXISTS contract;
CREATE SCHEMA IF NOT EXISTS research;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS ops.data_access_checkpoint (
    run_id VARCHAR(160) NOT NULL,
    job_name VARCHAR(160) NOT NULL,
    scope_key VARCHAR(500) NOT NULL,
    cursor_position BIGINT NOT NULL CHECK (cursor_position >= 0),
    done BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, job_name, scope_key)
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'raw_writer') THEN
        CREATE ROLE raw_writer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'standard_writer') THEN
        CREATE ROLE standard_writer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pit_writer') THEN
        CREATE ROLE pit_writer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'feature_writer') THEN
        CREATE ROLE feature_writer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'contract_publisher') THEN
        CREATE ROLE contract_publisher NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'research_reader') THEN
        CREATE ROLE research_reader NOLOGIN;
    END IF;
END $$;

REVOKE ALL ON SCHEMA public, raw, std, pit, feature, contract, research, mart FROM PUBLIC;
GRANT USAGE ON SCHEMA ops TO raw_writer, standard_writer, pit_writer, feature_writer,
    contract_publisher, research_reader;
GRANT SELECT, INSERT, UPDATE ON ops.data_access_checkpoint
    TO raw_writer, standard_writer, pit_writer, feature_writer;
GRANT SELECT ON ops.data_access_checkpoint TO contract_publisher, research_reader;

-- Raw is append-only at the database boundary. Concrete raw tables should be
-- granted INSERT/SELECT individually by deployment after schema inspection;
-- no role receives UPDATE/DELETE on raw facts.
GRANT USAGE ON SCHEMA raw TO raw_writer;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA raw TO raw_writer;
REVOKE UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA raw FROM raw_writer;

GRANT USAGE ON SCHEMA std TO standard_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA std TO standard_writer;
GRANT USAGE ON SCHEMA pit TO pit_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA pit TO pit_writer;
GRANT USAGE ON SCHEMA feature TO feature_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA feature TO feature_writer;

GRANT USAGE ON SCHEMA contract TO contract_publisher;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA contract TO contract_publisher;
REVOKE UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA contract FROM contract_publisher;

GRANT USAGE ON SCHEMA contract, research, mart TO research_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA contract, research, mart TO research_reader;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA contract, research, mart FROM research_reader;

-- Keep future tables inside the same boundary; deployments may narrow these
-- grants further for individual datasets.
ALTER DEFAULT PRIVILEGES IN SCHEMA raw
    GRANT SELECT, INSERT ON TABLES TO raw_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw
    REVOKE UPDATE, DELETE, TRUNCATE ON TABLES FROM raw_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA std
    GRANT SELECT, INSERT, UPDATE ON TABLES TO standard_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA pit
    GRANT SELECT, INSERT, UPDATE ON TABLES TO pit_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA feature
    GRANT SELECT, INSERT, UPDATE ON TABLES TO feature_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA contract
    GRANT SELECT, INSERT ON TABLES TO contract_publisher;
ALTER DEFAULT PRIVILEGES IN SCHEMA contract, research, mart
    GRANT SELECT ON TABLES TO research_reader;
