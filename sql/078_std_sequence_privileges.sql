-- Standard schema sequence privileges.
-- New identity/serial sequences created after the historical grants in 048
-- also need to be usable by the application account for INSERT/UPSERT.
BEGIN;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA std TO ashares_app;

-- Future sequences created by the database owner in std inherit the same
-- minimum privilege.
ALTER DEFAULT PRIVILEGES FOR ROLE ashares IN SCHEMA std
    GRANT USAGE, SELECT ON SEQUENCES TO ashares_app;

COMMIT;
