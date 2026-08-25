-- Close the physical-migration state machine for databases that already ran 053.
-- Metadata-only migration; it does not copy, update, or delete foundation data.

CREATE OR REPLACE FUNCTION ops.guard_foundation_table_migration()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := CURRENT_TIMESTAMP;
    IF TG_OP = 'INSERT' AND NEW.status <> 'REGISTERED' THEN
        RAISE EXCEPTION 'new migration must start in REGISTERED state';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.status IS DISTINCT FROM OLD.status AND NOT (
        (OLD.status = 'REGISTERED' AND NEW.status IN ('DUAL_READ', 'DUAL_WRITE', 'VALIDATED', 'ABORTED')) OR
        (OLD.status = 'DUAL_READ' AND NEW.status IN ('DUAL_WRITE', 'VALIDATED', 'ABORTED')) OR
        (OLD.status = 'DUAL_WRITE' AND NEW.status IN ('VALIDATED', 'ABORTED')) OR
        (OLD.status = 'VALIDATED' AND NEW.status = 'CUTOVER') OR
        (OLD.status = 'CUTOVER' AND NEW.status = 'ROLLED_BACK')
    ) THEN
        RAISE EXCEPTION 'invalid migration transition: % -> %', OLD.status, NEW.status;
    END IF;
    RETURN NEW;
END;
$$;
