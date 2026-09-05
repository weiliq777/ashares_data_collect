-- Bind formal Backtest Runs to the immutable application strategy version.
--
-- backtest.strategy_definition is a legacy migration-evidence table. New
-- application registrations live in backtest.app_strategy_version, whose
-- composite identity is (strategy_id, strategy_version). Existing runs are
-- preserved: their nullable strategy_version remains NULL and they are not
-- rewritten. New RUNNING/SUCCEEDED runs are required to bind an APPROVED
-- application version.

ALTER TABLE IF EXISTS backtest.run_manifest
    ADD COLUMN IF NOT EXISTS strategy_version VARCHAR(40);

ALTER TABLE IF EXISTS backtest.run_manifest
    DROP CONSTRAINT IF EXISTS run_manifest_strategy_id_fkey;

DO $$
BEGIN
    IF to_regclass('backtest.run_manifest') IS NULL
       OR to_regclass('backtest.app_strategy_version') IS NULL THEN
        RAISE EXCEPTION
            'backtest strategy-version cutover requires run_manifest and app_strategy_version';
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'backtest.run_manifest'::regclass
           AND conname = 'run_manifest_strategy_version_fkey'
    ) THEN
        ALTER TABLE backtest.run_manifest
            ADD CONSTRAINT run_manifest_strategy_version_fkey
            FOREIGN KEY (strategy_id, strategy_version)
            REFERENCES backtest.app_strategy_version(strategy_id, strategy_version)
            NOT VALID;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION backtest.assert_run_strategy_binding()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    registered_status VARCHAR(16);
BEGIN
    IF NEW.status IN ('RUNNING', 'SUCCEEDED') THEN
        IF NEW.strategy_version IS NULL OR btrim(NEW.strategy_version) = '' THEN
            RAISE EXCEPTION
                'formal Backtest Run requires strategy_version: %', NEW.run_id;
        END IF;

        SELECT approval_status
          INTO registered_status
          FROM backtest.app_strategy_version
         WHERE strategy_id = NEW.strategy_id
           AND strategy_version = NEW.strategy_version;
        IF NOT FOUND OR registered_status IS DISTINCT FROM 'APPROVED' THEN
            RAISE EXCEPTION
                'formal Backtest Run requires an APPROVED application strategy: %/%',
                NEW.strategy_id, NEW.strategy_version;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_run_strategy_binding
    ON backtest.run_manifest;
CREATE TRIGGER trg_backtest_run_strategy_binding
BEFORE INSERT OR UPDATE ON backtest.run_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_run_strategy_binding();

COMMENT ON COLUMN backtest.run_manifest.strategy_version IS
    'Immutable application strategy version bound to the formal run; NULL only on legacy runs preserved before strategy-version cutover.';
