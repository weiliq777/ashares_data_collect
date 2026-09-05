-- Bind a narrowly-scoped late-arriving benchmark repair without mutating the
-- validated data baseline.  A repair is auxiliary Dataset input only: the
-- Research Snapshot and all foundation source boundaries remain unchanged.

ALTER TABLE backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS benchmark_source_manifest JSONB,
    ADD COLUMN IF NOT EXISTS benchmark_source_manifest_hash CHAR(64);

CREATE OR REPLACE FUNCTION backtest.assert_dataset_benchmark_source_manifest()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.benchmark_source_manifest_hash IS NOT NULL THEN
        IF NEW.benchmark_source_manifest IS NULL
           OR length(btrim(NEW.benchmark_source_manifest_hash)) <> 64 THEN
            RAISE EXCEPTION 'benchmark source repair requires a manifest and 64-char hash: %', NEW.dataset_id;
        END IF;
        -- The application computes the canonical compact JSON hash.  The
        -- trigger validates presence/shape; PostgreSQL's JSON serialization
        -- is intentionally not used as the application canonicalizer.
        IF jsonb_typeof(NEW.benchmark_source_manifest) <> 'object' THEN
            RAISE EXCEPTION 'benchmark source repair manifest must be a JSON object: %', NEW.dataset_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dataset_benchmark_source_manifest
    ON backtest.dataset_manifest;
CREATE TRIGGER trg_dataset_benchmark_source_manifest
BEFORE INSERT OR UPDATE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_benchmark_source_manifest();

COMMENT ON COLUMN backtest.dataset_manifest.benchmark_source_manifest IS
    'Explicit late-arriving benchmark repair inputs; does not change the bound data baseline.';
COMMENT ON COLUMN backtest.dataset_manifest.benchmark_source_manifest_hash IS
    'SHA-256 of canonical benchmark_source_manifest JSON.';
