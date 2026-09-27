-- Multi-baseline Backtest Dataset composition.
-- This stores only immutable references/ranges; it does not copy Dataset facts.
BEGIN;

ALTER TABLE backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS dataset_kind VARCHAR(20) NOT NULL DEFAULT 'MATERIALIZED';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid='backtest.dataset_manifest'::regclass
           AND conname='backtest_dataset_kind_check'
    ) THEN
        ALTER TABLE backtest.dataset_manifest
            ADD CONSTRAINT backtest_dataset_kind_check
            CHECK (dataset_kind IN ('MATERIALIZED','COMPOSITE'));
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS backtest.dataset_component (
    composite_dataset_id UUID NOT NULL
        REFERENCES backtest.dataset_manifest(dataset_id),
    ordinal SMALLINT NOT NULL CHECK (ordinal >= 0),
    component_dataset_id UUID NOT NULL
        REFERENCES backtest.dataset_manifest(dataset_id),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    component_input_manifest_hash CHAR(64) NOT NULL,
    component_manifest_hash CHAR(64) NOT NULL,
    component_baseline_tag VARCHAR(100) NOT NULL,
    component_baseline_quality_report_hash CHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (composite_dataset_id, ordinal),
    UNIQUE (composite_dataset_id, component_dataset_id),
    CHECK (composite_dataset_id <> component_dataset_id),
    CHECK (effective_from <= effective_to)
);

CREATE INDEX IF NOT EXISTS idx_backtest_dataset_component_child
    ON backtest.dataset_component (component_dataset_id, effective_from, effective_to);

CREATE OR REPLACE FUNCTION backtest.assert_dataset_component_mutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    parent_id UUID;
    child_id UUID;
    parent_status VARCHAR(20);
    parent_kind VARCHAR(20);
    child_status VARCHAR(20);
    child_kind VARCHAR(20);
BEGIN
    parent_id := CASE WHEN TG_OP='DELETE'
                      THEN OLD.composite_dataset_id ELSE NEW.composite_dataset_id END;
    SELECT status, dataset_kind INTO parent_status, parent_kind
      FROM backtest.dataset_manifest WHERE dataset_id=parent_id;
    IF parent_status IS NULL OR parent_kind <> 'COMPOSITE' THEN
        RAISE EXCEPTION 'dataset component parent must be COMPOSITE: %', parent_id;
    END IF;
    IF parent_status IN ('VALIDATED','FROZEN') THEN
        RAISE EXCEPTION 'validated/frozen Dataset composition is immutable: %', parent_id;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        child_id := NEW.component_dataset_id;
        SELECT status, dataset_kind INTO child_status, child_kind
          FROM backtest.dataset_manifest WHERE dataset_id=child_id;
        IF child_status <> 'FROZEN' OR child_kind <> 'MATERIALIZED' THEN
            RAISE EXCEPTION
                'Dataset component must reference a FROZEN MATERIALIZED Dataset: %',
                child_id;
        END IF;
        IF NEW.effective_from < (SELECT warmup_start FROM backtest.dataset_manifest WHERE dataset_id=child_id)
           OR NEW.effective_to > (SELECT evaluation_end FROM backtest.dataset_manifest WHERE dataset_id=child_id) THEN
            RAISE EXCEPTION 'Dataset component range exceeds child coverage: %', child_id;
        END IF;
        RETURN NEW;
    END IF;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_dataset_component_mutable
    ON backtest.dataset_component;
CREATE TRIGGER trg_backtest_dataset_component_mutable
BEFORE INSERT OR UPDATE OR DELETE ON backtest.dataset_component
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_component_mutable();

CREATE OR REPLACE FUNCTION backtest.assert_dataset_kind_immutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.dataset_kind IS DISTINCT FROM OLD.dataset_kind THEN
        RAISE EXCEPTION 'backtest dataset kind is immutable: %', OLD.dataset_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_backtest_dataset_kind_immutable
    ON backtest.dataset_manifest;
CREATE TRIGGER trg_backtest_dataset_kind_immutable
BEFORE UPDATE OF dataset_kind ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_kind_immutable();

COMMIT;
