-- Allow an explicit, auditable rebuild of a VALIDATED but not FROZEN Dataset.
-- The application must transition VALIDATED -> FAILED with a non-empty reason,
-- clear all materialization/code identity fields, and then rebuild through the
-- normal FAILED -> BUILDING path. FROZEN remains immutable.

BEGIN;

CREATE OR REPLACE FUNCTION backtest.assert_dataset_manifest_state()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'backtest dataset manifest is append-only: %', OLD.dataset_id;
    END IF;
    IF OLD.status = 'FROZEN' THEN
        RAISE EXCEPTION 'FROZEN backtest dataset is immutable: %', OLD.dataset_id;
    END IF;
    IF OLD.status = 'VALIDATED' AND NEW.status = 'VALIDATED' THEN
        RAISE EXCEPTION 'VALIDATED backtest dataset is immutable before freeze: %', OLD.dataset_id;
    END IF;

    IF OLD.status = 'VALIDATED' AND NEW.status = 'FAILED' THEN
        IF NULLIF(BTRIM(COALESCE(NEW.failure_reason, '')), '') IS NULL THEN
            RAISE EXCEPTION 'pre-freeze Dataset rebuild requires failure_reason: %', OLD.dataset_id;
        END IF;
        IF NEW.dataset_id IS DISTINCT FROM OLD.dataset_id
           OR NEW.dataset_version IS DISTINCT FROM OLD.dataset_version
           OR NEW.dataset_name IS DISTINCT FROM OLD.dataset_name
           OR NEW.data_baseline_tag IS DISTINCT FROM OLD.data_baseline_tag
           OR NEW.baseline_quality_report_hash IS DISTINCT FROM OLD.baseline_quality_report_hash
           OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
           OR NEW.adapter_version IS DISTINCT FROM OLD.adapter_version
           OR NEW.feature_version IS DISTINCT FROM OLD.feature_version
           OR NEW.price_adjustment_mode IS DISTINCT FROM OLD.price_adjustment_mode
           OR NEW.warmup_start IS DISTINCT FROM OLD.warmup_start
           OR NEW.evaluation_start IS DISTINCT FROM OLD.evaluation_start
           OR NEW.evaluation_end IS DISTINCT FROM OLD.evaluation_end
           OR NEW.calendar_rule_version IS DISTINCT FROM OLD.calendar_rule_version
           OR NEW.market_rule_version IS DISTINCT FROM OLD.market_rule_version
           OR NEW.fee_rule_version IS DISTINCT FROM OLD.fee_rule_version
           OR NEW.out_of_sample_start IS DISTINCT FROM OLD.out_of_sample_start
           OR NEW.benchmark_code IS DISTINCT FROM OLD.benchmark_code
           OR NEW.selection_predicate IS DISTINCT FROM OLD.selection_predicate
           OR NEW.selected_columns IS DISTINCT FROM OLD.selected_columns
           OR NEW.hash_algorithm IS DISTINCT FROM OLD.hash_algorithm
           OR NEW.availability_rules IS DISTINCT FROM OLD.availability_rules
           OR NEW.created_at IS DISTINCT FROM OLD.created_at
           OR NEW.benchmark_source_manifest IS DISTINCT FROM OLD.benchmark_source_manifest
           OR NEW.benchmark_source_manifest_hash IS DISTINCT FROM OLD.benchmark_source_manifest_hash THEN
            RAISE EXCEPTION 'validated Dataset definition cannot change during pre-freeze rebuild: %', OLD.dataset_id;
        END IF;
        IF NEW.validated_at IS NOT NULL
           OR NEW.quality_report IS NOT NULL
           OR NEW.quality_report_hash IS NOT NULL
           OR NEW.calendar_hash IS NOT NULL
           OR NEW.rules_hash IS NOT NULL
           OR NEW.execution_hash IS NOT NULL
           OR NEW.corporate_action_hash IS NOT NULL
           OR NEW.benchmark_hash IS NOT NULL
           OR NEW.input_manifest_hash IS NOT NULL
           OR NEW.code_version IS NOT NULL
           OR NEW.git_commit IS NOT NULL
           OR NEW.git_dirty IS NOT FALSE
           OR NEW.diff_hash IS NOT NULL
           OR NEW.code_archive_path IS NOT NULL
           OR NEW.code_archive_sha256 IS NOT NULL
           OR NEW.code_archive_size_bytes IS NOT NULL THEN
            RAISE EXCEPTION 'pre-freeze Dataset rebuild must clear materialization identity: %', OLD.dataset_id;
        END IF;
    END IF;

    IF OLD.status = 'VALIDATED' AND NEW.status = 'FROZEN' THEN
        IF NEW.dataset_id IS DISTINCT FROM OLD.dataset_id
           OR NEW.dataset_version IS DISTINCT FROM OLD.dataset_version
           OR NEW.dataset_name IS DISTINCT FROM OLD.dataset_name
           OR NEW.data_baseline_tag IS DISTINCT FROM OLD.data_baseline_tag
           OR NEW.baseline_quality_report_hash IS DISTINCT FROM OLD.baseline_quality_report_hash
           OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
           OR NEW.adapter_version IS DISTINCT FROM OLD.adapter_version
           OR NEW.feature_version IS DISTINCT FROM OLD.feature_version
           OR NEW.price_adjustment_mode IS DISTINCT FROM OLD.price_adjustment_mode
           OR NEW.warmup_start IS DISTINCT FROM OLD.warmup_start
           OR NEW.evaluation_start IS DISTINCT FROM OLD.evaluation_start
           OR NEW.evaluation_end IS DISTINCT FROM OLD.evaluation_end
           OR NEW.calendar_rule_version IS DISTINCT FROM OLD.calendar_rule_version
           OR NEW.market_rule_version IS DISTINCT FROM OLD.market_rule_version
           OR NEW.fee_rule_version IS DISTINCT FROM OLD.fee_rule_version
           OR NEW.calendar_hash IS DISTINCT FROM OLD.calendar_hash
           OR NEW.rules_hash IS DISTINCT FROM OLD.rules_hash
           OR NEW.execution_hash IS DISTINCT FROM OLD.execution_hash
           OR NEW.corporate_action_hash IS DISTINCT FROM OLD.corporate_action_hash
           OR NEW.input_manifest_hash IS DISTINCT FROM OLD.input_manifest_hash
           OR NEW.selection_predicate IS DISTINCT FROM OLD.selection_predicate
           OR NEW.selected_columns IS DISTINCT FROM OLD.selected_columns
           OR NEW.hash_algorithm IS DISTINCT FROM OLD.hash_algorithm
           OR NEW.availability_rules IS DISTINCT FROM OLD.availability_rules
           OR NEW.created_at IS DISTINCT FROM OLD.created_at
           OR NEW.built_at IS DISTINCT FROM OLD.built_at
           OR NEW.validated_at IS DISTINCT FROM OLD.validated_at
           OR NEW.failed_at IS DISTINCT FROM OLD.failed_at
           OR NEW.failure_reason IS DISTINCT FROM OLD.failure_reason
           OR NEW.notes IS DISTINCT FROM OLD.notes THEN
            RAISE EXCEPTION 'validated Dataset identity cannot change during freeze: %', OLD.dataset_id;
        END IF;
    END IF;

    IF NEW.status <> OLD.status AND NOT (
        (OLD.status = 'DRAFT' AND NEW.status IN ('BUILDING','FAILED')) OR
        (OLD.status = 'FAILED' AND NEW.status IN ('BUILDING','FAILED')) OR
        (OLD.status = 'BUILDING' AND NEW.status IN ('VALIDATED','FAILED')) OR
        (OLD.status = 'VALIDATED' AND NEW.status IN ('FROZEN','FAILED'))
    ) THEN
        RAISE EXCEPTION 'invalid backtest dataset status transition: % -> %', OLD.status, NEW.status;
    END IF;

    IF NEW.status = 'FROZEN' THEN
        IF OLD.status <> 'VALIDATED' OR NEW.input_manifest_hash IS NULL
           OR NEW.quality_report_hash IS NULL
           OR NEW.baseline_quality_report_hash IS NULL THEN
            RAISE EXCEPTION 'only validated dataset with hashes can be frozen: %', NEW.dataset_id;
        END IF;
        IF NEW.git_dirty AND NEW.code_archive_path IS NULL THEN
            RAISE EXCEPTION 'dirty code requires an explicit code archive before freezing: %', NEW.dataset_id;
        END IF;
        NEW.frozen_at := CURRENT_TIMESTAMP;
    ELSIF NEW.status = 'BUILDING' AND OLD.status IN ('DRAFT','FAILED') THEN
        NEW.built_at := CURRENT_TIMESTAMP;
    ELSIF NEW.status = 'VALIDATED' THEN
        NEW.validated_at := CURRENT_TIMESTAMP;
    ELSIF NEW.status = 'FAILED' AND OLD.status = 'VALIDATED' THEN
        NEW.failed_at := CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$;

COMMIT;
