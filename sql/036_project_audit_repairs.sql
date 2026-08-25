-- T-037: whole-project correctness, source-boundary and audit repairs.
-- This migration only extends derived/audit structures and appends missing
-- stock_basic versions.  It never UPDATEs/DELETEs any Raw fact.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE ops.tushare_data_release
    ADD COLUMN IF NOT EXISTS source_manifest JSONB,
    ADD COLUMN IF NOT EXISTS source_manifest_hash CHAR(64);

ALTER TABLE research.snapshot_manifest
    ADD COLUMN IF NOT EXISTS baseline_source_manifest_hash CHAR(64);

ALTER TABLE backtest.dataset_manifest
    ADD COLUMN IF NOT EXISTS baseline_source_manifest_hash CHAR(64);

ALTER TABLE fina_indicator_standard
    ADD COLUMN IF NOT EXISTS vintage_quality VARCHAR(50),
    ADD COLUMN IF NOT EXISTS source_observed_at TIMESTAMP;
ALTER TABLE financial_income_standard
    ADD COLUMN IF NOT EXISTS vintage_quality VARCHAR(50),
    ADD COLUMN IF NOT EXISTS source_observed_at TIMESTAMP;
ALTER TABLE financial_balance_sheet_standard
    ADD COLUMN IF NOT EXISTS vintage_quality VARCHAR(50),
    ADD COLUMN IF NOT EXISTS source_observed_at TIMESTAMP;
ALTER TABLE financial_cash_flow_standard
    ADD COLUMN IF NOT EXISTS vintage_quality VARCHAR(50),
    ADD COLUMN IF NOT EXISTS source_observed_at TIMESTAMP;
ALTER TABLE pit.financial_asof
    ADD COLUMN IF NOT EXISTS vintage_quality VARCHAR(50),
    ADD COLUMN IF NOT EXISTS source_observed_at TIMESTAMP;
ALTER TABLE feature.company_financial_v1
    ADD COLUMN IF NOT EXISTS vintage_quality VARCHAR(50),
    ADD COLUMN IF NOT EXISTS source_observed_at TIMESTAMP;

ALTER TABLE research.stock_snapshot_daily
    ADD COLUMN IF NOT EXISTS financial_vintage_quality VARCHAR(50),
    ADD COLUMN IF NOT EXISTS financial_source_observed_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS instrument_source_record_key VARCHAR(180),
    ADD COLUMN IF NOT EXISTS instrument_source_record_version BIGINT,
    ADD COLUMN IF NOT EXISTS instrument_source_payload_hash CHAR(64);

ALTER TABLE backtest.performance_daily
    ADD COLUMN IF NOT EXISTS cash_receivable NUMERIC(20,6) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stock_receivable_value NUMERIC(20,6) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS receivable_total NUMERIC(20,6) NOT NULL DEFAULT 0;

ALTER TABLE research.evidence_reference
    ADD COLUMN IF NOT EXISTS authority_level VARCHAR(30) NOT NULL DEFAULT 'OTHER';
ALTER TABLE research.evidence_reference
    DROP CONSTRAINT IF EXISTS evidence_reference_authority_level_check;
ALTER TABLE research.evidence_reference
    ADD CONSTRAINT evidence_reference_authority_level_check
    CHECK (authority_level IN ('OFFICIAL','PROFESSIONAL','MEDIA','INTERNAL_FACT','OTHER'));

-- sql/035 creates the append-only table.  Backfill legacy first-seen payloads
-- only when no identical payload exists; the legacy Raw table remains untouched.
INSERT INTO tushare_stock_basic_raw_version
    (ts_code,business_key,payload,payload_hash,source,batch_id,fetched_at)
SELECT r.ts_code, r.ts_code, r.payload,
       encode(digest(convert_to(r.payload::text,'UTF8'),'sha256'),'hex'),
       'tushare', 'migration-036-stock-basic-backfill', r.fetched_at
  FROM tushare_stock_basic_raw r
ON CONFLICT (business_key,payload_hash) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_release_source_manifest_hash
    ON ops.tushare_data_release(source_manifest_hash);
CREATE INDEX IF NOT EXISTS idx_fina_indicator_vintage_available
    ON fina_indicator_standard(ts_code, version_available_date, source_observed_at);
CREATE INDEX IF NOT EXISTS idx_financial_income_vintage_available
    ON financial_income_standard(stock_code, version_available_date, source_observed_at);
CREATE INDEX IF NOT EXISTS idx_financial_cashflow_vintage_available
    ON financial_cash_flow_standard(stock_code, version_available_date, source_observed_at);

CREATE OR REPLACE FUNCTION public.assert_release_source_manifest()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status='validated' AND (
        NEW.source_manifest IS NULL OR NEW.source_manifest_hash IS NULL
        OR length(btrim(NEW.source_manifest_hash)) <> 64
    ) THEN
        RAISE EXCEPTION 'validated release requires immutable Raw source manifest: %', NEW.release_tag;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_release_source_manifest ON ops.tushare_data_release;
CREATE TRIGGER trg_release_source_manifest
BEFORE INSERT OR UPDATE ON ops.tushare_data_release
FOR EACH ROW EXECUTE FUNCTION public.assert_release_source_manifest();

CREATE OR REPLACE FUNCTION research.assert_snapshot_source_boundary()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE release_hash CHAR(64);
BEGIN
    IF NEW.status='FROZEN' AND NEW.schema_version='research-v1.2' THEN
        SELECT source_manifest_hash INTO release_hash
          FROM ops.tushare_data_release WHERE release_tag=NEW.data_baseline_tag;
        IF NEW.baseline_source_manifest_hash IS NULL
           OR release_hash IS NULL
           OR btrim(NEW.baseline_source_manifest_hash) <> btrim(release_hash) THEN
            RAISE EXCEPTION 'research-v1.2 snapshot Raw boundary mismatch: %', NEW.snapshot_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_snapshot_source_boundary ON research.snapshot_manifest;
CREATE TRIGGER trg_snapshot_source_boundary
BEFORE INSERT OR UPDATE ON research.snapshot_manifest
FOR EACH ROW EXECUTE FUNCTION research.assert_snapshot_source_boundary();

CREATE OR REPLACE FUNCTION backtest.assert_dataset_source_boundary()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE release_hash CHAR(64); bad_members BIGINT;
BEGIN
    IF NEW.status='FROZEN' THEN
        SELECT source_manifest_hash INTO release_hash
          FROM ops.tushare_data_release WHERE release_tag=NEW.data_baseline_tag;
        IF NEW.baseline_source_manifest_hash IS NULL
           OR release_hash IS NULL
           OR btrim(NEW.baseline_source_manifest_hash) <> btrim(release_hash) THEN
            RAISE EXCEPTION 'FROZEN Dataset Raw boundary mismatch: %', NEW.dataset_id;
        END IF;
        SELECT COUNT(*) INTO bad_members
          FROM backtest.dataset_member d
          JOIN research.snapshot_manifest s ON s.snapshot_id=d.research_snapshot_id
         WHERE d.dataset_id=NEW.dataset_id
           AND s.baseline_source_manifest_hash IS DISTINCT FROM NEW.baseline_source_manifest_hash;
        IF bad_members <> 0 THEN
            RAISE EXCEPTION 'Dataset members use different Raw boundaries: %/%', NEW.dataset_id, bad_members;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_dataset_source_boundary ON backtest.dataset_manifest;
CREATE TRIGGER trg_dataset_source_boundary
BEFORE INSERT OR UPDATE ON backtest.dataset_manifest
FOR EACH ROW EXECUTE FUNCTION backtest.assert_dataset_source_boundary();

CREATE OR REPLACE FUNCTION research.assert_evidence_audit_fields()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE cutoff DATE;
BEGIN
    SELECT research_cutoff_date INTO cutoff
      FROM research.snapshot_manifest WHERE snapshot_id=NEW.snapshot_id;
    IF NEW.claim_role IN ('supporting','contrary') AND (
        NEW.content_hash IS NULL OR length(btrim(NEW.content_hash)) <> 64
    ) THEN
        RAISE EXCEPTION 'supporting/contrary evidence requires content hash: %', NEW.evidence_id;
    END IF;
    IF NEW.published_at IS NOT NULL AND NEW.published_at::date > cutoff THEN
        RAISE EXCEPTION 'evidence publication is after snapshot cutoff: %', NEW.evidence_id;
    END IF;
    IF NEW.effective_date IS NOT NULL AND NEW.effective_date > cutoff THEN
        RAISE EXCEPTION 'evidence effective date is after snapshot cutoff: %', NEW.evidence_id;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_evidence_audit_fields ON research.evidence_reference;
CREATE TRIGGER trg_evidence_audit_fields
BEFORE INSERT OR UPDATE ON research.evidence_reference
FOR EACH ROW EXECUTE FUNCTION research.assert_evidence_audit_fields();

CREATE OR REPLACE FUNCTION research.assert_buy_approval_evidence()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE candidate_direction VARCHAR(10); candidate_snapshot UUID; candidate_code VARCHAR(20);
        refs JSONB; ref_count BIGINT; supporting_count BIGINT; contrary_count BIGINT;
        source_count BIGINT; authoritative_count BIGINT; invalid_count BIGINT;
BEGIN
    IF NEW.decision <> 'APPROVE' THEN
        RETURN NEW;
    END IF;
    SELECT c.direction,c.snapshot_id,c.ts_code,a.evidence_ids
      INTO candidate_direction,candidate_snapshot,candidate_code,refs
      FROM research.candidate c
      JOIN research.ai_audit a ON a.audit_id=NEW.audit_id AND a.candidate_id=c.candidate_id
     WHERE c.candidate_id=NEW.candidate_id;
    IF candidate_direction <> 'BUY' THEN
        RETURN NEW;
    END IF;
    WITH requested AS (
        SELECT DISTINCT value::uuid AS evidence_id
          FROM jsonb_array_elements_text(COALESCE(refs,'[]'::jsonb))
    ), checked AS (
        SELECT r.evidence_id,e.evidence_id AS matched_evidence_id,
               e.claim_role,e.source_system,e.authority_level,
               e.content_hash,e.published_at,e.effective_date,m.research_cutoff_date,
               e.snapshot_id,e.ts_code
          FROM requested r
          LEFT JOIN research.evidence_reference e ON e.evidence_id=r.evidence_id
          LEFT JOIN research.snapshot_manifest m ON m.snapshot_id=e.snapshot_id
    )
    SELECT COUNT(*), COUNT(*) FILTER (WHERE claim_role='supporting'),
           COUNT(*) FILTER (WHERE claim_role='contrary'), COUNT(DISTINCT source_system),
           COUNT(*) FILTER (WHERE authority_level IN ('OFFICIAL','INTERNAL_FACT')),
           COUNT(*) FILTER (WHERE matched_evidence_id IS NULL
               OR snapshot_id IS DISTINCT FROM candidate_snapshot
               OR ts_code IS DISTINCT FROM candidate_code OR content_hash IS NULL
               OR published_at::date > research_cutoff_date
               OR effective_date > research_cutoff_date)
      INTO ref_count,supporting_count,contrary_count,source_count,authoritative_count,invalid_count
      FROM checked;
    IF ref_count=0 OR supporting_count=0 OR contrary_count=0 OR source_count<2
       OR authoritative_count=0 OR invalid_count<>0 THEN
        RAISE EXCEPTION 'BUY approval evidence gate failed for candidate %', NEW.candidate_id;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_buy_approval_evidence ON research.human_decision;
CREATE TRIGGER trg_buy_approval_evidence
BEFORE INSERT ON research.human_decision
FOR EACH ROW EXECUTE FUNCTION research.assert_buy_approval_evidence();
