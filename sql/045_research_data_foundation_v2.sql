-- A-share generic research data foundation contract v2.
-- Additive migration only: Raw facts and frozen snapshots are never removed or
-- reinterpreted.  Company financial v2 uses an independent table because its
-- PIT grain and profit semantics differ from company_financial_v1.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE financial_income_standard
    ADD COLUMN IF NOT EXISTS net_profit_consolidated DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS net_profit_parent DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS data_available_timestamp TIMESTAMPTZ;

ALTER TABLE financial_balance_sheet_standard
    ADD COLUMN IF NOT EXISTS parent_equity_end DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS total_equity_end DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS data_available_timestamp TIMESTAMPTZ;

ALTER TABLE financial_cash_flow_standard
    ADD COLUMN IF NOT EXISTS operating_cashflow_ytd DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cashflow_net_profit_reconciliation DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS data_available_timestamp TIMESTAMPTZ;

ALTER TABLE fina_indicator_standard
    ADD COLUMN IF NOT EXISTS roe_reported DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS roe_annualized DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS roe_quarterly DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS linked_comp_type VARCHAR(10),
    ADD COLUMN IF NOT EXISTS comp_type_quality_status VARCHAR(40),
    ADD COLUMN IF NOT EXISTS data_available_timestamp TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fina_indicator_raw_business_available
    ON fina_indicator_raw_version
       (business_key, (COALESCE(f_ann_date, ann_date)), payload_hash);

ALTER TABLE valuation_daily
    ADD COLUMN IF NOT EXISTS pe_ttm_vendor DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ps_ttm_vendor DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS pb_vendor DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dv_ttm DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dv_ttm_vendor DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dv_ttm_source VARCHAR(40),
    ADD COLUMN IF NOT EXISTS source_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS data_available_timestamp TIMESTAMPTZ;

ALTER TABLE feature.valuation_daily_v1
    ADD COLUMN IF NOT EXISTS pe_ttm_vendor DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ps_ttm_vendor DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS pb_vendor DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dv_ttm DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dv_ttm_vendor DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dv_ttm_source VARCHAR(40),
    ADD COLUMN IF NOT EXISTS dv_ttm_percentile_5y DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dv_window_start DATE,
    ADD COLUMN IF NOT EXISTS dv_window_end DATE,
    ADD COLUMN IF NOT EXISTS dv_observation_days INTEGER,
    ADD COLUMN IF NOT EXISTS dv_window_complete BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS source_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS data_available_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS feature_contract_version VARCHAR(50);

CREATE TABLE IF NOT EXISTS feature.company_financial_v2 (
    feature_id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    report_date DATE NOT NULL,
    effective_announce_date DATE,
    data_available_timestamp TIMESTAMPTZ NOT NULL,
    source_observed_at TIMESTAMPTZ,
    report_type VARCHAR(10),
    comp_type VARCHAR(10),
    end_type VARCHAR(10),

    revenue_ytd DOUBLE PRECISION,
    revenue_q DOUBLE PRECISION,
    revenue_ttm DOUBLE PRECISION,
    oper_cost_ytd DOUBLE PRECISION,
    oper_cost_q DOUBLE PRECISION,
    oper_cost_ttm DOUBLE PRECISION,
    total_cogs_ytd DOUBLE PRECISION,
    total_cogs_q DOUBLE PRECISION,
    total_cogs_ttm DOUBLE PRECISION,
    net_profit_consolidated_ytd DOUBLE PRECISION,
    net_profit_consolidated_q DOUBLE PRECISION,
    net_profit_consolidated_ttm DOUBLE PRECISION,
    net_profit_parent_ytd DOUBLE PRECISION,
    net_profit_parent_q DOUBLE PRECISION,
    net_profit_parent_ttm DOUBLE PRECISION,
    operating_cashflow_ytd DOUBLE PRECISION,
    operating_cashflow_q DOUBLE PRECISION,
    operating_cashflow_ttm DOUBLE PRECISION,
    parent_equity_begin DOUBLE PRECISION,
    parent_equity_end DOUBLE PRECISION,
    total_equity_end DOUBLE PRECISION,

    roe_reported DOUBLE PRECISION,
    roe_annualized DOUBLE PRECISION,
    roe_quarterly DOUBLE PRECISION,
    roe_ttm DOUBLE PRECISION,
    debt_to_assets DOUBLE PRECISION,
    gross_margin_ttm DOUBLE PRECISION,
    net_margin_consolidated_ttm DOUBLE PRECISION,
    ocf_to_net_profit_consolidated_ttm DOUBLE PRECISION,
    revenue_growth_yoy DOUBLE PRECISION,
    profit_growth_yoy DOUBLE PRECISION,
    revenue_cagr_5y DOUBLE PRECISION,
    profit_cagr_5y DOUBLE PRECISION,

    ttm_basis VARCHAR(30) NOT NULL DEFAULT 'UNAVAILABLE',
    ttm_required_component_count INTEGER NOT NULL DEFAULT 0,
    ttm_component_count INTEGER NOT NULL DEFAULT 0,
    ttm_complete BOOLEAN NOT NULL DEFAULT FALSE,
    roe_quality_reason VARCHAR(80),
    ocf_quality_reason VARCHAR(80),
    cashflow_quality_status VARCHAR(40) NOT NULL DEFAULT 'UNVERIFIED',
    roe_quality_status VARCHAR(40) NOT NULL DEFAULT 'UNVERIFIED',
    growth_quality_status VARCHAR(40) NOT NULL DEFAULT 'UNVERIFIED',

    roe_avg_5y DOUBLE PRECISION,
    roe_std_5y DOUBLE PRECISION,
    roe_observation_years INTEGER NOT NULL DEFAULT 0,
    ocf_positive_years_5y INTEGER,
    profit_positive_years_5y INTEGER,
    cashflow_observation_years_5y INTEGER NOT NULL DEFAULT 0,
    cashflow_window_complete BOOLEAN NOT NULL DEFAULT FALSE,
    profit_loss_event_5y BOOLEAN,
    loss_year_count_5y INTEGER,
    profit_observation_years_5y INTEGER NOT NULL DEFAULT 0,
    profit_window_complete BOOLEAN NOT NULL DEFAULT FALSE,
    profit_recovery_flag BOOLEAN,
    recovery_basis_period VARCHAR(30),
    profit_metric_basis VARCHAR(40) NOT NULL DEFAULT 'NET_PROFIT_PARENT',

    component_lineage JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_fingerprint CHAR(64) NOT NULL,
    pit_evidence_level VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN',
    quality_status VARCHAR(40) NOT NULL DEFAULT 'UNVERIFIED',
    feature_contract_version VARCHAR(50) NOT NULL DEFAULT 'feature-contract-v2.0',
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, report_date, data_available_timestamp, source_fingerprint),
    CHECK (ttm_component_count >= 0),
    CHECK (ttm_required_component_count >= 0),
    CHECK (ttm_component_count <= ttm_required_component_count),
    CHECK (NOT ttm_complete OR ttm_component_count = ttm_required_component_count)
);

ALTER TABLE feature.company_financial_v2
    ADD COLUMN IF NOT EXISTS pit_evidence_level VARCHAR(50)
        NOT NULL DEFAULT 'UNKNOWN';

CREATE INDEX IF NOT EXISTS idx_company_financial_v2_asof
    ON feature.company_financial_v2
       (ts_code, data_available_timestamp DESC, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_company_financial_v2_report
    ON feature.company_financial_v2
       (ts_code, report_date, report_type, comp_type, end_type);
CREATE INDEX IF NOT EXISTS idx_company_financial_v2_quality
    ON feature.company_financial_v2
       (quality_status, cashflow_quality_status, roe_quality_status);

CREATE TABLE IF NOT EXISTS ops.foundation_rebuild_run (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_version VARCHAR(50) NOT NULL,
    run_mode VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    warmup_start DATE,
    output_start DATE,
    output_end DATE,
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    code_version VARCHAR(100),
    git_commit CHAR(40),
    git_dirty BOOLEAN,
    diff_hash CHAR(64),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (run_mode IN ('SMOKE', 'PRODUCTION')),
    CHECK (status IN ('PENDING', 'RUNNING', 'FAILED', 'VALIDATED', 'COMPLETED'))
);

ALTER TABLE ops.foundation_rebuild_run
    ADD COLUMN IF NOT EXISTS code_version VARCHAR(100),
    ADD COLUMN IF NOT EXISTS git_commit CHAR(40),
    ADD COLUMN IF NOT EXISTS git_dirty BOOLEAN,
    ADD COLUMN IF NOT EXISTS diff_hash CHAR(64);

CREATE TABLE IF NOT EXISTS ops.foundation_rebuild_checkpoint (
    run_id UUID NOT NULL REFERENCES ops.foundation_rebuild_run(run_id),
    stage VARCHAR(60) NOT NULL,
    partition_key VARCHAR(180) NOT NULL,
    status VARCHAR(20) NOT NULL,
    rows_attempted BIGINT NOT NULL DEFAULT 0,
    rows_written BIGINT NOT NULL DEFAULT 0,
    result_hash CHAR(64),
    error_message TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, stage, partition_key),
    CHECK (status IN ('PENDING', 'RUNNING', 'FAILED', 'COMPLETED'))
);

CREATE INDEX IF NOT EXISTS idx_foundation_checkpoint_status
    ON ops.foundation_rebuild_checkpoint (run_id, stage, status, partition_key);

ALTER TABLE research.stock_snapshot_daily
    ADD COLUMN IF NOT EXISTS financial_feature_contract_version VARCHAR(50),
    ADD COLUMN IF NOT EXISTS financial_data_available_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS net_profit_consolidated_ttm DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS net_profit_parent_ttm DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS operating_cashflow_ttm DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ocf_to_net_profit_consolidated_ttm DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS roe_ttm DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS gross_margin_ttm DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS net_margin_consolidated_ttm DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS revenue_growth_yoy DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS profit_growth_yoy DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cashflow_quality_status VARCHAR(40),
    ADD COLUMN IF NOT EXISTS roe_quality_status VARCHAR(40),
    ADD COLUMN IF NOT EXISTS growth_quality_status VARCHAR(40),
    ADD COLUMN IF NOT EXISTS ttm_complete BOOLEAN,
    ADD COLUMN IF NOT EXISTS dv_ttm DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dv_ttm_percentile_5y DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS dv_ttm_source VARCHAR(40),
    ADD COLUMN IF NOT EXISTS dv_window_start DATE,
    ADD COLUMN IF NOT EXISTS dv_window_end DATE,
    ADD COLUMN IF NOT EXISTS dv_observation_days INTEGER,
    ADD COLUMN IF NOT EXISTS dv_window_complete BOOLEAN;

ALTER TABLE research.stock_snapshot_daily
    ADD COLUMN IF NOT EXISTS financial_component_lineage JSONB,
    ADD COLUMN IF NOT EXISTS financial_source_fingerprint CHAR(64);

ALTER TABLE research.snapshot_manifest
    ADD COLUMN IF NOT EXISTS feature_contract_version VARCHAR(50),
    ADD COLUMN IF NOT EXISTS decision_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS data_cutoff_timestamp TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pit_evidence_level VARCHAR(40);

INSERT INTO ops.feature_registry
    (feature_name, feature_version, grain, lookback_rule, source_tables, code_version, status)
VALUES
    ('company_financial_v2', 'v2', 'stock x report/version availability',
     'PIT components, quarterly bridge, TTM and five-year annual windows',
     ARRAY['financial_income_standard','financial_balance_sheet_standard',
           'financial_cash_flow_standard','fina_indicator_standard'],
     'feature-contract-v2.0', 'ACTIVE')
ON CONFLICT (feature_name) DO UPDATE SET
    feature_version=EXCLUDED.feature_version,
    grain=EXCLUDED.grain,
    lookback_rule=EXCLUDED.lookback_rule,
    source_tables=EXCLUDED.source_tables,
    code_version=EXCLUDED.code_version,
    status=EXCLUDED.status;
