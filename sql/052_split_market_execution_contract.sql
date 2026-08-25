-- Split legacy backtest.market_rule_effective into foundation facts and
-- application execution assumptions.  This migration is additive and
-- idempotent: legacy rows remain untouched as migration evidence.
CREATE SCHEMA IF NOT EXISTS foundation;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS foundation.market_rule_effective (
    rule_version VARCHAR(80) NOT NULL,
    market_rule_version VARCHAR(80) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    market VARCHAR(30) NOT NULL,
    min_buy_lot INTEGER NOT NULL DEFAULT 100 CHECK (min_buy_lot > 0),
    allow_sell_odd_lot BOOLEAN NOT NULL DEFAULT TRUE,
    t_plus_one BOOLEAN NOT NULL DEFAULT TRUE,
    price_tick DOUBLE PRECISION NOT NULL DEFAULT 0.01 CHECK (price_tick > 0),
    cash_settlement_days INTEGER NOT NULL DEFAULT 0 CHECK (cash_settlement_days >= 0),
    limit_pct DOUBLE PRECISION,
    approval_status VARCHAR(20) NOT NULL DEFAULT 'MIGRATED',
    approved_by VARCHAR(120),
    approved_at TIMESTAMPTZ,
    evidence_hash CHAR(64),
    source_legacy_table VARCHAR(120) NOT NULL DEFAULT 'backtest.market_rule_effective',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (rule_version, effective_from, market),
    CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CHECK (evidence_hash IS NULL OR evidence_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS foundation.tax_rule_effective (
    tax_rule_version VARCHAR(80) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    market VARCHAR(30) NOT NULL,
    stamp_duty_sell_rate DOUBLE PRECISION NOT NULL CHECK (stamp_duty_sell_rate >= 0),
    transfer_fee_rate DOUBLE PRECISION NOT NULL CHECK (transfer_fee_rate >= 0),
    approval_status VARCHAR(20) NOT NULL DEFAULT 'MIGRATED',
    approved_by VARCHAR(120),
    approved_at TIMESTAMPTZ,
    evidence_hash CHAR(64),
    source_legacy_table VARCHAR(120) NOT NULL DEFAULT 'backtest.market_rule_effective',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tax_rule_version, effective_from, market),
    CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CHECK (evidence_hash IS NULL OR evidence_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_foundation_market_rule_lookup
    ON foundation.market_rule_effective (market, market_rule_version, effective_from, effective_to);
CREATE INDEX IF NOT EXISTS idx_foundation_tax_rule_lookup
    ON foundation.tax_rule_effective (market, tax_rule_version, effective_from, effective_to);

DO $$
BEGIN
    IF to_regclass('backtest.market_rule_effective') IS NOT NULL THEN
        INSERT INTO foundation.market_rule_effective (
            rule_version, market_rule_version, effective_from, effective_to, market,
            min_buy_lot, allow_sell_odd_lot, t_plus_one, price_tick, cash_settlement_days,
            limit_pct, approval_status, approved_by, approved_at, evidence_hash
        )
        SELECT rule_version, market_rule_version, effective_from, effective_to, market,
               min_buy_lot, allow_sell_odd_lot, t_plus_one, price_tick, cash_settlement_days,
               limit_pct, COALESCE(approval_status, 'MIGRATED'), approved_by, approved_at,
               approval_hash
          FROM backtest.market_rule_effective
        ON CONFLICT (rule_version, effective_from, market) DO NOTHING;

        INSERT INTO foundation.tax_rule_effective (
            tax_rule_version, effective_from, effective_to, market,
            stamp_duty_sell_rate, transfer_fee_rate, approval_status,
            approved_by, approved_at, evidence_hash
        )
        SELECT fee_rule_version, effective_from, effective_to, market,
               stamp_duty_sell_rate, transfer_fee_rate,
               COALESCE(approval_status, 'MIGRATED'), approved_by, approved_at,
               approval_hash
          FROM backtest.market_rule_effective
        ON CONFLICT (tax_rule_version, effective_from, market) DO NOTHING;
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION foundation.reject_immutable_rule_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; publish a new effective rule version',
        TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
END;
$$;

DROP TRIGGER IF EXISTS trg_market_rule_append_only ON foundation.market_rule_effective;
CREATE TRIGGER trg_market_rule_append_only
BEFORE UPDATE OR DELETE ON foundation.market_rule_effective
FOR EACH ROW EXECUTE FUNCTION foundation.reject_immutable_rule_change();

DROP TRIGGER IF EXISTS trg_tax_rule_append_only ON foundation.tax_rule_effective;
CREATE TRIGGER trg_tax_rule_append_only
BEFORE UPDATE OR DELETE ON foundation.tax_rule_effective
FOR EACH ROW EXECUTE FUNCTION foundation.reject_immutable_rule_change();

GRANT USAGE ON SCHEMA foundation TO ashares_app;
GRANT SELECT ON foundation.market_rule_effective, foundation.tax_rule_effective TO ashares_app;
