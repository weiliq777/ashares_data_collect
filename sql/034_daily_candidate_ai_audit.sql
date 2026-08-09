-- Daily forward-paper decision layer.
--
-- This migration adds immutable candidate/AI-audit lineage and allows a
-- PAPER_MANUAL portfolio to bind one FROZEN Research Snapshot per trading day.
-- It does not modify any Raw/Standard/PIT/Feature table and does not create a
-- broker or automatic-trading path.

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS research;

CREATE TABLE IF NOT EXISTS research.candidate_run (
    candidate_run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    research_date DATE NOT NULL,
    run_type VARCHAR(24) NOT NULL DEFAULT 'DAILY_SIGNAL',
    strategy_id VARCHAR(120) NOT NULL,
    strategy_version VARCHAR(80) NOT NULL,
    strategy_parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    parameters_hash CHAR(64) NOT NULL,
    input_hash CHAR(64) NOT NULL,
    portfolio_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    portfolio_state_hash CHAR(64),
    observation_input_hash CHAR(64),
    rule_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    rule_snapshot_hash CHAR(64),
    code_version VARCHAR(180),
    git_commit CHAR(40),
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    failure_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    UNIQUE (snapshot_id, run_type, strategy_id, strategy_version, input_hash),
    CHECK (run_type = 'DAILY_SIGNAL'),
    CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','BLOCKED'))
);

CREATE INDEX IF NOT EXISTS idx_candidate_run_date_status
    ON research.candidate_run (research_date, status, created_at);

CREATE TABLE IF NOT EXISTS research.candidate (
    candidate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_run_id UUID NOT NULL REFERENCES research.candidate_run(candidate_run_id),
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    research_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    direction VARCHAR(10) NOT NULL CHECK (direction IN ('BUY','SELL','HOLD')),
    signal_score DOUBLE PRECISION NOT NULL,
    indicator_price DOUBLE PRECISION,
    raw_reference_price DOUBLE PRECISION,
    stop_loss_price_raw DOUBLE PRECISION,
    theoretical_shares DOUBLE PRECISION,
    planned_shares INTEGER NOT NULL DEFAULT 0 CHECK (planned_shares >= 0),
    required_capital DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (required_capital >= 0),
    max_loss_at_stop DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (max_loss_at_stop >= 0),
    gap_stress_loss DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (gap_stress_loss >= 0),
    estimated_slippage DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (estimated_slippage >= 0),
    actual_risk_pct DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (actual_risk_pct >= 0),
    eligible BOOLEAN NOT NULL DEFAULT FALSE,
    reject_reason TEXT,
    reason JSONB NOT NULL DEFAULT '{}'::jsonb,
    candidate_hash CHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (candidate_run_id, ts_code, direction),
    UNIQUE (candidate_id, candidate_hash)
);

CREATE INDEX IF NOT EXISTS idx_candidate_lookup
    ON research.candidate (research_date, direction, eligible, signal_score DESC, ts_code);

DO $$
BEGIN
    ALTER TABLE research.candidate DROP CONSTRAINT IF EXISTS candidate_direction_check;
    ALTER TABLE research.candidate
        ADD CONSTRAINT candidate_direction_check
        CHECK (direction IN ('BUY','SELL','HOLD'));
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;

CREATE TABLE IF NOT EXISTS research.ai_audit (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id UUID NOT NULL REFERENCES research.candidate(candidate_id),
    portfolio_id UUID REFERENCES portfolio.account_manifest(portfolio_id),
    input_hash CHAR(64) NOT NULL,
    skill_name VARCHAR(120) NOT NULL,
    skill_version VARCHAR(80) NOT NULL,
    model_provider VARCHAR(80),
    model_version VARCHAR(160),
    prompt_version VARCHAR(120),
    action VARCHAR(24) NOT NULL CHECK (action IN (
        'PASS_REVIEW','NEEDS_REVIEW','REJECT','HOLD','SELL_REVIEW'
    )),
    risk_level VARCHAR(20) NOT NULL CHECK (risk_level IN ('LOW','MEDIUM','HIGH','UNKNOWN')),
    evidence_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    result JSONB NOT NULL,
    output_hash CHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (candidate_id, input_hash, skill_name, skill_version, model_version, prompt_version)
);

CREATE INDEX IF NOT EXISTS idx_ai_audit_candidate
    ON research.ai_audit (candidate_id, created_at DESC);

CREATE TABLE IF NOT EXISTS research.human_decision (
    decision_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id UUID NOT NULL REFERENCES research.candidate(candidate_id),
    audit_id UUID REFERENCES research.ai_audit(audit_id),
    portfolio_id UUID REFERENCES portfolio.account_manifest(portfolio_id),
    decision VARCHAR(24) NOT NULL CHECK (decision IN ('APPROVE','REJECT','DEFER')),
    actor VARCHAR(120) NOT NULL,
    reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (candidate_id, portfolio_id)
);

-- PostgreSQL UNIQUE constraints treat NULL portfolio IDs as distinct.  The
-- explicit expression index makes the optional global decision key truly
-- idempotent as well; a duplicate existing history must fail the migration.
ALTER TABLE research.human_decision
    DROP CONSTRAINT IF EXISTS human_decision_candidate_id_portfolio_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS uq_research_human_decision_candidate_portfolio
    ON research.human_decision (candidate_id, COALESCE(portfolio_id, '00000000-0000-0000-0000-000000000000'::uuid));

CREATE TABLE IF NOT EXISTS portfolio.snapshot_binding (
    portfolio_id UUID NOT NULL REFERENCES portfolio.account_manifest(portfolio_id),
    valuation_date DATE NOT NULL,
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    candidate_run_id UUID REFERENCES research.candidate_run(candidate_run_id),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (portfolio_id, valuation_date),
    UNIQUE (portfolio_id, valuation_date, snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_snapshot_binding_snapshot
    ON portfolio.snapshot_binding (snapshot_id, valuation_date);

ALTER TABLE portfolio.order_intent
    ADD COLUMN IF NOT EXISTS source_candidate_id UUID REFERENCES research.candidate(candidate_id),
    ADD COLUMN IF NOT EXISTS source_ai_audit_id UUID REFERENCES research.ai_audit(audit_id),
    ADD COLUMN IF NOT EXISTS source_candidate_hash CHAR(64);

DO $$
BEGIN
    ALTER TABLE portfolio.order_intent DROP CONSTRAINT IF EXISTS order_intent_source_type_check;
    ALTER TABLE portfolio.order_intent DROP CONSTRAINT IF EXISTS ck_portfolio_intent_source_type;
    ALTER TABLE portfolio.order_intent
        ADD CONSTRAINT ck_portfolio_intent_source_type
        CHECK (source_type IN ('MANUAL','RESEARCH','BACKTEST','DAILY_SIGNAL'));
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_intent_daily_candidate
    ON portfolio.order_intent (portfolio_id, source_candidate_id)
    WHERE source_candidate_id IS NOT NULL;

CREATE OR REPLACE FUNCTION portfolio.assert_snapshot_binding()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    account_status VARCHAR(20);
    snapshot_status VARCHAR(20);
    snapshot_cutoff DATE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'portfolio snapshot bindings are append-only: % / %',
            OLD.portfolio_id, OLD.valuation_date;
    END IF;
    SELECT status INTO account_status
      FROM portfolio.account_manifest
     WHERE portfolio_id = NEW.portfolio_id;
    IF account_status NOT IN ('ACTIVE','ARCHIVED') THEN
        RAISE EXCEPTION 'snapshot binding requires ACTIVE/ARCHIVED portfolio: %', NEW.portfolio_id;
    END IF;
    SELECT status, research_cutoff_date
      INTO snapshot_status, snapshot_cutoff
      FROM research.snapshot_manifest
     WHERE snapshot_id = NEW.snapshot_id;
    IF snapshot_status <> 'FROZEN' OR snapshot_cutoff <> NEW.valuation_date THEN
        RAISE EXCEPTION 'snapshot binding requires matching FROZEN snapshot: % / %',
            NEW.snapshot_id, NEW.valuation_date;
    END IF;
    IF NEW.candidate_run_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM research.candidate_run
         WHERE candidate_run_id = NEW.candidate_run_id
           AND snapshot_id = NEW.snapshot_id
           AND research_date = NEW.valuation_date
           AND status = 'SUCCEEDED'
    ) THEN
        RAISE EXCEPTION 'snapshot binding candidate run mismatch: %', NEW.candidate_run_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_portfolio_snapshot_binding ON portfolio.snapshot_binding;
CREATE TRIGGER trg_portfolio_snapshot_binding
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.snapshot_binding
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_snapshot_binding();

-- Existing Portfolio v1 required one fixed account snapshot. Forward-paper
-- portfolios use the append-only daily binding table instead; historical
-- BACKTEST intents retain the old fixed-snapshot Air-Gap.
CREATE OR REPLACE FUNCTION portfolio.assert_valuation_append_only()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    account_status VARCHAR(20);
    snapshot_status VARCHAR(20);
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'portfolio valuation rows are append-only: % / %', OLD.portfolio_id, OLD.trade_date;
    END IF;
    SELECT status INTO account_status
      FROM portfolio.account_manifest
     WHERE portfolio_id = NEW.portfolio_id;
    IF account_status NOT IN ('ACTIVE','ARCHIVED') THEN
        RAISE EXCEPTION 'valuation requires ACTIVE/ARCHIVED portfolio: %', NEW.portfolio_id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM portfolio.snapshot_binding
         WHERE portfolio_id = NEW.portfolio_id
           AND valuation_date = NEW.trade_date
           AND snapshot_id = NEW.source_snapshot_id
    ) THEN
        RAISE EXCEPTION 'valuation snapshot is not bound to portfolio date: % / %',
            NEW.portfolio_id, NEW.trade_date;
    END IF;
    SELECT status INTO snapshot_status
      FROM research.snapshot_manifest
     WHERE snapshot_id = NEW.source_snapshot_id;
    IF snapshot_status <> 'FROZEN' THEN
        RAISE EXCEPTION 'valuation requires FROZEN Research Snapshot: % (%)',
            NEW.source_snapshot_id, snapshot_status;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION portfolio.assert_intent_state()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    account_status VARCHAR(20);
    account_snapshot UUID;
    account_watchlist UUID;
    account_t_plus_one BOOLEAN;
    snapshot_status VARCHAR(20);
    snapshot_cutoff DATE;
    watchlist_snapshot UUID;
    watchlist_status VARCHAR(20);
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'portfolio order intents are append-only: %', OLD.intent_id;
    END IF;
    IF TG_OP = 'INSERT' AND NEW.status <> 'PENDING_APPROVAL' THEN
        RAISE EXCEPTION 'new portfolio intents must start in PENDING_APPROVAL status: %', NEW.intent_id;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.status <> OLD.status AND NOT (
        (OLD.status = 'PENDING_APPROVAL' AND NEW.status IN ('APPROVED','REJECTED','CANCELLED','EXPIRED')) OR
        (OLD.status = 'APPROVED' AND NEW.status IN ('EXECUTED','CANCELLED','EXPIRED'))
    ) THEN
        RAISE EXCEPTION 'invalid portfolio intent status transition: % -> %', OLD.status, NEW.status;
    END IF;
    SELECT status, source_snapshot_id, source_watchlist_id, t_plus_one
      INTO account_status, account_snapshot, account_watchlist, account_t_plus_one
      FROM portfolio.account_manifest
     WHERE portfolio_id = NEW.portfolio_id FOR UPDATE;
    IF account_status IS NULL OR account_status <> 'ACTIVE' THEN
        RAISE EXCEPTION 'portfolio order intent requires ACTIVE account: %', NEW.portfolio_id;
    END IF;
    IF NEW.source_type = 'DAILY_SIGNAL' THEN
        IF NOT EXISTS (
            SELECT 1 FROM portfolio.snapshot_binding
             WHERE portfolio_id = NEW.portfolio_id
               AND valuation_date = NEW.decision_date
               AND snapshot_id = NEW.snapshot_id
        ) THEN
            RAISE EXCEPTION 'DAILY_SIGNAL intent requires a bound daily snapshot: %', NEW.intent_id;
        END IF;
    ELSIF account_snapshot <> NEW.snapshot_id THEN
        RAISE EXCEPTION 'portfolio intent snapshot mismatch: % <> %', NEW.snapshot_id, account_snapshot;
    END IF;
    IF NEW.source_type <> 'DAILY_SIGNAL' AND account_watchlist IS NOT NULL
       AND (NEW.watchlist_id IS NULL OR NEW.watchlist_id <> account_watchlist) THEN
        RAISE EXCEPTION 'portfolio intent must use the account watchlist: %', account_watchlist;
    END IF;
    SELECT status, research_cutoff_date INTO snapshot_status, snapshot_cutoff
      FROM research.snapshot_manifest WHERE snapshot_id = NEW.snapshot_id;
    IF snapshot_status <> 'FROZEN' THEN
        RAISE EXCEPTION 'portfolio intent requires FROZEN snapshot: % (%)', NEW.snapshot_id, snapshot_status;
    END IF;
    IF NEW.decision_date <> snapshot_cutoff THEN
        RAISE EXCEPTION 'portfolio intent decision date must equal snapshot cutoff: % <> %',
            NEW.decision_date, snapshot_cutoff;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM trading_calendar
         WHERE exchange='SSE' AND trade_date=NEW.decision_date AND is_open IS TRUE
    ) OR NOT EXISTS (
        SELECT 1 FROM trading_calendar
         WHERE exchange='SSE' AND trade_date=NEW.intended_execution_date AND is_open IS TRUE
    ) THEN
        RAISE EXCEPTION 'portfolio intent dates must be SSE open dates: %', NEW.intent_id;
    END IF;
    IF account_t_plus_one AND NEW.intended_execution_date <= NEW.decision_date THEN
        RAISE EXCEPTION 'T+1 portfolio intent requires execution date after decision date: %', NEW.intent_id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM research.stock_snapshot_daily
         WHERE snapshot_id = NEW.snapshot_id AND research_date = snapshot_cutoff AND ts_code = NEW.ts_code
    ) THEN
        RAISE EXCEPTION 'portfolio intent stock is absent from snapshot: %', NEW.ts_code;
    END IF;
    IF NEW.watchlist_id IS NOT NULL THEN
        SELECT snapshot_id, status INTO watchlist_snapshot, watchlist_status
          FROM research.watchlist WHERE watchlist_id = NEW.watchlist_id;
        IF watchlist_snapshot IS NULL OR watchlist_snapshot <> NEW.snapshot_id OR watchlist_status <> 'ACTIVE' THEN
            RAISE EXCEPTION 'portfolio intent watchlist is invalid: %', NEW.watchlist_id;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM research.watchlist_member
             WHERE watchlist_id = NEW.watchlist_id AND snapshot_id = NEW.snapshot_id AND ts_code = NEW.ts_code
        ) THEN
            RAISE EXCEPTION 'portfolio intent stock is absent from watchlist: %', NEW.ts_code;
        END IF;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.status IN ('APPROVED','EXECUTED') THEN
        IF ROW(OLD.portfolio_id, OLD.snapshot_id, OLD.watchlist_id, OLD.ts_code,
               OLD.decision_date, OLD.intended_execution_date, OLD.direction,
               OLD.requested_shares, OLD.reference_price, OLD.stop_loss_price_raw,
               OLD.source_type, OLD.source_candidate_id, OLD.source_ai_audit_id,
               OLD.source_candidate_hash, OLD.reason, OLD.approved_by, OLD.approved_at,
               OLD.executed_at, OLD.cancelled_at)
           IS DISTINCT FROM
           ROW(NEW.portfolio_id, NEW.snapshot_id, NEW.watchlist_id, NEW.ts_code,
               NEW.decision_date, NEW.intended_execution_date, NEW.direction,
               NEW.requested_shares, NEW.reference_price, NEW.stop_loss_price_raw,
               NEW.source_type, NEW.source_candidate_id, NEW.source_ai_audit_id,
               NEW.source_candidate_hash, NEW.reason, NEW.approved_by, NEW.approved_at,
               NEW.executed_at, NEW.cancelled_at) THEN
            RAISE EXCEPTION 'approved portfolio intent content is immutable: %', OLD.intent_id;
        END IF;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.status = 'EXECUTED'
       AND NOT EXISTS (SELECT 1 FROM portfolio.trade_fill WHERE intent_id = NEW.intent_id) THEN
        RAISE EXCEPTION 'EXECUTED portfolio intent requires a trade fill: %', NEW.intent_id;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.status = 'APPROVED' AND OLD.status <> 'APPROVED' THEN
        IF NULLIF(BTRIM(COALESCE(NEW.approved_by, '')), '') IS NULL THEN
            RAISE EXCEPTION 'APPROVED portfolio intent requires approved_by: %', NEW.intent_id;
        END IF;
        NEW.approved_at := COALESCE(NEW.approved_at, CURRENT_TIMESTAMP);
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.status = 'EXECUTED' THEN
        NEW.executed_at := COALESCE(NEW.executed_at, CURRENT_TIMESTAMP);
    ELSIF TG_OP = 'UPDATE' AND NEW.status = 'CANCELLED' THEN
        NEW.cancelled_at := COALESCE(NEW.cancelled_at, CURRENT_TIMESTAMP);
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION portfolio.assert_backtest_airgap_source()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    order_run UUID;
    order_dataset UUID;
    order_signal UUID;
    order_audit_hash CHAR(64);
    run_status VARCHAR(20);
    dataset_status VARCHAR(20);
    order_status VARCHAR(20);
    account_snapshot UUID;
    account_watchlist UUID;
    order_ts_code VARCHAR(20);
    order_direction VARCHAR(10);
    order_date DATE;
    candidate_snapshot UUID;
    candidate_date DATE;
    candidate_code VARCHAR(20);
    candidate_direction VARCHAR(10);
    candidate_hash CHAR(64);
    candidate_run_status VARCHAR(20);
    audit_candidate UUID;
    audit_input_hash CHAR(64);
    audit_action VARCHAR(24);
    audit_risk_level VARCHAR(20);
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'portfolio intents are append-only: %', OLD.intent_id;
    END IF;
    IF NEW.source_type = 'DAILY_SIGNAL' THEN
        IF (TG_OP = 'INSERT' AND NEW.status <> 'PENDING_APPROVAL')
           OR NEW.source_candidate_id IS NULL OR NEW.source_ai_audit_id IS NULL
           OR NEW.source_candidate_hash IS NULL
           OR length(btrim(NEW.source_candidate_hash)) <> 64 THEN
            RAISE EXCEPTION 'DAILY_SIGNAL intent must start pending with candidate/audit metadata: %', NEW.intent_id;
        END IF;
        SELECT c.snapshot_id, c.research_date, c.ts_code, c.direction, c.candidate_hash,
               r.status
          INTO candidate_snapshot, candidate_date, candidate_code, candidate_direction,
               candidate_hash, candidate_run_status
          FROM research.candidate c
          JOIN research.candidate_run r ON r.candidate_run_id = c.candidate_run_id
         WHERE c.candidate_id = NEW.source_candidate_id;
        IF candidate_snapshot IS NULL OR candidate_run_status <> 'SUCCEEDED' THEN
            RAISE EXCEPTION 'DAILY_SIGNAL candidate run is not SUCCEEDED: %', NEW.source_candidate_id;
        END IF;
        SELECT candidate_id, input_hash, action, risk_level
          INTO audit_candidate, audit_input_hash, audit_action, audit_risk_level
          FROM research.ai_audit WHERE audit_id = NEW.source_ai_audit_id;
        IF audit_candidate IS NULL OR audit_candidate <> NEW.source_candidate_id
           OR btrim(NEW.source_candidate_hash) <> btrim(candidate_hash)
           OR audit_action = 'REJECT' OR audit_risk_level = 'HIGH'
           OR (candidate_direction = 'BUY' AND audit_action NOT IN ('PASS_REVIEW','NEEDS_REVIEW'))
           OR (candidate_direction = 'SELL' AND audit_action NOT IN ('PASS_REVIEW','NEEDS_REVIEW','SELL_REVIEW')) THEN
            RAISE EXCEPTION 'DAILY_SIGNAL candidate/audit hash mismatch: %', NEW.intent_id;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM research.human_decision
             WHERE candidate_id=NEW.source_candidate_id
               AND audit_id=NEW.source_ai_audit_id
               AND portfolio_id=NEW.portfolio_id
               AND decision='APPROVE'
        ) THEN
            RAISE EXCEPTION 'DAILY_SIGNAL intent requires an APPROVE human decision: %', NEW.intent_id;
        END IF;
        IF candidate_snapshot <> NEW.snapshot_id OR candidate_date <> NEW.decision_date
           OR candidate_code <> NEW.ts_code OR candidate_direction <> NEW.direction THEN
            RAISE EXCEPTION 'DAILY_SIGNAL source candidate does not match intent: %', NEW.intent_id;
        END IF;
        IF TG_OP = 'UPDATE' THEN
            IF ROW(OLD.source_type, OLD.source_candidate_id, OLD.source_ai_audit_id,
                   OLD.source_candidate_hash)
               IS DISTINCT FROM
               ROW(NEW.source_type, NEW.source_candidate_id, NEW.source_ai_audit_id,
                   NEW.source_candidate_hash) THEN
                RAISE EXCEPTION 'DAILY_SIGNAL source metadata is immutable: %', OLD.intent_id;
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.source_type <> 'BACKTEST' THEN
        IF NEW.source_backtest_run_id IS NOT NULL OR NEW.source_backtest_order_id IS NOT NULL
           OR NEW.source_dataset_id IS NOT NULL OR NEW.source_signal_id IS NOT NULL
           OR NEW.source_order_hash IS NOT NULL OR NEW.source_candidate_id IS NOT NULL
           OR NEW.source_ai_audit_id IS NOT NULL OR NEW.source_candidate_hash IS NOT NULL THEN
            RAISE EXCEPTION 'non-source intent cannot carry source metadata: %', NEW.intent_id;
        END IF;
        RETURN NEW;
    END IF;
    IF (TG_OP = 'INSERT' AND NEW.status <> 'PENDING_APPROVAL')
       OR NEW.source_backtest_run_id IS NULL OR NEW.source_backtest_order_id IS NULL
       OR NEW.source_dataset_id IS NULL OR NEW.source_order_hash IS NULL
       OR length(btrim(NEW.source_order_hash)) <> 64 THEN
        RAISE EXCEPTION 'BACKTEST intent must start pending with complete source metadata: %', NEW.intent_id;
    END IF;
    SELECT o.run_id, r.dataset_id, o.signal_id, o.order_audit_hash,
           r.status, d.status, o.status, o.ts_code, o.direction, o.order_date
      INTO order_run, order_dataset, order_signal, order_audit_hash,
           run_status, dataset_status, order_status, order_ts_code, order_direction, order_date
      FROM backtest.order o
      JOIN backtest.run_manifest r ON r.run_id = o.run_id
      JOIN backtest.dataset_manifest d ON d.dataset_id = r.dataset_id
     WHERE o.order_id = NEW.source_backtest_order_id;
    IF order_run IS NULL THEN
        RAISE EXCEPTION 'BACKTEST source order does not exist: %', NEW.source_backtest_order_id;
    END IF;
    IF order_run <> NEW.source_backtest_run_id OR order_dataset <> NEW.source_dataset_id
       OR run_status <> 'SUCCEEDED' OR dataset_status <> 'FROZEN' OR order_status <> 'FILLED'
       OR order_audit_hash IS NULL OR btrim(NEW.source_order_hash) <> btrim(order_audit_hash) THEN
        RAISE EXCEPTION 'BACKTEST source must be a FILLED order with matching database audit hash: %', NEW.source_backtest_order_id;
    END IF;
    IF order_signal IS DISTINCT FROM NEW.source_signal_id
       OR order_ts_code <> NEW.ts_code OR order_direction <> NEW.direction
       OR order_date <> NEW.intended_execution_date THEN
        RAISE EXCEPTION 'BACKTEST source does not match Portfolio intent: %', NEW.intent_id;
    END IF;
    SELECT a.source_snapshot_id, a.source_watchlist_id INTO account_snapshot, account_watchlist
      FROM portfolio.account_manifest a WHERE a.portfolio_id = NEW.portfolio_id;
    IF account_snapshot IS NULL OR NEW.snapshot_id <> account_snapshot
       OR (account_watchlist IS NOT NULL AND NEW.watchlist_id IS DISTINCT FROM account_watchlist) THEN
        RAISE EXCEPTION 'BACKTEST intent must use account snapshot/watchlist: %', NEW.intent_id;
    END IF;
    IF TG_OP = 'UPDATE' AND ROW(OLD.source_type, OLD.source_backtest_run_id,
       OLD.source_backtest_order_id, OLD.source_dataset_id, OLD.source_signal_id,
       OLD.source_order_hash) IS DISTINCT FROM ROW(NEW.source_type, NEW.source_backtest_run_id,
       NEW.source_backtest_order_id, NEW.source_dataset_id, NEW.source_signal_id,
       NEW.source_order_hash) THEN
        RAISE EXCEPTION 'approved BACKTEST source metadata is immutable: %', OLD.intent_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_portfolio_backtest_airgap ON portfolio.order_intent;
CREATE TRIGGER trg_portfolio_backtest_airgap
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.order_intent
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_backtest_airgap_source();

-- DAILY_SIGNAL fills are forward paper fills.  Their decision Snapshot is
-- frozen at T, while the paper fill may occur on T+1, so the old Portfolio v1
-- trigger's account-cutoff check must be extended without weakening its
-- approval, fee, lot, cash, and T+1 checks.
CREATE OR REPLACE FUNCTION portfolio.assert_trade_fill()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    intent_status VARCHAR(24);
    intent_direction VARCHAR(10);
    intent_source_type VARCHAR(20);
    intent_portfolio UUID;
    intent_snapshot UUID;
    intent_ts_code VARCHAR(20);
    intent_execution_date DATE;
    intent_decision_date DATE;
    intent_requested_shares INTEGER;
    intent_stop_loss NUMERIC;
    account_status VARCHAR(20);
    min_lot INTEGER;
    allow_odd BOOLEAN;
    account_t_plus_one BOOLEAN;
    account_snapshot UUID;
    account_cutoff DATE;
    account_reserve_cash DOUBLE PRECISION;
    account_price_tick NUMERIC;
    account_commission_rate DOUBLE PRECISION;
    account_min_commission NUMERIC;
    account_transfer_fee_rate DOUBLE PRECISION;
    account_stamp_duty_rate DOUBLE PRECISION;
    available_cash NUMERIC;
    gross_value NUMERIC;
    expected_commission NUMERIC;
    expected_stamp_duty NUMERIC;
    expected_transfer_fee NUMERIC;
    required_cash NUMERIC;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'portfolio trade fills are append-only: %', OLD.fill_id;
    END IF;
    SELECT status, direction, source_type, portfolio_id, snapshot_id, ts_code,
           decision_date, intended_execution_date, requested_shares,
           stop_loss_price_raw
      INTO intent_status, intent_direction, intent_source_type, intent_portfolio,
           intent_snapshot, intent_ts_code, intent_decision_date,
           intent_execution_date, intent_requested_shares, intent_stop_loss
      FROM portfolio.order_intent WHERE intent_id = NEW.intent_id;
    IF intent_status IS NULL OR intent_status <> 'APPROVED' THEN
        RAISE EXCEPTION 'paper fill requires HUMAN-approved intent: % (%)', NEW.intent_id, intent_status;
    END IF;
    IF intent_portfolio <> NEW.portfolio_id OR intent_direction <> NEW.direction
       OR intent_ts_code <> NEW.ts_code THEN
        RAISE EXCEPTION 'portfolio fill intent mismatch: %', NEW.intent_id;
    END IF;
    IF NEW.fill_shares <> intent_requested_shares THEN
        RAISE EXCEPTION 'PAPER_MANUAL fill must equal requested shares: %', NEW.intent_id;
    END IF;
    SELECT status, min_buy_lot, allow_sell_odd_lot, t_plus_one,
           source_snapshot_id, reserve_cash_pct, price_tick,
           commission_rate, min_commission, transfer_fee_rate, stamp_duty_sell_rate
      INTO account_status, min_lot, allow_odd, account_t_plus_one,
           account_snapshot, account_reserve_cash, account_price_tick,
           account_commission_rate, account_min_commission,
           account_transfer_fee_rate, account_stamp_duty_rate
      FROM portfolio.account_manifest WHERE portfolio_id = NEW.portfolio_id;
    IF account_status <> 'ACTIVE' THEN
        RAISE EXCEPTION 'portfolio fill requires ACTIVE account: %', NEW.portfolio_id;
    END IF;
    IF NEW.fill_date < intent_execution_date THEN
        RAISE EXCEPTION 'portfolio fill cannot precede intended execution date: %', NEW.intent_id;
    END IF;
    IF intent_source_type = 'DAILY_SIGNAL' THEN
        IF NOT EXISTS (
            SELECT 1 FROM portfolio.snapshot_binding
             WHERE portfolio_id=NEW.portfolio_id
               AND valuation_date=intent_decision_date
               AND snapshot_id=intent_snapshot
        ) THEN
            RAISE EXCEPTION 'DAILY_SIGNAL fill requires a bound decision Snapshot: %', NEW.intent_id;
        END IF;
        SELECT research_cutoff_date INTO account_cutoff
          FROM research.snapshot_manifest
         WHERE snapshot_id=intent_snapshot AND status='FROZEN';
        IF account_cutoff IS NULL THEN
            RAISE EXCEPTION 'DAILY_SIGNAL fill requires a FROZEN decision Snapshot: %', NEW.intent_id;
        END IF;
    ELSE
        SELECT research_cutoff_date INTO account_cutoff
          FROM research.snapshot_manifest
         WHERE snapshot_id=account_snapshot AND status='FROZEN';
        IF account_cutoff IS NULL OR NEW.fill_date > account_cutoff THEN
            RAISE EXCEPTION 'portfolio fill date is after bound FROZEN Snapshot cutoff: %', NEW.fill_id;
        END IF;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM trading_calendar
         WHERE exchange='SSE' AND trade_date=NEW.fill_date AND is_open IS TRUE
    ) THEN
        RAISE EXCEPTION 'portfolio fill date is not an SSE open date: %', NEW.fill_id;
    END IF;
    IF abs(NEW.fill_price / account_price_tick - round(NEW.fill_price / account_price_tick)) > 1e-8 THEN
        RAISE EXCEPTION 'portfolio fill price does not match price tick: %', NEW.fill_id;
    END IF;
    IF NEW.direction = 'BUY' AND NEW.fill_shares % min_lot <> 0 THEN
        RAISE EXCEPTION 'portfolio BUY must use min lot %: %', min_lot, NEW.fill_shares;
    END IF;
    IF NEW.direction = 'SELL' AND NOT allow_odd AND NEW.fill_shares % min_lot <> 0 THEN
        RAISE EXCEPTION 'portfolio SELL odd lot is disabled: %', NEW.fill_id;
    END IF;
    IF NEW.direction = 'BUY' AND intent_stop_loss IS NOT NULL
       AND NEW.fill_price <= intent_stop_loss THEN
        RAISE EXCEPTION 'portfolio BUY fill must be above stop loss: %', NEW.intent_id;
    END IF;
    gross_value := NEW.fill_price * NEW.fill_shares;
    expected_commission := GREATEST(gross_value * account_commission_rate, account_min_commission);
    expected_stamp_duty := CASE WHEN NEW.direction = 'SELL' THEN gross_value * account_stamp_duty_rate ELSE 0 END;
    expected_transfer_fee := gross_value * account_transfer_fee_rate;
    IF abs(NEW.commission - expected_commission) > 0.000001
       OR abs(NEW.stamp_duty - expected_stamp_duty) > 0.000001
       OR abs(NEW.transfer_fee - expected_transfer_fee) > 0.000001 THEN
        RAISE EXCEPTION 'portfolio fill fees do not match bound account rules: %', NEW.fill_id;
    END IF;
    IF NEW.direction = 'BUY' AND EXISTS (
        SELECT 1 FROM portfolio.position_lot
         WHERE portfolio_id=NEW.portfolio_id AND ts_code=NEW.ts_code AND remaining_shares>0
    ) THEN
        RAISE EXCEPTION 'portfolio v1 does not allow adding to an existing position: %', NEW.ts_code;
    END IF;
    IF NEW.direction = 'SELL' THEN
        IF EXISTS (
            SELECT 1 FROM portfolio.position_lot
             WHERE portfolio_id=NEW.portfolio_id AND ts_code=NEW.ts_code
               AND remaining_shares>0 AND available_sell_date>NEW.fill_date
        ) THEN
            RAISE EXCEPTION 'portfolio SELL violates T+1 available date: %', NEW.ts_code;
        END IF;
        IF COALESCE((SELECT SUM(remaining_shares) FROM portfolio.position_lot
                      WHERE portfolio_id=NEW.portfolio_id AND ts_code=NEW.ts_code
                        AND remaining_shares>0),0) < NEW.fill_shares THEN
            RAISE EXCEPTION 'portfolio SELL exceeds available position: %', NEW.ts_code;
        END IF;
    ELSE
        SELECT COALESCE(SUM(amount) FILTER (WHERE available_date<=NEW.fill_date),0)
          INTO available_cash FROM portfolio.cash_ledger WHERE portfolio_id=NEW.portfolio_id;
        required_cash := gross_value + expected_commission + expected_transfer_fee;
        IF required_cash > available_cash * (1-account_reserve_cash) + 1e-8 THEN
            RAISE EXCEPTION 'portfolio BUY exceeds available/reserved cash: %', NEW.intent_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_portfolio_trade_fill ON portfolio.trade_fill;
CREATE TRIGGER trg_portfolio_trade_fill
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.trade_fill
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_trade_fill();

-- Existing valuation rows are given an explicit daily binding before the new
-- append-only valuation trigger is enabled for future rows.
INSERT INTO portfolio.snapshot_binding (portfolio_id, valuation_date, snapshot_id)
SELECT portfolio_id, trade_date, source_snapshot_id FROM portfolio.position_daily
ON CONFLICT DO NOTHING;
INSERT INTO portfolio.snapshot_binding (portfolio_id, valuation_date, snapshot_id)
SELECT portfolio_id, trade_date, source_snapshot_id FROM portfolio.performance_daily
ON CONFLICT DO NOTHING;

COMMENT ON TABLE research.candidate IS
    'Deterministic daily strategy candidates; candidate data is immutable and tied to one FROZEN Snapshot.';
COMMENT ON TABLE research.ai_audit IS
    'AI is an audit layer only; it cannot directly create a fill or mutate a Portfolio ledger.';
COMMENT ON TABLE portfolio.snapshot_binding IS
    'Append-only mapping from a multi-day paper portfolio valuation date to its FROZEN Research Snapshot.';
