-- 阶段三 Portfolio v1：人工确认的模拟组合与不可变事件账本。
-- 本迁移只创建 portfolio 对象，不修改任何 Raw/Standard/PIT/Feature/Research/Backtest 数据。
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS portfolio;
ALTER TABLE IF EXISTS portfolio.account_manifest
    ADD COLUMN IF NOT EXISTS reserve_cash_pct DOUBLE PRECISION NOT NULL DEFAULT 0.10;
ALTER TABLE IF EXISTS portfolio.account_manifest
    ADD COLUMN IF NOT EXISTS cash_settlement_days INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS portfolio.account_manifest (
    portfolio_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_name VARCHAR(160) NOT NULL,
    portfolio_version VARCHAR(40) NOT NULL,
    source_snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    source_watchlist_id UUID REFERENCES research.watchlist(watchlist_id),
    initial_cash NUMERIC(20,6) NOT NULL CHECK (initial_cash > 0),
    min_buy_lot INTEGER NOT NULL DEFAULT 100 CHECK (min_buy_lot > 0),
    allow_sell_odd_lot BOOLEAN NOT NULL DEFAULT TRUE,
    t_plus_one BOOLEAN NOT NULL DEFAULT TRUE,
    reserve_cash_pct DOUBLE PRECISION NOT NULL DEFAULT 0.10 CHECK (reserve_cash_pct >= 0 AND reserve_cash_pct < 1),
    price_tick NUMERIC(12,8) NOT NULL DEFAULT 0.01 CHECK (price_tick > 0),
    commission_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0003 CHECK (commission_rate >= 0),
    min_commission NUMERIC(20,6) NOT NULL DEFAULT 5 CHECK (min_commission >= 0),
    stamp_duty_sell_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0005 CHECK (stamp_duty_sell_rate >= 0),
    transfer_fee_rate DOUBLE PRECISION NOT NULL DEFAULT 0.00001 CHECK (transfer_fee_rate >= 0),
    slippage_bps DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (slippage_bps >= 0),
    cash_settlement_days INTEGER NOT NULL DEFAULT 0 CHECK (cash_settlement_days >= 0),
    status VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    code_version VARCHAR(180),
    git_commit CHAR(40),
    code_archive_path TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at TIMESTAMP,
    archived_at TIMESTAMP,
    UNIQUE (portfolio_name, portfolio_version),
    CHECK (status IN ('DRAFT','ACTIVE','ARCHIVED'))
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'portfolio.account_manifest'::regclass
           AND conname = 'ck_portfolio_cash_settlement_days'
    ) THEN
        ALTER TABLE portfolio.account_manifest
            ADD CONSTRAINT ck_portfolio_cash_settlement_days
            CHECK (cash_settlement_days >= 0);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_portfolio_account_source
    ON portfolio.account_manifest (source_snapshot_id, source_watchlist_id, status);

CREATE TABLE IF NOT EXISTS portfolio.order_intent (
    intent_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id UUID NOT NULL REFERENCES portfolio.account_manifest(portfolio_id),
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    watchlist_id UUID REFERENCES research.watchlist(watchlist_id),
    ts_code VARCHAR(20) NOT NULL,
    decision_date DATE NOT NULL,
    intended_execution_date DATE NOT NULL,
    direction VARCHAR(10) NOT NULL CHECK (direction IN ('BUY','SELL')),
    requested_shares INTEGER NOT NULL CHECK (requested_shares > 0),
    reference_price NUMERIC(20,8),
    stop_loss_price_raw NUMERIC(20,8),
    source_type VARCHAR(20) NOT NULL DEFAULT 'MANUAL',
    status VARCHAR(24) NOT NULL DEFAULT 'PENDING_APPROVAL',
    reason JSONB NOT NULL DEFAULT '{}'::jsonb,
    approved_by VARCHAR(120),
    approved_at TIMESTAMP,
    executed_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (source_type IN ('MANUAL','RESEARCH')),
    CHECK (status IN ('PENDING_APPROVAL','APPROVED','REJECTED','EXECUTED','CANCELLED','EXPIRED')),
    UNIQUE (portfolio_id, intent_id)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_intent_lookup
    ON portfolio.order_intent (portfolio_id, intended_execution_date, status, ts_code);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'portfolio.order_intent'::regclass
           AND conname = 'ck_portfolio_intent_dates'
    ) THEN
        ALTER TABLE portfolio.order_intent
            ADD CONSTRAINT ck_portfolio_intent_dates
            CHECK (decision_date <= intended_execution_date);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS portfolio.trade_fill (
    fill_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id UUID NOT NULL REFERENCES portfolio.account_manifest(portfolio_id),
    intent_id UUID NOT NULL REFERENCES portfolio.order_intent(intent_id),
    ts_code VARCHAR(20) NOT NULL,
    fill_date DATE NOT NULL,
    direction VARCHAR(10) NOT NULL CHECK (direction IN ('BUY','SELL')),
    fill_price NUMERIC(20,8) NOT NULL CHECK (fill_price > 0),
    fill_shares INTEGER NOT NULL CHECK (fill_shares > 0),
    commission NUMERIC(20,8) NOT NULL DEFAULT 0 CHECK (commission >= 0),
    stamp_duty NUMERIC(20,8) NOT NULL DEFAULT 0 CHECK (stamp_duty >= 0),
    transfer_fee NUMERIC(20,8) NOT NULL DEFAULT 0 CHECK (transfer_fee >= 0),
    slippage_cost NUMERIC(20,8) NOT NULL DEFAULT 0 CHECK (slippage_cost >= 0),
    source_type VARCHAR(20) NOT NULL DEFAULT 'PAPER_MANUAL',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (source_type = 'PAPER_MANUAL'),
    UNIQUE (intent_id)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_fill_lookup
    ON portfolio.trade_fill (portfolio_id, fill_date, ts_code, direction);

CREATE TABLE IF NOT EXISTS portfolio.position_lot (
    lot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id UUID NOT NULL REFERENCES portfolio.account_manifest(portfolio_id),
    ts_code VARCHAR(20) NOT NULL,
    open_fill_id UUID NOT NULL UNIQUE REFERENCES portfolio.trade_fill(fill_id),
    open_date DATE NOT NULL,
    available_sell_date DATE NOT NULL,
    open_shares INTEGER NOT NULL CHECK (open_shares > 0),
    remaining_shares INTEGER NOT NULL CHECK (remaining_shares >= 0 AND remaining_shares <= open_shares),
    cost_price NUMERIC(20,8) NOT NULL CHECK (cost_price > 0),
    stop_loss_price_raw NUMERIC(20,8),
    close_date DATE,
    close_fill_id UUID REFERENCES portfolio.trade_fill(fill_id),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (close_date IS NULL OR close_date >= open_date)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_lot_active
    ON portfolio.position_lot (portfolio_id, ts_code, remaining_shares, available_sell_date);

CREATE TABLE IF NOT EXISTS portfolio.cash_ledger (
    ledger_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id UUID NOT NULL REFERENCES portfolio.account_manifest(portfolio_id),
    trade_date DATE NOT NULL,
    event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('INIT','BUY','SELL','DIVIDEND','FEE','ADJUSTMENT')),
    amount NUMERIC(20,8) NOT NULL,
    balance NUMERIC(20,8) NOT NULL CHECK (balance >= 0),
    available_date DATE NOT NULL,
    reference_id UUID,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_portfolio_cash_lookup
    ON portfolio.cash_ledger (portfolio_id, trade_date, ledger_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_cash_ledger_init
    ON portfolio.cash_ledger (portfolio_id)
    WHERE event_type = 'INIT';

CREATE TABLE IF NOT EXISTS portfolio.position_daily (
    portfolio_id UUID NOT NULL REFERENCES portfolio.account_manifest(portfolio_id),
    source_snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    hold_shares INTEGER NOT NULL CHECK (hold_shares >= 0),
    average_cost NUMERIC(20,8),
    raw_close NUMERIC(20,8),
    market_value NUMERIC(20,8),
    unrealized_pnl NUMERIC(20,8),
    valuation_input_hash CHAR(64) NOT NULL,
    valuation_status VARCHAR(30) NOT NULL DEFAULT 'VALID',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (portfolio_id, trade_date, ts_code)
);

CREATE TABLE IF NOT EXISTS portfolio.performance_daily (
    portfolio_id UUID NOT NULL REFERENCES portfolio.account_manifest(portfolio_id),
    source_snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    trade_date DATE NOT NULL,
    cash_balance NUMERIC(20,8) NOT NULL,
    position_value NUMERIC(20,8) NOT NULL,
    total_equity NUMERIC(20,8) NOT NULL,
    daily_return DOUBLE PRECISION,
    cumulative_return DOUBLE PRECISION,
    drawdown DOUBLE PRECISION,
    turnover NUMERIC(20,8) NOT NULL DEFAULT 0,
    valuation_input_hash CHAR(64) NOT NULL,
    valuation_status VARCHAR(30) NOT NULL DEFAULT 'VALID',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (portfolio_id, trade_date)
);

-- 兼容迁移：旧版 Portfolio 表若已存在，按账户绑定的 Research Snapshot 补齐血缘字段，
-- 然后将估值行提升为不可变、可追溯的输入切片。
ALTER TABLE IF EXISTS portfolio.position_daily
    ADD COLUMN IF NOT EXISTS source_snapshot_id UUID REFERENCES research.snapshot_manifest(snapshot_id);
ALTER TABLE IF EXISTS portfolio.position_daily
    ADD COLUMN IF NOT EXISTS valuation_input_hash CHAR(64);
ALTER TABLE IF EXISTS portfolio.performance_daily
    ADD COLUMN IF NOT EXISTS source_snapshot_id UUID REFERENCES research.snapshot_manifest(snapshot_id);
ALTER TABLE IF EXISTS portfolio.performance_daily
    ADD COLUMN IF NOT EXISTS valuation_input_hash CHAR(64);

UPDATE portfolio.position_daily p
   SET source_snapshot_id = a.source_snapshot_id
  FROM portfolio.account_manifest a
 WHERE a.portfolio_id = p.portfolio_id
   AND p.source_snapshot_id IS NULL;
UPDATE portfolio.performance_daily p
   SET source_snapshot_id = a.source_snapshot_id
  FROM portfolio.account_manifest a
 WHERE a.portfolio_id = p.portfolio_id
   AND p.source_snapshot_id IS NULL;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM portfolio.position_daily WHERE source_snapshot_id IS NULL)
       OR EXISTS (SELECT 1 FROM portfolio.performance_daily WHERE source_snapshot_id IS NULL)
       OR EXISTS (SELECT 1 FROM portfolio.position_daily WHERE valuation_input_hash IS NULL)
       OR EXISTS (SELECT 1 FROM portfolio.performance_daily WHERE valuation_input_hash IS NULL) THEN
        RAISE EXCEPTION 'existing portfolio valuation rows lack snapshot/hash lineage; review before migration';
    END IF;
END $$;

ALTER TABLE portfolio.position_daily
    ALTER COLUMN source_snapshot_id SET NOT NULL,
    ALTER COLUMN valuation_input_hash SET NOT NULL;
ALTER TABLE portfolio.performance_daily
    ALTER COLUMN source_snapshot_id SET NOT NULL,
    ALTER COLUMN valuation_input_hash SET NOT NULL;

CREATE TABLE IF NOT EXISTS portfolio.audit_event (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id UUID NOT NULL REFERENCES portfolio.account_manifest(portfolio_id),
    event_date DATE NOT NULL,
    event_type VARCHAR(40) NOT NULL,
    intent_id UUID REFERENCES portfolio.order_intent(intent_id),
    fill_id UUID REFERENCES portfolio.trade_fill(fill_id),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_portfolio_audit_lookup
    ON portfolio.audit_event (portfolio_id, event_date, event_type, created_at);

CREATE OR REPLACE FUNCTION portfolio.assert_valuation_append_only()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    account_snapshot UUID;
    account_status VARCHAR(20);
    snapshot_status VARCHAR(20);
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'portfolio valuation rows are append-only: % / %', OLD.portfolio_id, OLD.trade_date;
    END IF;
    SELECT source_snapshot_id, status
      INTO account_snapshot, account_status
      FROM portfolio.account_manifest
     WHERE portfolio_id = NEW.portfolio_id;
    IF account_snapshot IS NULL OR account_status NOT IN ('ACTIVE','ARCHIVED') THEN
        RAISE EXCEPTION 'valuation requires ACTIVE/ARCHIVED portfolio: %', NEW.portfolio_id;
    END IF;
    IF NEW.source_snapshot_id <> account_snapshot THEN
        RAISE EXCEPTION 'valuation snapshot does not match portfolio account: %', NEW.portfolio_id;
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

DROP TRIGGER IF EXISTS trg_portfolio_position_daily_append_only ON portfolio.position_daily;
CREATE TRIGGER trg_portfolio_position_daily_append_only
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.position_daily
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_valuation_append_only();

DROP TRIGGER IF EXISTS trg_portfolio_performance_daily_append_only ON portfolio.performance_daily;
CREATE TRIGGER trg_portfolio_performance_daily_append_only
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.performance_daily
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_valuation_append_only();

CREATE OR REPLACE FUNCTION portfolio.assert_account_status()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    snapshot_status VARCHAR(20);
    watchlist_status VARCHAR(20);
    watchlist_snapshot UUID;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'portfolio account is append-only; DELETE is forbidden: %', OLD.portfolio_id;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.status IN ('ACTIVE','ARCHIVED') AND
       ROW(OLD.portfolio_name, OLD.portfolio_version, OLD.source_snapshot_id,
           OLD.source_watchlist_id, OLD.initial_cash, OLD.min_buy_lot,
           OLD.allow_sell_odd_lot, OLD.t_plus_one, OLD.price_tick,
           OLD.reserve_cash_pct,
           OLD.commission_rate, OLD.min_commission, OLD.stamp_duty_sell_rate,
           OLD.transfer_fee_rate, OLD.slippage_bps, OLD.cash_settlement_days,
           OLD.code_version,
           OLD.git_commit, OLD.code_archive_path)
       IS DISTINCT FROM
       ROW(NEW.portfolio_name, NEW.portfolio_version, NEW.source_snapshot_id,
           NEW.source_watchlist_id, NEW.initial_cash, NEW.min_buy_lot,
           NEW.allow_sell_odd_lot, NEW.t_plus_one, NEW.price_tick,
           NEW.reserve_cash_pct,
           NEW.commission_rate, NEW.min_commission, NEW.stamp_duty_sell_rate,
           NEW.transfer_fee_rate, NEW.slippage_bps, NEW.cash_settlement_days,
           NEW.code_version,
           NEW.git_commit, NEW.code_archive_path) THEN
        RAISE EXCEPTION 'active/archived portfolio settings are immutable: %', OLD.portfolio_id;
    END IF;
    IF TG_OP = 'INSERT' AND NEW.status <> 'DRAFT' THEN
        RAISE EXCEPTION 'new portfolio accounts must start in DRAFT status: %', NEW.portfolio_id;
    END IF;
    IF TG_OP = 'UPDATE' AND NOT (
        (OLD.status = 'DRAFT' AND NEW.status IN ('DRAFT','ACTIVE','ARCHIVED')) OR
        (OLD.status = 'ACTIVE' AND NEW.status = 'ARCHIVED') OR
        (OLD.status = 'ARCHIVED' AND NEW.status = 'ARCHIVED')
    ) THEN
        RAISE EXCEPTION 'invalid portfolio status transition: % -> %', OLD.status, NEW.status;
    END IF;
    SELECT status INTO snapshot_status
      FROM research.snapshot_manifest WHERE snapshot_id = NEW.source_snapshot_id;
    IF snapshot_status IS NULL THEN
        RAISE EXCEPTION 'portfolio references missing Research Snapshot: %', NEW.source_snapshot_id;
    END IF;
    IF NEW.status = 'ACTIVE' AND snapshot_status <> 'FROZEN' THEN
        RAISE EXCEPTION 'ACTIVE portfolio requires FROZEN Research Snapshot: % (%)',
            NEW.source_snapshot_id, snapshot_status;
    END IF;
    IF NEW.source_watchlist_id IS NOT NULL THEN
        SELECT status, snapshot_id INTO watchlist_status, watchlist_snapshot
          FROM research.watchlist WHERE watchlist_id = NEW.source_watchlist_id;
        IF watchlist_status IS NULL OR watchlist_snapshot <> NEW.source_snapshot_id THEN
            RAISE EXCEPTION 'portfolio watchlist/snapshot mismatch: % / %',
                NEW.source_watchlist_id, NEW.source_snapshot_id;
        END IF;
        IF NEW.status = 'ACTIVE' AND watchlist_status <> 'ACTIVE' THEN
            RAISE EXCEPTION 'ACTIVE portfolio requires ACTIVE watchlist: %', NEW.source_watchlist_id;
        END IF;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.status = 'ACTIVE' AND OLD.status <> 'ACTIVE' THEN
        NEW.activated_at := COALESCE(NEW.activated_at, CURRENT_TIMESTAMP);
    ELSIF TG_OP = 'UPDATE' AND NEW.status = 'ARCHIVED' AND OLD.status <> 'ARCHIVED' THEN
        NEW.archived_at := COALESCE(NEW.archived_at, CURRENT_TIMESTAMP);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_portfolio_account_status ON portfolio.account_manifest;
CREATE TRIGGER trg_portfolio_account_status
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.account_manifest
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_account_status();

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
     WHERE portfolio_id = NEW.portfolio_id
     FOR UPDATE;
    IF account_status IS NULL OR account_status <> 'ACTIVE' THEN
        RAISE EXCEPTION 'portfolio order intent requires ACTIVE account: %', NEW.portfolio_id;
    END IF;
    IF account_snapshot <> NEW.snapshot_id THEN
        RAISE EXCEPTION 'portfolio intent snapshot mismatch: % <> %', NEW.snapshot_id, account_snapshot;
    END IF;
    IF account_watchlist IS NOT NULL
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
    ) THEN
        RAISE EXCEPTION 'portfolio intent decision date is not an SSE open date: %', NEW.decision_date;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM trading_calendar
         WHERE exchange='SSE' AND trade_date=NEW.intended_execution_date AND is_open IS TRUE
    ) THEN
        RAISE EXCEPTION 'portfolio intent execution date is not an SSE open date: %', NEW.intended_execution_date;
    END IF;
    IF account_t_plus_one AND NEW.intended_execution_date <= NEW.decision_date THEN
        RAISE EXCEPTION 'T+1 portfolio intent requires execution date after decision date: %', NEW.intent_id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM research.stock_snapshot_daily
         WHERE snapshot_id = NEW.snapshot_id
           AND research_date = snapshot_cutoff
           AND ts_code = NEW.ts_code
    ) THEN
        RAISE EXCEPTION 'portfolio intent stock is absent from snapshot: %', NEW.ts_code;
    END IF;
    IF NEW.watchlist_id IS NOT NULL THEN
        SELECT snapshot_id, status INTO watchlist_snapshot, watchlist_status
          FROM research.watchlist WHERE watchlist_id = NEW.watchlist_id;
        IF watchlist_snapshot IS NULL OR watchlist_snapshot <> NEW.snapshot_id THEN
            RAISE EXCEPTION 'portfolio intent watchlist/snapshot mismatch: % / %',
                NEW.watchlist_id, NEW.snapshot_id;
        END IF;
        IF watchlist_status <> 'ACTIVE' THEN
            RAISE EXCEPTION 'portfolio intent requires ACTIVE watchlist: %', NEW.watchlist_id;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM research.watchlist_member
             WHERE watchlist_id = NEW.watchlist_id
               AND snapshot_id = NEW.snapshot_id
               AND ts_code = NEW.ts_code
        ) THEN
            RAISE EXCEPTION 'portfolio intent stock is absent from watchlist: %', NEW.ts_code;
        END IF;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.status IN ('APPROVED','EXECUTED') THEN
        IF ROW(OLD.portfolio_id, OLD.snapshot_id, OLD.watchlist_id, OLD.ts_code,
               OLD.decision_date, OLD.intended_execution_date, OLD.direction,
               OLD.requested_shares, OLD.reference_price, OLD.stop_loss_price_raw,
               OLD.source_type, OLD.reason, OLD.approved_by, OLD.approved_at,
               OLD.executed_at, OLD.cancelled_at)
           IS DISTINCT FROM
           ROW(NEW.portfolio_id, NEW.snapshot_id, NEW.watchlist_id, NEW.ts_code,
               NEW.decision_date, NEW.intended_execution_date, NEW.direction,
               NEW.requested_shares, NEW.reference_price, NEW.stop_loss_price_raw,
               NEW.source_type, NEW.reason, NEW.approved_by, NEW.approved_at,
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
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.status = 'CANCELLED' THEN
        NEW.cancelled_at := COALESCE(NEW.cancelled_at, CURRENT_TIMESTAMP);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_portfolio_intent_state ON portfolio.order_intent;
CREATE TRIGGER trg_portfolio_intent_state
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.order_intent
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_intent_state();

CREATE OR REPLACE FUNCTION portfolio.assert_trade_fill()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    intent_status VARCHAR(24);
    intent_direction VARCHAR(10);
    intent_portfolio UUID;
    intent_ts_code VARCHAR(20);
    intent_execution_date DATE;
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
    SELECT status, direction, portfolio_id, ts_code, intended_execution_date,
           requested_shares, stop_loss_price_raw
      INTO intent_status, intent_direction, intent_portfolio, intent_ts_code,
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
    SELECT research_cutoff_date INTO account_cutoff
      FROM research.snapshot_manifest
     WHERE snapshot_id = account_snapshot AND status = 'FROZEN';
    IF account_cutoff IS NULL OR NEW.fill_date > account_cutoff THEN
        RAISE EXCEPTION 'portfolio fill date is after bound FROZEN Snapshot cutoff: %', NEW.fill_id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM trading_calendar
         WHERE exchange='SSE' AND trade_date=NEW.fill_date AND is_open IS TRUE
    ) THEN
        RAISE EXCEPTION 'portfolio fill date is not an SSE open date: %', NEW.fill_date;
    END IF;
    IF abs(NEW.fill_price / account_price_tick - round(NEW.fill_price / account_price_tick)) > 1e-8 THEN
        RAISE EXCEPTION 'portfolio fill price does not match price tick: %', NEW.fill_price;
    END IF;
    IF NEW.direction = 'BUY' AND NEW.fill_shares % min_lot <> 0 THEN
        RAISE EXCEPTION 'portfolio BUY must use min lot %: %', min_lot, NEW.fill_shares;
    END IF;
    IF NEW.direction = 'SELL' AND NOT allow_odd AND NEW.fill_shares % min_lot <> 0 THEN
        RAISE EXCEPTION 'portfolio SELL odd lot is disabled: %', NEW.fill_shares;
    END IF;
    IF NEW.direction = 'BUY' AND intent_stop_loss IS NOT NULL
       AND NEW.fill_price <= intent_stop_loss THEN
        RAISE EXCEPTION 'portfolio BUY fill must be above stop loss: %', NEW.intent_id;
    END IF;
    gross_value := NEW.fill_price * NEW.fill_shares;
    expected_commission := GREATEST(gross_value * account_commission_rate, account_min_commission);
    expected_stamp_duty := CASE
        WHEN NEW.direction = 'SELL' THEN gross_value * account_stamp_duty_rate
        ELSE 0
    END;
    expected_transfer_fee := gross_value * account_transfer_fee_rate;
    IF abs(NEW.commission - expected_commission) > 0.000001
       OR abs(NEW.stamp_duty - expected_stamp_duty) > 0.000001
       OR abs(NEW.transfer_fee - expected_transfer_fee) > 0.000001 THEN
        RAISE EXCEPTION 'portfolio fill fees do not match bound account rules: %', NEW.fill_id;
    END IF;
    IF NEW.direction = 'BUY' AND EXISTS (
        SELECT 1 FROM portfolio.position_lot
         WHERE portfolio_id = NEW.portfolio_id
           AND ts_code = NEW.ts_code
           AND remaining_shares > 0
    ) THEN
        RAISE EXCEPTION 'portfolio v1 does not allow adding to an existing position: %', NEW.ts_code;
    END IF;
    IF NEW.direction = 'SELL' THEN
        IF EXISTS (
            SELECT 1 FROM portfolio.position_lot
             WHERE portfolio_id = NEW.portfolio_id
               AND ts_code = NEW.ts_code
               AND remaining_shares > 0
               AND available_sell_date > NEW.fill_date
        ) THEN
            RAISE EXCEPTION 'portfolio SELL violates T+1 available date: %', NEW.ts_code;
        END IF;
        IF COALESCE((SELECT SUM(remaining_shares)
                       FROM portfolio.position_lot
                      WHERE portfolio_id = NEW.portfolio_id
                        AND ts_code = NEW.ts_code
                        AND remaining_shares > 0), 0) < NEW.fill_shares THEN
            RAISE EXCEPTION 'portfolio SELL exceeds available position: %', NEW.ts_code;
        END IF;
    ELSE
        SELECT COALESCE(SUM(amount) FILTER (WHERE available_date <= NEW.fill_date), 0)
          INTO available_cash
          FROM portfolio.cash_ledger
         WHERE portfolio_id = NEW.portfolio_id;
        required_cash := gross_value
            + expected_commission
            + expected_transfer_fee;
        IF required_cash > available_cash * (1 - account_reserve_cash) + 1e-8 THEN
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

CREATE OR REPLACE FUNCTION portfolio.assert_cash_ledger_append_only()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    previous_balance NUMERIC;
    previous_trade_date DATE;
    initial_cash NUMERIC;
    has_existing BOOLEAN;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'portfolio cash ledger is append-only: %', COALESCE(OLD.ledger_id, NEW.ledger_id);
    END IF;
    IF NEW.available_date < NEW.trade_date THEN
        RAISE EXCEPTION 'portfolio cash cannot be available before its trade date: %', NEW.ledger_id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM trading_calendar
         WHERE exchange='SSE' AND trade_date=NEW.trade_date AND is_open IS TRUE
    ) OR NOT EXISTS (
        SELECT 1 FROM trading_calendar
         WHERE exchange='SSE' AND trade_date=NEW.available_date AND is_open IS TRUE
    ) THEN
        RAISE EXCEPTION 'portfolio cash dates must be SSE open dates: %', NEW.ledger_id;
    END IF;
    SELECT a.initial_cash
      INTO initial_cash
      FROM portfolio.account_manifest a
     WHERE a.portfolio_id = NEW.portfolio_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'portfolio cash ledger requires an existing account: %', NEW.portfolio_id;
    END IF;
    SELECT COALESCE(SUM(amount), 0), MAX(trade_date)
      INTO previous_balance, previous_trade_date
      FROM portfolio.cash_ledger
     WHERE portfolio_id = NEW.portfolio_id;
    SELECT EXISTS (
               SELECT 1 FROM portfolio.cash_ledger
                WHERE portfolio_id = NEW.portfolio_id
           )
      INTO has_existing;
    IF NOT has_existing THEN
        IF NEW.event_type <> 'INIT'
           OR abs(NEW.amount - initial_cash) > 0.000001
           OR abs(NEW.balance - initial_cash) > 0.000001 THEN
            RAISE EXCEPTION 'first portfolio cash ledger row must initialize account cash: %', NEW.ledger_id;
        END IF;
    ELSIF NEW.event_type = 'INIT' THEN
        RAISE EXCEPTION 'portfolio cash ledger can contain only one INIT row: %', NEW.ledger_id;
    END IF;
    IF previous_trade_date IS NOT NULL AND NEW.trade_date < previous_trade_date THEN
        RAISE EXCEPTION 'portfolio cash ledger must be appended in trade-date order: %', NEW.ledger_id;
    END IF;
    IF abs(NEW.balance - (previous_balance + NEW.amount)) > 0.000001 THEN
        RAISE EXCEPTION 'portfolio cash ledger balance does not reconcile: %', NEW.ledger_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_portfolio_cash_ledger_append_only ON portfolio.cash_ledger;
CREATE TRIGGER trg_portfolio_cash_ledger_append_only
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.cash_ledger
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_cash_ledger_append_only();

CREATE OR REPLACE FUNCTION portfolio.assert_position_lot_update()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    account_t_plus_one BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'portfolio position lots cannot be deleted: %', OLD.lot_id;
    END IF;
    IF TG_OP = 'UPDATE' AND ROW(OLD.portfolio_id, OLD.ts_code, OLD.open_fill_id, OLD.open_date,
        OLD.available_sell_date, OLD.open_shares, OLD.cost_price, OLD.stop_loss_price_raw)
        IS DISTINCT FROM ROW(NEW.portfolio_id, NEW.ts_code, NEW.open_fill_id, NEW.open_date,
        NEW.available_sell_date, NEW.open_shares, NEW.cost_price, NEW.stop_loss_price_raw) THEN
        RAISE EXCEPTION 'portfolio position lot opening facts are immutable: %', OLD.lot_id;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.remaining_shares > OLD.remaining_shares THEN
        RAISE EXCEPTION 'portfolio position lot remaining shares cannot increase: %', OLD.lot_id;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.remaining_shares = 0
       AND ROW(NEW.remaining_shares, NEW.close_date, NEW.close_fill_id)
           IS DISTINCT FROM ROW(OLD.remaining_shares, OLD.close_date, OLD.close_fill_id) THEN
        RAISE EXCEPTION 'closed portfolio position lot is immutable: %', OLD.lot_id;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.remaining_shares = 0
       AND (NEW.close_date IS NULL OR NEW.close_fill_id IS NULL) THEN
        RAISE EXCEPTION 'closed portfolio position lot requires close date and fill: %', NEW.lot_id;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.remaining_shares > 0
       AND (NEW.close_date IS NOT NULL OR NEW.close_fill_id IS NOT NULL) THEN
        RAISE EXCEPTION 'open portfolio position lot cannot carry close facts: %', NEW.lot_id;
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT t_plus_one INTO account_t_plus_one
          FROM portfolio.account_manifest WHERE portfolio_id = NEW.portfolio_id;
        IF NOT EXISTS (
            SELECT 1 FROM trading_calendar
             WHERE exchange='SSE' AND trade_date=NEW.open_date AND is_open IS TRUE
        ) OR NOT EXISTS (
            SELECT 1 FROM trading_calendar
             WHERE exchange='SSE' AND trade_date=NEW.available_sell_date AND is_open IS TRUE
        ) THEN
            RAISE EXCEPTION 'portfolio position dates must be SSE open dates: %', NEW.lot_id;
        END IF;
        IF account_t_plus_one AND NEW.available_sell_date <= NEW.open_date THEN
            RAISE EXCEPTION 'T+1 position lot cannot be sold on its open date: %', NEW.lot_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_portfolio_position_lot ON portfolio.position_lot;
CREATE TRIGGER trg_portfolio_position_lot
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.position_lot
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_position_lot_update();

CREATE OR REPLACE FUNCTION portfolio.assert_audit_event_append_only()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'portfolio audit events are append-only: %', OLD.event_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_portfolio_audit_event ON portfolio.audit_event;
CREATE TRIGGER trg_portfolio_audit_event
BEFORE INSERT OR UPDATE OR DELETE ON portfolio.audit_event
FOR EACH ROW EXECUTE FUNCTION portfolio.assert_audit_event_append_only();
