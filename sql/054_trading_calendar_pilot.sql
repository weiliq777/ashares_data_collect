-- Physical-migration pilot: the small trading calendar only.
-- No reader is switched by this migration.  The legacy public table remains
-- intact until Repository-level dual-read/dual-write evidence is complete.

CREATE SCHEMA IF NOT EXISTS std;

CREATE TABLE IF NOT EXISTS std.trading_calendar (
    exchange VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    pretrade_date DATE,
    source VARCHAR(30) NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (exchange, trade_date, source)
);

CREATE INDEX IF NOT EXISTS idx_std_trading_calendar_open_date
    ON std.trading_calendar (exchange, trade_date)
    WHERE is_open IS TRUE;

GRANT SELECT, INSERT, UPDATE, DELETE ON std.trading_calendar TO ashares_app;
